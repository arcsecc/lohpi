package mux

import (
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	_ "github.com/tomcat-bit/lohpi/core/cache"
	"github.com/tomcat-bit/lohpi/core/comm"
	"github.com/tomcat-bit/lohpi/core/message"
	"github.com/tomcat-bit/lohpi/core/netutil"
	pb "github.com/tomcat-bit/lohpi/protobuf"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"crypto/x509/pkix"

	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"

	"github.com/joonnna/ifrit"
)

var (
	errInvalidPermission = errors.New("Requested to send an invalid permission")
)

type MuxConfig struct {
	MuxHttpPort   int    `default:8080`
	MuxHttpsIP    string `default:"127.0.1.1:8081"`
	PolicyStoreIP string `default:"127.0.1.1:8082"`
	LohpiCaAddr   string `default:"127.0.1.1:8301"`
	RecIP         string `default:"127.0.1.1:8084"`
}

type service interface {
	Register(pb.MuxServer)
	StudyList()
	Handshake()
}

type Event struct {
	Timestamp   time.Time
	Description string
}

type Mux struct {
	// Configuration
	config     *MuxConfig
	configLock sync.RWMutex

	// Underlying Ifrit client
	ifritClient *ifrit.Client

	// In-memory cache structures
	nodeMapLock sync.RWMutex
	nodeMap     map[string]*pb.Node
	//cache        *cache.Cache

	// Dataset nodes
	datasetNodesMap     map[string]*pb.Node // Dataset identifier mapped to storage node
	datasetNodesMapLock sync.RWMutex

	// HTTP-related stuff. Used by the demonstrator using cURL
	httpListener net.Listener
	httpServer   *http.Server

	// gRPC service
	listener     net.Listener
	serverConfig *tls.Config
	grpcs        *gRPCServer
	s            service

	// Ignored IP addresses for the Ifrit client
	ignoredIP map[string]string

	eventChan <-chan Event
}

// Returns a new mux using the given configuration and HTTP port number. Returns a non-nil error, if any
func NewMux(config *MuxConfig) (*Mux, error) {
	ifritClient, err := ifrit.NewClient()
	if err != nil {
		log.Errorln(err)
		return nil, err
	}

	go ifritClient.Start()

	portString := strings.Split(config.MuxHttpsIP, ":")[1]
	port, err := strconv.Atoi(portString)
	if err != nil {
		log.Errorln(err)
		return nil, err
	}

	netutil.ValidatePortNumber(&port)
	listener, err := netutil.ListenOnPort(port)
	if err != nil {
		log.Errorln(err)
		return nil, err
	}

	pk := pkix.Name{
		CommonName: "Mux",
		Locality:   []string{listener.Addr().String()},
	}

	cu, err := comm.NewCu(pk, config.LohpiCaAddr)
	if err != nil {
		log.Errorln(err)
		return nil, err
	}

	s, err := newMuxGRPCServer(cu.Certificate(), cu.CaCertificate(), cu.Priv(), listener)
	if err != nil {
		log.Errorln(err)
		return nil, err
	}

	httpListener, err := netutil.ListenOnPort(config.MuxHttpPort)
	if err != nil {
		log.Errorln(err)
		return nil, err
	}

	m := &Mux{
		config:     config,
		configLock: sync.RWMutex{},

		ifritClient: ifritClient,

		// HTTP
		httpListener: httpListener,

		// Network nodes
		nodeMap:     make(map[string]*pb.Node),
		nodeMapLock: sync.RWMutex{},

		// Dataset nodes
		datasetNodesMap:     make(map[string]*pb.Node),
		datasetNodesMapLock: sync.RWMutex{},

		// gRPC server
		grpcs: s,

		// Collection if nodes that should be ignored in certain cases
		ignoredIP: make(map[string]string),

		eventChan: make(<-chan Event),
	}

	m.grpcs.Register(m)
	m.ifritClient.RegisterStreamHandler(m.streamHandler)
	m.ifritClient.RegisterMsgHandler(m.messageHandler)
	//ifritClient.RegisterGossipHandler(self.GossipMessageHandler)
	//ifritClient.RegisterResponseHandler(self.GossipResponseHandler)

	return m, nil
}

func (m *Mux) Start() {
	go m.ifritClient.Start()
	go m.startHttpServer()
	go m.grpcs.Start()

	log.Println("Mux running gRPC server at", m.grpcs.Addr(), "and Ifrit client at", m.ifritClient.Addr())
}

func (m *Mux) ServeHttp() error {
	log.Println("TOOD: implement ServeHttp()")
	return nil
}

func (m *Mux) Configuration() *MuxConfig {
	m.configLock.RLock()
	defer m.configLock.RUnlock()
	return m.config
}

func (m *Mux) Stop() {
	m.ifritClient.Stop()
	m.shutdownHttpServer()
}

func (m *Mux) StorageNodes() map[string]*pb.Node {
	m.nodeMapLock.RLock()
	defer m.nodeMapLock.RUnlock()
	return m.nodeMap
}

func (m *Mux) InitializeLogfile() error {
	logfilePath := "mux.log"

	file, err := os.OpenFile(logfilePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.SetOutput(os.Stdout)
		return fmt.Errorf("Could not open logfile %s. Error: %s", logfilePath, err.Error())
	}

	log.SetOutput(file)
	log.SetFormatter(&log.TextFormatter{})
	return nil
}

// Returns the event channel that can be read from
func (m *Mux) EventChannel() <-chan Event {
	return m.eventChan
}

// PIVATE METHODS BELOW THIS LINE
func (m *Mux) messageHandler(data []byte) ([]byte, error) {
	msg := &pb.Message{}
	if err := proto.Unmarshal(data, msg); err == nil {
		log.Errorln(err)
		return nil, err
	}

	if err := m.verifyMessageSignature(msg); err != nil {
		log.Errorln(err)
		return nil, err
	}

	switch msgType := msg.Type; string(msgType) {
	case string(message.MSG_TYPE_INSERT_DATASET):
		m.insertDataset(msg)
	default:
		log.Warnln("Unknown message type at mux handler: %s\n", msg.GetType())
	}

	resp, err := proto.Marshal(&pb.Message{Type: message.MSG_TYPE_OK})
	if err != nil {
		log.Errorln(err)
		return nil, err
	}

	return resp, nil
}

func (m *Mux) streamHandler(input chan []byte, output chan []byte) {

}

func (m *Mux) insertDataset(msg *pb.Message) {
	m.datasetNodesMapLock.Lock()
	defer m.datasetNodesMapLock.Unlock()
	id := msg.GetDataset().GetName()
	m.datasetNodesMap[id] = msg.GetSender()
}

func (m *Mux) datasetMap() map[string]*pb.Node {
	m.datasetNodesMapLock.RLock()
	defer m.datasetNodesMapLock.RUnlock()
	return m.datasetNodesMap
}

// Adds the given node to the network and returns the Mux's IP address
func (m *Mux) Handshake(ctx context.Context, node *pb.Node) (*pb.HandshakeResponse, error) {
	if _, ok := m.StorageNodes()[node.GetName()]; !ok {
		m.insertStorageNode(node)
		log.Infoln("Mux added %s to map with Ifrit IP %s and HTTPS adrress %s\n", 
			node.GetName(), node.GetIfritAddress(), node.GetHttpAddress())
	} else {
		return nil, fmt.Errorf("Mux: node '%s' already exists in network\n", node.GetName())
	}
	return &pb.HandshakeResponse{
		Ip: m.ifritClient.Addr(),
		Id: []byte(m.ifritClient.Id()),
	}, nil
}

// Adds the given node to the list of ignored IP addresses and returns the Mux's IP address
func (m *Mux) IgnoreIP(ctx context.Context, node *pb.Node) (*pb.Node, error) {
	m.ignoredIP[node.GetName()] = node.GetIfritAddress()
	return &pb.Node{
		Name:    "Mux",
		IfritAddress: m.ifritClient.Addr(),
		// HTTP
	}, nil
}

// Invoked by ifrit message handler
func (m *Mux) verifyMessageSignature(msg *pb.Message) error {
	return nil 
	// Verify the integrity of the node
	r := msg.GetSignature().GetR()
	s := msg.GetSignature().GetS()

	msg.Signature = nil

	// Marshal it before verifying its integrity
	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	if !m.ifritClient.VerifySignature(r, s, data, string(msg.GetSender().GetId())) {
		return errors.New("Mux could not securely verify the integrity of the message")
	}

	// Restore message
	msg.Signature = &pb.MsgSignature{
		R: r,
		S: s,
	}

	return nil
}

func (m *Mux) insertStorageNode(node *pb.Node) {
	m.nodeMapLock.Lock()
	defer m.nodeMapLock.Unlock()
	m.nodeMap[node.GetName()] = node
}

func (m *Mux) nodeExists(name string) bool {
	m.nodeMapLock.RLock()
	defer m.nodeMapLock.RUnlock()
	_, ok := m.nodeMap[name]
	return ok
}

func (m *Mux) pbNode() *pb.Node {
	return &pb.Node{
		Name:    "Lohpi mulitplexer",
		IfritAddress: m.ifritClient.Addr(),
		Role:    "MUX",
		//ContactEmail
		Id: []byte(m.ifritClient.Id()),
		//HttpAddress:
	}
}
