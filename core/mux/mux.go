package mux

import (
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	_ "github.com/arcsecc/lohpi/core/cache"
	"github.com/arcsecc/lohpi/core/comm"
	"github.com/arcsecc/lohpi/core/message"
	"github.com/arcsecc/lohpi/core/netutil"
	pb "github.com/arcsecc/lohpi/protobuf"
	"github.com/lestrrat-go/jwx/jwk"
	"net"
	"net/http"
	"os"
	"sync"
	"time"
	"container/list"

	"crypto/x509/pkix"

	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"

	"github.com/joonnna/ifrit"
)

var (
	errInvalidPermission = errors.New("Requested to send an invalid permission")
)

type Config struct {
	HttpPort   			int    `default:"8080"`
	GRPCPort    		int    `default:"8081"`
	LohpiCaAddr 		string 	`default:"127.0.1.1:8301"`
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

/* Some time in the future we need to redefine the policy definition concepts */
type ClientCheckout struct {
	Dataset string
	Timestamp time.Time
	ClientID string
}

type Mux struct {
	// Configuration
	config     *Config
	configLock sync.RWMutex

	// Underlying Ifrit client
	ifritClient *ifrit.Client

	// In-memory cache structures
	nodeMapLock sync.RWMutex
	nodeMap     map[string]*pb.Node
	//cache        *cache.Cache

	cu *comm.CryptoUnit
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

	// Datasets that have been checked out
	clientCheckoutMap map[string][]string  //datase id -> list of client who have checked out the data
	clientCheckoutMapLock sync.RWMutex

	invalidatedDatasets *list.List
	invalidatedDatasetsLock sync.RWMutex

	// Fetch the JWK
	ar *jwk.AutoRefresh
}

// Returns a new mux using the given configuration and HTTP port number. Returns a non-nil error, if any
func NewMux(config *Config) (*Mux, error) {
	ifritClient, err := ifrit.NewClient()
	if err != nil {
		log.Errorln(err)
		return nil, err
	}

	listener, err := netutil.ListenOnPort(config.GRPCPort)
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

	m := &Mux{
		config:     config,
		configLock: sync.RWMutex{},

		ifritClient: ifritClient,

		// HTTP
		//httpListener: httpListener,
		cu: cu,

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

		clientCheckoutMap: make(map[string][]string),
		invalidatedDatasets: list.New(),
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
	go m.startHttpServer(":8080")
	go m.grpcs.Start()

	log.Println("Mux running gRPC server at", m.grpcs.Addr(), "and Ifrit client at", m.ifritClient.Addr())
}

func (m *Mux) ServeHttp() error {
	log.Println("TOOD: implement ServeHttp()")
	return nil
}

func (m *Mux) Configuration() *Config {
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

func (m *Mux) InitializeLogfile(logToFile bool) error {
	logfilePath := "mux.log"

	if logToFile {
		file, err := os.OpenFile(logfilePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			log.SetOutput(os.Stdout)
			return fmt.Errorf("Could not open logfile %s. Error: %s", logfilePath, err.Error())
		}
		log.SetOutput(file)
		log.SetFormatter(&log.TextFormatter{})
	} else {
		log.Infoln("Setting logs to standard output")
		log.SetOutput(os.Stdout)
	}
	
	return nil
}

// PIVATE METHODS BELOW THIS LINE
func (m *Mux) messageHandler(data []byte) ([]byte, error) {
	msg := &pb.Message{}
	if err := proto.Unmarshal(data, msg); err != nil {
		panic(err)
		log.Errorln(err)
		return nil, err
	}

	if err := m.verifyMessageSignature(msg); err != nil {
		panic(err)
		log.Errorln(err)
		return nil, err
	}

	switch msgType := msg.Type; msgType {
	case message.MSG_TYPE_ADD_DATASET_IDENTIFIER:
		go m.addDatasetNode(msg.GetStringValue(), msg.GetSender())
		
	case message.MSG_POLICY_REVOKE:
		m.addRevokedDataset(msg.GetStringValue())

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

func (m *Mux) addRevokedDataset(dataset string) {
	m.invalidatedDatasetsLock.Lock()
	defer m.invalidatedDatasetsLock.Unlock()
	m.invalidatedDatasets.PushBack(dataset)
}

func (m *Mux) revokedDatasets() *list.List {
	m.invalidatedDatasetsLock.RLock()
	defer m.invalidatedDatasetsLock.RUnlock()
	return m.invalidatedDatasets
}

func (m *Mux) streamHandler(input chan []byte, output chan []byte) {

}

func (m *Mux) addDatasetNode(id string, node *pb.Node) {
	m.datasetNodesMapLock.Lock()
	defer m.datasetNodesMapLock.Unlock()
	m.datasetNodesMap[id] = node
}

func (m *Mux) datasetNodes() map[string]*pb.Node {
	m.datasetNodesMapLock.RLock()
	defer m.datasetNodesMapLock.RUnlock()
	return m.datasetNodesMap
}

func (m *Mux) datasetNode(id string) *pb.Node {
	m.datasetNodesMapLock.RLock()
	defer m.datasetNodesMapLock.RUnlock()
	return m.datasetNodesMap[id]
}

// Adds the given node to the network and returns the Mux's IP address
func (m *Mux) Handshake(ctx context.Context, node *pb.Node) (*pb.HandshakeResponse, error) {
	if _, ok := m.StorageNodes()[node.GetName()]; !ok {
		m.insertStorageNode(node)
		log.Infof("Mux added '%s' to map with Ifrit IP %s and HTTPS adrress %s\n", 
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
