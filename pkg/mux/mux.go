package mux

import (
	"fmt"
	"log"
	"crypto/tls"
	"errors"
	"github.com/tomcat-bit/lohpi/pkg/comm"
	
	"github.com/tomcat-bit/lohpi/pkg/cache"
	"github.com/tomcat-bit/lohpi/pkg/netutil"
	pb "github.com/tomcat-bit/lohpi/protobuf"
	"github.com/golang/protobuf/proto"
	"github.com/tomcat-bit/lohpi/pkg/message"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
_	"context"

	"crypto/x509/pkix"

	"github.com/golang/protobuf/ptypes/empty"
	"golang.org/x/net/context"

	"github.com/joonnna/ifrit"
)

var (
	errInvalidPermission = errors.New("Requested to send an invalid permission")
)

type MuxConfig struct {
	MuxHttpPort				int 		`default:8080`
	MuxHttpsIP				string		`default:"127.0.1.1:8081"`
	PolicyStoreIP			string 		`default:"127.0.1.1:8082"`
	LohpiCaAddr 			string		`default:"127.0.1.1:8301"`
	RecIP 					string 		`default:"127.0.1.1:8084"`
	CacheSize 				int 		`default: 1000`
}

type service interface {
	Register(pb.MuxServer)
	StudyList()
	Handshake()
}

// 
type NeighborNode struct {

}

type Mux struct {
	// Configuration
	config *MuxConfig

	// Underlying Ifrit client
	ifritClient *ifrit.Client

	// In-memory cache structures
	nodeListLock sync.RWMutex
	cache        *cache.Cache

	// HTTP-related stuff. Used by the demonstrator using cURL
	httpListener net.Listener
	httpServer   *http.Server

	// Sync
	wg       *sync.WaitGroup
	exitChan chan bool

	// gRPC service
	listener     	net.Listener
	serverConfig 	*tls.Config
	grpcs 			*gRPCServer
	s 				service

	// Rec client
	recClient *comm.RecGRPCClient

	// Ignored IP addresses for the Ifrit client
	ignoredIP map[string]string
}

// Returns a new mux using the given configuration and HTTP port number. Returns a non-nil error, if any
func NewMux(config *MuxConfig) (*Mux, error) {
	ifritClient, err := ifrit.NewClient()
	if err != nil {
		return nil, err
	}

	go ifritClient.Start()
	//ifritClient.RegisterGossipHandler(self.GossipMessageHandler)
	//ifritClient.RegisterResponseHandler(self.GossipResponseHandler)

	portString := strings.Split(config.MuxHttpsIP, ":")[1]
	port, err := strconv.Atoi(portString)
	if err != nil {
		return nil, err
	}

	netutil.ValidatePortNumber(&port)
	listener, err := netutil.ListenOnPort(port)
	if err != nil {
		return nil, err
	}

	pk := pkix.Name{
		Locality: []string{listener.Addr().String()},
	}

	cu, err := comm.NewCu(pk, config.LohpiCaAddr)
	if err != nil {
		return nil, err
	}

	s, err := newMuxGRPCServer(cu.Certificate(), cu.CaCertificate(), cu.Priv(), listener)
	if err != nil {
		return nil, err
	}
	
	// Move me somewhere else because im not a part of the Lohpi network!
	recClient, err := comm.NewRecClient(cu.Certificate(), cu.CaCertificate(), cu.Priv())
	if err != nil {
		return nil, err
	}

	// Initiate HTTP connection without TLS. Used by demonstrator
	httpListener, err := netutil.ListenOnPort(config.MuxHttpPort)
	if err != nil {
		return nil, err
	}

	cache := cache.NewCache(ifritClient)

	m := &Mux{
		config: config, 

		ifritClient: ifritClient,
		exitChan:    make(chan bool, 1),

		// In-memory caches used to describe the network data
		cache: cache,

		// HTTP
		httpListener: httpListener,

		// Sync
		nodeListLock: sync.RWMutex{},
		wg:           &sync.WaitGroup{},

		// gRPC server
		grpcs: s,

		// Rec grpc client
		recClient:		recClient,

		// Collection if nodes that should be ignored in certain cases
		ignoredIP: 		make(map[string]string),
	}

	m.grpcs.Register(m)

	return m, nil
}

func (m *Mux) Start() {
	go m.ifritClient.Start()
	m.ifritClient.RegisterMsgHandler(m.messageHandler)
	go m.HttpHandler()
	go m.grpcs.Start()
	
	log.Println("Mux running gRPC server at", m.grpcs.Addr(), "and Ifrit client at", m.ifritClient.Addr())
}

func (m *Mux) messageHandler(data []byte) ([]byte, error) {
	msg := &pb.Message{}
	if err := proto.Unmarshal(data, msg); err != nil {
		panic(err)
	}

	if err := m.verifyMessageSignature(msg); err != nil {
		panic(err)
	}

	switch msgType := msg.Type; msgType {
	case message.MSG_TYPE_OBJECT_HEADER_LIST:
		m.updateObjectHeaders(msg.GetObjectHeaders())
	default:
		fmt.Printf("Unknown message type: %s\n", msg.GetType())
	}

	resp, err := proto.Marshal(&pb.Message{Type: message.MSG_TYPE_OK})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (m *Mux) Configuration() *MuxConfig {
	return m.config
}

func (m *Mux) Stop() {
	m.ifritClient.Stop()
}

// Updates the internal cache by assigning the object headers' ids to the object headers themselves
func (m *Mux) updateObjectHeaders(objectHeaders *pb.ObjectHeaders) {
	for _, header := range objectHeaders.GetObjectHeaders() {
		objectID := header.GetName()
		m.cache.InsertObjectHeader(objectID, header)
	}
}

// Returns a list of studies available to the network
func (m *Mux) GetObjectHeaders(ctx context.Context, e *empty.Empty) (*pb.ObjectHeaders, error) {
	/*m.cache.FetchRemoteObjectHeaders()
	studies := &pb.ObjectHeaders{
		ObjectHeaders: make([]*pb.ObjectHeader, 0),
	}

	for objectName, objectValue := range m.cache.ObjectHeaders() {
		object := pb.ObjectHeader{
			Name: 		objectName,
			Node:		&pb.Node{
				Address:	objectValue.GetAddress(),
		}

		studies.Studies = append(studies.Studies, &study)
	}*/
		return nil, nil
	//return studies, nil
}

func (m *Mux) GetObjectMetadata(ctx context.Context, req *pb.DataUserRequest) (*pb.Metadata, error) {
	return nil, nil
}

func (m *Mux) GetSubjectNumber(ctx context.Context, req *pb.DataUserRequest) (*pb.StudyCount, error) {
	return nil, nil
}

func (m *Mux) GetObjectData(ctx context.Context, req *pb.DataUserRequest) (*pb.ObjectData, error) {
	return nil, nil
}

// Adds the given node to the network and returns the Mux's IP address
func (m *Mux) Handshake(ctx context.Context, node *pb.Node) (*pb.HandshakeResponse, error) {
	if !m.cache.NodeExists(node.GetName()) {
		m.cache.InsertNode(node.GetName(), &pb.Node{
			Name: 			node.GetName(),
			Address: 		node.GetAddress(),
			Role: 			node.GetRole(),
			ContactEmail: 	node.GetContactEmail(),
			Id: 			node.GetId(),
		})
		log.Printf("Mux added %s to map with IP %s\n", node.GetName(), node.GetAddress())
	} else {
		errMsg := fmt.Sprintf("Mux: node '%s' already exists in network\n", node.GetName())
		return nil, errors.New(errMsg)
	}
	return &pb.HandshakeResponse{
		Ip: m.ifritClient.Addr(),
		Id: []byte(m.ifritClient.Id()),
	}, nil
}

// Adds the given node to the list of ignored IP addresses and returns the Mux's IP address 
func (m *Mux) IgnoreIP(ctx context.Context, node *pb.Node) (*pb.Node, error) {
	m.ignoredIP[node.GetName()] = node.GetAddress()
	return &pb.Node{
		Name: "Mux",
		Address: m.ifritClient.Addr(),
	}, nil
}

func (m *Mux) StudyMetadata(context.Context, *pb.ObjectHeader) (*pb.Metadata, error) {
	return nil, nil 
}

// Invoked by ifrit message handler
func (m *Mux) verifyMessageSignature(msg *pb.Message) error {
	return nil
	// Verify the integrity of the node
	r := msg.GetSignature().GetR()
	s := msg.GetSignature().GetS()

	log.Printf("MSG: %v+\n", msg)

	msg.Signature = nil

	// Marshal it before verifying its integrity
	data, err := proto.Marshal(msg)
	if err != nil {
		panic(err)
	}	
	
	if !m.ifritClient.VerifySignature(r, s, data, string(msg.GetSender().GetId())) {
		return errors.New("Mux could not securely verify the integrity of the message")
	}

	// Restore message. Defer?
	msg.Signature = &pb.MsgSignature{
		R: r,
		S: s,
	}

	return nil
}