package mux

import (
	"fmt"
	"log"
	"crypto/tls"
	"errors"
	vlog "github.com/inconshreveable/log15"
	"github.com/spf13/viper"
	"github.com/tomcat-bit/lohpi/internal/comm"
	
	"github.com/tomcat-bit/lohpi/internal/core/cache"
	"github.com/tomcat-bit/lohpi/internal/netutil"
	pb "github.com/tomcat-bit/lohpi/protobuf"
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

type Config struct {
	MuxIP					string		`default:"127.0.1.1:8080"`
	LohpiCaAddr 			string		`default:"127.0.1.1:8301"`
	RecIP 					string 		`default:"127.0.1.1:8083"`
}

type service interface {
	Register(pb.MuxServer)
	StudyList()
	Handshake()
}

type Mux struct {
	// Configuration
	config *Config

	// Underlying Ifrit client
	ifritClient *ifrit.Client

	// In-memory cache structures
	nodeListLock sync.RWMutex
	cache        *cache.Cache

	// HTTP
	portNum      int
	listener     net.Listener
	httpServer   *http.Server
	serverConfig *tls.Config

	// HTTP-related stuff. Used by the demonstrator using cURL
	httpPortNum   int
	_httpListener net.Listener
	_httpServer   *http.Server

	// Sync
	wg       *sync.WaitGroup
	exitChan chan bool

	// gRPC service
	grpcs *gRPCServer
	s service

	// Rec client
	recClient *comm.RecGRPCClient

	// Ignored IP addresses for the Ifrit client
	ignoredIP map[string]string
}

func NewMux(config *Config, httpPortNum int) (*Mux, error) {
	ifritClient, err := ifrit.NewClient()
	if err != nil {
		return nil, err
	}

	go ifritClient.Start()
	//ifritClient.RegisterGossipHandler(self.GossipMessageHandler)
	//ifritClient.RegisterResponseHandler(self.GossipResponseHandler)

	portString := strings.Split(viper.GetString("lohpi_mux_addr"), ":")[1]
	port, err := strconv.Atoi(portString)
	if err != nil {
		return nil, err
	}

	netutil.ValidatePortNumber(&port)
	listener, err := netutil.ListenOnPort(port)
	if err != nil {
		return nil, err
	}

	vlog.Debug("addrs", "rpc", listener.Addr().String(), "udp")

	pk := pkix.Name{
		Locality: []string{listener.Addr().String()},
	}

	cu, err := comm.NewCu(pk, viper.GetString("lohpi_ca_addr"))
	if err != nil {
		return nil, err
	}

	s, err := newMuxGRPCServer(cu.Certificate(), cu.CaCertificate(), cu.Priv(), listener)
	if err != nil {
		return nil, err
	}
	
	recClient, err := comm.NewRecClient(cu.Certificate(), cu.CaCertificate(), cu.Priv())
	if err != nil {
		return nil, err
	}

	// Initiate HTTP connection without TLS. Used by demonstrator
	_httpListener, err := netutil.ListenOnPort(httpPortNum)
	if err != nil {
		panic(err)
	}

	cache, err := cache.NewCache(ifritClient)
	if err != nil {
		return nil, err
	}

	m := &Mux{
		config: config, 

		ifritClient: ifritClient,
		exitChan:    make(chan bool, 1),

		// In-memory caches used to describe the network data
		cache: cache,

		// HTTPS
		portNum:      port,

		// HTTP
		httpPortNum:   httpPortNum,
		_httpListener: _httpListener,

		// Sync
		nodeListLock: sync.RWMutex{},
		wg:           &sync.WaitGroup{},

		// gRPC server
		grpcs: s,

		// Rec grpc client
		recClient:		recClient,

		ignoredIP: 		make(map[string]string),
	}

	m.grpcs.Register(m)
	return m, nil
}

func (m *Mux) Start() {
	go m.ifritClient.Start()
	go m.HttpHandler()
	//go m.HttpsHandler()
	go m.grpcs.Start()
	// sync here?

	// Handshake with policy store
	log.Println("Mux is ready")
}

func (m *Mux) StudyList(ctx context.Context, e *empty.Empty) (*pb.Studies, error) {
	m.cache.FetchRemoteStudyLists()
	studies := &pb.Studies{
		Studies: make([]*pb.Study, 0),
	}

	for s := range m.cache.Studies() {
		study := pb.Study{
			Name: s,
		}

		studies.Studies = append(studies.Studies, &study)
	}
	return studies, nil
}

func (m *Mux) Handshake(ctx context.Context, node *pb.Node) (*pb.HandshakeResponse, error) {
	if !m.cache.NodeExists(node.GetName()) {
		m.cache.UpdateNodes(node.GetName(), node.GetAddress())
	} else {
		errMsg := fmt.Sprintf("Mux: node '%s' already exists in network\n", node.GetName())
		return nil, errors.New(errMsg)
	}
	log.Printf("Mux: added %s to map with IP %s\n", node.GetName(), node.GetAddress())
	return &pb.HandshakeResponse{
		Ip: m.ifritClient.Addr(),
	}, nil
}

func (m *Mux) IgnoreIP(ctx context.Context, node *pb.Node) (*pb.Node, error) {
	m.ignoredIP[node.GetName()] = node.GetAddress()
	return &pb.Node{
		Name: "Mux",
		Address: m.ifritClient.Addr(),
	}, nil
}

func (m *Mux) StudyMetadata(context.Context, *pb.Study) (*pb.Metadata, error) {
	return nil, nil 
}

func (m *Mux) Stop() {
	m.ifritClient.Stop()
}

