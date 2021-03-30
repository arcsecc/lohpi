package directoryserver

import (
	"container/list"
	"crypto/tls"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"github.com/arcsecc/lohpi/core/cache"
	"github.com/arcsecc/lohpi/core/comm"
	"github.com/arcsecc/lohpi/core/message"
	"github.com/arcsecc/lohpi/core/netutil"
	pb "github.com/arcsecc/lohpi/protobuf"
	"github.com/golang/protobuf/proto"
	"github.com/joonnna/ifrit"
	"github.com/lestrrat-go/jwx/jwk"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"net"
	"net/http"
	"os"
	"sync"
)

var (
	errInvalidPermission = errors.New("Requested to send an invalid permission")
)

type Config struct {
	HttpPort    int    `default:"8080"`
	GRPCPort    int    `default:"8081"`
	LohpiCaAddr string `default:"127.0.1.1:8301"`
}

type DirectoryServer struct {
	// Configuration
	config     *Config
	configLock sync.RWMutex

	// Underlying Ifrit client
	ifritClient *ifrit.Client
	memCache    *cache.Cache

	// In-memory cache structures
	nodeMapLock sync.RWMutex
	nodeMap     map[string]*pb.Node

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

	// Datasets that have been checked out
	clientCheckoutMap     map[string][]string //datase id -> list of client who have checked out the data
	clientCheckoutMapLock sync.RWMutex

	invalidatedDatasets     *list.List
	invalidatedDatasetsLock sync.RWMutex

	// Fetch the JWK
	pubKeyCache *jwk.AutoRefresh
}

// Returns a new DirectoryServer using the given configuration and HTTP port number. Returns a non-nil error, if any
func NewDirectoryServer(config *Config) (*DirectoryServer, error) {
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
		CommonName: "DirectoryServer",
		Locality:   []string{listener.Addr().String()},
	}

	cu, err := comm.NewCu(pk, config.LohpiCaAddr)
	if err != nil {
		log.Errorln(err)
		return nil, err
	}

	s, err := newDirectoryGRPCServer(cu.Certificate(), cu.CaCertificate(), cu.Priv(), listener)
	if err != nil {
		log.Errorln(err)
		return nil, err
	}

	ds := &DirectoryServer{
		config:      config,
		configLock:  sync.RWMutex{},
		ifritClient: ifritClient,

		// HTTP
		cu: cu,

		// Network nodes
		nodeMap:     make(map[string]*pb.Node),
		nodeMapLock: sync.RWMutex{},

		// Dataset nodes
		datasetNodesMap:     make(map[string]*pb.Node),
		datasetNodesMapLock: sync.RWMutex{},

		// gRPC server
		grpcs: s,

		memCache: cache.NewCache(ifritClient),
	}

	ds.grpcs.Register(ds)
	ds.ifritClient.RegisterStreamHandler(ds.streamHandler)
	ds.ifritClient.RegisterMsgHandler(ds.messageHandler)
	//ifritClient.RegisterGossipHandler(self.GossipMessageHandler)
	//ifritClient.RegisterResponseHandler(self.GossipResponseHandler)

	return ds, nil
}

func (d *DirectoryServer) Start() {
	log.Infoln("Directory server running gRPC server at", d.grpcs.Addr(), "and Ifrit client at", d.ifritClient.Addr())
	go d.ifritClient.Start()
	go d.startHttpServer(":8080")
	go d.grpcs.Start()
}

func (d *DirectoryServer) Configuration() *Config {
	d.configLock.RLock()
	defer d.configLock.RUnlock()
	return d.config
}

func (d *DirectoryServer) Stop() {
	d.ifritClient.Stop()
	d.shutdownHttpServer()
}

func (d *DirectoryServer) Cache() *cache.Cache {
	return d.memCache
}

func (d *DirectoryServer) InitializeLogfile(logToFile bool) error {
	logfilePath := "DirectoryServer.log"

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
func (d *DirectoryServer) messageHandler(data []byte) ([]byte, error) {
	msg := &pb.Message{}
	if err := proto.Unmarshal(data, msg); err != nil {
		log.Errorln(err)
		return nil, err
	}

	if err := d.verifyMessageSignature(msg); err != nil {
		log.Errorln(err)
		return nil, err
	}

	switch msgType := msg.Type; msgType {
	case message.MSG_TYPE_ADD_DATASET_IDENTIFIER:
		d.memCache.AddDatasetNode(msg.GetStringValue(), msg.GetSender())

	default:
		log.Warnln("Unknown message type at DirectoryServer handler: %s\n", msg.GetType())
	}

	resp, err := proto.Marshal(&pb.Message{Type: message.MSG_TYPE_OK})
	if err != nil {
		log.Errorln(err)
		return nil, err
	}

	return resp, nil
}

func (d *DirectoryServer) streamHandler(input chan []byte, output chan []byte) {

}

// Adds the given node to the network and returns the DirectoryServer's IP address
func (d *DirectoryServer) Handshake(ctx context.Context, node *pb.Node) (*pb.HandshakeResponse, error) {
	if _, ok := d.memCache.Nodes()[node.GetName()]; !ok {
		d.memCache.AddNode(node.GetName(), node)
		log.Infof("DirectoryServer added '%s' to map with Ifrit IP %s and HTTPS adrress %s\n",
			node.GetName(), node.GetIfritAddress(), node.GetHttpAddress())
	} else {
		return nil, fmt.Errorf("DirectoryServer: node '%s' already exists in network\n", node.GetName())
	}
	return &pb.HandshakeResponse{
		Ip: d.ifritClient.Addr(),
		Id: []byte(d.ifritClient.Id()),
	}, nil
}

// Invoked by ifrit message handler
func (d *DirectoryServer) verifyMessageSignature(msg *pb.Message) error {
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

	if !d.ifritClient.VerifySignature(r, s, data, string(msg.GetSender().GetId())) {
		return errors.New("DirectoryServer could not securely verify the integrity of the message")
	}

	// Restore message
	msg.Signature = &pb.MsgSignature{
		R: r,
		S: s,
	}

	return nil
}

func (d *DirectoryServer) revokedDatasets() *list.List {
	d.invalidatedDatasetsLock.RLock()
	defer d.invalidatedDatasetsLock.RUnlock()
	return d.invalidatedDatasets
}

func (d *DirectoryServer) pbNode() *pb.Node {
	return &pb.Node{
		Name:         "Lohpi directory server",
		IfritAddress: d.ifritClient.Addr(),
		Role:         "Directory server",
		Id:           []byte(d.ifritClient.Id()),
	}
}
