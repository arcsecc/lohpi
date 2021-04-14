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
	"strconv"
	"net/http"
	"sync"
)

type Config struct {
	HTTPPort    	int    	
	GRPCPort    	int    	
	LohpiCaAddress 	string
	LohpiCaPort		int 	
	UseTLS			bool 	
	CertificateFile string 	
	PrivateKeyFile	string 	
}

type DirectoryServerCore struct {
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

	// HTTP-related stuff. Used by the demonstrator using cURL
	httpListener net.Listener
	httpServer   *http.Server

	// gRPC service
	listener     net.Listener
	serverConfig *tls.Config
	grpcs        *gRPCServer

	// Datasets that have been checked out
	// TODO move me to memcache?
	clientCheckoutMap     map[string][]string //datase id -> list of client who have checked out the data
	clientCheckoutMapLock sync.RWMutex

	invalidatedDatasets     *list.List
	invalidatedDatasetsLock sync.RWMutex

	// Fetch the JWK
	pubKeyCache *jwk.AutoRefresh
}

// Returns a new DirectoryServer using the given configuration and HTTP port number. Returns a non-nil error, if any
func NewDirectoryServerCore(config *Config) (*DirectoryServerCore, error) {
	ifritClient, err := ifrit.NewClient()
	if err != nil {
		return nil, err
	}

	listener, err := netutil.ListenOnPort(config.GRPCPort)
	if err != nil {
		return nil, err
	}

	pk := pkix.Name{
		CommonName: "DirectoryServerCore",
		Locality:   []string{listener.Addr().String()},
	}

	cu, err := comm.NewCu(pk, config.LohpiCaAddress + ":" + strconv.Itoa(config.LohpiCaPort))
	if err != nil {
		return nil, err
	}

	s, err := newDirectoryGRPCServer(cu.Certificate(), cu.CaCertificate(), cu.Priv(), listener)
	if err != nil {
		return nil, err
	}

	ds := &DirectoryServerCore{
		config:      config,
		configLock:  sync.RWMutex{},
		ifritClient: ifritClient,

		// HTTP
		cu: cu,

		// gRPC server
		grpcs: s,

		memCache: cache.NewCache(ifritClient),

		clientCheckoutMap: make(map[string][]string, 0),
		invalidatedDatasets: list.New(),
	}

	ds.grpcs.Register(ds)
	ds.ifritClient.RegisterMsgHandler(ds.messageHandler)
	//ifritClient.RegisterGossipHandler(self.GossipMessageHandler)
	//ifritClient.RegisterResponseHandler(self.GossipResponseHandler)

	return ds, nil
}

func (d *DirectoryServerCore) Start() {
	log.Infoln("Directory server running gRPC server at", d.grpcs.Addr(), "and Ifrit client at", d.ifritClient.Addr())
	go d.ifritClient.Start()
	go d.startHttpServer(":" + string(d.config.HTTPPort))
	go d.grpcs.Start()
}

func (d *DirectoryServerCore) Stop() {
	d.ifritClient.Stop()
	d.shutdownHttpServer()
}

func (d *DirectoryServerCore) Cache() *cache.Cache {
	return d.memCache
}

// PIVATE METHODS BELOW THIS LINE
func (d *DirectoryServerCore) messageHandler(data []byte) ([]byte, error) {
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

	case message.MSG_POLICY_REVOKE:
		d.addRevokedDataset(msg.GetStringValue())

	default:
		log.Warnln("Unknown message type at DirectoryServerCore handler: %s\n", msg.GetType())
	}

	resp, err := proto.Marshal(&pb.Message{Type: message.MSG_TYPE_OK})
	if err != nil {
		log.Errorln(err)
		return nil, err
	}

	return resp, nil
}

func (d *DirectoryServerCore) addRevokedDataset(dataset string) {
	d.invalidatedDatasetsLock.Lock()
	defer d.invalidatedDatasetsLock.Unlock()
	d.invalidatedDatasets.PushBack(dataset)
}

func (d *DirectoryServerCore) revokedDatasets() *list.List {
	d.invalidatedDatasetsLock.RLock()
	defer d.invalidatedDatasetsLock.RUnlock()
	return d.invalidatedDatasets
}

// Adds the given node to the network and returns the DirectoryServerCore's IP address
func (d *DirectoryServerCore) Handshake(ctx context.Context, node *pb.Node) (*pb.HandshakeResponse, error) {
	if _, ok := d.memCache.Nodes()[node.GetName()]; !ok {
		d.memCache.AddNode(node.GetName(), node)
		log.Infof("DirectoryServerCore added '%s' to map with Ifrit IP %s and HTTPS adrress %s\n",
			node.GetName(), node.GetIfritAddress(), node.GetHttpAddress())
	} else {
		return nil, fmt.Errorf("DirectoryServerCore: node '%s' already exists in network\n", node.GetName())
	}
	return &pb.HandshakeResponse{
		Ip: d.ifritClient.Addr(),
		Id: []byte(d.ifritClient.Id()),
	}, nil
}

// Verifies the signature of the given message. Returns a non-nil error if the signature is not valid.
func (d *DirectoryServerCore) verifyMessageSignature(msg *pb.Message) error {
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
		return errors.New("DirectoryServerCore could not securely verify the integrity of the message")
	}

	// Restore message
	msg.Signature = &pb.MsgSignature{
		R: r,
		S: s,
	}

	return nil
}

func (d *DirectoryServerCore) pbNode() *pb.Node {
	return &pb.Node{
		Name:         "Lohpi directory server",
		IfritAddress: d.ifritClient.Addr(),
		Role:         "Directory server",
		Id:           []byte(d.ifritClient.Id()),
	}
}
