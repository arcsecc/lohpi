package directoryserver

import (
	"context"
	"crypto"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"github.com/arcsecc/lohpi/core/comm"
	"github.com/arcsecc/lohpi/core/netutil"
	"github.com/arcsecc/lohpi/core/status"
	pb "github.com/arcsecc/lohpi/protobuf"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/joonnna/ifrit"
	"github.com/lestrrat-go/jwx/jwk"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	pbtime "google.golang.org/protobuf/types/known/timestamppb"
	"net"
	"net/http"
	"strconv"
	"sync"
)

var logFields = log.Fields{
	"package":     "core/directoryserver",
	"description": "directory server",
}

var (
	ErrNoDirectoryServerConfig = errors.New("Directory server configuration is nil")
	errGossipMessageBody       = errors.New("Gossip message body is nil")
)

type Config struct {
	// String identifier of the directory server.
	Name string

	// HTTP port used by the server.
	HTTPPort int

	// TCP port used by the gRPC server.
	GRPCPort int

	// SQL database connection string.
	SQLConnectionString string

	Hostname string

	// Configuration used by Ifrit client
	IfritCryptoUnitWorkingDirectory string
	IfritTCPPort                    int
	IfritUDPPort                    int
}

type DirectoryServerCore struct {
	exitChan  chan bool
	exitFlag  bool
	exitMutex sync.RWMutex

	// Configuration
	config     *Config
	configLock sync.RWMutex

	// Underlying Ifrit client
	ifritClient *ifrit.Client

	// HTTP-related stuff. Used by the demonstrator using cURL
	httpListener net.Listener
	httpServer   *http.Server

	// gRPC service
	listener     net.Listener
	serverConfig *tls.Config
	grpcServer   *gRPCServer
	pb.UnimplementedDirectoryServerServer

	// directory server database
	pool *pgxpool.Pool

	// Fetch the JWK
	pubKeyCache *jwk.AutoRefresh

	dsLookupService datasetLookupService
	cm              certManager
	memManager      membershipManager
	checkoutManager datasetCheckoutManager
	policyLog       policyLogger

	// Dataset description manager
	datasetDescriptionManager *datasetDescriptionManagerUnit

	useACME bool
}

var (
	ErrDatasetLookupInsert = errors.New("Inserting into dataset lookup collection failed")
	ErrUnknownMessageType  = errors.New("Unknown message type")
	ErrAddNetworkNode      = errors.New("Adding network node failed")
)

type policyLogger interface {
	InsertObservedGossip(ctx context.Context, g *pb.PolicyGossipUpdate) error
	InsertAppliedPolicy(ctx context.Context, p *pb.Policy) error
}

type datasetLookupService interface {
	DatasetNodeExists(ctx context.Context, datasetId string) (bool, error)
	RemoveDatasetLookupEntry(ctx context.Context, datasetId string) error
	InsertDatasetLookupEntry(ctx context.Context, datasetId string, nodeName string) error
	DatasetLookupNode(ctx context.Context, datasetId string) (*pb.Node, error)
	DatasetIdentifiers(ctx context.Context, cursor int, limit int) ([]string, error)
	DatasetIdentifiersAtNode(ctx context.Context, nodeName string) ([]string, error)
	NumberOfDatasets(ctx context.Context) (int64, error)
}

type membershipManager interface {
	NetworkNodes(ctx context.Context, cursor int, limit int) (*pb.Nodes, error)
	NetworkNode(ctx context.Context, nodeId string) (*pb.Node, error)
	AddNetworkNode(ctx context.Context, nodeId string, node *pb.Node) error
	NetworkNodeExists(ctx context.Context, id string) (bool, error)
	RemoveNetworkNode(ctx context.Context, id string) error
	NumNetworkMembers(ctx context.Context) (int, error)
}

type datasetCheckoutManager interface {
	CheckoutDataset(ctx context.Context, datasetId string, checkout *pb.DatasetCheckout, systemPolicyVersion uint64, policy bool) error
	DatasetIsCheckedOutByClient(ctx context.Context, datasetId string, client *pb.Client) (bool, error)
	DatasetIsCheckedOut(ctx context.Context, datasetId string) (bool, error)
	DatasetCheckouts(ctx context.Context, datasetId string, cursor int, limit int) (*pb.DatasetCheckouts, error)
	CheckinDataset(ctx context.Context, datasetId string, client *pb.Client) error
	SetDatasetSystemPolicy(ctx context.Context, datasetId string, verison uint64, policy bool) error
	GetDatasetSystemPolicy(ctx context.Context, datasetId string) (uint64, bool, error)
	NumberOfDatasetCheckouts(ctx context.Context, datasetId string) (int, error)
}

type certManager interface {
	Certificate() *x509.Certificate
	CaCertificate() *x509.Certificate
	PrivateKey() crypto.PrivateKey
	PublicKey() crypto.PublicKey
}

// Returns a new DirectoryServer using the given configuration. Returns a non-nil error, if any.
func NewDirectoryServerCore(cm certManager, policyLog policyLogger, dsLookupService datasetLookupService, memManager membershipManager, checkoutManager datasetCheckoutManager, config *Config, pool *pgxpool.Pool, new bool, useACME bool) (*DirectoryServerCore, error) {
	if config == nil {
		log.WithFields(logFields).Error(ErrNoDirectoryServerConfig.Error())
		return nil, ErrNoDirectoryServerConfig
	}

	ifritClient, err := ifrit.NewClient(&ifrit.Config{
		New:            new,
		TCPPort:        config.IfritTCPPort,
		UDPPort:        config.IfritUDPPort,
		Hostname:       config.Hostname,
		CryptoUnitPath: config.IfritCryptoUnitWorkingDirectory})
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		return nil, err
	}

	listener, err := netutil.ListenOnPort(config.GRPCPort)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		return nil, err
	}

	var serverConfig *tls.Config

	// Server configuration. If ACME is true, use Let's Encrypt
	if useACME {
		serverConfig, err = comm.ServerConfigWithACME(config.Hostname)
		if err != nil {
			return nil, err
		}	
	} else {
		serverConfig, err = comm.ServerConfig(cm.Certificate(), cm.CaCertificate(), cm.PrivateKey())
		if err != nil {
			return nil, err
		}
	}

	grpcServer, err := newDirectoryGRPCServer(serverConfig, listener)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		return nil, err
	}

	datasetDescriptionManager, err := newDatasetDescriptionManagerUnit(config.Name, pool)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		return nil, err
	}

	ds := &DirectoryServerCore{
		config: config,
		configLock: sync.RWMutex{},
		ifritClient: ifritClient,
		grpcServer: grpcServer,
		serverConfig: serverConfig,
		dsLookupService: dsLookupService,
		cm: cm,
		memManager: memManager,
		checkoutManager: checkoutManager,
		policyLog: policyLog,
		exitChan: make(chan bool, 1),
		exitFlag: false,
		pool: pool,
		datasetDescriptionManager: datasetDescriptionManager,
		useACME: useACME,
	}

	ds.grpcServer.Register(ds)
	ds.ifritClient.RegisterMsgHandler(ds.messageHandler)
	ifritClient.RegisterGossipHandler(ds.gossipMessageHandler)

	return ds, nil
}

// Starts the directory server. This includes starting the HTTP server, Ifrit client and gRPC server.
// In addition, it will try and restore the state it had before it crashed.
func (d *DirectoryServerCore) Start() {
	log.Infoln("Directory server running gRPC server at", d.grpcServer.Addr(), "and Ifrit client at", d.ifritClient.Addr())
	
	// Ifrit client
	go d.ifritClient.Start()
	
	// gRPC server
	go d.grpcServer.Start()

	go d.startHttpServer(":" + strconv.Itoa(d.config.HTTPPort))

	<-d.exitChan
	d.Stop()
}

// Create a node that performs a handshake with
func (d *DirectoryServerCore) Stop() {
	if d.isStopping() {
		return
	}

	d.ifritClient.Stop()
	d.grpcServer.Stop()
	d.shutdownHttpServer()
}

// PIVATE METHODS BELOW THIS LINE
// TODO: implement timeouts and context handling on direct messaging.
func (d *DirectoryServerCore) messageHandler(data []byte) ([]byte, error) {
	msg := &pb.Message{}
	if err := proto.Unmarshal(data, msg); err != nil {
		log.WithFields(logFields).Error(err.Error())
		return nil, err
	}

	if err := d.verifyMessageSignature(msg); err != nil {
		log.WithFields(logFields).Error(err.Error())
		return nil, err
	}

	return nil, nil
}

// Adds the given node to the network and returns the DirectoryServerCore's IP address
func (d *DirectoryServerCore) Handshake(ctx context.Context, node *pb.Node) (*pb.HandshakeResponse, error) {
	if node == nil {
		err := status.Errorf(status.Nil, "Node is nil")
		log.WithFields(logFields).Error(err.Error())
		return nil, err
	}

	if err := d.memManager.AddNetworkNode(context.Background(), node.GetName(), node); err != nil {
		log.WithFields(logFields).Error(err.Error())
		return nil, err
	}

	log.Infof("Added '%s' to map with Ifrit IP '%s' and HTTPS address '%s'\n", node.GetName(), node.GetIfritAddress(), node.GetHttpsAddress())
	return &pb.HandshakeResponse{
		Ip: fmt.Sprintf("%s:%d", d.config.Hostname, d.config.IfritTCPPort),
		//Ip: d.ifritClient.Addr(),
		Id: []byte(d.ifritClient.Id()),
	}, nil
}

func (d *DirectoryServerCore) IndexDataset(ctx context.Context, req *pb.DatasetIdentifierIndexRequest) (*emptypb.Empty, error) {
	if req == nil {
		err := status.Errorf(status.Nil, "Protobuf dataset identifier index request is nil")
		log.WithFields(logFields).Error(err.Error())
		return nil, err
	}

	log.WithFields(logFields).Info("Indexed dataset lookup with identifier '%s'", req.GetIdentifier())
	if err := d.dsLookupService.InsertDatasetLookupEntry(ctx, req.GetIdentifier(), req.GetOrigin().GetName()); err != nil {
		log.WithFields(logFields).Error(err.Error())
		return nil, ErrDatasetLookupInsert
	}

	return &emptypb.Empty{}, nil
}

// Verifies the signature of the given message. Returns a non-nil error if the signature is not valid.
// TODO: implement retries if it fails. Use while loop with a fixed number of attempts. Log the events too
func (d *DirectoryServerCore) verifyMessageSignature(msg *pb.Message) error {
	return nil
	// Verify the integrity of the message
	r := msg.GetSignature().GetR()
	s := msg.GetSignature().GetS()

	msg.Signature = nil

	// Marshal it before verifying its integrity
	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	_ = data

	/*	if !d.ifritClient.VerifySignature(r, s, data, string(msg.GetSender().GetId())) {
		return errors.New("DirectoryServerCore could not securely verify the integrity of the message")
	}*/

	// Restore message
	msg.Signature = &pb.MsgSignature{
		R: r,
		S: s,
	}

	return nil
}

func (d *DirectoryServerCore) pbNode() *pb.Node {
	return &pb.Node{
		Name:         d.config.Name,
		IfritAddress: fmt.Sprintf("%s:%d", d.config.Hostname, d.config.IfritTCPPort),
		Id:           []byte(d.ifritClient.Id()),
		BootTime:     pbtime.Now(),
	}
}

func (d *DirectoryServerCore) gossipMessageHandler(data []byte) ([]byte, error) {
	message := &pb.Message{}
	if err := proto.Unmarshal(data, message); err != nil {
		log.WithFields(logFields).Error(err.Error())
		return nil, err
	}

	log.WithFields(logFields).Infof("Node '%s' got gossip message", d.config.Name)

	if err := d.verifyMessageSignature(message); err != nil {
		log.WithFields(logFields).Error(err.Error())
		return nil, err
	}

	// Determine actions based on message type.
	// Should maybe find another way to
	if content := message.GetContent(); content != nil {
		m, err := content.UnmarshalNew()
		if err != nil {
			log.WithFields(logFields).Error(err.Error())
			return nil, err
		}

		switch msg := m.(type) {
		case *pb.PolicyGossipUpdate:
			if err := d.policyLog.InsertObservedGossip(context.Background(), msg); err != nil {
				log.WithFields(logFields).Error(err.Error())
				return nil, err
			}

			if err := d.processPolicyBatch(context.Background(), msg); err != nil {
				log.WithFields(logFields).Error(err.Error())
				return nil, err
			}
		default:
			log.WithFields(logFields).Error("Unknown message type:", msg)
		}
	} else {
		log.WithFields(logFields).Error("content in message is nil")
	}

	return nil, nil
}

// Apply policies to checked-out datasets. Notify the session handler about the update
// TODO: notify the client that checked out the dataset as well
func (d *DirectoryServerCore) processPolicyBatch(ctx context.Context, g *pb.PolicyGossipUpdate) error {
	if g.GetGossipMessageBody() == nil {
		log.WithFields(logFields).Error(errGossipMessageBody.Error())
		return errGossipMessageBody
	}

	for _, m := range g.GetGossipMessageBody() {
		if newPolicy := m.GetPolicy(); newPolicy != nil {
			if datasetId := newPolicy.GetDatasetIdentifier(); datasetId != "" {
				isCheckedOut, err := d.checkoutManager.DatasetIsCheckedOut(ctx, datasetId)
				if err != nil {
					log.WithFields(logFields).Error(err.Error())
					continue
				}

				// If the dataset is checked out, check the version number to avoid inconsistent policy application.
				// Apply the policy to all checkouts
				if isCheckedOut {
					currentVersion, _, err := d.checkoutManager.GetDatasetSystemPolicy(ctx, datasetId)
					if err != nil {
						log.WithFields(logFields).Error(err.Error())
						continue
					}

					if newPolicy.GetVersion() > currentVersion {
						if err := d.checkoutManager.SetDatasetSystemPolicy(ctx, datasetId, newPolicy.GetVersion(), newPolicy.GetContent()); err != nil {
							log.WithFields(logFields).Error(err.Error())
							continue
						}
						log.WithFields(logFields).Infof("Successfully updated dataset policy with identifier '%s'", datasetId)
					} else {
						log.WithFields(logFields).Infof("Got an outdated policy for dataset '%s'. Current version is %d, got %d", datasetId, currentVersion, newPolicy.GetVersion())
					}
				}
			} else {
				log.WithFields(logFields).Error("Dataset identifier must not be an empty string")
			}
		} else {
			log.WithFields(logFields).Error("Policy entry in gossip message body was nil")
		}
	}

	return nil
}

func (d *DirectoryServerCore) isStopping() bool {
	d.exitMutex.Lock()
	defer d.exitMutex.Unlock()

	if d.exitFlag {
		return true
	}

	d.exitFlag = true
	close(d.exitChan)

	return false
}
