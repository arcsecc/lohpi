package policystore

import (
	"context"
	"crypto"
	"crypto/x509"
	"fmt"
	"github.com/arcsecc/lohpi/core/netutil"
	"github.com/arcsecc/lohpi/core/policystore/git"
	"github.com/arcsecc/lohpi/core/policystore/multicast"
	"github.com/arcsecc/lohpi/core/policystore/policysync"
	"github.com/arcsecc/lohpi/core/status"
	pb "github.com/arcsecc/lohpi/protobuf"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/joonnna/ifrit"
	"github.com/lestrrat-go/jwx/jwk"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
	pbtime "google.golang.org/protobuf/types/known/timestamppb"
	"net"
	"net/http"
	"sync"
	"time"
)

type Config struct {
	Name string
	Hostname string
	GossipInterval time.Duration
	HTTPPort int
	GRPCPort int
	MulticastAcceptanceLevel float64
	NumDirectRecipients int
	CaAddress string
	DirectoryServerAddress string
	DirectoryServerGPRCPort int
	GitRepositoryPath string
	SQLConnectionString string

	// Configuration used by Ifrit client
	IfritCryptoUnitWorkingDirectory string
	IfritTCPPort                    int
	IfritUDPPort                    int
}

type storageNodeLookupService interface {
	DatasetNodeExists(ctx context.Context, datasetId string) (bool, error)
	RemoveDatasetLookupEntry(ctx context.Context, datasetId string) error
	InsertDatasetLookupEntry(ctx context.Context, datasetId string, nodeName string) error
	DatasetLookupNode(ctx context.Context, datasetId string) (*pb.Node, error)
	DatasetIdentifiers(ctx context.Context, cursor int, limit int) ([]string, error)
	DatasetIdentifiersAtNode(ctx context.Context, nodeName string) ([]string, error)
	DatasetExistsAtNode(ctx context.Context, datasetId string, nodeName string) (bool, error)
}

type datasetManager interface {
	Dataset(ctx context.Context, datasetId string) (*pb.Dataset, error)
	Datasets(ctx context.Context) (map[string]*pb.Dataset, error)
	DatasetIdentifiers(ctx context.Context, fromIdx int64, toIdx int64) ([]string, error)
	DatasetExists(ctx context.Context, datasetId string) (bool, error)
	RemoveDataset(ctx context.Context, datasetId string) error
	SetDatasetPolicy(ctx context.Context, datasetId string, policy *pb.Policy) error
	GetDatasetPolicy(ctx context.Context, datasetId string) (*pb.Policy, error)
	InsertDataset(ctx context.Context, datasetId string, dataset *pb.Dataset) error
	DatasetsAtNode(ctx context.Context, node *pb.Node) ([]*pb.Dataset, error)
}

type membershipManager interface {
	NetworkNode(ctx context.Context, nodeId string) (*pb.Node, error)
	AddNetworkNode(ctx context.Context, nodeid string, network *pb.Node) error
	NetworkNodeExists(ctx context.Context, id string) (bool, error)
	RemoveNetworkNode(ctx context.Context, id string) error
	NetworkNodes(ctx context.Context, cursor int, limit int) (*pb.Nodes, error)
}

type certManager interface {
	Certificate() *x509.Certificate
	CaCertificate() *x509.Certificate
	PrivateKey() crypto.PrivateKey
	PublicKey() crypto.PublicKey
}

type policyLogger interface {
	InsertObservedGossip(ctx context.Context, g *pb.PolicyGossipUpdate) error
	InsertAppliedPolicy(ctx context.Context, p *pb.Policy) error
}

var logFields = log.Fields{
	"package": "core/policy",
	"description": "policy engine",
}

type PolicyStoreCore struct {
	// Policy store's configuration
	config     *Config
	configLock sync.RWMutex

	// The underlying Fireflies client
	ifritClient *ifrit.Client

	// MUX's Ifrit ip
	directoryServerIfritIP string

	// Go-git
	gitRepo *git.GitRepository

	// Sync
	exitChan    chan bool
	networkLock sync.Mutex

	// gRPC service
	grpcs *gRPCServer
	pb.UnsafePolicyStoreServer

	// Multicast specifics
	multicastManager *multicast.MulticastManager
	stopBatching     chan bool

	// HTTP server stuff
	httpListener net.Listener
	httpServer   *http.Server

	// Fetch the JWK
	ar *jwk.AutoRefresh

	// Policy syncer
	policySyncUnit *policysync.PolicySyncUnit

	cm              certManager
	dsManager       datasetManager
	memManager      membershipManager
	dsLookupService storageNodeLookupService
	policyLog       policyLogger
}

func NewPolicyStoreCore(cm certManager, dsLookupService storageNodeLookupService, memManager membershipManager, dsManager datasetManager, config *Config, policyLog policyLogger, pool *pgxpool.Pool, new bool) (*PolicyStoreCore, error) {
	ifritClient, err := ifrit.NewClient(&ifrit.Config{
		New: new,
		TCPPort: config.IfritTCPPort,
		UDPPort: config.IfritUDPPort,
		Hostname: config.Hostname,
		CryptoUnitPath: config.IfritCryptoUnitWorkingDirectory})
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		return nil, err
	}

	var gitRepo *git.GitRepository
	if config.GitRepositoryPath != "" {
		gitRepo, err = git.NewGitRepository(config.GitRepositoryPath)
		if err != nil {
			log.WithFields(logFields).Error(err.Error())
			return nil, err
		}
	}

	listener, err := netutil.ListenOnPort(config.GRPCPort)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		return nil, err
	}

	s, err := newPolicyStoreGRPCServer(cm.Certificate(), cm.CaCertificate(), cm.PrivateKey(), listener)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		return nil, err
	}

	multicastManager, err := multicast.NewMulticastManager(ifritClient, config.MulticastAcceptanceLevel, config.NumDirectRecipients, config.SQLConnectionString, config.Name, pool)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		return nil, err
	}

	policySyncUnit, err := policysync.NewPolicySyncUnit(config.Name, pool, dsManager)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		return nil, err
	}

	// TODO: remove some of the variables into other interfaces/packages?
	ps := &PolicyStoreCore{
		ifritClient: ifritClient,
		gitRepo: gitRepo,
		exitChan: make(chan bool, 1),
		cm: cm,
		grpcs: s,
		config: config,
		multicastManager: multicastManager,
		stopBatching: make(chan bool),
		dsLookupService: dsLookupService,
		memManager: memManager,
		dsManager: dsManager,
		policyLog: policyLog,
		policySyncUnit: policySyncUnit,
	}

	ps.grpcs.Register(ps)

	// Set Ifrit callbacks
	ps.ifritClient.RegisterGossipHandler(ps.gossipHandler)

	return ps, nil
}

func (ps *PolicyStoreCore) Start() error {
	log.WithFields(logFields).Infof("Policy store running gRPC server at %s and Ifrit client at %s", ps.grpcs.Addr(), ps.ifritClient.Addr())
	go ps.grpcs.Start()
	go ps.startHttpServer(fmt.Sprintf(":%d", ps.PolicyStoreConfig().HTTPPort))
	go ps.ifritClient.Start()
	return nil
}

// Runs the policy batcher. The function blocks until it stops multicasting policy batches.
func (ps *PolicyStoreCore) RunPolicyBatcher() {
	mcTimer := time.NewTimer(ps.PolicyStoreConfig().GossipInterval)
	ctx := context.Background()

	// Main event loop for multicasting messages
	for {
		select {
		// When a probing session starts, it blocks the multicast timer from being reset.
		// This allows us to run policy updates and probing in an orderly fashion
		case <-mcTimer.C:
			func() {
				defer mcTimer.Reset(ps.PolicyStoreConfig().GossipInterval)
				if ps.multicastManager.IsProbing() {
					log.WithFields(logFields).Info("Manager is probing the network")
					return
				}

				// Redundant..?
				if ps.multicastManager.IsMulticasting() {
					log.WithFields(logFields).Info("Manager is already multicasting a policy batch")
					return
				}

				// NOTE: set sane values of cursor and limit
				members, err := ps.memManager.NetworkNodes(context.Background(), 0, 100)
				if err != nil {
					log.WithFields(logFields).Error(err.Error())
					return
				}

				if err := ps.multicastManager.Multicast(ctx, &multicast.Config{
					Mode: multicast.GeopgrahicallyDistributedMembers,
					Members: members}); err != nil {
					log.WithFields(logFields).Error(err.Error())
					return
				}

				log.WithFields(logFields).Info("Multicast manager completed a burst of message multicasting")
			}()
		case <-ps.stopBatching:
			// TODO: garbage collect timer
			return
		}
	}
}

// Stops the policy batcher.
func (ps *PolicyStoreCore) StopPolicyBatcher() {
	log.WithFields(logFields).Infoln("Stopping policy batcher")
	ps.stopBatching <- true
}

func (ps *PolicyStoreCore) IfritAddress() string {
	return ps.ifritClient.Addr()
}

// Invoked by ifrit message handler
func (ps *PolicyStoreCore) verifyMessageSignature(msg *pb.Message) error {
	return nil
	r := msg.GetSignature().GetR()
	s := msg.GetSignature().GetS()

	msg.Signature = nil

	// Marshal it before verifying its integrity
	data, err := proto.Marshal(msg)
	if err != nil {
		log.WithFields(logFields).Print(err.Error())
		return err
	}
	_ = data

	/*if !ps.ifritClient.VerifySignature(r, s, data, string(m.ifritClient.Id())) {
		return errors.New("Policy store could not securely verify the gossip of the message")
	}*/

	// Restore message
	msg.Signature = &pb.MsgSignature{
		R: r,
		S: s,
	}

	return nil
}

// Initializes a probing procedures that awaits for n acknowledgements
// using the current network settings
// TODO: fix queuing
func (ps *PolicyStoreCore) probeHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if ps.multicastManager.IsProbing() {
		fmt.Fprintf(w, "Probing session is already running")
		return
	} else {
		fmt.Fprintf(w, "Starting probing session")
	}

	if err := ps.multicastManager.ProbeNetwork(multicast.LruMembers); err != nil {
		fmt.Fprintf(w, err.Error())
	}
}

func (ps *PolicyStoreCore) stopProbe(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if !ps.multicastManager.IsProbing() {
		fmt.Fprintf(w, "Probing session is not running")
		return
	} else {
		fmt.Fprintf(w, "Stopping probing session")
	}

	ps.multicastManager.StopProbing()
}

func (ps *PolicyStoreCore) Stop() {
	// TODO: shutdown the servers and so on...
	ps.ifritClient.Stop()
}

// Handshake endpoint for nodes to join the network
func (ps *PolicyStoreCore) Handshake(ctx context.Context, node *pb.Node) (*pb.HandshakeResponse, error) {
	if node == nil {
		return nil, status.Errorf(status.Nil, "Node is nil")
	}

	if err := ps.memManager.AddNetworkNode(context.Background(), node.GetName(), node); err != nil {
		log.WithFields(logFields).Error(err.Error())
		return nil, err
	}

	log.WithFields(logFields).Infof("Policy store added node '%s' to map with Ifrit IP '%s'\n", node.GetName(), node.GetIfritAddress())
	ipString := fmt.Sprintf("%s:%d", ps.PolicyStoreConfig().Hostname, ps.PolicyStoreConfig().IfritTCPPort)

	return &pb.HandshakeResponse{
		Ip: ipString,
		Id: []byte(ps.ifritClient.Id()),
	}, nil
}

func (ps *PolicyStoreCore) IndexDataset(ctx context.Context, req *pb.DatasetIndexRequest) (*pb.Policy, error) {
	if req == nil {
		return nil, status.Errorf(status.Nil, "dataset index request is nil")
	}

	var policy *pb.Policy

	if origin := req.GetOrigin(); origin != nil {
		exists, err := ps.dsManager.DatasetExists(context.Background(), req.GetIdentifier())
		if err != nil {
			log.WithFields(logFields).Error(err.Error())
			return nil, err
		}

		if !exists {
			if indexOption := req.GetIndexOption(); indexOption != nil {
				policy = ps.getInitialDatasetPolicy(req.GetIdentifier())
				newDataset := &pb.Dataset{
					Identifier: req.GetIdentifier(),
					Policy: policy,
					IndexOption: indexOption,
				}

				if err := ps.dsManager.InsertDataset(ctx, req.GetIdentifier(), newDataset); err != nil {
					log.WithFields(logFields).Error(err.Error())
					return nil, status.Errorf(status.IndexFailed, "Indexing dataset with identifier '%s' failed", req.GetIdentifier())
				}
			} else {
				return nil, status.Errorf(status.Nil, "Indexing options was nil for dataset with identifier '%s'", req.GetIdentifier())
			}
		}

		// Add the dataset to the lookup manager.
		if err := ps.dsLookupService.InsertDatasetLookupEntry(context.Background(), req.GetIdentifier(), origin.GetName()); err != nil {
			log.WithFields(logFields).Error(err.Error())
			return nil, status.Errorf(status.IndexFailed, "Indexing dataset with identifier '%s' failed", req.GetIdentifier())
		}
	} else {
		return nil, status.Errorf(status.Nil, "Origin node is nil")
	}

	dataset, err := ps.dsManager.Dataset(ctx, req.GetIdentifier())
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
	}

	if dataset != nil {
		if p := dataset.GetPolicy(); p != nil {
			policy = p
		} else {
			return nil, status.Errorf(status.Nil, "Dataset policy is for dataset %s nil", req.GetIdentifier())
		}
	} else {
		return nil, status.Errorf(status.Nil, "Dataset with identifier '%s' nil", req.GetIdentifier())
	}

	return policy, nil
}

func (ps *PolicyStoreCore) ReconcilePolicies(ctx context.Context, req *pb.PolicyReconcileRequest) (*pb.PolicyReconcileResponse, error) {
	return ps.policySyncUnit.ReconcilePolicies(ctx, req)
}

// Set the initial policy of a dataset
func (ps *PolicyStoreCore) getInitialDatasetPolicy(datasetId string) *pb.Policy {
	return &pb.Policy{
		//Issuer: ps.PolicyStoreConfig().Name,
		DatasetIdentifier: datasetId,
		Content: false,
		DateCreated: pbtime.Now(),
		DateUpdated: pbtime.Now(),
	}
}

// Ifrit gossip handler
func (ps *PolicyStoreCore) gossipHandler(data []byte) ([]byte, error) {
	message := &pb.Message{}
	if err := proto.Unmarshal(data, message); err != nil {
		log.WithFields(logFields).Error(err.Error())
		return nil, err
	}

	log.WithFields(logFields).Infof("Node '%s' got gossip message", ps.config.Name)

	if err := ps.verifyMessageSignature(message); err != nil {
		log.WithFields(logFields).Error(err.Error())
		return nil, err
	}

	// Determine actions based on message type
	if content := message.GetContent(); content != nil {
		m, err := content.UnmarshalNew()
		if err != nil {
			log.WithFields(logFields).Error(err.Error())
			return nil, err
		}

		switch msg := m.(type) {
		case *pb.PolicyGossipUpdate:
			if err := ps.policyLog.InsertObservedGossip(context.Background(), msg); err != nil {
				log.WithFields(logFields).Error(err.Error())
			}
		default:
			log.WithFields(logFields).Error("Unknown message type:", msg)
		}
	} else {
		log.WithFields(logFields).Error("content in message is nil")
	}

	return nil, nil
}

func (ps *PolicyStoreCore) submitPolicyForDistribution(p *pb.Policy) {
	ps.multicastManager.InsertPolicy(p)
}

func (ps *PolicyStoreCore) PolicyStoreConfig() *Config {
	ps.configLock.RLock()
	defer ps.configLock.RUnlock()
	return ps.config
}

// Shut down all resources assoicated with the policy store
func (ps *PolicyStoreCore) Shutdown() {
	log.WithFields(logFields).Print("Shutting down policy store")
	// TODO: shut down servers too
	ps.ifritClient.Stop()
}

func (ps *PolicyStoreCore) pbNode() *pb.Node {
	return &pb.Node{
		//Issuer:       ps.PolicyStoreConfig().Name,
		IfritAddress: fmt.Sprintf("%s:%d", ps.PolicyStoreConfig().Hostname, ps.PolicyStoreConfig().IfritTCPPort),
		Id:           []byte(ps.ifritClient.Id()),
	}
}
