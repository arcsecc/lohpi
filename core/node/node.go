package node

import (
	"context"
	"crypto"
	"crypto/x509"
	"fmt"
	"github.com/arcsecc/lohpi/core/comm"
	"github.com/arcsecc/lohpi/core/node/policysync"
	pb "github.com/arcsecc/lohpi/protobuf"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/joonnna/ifrit"
	"github.com/lestrrat-go/jwx/jwk"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
	pbtime "google.golang.org/protobuf/types/known/timestamppb"
	"net/http"
	"sync"
	"time"
)

var (
	errNoAddr               = errors.New("No certificate authority address provided, can't continue")
	ErrNoDatasetIndexOption = errors.New("No dataset index option provided")
)

type Client struct {
	Name string `json:"name"`
	Oid  string `json:"oid"`
}

type Config struct {
	// The name of this node
	Name string

	// Port used by the HTTP server
	Port int

	// Hostname used by the HTTP server
	Hostname string

	// Ifrit's TCP port
	IfritTCPPort int

	// Ifrit's UDP port
	IfritUDPPort int

	// Ifrit's cryptographic resources
	IfritCryptoUnitWorkingDirectory string

	// Geographic zone
	GeoZone string

	// Interval between issuing set reconciliation.
	SetReconciliationInterval time.Duration
}

func init() {
	log.SetReportCaller(true)
}

var logFields = log.Fields{
	"package":     "core/node",
	"description": "dataset node",
}

type gossipMessage struct {
	Content [][]byte
	Hash    []byte
	Addr    string
}

var (
	ErrNoConfig                          = errors.New("Node configuration is nil")
	ErrFailedIndexing                    = errors.New("Failed to index dataset")
	ErrNoDirectoryServerHandshakeAddress = errors.New("No directory server handshake address")
	ErrDirectoryServerHandshakeFailed    = errors.New("Directory server handshake failed")
	ErrNoPolicyStoreHandshakeAddress     = errors.New("No policy store handshake address")
	ErrPolicyStoreHandshakeFailed        = errors.New("Policy store handshake failed")
	errGossipMessage                     = errors.New("Gossip message is nil")
	errGossipMessageBody                 = errors.New("Gossip message body is nil")
)

type DatasetIndexingOptions struct {
	AllowMultipleCheckouts bool
}

// Types used in client requests
type clientRequestHandler func(id string, w http.ResponseWriter, r *http.Request)

type NodeCore struct {
	// Underlying ifrit client
	ifritClient *ifrit.Client

	exitChan  chan bool
	exitFlag  bool
	exitMutex sync.RWMutex

	// Configuration
	conf       *Config
	configLock sync.RWMutex

	// gRPC client towards the directory server
	directoryServerClient *comm.DirectoryGRPCClient

	// Policy store
	psClient *comm.PolicyStoreGRPCClient

	// Used for identifying data coming from policy store
	directoryServerID      []byte
	directoryServerIDMutex sync.RWMutex

	policyStoreID      []byte // TODO: allow for any number of policy store instances :)
	policyStoreIDMutex sync.RWMutex

	// Ifrit address
	directoryServerIfritIP      string
	directoryServerIfritIPMutex sync.RWMutex

	policyStoreIfritIP      string
	policyStoreIfritIPMutex sync.RWMutex

	// gRPC address
	directoryServerGRPCIP      string
	directoryServerGRPCIPMutex sync.RWMutex

	policyStoreGRPCIP      string
	policyStoreGRPCIPMutex sync.RWMutex

	// Object id -> empty value for quick indexing
	datasetMap     map[string]*pb.Policy
	datasetMapLock sync.RWMutex

	httpServer *http.Server

	datasetHandlerLock  sync.RWMutex
	metadataHandlerLock sync.RWMutex

	datasetHandlerFunc  clientRequestHandler
	metadataHandlerFunc clientRequestHandler

	dsManager       datasetManager
	policyLog       policyLogger
	checkoutManager datasetCheckoutManager

	pbnode      *pb.Node
	boottime    *pbtime.Timestamp
	pubKeyCache *jwk.AutoRefresh

	policySyncUnit *policysync.PolicySyncUnit
}

type certManager interface {
	Certificate() *x509.Certificate
	CaCertificate() *x509.Certificate
	PrivateKey() crypto.PrivateKey
	PublicKey() crypto.PublicKey
}

type policyLogger interface {
	InsertObservedGossip(ctx context.Context, g *pb.PolicyGossipUpdate) error
	GossipIsObserved(ctx context.Context, g *pb.PolicyGossipUpdate) (bool, error)
	InsertAppliedPolicy(ctx context.Context, p *pb.Policy) error
}

type datasetManager interface {
	Dataset(ctx context.Context, datasetId string) (*pb.Dataset, error)
	Datasets(ctx context.Context) (map[string]*pb.Dataset, error) // index and cursor here!
	DatasetIdentifiers(ctx context.Context, idx int64, length int64) ([]string, error)
	DatasetExists(ctx context.Context, datasetId string) (bool, error)
	RemoveDataset(ctx context.Context, datasetId string) error
	SetDatasetPolicy(ctx context.Context, datasetId string, policy *pb.Policy) error
	GetDatasetPolicy(ctx context.Context, datasetId string) (*pb.Policy, error)
	InsertDataset(ctx context.Context, datasetId string, dataset *pb.Dataset) error
}

type datasetCheckoutManager interface {
	CheckoutDataset(ctx context.Context, datasetId string, checkout *pb.DatasetCheckout, systemPolicyVersion uint64, policy bool) error
	DatasetIsCheckedOutByClient(ctx context.Context, datasetId string, client *pb.Client) (bool, error)
	DatasetIsCheckedOut(ctx context.Context, datasetId string) (bool, error)
	CheckinDataset(ctx context.Context, datasetId string, client *pb.Client) error
	DatasetCheckouts(ctx context.Context, datasetId string, cursor int, limit int) (*pb.DatasetCheckouts, error)
}

func NewNodeCore(cm certManager, policyLog policyLogger, dsManager datasetManager, checkoutManager datasetCheckoutManager, config *Config, pool *pgxpool.Pool, new bool) (*NodeCore, error) {
	if config == nil {
		return nil, ErrNoConfig
	}

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

	directoryServerClient, err := comm.NewDirectoryServerGRPCClient(cm.Certificate(), cm.CaCertificate(), cm.PrivateKey())
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		return nil, err
	}

	psClient, err := comm.NewPolicyStoreClient(cm.Certificate(), cm.CaCertificate(), cm.PrivateKey())
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		return nil, err
	}

	pbnode := &pb.Node{
		Name: config.Name,
		IfritAddress: fmt.Sprintf("%s:%d", config.Hostname, config.IfritTCPPort),
		//I ritAddress: ifritClient.Addr(),
		Id: []byte(ifritClient.Id()),
		HttpsAddress: config.Hostname,
		Port: int32(config.Port),
		BootTime: pbtime.Now(),
		GeoZone: config.GeoZone,
	}

	// Set reconciliation manager
	policySyncUnit, err := policysync.NewPolicySyncUnit(pbnode, config.SetReconciliationInterval, psClient, dsManager, policyLog)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		return nil, err
	}

	node := &NodeCore{
		ifritClient: ifritClient,
		directoryServerClient: directoryServerClient,
		conf: config,
		psClient: psClient,
		datasetMap: make(map[string]*pb.Policy),
		policyLog: policyLog,
		dsManager: dsManager,
		checkoutManager: checkoutManager,
		policySyncUnit: policySyncUnit,
		exitChan: make(chan bool, 1),
		exitFlag: false,
		pbnode: pbnode,
	}

	// Set the Ifrit handlers
	node.ifritClient.RegisterMsgHandler(node.messageHandler)
	node.ifritClient.RegisterGossipHandler(node.gossipHandler)

	return node, nil
}

func (n *NodeCore) IfritClient() *ifrit.Client {
	return n.ifritClient
}

func (n *NodeCore) Start() {
	log.WithFields(logFields).Infoln("Starting Lohpi node services...")
	go n.ifritClient.Start()
	go n.startHTTPServer()
	go n.startPolicySyncUnit()
	<-n.exitChan

	log.WithFields(logFields).Infoln("Stopping Lohpi node services...")
	n.Stop()
}

func (n *NodeCore) startPolicySyncUnit() {
	//addr := n.getPolicyStoreGRPCIP()
	//n.policySyncUnit.Start(addr)
}

func (n *NodeCore) Stop() {
	if n.isStopping() {
		return
	}

	if err := n.stopHTTPServer(); err != nil {
		log.WithFields(logFields).Error(err.Error())
	}

	n.ifritClient.Stop()
	n.policySyncUnit.Stop()
}

func (n *NodeCore) isStopping() bool {
	n.exitMutex.Lock()
	defer n.exitMutex.Unlock()

	if n.exitFlag {
		return true
	}

	n.exitFlag = true
	close(n.exitChan)

	return false
}

// Indexes the dataset using the id as key. A policy from the policy store will be assigned to the dataset.
// It will also populate the node's database with the available identifiers and assoicate them with policies.
// You will have to call this method to make the datasets available to the clients.
func (n *NodeCore) IndexDataset(ctx context.Context, datasetId string, indexOptions *DatasetIndexingOptions) error {
	if indexOptions == nil {
		log.WithFields(logFields).Error(ErrNoDatasetIndexOption.Error())
		return ErrNoDatasetIndexOption
	}

	exists, err := n.dsManager.DatasetExists(context.Background(), datasetId)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		return err
	}

	if exists {
		log.WithFields(logFields).Infof("Dataset with identifier '%s' is already indexed", datasetId)
	}

	if err := n.requestIndexDataset(ctx, datasetId, indexOptions); err != nil {
		log.WithFields(logFields).Error(err.Error())
		return ErrFailedIndexing
	}

	if err := n.requestDatasetIdentifierIndex(ctx, datasetId); err != nil {
		log.WithFields(logFields).Error(err.Error())
		return ErrFailedIndexing
	}

	return nil
}

func (n *NodeCore) requestIndexDataset(ctx context.Context, datasetId string, indexOptions *DatasetIndexingOptions) error {
	addr := n.getPolicyStoreGRPCIP()
	if addr == "" {
		err := fmt.Errorf("Policy store address is not set")
		log.WithFields(logFields).Error(err.Error())
		return err
	}

	conn, err := n.psClient.Dial(addr)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		return err
	}
	defer conn.CloseConn()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	indexOpt := &pb.IndexOption{
		AllowMultipleCheckouts: indexOptions.AllowMultipleCheckouts,
	}

	datasetIndexRequest := &pb.DatasetIndexRequest{
		Identifier:  datasetId,
		IndexOption: indexOpt,
		Origin: n.getPbNode(),
	}

	policyResponse, err := conn.IndexDataset(ctx, datasetIndexRequest)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		return err
	}

	defer conn.CloseConn()

	dataset := &pb.Dataset{
		Identifier: datasetId,
		Policy: policyResponse,
		IndexOption: indexOpt,
	}

	// Apply the update from policy store to storage
	if err := n.dsManager.InsertDataset(ctx, datasetId, dataset); err != nil {
		log.WithFields(logFields).Error(err.Error())
		return ErrFailedIndexing
	}

	return nil
}

func (n *NodeCore) requestDatasetIdentifierIndex(ctx context.Context, datasetId string) error {
	addr := n.getDirectoryServerGRPCIP()
	if addr == "" {
		err := fmt.Errorf("Directory server address is not set")
		log.WithFields(logFields).Error(err.Error())
		return err
	}

	conn, err := n.directoryServerClient.Dial(addr)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		return err
	}
	defer conn.CloseConn()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	req := &pb.DatasetIdentifierIndexRequest{
		Identifier: datasetId,
		Origin: n.getPbNode(),
	}

	_, err = conn.IndexDataset(ctx, req)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		return err
	}

	defer conn.CloseConn()

	return nil
}

// Removes the dataset policy from the node. The dataset will no longer be available to clients.
func (n *NodeCore) RemoveDataset(id string) error {
	return n.dsManager.RemoveDataset(context.Background(), id)
}

func (n *NodeCore) HandshakeDirectoryServer(addr string) error {
	if addr == "" {
		return ErrNoDirectoryServerHandshakeAddress
	}

	log.WithFields(logFields).Infof("Performing handshake with directory server at address %s\n", addr)
	conn, err := n.directoryServerClient.Dial(addr)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		return ErrDirectoryServerHandshakeFailed
	}

	defer conn.CloseConn()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*1)
	defer cancel()

	r, err := conn.Handshake(ctx, n.getPbNode())
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		return ErrDirectoryServerHandshakeFailed
	}

	n.setDirectoryServerIfritIP(r.GetIp())
	n.setDirectoryServerID(r.GetId())
	n.setDirectoryServerGRPCIP(addr)

	return nil
}

func (n *NodeCore) IfritAddress() string {
	return n.ifritClient.Addr()
}

func (n *NodeCore) Name() string {
	return n.config().Name
}

func (n *NodeCore) HandshakePolicyStore(addr string) error {
	if addr == "" {
		return ErrNoPolicyStoreHandshakeAddress
	}

	log.WithFields(logFields).Infof("Performing handshake with policy store at address %s\n", addr)
	conn, err := n.psClient.Dial(addr)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		return ErrPolicyStoreHandshakeFailed
	}
	defer conn.CloseConn()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*1)
	defer cancel()

	r, err := conn.Handshake(ctx, n.getPbNode())
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		return ErrNoPolicyStoreHandshakeAddress
	}

	defer conn.CloseConn()

	n.setPolicyStoreIfritIP(r.GetIp())
	n.setPolicyStoreID(r.GetId())
	n.setPolicyStoreGRPCIP(addr)

	return nil
}

// Main entry point for handling Ifrit direct messaging
func (n *NodeCore) messageHandler(data []byte) ([]byte, error) {
	// Parent context used for downstream signal. TODO: should implement context handling on sending side!
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	message := &pb.Message{}
	if err := proto.Unmarshal(data, message); err != nil {
		log.WithFields(logFields).Error(err.Error())
		return nil, err
	}

	log.WithFields(logFields).Infof("Node '%s' got a direct message", n.config().Name)

	if err := n.verifyMessageSignature(message); err != nil {
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
			if err := n.ifritClient.SetGossipContent(data); err != nil {
				log.WithFields(logFields).Error(err.Error())
			}

			// Might have seen msg before?
			if err := n.processPolicyBatch(ctx, msg); err != nil {
				log.WithFields(logFields).Error(err.Error())
			}

			if err := n.policyLog.InsertObservedGossip(ctx, msg); err != nil {
				log.WithFields(logFields).Error(err.Error())
			}
		default:
			log.WithFields(logFields).Error("Expected message to be a *pb.Policy, but it was ", msg)
		}
	} else {
		log.WithFields(logFields).Error("content in message is nil")
	}

	// ack!
	return nil, nil
}

// Check all messages in the gossip batch and see which ones we should apply
func (n *NodeCore) processPolicyBatch(ctx context.Context, g *pb.PolicyGossipUpdate) error {
	if g.GetGossipMessageBody() == nil {
		log.WithFields(logFields).Error(errGossipMessageBody.Error())
		return errGossipMessageBody
	}

	// Filter between relevant an irrelevant policies
	for _, m := range g.GetGossipMessageBody() {
		if newPolicy := m.GetPolicy(); newPolicy != nil {
			if datasetId := newPolicy.GetDatasetIdentifier(); datasetId != "" {
				exists, err := n.dsManager.DatasetExists(ctx, datasetId)
				if err != nil {
					log.WithFields(logFields).Error(err.Error())
				}

				if exists {
					dataset, err := n.dsManager.Dataset(ctx, datasetId)
					if err != nil {
						log.WithFields(logFields).Error(err.Error())
						continue
					}

					if dataset.GetPolicy().GetVersion() >= newPolicy.GetVersion() {
						log.WithFields(logFields).Infof("Got an outdated policy. Current version is %d. Got version %d", dataset.GetPolicy().GetVersion(), newPolicy.GetVersion())
						continue
					}

					if err := n.dsManager.SetDatasetPolicy(ctx, datasetId, newPolicy); err != nil {
						log.WithFields(logFields).Error(err.Error())
						continue
					}

					log.WithFields(logFields).Infof("Changed dataset %s's policy to %t\n", datasetId, newPolicy.GetContent())

					if err := n.policyLog.InsertAppliedPolicy(ctx, newPolicy); err != nil {
						log.WithFields(logFields).Error(err.Error())
						continue
					}
				} else {
					log.WithFields(logFields).Warnf("Dataset %s is not indexed at node %s", datasetId, n.Name())
				}
			} else {
				log.WithFields(logFields).Error("Dataset identifier was an emtpy string")
			}
		} else {
			log.WithFields(logFields).Error("Policy entry in gossip message body was nil")
		}
	}

	return nil
}

// Invoked on each received gossip message.
func (n *NodeCore) gossipHandler(data []byte) ([]byte, error) {
	message := &pb.Message{}
	if err := proto.Unmarshal(data, message); err != nil {
		log.WithFields(logFields).Error(err.Error())
		return nil, err
	}

	log.WithFields(logFields).Infof("Node '%s' got gossip message", n.config().Name)

	if err := n.verifyPolicyStoreMessage(message); err != nil {
		log.WithFields(logFields).Warn(err.Error())
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
			isObserved, err := n.policyLog.GossipIsObserved(context.Background(), msg)
			if err != nil {
				log.WithFields(logFields).Error(err.Error())
				return nil, err
			}

			if !isObserved {
				if err := n.processPolicyBatch(context.Background(), msg); err != nil {
					log.WithFields(logFields).Error(err.Error())
					return nil, err
				}
			}

			// Insert observed gossip message
			if err := n.policyLog.InsertObservedGossip(context.Background(), msg); err != nil {
				log.WithFields(logFields).Error(err.Error())
			}

			// Register gossip observation to reset set recon timer.
			// It doesn't matter if it was relevant or not.
			n.policySyncUnit.ResetPolicyReconciliationTimer()

		default:
			log.WithFields(logFields).Error("Unknown message type:", msg)
		}
	} else {
		log.WithFields(logFields).Error("content in message is nil")
	}

	return nil, nil
}

// Returns the string representation of the node.
func (n *NodeCore) String() string {
	return fmt.Sprintf("Name: %s\tIfrit address: %s", n.config().Name, n.IfritClient().Addr())
}

// Acknowledges the given probe message.  TODO MORE HERE
func (n *NodeCore) acknowledgeProbe(msg *pb.Message, d []byte) ([]byte, error) {
	/*if err := n.verifyPolicyStoreMessage(msg); err != nil {
		return nil, err
	}

	// Spread the probe message onwards through the network
	gossipContent, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}

	n.ifritClient.SetGossipContent(gossipContent)

	// Acknowledge the probe message
	resp := &pb.Message{
		Type: message.MSG_TYPE_PROBE_ACK,
		Sender: n.getPbNode(),
		Probe: msg.GetProbe(),
	}

	data, err := proto.Marshal(resp)
	if err != nil {
		return nil, err
	}

	r, s, err := n.ifritClient.Sign(data)
	if err != nil {
		return nil, err
	}

	msg.Signature = &pb.MsgSignature{R: r, S: s}
	data, err = proto.Marshal(msg)
	if err != nil {
		return nil, err
	}

	log.WithFields(logFields).Print(n.config().Name, " sending ack to Policy store")
	n.ifritClient.SendTo(n.policyStoreIP, data)*/
	return nil, nil
}

// TODO remove me and use PbNode in ps gossip message instead
func (n *NodeCore) verifyPolicyStoreMessage(msg *pb.Message) error {
	return nil
	r := msg.GetSignature().GetR()
	s := msg.GetSignature().GetS()

	msg.Signature = nil

	data, err := proto.Marshal(msg)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		return err
	}

	if !n.ifritClient.VerifySignature(r, s, data, string(n.policyStoreID)) {
		err := errors.New("Could not securely verify the integrity of the policy store message")
		log.WithFields(logFields).Error(err.Error())
		return err
	}

	// Restore message
	msg.Signature = &pb.MsgSignature{
		R: r,
		S: s,
	}

	return nil
}

func (n *NodeCore) getPbNode() *pb.Node {
	return n.pbnode
}

func (n *NodeCore) verifyMessageSignature(msg *pb.Message) error {
	return nil // try again with sleep time...
	// Verify the integrity of the node
	r := msg.GetSignature().GetR()
	s := msg.GetSignature().GetS()

	msg.Signature = nil

	// Marshal it before verifying its integrity
	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	_ = data

	/*if !n.ifritClient.VerifySignature(r, s, data, string(msg.GetSender().GetId())) {
		return errors.New("Mux could not securely verify the integrity of the message")
	}*/

	// Restore message
	msg.Signature = &pb.MsgSignature{
		R: r,
		S: s,
	}

	return nil
}

// Use this to refer to configuration variables
func (n *NodeCore) config() *Config {
	n.configLock.RLock()
	defer n.configLock.RUnlock()
	return n.conf
}

func (n *NodeCore) entity() string {
	return n.Name()
}

func (n *NodeCore) setPolicyStoreIfritIP(ip string) {
	n.policyStoreIfritIPMutex.Lock()
	defer n.policyStoreIfritIPMutex.Unlock()
	n.policyStoreIfritIP = ip
}

func (n *NodeCore) getPolicyStoreIfritIP() string {
	n.policyStoreIfritIPMutex.RLock()
	defer n.policyStoreIfritIPMutex.RUnlock()
	return n.policyStoreIfritIP
}

func (n *NodeCore) setPolicyStoreID(id []byte) {
	n.policyStoreIDMutex.Lock()
	defer n.policyStoreIDMutex.Unlock()
	n.policyStoreID = id
}

func (n *NodeCore) getPolicyStoreID() []byte {
	n.policyStoreIDMutex.RLock()
	defer n.policyStoreIDMutex.RUnlock()
	return n.policyStoreID
}

func (n *NodeCore) setDirectoryServerIfritIP(ip string) {
	n.directoryServerIfritIPMutex.Lock()
	defer n.directoryServerIfritIPMutex.Unlock()
	n.directoryServerIfritIP = ip
}

func (n *NodeCore) getDirectoryServerIfritIP() string {
	n.directoryServerIfritIPMutex.RLock()
	defer n.directoryServerIfritIPMutex.RUnlock()
	return n.directoryServerIfritIP
}

func (n *NodeCore) setDirectoryServerID(id []byte) {
	n.directoryServerIDMutex.Lock()
	defer n.directoryServerIDMutex.Unlock()
	n.directoryServerID = id
}

func (n *NodeCore) getDirectoryServerID() []byte {
	n.directoryServerIDMutex.RLock()
	defer n.directoryServerIDMutex.RUnlock()
	return n.directoryServerID
}

func (n *NodeCore) setDirectoryServerGRPCIP(ip string) {
	n.directoryServerGRPCIPMutex.Lock()
	defer n.directoryServerGRPCIPMutex.Unlock()
	n.directoryServerGRPCIP = ip
}

func (n *NodeCore) getDirectoryServerGRPCIP() string {
	n.directoryServerGRPCIPMutex.RLock()
	defer n.directoryServerGRPCIPMutex.RUnlock()
	return n.directoryServerGRPCIP
}

func (n *NodeCore) getPolicyStoreGRPCIP() string {
	n.policyStoreGRPCIPMutex.RLock()
	defer n.policyStoreGRPCIPMutex.RUnlock()
	return n.policyStoreGRPCIP
}

func (n *NodeCore) setPolicyStoreGRPCIP(ip string) {
	n.policyStoreGRPCIPMutex.Lock()
	defer n.policyStoreGRPCIPMutex.Unlock()
	n.policyStoreGRPCIP = ip
}
