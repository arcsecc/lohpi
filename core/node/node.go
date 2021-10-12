package node

import (
	"context"
	"fmt"
	"github.com/arcsecc/lohpi/core/comm"
	"github.com/arcsecc/lohpi/core/message"
	"crypto/x509"
	"crypto/ecdsa"
	"encoding/json"
	"github.com/lestrrat-go/jwx/jws"
	"github.com/lestrrat-go/jwx/jwk"
	pb "github.com/arcsecc/lohpi/protobuf"
	"github.com/golang/protobuf/proto"
	"github.com/joonnna/ifrit"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"net/http"
	pbtime "google.golang.org/protobuf/types/known/timestamppb"
	"sync"
	"time"
)

var (
	errNoAddr = errors.New("No certificate authority address provided, can't continue")
)

type Client struct {
	Name string `json:"name"`
	Oid  string `json:"oid"`
}

type Config struct {
	SQLConnectionString string 

	// The name of this node
	Name string

	// Port used by the HTTP server
	Port int

	// Hostname used by the HTTP server
	Hostname string

	// Ifrit's TCP port. Default value is 5000.
	IfritTCPPort int

	// Ifrit's UDP port. Default value is 6000.
	IfritUDPPort int

	IfritCryptoUnitWorkingDirectory string
}

func init() { 
	log.SetReportCaller(true)
}

var nodeLogFields = log.Fields{
	"package": "core/node",
	"description": "dataset node",
}

// TODO: use logrus to create machine-readable logs with fields!
// TODO move me somewhere else. ref gossip.go
type gossipMessage struct {
	Content [][]byte
	Hash    []byte
	Addr    string
}

var (
	ErrNoConfig = errors.New("Node configuration is nil")
	ErrFailedIndexing = errors.New("Failed to index dataset")
	ErrNoDirectoryServerHandshakeAddress = errors.New("No directory server handshake address")
	ErrDirectoryServerHandshakeFailed = errors.New("Directory server handshake failed")
	ErrNoPolicyStoreHandshakeAddress = errors.New("No policy store handshake address")
	ErrPolicyStoreHandshakeFailed = errors.New("Policy store handshake failed")
)

type DatasetIndexingOptions struct {
	AllowMultipleCheckouts bool
}

// Types used in client requests
type clientRequestHandler func(id string, w http.ResponseWriter, r *http.Request)

// TODO: refine storage of data. Ie: use database as the secondary storage with in-memory maps
// as the primary storage. Use retention policy to flush the caches to disk/db? 
// Only flush deltas to disk/db to reduce data transfer
type NodeCore struct {
	// Underlying ifrit client
	ifritClient *ifrit.Client

	exitChan chan bool
	exitFlag  bool
	exitMutex sync.RWMutex

	// Configuration 
	conf *Config
	configLock sync.RWMutex

	// gRPC client towards the directory server
	directoryServerClient *comm.DirectoryGRPCClient

	// Policy store
	psClient *comm.PolicyStoreGRPCClient

	// Used for identifying data coming from policy store
	directoryServerID []byte
	directoryServerIDMutex sync.RWMutex 
	
	policyStoreID []byte	// TODO: allow for any number of policy store instances :)
	policyStoreIDMutex sync.RWMutex

	directoryServerIP string
	directoryServerIPMutex sync.RWMutex

	policyStoreIP string
	policyStoreIPMutex sync.RWMutex

	// Object id -> empty value for quick indexing
	datasetMap     map[string]*pb.Policy
	datasetMapLock sync.RWMutex

	httpServer   *http.Server

	datasetHandlerLock sync.RWMutex
	metadataHandlerLock sync.RWMutex

	datasetHandlerFunc clientRequestHandler
	metadataHandlerFunc clientRequestHandler

	dsManager datasetManager
	policyLog policyLogger
	stateSync stateSyncer
	dcManager datasetCheckoutManager

	pbnode *pb.Node
	boottime *pbtime.Timestamp
	pubKeyCache *jwk.AutoRefresh	
}

type certManager interface {
	Certificate() *x509.Certificate
	CaCertificate() *x509.Certificate
	PrivateKey() *ecdsa.PrivateKey
	PublicKey() *ecdsa.PublicKey
}

type policyLogger interface {
	InsertObservedGossip(g *pb.GossipMessage) error
	GossipIsObserved(g *pb.GossipMessage) bool
	InsertAppliedPolicy(p *pb.Policy) error
}

type datasetManager interface {
	Dataset(datasetId string) *pb.Dataset
	Datasets() map[string]*pb.Dataset // index and cursor here!
	DatasetIdentifiers(idx int64, length int64) []string
	DatasetExists(datasetId string) bool
	RemoveDataset(datasetId string) error 
	SetDatasetPolicy(datasetId string, policy *pb.Policy) error
	GetDatasetPolicy(datasetId string) *pb.Policy
	InsertDataset(datasetId string, dataset *pb.Dataset) error
}

type datasetCheckoutManager interface {
	CheckoutDataset(datasetId string, checkout *pb.DatasetCheckout) error
	DatasetIsCheckedOutByClient(datasetId string, client *pb.Client) bool
	DatasetIsCheckedOut(datasetId string) bool
	//CheckinDataset(datasetId string, client *pb.Client) error 
	DatasetCheckouts(datasetId string) ([]*pb.DatasetCheckout, error)
}

type stateSyncer interface {
	RegisterIfritClient(client *ifrit.Client) 
	ResolveDatasetIdentifiersDeltas(currentDatasets []string, incomingDatasets []string) ([]string, []string)
	//ResolveDatasets(ctx context.Context, currentDatasets map[string]*pb.Dataset, incomingDatasets map[string]*pb.Dataset) (map[string]*pb.Dataset, error)
}

// TODO: reload dateapplied and datecreated from the databases on boot time.
func NewNodeCore(cm certManager, policyLog policyLogger, dsManager datasetManager, stateSync stateSyncer, dcManager datasetCheckoutManager, config *Config, new bool) (*NodeCore, error) {
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
		log.WithFields(nodeLogFields).Error(err.Error())
		return nil, err
	}

	directoryServerClient, err := comm.NewDirectoryServerGRPCClient(cm.Certificate(), cm.CaCertificate(), cm.PrivateKey())
	if err != nil {
		log.WithFields(nodeLogFields).Error(err.Error())
		return nil, err
	}

	psClient, err := comm.NewPolicyStoreClient(cm.Certificate(), cm.CaCertificate(), cm.PrivateKey())
	if err != nil {
		log.WithFields(nodeLogFields).Error(err.Error())
		return nil, err
	}

	pbnode := &pb.Node{
		Name:         	config.Name,
		IfritAddress: 	fmt.Sprintf("%s:%d", config.Hostname, config.IfritTCPPort),
		//IfritAddress: 	ifritClient.Addr(),
		Id:           	[]byte(ifritClient.Id()),
		HttpsAddress:	config.Hostname,
		Port: 			int32(config.Port),
		BootTime:		pbtime.Now(),
	}

	node := &NodeCore{
		ifritClient:  			  ifritClient,
		directoryServerClient:    directoryServerClient,
		conf:		   			  config,
		psClient:     			  psClient,
		datasetMap:     		  make(map[string]*pb.Policy),
		datasetMapLock: 		  sync.RWMutex{},
		policyLog: 	  			  policyLog,
		dsManager: 				  dsManager,
		stateSync: 				  stateSync,
		dcManager: 				  dcManager,
		exitChan:				  make(chan bool, 1),
		exitFlag: 				  false,
		pbnode: 			  	  pbnode,	
	}

	// Set the Ifrit handlers
	node.ifritClient.RegisterMsgHandler(node.messageHandler)
	node.ifritClient.RegisterGossipHandler(node.gossipHandler)
	node.stateSync.RegisterIfritClient(ifritClient)

	return node, nil
}

// TODO: restore checkouts from previous runs so that we don't lose information from memory.
// Especially for checkout datasets 
func (n *NodeCore) IfritClient() *ifrit.Client {
	return n.ifritClient
}

func (n *NodeCore) Start() {
	log.WithFields(nodeLogFields).Infoln("Starting Lohpi node's services...")
	go n.ifritClient.Start()
	go n.startHTTPServer()
	<-n.exitChan

	log.WithFields(nodeLogFields).Infoln("Stopping Lohpi node's services...")
	n.Stop()
}

func (n *NodeCore) Stop() {
	if n.isStopping() {
		return
	}

	if err := n.stopHTTPServer(); err != nil {
		log.WithFields(nodeLogFields).Error(err.Error())
	}
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

// RequestPolicy requests policies from policy store that are assigned to the dataset given by the id.
// It will also populate the node's database with the available identifiers and assoicate them with policies.
// You will have to call this method to make the datasets available to the clients. 
func (n *NodeCore) IndexDataset(datasetId string, indexOptions *DatasetIndexingOptions) error {
	if n.dsManager.DatasetExists(datasetId) {
		log.WithFields(nodeLogFields).Infof("Dataset with identifier '%s' is already indexed by the server\n", datasetId)
	}

	// Send policy request to policy store
	policyResponse, err := n.pbSendPolicyStorePolicyRequest(datasetId)
	if err != nil {
		return ErrFailedIndexing
	}

	dataset := &pb.Dataset{
		Identifier: datasetId,
		Policy: policyResponse,
		AllowMultipleCheckouts: indexOptions.AllowMultipleCheckouts,
	}

	// Apply the update from policy store to storage
	if err := n.dsManager.InsertDataset(datasetId, dataset); err != nil {
		log.WithFields(nodeLogFields).Error(err.Error())
		return ErrFailedIndexing
	}

	// Notify the directory server of the newest policy update
	if err := n.pbSendDatsetIdentifier(datasetId, n.directoryServerIP); err != nil {
		log.WithFields(nodeLogFields).Error(err.Error())
		return ErrFailedIndexing
	}

	return nil
}

// Removes the dataset policy from the node. The dataset will no longer be available to clients.
func (n *NodeCore) RemoveDataset(id string) {
	log.WithFields(nodeLogFields).Infof("Removing dataset with id '%s'\n", id)
	n.dsManager.RemoveDataset(id)
}

// Shuts down the node
func (n *NodeCore) Shutdown() {
	log.WithFields(nodeLogFields).Infoln("Shutting down Lohpi node")
	n.ifritClient.Stop()
	n.exitChan <-true
	//n.dsManager.StopSynchronizer()
}

func (n *NodeCore) HandshakeDirectoryServer(addr string) error {
	if addr == "" {
		return ErrNoDirectoryServerHandshakeAddress
	}

	log.WithFields(nodeLogFields).Infof("Performing handshake with directory server at address %s\n", addr)
	conn, err := n.directoryServerClient.Dial(addr)
	if err != nil {
		log.WithFields(nodeLogFields).Error(err.Error())
		return ErrDirectoryServerHandshakeFailed
	}

	defer conn.CloseConn()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute * 1)
	defer cancel()

	r, err := conn.Handshake(ctx, n.getPbNode())
	if err != nil {
		log.WithFields(nodeLogFields).Error(err.Error())
		return ErrDirectoryServerHandshakeFailed
	}

	n.setDirectoryServerIP(r.GetIp())
	n.setDirectoryServerID(r.GetId())

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

	log.WithFields(nodeLogFields).Infof("Performing handshake with policy store at address %s\n", addr)
	conn, err := n.psClient.Dial(addr)
	if err != nil {
		log.WithFields(nodeLogFields).Error(err.Error())
		return ErrPolicyStoreHandshakeFailed
	}
	defer conn.CloseConn()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute * 1)
	defer cancel()

	r, err := conn.Handshake(ctx, n.getPbNode())
	if err != nil {
		log.WithFields(nodeLogFields).Error(err.Error())
		return ErrNoPolicyStoreHandshakeAddress
	}

	defer conn.CloseConn()

	n.setPolicyStoreIP(r.GetIp())
	n.setPolicyStoreID(r.GetId())
	
	return nil
}

// Main entry point for handling Ifrit direct messaging
func (n *NodeCore) messageHandler(data []byte) ([]byte, error) {
	msg := &pb.Message{}
	if err := proto.Unmarshal(data, msg); err != nil {
		log.WithFields(nodeLogFields).Error(err.Error())
		return nil, err
	}

	log.WithFields(nodeLogFields).
		WithField("Ifrit RPC message", msg).
		Infof("Node '%s' got direct message %s\n", n.config().Name, msg.GetType())

	if err := n.verifyMessageSignature(msg); err != nil {
		log.WithFields(nodeLogFields).Error(err.Error())
		return nil, err
	}

	switch msgType := msg.Type; msgType {
	case message.MSG_TYPE_GET_NODE_INFO:
		return []byte(n.String()), nil

	case message.MSG_SYNCHRONIZE_DATASET_IDENTIFIERS:
		currentIdentifiers := n.dsManager.DatasetIdentifiers(0, -1) // range?
		incomingIdentifiers := msg.GetStringSlice()
		newIdentifiers, staleIdentifiers := n.stateSync.ResolveDatasetIdentifiersDeltas(currentIdentifiers, incomingIdentifiers)

		log.WithFields(nodeLogFields).
			Infof(``)
		return n.pbMarshalDatasetIdentifiersResponse(newIdentifiers, staleIdentifiers)

	case message.MSG_TYPE_GET_DATASET_IDENTIFIERS:
		return n.pbMarshalDatasetIdentifiers(msg)

	case message.MSG_TYPE_POLICY_STORE_UPDATE:
		// Might avoid calling SetGossipContent for each multicast message received?
		if err := n.ifritClient.SetGossipContent(data); err != nil {
			log.WithFields(nodeLogFields).Error(err.Error())
		}
		
		if g := msg.GetGossipMessage(); g != nil {
			if err := n.processPolicyBatch(msg); err != nil {
				log.WithFields(nodeLogFields).Error(err.Error())
			}

			if err := n.policyLog.InsertObservedGossip(g); err != nil {
				log.WithFields(nodeLogFields).Error(err.Error())
			}
		}

	case message.MSG_TYPE_ROLLBACK_CHECKOUT:
		n.rollbackCheckout(msg)

	case message.MSG_TYPE_PROBE:
		return n.acknowledgeProbe(msg, data)

	case message.MSG_SYNCHRONIZE_DATASETS:
		return n.pbMarshalDatasetCollectionResponse(msg)

	case message.MSG_DELTA_DATASETS:
		if err := n.insertDeltaDataset(msg); err != nil {
			log.WithFields(nodeLogFields).Error(err.Error())
		}
	
	default:
		log.WithFields(nodeLogFields).
			WithField("Ifrit RPC message", "Unknown message type").
			Errorf("Unknown message type: %s\n", msg.GetType())
	}

	return n.pbMarshalAcknowledgeMessage()
}

func (n *NodeCore) insertDeltaDataset(msg *pb.Message) error {
	if deltaDatasets := msg.GetDatasetSyncronizationDelta().GetDatasets(); deltaDatasets != nil {
		for _, d := range deltaDatasets {
			if err := n.dsManager.InsertDataset(d.GetIdentifier(), d); err != nil {
				log.WithFields(nodeLogFields).Error(err.Error())
			}
		}
	}

	return nil
}

// TODO finish me
func (n *NodeCore) rollbackCheckout(msg *pb.Message) {
	/*id := msg.GetStringValue()
	if err := n.dbCheckinDataset(id); err != nil {
		log.WithFields(nodeLogFields).Error(err.Error())
	}*/
}

// Check all messages in the gossip batch and see which ones we should apply
func (n *NodeCore) processPolicyBatch(msg *pb.Message) error {
	if msg.GetGossipMessage() == nil {
		err := errors.New("Gossip message is nil")
		log.WithFields(nodeLogFields).Error(err.Error())
		return err
	}

	gspMsg := msg.GetGossipMessage()
	if gspMsg.GetGossipMessageBody() == nil {
		err := errors.New("Gossip message body is nil")
		log.WithFields(nodeLogFields).Error(err.Error())
		return err
	}
	
	// Filter between relevant an irrelevant policies
	for _, m := range gspMsg.GetGossipMessageBody() {
		if newPolicy := m.GetPolicy(); newPolicy != nil {
			if datasetId := newPolicy.GetDatasetIdentifier(); datasetId != "" {
				if n.dsManager.DatasetExists(datasetId) {
					if dataset := n.dsManager.Dataset(datasetId); dataset != nil {
						if dataset.GetPolicy().GetVersion() >= newPolicy.GetVersion() {
							log.WithFields(nodeLogFields).Infof("Got an outdated policy: Current version is %d. Got version %d", dataset.GetPolicy().GetVersion(), newPolicy.GetVersion())
							return nil
						}
			
						if err := n.dsManager.SetDatasetPolicy(datasetId, newPolicy); err != nil {
							log.WithFields(nodeLogFields).Error(err.Error())
							return err
						}
			
						log.WithFields(nodeLogFields).Infof("Changed dataset %s's policy to %t\n", datasetId, newPolicy.GetContent())
			
						if err := n.policyLog.InsertAppliedPolicy(newPolicy); err != nil {
							log.WithFields(nodeLogFields).Error(err.Error())
							return err
						}		
					} else {
						err := fmt.Errorf("Tried to get dataset identifier but it was nil")
						log.WithFields(nodeLogFields).Error(err.Error())
						return err
					}
				} else {
					return fmt.Errorf("Dataset %s is not indexed at node %s", datasetId, n.Name())
				}
			} else {
				log.WithFields(nodeLogFields).Errorf("Dataset identifier was an emtpy string")	
			}
		} else {
			log.WithFields(nodeLogFields).Errorf("Policy entry in gossip message body was nil")
		}
	}

	return nil
}

// TODO: match the required access credentials of the dataset to the
// access attributes of the client. Return true if the client can access the data,
// return false otherwise. Also, verify that all the fields are present
func (n *NodeCore) clientIsAllowed(r *pb.DatasetRequest) bool {
	if r == nil {
		log.WithFields(nodeLogFields).Error("Dataset request is nil")
		return false
	}

	//return n.dbDatasetIsAvailable(r.GetIdentifier())
	return true
}

// Invoked on each received gossip message. 
func (n *NodeCore) gossipHandler(data []byte) ([]byte, error) {
	msg := &pb.Message{}
	if err := proto.Unmarshal(data, msg); err != nil {
		log.WithFields(nodeLogFields).Error(err.Error())
		return nil, err
	}

	log.WithFields(nodeLogFields).Infof("Node '%s' got gossip message %s\n", n.config().Name, msg.GetType())

	if err := n.verifyPolicyStoreMessage(msg); err != nil {
		log.WithFields(nodeLogFields).Warn(err.Error())
	}

	switch msgType := msg.Type; msgType {
	case message.MSG_TYPE_PROBE:
		//n.ifritClient.SetGossipContent(data)
	case message.MSG_TYPE_POLICY_STORE_UPDATE:
		if gspMsg := msg.GetGossipMessage(); gspMsg != nil {
			if !n.policyLog.GossipIsObserved(gspMsg) {
				if err := n.processPolicyBatch(msg); err != nil {
					log.WithFields(nodeLogFields).Error(err.Error())
				}
			} else {
				log.WithFields(nodeLogFields).
					WithField("gossip handler", "observation").
					Debugf("Already seen gossip meesage %+v", gspMsg)
			}
		} else {
			log.WithFields(nodeLogFields).Errorf("Gossip message body was nil")
		}
	default:
		fmt.Printf("Unknown gossip message type: %s\n", msg.GetType())
	}

	// Insert observed gossip message
	if err := n.policyLog.InsertObservedGossip(msg.GetGossipMessage()); err != nil {
		log.WithFields(nodeLogFields).Error(err.Error())
	}

	return nil, nil
}

// Returns the string representation of the node.
func (n *NodeCore) String() string {
	return fmt.Sprintf("Name: %s\tIfrit address: %s", n.config().Name, n.IfritClient().Addr())
}

// Acknowledges the given probe message.  TODO MORE HERE
func (n *NodeCore) acknowledgeProbe(msg *pb.Message, d []byte) ([]byte, error) {
	if err := n.verifyPolicyStoreMessage(msg); err != nil {
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

	log.WithFields(nodeLogFields).Print(n.config().Name, "sending ack to Policy store")
	n.ifritClient.SendTo(n.policyStoreIP, data)
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
		log.WithFields(nodeLogFields).Error(err.Error())
		return err
	}

	if !n.ifritClient.VerifySignature(r, s, data, string(n.policyStoreID)) {
		err := errors.New("Could not securely verify the integrity of the policy store message")
		log.WithFields(nodeLogFields).Error(err.Error())
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

	if !n.ifritClient.VerifySignature(r, s, data, string(msg.GetSender().GetId())) {
		return errors.New("Mux could not securely verify the integrity of the message")
	}

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

func (n *NodeCore) setPolicyStoreIP(ip string) { 
	n.policyStoreIPMutex.Lock()
	defer n.policyStoreIPMutex.Unlock()
	n.policyStoreIP = ip
}

func (n *NodeCore) getPolicyStoreIP() string { 
	n.policyStoreIPMutex.RLock()
	defer n.policyStoreIPMutex.RUnlock()
	return n.policyStoreIP
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

func (n *NodeCore) setDirectoryServerIP(ip string) {
	n.directoryServerIPMutex.Lock()
	defer n.directoryServerIPMutex.Unlock()
	n.directoryServerIP = ip
}

func (n *NodeCore) getDirectoryServerIP() string {
	n.directoryServerIPMutex.RLock()
	defer n.directoryServerIPMutex.RUnlock()
	return n.directoryServerIP
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

func jwtTokenToPbClient(token string) (*pb.Client, error) {
	if token == "" {
		return nil, errors.New("Token string is empty")
	}

	msg, err := jws.ParseString(token)
	if err != nil {
		return nil, err
	}

	s := msg.Payload()
	if s == nil {
		return nil, errors.New("Payload was nil")
	}

	c := struct {
		Name    		string `json:"name"`
		Oid 			string `json:"oid"`
		EmailAddress 	string `json:"email"`
	}{}

	if err := json.Unmarshal(s, &c); err != nil {
		return nil, err
	}

	return &pb.Client{
		Name: c.Name,
		ID: c.Oid,
		EmailAddress: c.EmailAddress,
	}, nil
}