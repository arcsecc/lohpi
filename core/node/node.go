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

	// If true, a dataset can be checked out multiple times by the same client
	AllowMultipleCheckouts bool

	// Port used by the HTTP server
	Port int

	// Interval that defines when the synchronization procedure will run
	SyncInterval time.Duration

	// Hostname used by the HTTP server
	HostName string
}

// TODO: use logrus to create machine-readable logs with fields!
// TODO move me somewhere else. ref gossip.go
type gossipMessage struct {
	Content [][]byte
	Hash    []byte
	Addr    string
}

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

	// Configuration 
	conf *Config
	configLock sync.RWMutex

	// gRPC client towards the directory server
	directoryServerClient *comm.DirectoryGRPCClient

	// Policy store
	psClient *comm.PolicyStoreGRPCClient

	// Used for identifying data coming from policy store
	directoryServerID []byte
	policyStoreID []byte	// TODO: allow for any number of policy store instances :)

	directoryServerIP string
	policyStoreIP string

	// Object id -> empty value for quick indexing
	datasetMap     map[string]*pb.Policy
	datasetMapLock sync.RWMutex

	httpServer   *http.Server

	datasetHandlerLock sync.RWMutex
	metadataHandlerLock sync.RWMutex

	datasetHandlerFunc clientRequestHandler
	metadataHandlerFunc clientRequestHandler

	dsManager datasetManager
	gossipObs gossipObserver
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

type gossipObserver interface {
	AddGossip(g *pb.GossipMessage) error
	GossipIsObserved(g *pb.GossipMessage) bool
	LatestGossip() *pb.GossipMessageID // TODO fix me!
}

type datasetManager interface {
	Dataset(datasetId string) *pb.Dataset
	Datasets() map[string]*pb.Dataset
	DatasetIdentifiers() []string
	DatasetExists(datasetId string) bool
	RemoveDataset(datasetId string) error 
	SetDatasetPolicy(datasetId string, policy *pb.Policy) error
	GetDatasetPolicy(datasetId string) *pb.Policy
	InsertDataset(datasetId string, dataset *pb.Dataset) error
}

type datasetCheckoutManager interface {
	CheckoutDataset(datasetId string, checkout *pb.DatasetCheckout) error
	DatasetIsCheckedOut(datasetId string, client *pb.Client) (bool, error)
	//CheckinDataset(datasetId string, client *pb.Client) error 
	DatasetCheckouts() ([]*pb.DatasetCheckout, error)
}

type stateSyncer interface {
	RegisterIfritClient(client *ifrit.Client) 
	SynchronizeDatasets(ctx context.Context, datasets map[string]*pb.Dataset, targetAddr string) (map[string]*pb.Dataset, error)
	//ResolveDatasets(ctx context.Context, currentDatasets map[string]*pb.Dataset, incomingDatasets map[string]*pb.Dataset) (map[string]*pb.Dataset, error)
}

// TODO: reload dateapplied and datecreated from the databases on boot time.
func NewNodeCore(cm certManager, gossipObs gossipObserver, dsManager datasetManager, stateSync stateSyncer, dcManager datasetCheckoutManager, config *Config) (*NodeCore, error) {
	if config == nil {
		return nil, errors.New("Configuration is nil")
	}
	
	ifritClient, err := ifrit.NewClient(&ifrit.ClientConfig{
		UdpPort: 8000, 
		TcpPort: 5000,
		Hostname: "lohpi-azureblobnode.norwayeast.azurecontainer.io",
		CertPath: "./crypto/ifrit",})
	if err != nil {
		return nil, err
	}

	directoryServerClient, err := comm.NewDirectoryServerGRPCClient(cm.Certificate(), cm.CaCertificate(), cm.PrivateKey())
	if err != nil {
		return nil, err
	}

	psClient, err := comm.NewPolicyStoreClient(cm.Certificate(), cm.CaCertificate(), cm.PrivateKey())
	if err != nil {
		return nil, err
	}

	pbnode := &pb.Node{
		Name:         	config.Name,
		IfritAddress: 	fmt.Sprintf("%s:%d", config.HostName, 5000),
		Id:           	[]byte(ifritClient.Id()),
		HttpsAddress:	config.HostName,
		Port: 			int32(config.Port),
		BootTime:		pbtime.Now(),
	}

	log.Println("pbnode:", pbnode.HttpsAddress)

	node := &NodeCore{
		ifritClient:  			  ifritClient,
		directoryServerClient:    directoryServerClient,
		conf:		   			  config,
		psClient:     			  psClient,
		datasetMap:     		  make(map[string]*pb.Policy),
		datasetMapLock: 		  sync.RWMutex{},
		gossipObs: 	  			  gossipObs,
		dsManager: 				  dsManager,
		stateSync: 				  stateSync,
		dcManager: 				  dcManager,
		exitChan:				  make(chan bool, 1),
		pbnode: 			  	  pbnode,	
	}

	// Set the Ifrit handlers
	node.ifritClient.RegisterMsgHandler(node.messageHandler)
	node.ifritClient.RegisterGossipHandler(node.gossipHandler)

	// Register the Ifrit client such that the syncer can use it
	node.stateSync.RegisterIfritClient(ifritClient)

	return node, nil
}

// TODO: restore checkouts from previous runs so that we don't lose information from memory.
// Especially for checkout datasets 
func (n *NodeCore) IfritClient() *ifrit.Client {
	return n.ifritClient
}

func (n *NodeCore) Start() {
	go n.ifritClient.Start()
	go n.startStateSyncer()
	go n.startHTTPServer()
}

// TODO: refine me! Fix the abstraction boundaries
func (n *NodeCore) startStateSyncer() {
	for {
		select {
		case <-n.exitChan:
			log.Info("Exiting sync")
			return
		case <-time.After(n.config().SyncInterval):
/*			deltaMap, err := n.stateSync.SynchronizeDatasets(context.Background(), n.dsManager.Datasets(), n.policyStoreIP)
			if err != nil {
				log.Errorln(err.Error())
			}

			if deltaMap != nil {
				for id, ds := range deltaMap {
					if err := n.dsManager.SetDatasetPolicy(id, ds.GetPolicy()); err != nil {
						log.Error(err.Error())
					}
				}
			}

			// Send the correct identifiers to the directory server
			if err := n.pbResolveDatsetIdentifiers(n.directoryServerIP); err != nil {
				log.Error(err.Error())
			}*/
		}
	}
}

// RequestPolicy requests policies from policy store that are assigned to the dataset given by the id.
// It will also populate the node's database with the available identifiers and assoicate them with policies.
// You will have to call this method to make the datasets available to the clients. 
func (n *NodeCore) IndexDataset(datasetId string, indexOptions *DatasetIndexingOptions) error {
	if n.dsManager.DatasetExists(datasetId) {
		log.Infof("Dataset with identifier '%s' is already indexed by the server\n", datasetId)
	}

	// Send policy request to policy store
	policyResponse, err := n.pbSendPolicyStorePolicyRequest(datasetId, n.policyStoreIP)
	if err != nil {
		return err
	}

	dataset := &pb.Dataset{
		Identifier: datasetId,
		Policy: policyResponse,
		AllowMultipleCheckouts: indexOptions.AllowMultipleCheckouts,
	}

	// Apply the update from policy store to storage
	if err := n.dsManager.InsertDataset(datasetId, dataset); err != nil {
		return err
	}

	// Notify the directory server of the newest policy update
	if err := n.pbSendDatsetIdentifier(datasetId, n.directoryServerIP); err != nil {
		return err
	}

	return nil
}

// Removes the dataset policy from the node. The dataset will no longer be available to clients.
func (n *NodeCore) RemoveDataset(id string) {
	n.dsManager.RemoveDataset(id)
}

// Shuts down the node
func (n *NodeCore) Shutdown() {
	log.Infoln("Shutting down Lohpi node")
	n.ifritClient.Stop()
	n.exitChan <-true
	//n.dsManager.StopSynchronizer()
}

func (n *NodeCore) HandshakeDirectoryServer(addr string) error {
	if addr == "" {
		return errors.New("Address is empty")
	}

	log.Infof("Performing handshake with directory server at address %s\n", addr)
	conn, err := n.directoryServerClient.Dial(addr)
	if err != nil {
		return err
	}

	defer conn.CloseConn()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()

	log.Println("getPbNode():", n.getPbNode())
	r, err := conn.Handshake(ctx, n.getPbNode())
	if err != nil {
		panic(err)
		return err
	}

	n.directoryServerIP = r.GetIp()
	n.directoryServerID = r.GetId()

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
		return errors.New("Address is empty")
	}

	log.Infof("Performing handshake with policy store at address %s\n", addr)
	conn, err := n.psClient.Dial(addr)
	if err != nil {
		return err
	}
	defer conn.CloseConn()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()

	r, err := conn.Handshake(ctx, n.getPbNode())
	if err != nil {
		return err
	}

	defer conn.CloseConn()

	n.policyStoreIP = r.GetIp()
	n.policyStoreID = r.GetId()
	return nil
}

// Main entry point for handling Ifrit direct messaging
func (n *NodeCore) messageHandler(data []byte) ([]byte, error) {
	msg := &pb.Message{}
	if err := proto.Unmarshal(data, msg); err != nil {
		log.Errorln(err)
		return nil, err
	}

	log.Infof("Node '%s' got direct message %s\n", n.config().Name, msg.GetType())
	if err := n.verifyMessageSignature(msg); err != nil {
		log.Errorln(err)
		return nil, err
	}

	switch msgType := msg.Type; msgType {
	case message.MSG_TYPE_GET_NODE_INFO:
		return []byte(n.String()), nil

	case message.MSG_TYPE_GET_DATASET_IDENTIFIERS:
		return n.pbMarshalDatasetIdentifiers(msg)

	case message.MSG_TYPE_POLICY_STORE_UPDATE:
		n.ifritClient.SetGossipContent(data)
		return n.processPolicyBatch(msg)

	case message.MSG_TYPE_ROLLBACK_CHECKOUT:
		n.rollbackCheckout(msg)

	case message.MSG_TYPE_PROBE:
		return n.acknowledgeProbe(msg, data)

	default:
		fmt.Printf("Unknown message type: %s\n", msg.GetType())
	}

	return n.pbMarshalAcknowledgeMessage()
}

// TODO finish me
func (n *NodeCore) rollbackCheckout(msg *pb.Message) {
	/*id := msg.GetStringValue()
	if err := n.dbCheckinDataset(id); err != nil {
		log.Errorln(err.Error())
	}*/
}

// Check all messages in the gossip batch and see which ones we should apply
func (n *NodeCore) processPolicyBatch(msg *pb.Message) ([]byte, error) {
	if msg.GetGossipMessage() == nil {
		err := errors.New("Gossip message is nil")
		log.Errorln(err.Error())
		return nil, err
	}

	gspMsg := msg.GetGossipMessage()
	if gspMsg.GetGossipMessageBody() == nil {
		err := errors.New("Gossip message body is nil")
		log.Errorln(err.Error())
		return nil, err
	}
	
	for _, m := range gspMsg.GetGossipMessageBody() {
		if err := n.applyPolicy(m.GetPolicy()); err != nil {
			log.Errorln(err.Error())
		}
	}

	return nil, nil
}

// Apply the given policy to the correct dataset.
// TODO: log things better using logrus. 
func (n *NodeCore) applyPolicy(p *pb.Policy) error {
	datasetId := p.GetDatasetIdentifier()
	if n.dsManager.DatasetExists(datasetId) {
		dataset := n.dsManager.Dataset(datasetId)
		if dataset == nil {
			return errors.New("Tried to get dataset description but it was nil")
		}

		if dataset.GetPolicy().GetVersion() >= p.GetVersion() {
			log.Infoln("Got an outdated policy. Applies a new version")
			return nil
		}

		if err := n.dsManager.SetDatasetPolicy(datasetId, p); err != nil {
			return err
		}

		// Log deltas in policies to logfile
		// TODO: put loggers in interfaces?
		log.Infof("Changed dataset %s's policy to %s\n", datasetId, p.GetContent())

		// Update revocation list. Optimization: don't push update message if policy is reduntant.
		/*if n.dbDatasetIsCheckedOutByClient(datasetId) {
			if err := n.pbSendDatasetRevocationUpdate(datasetId, p.GetContent()); err != nil {
				return err
			}
		}*/
	} else {
		return fmt.Errorf("Dataset %s does not exist", datasetId)
	}
	return nil
}

// TODO: match the required access credentials of the dataset to the
// access attributes of the client. Return true if the client can access the data,
// return false otherwise. Also, verify that all the fields are present
func (n *NodeCore) clientIsAllowed(r *pb.DatasetRequest) bool {
	if r == nil {
		log.Errorln("Dataset request is nil")
		return false
	}

	//return n.dbDatasetIsAvailable(r.GetIdentifier())
	return true
}

// Invoked on each received gossip message. 
func (n *NodeCore) gossipHandler(data []byte) ([]byte, error) {
	msg := &pb.Message{}
	if err := proto.Unmarshal(data, msg); err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	log.Infof("Node '%s' got gossip message %s\n", n.config().Name, msg.GetType())

	if err := n.verifyPolicyStoreMessage(msg); err != nil {
		log.Warnln(err.Error())
	}

	switch msgType := msg.Type; msgType {
	case message.MSG_TYPE_PROBE:
		//n.ifritClient.SetGossipContent(data)
	case message.MSG_TYPE_POLICY_STORE_UPDATE:
/*		if !n.policyObs.gossipIsObserved(msg.GetGossipMessage()) {
			n.policyObs.addObservedGossip(msg.GetGossipMessage())
			return n.processPolicyBatch(msg)
		}*/
		
	default:
		fmt.Printf("Unknown gossip message type: %s\n", msg.GetType())
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

	log.Println(n.config().Name, "sending ack to Policy store")
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
		log.Errorln(err.Error())
		return err
	}

	if !n.ifritClient.VerifySignature(r, s, data, string(n.policyStoreID)) {
		err := errors.New("Could not securely verify the integrity of the policy store message")
		log.Errorln(err.Error())
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