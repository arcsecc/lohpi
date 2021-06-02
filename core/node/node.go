package node

import (
	"context"
	"crypto/x509/pkix"
	"database/sql"
	"fmt"
	"github.com/arcsecc/lohpi/core/comm"
	"github.com/arcsecc/lohpi/core/message"
	"github.com/arcsecc/lohpi/core/netutil"
	pb "github.com/arcsecc/lohpi/protobuf"
	"github.com/golang/protobuf/proto"
	"github.com/joonnna/ifrit"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"net"
	"net/http"
	"sync"
	"time"
)

var (
	errNoAddr = errors.New("No certificate authority address provided, can't continue")
)

type Config struct {
	HostName					string
	HTTPPort                	int
	PolicyStoreAddress   		string
	DirectoryServerAddress  	string
	LohpiCaAddress 				string
	Name 						string
	PostgresSQLConnectionString string
	DatabaseRetentionInterval	time.Duration
	AllowMultipleCheckouts 		bool
	PolicyObserverWorkingDirectory string // interface me
}

// TODO: use logrus to create machine-readable logs with fields!

// TODO move me somewhere else. ref gossip.go
type gossipMessage struct {
	Content [][]byte
	Hash    []byte
	Addr    string
}

// Types used in client requests
type clientRequestHandler func(id string, w http.ResponseWriter, r *http.Request)

// TODO: refine storage of data. Ie: use database as the secondary storage with in-memory maps
// as the primary storage. Use retention policy to flush the caches to disk/db? 
// Only flush deltas to disk/db to reduce data transfer
type NodeCore struct {
	// Underlying ifrit client
	ifritClient *ifrit.Client

	// Crypto unit
	cu *comm.CryptoUnit

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

	listener net.Listener
	httpServer   *http.Server

	// Azure database
	datasetCheckoutDB *sql.DB
	datasetPolicyDB   *sql.DB

	datasetHandlerLock sync.RWMutex
	metadataHandlerLock sync.RWMutex

	datasetHandlerFunc clientRequestHandler
	metadataHandlerFunc clientRequestHandler

	policyObs *policyObserver
}

// Entry of all gossip messages that are observed since the node joined the network
type policyGossipObservation struct {
	ArrivedAt time.Time
	MessageID *pb.GossipMessageID
}

func NewNodeCore(config *Config) (*NodeCore, error) {
	ifritClient, err := ifrit.NewClient()
	if err != nil {
		return nil, err
	}

	go ifritClient.Start()

	listener, err := netutil.GetListener()
	if err != nil {
		return nil, err
	}

	pk := pkix.Name{
		CommonName: config.Name,
		Locality:   []string{listener.Addr().String()},
	}

	cu, err := comm.NewCu(pk, config.LohpiCaAddress)
	if err != nil {
		return nil, err
	}

	directoryServerClient, err := comm.NewDirectoryServerGRPCClient(cu.Certificate(), cu.CaCertificate(), cu.Priv())
	if err != nil {
		return nil, err
	}

	psClient, err := comm.NewPolicyStoreClient(cu.Certificate(), cu.CaCertificate(), cu.Priv())
	if err != nil {
		return nil, err
	}

	// Observers and logs policies as they arrive at the node
	policyObs, err := newPolicyObserver(&policyObserverConfig{
		outputDirectory: config.PolicyObserverWorkingDirectory,
		logfilePrefix: config.Name,
		capacity: 10,
	})
	if err != nil {
		return nil, err
	}

	node := &NodeCore{
		ifritClient:  			  ifritClient,
		directoryServerClient:    directoryServerClient,
		conf:		   			  config,
		psClient:     			  psClient,
		listener: 				  listener,
		cu:           			  cu,
		datasetMap:     		  make(map[string]*pb.Policy),
		datasetMapLock: 		  sync.RWMutex{},
		policyObs: 	  			  policyObs,
	}

	// Initialize the PostgresSQL database
	// TODO: remove me and config me somewhere else :)
	/*if err := node.initializePostgreSQLdb(config.PostgresSQLConnectionString); err != nil {
		panic(err)
		return nil, err
	}*/

	// Set the Ifrit handlers
	node.ifritClient.RegisterMsgHandler(node.messageHandler)
	node.ifritClient.RegisterGossipHandler(node.gossipHandler)

	return node, nil
}

func (n *NodeCore) SetConfig(c *Config) {
	n.configLock.Lock()
	defer n.configLock.Unlock()
	n.conf = c
}

// TODO: restore checkouts from previous runs so that we don't lose information from memory.
// Especially for checkout datasets 
func (n *NodeCore) IfritClient() *ifrit.Client {
	return n.ifritClient
}

// RequestPolicy requests policies from policy store that are assigned to the dataset given by the id.
// It will also populate the node's database with the available identifiers and assoicate them with policies.
// You will have to call this method to make the datasets available to the clients. 
func (n *NodeCore) IndexDataset(datasetId string) error {
	if n.datasetExists(datasetId) {
		return fmt.Errorf("Dataset with identifier '%s' is already indexed by the server", datasetId)
	}

	// Send policy request to policy store
	policyResponse, err := n.pbSendPolicyStorePolicyRequest(datasetId, n.policyStoreIP)
	if err != nil {
		return err
	}
	
	// Apply the update from policy store to storage
	if err := n.dbSetObjectPolicy(datasetId, policyResponse.GetContent()); err != nil {
		return err
	}

	// Notify the directory server of the newest policy update
	if err := n.pbSendDatsetIdentifier(datasetId, n.directoryServerIP); err != nil {
		return err
	}

	n.insertDataset(datasetId, policyResponse)

	return nil
}

// Removes the dataset policy from the node. The dataset will no longer be available to clients.
func (n *NodeCore) RemoveDataset(id string) error {
	return nil
}

// Shuts down the node
func (n *NodeCore) Shutdown() {
	log.Infoln("Shutting down Lohpi node")
	n.ifritClient.Stop()
	// TODO more here
}

func (n *NodeCore) HandshakeDirectoryServer() error {
	log.Infof("Performing handshake with directory server at address %s:%s\n", n.config().DirectoryServerAddress)
	conn, err := n.directoryServerClient.Dial(n.config().DirectoryServerAddress)
	if err != nil {
		return err
	}

	defer conn.CloseConn()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()

	r, err := conn.Handshake(ctx, n.PbNode())
	if err != nil {
		return err
	}

	n.directoryServerIP = r.GetIp()
	n.directoryServerID = r.GetId()
	return nil
}

// TODO: set proper config here :))
func (n *NodeCore) StartHTTPServer(port int) {
	go n.startHTTPServer(fmt.Sprintf(":%d", port))
}

func (n *NodeCore) IfritAddress() string {
	return n.ifritClient.Addr()
}

func (n *NodeCore) Name() string {
	return n.config().Name
}

func (n *NodeCore) HandshakePolicyStore() error {
	log.Infof("Performing handshake with policy store at address %s:%s\n", n.config().PolicyStoreAddress)
	conn, err := n.psClient.Dial(n.config().PolicyStoreAddress)
	if err != nil {
		return err
	}
	defer conn.CloseConn()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()

	r, err := conn.Handshake(ctx, n.PbNode())
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
	id := msg.GetStringValue()
	if err := n.dbCheckinDataset(id); err != nil {
		log.Errorln(err.Error())
	}
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
	datasetId := p.GetObjectIdentifier()
	if n.dbDatasetExists(datasetId) {
		currentPolicy := n.getDatasetEntry(datasetId)
		if currentPolicy == nil {
			return errors.New("Tried to get current policy but it was nil")
		}

		if currentPolicy.GetVersion() >= p.GetVersion() {
			log.Infoln("Got an outdated policy. Applies a new version")
			return nil
		}

		if err := n.dbSetObjectPolicy(datasetId, p.GetContent()); err != nil {
			return err
		}

		n.insertDataset(datasetId, p)

		// Log deltas in policies to logfile
		// TODO: put loggers in interfaces?
		log.Infof("Changed dataset %s's policy to %s\n", datasetId, p.GetContent())

		// Update revocation list. Optimization: don't push update message if policy is reduntant.
		if n.dbDatasetIsCheckedOutByClient(datasetId) {
			if err := n.pbSendDatasetRevocationUpdate(datasetId, p.GetContent()); err != nil {
				return err
			}
		}
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

	return n.dbDatasetIsAvailable(r.GetIdentifier())
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
		if !n.policyObs.gossipIsObserved(msg.GetGossipMessage()) {
			n.policyObs.addObservedGossip(msg.GetGossipMessage())
			return n.processPolicyBatch(msg)
		}
		
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
		Sender: n.PbNode(),
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

func (n *NodeCore) PbNode() *pb.Node {
	return &pb.Node{
		Name:         	n.config().Name,
		IfritAddress: 	n.ifritClient.Addr(),
		Role:         	"Storage node",
		Id:           	[]byte(n.ifritClient.Id()),
		HttpsAddress:  	n.config().HostName,
		Port: 			int32(n.config().HTTPPort),
	}
}

func (n *NodeCore) verifyMessageSignature(msg *pb.Message) error {
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

func (n *NodeCore) insertDataset(id string, p *pb.Policy) {
	n.datasetMapLock.Lock()
	defer n.datasetMapLock.Unlock()
	n.datasetMap[id] = p
}

func (n *NodeCore) datasetExists(id string) bool {
	n.datasetMapLock.RLock()
	defer n.datasetMapLock.RUnlock()
	_, ok := n.datasetMap[id]
	return ok
}

func (n *NodeCore) removeDataset(id string) {
	n.datasetMapLock.Lock()
	defer n.datasetMapLock.Unlock()
	delete(n.datasetMap, id)
}

func (n *NodeCore) getDatasetMap() map[string]*pb.Policy {
	n.datasetMapLock.RLock()
	defer n.datasetMapLock.RUnlock()
	return n.datasetMap
}

func (n *NodeCore) getDatasetEntry(id string) *pb.Policy {
	n.datasetMapLock.RLock()
	defer n.datasetMapLock.RUnlock()
	return n.datasetMap[id]
}

func (n *NodeCore) datasetIdentifiers() []string {
	ids := make([]string, 0)
	m := n.getDatasetMap()
	for id := range m {
		ids = append(ids, id)
	}
	return ids
}

// Use this to refer to configuration variables
func (n *NodeCore) config() *Config {
	n.configLock.RLock()
	defer n.configLock.RUnlock()
	return n.conf
}