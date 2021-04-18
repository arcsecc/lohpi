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
	"strconv"
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
	HTTPPort                	int
	PolicyStoreAddress   		string
	PolicyStoreGRPCPport 		int
	DirectoryServerAddress  	string
	DirectoryServerGPRCPort 	int
	LohpiCaAddress 				string
	LohpiCaPort					int
	Name 						string
	PostgresSQLConnectionString string
	DatabaseRetentionInterval	time.Duration
	AllowMultipleCheckouts 		bool
	DebugEnabled 				bool
	TLSEnabled					bool
}

// TODO move me somewhere else. ref gossip.go
type gossipMessage struct {
	Content [][]byte
	Hash    []byte
	Addr    string
}

type ObjectPolicy struct {
	Attribute string
	Value     string
}

// Policy assoicated with metadata
type Policy struct {
	Issuer           string
	ObjectIdentifier string
	Content          string
}

// Function type used to fetch compressed archives from external sources.
type externalArchiveHandler = func(id string) (string, error)

// Function type to fetch metadata from the external data source
type externalMetadataHandler = func(id string) (string, error)

type NodeCore struct {
	// Underlying ifrit client
	ifritClient *ifrit.Client

	// Crypto unit
	cu *comm.CryptoUnit

	// Configuration 
	conf *Config
	configLock sync.RWMutex

	// gRPC client towards the Mux
	muxClient *comm.DirectoryGRPCClient

	// Policy store
	psClient *comm.PolicyStoreGRPCClient

	// Used for identifying data coming from policy store
	directoryServerID []byte
	policyStoreID []byte	// TODO: allow for any number of policy store instances :)

	directoryServerIP string
	policyStoreIP string

	// Object id -> empty value for quick indexing
	datasetMap     map[string]struct{}
	datasetMapLock sync.RWMutex

	httpListener net.Listener
	httpServer   *http.Server

	// Callback to fetch remote archives
	datasetCallback     externalArchiveHandler
	datasetCallbackLock sync.RWMutex

	// Callback to fetch external metadata
	externalMetadataCallback     externalMetadataHandler
	externalMetadataCallbackLock sync.RWMutex

	// Azure database
	datasetCheckoutDB *sql.DB
	datasetPolicyDB   *sql.DB
}

func NewNodeCore(config *Config) (*NodeCore, error) {
	ifritClient, err := ifrit.NewClient()
	if err != nil {
		return nil, err
	}

	go ifritClient.Start()

	httpListener, err := netutil.GetListener()
	if err != nil {
		return nil, err
	}

	pk := pkix.Name{
		CommonName: config.Name,
		Locality:   []string{httpListener.Addr().String()},
	}

	cu, err := comm.NewCu(pk, config.LohpiCaAddress + ":" + strconv.Itoa(config.LohpiCaPort))
	if err != nil {
		return nil, err
	}

	muxClient, err := comm.NewDirectoryServerGRPCClient(cu.Certificate(), cu.CaCertificate(), cu.Priv())
	if err != nil {
		return nil, err
	}

	psClient, err := comm.NewPolicyStoreClient(cu.Certificate(), cu.CaCertificate(), cu.Priv())
	if err != nil {
		return nil, err
	}

	node := &NodeCore{
		ifritClient:  ifritClient,
		muxClient:    muxClient,
		conf:       config,
		psClient:     psClient,
		httpListener: httpListener,
		cu:           cu,

		// TODO: revise me
		datasetMap:     make(map[string]struct{}),
		datasetMapLock: sync.RWMutex{},
	}

	// Initialize the PostgresSQL database
	if err := node.initializePostgreSQLdb(config.PostgresSQLConnectionString); err != nil {
		panic(err)
		return nil, err
	}

	// Remove all stale identifiers since the last run. This will remove all the identifiers
	// from the table. The node cannot host datasets that have been removed from the remote location
	// since it last ran.
	// TODO: can we do something better?
	if err := node.dbResetDatasetIdentifiers(); err != nil {
		log.Errorf(err.Error())
		panic(err)
		return nil, err
	}

	// MAJOR TODO: refine large parts of the system by moving low-level message passing logic to low-level modules.
	// It should be clear which parts of the system is talking!
	 
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
func (n *NodeCore) IndexDataset(id string, ctx context.Context) error {
	if n.datasetExists(id) {
		return fmt.Errorf("Dataset with identifier '%s' is already indexed by the node", id)
	}

	// Send policy request to policy store
	policyResponse, err := n.pbSendPolicyStorePolicyRequest(id, n.policyStoreIP)
	if err != nil {
		return err
	}

	//n.insertDataset(datasetId, struct{}{})
	
	// Apply the update from policy store to storage
	if err := n.dbSetObjectPolicy(id, policyResponse.GetContent()); err != nil {
		return err
	}

	// Notify the directory server of the newest policy update
	if err := n.pbSendDatsetIdentifier(id, n.directoryServerIP); err != nil {
		return err
	}

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
}

// Joins the network by starting the underlying Ifrit node. Further, it performs handshakes
// with the policy store and multiplexer at known addresses.
func (n *NodeCore) JoinNetwork() error {
	if err := n.directoryServerHandshake(); err != nil {
		return err
	}

	if err := n.policyStoreHandshake(); err != nil {
		return err
	}

	go n.startHttpServer(fmt.Sprintf(":%d", n.config().HTTPPort))

	n.ifritClient.RegisterMsgHandler(n.messageHandler)
	n.ifritClient.RegisterGossipHandler(n.gossipHandler)

	return nil
}

func (n *NodeCore) IfritAddress() string {
	return n.ifritClient.Addr()
}

func (n *NodeCore) HTTPAddress() string {
	return ""
}

func (n *NodeCore) NodeName() string {
	return n.config().Name
}

// PRIVATE METHODS BELOW
func (n *NodeCore) directoryServerHandshake() error {
	log.Infof("Performing handshake with directory server at address %s:%s\n", n.config().DirectoryServerAddress, strconv.Itoa(n.config().DirectoryServerGPRCPort))
	conn, err := n.muxClient.Dial(n.config().DirectoryServerAddress + ":" + strconv.Itoa(n.config().DirectoryServerGPRCPort))
	if err != nil {
		return err
	}

	defer conn.CloseConn()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()

	r, err := conn.Handshake(ctx, &pb.Node{
		Name:         n.config().Name,
		IfritAddress: n.ifritClient.Addr(),
		Role:         "Storage node",
		Id:           []byte(n.ifritClient.Id()),
	})
	if err != nil {
		return err
	}

	n.directoryServerIP = r.GetIp()
	n.directoryServerID = r.GetId()
	return nil
}

func (n *NodeCore) policyStoreHandshake() error {
	log.Infof("Performing handshake with policy store at address %s:%s\n", n.config().PolicyStoreAddress, n.config().PolicyStoreGRPCPport)
	conn, err := n.psClient.Dial(n.config().PolicyStoreAddress + ":" + strconv.Itoa(n.config().PolicyStoreGRPCPport))
	if err != nil {
		return err
	}
	defer conn.CloseConn()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()

	r, err := conn.Handshake(ctx, &pb.Node{
		Name:         n.config().Name,
		IfritAddress: n.ifritClient.Addr(),
		Role:         "Storage node",
		Id:           []byte(n.ifritClient.Id()),
	})
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
		panic(err)
		return nil, err
	}

	log.Infof("Node '%s' got message %s\n", n.config().Name, msg.GetType())
	if err := n.verifyMessageSignature(msg); err != nil {
		log.Errorln(err)
		panic(err)
		return nil, err
	}

	switch msgType := msg.Type; msgType {
	case message.MSG_TYPE_GET_NODE_INFO:
		return n.nodeInfo()

	case message.MSG_TYPE_GET_DATASET_IDENTIFIERS:
		return n.pbMarshalDatasetIdentifiers(msg)

	case message.MSG_TYPE_GET_DATASET_URL:
		return n.pbMarshalDatasetURL(msg)
		
	case message.MSG_TYPE_GET_DATASET_METADATA_URL:
		return n.pbMarshalDatasetMetadataURL(msg)

	case message.MSG_TYPE_POLICY_STORE_UPDATE:
		n.processPolicyBatch(msg)

	case message.MSG_TYPE_ROLLBACK_CHECKOUT:
		n.rollbackCheckout(msg)

	case message.MSG_TYPE_PROBE:
		return n.acknowledgeProbe(msg, data)

	default:
		fmt.Printf("Unknown message type: %s\n", msg.GetType())
	}

	return n.pbMarshalAcknowledgeMessage()
}

func (n *NodeCore) rollbackCheckout(msg *pb.Message) {
	id := msg.GetStringValue()
	if err := n.dbCheckinDataset(id); err != nil {
		log.Errorln(err.Error())
	}
}

// TODO refine this. Check all gossip batches and see which one we should apply
func (n *NodeCore) processPolicyBatch(msg *pb.Message) ([]byte, error) {
	if msg.GetGossipMessage() == nil {
		err := errors.New("Gossip message is nil")
		log.Errorln(err.Error())
		return nil, err
	}

	gspMsg := msg.GetGossipMessage()
	if gspMsg.GetGossipMessageBody() == nil {
		err := errors.New("Gossip message body is nil")
		log.Fatalln(err.Error())
		return nil, err
	} else {
		for _, m := range gspMsg.GetGossipMessageBody() {
			// TODO check policy ID and so on...
			policy := m.GetPolicy()
			datasetId := policy.GetObjectIdentifier()
			if n.dbDatasetExists(datasetId) {
				log.Println("datasetId, policy.GetContent():", datasetId, policy.GetContent())
				if err := n.dbSetObjectPolicy(datasetId, policy.GetContent()); err != nil {
					log.Errorln(err.Error())
					continue
				}

				// Withdraw consent if a more restrictive policy is issued
				if n.dbDatasetIsCheckedOutByClient(datasetId) {
					log.Println("Sending revocation")
					if err := n.pbSendDatasetRevocationUpdate(datasetId, policy.GetContent()); err != nil {
						log.Errorln(err.Error())
					}
				}
			}
		}
	}
	return nil, nil
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

func (n *NodeCore) gossipHandler(data []byte) ([]byte, error) {
	msg := &pb.Message{}
	if err := proto.Unmarshal(data, msg); err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	if err := n.verifyPolicyStoreMessage(msg); err != nil {
		log.Warnln(err.Error())
	}

	switch msgType := msg.Type; msgType {
	case message.MSG_TYPE_PROBE:
		//n.ifritClient.SetGossipContent(data)
	case message.MSG_TYPE_POLICY_STORE_UPDATE:
		/*log.Println("Got new policy from policy store!")
		n.setPolicy(msg)*/

	default:
		fmt.Printf("Unknown gossip message type: %s\n", msg.GetType())
	}

	/*
		check recipients
		check version number
		check object (subject or study). Apply the policy if needed
		store the message on disk
	*/
	return nil, nil
}

// Returns the URL of the dataset
func (n *NodeCore) fetchDatasetURL(id string) (string, error) {
	if handler := n.getDatasetCallback(); handler != nil {
		externalArchiveUrl, err := handler(id)
		if err != nil {
			return "", err
		}

		return externalArchiveUrl, nil
	}
	
	log.Warnln("Handler in fetchDatasetURL not set")
	return "", nil
}

// Fetches the URL of the external metadata, given by the dataset id
func (n *NodeCore) fetchDatasetMetadataURL(id string) (string, error) {
	if handler := n.getExternalMetadataHandler(); handler != nil {
		metadataUrl, err := handler(id)
		if err != nil {
			return "", err
		}

		return metadataUrl, nil
	}
	
	log.Warnln("Handler in fetchDatasetMetadataURL is not set")
	return "", nil
}

func (n *NodeCore) nodeInfo() ([]byte, error) {
	str := fmt.Sprintf("Name: %s\tIfrit address: %s", n.config().Name, n.IfritClient().Addr())
	return []byte(str), nil
}

// Acknowledges the given probe message.  TODO MORE HERE
func (n *NodeCore) acknowledgeProbe(msg *pb.Message, d []byte) ([]byte, error) {
	if err := n.verifyPolicyStoreMessage(msg); err != nil {
		return []byte{}, err
	}

	// Spread the probe message onwards through the network
	gossipContent, err := proto.Marshal(msg)
	if err != nil {
		return []byte{}, err
	}

	n.ifritClient.SetGossipContent(gossipContent)

	// Acknowledge the probe message
	resp := &pb.Message{
		Type: message.MSG_TYPE_PROBE_ACK,
		Sender: &pb.Node{
			Name:         n.config().Name,
			IfritAddress: n.ifritClient.Addr(),
			Role:         "Storage node",
			Id:           []byte(n.ifritClient.Id()),
		},
		Probe: msg.GetProbe(),
	}

	data, err := proto.Marshal(resp)
	if err != nil {
		return []byte{}, err
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

// TODO remove me and use pbNode in ps gossip message instead
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

func (n *NodeCore) pbNode() *pb.Node {
	return &pb.Node{
		Name:         n.NodeName(),
		IfritAddress: n.ifritClient.Addr(),
		HttpAddress:  n.httpListener.Addr().String(),
		Role:         "storage node",
		//ContactEmail
		Id: []byte(n.ifritClient.Id()),
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

func (n *NodeCore) insertDataset(id string, elem struct{}) {
	n.datasetMapLock.Lock()
	defer n.datasetMapLock.Unlock()
	n.datasetMap[id] = struct{}{}
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

func (n *NodeCore) getDatasetMap() map[string]struct{} {
	n.datasetMapLock.RLock()
	defer n.datasetMapLock.RUnlock()
	return n.datasetMap
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