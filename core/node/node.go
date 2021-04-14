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
	"os"
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
}

// TODO move me somewhere else. ref gossip.go
type gossipMessage struct {
	Content [][]byte
	Hash    []byte
	Addr    string
}

// Normally used to transfer metadata from external sources
type ExternalMetadata struct {
	URL string
}

// Used to describe an external archive
type ExternalDataset struct {
	URL string
}

type ObjectPolicy struct {
	Attribute string
	Value     string
}

// Used to configure the time intervals between each fetching of the identifiers of the dataset.
// You really should use this config to enable a push-based approach.
type RefreshConfig struct {
	RefreshInterval time.Duration
	URL             string
}

// Policy assoicated with metadata
type Policy struct {
	Issuer           string
	ObjectIdentifier string
	Content          string
}

type NodeCore struct {
	// Underlying ifrit client
	ifritClient *ifrit.Client

	// Crypto unit
	cu *comm.CryptoUnit

	// Configuration 
	config *Config

	// gRPC client towards the Mux
	muxClient *comm.DirectoryGRPCClient

	// Policy store
	psClient *comm.PolicyStoreGRPCClient

	// Used for identifying data coming from policy store
	directoryServerID []byte
	policyStoreID []byte

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

	// Config for fetching data from remote source. Remove me?
	refreshConfigLock sync.RWMutex
	refreshConfig     RefreshConfig

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

	muxClient, err := comm.NewMuxGRPCClient(cu.Certificate(), cu.CaCertificate(), cu.Priv())
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
		config:       config,
		psClient:     psClient,
		httpListener: httpListener,
		cu:           cu,

		// TODO: revise me
		datasetMap:     make(map[string]struct{}),
		datasetMapLock: sync.RWMutex{},
	}

	// Initialize the PostgresSQL database
	if err := node.initializePostgreSQLdb(config.PostgresSQLConnectionString); err != nil {
		return nil, err
	}

	// Remove all stale identifiers since the last run. This will remove all the identifiers
	// from the table. The node cannot host datasets that have been removed from the remote location
	// since it last ran.
	// TODO: can we do something better?
	if err := node.dbResetDatasetIdentifiers(); err != nil {
		log.Errorf(err.Error())
		return nil, err
	}

	// MAJOR TODO: refine large parts of the system by moving low-level message passing logic to low-level modules.
	// It should be clear which parts of the system is talking!
	 
	return node, nil
}

// TODO: restore checkouts from previous runs so that we don't lose information from memory.
// Especially for checkout datasets 
func (n *NodeCore) IfritClient() *ifrit.Client {
	return n.ifritClient
}

func (n *NodeCore) InitializeLogfile(logToFile bool) error {
	logfilePath := "node.log"

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

// RequestPolicy requests policies from policy store that are assigned to the dataset given by the id.
// It will also populate the node's database with the available identifiers and assoicate them with policies.
// All policies have a default value of nil, which leads to client requests being rejected. You must call this method to
// make the dataset available to the clients by fetching the latest dataset. The call will block until a context timeout or
// the policy is applied to the dataset. Dataset identifiers that have not been passed to this method will not
// be available to clients.
func (n *NodeCore) IndexDataset(id string, ctx context.Context) error {
	//TODO: handle context
	if id == "" {
		return errors.New("Dataset identifier must be a non-empty string")
	}

	if n.datasetExists(id) {
		return fmt.Errorf("Dataset with identifier '%s' already exists", id)
	}

	// Send policy request to policy store
	// ctx too...
	if err := n.pbRequestPolicy(id); err != nil {
		log.Errorln(err.Error())
		return err
	}

	// Add notify mux of the newest policy update
	if err := n.sendDatsetIdentifier(id, n.config.DirectoryServerAddress); err != nil {
		log.Errorln(err.Error())
		return err
	}

	return nil
}

// Requests the newest policy from the policy store
func (n *NodeCore) pbRequestPolicy(id string) error {
	msg := &pb.Message{
		Type:   message.MSG_TYPE_GET_DATASET_POLICY,
		Sender: n.pbNode(),
		PolicyRequest: &pb.PolicyRequest{
			Identifier: id,
		},
	}

	data, err := proto.Marshal(msg)
	if err != nil {
		panic(err)
		log.Errorln(err.Error())
		return err
	}

	r, s, err := n.ifritClient.Sign(data)
	if err != nil {
		panic(err)
		log.Errorln(err.Error())
		return err
	}

	msg.Signature = &pb.MsgSignature{R: r, S: s}
	data, err = proto.Marshal(msg)
	if err != nil {
		panic(err)
		log.Errorln(err.Error())
		return err
	}

	// TODO: use context with timeout :)
	ch := n.ifritClient.SendTo(n.config.PolicyStoreAddress, data)

	select {
	case resp := <-ch:
		respMsg := &pb.Message{}
		if err := proto.Unmarshal(resp, respMsg); err != nil {
			log.Errorln(err.Error())
			return err
		}

		if err := n.verifyMessageSignature(respMsg); err != nil {
			panic(err)
			log.Errorln(err.Error())
			return err
		}

		// Insert into database
		pResponse := respMsg.GetPolicyResponse()
		if err := n.dbSetObjectPolicy(id, pResponse.GetObjectPolicy().GetContent()); err != nil {
			log.Errorln(err.Error())
			return err
		}

		// Insert into map too if all went well in the database transaction
		// TODO: store other things in the struct? More complex policies?
		n.insertDataset(id, struct{}{})
	}
	return nil
}

func (n *NodeCore) sendDatsetIdentifier(id, recipient string) error {
	msg := &pb.Message{
		Type:        message.MSG_TYPE_ADD_DATASET_IDENTIFIER,
		Sender:      n.pbNode(),
		StringValue: id,
	}

	data, err := proto.Marshal(msg)
	if err != nil {
		panic(err)
		log.Errorln(err.Error())
		return err
	}

	r, s, err := n.ifritClient.Sign(data)
	if err != nil {
		panic(err)
		log.Errorln(err.Error())
		return err
	}

	msg.Signature = &pb.MsgSignature{R: r, S: s}
	data, err = proto.Marshal(msg)
	if err != nil {
		panic(err)
		log.Errorln(err.Error())
		return err
	}

	// TODO: use context with timeout :)
	// No need to do anything more. The mux doesn't respond to messages
	n.ifritClient.SendTo(recipient, data)
	return nil
}

// Removes the dataset policy from the node. The dataset will no longer be available to clients.
func (n *NodeCore) RemoveDataset(id string) error {
	return nil
}

// Shuts down the node
func (n *NodeCore) Shutdown() {
	log.Printf("Shutting down Lohpi node\n")
	n.ifritClient.Stop()
	//	fuse.Shutdown() // might fail...
}

// Joins the network by starting the underlying Ifrit node. Further, it performs handshakes
// with the policy store and multiplexer at known addresses.
func (n *NodeCore) JoinNetwork() error {
	if err := n.directoryServerHandshake(); err != nil {
		panic(err)
		return err
	}

	if err := n.policyStoreHandshake(); err != nil {
		panic(err)
		return err
	}

	go n.startHttpServer(fmt.Sprintf(":%d", n.config.HTTPPort))

	n.ifritClient.RegisterMsgHandler(n.messageHandler)
	n.ifritClient.RegisterGossipHandler(n.gossipHandler)
	//	n.ifritClient.RegisterStreamHandler(n.streamHandler)

	return nil
}

func (n *NodeCore) IfritAddress() string {
	return n.ifritClient.Addr()
}

func (n *NodeCore) HTTPAddress() string {
	return ""
}

func (n *NodeCore) NodeName() string {
	return n.config.Name
}

// Function type used to fetch compressed archives from external sources.
type externalArchiveHandler = func(id string) (string, error)

// Function type to fetch metadata from the external data source
type externalMetadataHandler = func(id string) (string, error)

// PRIVATE METHODS BELOW
func (n *NodeCore) directoryServerHandshake() error {
	log.Infof("Performing handshake with directory server at address %s:%s\n", n.config.DirectoryServerAddress, strconv.Itoa(n.config.DirectoryServerGPRCPort))
	conn, err := n.muxClient.Dial(n.config.DirectoryServerAddress + ":" + strconv.Itoa(n.config.DirectoryServerGPRCPort))
	if err != nil {
		return err
	}

	defer conn.CloseConn()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()

	r, err := conn.Handshake(ctx, &pb.Node{
		Name:         n.config.Name,
		IfritAddress: n.ifritClient.Addr(),
		Role:         "Storage node",
		Id:           []byte(n.ifritClient.Id()),
	})
	if err != nil {
		panic(err)
		log.Fatalln(err)
		return err
	}

	n.directoryServerIP = r.GetIp()
	n.directoryServerID = r.GetId()
	return nil
}

func (n *NodeCore) policyStoreHandshake() error {
	log.Infof("Performing handshake with policy store at address %s:%s\n", n.config.PolicyStoreAddress, n.config.PolicyStoreGRPCPport)
	conn, err := n.psClient.Dial(n.config.PolicyStoreAddress + ":" + strconv.Itoa(n.config.PolicyStoreGRPCPport))
	if err != nil {
		return err
	}
	defer conn.CloseConn()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()

	r, err := conn.Handshake(ctx, &pb.Node{
		Name:         n.config.Name,
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
		panic(err)
	}

	log.Infof("Node '%s' got message %s\n", n.config.Name, msg.GetType())
	if err := n.verifyMessageSignature(msg); err != nil {
		panic(err)
		log.Errorln(err)
		return nil, err
	}

	switch msgType := msg.Type; msgType {
	case message.MSG_TYPE_GET_NODE_INFO:
		return n.nodeInfo()

	case message.MSG_TYPE_GET_DATASET_IDENTIFIERS:
		return n.pbDatasetIdentifiers(msg)

		// Only allow one check-out at a time by the same client of the same dataset.
		// Respond with "Unauthorized" if something fails. We might disable this feature
		// based on certain criterias.
	case message.MSG_TYPE_GET_DATASET_URL:
		// Check if dataset is indexed by the policy enforcement. If it is not, reject the request (401).
		if !n.dbDatasetExists(msg.GetDatasetRequest().GetIdentifier()) {
			return n.unauthorizedAccess(fmt.Sprintf("Dataset '%s' is not indexed by the node", msg.GetDatasetRequest().GetIdentifier()))
		}
		/*if n.isCheckedOutByClient(msg.GetDatasetRequest()) {
			return n.unauthorizedAccess("Client has already checked out this dataset")
		}*/

		if n.clientIsAllowed(msg.GetDatasetRequest()) {
			if err := n.dbCheckoutDataset(msg.GetDatasetRequest()); err != nil {
				log.Errorln(err.Error())
				return nil, err
			}
			return n.fetchDatasetURL(msg)
		} else {
			return n.unauthorizedAccess("Client has invalid access attributes")
		}

	//	case message.MSG_TYPE_GET_DATASET:
	//	return n.marshalledStorageObjectContent(msg)s

	case message.MSG_TYPE_DATASET_EXISTS:
		return n.pbDatasetExists(msg)

	case message.MSG_TYPE_GET_DATASET_METADATA_URL:
		return n.fetchDatasetMetadataURL(msg)
		// TODO finish me

	case message.MSG_TYPE_POLICY_STORE_UPDATE:
		// gossip
		log.Infoln("Got new policy batch from policy store")
		return n.processPolicyBatch(msg)

	case message.MSG_TYPE_ROLLBACK_CHECKOUT:
		n.rollbackCheckout(msg)

	case message.MSG_TYPE_PROBE:
		return n.acknowledgeProbe(msg, data)

	default:
		fmt.Printf("Unknown message type: %s\n", msg.GetType())
	}

	return n.acknowledgeMessage()
}

func (n *NodeCore) rollbackCheckout(msg *pb.Message) {
	id := msg.GetStringValue()
	if err := n.dbCheckinDataset(id); err != nil {
		log.Errorln(err.Error())
	}
}

// TODO check the internal tables and so on...
func (n *NodeCore) processPolicyBatch(msg *pb.Message) ([]byte, error) {
	if msg.GetGossipMessage() == nil {
		err := errors.New("Gossip message is nil")
		log.Fatalln(err.Error())
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
			if n.datasetExists(datasetId) {
				if err := n.dbSetObjectPolicy(datasetId, policy.GetContent()); err != nil {
					log.Errorln(err.Error())
					return nil, err
				}

				if n.isCheckedOutByClient(datasetId) {
					n.notifyPolicyChangeToDirectoryServer(datasetId)
				}
			}
		}
	}
	return nil, nil
}

// Only disallow permissions for now
func (n *NodeCore) notifyPolicyChangeToDirectoryServer(dataset string) {
	msg := &pb.Message{
		Type:        message.MSG_POLICY_REVOKE,
		Sender:      n.pbNode(),
		StringValue: dataset,
		BoolValue:   false,
	}

	data, err := proto.Marshal(msg)
	if err != nil {
		panic(err)
	}

	r, s, err := n.ifritClient.Sign(data)
	if err != nil {
		log.Errorln(err.Error())
		panic(err)
	}

	msg.Signature = &pb.MsgSignature{R: r, S: s}
	data, err = proto.Marshal(msg)
	if err != nil {
		log.Errorln(err.Error())
		panic(err)
	}

	n.ifritClient.SendTo(n.directoryServerIP, data)
}

// Returns true if a client has already checked out the dataset,
// returns false otherwise.
func (n *NodeCore) isCheckedOutByClient(dataset string) bool {
	if dataset == "" {
		log.Errorln("Dataset identifier is empty")
		return true
	}

	return n.dbDatasetIsCheckedOutByClient(dataset)
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

// Returns a message notifying the recipient that the data request was unauthorized.
func (n *NodeCore) unauthorizedAccess(errorMsg string) ([]byte, error) {
	respMsg := &pb.Message{
		DatasetResponse: &pb.DatasetResponse{
			IsAllowed:    false,
			ErrorMessage: errorMsg,
		},
	}

	data, err := proto.Marshal(respMsg)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	r, s, err := n.ifritClient.Sign(data)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	respMsg.Signature = &pb.MsgSignature{R: r, S: s}
	return proto.Marshal(respMsg)
}

// TODO: separate internal and external data sources
func (n *NodeCore) fetchDatasetURL(msg *pb.Message) ([]byte, error) {
	id := msg.GetDatasetRequest().GetIdentifier()
	if handler := n.getDatasetCallback(); handler != nil {
		externalArchiveUrl, err := handler(id)
		if err != nil {
			log.Errorln(err.Error())
			return nil, err
		}

		respMsg := &pb.Message{
			DatasetResponse: &pb.DatasetResponse{
				URL:       externalArchiveUrl,
				IsAllowed: true,
			},
		}

		data, err := proto.Marshal(respMsg)
		if err != nil {
			log.Errorln(err.Error())
			return nil, err
		}

		r, s, err := n.ifritClient.Sign(data)
		if err != nil {
			log.Errorln(err.Error())
			return nil, err
		}

		respMsg.Signature = &pb.MsgSignature{R: r, S: s}
		return proto.Marshal(respMsg)
	} else {
		log.Println("Dataset archive handler not registered")
	}

	return nil, nil
}

// TODO: consider abandoning fetchting the metadata url at each request and index it at the node instead
// Use psql or in-memory map?
func (n *NodeCore) fetchDatasetMetadataURL(msg *pb.Message) ([]byte, error) {
	id := msg.GetDatasetRequest().GetIdentifier()
	if handler := n.getExternalMetadataHandler(); handler != nil {
		metadataUrl, err := handler(id)
		if err != nil {
			log.Errorln(err.Error())
			return nil, err
		}

		respMsg := &pb.Message{
			StringValue: metadataUrl,
			Sender:      n.pbNode(),
		}

		data, err := proto.Marshal(respMsg)
		if err != nil {
			log.Errorln(err.Error())
			return nil, err
		}

		r, s, err := n.ifritClient.Sign(data)
		if err != nil {
			log.Errorln(err.Error())
			return nil, err

		}

		respMsg.Signature = &pb.MsgSignature{R: r, S: s}
		return proto.Marshal(respMsg)
	} else {
		log.Warnln("ExternalMetadataHandler is not set")
	}

	return nil, nil
}

func (n *NodeCore) pbDatasetExists(msg *pb.Message) ([]byte, error) {
	respMsg := &pb.Message{}
	respMsg.BoolValue = n.datasetExists(msg.GetStringValue())

	data, err := proto.Marshal(respMsg)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	r, s, err := n.ifritClient.Sign(data)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}
	respMsg.Signature = &pb.MsgSignature{R: r, S: s}
	return proto.Marshal(respMsg)
}

// TODO: create protobuf/ifrit related functions their own notation :))
func (n *NodeCore) pbDatasetIdentifiers(msg *pb.Message) ([]byte, error) {
	respMsg := &pb.Message{
		StringSlice: n.datasetIdentifiers(),
	}

	data, err := proto.Marshal(respMsg)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	r, s, err := n.ifritClient.Sign(data)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	respMsg.Signature = &pb.MsgSignature{R: r, S: s}
	return proto.Marshal(respMsg)
}

func (n *NodeCore) acknowledgeMessage() ([]byte, error) {
	msg := &pb.Message{Type: message.MSG_TYPE_OK}
	data, err := proto.Marshal(msg)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	r, s, err := n.ifritClient.Sign(data)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	msg.Signature = &pb.MsgSignature{R: r, S: s}
	return proto.Marshal(msg)
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

func (n *NodeCore) nodeInfo() ([]byte, error) {
	str := fmt.Sprintf("Name: %s\tIfrit address: %s", n.config.Name, n.IfritClient().Addr())
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
			Name:         n.config.Name,
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
		log.Errorln(err.Error())
		return nil, err
	}

	msg.Signature = &pb.MsgSignature{R: r, S: s}
	data, err = proto.Marshal(msg)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	log.Println(n.config.Name, "sending ack to Policy store")
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

