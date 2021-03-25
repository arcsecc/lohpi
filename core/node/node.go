package node

import (
	"context"
	"crypto/x509/pkix"
	"database/sql"
	"fmt"
	"github.com/arcsecc/lohpi/core/comm"
	"github.com/arcsecc/lohpi/core/keyvault"
	"github.com/arcsecc/lohpi/core/message"
	"github.com/arcsecc/lohpi/core/netutil"
	pb "github.com/arcsecc/lohpi/protobuf"
	"github.com/golang/protobuf/proto"
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
	Port                 int    `default:"8090"`
	PolicyStoreAddr      string `default:"127.0.1.1:8084"`
	MuxAddr              string `default:"127.0.1.1:8081"`
	LohpiCaAddr          string `default:"127.0.1.1:8301"`
	AzureKeyVaultName    string `required:true`
	AzureKeyVaultSecret  string `required:true`
	AzureClientSecret    string `required:true`
	AzureClientId        string `required:true`
	AzureTenantId        string `required:true`
	AzureKeyVaultBaseURL string `required:true`
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

type Node struct {
	// Underlying ifrit client
	ifritClient *ifrit.Client

	// Stringy identifier of this node
	name string

	// The IP address of the Lohpi mux. Used when invoking ifrit.Client.SendTo()
	muxIP         string
	policyStoreIP string

	// Crypto unit
	cu *comm.CryptoUnit

	// Config
	config *Config

	// gRPC client towards the Mux
	muxClient *comm.DirectoryGRPCClient

	// Policy store
	psClient *comm.PolicyStoreGRPCClient

	// Used for identifying data coming from policy store
	muxID         []byte
	policyStoreID []byte

	// Object id -> empty value for quick indexing
	datasetMap     map[string]struct{}
	datasetMapLock sync.RWMutex

	httpListener net.Listener
	httpServer   *http.Server

	// Callback to fetch remote archives
	datasetCallback     ExternalArchiveHandler
	datasetCallbackLock sync.RWMutex

	// Callback to fetch external metadata
	externalMetadataHandler     ExternalMetadataHandler
	externalMetadataHandlerLock sync.RWMutex

	refreshConfigLock sync.RWMutex
	refreshConfig     RefreshConfig

	// If true, callbaks used for remote resources are invoked.
	// If false, only in-memory maps are used for query processing.
	useRemoteURL bool

	// If true, use in-memory maps. Only use remote URL when absolutely
	// nescessary (ie. missing results)
	useCache bool

	// Azure database
	clientCheckoutTable *sql.DB
	policyDB            *sql.DB
	policyIdentifier    string

	// Key vault manager
	kvClient *keyvault.KeyVaultClient
}

// Database-related consts
var (
	dbName               = "nodepolicydb" // change me to nodedb
	schemaName           = "nodedbschema"
	datasetPolicyTable   = "policy_table"
	datasetCheckoutTable = "dataset_checkout_table"
)

func NewNode(name string, config *Config) (*Node, error) {
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
		CommonName: name,
		Locality:   []string{httpListener.Addr().String()},
	}

	cu, err := comm.NewCu(pk, config.LohpiCaAddr)
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

	kvClientConfig := keyvault.KeyVaultClientConfig{
		ClientID:     config.AzureClientId,
		ClientSecret: config.AzureClientSecret,
		TenantID:     config.AzureTenantId,
		BaseURL:      config.AzureKeyVaultBaseURL,
		SecretName:   config.AzureKeyVaultSecret,
	}

	keyClient, err := keyvault.NewKeyVaultClient(kvClientConfig)
	if err != nil {
		log.Errorf(err.Error())
		return nil, err
	}

	node := &Node{
		name:         name,
		ifritClient:  ifritClient,
		muxClient:    muxClient,
		config:       config,
		psClient:     psClient,
		httpListener: httpListener,
		cu:           cu,

		datasetMap:     make(map[string]struct{}),
		datasetMapLock: sync.RWMutex{},
		kvClient:       keyClient,
	}

	// Create the database if needed
	if err := node.initializePolicyDb(); err != nil {
		log.Errorf(err.Error())
		return nil, err
	}

	// Remove all stale identifiers since the last run. This will remove all the identifiers
	// from the table. The node cannot host datasets that have been removed from the remote location
	// since it last ran.
	if err := node.dbResetDatasetIdentifiers(); err != nil {
		log.Errorf(err.Error())
		return nil, err
	}

	return node, nil
}

func (n *Node) IfritClient() *ifrit.Client {
	return n.ifritClient
}

func (n *Node) InitializeLogfile(logToFile bool) error {
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
func (n *Node) IndexDataset(id string, ctx context.Context) error {
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
	if err := n.sendDatsetIdentifier(id, n.muxIP); err != nil {
		log.Errorln(err.Error())
		return err
	}

	return nil
}

// Requests the newest policy from the policy store
func (n *Node) pbRequestPolicy(id string) error {
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
	ch := n.ifritClient.SendTo(n.policyStoreIP, data)

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

func (n *Node) sendDatsetIdentifier(id, recipient string) error {
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
func (n *Node) RemoveDataset(id string) error {
	return nil
}

// Initializes the underlying node database using 'id' as the unique identifier for the relation.
func (n *Node) initializePolicyDb() error {
	resp, err := n.kvClient.GetSecret(n.config.AzureKeyVaultBaseURL, n.config.AzureKeyVaultSecret)
	if err != nil {
		return err
	}

	log.Println("Resp:", resp)

	if resp.Value == "" {
		return errors.New("Connection string from Azure Key Vault is empty")
	}

	return n.initializePostgreSQLdb(resp.Value)
}

// Shuts down the node
func (n *Node) Shutdown() {
	log.Printf("Shutting down Lohpi node\n")
	n.ifritClient.Stop()
	//	fuse.Shutdown() // might fail...
}

// Joins the network by starting the underlying Ifrit node. Further, it performs handshakes
// with the policy store and multiplexer at known addresses.
func (n *Node) JoinNetwork() error {
	if err := n.muxHandshake(); err != nil {
		panic(err)
		return err
	}

	if err := n.policyStoreHandshake(); err != nil {
		panic(err)
		return err
	}

	go n.startHttpServer(fmt.Sprintf(":%d", n.config.Port))

	n.ifritClient.RegisterMsgHandler(n.messageHandler)
	n.ifritClient.RegisterGossipHandler(n.gossipHandler)
	//	n.ifritClient.RegisterStreamHandler(n.streamHandler)

	return nil
}

func (n *Node) Address() string {
	return n.ifritClient.Addr()
}

func (n *Node) NodeName() string {
	return n.name
}

// If set to true, the node will use the in-memory caches before performing lookups in external sources.
func (n *Node) UseCache(use bool) {
	n.useCache = use
}

// Function type used to fetch compressed archives from external sources.
type ExternalArchiveHandler = func(id string) (*ExternalDataset, error)

// Function type to fetch metadata from the external data source
type ExternalMetadataHandler = func(id string) (*ExternalMetadata, error)

// PRIVATE METHODS BELOW
func (n *Node) muxHandshake() error {
	log.Infoln("Performing handshake with mux. Addr:", n.config.MuxAddr)
	conn, err := n.muxClient.Dial(n.config.MuxAddr)
	if err != nil {
		return err
	}

	defer conn.CloseConn()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()

	r, err := conn.Handshake(ctx, &pb.Node{
		Name:         n.name,
		IfritAddress: n.ifritClient.Addr(),
		Role:         "Storage node",
		Id:           []byte(n.ifritClient.Id()),
	})
	if err != nil {
		log.Fatalln(err)
		return err
	}

	n.muxIP = r.GetIp()
	n.muxID = r.GetId()
	return nil
}

func (n *Node) policyStoreHandshake() error {
	log.Infoln("Performing handshake with policy store at address", n.config.PolicyStoreAddr)
	conn, err := n.psClient.Dial(n.config.PolicyStoreAddr)
	if err != nil {
		return err
	}
	defer conn.CloseConn()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()

	r, err := conn.Handshake(ctx, &pb.Node{
		Name:         n.name,
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
func (n *Node) messageHandler(data []byte) ([]byte, error) {
	msg := &pb.Message{}
	if err := proto.Unmarshal(data, msg); err != nil {
		panic(err)
	}

	log.Infof("Node '%s' got message %s\n", n.name, msg.GetType())
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

		//if n.clientIsAllowed(msg.GetDatasetRequest()) {
		if err := n.dbCheckoutDataset(msg.GetDatasetRequest()); err != nil {
			log.Errorln(err.Error())
			return nil, err
		}
		return n.fetchDatasetURL(msg)
		/*} else {
			return n.unauthorizedAccess("Client has invalid access attributes")
		}*/

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

func (n *Node) rollbackCheckout(msg *pb.Message) {
	id := msg.GetStringValue()
	if err := n.dbCheckinDataset(id); err != nil {
		log.Errorln(err.Error())
	}
}

// TODO check the internal tables and so on...
func (n *Node) processPolicyBatch(msg *pb.Message) ([]byte, error) {
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
func (n *Node) notifyPolicyChangeToDirectoryServer(dataset string) {
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

	n.ifritClient.SendTo(n.muxIP, data)
}

// Returns true if a client has already checked out the dataset,
// returns false otherwise.
func (n *Node) isCheckedOutByClient(dataset string) bool {
	if dataset == "" {
		log.Errorln("Dataset identifier is empty")
		return true
	}

	return n.dbDatasetIsCheckedOutByClient(dataset)
}

// TODO: match the required access credentials of the dataset to the
// access attributes of the client. Return true if the client can access the data,
// return false otherwise. Also, verify that all the fields are present
func (n *Node) clientIsAllowed(r *pb.DatasetRequest) bool {
	if r == nil {
		log.Errorln("Dataset request is nil")
		return false
	}

	return n.dbDatasetIsAvailable(r.GetIdentifier())
}

// Returns a message notifying the recipient that the data request was unauthorized.
func (n *Node) unauthorizedAccess(errorMsg string) ([]byte, error) {
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
func (n *Node) fetchDatasetURL(msg *pb.Message) ([]byte, error) {
	id := msg.GetDatasetRequest().GetIdentifier()
	if handler := n.getDatasetCallback(); handler != nil {
		externalArchive, err := handler(id)
		if err != nil {
			log.Errorln(err.Error())
			return nil, err
		}

		respMsg := &pb.Message{
			DatasetResponse: &pb.DatasetResponse{
				URL:       externalArchive.URL,
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
func (n *Node) fetchDatasetMetadataURL(msg *pb.Message) ([]byte, error) {
	id := msg.GetDatasetRequest().GetIdentifier()
	if handler := n.getExternalMetadataHandler(); handler != nil {
		metadataUrl, err := handler(id)
		if err != nil {
			log.Errorln(err.Error())
			return nil, err
		}

		respMsg := &pb.Message{
			StringValue: metadataUrl.URL,
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

func (n *Node) pbDatasetExists(msg *pb.Message) ([]byte, error) {
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
func (n *Node) pbDatasetIdentifiers(msg *pb.Message) ([]byte, error) {
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

func (n *Node) acknowledgeMessage() ([]byte, error) {
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

func (n *Node) gossipHandler(data []byte) ([]byte, error) {
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

func (n *Node) nodeInfo() ([]byte, error) {
	str := fmt.Sprintf("Name: %s\tIfrit address: %s", n.name, n.IfritClient().Addr())
	return []byte(str), nil
}

// Acknowledges the given probe message.  TODO MORE HERE
func (n *Node) acknowledgeProbe(msg *pb.Message, d []byte) ([]byte, error) {
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
			Name:         n.name,
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

	log.Println(n.name, "sending ack to Policy store")
	n.ifritClient.SendTo(n.policyStoreIP, data)
	return nil, nil
}

// TODO remove me and use pbNode in ps gossip message instead
func (n *Node) verifyPolicyStoreMessage(msg *pb.Message) error {
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

func (n *Node) pbNode() *pb.Node {
	return &pb.Node{
		Name:         n.NodeName(),
		IfritAddress: n.ifritClient.Addr(),
		HttpAddress:  n.httpListener.Addr().String(),
		Role:         "storage node",
		//ContactEmail
		Id: []byte(n.ifritClient.Id()),
	}
}

func (n *Node) verifyMessageSignature(msg *pb.Message) error {
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

func (n *Node) insertDataset(id string, elem struct{}) {
	n.datasetMapLock.Lock()
	defer n.datasetMapLock.Unlock()
	n.datasetMap[id] = struct{}{}
}

func (n *Node) datasetExists(id string) bool {
	n.datasetMapLock.RLock()
	defer n.datasetMapLock.RUnlock()
	_, ok := n.datasetMap[id]
	return ok
}

func (n *Node) removeDataset(id string) {
	n.datasetMapLock.Lock()
	defer n.datasetMapLock.Unlock()
	delete(n.datasetMap, id)
}

func (n *Node) getDatasetMap() map[string]struct{} {
	n.datasetMapLock.RLock()
	defer n.datasetMapLock.RUnlock()
	return n.datasetMap
}

func (n *Node) datasetIdentifiers() []string {
	ids := make([]string, 0)
	m := n.getDatasetMap()
	for id := range m {
		ids = append(ids, id)
	}
	return ids
}
