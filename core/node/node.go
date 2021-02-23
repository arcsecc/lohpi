package node

import (
	"context"
	"database/sql"
	"crypto/x509/pkix"
	"os"
_	"net/url"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/joonnna/ifrit"
	"github.com/pkg/errors"
	"github.com/arcsecc/lohpi/core/comm"
	"github.com/arcsecc/lohpi/core/message"
	"github.com/arcsecc/lohpi/core/netutil"
	pb "github.com/arcsecc/lohpi/protobuf"
	log "github.com/sirupsen/logrus"
)

var (
	errNoAddr = errors.New("No certificate authority address provided, can't continue")
)

type Config struct {
	MuxIP          		string 		`default:"127.0.1.1:8081"`
	PolicyStoreIP  		string 		`default:"127.0.1.1:8082"`
	LohpiCaAddr    		string 		`default:"127.0.1.1:8301"`
	RecIP          		string 		`default:"127.0.1.1:8084"`
	FileDigesters  		int    		`default:20`
	HttpPort       		int    		`required:false`
	PolicyIdentifier 	string    	`required:true`

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
	Value string
}

type Dataset struct {
	name     string
	metadata *Metadata

	storageObjectsMap     map[string]*StorageObject
	storageObjectsMapLock sync.RWMutex

	// Global policy?
	// Actual data here as well
}

// Used to configure the time intervals between each fetching of the identifiers of the dataset.
// You really should use this config to enable a push-based approach. 
type RefreshConfig struct {
	RefreshInterval time.Duration
	URL string
}

// A generic representation of a storage object stored at this node.
// This can be a file, blob or anything at all.
type StorageObject struct {
	policy   *Policy
	metadata *Metadata
	content  []byte
}

// Policy assoicated with metadata
type Policy struct {
	Issuer           string
	ObjectIdentifier string
	Content          string
}

type StorageObjectContent []byte

type Metadata map[string]string

type objectFile struct {
	path       string
	attributes []byte
	content    []byte
	err        error
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
	muxClient *comm.MuxGRPCClient

	// Policy store
	psClient *comm.PolicyStoreGRPCClient

	// Used for identifying data coming from policy store
	muxID         []byte
	policyStoreID []byte

	// Object id -> object header
	datasetMap     map[string]*Dataset
	datasetMapLock sync.RWMutex

	httpListener net.Listener
	httpServer   *http.Server

	// Callbacks for external sources
	datasetIdentifiersHandler     ExernalDatasetIdentifiersHandler
	datasetIdentifiersHandlerLock sync.RWMutex

	// Callback to check if dataset exists
	identifierExistsHandler     ExternalIdentifierExistsHandler
	identifierExistsHandlerLock sync.RWMutex

	// Callback to fetch remote archives
	datasetCallback     ExternalArchiveHandler
	datasetCallbackLock sync.RWMutex

	// Callback to fetch external metadata
	externalMetadataHandler     ExternalMetadataHandler
	externalMetadataHandlerLock sync.RWMutex

	refreshConfigLock sync.RWMutex
	refreshConfig RefreshConfig

	// If true, callbaks used for remote resources are invoked.
	// If false, only in-memory maps are used for query processing.
	useRemoteURL bool

	// If true, use in-memory maps. Only use remote URL when absolutely 
	// nescessary (ie. missing results)
	useCache bool

	// Azure database
	clientCheckoutTable *sql.DB
	policyDB *sql.DB
	policyIdentifier string
}

// Database-related consts
var (
	dbName = "nodepolicydb"
	schemaName = "nodedbschema" 
	policyTable = "policy_table"
	datasetCheckoutTable = "dataset_checkout_table"
)

func NewNode(name string, config *Config) (*Node, error) {
	ifritClient, err := ifrit.NewClient()
	if err != nil {
		return nil, err
	}

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

	node := &Node{
		name:        		name,
		ifritClient: 		ifritClient,
		muxClient:   		muxClient,
		config:      		config,
		psClient:    		psClient,
		httpListener:      	httpListener,
		cu: cu,

		datasetMap:     make(map[string]*Dataset),
		datasetMapLock: sync.RWMutex{},
	}

	if err := node.initializePolicyDb(config.PolicyIdentifier); err != nil {
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

// Initializes the underlying node database using 'id' as the unique identifier for the relation.
func (n *Node) initializePolicyDb(policyIdentifier string) error {
	var connectionString = fmt.Sprintf(os.Getenv("NODE_DB_CONNECTION_STRING"))
	if connectionString == "" {
		return errors.New("Tried to fetch 'NODE_DB_CONNECTION_STRING' from environment but it was not set.")
	}

	log.Println("Using NODE_DB_CONNECTION_STRING")

	n.policyIdentifier = policyIdentifier
	return n.initializePostgreSQLdb(connectionString)
}

// Assoicated the given object attribute with the object identifier.
func (n *Node) SetObjectPolicy(id, objectAttribute string) error {
	return n.setObjectPolicy(id, objectAttribute)
}

// Returns true if the subject's attributes are compatible with the object's attributes, 
// returns false otherwise.
func (n *Node) SubjectIsAllowedAccess(subjectAttribute, objectAttribute string) bool {
	return n.subjectIsAllowedAccess(subjectAttribute, objectAttribute)
}

// Removes the object policy from the node
func (n *Node) RemoveObjectPolicy(id string) {
	n.removeObjectPolicy(id)
}

func (n *Node) DatasetExistsInDb(id string) bool {
	return n.datasetExistsInDb(id)
}

// Might be used as a way to specify attributes more precisely
/*func (n *Node) InitializePolicyDb(primaryKey string, attributes []string) error {
	var attr bytes.Buffer

	for n, a := range attributes {
		if n == len(attributes) - 1 {
			attr.WriteString(a + " VARCHAR(50)")
		} else {
			attr.WriteString(a + " VARCHAR(50), ")
		}
	}

	q := `CREATE TABLE IF NOT EXISTS ` + schemaName + `.` + policyTable + ` (
		` + primaryKey + ` VARCHAR(45) PRIMARY KEY NOT NULL, ` + attr.String() + `);`

	if err := n.initializeAzurePostgreSQLdb(q); err != nil {
		return err
	}
	return nil 
}*/

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
		return err
	}

	if err := n.policyStoreHandshake(); err != nil {
		return err
	}

	go n.startHttpServer()
	
	n.ifritClient.RegisterMsgHandler(n.messageHandler)
	n.ifritClient.RegisterGossipHandler(n.gossipHandler)
//	n.ifritClient.RegisterStreamHandler(n.streamHandler)
	go n.ifritClient.Start()

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

// Function type used for callbacks to fetch datasets on-demand. 
type ExernalDatasetIdentifiersHandler = func(id string) ([]string, error)

// Function type used to fetch compressed archives from external sources.
type ExternalArchiveHandler = func(id string) (*ExternalDataset, error)

// Function type to check if external identifiers in a dataset exists.
type ExternalIdentifierExistsHandler = func(id string) bool

// Function type to fetch metadata from the external data source
type ExternalMetadataHandler = func(id string) (*ExternalMetadata, error)

// PRIVATE METHODS BELOW
func (n *Node) muxHandshake() error {
	conn, err := n.muxClient.Dial(n.config.MuxIP)
	if err != nil {
		return err
	}

	defer conn.CloseConn()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()

	r, err := conn.Handshake(ctx, &pb.Node{
		Name:    n.name,
		IfritAddress: n.ifritClient.Addr(),
		Role:    "Storage node",
		Id:      []byte(n.ifritClient.Id()),
	})
	if err != nil {
		log.Fatal(err)
	}

	n.muxIP = r.GetIp()
	n.muxID = r.GetId()
	return nil
}

func (n *Node) policyStoreHandshake() error {
	return nil
	conn, err := n.psClient.Dial(n.config.PolicyStoreIP)
	if err != nil {
		return err
	}
	defer conn.CloseConn()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()

	r, err := conn.Handshake(ctx, &pb.Node{
		Name:    n.name,
		IfritAddress: n.ifritClient.Addr(),
		Role:    "Storage node",
		Id:      []byte(n.ifritClient.Id()),
	})
	if err != nil {
		log.Fatal(err)
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

	log.Printf("Node '%s' got message %s\n", n.name, msg.GetType())

	switch msgType := msg.Type; msgType {
	case message.MSG_TYPE_GET_NODE_INFO:
		return n.nodeInfo()

	case message.MSG_TYPE_GET_DATASET_IDENTIFIERS:
		return n.fetchDatasetIdentifiers(msg)

	case message.MSG_TYPE_GET_DATASET_URL:
		if n.clientIsAllowed(msg.GetDatasetRequest()) {
			if err := n.checkoutDataset(msg.GetDatasetRequest()); err != nil {
				panic(err)
			}
			return n.fetchDatasetURL(msg)
		} else {
			return n.unauthorizedAccess(msg)
		}

	case message.MSG_TYPE_GET_DATASET:
	//	return n.marshalledStorageObjectContent(msg)

	case message.MSG_TYPE_DATASET_EXISTS:
		return n.datasetExists(msg)

	case message.MSG_TYPE_GET_DATASET_METADATA_URL:
		return n.fetchDatasetMetadataURL(msg)
		// TODO finish me
	case message.MSG_TYPE_POLICY_STORE_UPDATE:
		log.Println("Got new policy from policy store!")

	case message.MSG_TYPE_PROBE:
		return n.acknowledgeProbe(msg, data)

	default:
		fmt.Printf("Unknown message type: %s\n", msg.GetType())
	}

	return n.acknowledgeMessage()
}

// TODO: match the required access credentials of the dataset to the 
// access attributes of the client. Return true if the client can access the data, 
// return false otherwise. Also, verify that all the fields are present
func (n *Node) clientIsAllowed(r *pb.DatasetRequest) bool {
	return true
}

func (n *Node) unauthorizedAccess(msg *pb.Message) ([]byte, error) {
	respMsg := &pb.DatasetResponse{
		IsAllowed: false,
	}

	data, err := proto.Marshal(respMsg)
	if err != nil {
		return []byte{}, err
	}

	r, s, err := n.ifritClient.Sign(data)
	if err != nil {
		return []byte{}, err
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
			panic(err)
		}

		respMsg := &pb.Message{
			DatasetResponse: &pb.DatasetResponse{
				URL: externalArchive.URL,
				IsAllowed: true,
			},
		}

		data, err := proto.Marshal(respMsg)
		if err != nil {
			panic(err)
		}

		r, s, err := n.ifritClient.Sign(data)
		if err != nil {
			panic(err)
		}

		respMsg.Signature = &pb.MsgSignature{R: r, S: s}
		return proto.Marshal(respMsg)
	} else {
		log.Println("Dataset archive handler not registered")
	}

	return nil, nil 
}

func (n *Node) fetchDatasetMetadataURL(msg *pb.Message) ([]byte, error) {

	id := msg.GetStringValue()
	if handler := n.getExternalMetadataHandler(); handler != nil {
		metadataUrl, err := handler(id)
		if err != nil {
			panic(err)
		}

		respMsg := &pb.Message{
			StringValue: metadataUrl.URL,
			Sender: n.pbNode(),
		}

		data, err := proto.Marshal(respMsg)
		if err != nil {
			panic(err)
		}

		r, s, err := n.ifritClient.Sign(data)
		if err != nil {
			panic(err)
		}

		respMsg.Signature = &pb.MsgSignature{R: r, S: s}
		return proto.Marshal(respMsg)
	} else {
		log.Println("Dataset archive handler not registered")
	}

	return nil, nil 
}

func (n *Node) datasetExists(msg *pb.Message) ([]byte, error) {
	if n.identifierExistsHandler == nil {
		return []byte{}, nil
	}

	datasetId := msg.GetStringValue()
	respMsg := &pb.Message{}

	respMsg.BoolValue = n.identifierExistsHandler(datasetId)
	data, err := proto.Marshal(respMsg)
	if err != nil {
		return []byte{}, err
	}

	r, s, err := n.ifritClient.Sign(data)
	if err != nil {
		return []byte{}, err
	}

	respMsg.Signature = &pb.MsgSignature{R: r, S: s}
	return proto.Marshal(respMsg)
}

// BUG: timeout when issuing identifier requests to multiple nodes
func (n *Node) fetchDatasetIdentifiers(msg *pb.Message) ([]byte, error) {
	if n.datasetIdentifiersHandler == nil {
		return []byte{}, nil
	}

	datasetId := msg.GetStringValue()
	respMsg := &pb.Message{
		StringSlice: make([]string, 0),
	}

	var callbackErr error
	respMsg.StringSlice, callbackErr = n.datasetIdentifiersHandler(datasetId)
	if callbackErr != nil {
		return []byte{}, callbackErr
	}

	data, err := proto.Marshal(respMsg)
	if err != nil {
		return []byte{}, err
	}

	r, s, err := n.ifritClient.Sign(data)
	if err != nil {
		return []byte{}, err
	}

	respMsg.Signature = &pb.MsgSignature{R: r, S: s}
	return proto.Marshal(respMsg)
}

func (n *Node) acknowledgeMessage() ([]byte, error) {
	msg := &pb.Message{Type: message.MSG_TYPE_OK}
	data, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}

	r, s, err := n.ifritClient.Sign(data)
	if err != nil {
		return []byte{}, err
	}

	msg.Signature = &pb.MsgSignature{R: r, S: s}
	return proto.Marshal(msg)
}

func (n *Node) gossipHandler(data []byte) ([]byte, error) {
	msg := &pb.Message{}
	if err := proto.Unmarshal(data, msg); err != nil {
		return []byte{}, err
	}

	// Might need to move this one? check type!
	/*if err := n.verifyPolicyStoreMessage(msg); err != nil {
		log.Fatalf(err.Error())
	}*/

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
			Name:    n.name,
			IfritAddress: n.ifritClient.Addr(),
			Role:    "Storage node",
			Id:      []byte(n.ifritClient.Id()),
		},
		Probe: msg.GetProbe(),
	}

	data, err := proto.Marshal(resp)
	if err != nil {
		return []byte{}, err
	}

	// Sign the acknowledgment response
	r, s, err := n.ifritClient.Sign(data)
	if err != nil {
		return []byte{}, err
	}

	// Message with signature appended to it
	resp = &pb.Message{
		Type: message.MSG_TYPE_PROBE_ACK,
		Sender: &pb.Node{
			Name:    n.name,
			IfritAddress: n.ifritClient.Addr(),
			Role:    "Storage node",
			Id:      []byte(n.ifritClient.Id()),
		},
		Signature: &pb.MsgSignature{
			R: r,
			S: s,
		},
		Probe: msg.GetProbe(),
	}

	data, err = proto.Marshal(resp)
	if err != nil {
		return []byte{}, err
	}

	log.Println(n.name, "sending ack to Policy store")
	n.ifritClient.SendTo(n.policyStoreIP, data)
	return nil, nil
}

func (n *Node) verifyPolicyStoreMessage(msg *pb.Message) error {
	r := msg.GetSignature().GetR()
	s := msg.GetSignature().GetS()

	msg.Signature = nil

	data, err := proto.Marshal(msg)
	if err != nil {
		log.Println(err.Error())
		return err
	}

	if !n.ifritClient.VerifySignature(r, s, data, string(n.policyStoreID)) {
		return errors.New("Could not securely verify the integrity of the policy store message")
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
		Name:    n.NodeName(),
		IfritAddress: n.ifritClient.Addr(),
		HttpAddress: n.httpListener.Addr().String(),
		Role:    "storage node",
		//ContactEmail
		Id: []byte(n.ifritClient.Id()),
	}
}
