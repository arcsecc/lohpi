package directoryserver

import (
	"context"
	"crypto/ecdsa"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"errors"
	"fmt"
	"github.com/arcsecc/lohpi/core/message"
	"github.com/arcsecc/lohpi/core/netutil"
	pb "github.com/arcsecc/lohpi/protobuf"
	"github.com/golang/protobuf/proto"
	"github.com/joonnna/ifrit"
	"github.com/lestrrat-go/jwx/jwk"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"net"
	"net/http"
	"strconv"
	"sync"
)

type Config struct {
	// Human-readable string of the directory server.
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
	// Configuration
	config     *Config
	configLock sync.RWMutex

	// Underlying Ifrit client
	ifritClient *ifrit.Client

	// In-memory cache structures
	nodeMapLock sync.RWMutex
	nodeMap     map[string]*pb.Node

	// HTTP-related stuff. Used by the demonstrator using cURL
	httpListener net.Listener
	httpServer   *http.Server

	// gRPC service
	listener     net.Listener
	serverConfig *tls.Config
	grpcs        *gRPCServer

	// Datasets that have been checked out
	// TODO move me to memcache?
	clientCheckoutMap     map[string][]string //datase id -> list of client who have checked out the data
	clientCheckoutMapLock sync.RWMutex

	invalidatedDatasets     map[string]struct{}
	invalidatedDatasetsLock sync.RWMutex

	// directory server database
	datasetDB  *sql.DB
	checkoutDB *sql.DB

	// Fetch the JWK
	pubKeyCache *jwk.AutoRefresh

	pb.UnimplementedDirectoryServerServer

	dsLookupService datasetLookupService
	cm              certManager
	memManager      membershipManager
	checkoutManager datasetCheckoutManager
}

type datasetLookupService interface {
	DatasetNodeExists(datasetId string) bool
	RemoveDatasetLookupEntry(datasetId string) error
	InsertDatasetLookupEntry(datasetId string, nodeName string) error
	DatasetLookupNode(datasetId string) *pb.Node
	DatasetIdentifiers() []string
}

type membershipManager interface {
	NetworkNodes() map[string]*pb.Node
	NetworkNode(nodeId string) *pb.Node
	AddNetworkNode(nodeId string, node *pb.Node) error
	NetworkNodeExists(id string) bool
	RemoveNetworkNode(id string) error
}

type datasetCheckoutManager interface {
	CheckoutDataset(datasetId string, checkout *pb.DatasetCheckout) error
	//CheckinDataset(datasetId string, client *pb.Client) error
	DatasetIsCheckedOut(datasetId string, client *pb.Client) bool
	DatasetCheckouts(datasetId string) ([]*pb.DatasetCheckout, error)
}

type certManager interface {
	Certificate() *x509.Certificate
	CaCertificate() *x509.Certificate
	PrivateKey() *ecdsa.PrivateKey
	PublicKey() *ecdsa.PublicKey
}

// Returns a new DirectoryServer using the given configuration. Returns a non-nil error, if any.
func NewDirectoryServerCore(cm certManager, dsLookupService datasetLookupService, memManager membershipManager, checkoutManager datasetCheckoutManager, config *Config) (*DirectoryServerCore, error) {
	if config == nil {
		return nil, errors.New("Configuration for directory server is nil")
	}

	ifritClient, err := ifrit.NewClient(&ifrit.Config{
		New:            true,
		TCPPort:        config.IfritTCPPort,
		UDPPort:        config.IfritUDPPort,
		Hostname:       config.Hostname,
		CryptoUnitPath: config.IfritCryptoUnitWorkingDirectory})
	if err != nil {
		return nil, err
	}

	listener, err := netutil.ListenOnPort(config.GRPCPort)
	if err != nil {
		return nil, err
	}

	s, err := newDirectoryGRPCServer(cm.Certificate(), cm.CaCertificate(), cm.PrivateKey(), listener)
	if err != nil {
		panic(err)
		return nil, err
	}

	ds := &DirectoryServerCore{
		config:      config,
		configLock:  sync.RWMutex{},
		ifritClient: ifritClient,

		// gRPC server
		grpcs: s,

		clientCheckoutMap:   make(map[string][]string, 0),
		invalidatedDatasets: make(map[string]struct{}),

		dsLookupService: dsLookupService,
		cm:              cm,
		memManager:      memManager,
		checkoutManager: checkoutManager,
	}

	ds.grpcs.Register(ds)
	ds.ifritClient.RegisterMsgHandler(ds.messageHandler)
	ifritClient.RegisterGossipHandler(ds.gossipMessageHandler)
	//ifritClient.RegisterResponseHandler(self.GossipResponseHandler)

	// Initialize the PostgreSQL directory server database
	if err := ds.initializeDirectorydb(config.SQLConnectionString); err != nil {
		return nil, err
	}

	return ds, nil
}

// Starts the directory server. This includes starting the HTTP server, Ifrit client and gRPC server.
// In addition, it will try and restore the state it had before it crashed.
func (d *DirectoryServerCore) Start() {
	log.Infoln("Directory server running gRPC server at", d.grpcs.Addr(), "and Ifrit client at", d.ifritClient.Addr())
	go d.ifritClient.Start()
	go d.startHttpServer(":" + strconv.Itoa(d.config.HTTPPort))
	go d.grpcs.Start()
}

// Create a node that performs a handshake with
func (d *DirectoryServerCore) Stop() {
	d.ifritClient.Stop()
	d.grpcs.Stop()
	d.shutdownHttpServer()
}

// PIVATE METHODS BELOW THIS LINE
// TODO: implement timeouts and context handling on direct messaging.
func (d *DirectoryServerCore) messageHandler(data []byte) ([]byte, error) {
	msg := &pb.Message{}
	if err := proto.Unmarshal(data, msg); err != nil {
		log.Errorln(err)
		return nil, err
	}

	if err := d.verifyMessageSignature(msg); err != nil {
		log.Errorln(err)
		return nil, err
	}

	switch msgType := msg.Type; msgType {
	case message.MSG_TYPE_ADD_DATASET_IDENTIFIER:
		//if err := d.dsLookupService.InsertDatasetLookupEntry(msg.GetStringValue(), msg.GetSender()); err != nil {
		if err := d.dsLookupService.InsertDatasetLookupEntry(msg.GetStringValue(), msg.GetSender().GetName()); err != nil {
			log.Errorln(err.Error())
			return nil, err
		}

	case message.MSG_SYNCHRONIZE_DATASET_IDENTIFIERS:
		if err := d.resolveDatasetIdentifiersDeltas(msg.GetStringSlice(), msg.GetSender()); err != nil {
			log.Errorln(err.Error())
			return nil, err
		}

	case message.MSG_POLICY_REVOCATION_UPDATE:
		d.updateRevocationState(msg)
		//d.addRevokedDataset(msg.GetStringValue())

	default:
		log.Warnf("Unknown message type at DirectoryServerCore handler: %s\n", msg.GetType())
	}

	resp, err := proto.Marshal(&pb.Message{Type: message.MSG_TYPE_OK})
	if err != nil {
		log.Errorln(err)
		return nil, err
	}

	return resp, nil
}

func (d *DirectoryServerCore) resolveDatasetIdentifiersDeltas(newIdentifiers []string, node *pb.Node) error {
	if newIdentifiers == nil {
		return errors.New("newIdentifiers is nil")
	}

	if node == nil {
		return errors.New("node is nil")
	}

	/*currentIdentifiers := d.dsLookupService.DatasetIdentifiers()
	for _, id := range newIdentifiers {
	/*	if err := d.dsLookupService.InsertDatasetNode(id, node); err != nil {
			log.Errorln(err.Error())
		}
	}

	superfluous := make([]string, 0)

	// Find superfluous datasets in the collection and remove them. Use identifier as key
	for _, currentDatasetIdentifier := range currentIdentifiers {
		found := false
		for _, newIdentifier := range newIdentifiers {
			if currentDatasetIdentifier == newIdentifier {
				found = true
				break
			}
		}

		if found {
			continue
		} else {
			superfluous = append(superfluous, currentDatasetIdentifier)
		}
	}

	for _, s := range superfluous {
		d.dsLookupService.RemoveDatasetNode(s)
	}

	*/
	return nil
}

// TODO refine revocations :)
// Updates the revocation state of the dataset that has been checked out. If the checked-out dataset
// is to be revoked, put it into the map. Remove it from the map if the policy state changes to "false" to "true".
func (d *DirectoryServerCore) updateRevocationState(msg *pb.Message) {
	dataset := msg.GetStringValue()
	b := msg.GetBoolValue()

	// Check if it already is flagged as revoked
	if d.datasetIsInvalidated(dataset) {
		if b {
			d.removeRevokedDataset(dataset)
		}
	} else {
		if !b {
			d.addRevokedDataset(dataset)
		}
	}
}

func (d *DirectoryServerCore) addRevokedDataset(dataset string) {
	d.invalidatedDatasetsLock.Lock()
	defer d.invalidatedDatasetsLock.Unlock()
	d.invalidatedDatasets[dataset] = struct{}{}
}

func (d *DirectoryServerCore) revokedDatasets() map[string]struct{} {
	d.invalidatedDatasetsLock.RLock()
	defer d.invalidatedDatasetsLock.RUnlock()
	return d.invalidatedDatasets
}

func (d *DirectoryServerCore) removeRevokedDataset(dataset string) {
	d.invalidatedDatasetsLock.Lock()
	defer d.invalidatedDatasetsLock.Unlock()
	delete(d.invalidatedDatasets, dataset)
}

// Adds the given node to the network and returns the DirectoryServerCore's IP address
func (d *DirectoryServerCore) Handshake(ctx context.Context, node *pb.Node) (*pb.HandshakeResponse, error) {
	if node == nil {
		return nil, status.Error(codes.InvalidArgument, "pb node is nil")
	}

	if err := d.memManager.AddNetworkNode(node.GetName(), node); err != nil {
		return nil, err
	}

	log.Infof("Added '%s' to map with Ifrit IP '%s' and HTTPS address '%s'\n", node.GetName(), node.GetIfritAddress(), node.GetHttpsAddress())
	return &pb.HandshakeResponse{
		Ip: fmt.Sprintf("%s:%d", d.config.Hostname, d.config.IfritTCPPort),
		Id: []byte(d.ifritClient.Id()),
	}, nil
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

	if !d.ifritClient.VerifySignature(r, s, data, string(msg.GetSender().GetId())) {
		return errors.New("DirectoryServerCore could not securely verify the integrity of the message")
	}

	// Restore message
	msg.Signature = &pb.MsgSignature{
		R: r,
		S: s,
	}

	return nil
}

func (d *DirectoryServerCore) pbNode() *pb.Node {
	return &pb.Node{
		Name:         "Lohpi directory server",
		IfritAddress: fmt.Sprintf("%s:%d", d.config.Hostname, d.config.IfritTCPPort),
		Id:           []byte(d.ifritClient.Id()),
	}
}

// TODO: handle ctx
// Rollbacks the checkout of a dataset. This is useful if any errors occur somewhere in the pipeline.
func (d *DirectoryServerCore) rollbackCheckout(nodeAddr, dataset string, ctx context.Context) error {
	msg := &pb.Message{
		Type:        message.MSG_TYPE_ROLLBACK_CHECKOUT,
		Sender:      d.pbNode(),
		StringValue: dataset,
	}

	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	r, s, err := d.ifritClient.Sign(data)
	if err != nil {
		return err
	}

	msg.Signature = &pb.MsgSignature{R: r, S: s}
	data, err = proto.Marshal(msg)
	if err != nil {
		return err
	}

	ch := d.ifritClient.SendTo(nodeAddr, data)
	select {
	case resp := <-ch:
		respMsg := &pb.Message{}
		if resp != nil {
			if err := proto.Unmarshal(resp.Data, respMsg); err != nil {
				return err
			}

			if err := d.verifyMessageSignature(respMsg); err != nil {
				return err
			}
		}

	case <-ctx.Done():
		err := errors.New("Could not verify dataset checkout rollback")
		return err
	}

	return nil
}

func (d *DirectoryServerCore) gossipMessageHandler(data []byte) ([]byte, error) {
	msg := &pb.Message{}
	if err := proto.Unmarshal(data, msg); err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	log.Infof("Directory server got gossip message\n")

	if err := d.verifyMessageSignature(msg); err != nil {
		log.Warnln(err.Error())
		//return nil, err
	}

	switch msgType := msg.Type; msgType {
	case message.MSG_TYPE_PROBE:
		//n.ifritClient.SetGossipContent(data)
	case message.MSG_TYPE_POLICY_STORE_UPDATE:
		// Use policy observer
		return d.parsePolicyBatch(msg)
	default:
		fmt.Printf("Unknown gossip message type: %s\n", msg.GetType())
	}

	return nil, nil
}

func (d *DirectoryServerCore) parsePolicyBatch(msg *pb.Message) ([]byte, error) {
	if msg == nil {
		err := errors.New("Pb message is nil")
		log.Errorln(err.Error())
		return nil, err
	}

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

	wg := &sync.WaitGroup{}
	for _, m := range gspMsg.GetGossipMessageBody() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := d.applyPolicy(m.GetPolicy()); err != nil {
				log.Errorln(err.Error())
			}
		}()
	}

	wg.Wait()

	return nil, nil
}

// Apply policy to checked out dataset
func (d *DirectoryServerCore) applyPolicy(newPolicy *pb.Policy) error {
	if newPolicy == nil {
		return errors.New("Policy to be applied is nil")
	}

	return nil
}

// TODO: refine this a lot more :) move me to mem cache
func (d *DirectoryServerCore) insertCheckedOutDataset(dataset, clientId string) {
	d.clientCheckoutMapLock.Lock()
	defer d.clientCheckoutMapLock.Unlock()
	if d.clientCheckoutMap[dataset] == nil {
		d.clientCheckoutMap[dataset] = make([]string, 0)
	}
	d.clientCheckoutMap[dataset] = append(d.clientCheckoutMap[dataset], clientId)
}

func (d *DirectoryServerCore) getCheckedOutDatasetMap() map[string][]string {
	d.clientCheckoutMapLock.RLock()
	defer d.clientCheckoutMapLock.RUnlock()
	return d.clientCheckoutMap
}

func (d *DirectoryServerCore) datasetIsInvalidated(dataset string) bool {
	_, exists := d.revokedDatasets()[dataset]
	return exists
}
