package policystore

import (
	"context"
	"errors"
	"fmt"
	"crypto/ecdsa"
	"crypto/x509"
	"github.com/arcsecc/lohpi/core/message"
	"github.com/arcsecc/lohpi/core/netutil"
	"github.com/arcsecc/lohpi/core/policystore/multicast"
	pb "github.com/arcsecc/lohpi/protobuf"
	"github.com/go-git/go-git/v5"
	"github.com/golang/protobuf/proto"
	"github.com/joonnna/ifrit"
	"github.com/lestrrat-go/jwx/jwk"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	log "github.com/sirupsen/logrus"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	pbtime "google.golang.org/protobuf/types/known/timestamppb"
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
	IfritTCPPort int
	IfritUDPPort int
}

type storageNodeLookupService interface {
	DatasetNodeExists(datasetId string) bool
	RemoveDatasetLookupEntry(datasetId string) error
	DatasetNodeName(datasetId string) string
	InsertDatasetLookupEntry(datasetId string, nodeName string) error
	DatasetLookupNode(datasetId string) *pb.Node
	DatasetIdentifiers() []string
	DatasetIdentifiersAtNode(nodeName string) []string

}

type datasetManager interface {
	Dataset(datasetId string) *pb.Dataset
	Datasets() map[string]*pb.Dataset
	DatasetIdentifiers(fromIdx int64, toIdx int64) []string
	DatasetExists(datasetId string) bool
	RemoveDataset(datasetId string) error 
	SetDatasetPolicy(datasetId string, policy *pb.Policy) error
	InsertDataset(datasetId string, dataset *pb.Dataset) error
	DatasetsAtNode(node *pb.Node) []*pb.Dataset 
}

type membershipManager interface {
	NetworkNode(nodeId string) *pb.Node
	AddNetworkNode(nodeid string, network *pb.Node) error 
	NetworkNodeExists(id string) bool
	RemoveNetworkNode(id string) error
	NetworkNodes() map[string]*pb.Node
}

type certManager interface {
	Certificate() *x509.Certificate
	CaCertificate() *x509.Certificate
	PrivateKey() *ecdsa.PrivateKey
	PublicKey() *ecdsa.PublicKey
}

type setSyncer interface {
	RegisterIfritClient(client *ifrit.Client) 
	RequestDatasetSync(ctx context.Context, node *pb.Node, currentSets []*pb.Dataset) (int, error)
	DBLogDatasetReconciliation(nodeName string, numResolved int) error
}

type policyLogger interface {
	InsertObservedGossip(g *pb.GossipMessage) error
	InsertAppliedPolicy(p *pb.Policy) error
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
	repository *git.Repository

	// Sync
	exitChan    chan bool
	networkLock sync.Mutex

	// gRPC service
	grpcs *gRPCServer

	// Multicast specifics
	multicastManager *multicast.MulticastManager
	stopBatching chan bool

	// HTTP server stuff
	httpListener net.Listener
	httpServer   *http.Server

	// Fetch the JWK
	ar *jwk.AutoRefresh

	cm certManager
	setSync setSyncer
	dsManager datasetManager
	memManager membershipManager
	dsLookupService storageNodeLookupService
	policyLog policyLogger
}

func NewPolicyStoreCore(cm certManager, dsLookupService storageNodeLookupService, setSync setSyncer, memManager membershipManager, dsManager datasetManager, config *Config, policyLog policyLogger, new bool) (*PolicyStoreCore, error) {
	ifritClient, err := ifrit.NewClient(&ifrit.Config{
		New: new,
		TCPPort: config.IfritTCPPort,
		UDPPort: config.IfritUDPPort,
		Hostname: config.Hostname,
		CryptoUnitPath: config.IfritCryptoUnitWorkingDirectory})
	if err != nil {
		return nil, err
	}

	// Local git repository using go-git
	repository, err := initializeGitRepository(config.GitRepositoryPath)
	if err != nil {
		return nil, err
	}

	listener, err := netutil.ListenOnPort(config.GRPCPort)
	if err != nil {
		return nil, err
	}

	s, err := newPolicyStoreGRPCServer(cm.Certificate(), cm.CaCertificate(), cm.PrivateKey(), listener)
	if err != nil {
		return nil, err
	}

	multicastManager, err := multicast.NewMulticastManager(ifritClient, config.MulticastAcceptanceLevel, config.NumDirectRecipients, config.SQLConnectionString, config.Name)
	if err != nil {
		return nil, err
	}

	// TODO: remove some of the variables into other interfaces/packages?
	ps := &PolicyStoreCore{
		ifritClient:      ifritClient,
		repository:       repository,
		exitChan:         make(chan bool, 1),
		cm:               cm,
		grpcs:            s,
		config:           config,
		multicastManager: multicastManager,
		stopBatching:     make(chan bool),
		dsLookupService:  dsLookupService,
		memManager:		  memManager,
		dsManager: 		  dsManager,
		setSync: 		  setSync,
		policyLog: 		  policyLog,
	}

	ps.grpcs.Register(ps)

	// Set Ifrit callbacks
	ps.setSync.RegisterIfritClient(ifritClient)
	ps.ifritClient.RegisterMsgHandler(ps.messageHandler)
	ps.ifritClient.RegisterGossipHandler(ps.gossipHandler)

	return ps, nil
}

func (ps *PolicyStoreCore) Start() error {
	log.WithFields(logFields).Printf("Policy store running gRPC server at", ps.grpcs.Addr(), "and Ifrit client at", ps.ifritClient.Addr())
	go ps.grpcs.Start()
	go ps.startHttpServer(fmt.Sprintf(":%d", ps.PolicyStoreConfig().HTTPPort))
	go ps.ifritClient.Start()
	go ps.startStateSynchronizer()
	return nil
}

// Runs the policy batcher. The function blocks until it stops multicasting policy batches.
func (ps *PolicyStoreCore) RunPolicyBatcher() {
	mcTimer := time.NewTimer(ps.PolicyStoreConfig().GossipInterval)

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

				if ps.multicastManager.IsMulticasting() {
					log.WithFields(logFields).Info("Manager is already multicasting a policy batch")
					return
				}
				
				if err := ps.multicastManager.Multicast(&multicast.Config{
					Mode: multicast.RandomMembers,
					Members: ps.ifritMembersAddress()}); err != nil {
						log.WithFields(logFields).Error(err.Error())
					}

					log.WithFields(logFields).Info("Multicast manager completed a burst of message multicasting")
			}()
			case <-ps.stopBatching:
				// TODO: garbage collect timer
				return
		}
	}
}

func (ps *PolicyStoreCore) startStateSynchronizer() {
	for {
		for _, node := range ps.memManager.NetworkNodes() {
			select {
			case <-ps.exitChan:
				log.WithFields(logFields).Info("Exiting state synchronization")
				return 
			case <-time.After(10 * time.Second):
				log.WithFields(logFields).Infof("Started policy set reconciliation towards node with identifier '%s'", node.GetName())
				ctx, cancel := context.WithTimeout(context.Background(), 60 * time.Second)
				defer cancel()

				sets := ps.dsManager.DatasetsAtNode(node)

				numResolved, err := ps.setSync.RequestDatasetSync(ctx, node, sets)
				if err != nil {
					log.WithFields(logFields).Error(err.Error())
					continue
				}

				if err := ps.setSync.DBLogDatasetReconciliation(node.GetName(), numResolved); err != nil {
					log.WithFields(logFields).Error(err.Error())
					continue
				}

				log.WithFields(logFields).
					Infof("Dataset sync done. Resolved %d dataset policies with node '%s'", numResolved, node.GetName())
			}
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

func (ps *PolicyStoreCore) ifritMembersAddress() []string {
	memMap := ps.memManager.NetworkNodes()
	members := make([]string, 0)
	for _, n := range memMap {
		members = append(members, n.GetIfritAddress())
	}
	return members
}

// Invoked by ifrit message handler
// This should be invoked a couple of seconds after the Ifirt nodes have joined the network
// because the sender's ID might not be known to the recipient.
func (ps *PolicyStoreCore) verifyMessageSignature(msg *pb.Message) error {
	return nil
	// Verify the integrity of the node
	r := msg.GetSignature().GetR()
	s := msg.GetSignature().GetS()

	msg.Signature = nil

	// Marshal it before verifying its integrity
	data, err := proto.Marshal(msg)
	if err != nil {
		log.WithFields(logFields).Print(err.Error())
		return err
	}

	if !ps.ifritClient.VerifySignature(r, s, data, string(msg.GetSender().GetId())) {
		return errors.New("Policy store could not securely verify the integrity of the message")
	}

	// Restore message
	msg.Signature = &pb.MsgSignature{
		R: r,
		S: s,
	}

	return nil
}

// Invoked by ifrit message handler
func (ps *PolicyStoreCore) verifyPolicyStoreGossipSignature(msg *pb.Message) error {
	return nil
	// Verify the integrity of the node
	r := msg.GetSignature().GetR()
	s := msg.GetSignature().GetS()

	msg.Signature = nil

	// Marshal it before verifying its integrity
	data, err := proto.Marshal(msg)
	if err != nil {
		log.WithFields(logFields).Print(err.Error())
		return err
	}

	if !ps.ifritClient.VerifySignature(r, s, data, string(ps.ifritClient.Id())) {
		return errors.New("Policy store could not securely verify the gossip of the message")
	}

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
		return nil, status.Error(codes.InvalidArgument, "pb node is nil")
	}

	if err := ps.memManager.AddNetworkNode(node.GetName(), node); err != nil {
		return nil, err
	}

	log.WithFields(logFields).Infof("Policy store added node '%s' to map with Ifrit IP '%s'\n", node.GetName(), node.GetIfritAddress())
	ipString := fmt.Sprintf("%s:%d", ps.PolicyStoreConfig().Hostname, ps.PolicyStoreConfig().IfritTCPPort)

	return &pb.HandshakeResponse{
		Ip: ipString,
		Id: []byte(ps.ifritClient.Id()),
	}, nil
}

// Ifrit message handler
func (ps *PolicyStoreCore) messageHandler(data []byte) ([]byte, error) {
	msg := &pb.Message{}
	if err := proto.Unmarshal(data, msg); err != nil {
		log.WithFields(logFields).Error(err.Error())
		return nil, err
	}

	if err := ps.verifyMessageSignature(msg); err != nil {
		log.WithFields(logFields).Error(err.Error())
		return nil, err
	}

	switch msgType := msg.GetType(); msgType {
	case message.MSG_TYPE_GET_DATASET_POLICY:
		// Bug: allowmultiplecheckouts field remains false in psql table at policy store!
		return ps.processStorageNodePolicyRequest(msg)
		
	case message.MSG_TYPE_PROBE_ACK:
		log.WithFields(logFields).Print("POLICY STORE: received DM acknowledgment from node:", *msg)
		//log.WithFields(logFields).Print("MSG HANDLER IN PS:", msg)
		//		ps.multicastManager.RegisterProbeMessage(msg)

	default:
		log.WithFields(logFields).Errorf("Unknown direct message at policy store: %s\n", msgType)
	}

	// Sign response too
	resp, err := proto.Marshal(&pb.Message{Type: message.MSG_TYPE_OK})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (ps *PolicyStoreCore) processStorageNodePolicyRequest(msg *pb.Message) ([]byte, error) {
	if msg == nil {
		panic("msg is nil!")
	}

	// If the dataset has not been found in the in-memory map, restore it from git.
	if !ps.dsManager.DatasetExists(msg.GetPolicyRequest().GetIdentifier()) {
		newDataset := &pb.Dataset{}
		if !ps.gitDatasetExists(msg.GetSender().GetName(), msg.GetPolicyRequest().GetIdentifier()) {
			newDefaultPolicy := ps.getInitialDatasetPolicy(msg.GetPolicyRequest().GetIdentifier())
			if err := ps.gitStorePolicy(msg.GetSender().GetName(), msg.GetPolicyRequest().GetIdentifier(), newDefaultPolicy); err != nil {
				log.WithFields(logFields).Error(err.Error())
				return nil, err
			}

			// Insert the policy into git
			if err := ps.gitStorePolicy(msg.GetSender().GetName(), msg.GetPolicyRequest().GetIdentifier(), newDefaultPolicy); err != nil {
				log.WithFields(logFields).Error(err.Error())
				return nil, err
			}

			newDataset.Identifier = msg.GetPolicyRequest().GetIdentifier()
			newDataset.Policy = newDefaultPolicy
		} else {
			// The dataset policy exists in Git. Restore the dataset entry
			policyString, err := ps.gitGetDatasetPolicy(msg.GetSender().GetName(), msg.GetPolicyRequest().GetIdentifier())
			if err != nil {
				log.WithFields(logFields).Error(err.Error())
				return nil, err
			}

			b, err := strconv.ParseBool(policyString)
			if err != nil {
				log.WithFields(logFields).Error(err.Error())
				return nil, err
			}

			newDataset.Identifier = msg.GetPolicyRequest().GetIdentifier()
			// TOOO: store the timestamps in git too. Return a complete pb node from git interface?
			newDataset.Policy = &pb.Policy{
				DatasetIdentifier: msg.GetPolicyRequest().GetIdentifier(),
				Content: b, 
				DateCreated: pbtime.Now(), 
				DateUpdated: pbtime.Now()} 
		}

		// Insert the dataset into the collection, given the dataset identifier
		if err := ps.dsManager.InsertDataset(msg.GetPolicyRequest().GetIdentifier(), newDataset); err != nil {
			log.WithFields(logFields).Error(err.Error())
			return nil, err
		}
	} 

	// Add the dataset to the lookup manager
	if err := ps.dsLookupService.InsertDatasetLookupEntry(msg.GetPolicyRequest().GetIdentifier(), msg.GetSender().GetName()); err != nil {
		log.WithFields(logFields).Error(err.Error())
		return nil, err
	}
	
	return ps.pbMarshalDatasetPolicy(msg.GetPolicyRequest().GetIdentifier())
	//return nil, nil
}

// Resolves the deltas in the policy store's dataset and the incoming datasets. It removes the superfluous
// datasets that are no longer stored at the node. Returns a map of the datasets that are new to the policy store. 
func (ps *PolicyStoreCore) resolveDatasetDeltas(ctx context.Context, incomingDatasets map[string]*pb.Dataset) (map[string]*pb.Dataset, error) {
	if incomingDatasets == nil {
		return nil, errors.New("Incoming dataset is nil")
	}

	currentDatasets := ps.dsManager.Datasets()
	deltaSet := make(map[string]*pb.Dataset)

	// Add missing policies. Insert them into the collection and add default policy
	for datasetId, incomingDataset := range incomingDatasets {
		if !ps.dsManager.DatasetExists(incomingDataset.GetIdentifier()) {
			newDataset := &pb.Dataset{
				Identifier: datasetId,
				Policy: ps.getInitialDatasetPolicy(incomingDataset.GetIdentifier()),
			}
			
			if err := ps.dsManager.InsertDataset(incomingDataset.GetIdentifier(), newDataset); err != nil {
				log.WithFields(logFields).Error(err.Error())
				continue
			}
			deltaSet[incomingDataset.GetIdentifier()] = newDataset
		}

		// Add datasets with stale policies
		currentDataset := ps.dsManager.Dataset(datasetId)
		if incomingDataset.GetPolicy().GetVersion() < currentDataset.GetPolicy().GetVersion() {
			currentDataset.Policy.DateUpdated = pbtime.Now()
			deltaSet[datasetId] = currentDataset
		}
	}

	superfluous := make([]string, 0)

	// Find superfluous datasets in the collection and remove them. Use identifier as key
	for _, currentDataset := range currentDatasets {
		found := false
		for _, incomingDataset := range incomingDatasets {
			if currentDataset.GetIdentifier() == incomingDataset.GetIdentifier() {
				found = true
				break
			}
		}

		if found {
			continue
		} else {
			superfluous = append(superfluous, currentDataset.GetIdentifier())
		}
	}

	for _, s := range superfluous {
		ps.dsManager.RemoveDataset(s)
	}

	return deltaSet, nil
}

// Set the initial policy of a dataset
func (ps *PolicyStoreCore) getInitialDatasetPolicy(datasetId string) *pb.Policy {
	return &pb.Policy{
		//Issuer:          	ps.PolicyStoreConfig().Name,
		DatasetIdentifier:  datasetId,
		Content:          	false,
		DateCreated: 		pbtime.Now(),
		DateUpdated:   		pbtime.Now(),
	}
}

// Ifrit gossip handler
func (ps *PolicyStoreCore) gossipHandler(data []byte) ([]byte, error) {
	msg := &pb.Message{}
	if err := proto.Unmarshal(data, msg); err != nil {
		log.WithFields(logFields).Error(err.Error())
		return nil, err
	}

	if err := ps.policyLog.InsertObservedGossip(msg.GetGossipMessage()); err != nil {
		log.WithFields(logFields).Error(err.Error())
	}

	if err := ps.verifyPolicyStoreGossipSignature(msg); err != nil {
		return nil, err
	}

	switch msgType := msg.GetType(); msgType {
	case message.MSG_TYPE_PROBE:
		ps.ifritClient.SetGossipContent(data)	
	default:
		log.WithFields(logFields).Warnf("Unknown gossip message at policy store: %s\n", msgType)
	}

	return []byte(message.MSG_TYPE_OK), nil
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

// TODO: put me into common utils
func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

