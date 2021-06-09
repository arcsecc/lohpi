package policystore

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"github.com/arcsecc/lohpi/core/cache"
	"github.com/arcsecc/lohpi/core/comm"
	"github.com/arcsecc/lohpi/core/message"
	"github.com/arcsecc/lohpi/core/netutil"
	"github.com/arcsecc/lohpi/core/policystore/multicast"
	pb "github.com/arcsecc/lohpi/protobuf"
	"github.com/go-git/go-git/v5"
	"github.com/golang/protobuf/proto"
	"github.com/joonnna/ifrit"
	"github.com/lestrrat-go/jwx/jwk"
	log "github.com/sirupsen/logrus"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

type Config struct {
	Name                     string 
	Host                     string 
	PolicyBatchSize		     int    
	GossipInterval           time.Duration 
	HTTPPort                 int    
	GRPCPort                 int    
	MulticastAcceptanceLevel float64
	DirectRecipients	     int    
	MuxAddress               string 
	LohpiCaAddress           string 
	LohpiCaPort				 int			
	DirectoryServerAddress   string 
	DirectoryServerGPRCPort  int
	GitRepositoryPath 		string
	TLSEnabled				 bool
}

type PolicyStoreCore struct {
	// Policy store's configuration
	config     *Config
	configLock sync.RWMutex

	// The underlying Fireflies client
	ifritClient *ifrit.Client

	// MUX's Ifrit ip
	directoryServerIfritIP string

	// Cache manager
	memCache *cache.Cache

	// Go-git
	repository *git.Repository

	// Sync
	exitChan    chan bool
	networkLock sync.Mutex

	// Crypto
	cu         *comm.CryptoUnit
	publicKey  crypto.PublicKey
	privateKey *rsa.PrivateKey

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

	// Dataset id -> pb policy
	datasetPolicyMap map[string]*pb.Policy
	datasetPolicyMapLock sync.RWMutex
}

func NewPolicyStoreCore(config *Config) (*PolicyStoreCore, error) {
	// Ifrit client
	ifritClient, err := ifrit.NewClient()
	if err != nil {
		return nil, err
	}

	go ifritClient.Start()

	// Local git repository using go-git
	repository, err := initializeGitRepository(config.GitRepositoryPath)
	if err != nil {
		return nil, err
	}

	listener, err := netutil.ListenOnPort(config.GRPCPort)
	if err != nil {
		return nil, err
	}

	// Setup X509 parameters
	pk := pkix.Name{
		Locality:   []string{listener.Addr().String()},
		CommonName: "Policy Store",
	}

	cu, err := comm.NewCu(pk, config.LohpiCaAddress + ":" + strconv.Itoa(config.LohpiCaPort))
	if err != nil {
		return nil, err
	}

	s, err := newPolicyStoreGRPCServer(cu.Certificate(), cu.CaCertificate(), cu.PrivateKey(), listener)
	if err != nil {
		return nil, err
	}

	// In-memory cache
	memCache := cache.NewCache(ifritClient)

	multicastManager, err := multicast.NewMulticastManager(ifritClient, config.MulticastAcceptanceLevel, config.DirectRecipients)
	if err != nil {
		return nil, err
	}

	// TODO: remove some of the variables into other interfaces/packages?
	ps := &PolicyStoreCore{
		ifritClient:      ifritClient,
		repository:       repository,
		exitChan:         make(chan bool, 1),
		memCache:         memCache,
		cu:               cu,
		grpcs:            s,
		config:           config,
		configLock:       sync.RWMutex{},
		datasetPolicyMap: make(map[string]*pb.Policy),
		multicastManager: multicastManager,
		stopBatching:     make(chan bool),
	}

	ps.grpcs.Register(ps)

	// Set Ifrit callbacks
	ps.ifritClient.RegisterMsgHandler(ps.messageHandler)
	ps.ifritClient.RegisterGossipHandler(ps.gossipHandler)

	return ps, nil
}

func (ps *PolicyStoreCore) Start() error {
	// Start the services
	log.Println("Policy store running gRPC server at", ps.grpcs.Addr(), "and Ifrit client at", ps.ifritClient.Addr())
	go ps.grpcs.Start()
	go ps.startHttpServer(fmt.Sprintf(":%d", ps.PolicyStoreConfig().HTTPPort))
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
					return
				}

				if err := ps.multicastManager.Multicast(
					&multicast.Config{
						Mode:    multicast.RandomMembers,
						Members: ps.ifritMembersAddress(),
					}); err != nil {
					log.Println(err.Error())
				}
			}()
			case <-ps.stopBatching:
				// TODO garbage collect timer
				return
		}
	}
}

// Stops the policy batcher.
func (ps *PolicyStoreCore) StopPolicyBatcher() {
	log.Infoln("Stopping policy batcher")
	ps.stopBatching <- true
}

func (ps *PolicyStoreCore) IfritAddress() string {
	return ps.ifritClient.Addr()
}

func (ps *PolicyStoreCore) ifritMembersAddress() []string {
	memMap := ps.memCache.Nodes()
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
		log.Println(err.Error())
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
	// Verify the integrity of the node
	r := msg.GetSignature().GetR()
	s := msg.GetSignature().GetS()

	msg.Signature = nil

	// Marshal it before verifying its integrity
	data, err := proto.Marshal(msg)
	if err != nil {
		log.Println(err.Error())
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
	if _, ok := ps.memCache.DatasetNodes()[node.GetName()]; !ok {
		ps.memCache.AddNode(node.GetName(), node)
		log.Infof("Policy store added node '%s' to map with Ifrit IP '%s'\n",
			node.GetName(), node.GetIfritAddress())
	} else {
		return nil, fmt.Errorf("Policy store: node '%s' already exists in network\n", node.GetName())
	}
	return &pb.HandshakeResponse{
		Ip: ps.ifritClient.Addr(),
		Id: []byte(ps.ifritClient.Id()),
	}, nil
}

// Ifrit message handler
func (ps *PolicyStoreCore) messageHandler(data []byte) ([]byte, error) {
	msg := &pb.Message{}
	if err := proto.Unmarshal(data, msg); err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	if err := ps.verifyMessageSignature(msg); err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	switch msgType := msg.GetType(); msgType {
	case message.MSG_TYPE_GET_DATASET_POLICY:
		// If the dataset has not been found in the in-memory map, ask Git. If it is known to Git, insert it into the map
		// and return the policy. If it is not known to Git, set a default policy, insert it into the and return the policy.
		// TODO: consider simplifying all of this
		if p := ps.getDatasetPolicy(msg.GetPolicyRequest().GetIdentifier()); p == nil {
			if !ps.gitDatasetExists(msg.GetSender().GetName(), msg.GetPolicyRequest().GetIdentifier()) {
				newPolicy, err := ps.getInitialDatasetPolicy(msg.GetPolicyRequest())
				if err != nil {
					log.Errorln(err.Error())
					return nil, err
				}

				if err := ps.gitStorePolicy(msg.GetSender().GetName(), msg.GetPolicyRequest().GetIdentifier(), newPolicy); err != nil {
					log.Errorln(err.Error())
					return nil, err
				}
				ps.setDatasetPolicy(msg.GetPolicyRequest().GetIdentifier(), newPolicy)
				ps.memCache.AddDatasetNode(msg.GetPolicyRequest().GetIdentifier(), msg.GetSender())
			} else {
				policyString, err := ps.gitGetDatasetPolicy(msg.GetSender().GetName(), msg.GetPolicyRequest().GetIdentifier())
				if err != nil {
					log.Errorln(err.Error())
					return nil, err
				}

				b, err := strconv.ParseBool(policyString)
				if err != nil {
					log.Errorln(err.Error())
					return nil, err
				}

				newPolicy := &pb.Policy{
					Issuer:           ps.PolicyStoreConfig().Name,
					DatasetIdentifier: msg.GetPolicyRequest().GetIdentifier(),
					Content:          b,
				}
				ps.setDatasetPolicy(msg.GetPolicyRequest().GetIdentifier(), newPolicy)
				ps.memCache.AddDatasetNode(msg.GetPolicyRequest().GetIdentifier(), msg.GetSender())
			}
		} 
		return ps.pbMarshalDatasetPolicy(msg.GetSender().GetName(), msg.GetPolicyRequest())

	case message.MSG_TYPE_PROBE_ACK:
		log.Println("POLICY STORE: received DM acknowledgment from node:", *msg)
		//log.Println("MSG HANDLER IN PS:", msg)
		//		ps.multicastManager.RegisterProbeMessage(msg)

	default:
		fmt.Printf("Unknown message at policy store: %s\n", msgType)
	}

	// Sign response too
	resp, err := proto.Marshal(&pb.Message{Type: message.MSG_TYPE_OK})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// Set the initial policy of a dataset. 
func (ps *PolicyStoreCore) getInitialDatasetPolicy(policyReq *pb.PolicyRequest) (*pb.Policy, error) {
	if policyReq == nil {
		return nil, errors.New("Policy request is nil")
	}
	
	return &pb.Policy{
		Issuer:           ps.PolicyStoreConfig().Name,
		DatasetIdentifier: policyReq.GetIdentifier(),
		Content:          false,
	}, nil
}

// Ifrit gossip handler
func (ps *PolicyStoreCore) gossipHandler(data []byte) ([]byte, error) {
	msg := &pb.Message{}
	if err := proto.Unmarshal(data, msg); err != nil {
		panic(err)
	}

	if err := ps.verifyPolicyStoreGossipSignature(msg); err != nil {
		return []byte{}, nil
	}

	switch msgType := msg.GetType(); msgType {
	case message.MSG_TYPE_PROBE:
		ps.ifritClient.SetGossipContent(data)
	default:
		fmt.Printf("Unknown message at policy store: %s\n", msgType)
	}

	return []byte(message.MSG_TYPE_OK), nil
}

func (ps *PolicyStoreCore) submitPolicyForDistribution(p *pb.Policy) {
	ps.multicastManager.PolicyChan <- *p
}

// Returns the policy store's dataset identifier-to-policy map
func (ps *PolicyStoreCore) getDatasetPolicyMap() map[string]*pb.Policy {
	ps.datasetPolicyMapLock.RLock()
	defer ps.datasetPolicyMapLock.RUnlock()
	return ps.datasetPolicyMap
}

func (ps *PolicyStoreCore) setDatasetPolicy(id string, entry *pb.Policy) {
	ps.datasetPolicyMapLock.Lock()
	defer ps.datasetPolicyMapLock.Unlock()
	ps.datasetPolicyMap[id] = entry
}

func (ps *PolicyStoreCore) getDatasetPolicy(id string) *pb.Policy {
	ps.datasetPolicyMapLock.RLock()
	defer ps.datasetPolicyMapLock.RUnlock()
	return ps.datasetPolicyMap[id]
}

func (ps *PolicyStoreCore) datasetExists(id string) bool {
	_, ok := ps.getDatasetPolicyMap()[id]
	return ok
}

func (ps *PolicyStoreCore) PolicyStoreConfig() Config {
	ps.configLock.RLock()
	defer ps.configLock.RUnlock()
	return *ps.config
}

// Shut down all resources assoicated with the policy store
func (ps *PolicyStoreCore) Shutdown() {
	log.Println("Shutting down policy store")
	// TODO: shut down servers too
	ps.ifritClient.Stop()
}

func (ps *PolicyStoreCore) pbNode() *pb.Node {
	return &pb.Node{
		//Issuer:       ps.PolicyStoreConfig().Name,
		IfritAddress: ps.ifritClient.Addr(),
		Role:         "policy store",
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

