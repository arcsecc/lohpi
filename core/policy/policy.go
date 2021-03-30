package policy

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
	"github.com/arcsecc/lohpi/core/policy/multicast"
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
	// Policy store specific
	Name                     string  `default:"Policy store"`
	Host                     string  `default:"127.0.1.1"`
	BatchSize                int     `default:10`
	GossipInterval           uint32  `default:10`
	Port                     int     `default:8083`
	GRPCPort                 int     `default:8084`
	MulticastAcceptanceLevel float64 `default:0.5`
	NumDirectRecipients      int     `default:"1"`

	// Other parameters
	MuxAddress               string `default:"127.0.1.1:8081"`
	LohpiCaAddr              string `default:"127.0.1.1:8301"`
	RecIP                    string `default:"127.0.1.1:8084"`
	PolicyStoreGitRepository string `required:"true`
}

type PolicyStore struct {
	name string

	// Policy store's configuration
	config     Config
	configLock sync.RWMutex

	// The underlying Fireflies client
	ifritClient *ifrit.Client

	// MUX's Ifrit ip
	muxIfritIP string

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

	// HTTP server stuff
	httpListener net.Listener
	httpServer   *http.Server

	// Datasets in the network
	datasetPolicyMapLock sync.RWMutex
	datasetPolicyMap     map[string]*datasetPolicyMapEntry

	// Fetch the JWK
	ar *jwk.AutoRefresh
}

type datasetPolicyMapEntry struct {
	policy *pb.Policy
	node   string
}

func NewPolicyStore(config Config) (*PolicyStore, error) {
	// Ifrit client
	ifritClient, err := ifrit.NewClient()
	if err != nil {
		return nil, err
	}

	go ifritClient.Start()

	// Local git repository using go-git
	repository, err := initializeGitRepository(config.PolicyStoreGitRepository)
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

	cu, err := comm.NewCu(pk, config.LohpiCaAddr)
	if err != nil {
		return nil, err
	}

	s, err := newPolicyStoreGRPCServer(cu.Certificate(), cu.CaCertificate(), cu.Priv(), listener)
	if err != nil {
		return nil, err
	}

	// In-memory cache that maintains important data
	memCache := cache.NewCache(ifritClient)

	multicastManager, err := multicast.NewMulticastManager(ifritClient, config.MulticastAcceptanceLevel, config.NumDirectRecipients)
	if err != nil {
		return nil, err
	}

	// TODO: remove some of the variables into other interfaces/packages?
	ps := &PolicyStore{
		name:             "policy store", // TODO: more refined name. See X509 common name
		ifritClient:      ifritClient,
		repository:       repository,
		exitChan:         make(chan bool, 1),
		memCache:         memCache,
		cu:               cu,
		grpcs:            s,
		config:           config,
		configLock:       sync.RWMutex{},
		datasetPolicyMap: make(map[string]*datasetPolicyMapEntry),
		multicastManager: multicastManager,
	}

	ps.grpcs.Register(ps)

	return ps, nil
}

func (ps *PolicyStore) Start() error {
	// Start the services
	go ps.grpcs.Start()
	go ps.startHttpServer(fmt.Sprintf(":%d", ps.config.Port))

	// Set Ifrit callbacks
	ps.ifritClient.RegisterMsgHandler(ps.messageHandler)
	ps.ifritClient.RegisterGossipHandler(ps.gossipHandler)

	log.Println("Policy store running gRPC server at", ps.grpcs.Addr(), "and Ifrit client at", ps.ifritClient.Addr())

	multicastInterval := time.Duration(time.Duration(ps.config.GossipInterval) * time.Second)
	mcTimer := time.NewTimer(multicastInterval)

	// Main event loop for multicasting messages
	for {
		select {
		// When a probing session starts, it blocks the multicast timer from being reset.
		// This allows us to run policy updates and probing in an orderly fashion
		case <-mcTimer.C:
			func() {
				defer mcTimer.Reset(multicastInterval)

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
		}
	}

	return nil
}

func (ps *PolicyStore) ifritMembersAddress() []string {
	memMap := ps.memCache.DatasetNodes()
	members := make([]string, 0)
	for _, n := range memMap {
		members = append(members, n.GetIfritAddress())
	}
	return members
}

// Invoked by ifrit message handler
// This should be invoked a couple of seconds after the Ifirt nodes have joined the network
// because the sender's ID might not be known to the recipient.
func (ps *PolicyStore) verifyMessageSignature(msg *pb.Message) error {
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
func (ps *PolicyStore) verifyPolicyStoreGossipSignature(msg *pb.Message) error {
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
func (ps *PolicyStore) probeHandler(w http.ResponseWriter, r *http.Request) {
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

func (ps *PolicyStore) stopProbe(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if !ps.multicastManager.IsProbing() {
		fmt.Fprintf(w, "Probing session is not running")
		return
	} else {
		fmt.Fprintf(w, "Stopping probing session")
	}

	ps.multicastManager.StopProbing()
}

func (ps *PolicyStore) Stop() {
	// TODO: shutdown the servers and so on...
	ps.ifritClient.Stop()
}

// Handshake endpoint for nodes to join the network
func (ps *PolicyStore) Handshake(ctx context.Context, node *pb.Node) (*pb.HandshakeResponse, error) {
	if _, ok := ps.memCache.DatasetNodes()[node.GetName()]; !ok {
		ps.memCache.AddNode(node.GetName(), node)
		log.Infoln("Policy store added %s to map with Ifrit IP %s and HTTPS adrress %s\n",
			node.GetName(), node.GetIfritAddress(), node.GetHttpAddress())
	} else {
		return nil, fmt.Errorf("Policy store: node '%s' already exists in network\n", node.GetName())
	}
	return &pb.HandshakeResponse{
		Ip: ps.ifritClient.Addr(),
		Id: []byte(ps.ifritClient.Id()),
	}, nil
}

// Ifrit message handler
func (ps *PolicyStore) messageHandler(data []byte) ([]byte, error) {
	msg := &pb.Message{}
	if err := proto.Unmarshal(data, msg); err != nil {
		panic(err)
	}

	if err := ps.verifyMessageSignature(msg); err != nil {
		panic(err)
		return nil, err
	}

	switch msgType := msg.GetType(); msgType {
	case message.MSG_TYPE_GET_DATASET_POLICY:
		return ps.pbRespondDatasetPolicy(msg.GetSender().GetName(), msg.GetPolicyRequest())

	case message.MSG_TYPE_PROBE_ACK:
		//log.Println("POLICY STORE: received DM acknowledgment from node:", *msg)
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

// Ifrit gossip handler
func (ps *PolicyStore) gossipHandler(data []byte) ([]byte, error) {
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

// Returns the correct policy assoicated with a dataset to the node that stores it.
func (ps *PolicyStore) pbRespondDatasetPolicy(nodeIdentifier string, policyReq *pb.PolicyRequest) ([]byte, error) {
	if policyReq == nil {
		err := errors.New("Policy request is nil")
		log.Errorln(err.Error())
		return nil, err
	}

	resp := &pb.Message{
		Sender:         ps.pbNode(),
		PolicyResponse: &pb.PolicyResponse{},
	}

	var policy *pb.Policy

	// If dataset is known to git, send it to the node
	if ps.gitDatasetExists(nodeIdentifier, policyReq.GetIdentifier()) {
		// Map entry might be nil because the node was contacted at an earlier stage, so we
		// need to populate the map with the policy again
		if ps.getDatasetPolicy(policyReq.GetIdentifier()) == nil {
			policyText, err := ps.gitGetDatasetPolicy(nodeIdentifier, policyReq.GetIdentifier())
			if err != nil {
				panic(err)
			}

			policy = &pb.Policy{
				Issuer:           ps.name,
				ObjectIdentifier: policyReq.GetIdentifier(),
				Content:          policyText,
			}
			ps.gitStorePolicy(nodeIdentifier, policyReq.GetIdentifier(), policy)
		}
		ps.addDatasetPolicy(policyReq.GetIdentifier(), &datasetPolicyMapEntry{policy, nodeIdentifier})
		policy = ps.getDatasetPolicy(policyReq.GetIdentifier()).policy
	} else {
		policy = &pb.Policy{
			Issuer:           ps.name,
			ObjectIdentifier: policyReq.GetIdentifier(),
			Content:          strconv.FormatBool(false),
		}
		// Store the policy in the in-memory map and in the git log
		ps.gitStorePolicy(nodeIdentifier, policyReq.GetIdentifier(), policy)
		ps.addDatasetPolicy(policyReq.GetIdentifier(), &datasetPolicyMapEntry{policy, nodeIdentifier})
	}

	// Prepare response to node
	resp.PolicyResponse.ObjectPolicy = policy

	data, err := proto.Marshal(resp)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	r, s, err := ps.ifritClient.Sign(data)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	resp.Signature = &pb.MsgSignature{R: r, S: s}
	return proto.Marshal(resp)
}

func (ps *PolicyStore) submitPolicyForDistribution(p *pb.Policy) {
	ps.multicastManager.PolicyChan <- *p
}

// Returns the policy store's dataset identifier-to-policy map
func (ps *PolicyStore) getDatasetPolicyMap() map[string]*datasetPolicyMapEntry {
	ps.datasetPolicyMapLock.RLock()
	defer ps.datasetPolicyMapLock.RUnlock()
	return ps.datasetPolicyMap
}

func (ps *PolicyStore) addDatasetPolicy(id string, entry *datasetPolicyMapEntry) {
	ps.datasetPolicyMapLock.Lock()
	defer ps.datasetPolicyMapLock.Unlock()
	ps.datasetPolicyMap[id] = entry
}

func (ps *PolicyStore) getDatasetPolicy(id string) *datasetPolicyMapEntry {
	ps.datasetPolicyMapLock.RLock()
	defer ps.datasetPolicyMapLock.RUnlock()
	return ps.datasetPolicyMap[id]
}

func (ps *PolicyStore) datasetExists(id string) bool {
	_, ok := ps.getDatasetPolicyMap()[id]
	return ok
}

func (ps *PolicyStore) PsConfig() Config {
	ps.configLock.RLock()
	defer ps.configLock.RUnlock()
	return ps.config
}

// Shut down all resources assoicated with the policy store
func (ps *PolicyStore) Shutdown() {
	log.Println("Shutting down policy store")
	// TODO: shut down servers too
	ps.ifritClient.Stop()
}

func (ps *PolicyStore) pbNode() *pb.Node {
	return &pb.Node{
		Name:         ps.name,
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
