package policy

import (
	"errors"
	"path/filepath"
	"time"
	"fmt"
_	"io/ioutil"
	
	log "github.com/sirupsen/logrus"
	"os"
	"sync"
//	"math/rand"
	"context"
	"net"
	"net/http"

	"crypto"
	"crypto/rsa"
	"crypto/x509/pkix"

	"github.com/arcsecc/lohpi/core/comm"
	"github.com/arcsecc/lohpi/core/cache"
	"github.com/arcsecc/lohpi/core/message"
	"github.com/arcsecc/lohpi/core/policy/multicast"
	"github.com/arcsecc/lohpi/core/netutil"
	pb "github.com/arcsecc/lohpi/protobuf"
_	"google.golang.org/grpc/peer"

	"github.com/joonnna/workerpool"
	"github.com/golang/protobuf/proto"
	"github.com/go-git/go-git/v5"
	"github.com/joonnna/ifrit"
)

type Config struct {
	// Policy store specific
	Name						string 		`default:"Policy store"`
	Host						string      `default:"127.0.1.1"`
	BatchSize 					int 		`default:10`
	GossipInterval				int 		`default:10`
	Port						int 		`default:8083`
	GRPCPort					int 		`default:8084`
	MulticastAcceptanceLevel	float64		`default:0.5`
	NumDirectRecipients			int			`default:"1"`

	// Other parameters
	MuxAddress					string		`default:"127.0.1.1:8081"`
	LohpiCaAddr 				string		`default:"127.0.1.1:8301"`
	RecIP 						string 		`default:"127.0.1.1:8084"`
	PolicyStoreGitRepository  	string 		`required:"true`
}

type PolicyStore struct {
	name string

	// Policy store's configuration
	config Config
	configLock sync.RWMutex

	// The underlying Fireflies client
	ifritClient *ifrit.Client

	// MUX's Ifrit ip
	muxIfritIP string

	// Cache manager
	cache *cache.Cache

	// Go-git
	repository *git.Repository
	baseDir string

	// Sync 
	exitChan 	chan bool
	networkLock	sync.Mutex

	// Crypto
	cu         *comm.CryptoUnit
	publicKey  crypto.PublicKey
	privateKey *rsa.PrivateKey

	// Workpool
	dispatcher *workerpool.Dispatcher
	 
	// gRPC service
	grpcs *gRPCServer
	//s service

	// gRPC client towards the Mux
	muxClient *comm.MuxGRPCClient

	// Multicast specifics
	multicastManager *multicast.MulticastManager

	// HTTP server stuff
	httpListener net.Listener
	httpServer   *http.Server

	// Nodes in the network
	nodeMapLock sync.RWMutex
	nodeMap     map[string]*pb.Node

	// Datasets in the network 
	datasetPolicyMapLock sync.RWMutex
	datasetPolicyMap map[string]*datasetPolicyMapEntry
}

type datasetPolicyMapEntry struct {
	policy *pb.Policy
	node string
}

func NewPolicyStore(config Config) (*PolicyStore, error) {
	// Ifrit client
	c, err := ifrit.NewClient()
	if err != nil {
		return nil, err
	}

	// Local git repository using go-git
	repository, err := initializeGitRepository(config.PolicyStoreGitRepository)
	if err != nil {
		return nil, err
	}
	baseDir := filepath.Base(config.PolicyStoreGitRepository)

	listener, err := netutil.ListenOnPort(config.GRPCPort)
	if err != nil {
		return nil, err
	}

	// Setup X509 parameters
	pk := pkix.Name{
		Locality: []string{listener.Addr().String()},
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

	// In-memory cache
	//cache := cache.NewCache(c)

	multicastManager, err := multicast.NewMulticastManager(c, config.MulticastAcceptanceLevel, config.NumDirectRecipients)
	if err != nil {
		return nil, err
	}

	// TODO: remove some of the variables into other interfaces/packages?
	ps := &PolicyStore{
		name:			"policy store", // TODO: more refined name. See X509 common name
		ifritClient:  		c,
		repository:   		repository,
		exitChan:     		make(chan bool, 1),
		//cache:        		cache,
		cu:           		cu,
		grpcs:		  		s,
		dispatcher: 		workerpool.NewDispatcher(50),
		//muxClient:			muxClient,
		config: 			config,
		baseDir: 		baseDir,
		configLock:			sync.RWMutex{},
		nodeMap:     make(map[string]*pb.Node),
		nodeMapLock: sync.RWMutex{},
		datasetPolicyMap: make(map[string]*datasetPolicyMapEntry),
		multicastManager:	multicastManager,
	}

	ps.grpcs.Register(ps)

	return ps, nil
}

func (ps *PolicyStore) Start() error {
	// Synchronize the in-memory maps with the databases
	if err := ps.remoteGitSync(); err != nil {
		log.Errorln(err.Error())
		return err
	}

	if err := ps.syncPolicyMaps(); err != nil {
		log.Errorln(err.Error())
		return err
	}

	// Start the services
	go ps.grpcs.Start()
	go ps.ifritClient.Start()
	go ps.startHttpServer(fmt.Sprintf(":%d", ps.config.Port))

	// Set Ifrit callbacks
	ps.ifritClient.RegisterMsgHandler(ps.messageHandler)
	ps.ifritClient.RegisterGossipHandler(ps.gossipHandler)

	ps.dispatcher.Start()
	log.Println("Policy store running gRPC server at", ps.grpcs.Addr(), "and Ifrit client at", ps.ifritClient.Addr())
	
	// Initialize the Ifrit IP addresses that should be ignored
	// when using Ifrit message passing
/*	ignoredIPs := make(map[string]string)
	remoteIfritAddr, remoteName, err := ps.ignoreMuxIfritIPAddress()
	if err != nil {
		log.Fatal(err)
	}

	// Make sure we ignore the Mux before multicasting
	ignoredIPs[remoteIfritAddr] = remoteName
	ps.multicastManager.SetIgnoredIfritNodes(ignoredIPs)
*/
	multicastInterval := time.Duration(5 * time.Second)
	mcTimer := time.NewTimer(multicastInterval)

	// Main event loop for multicasting messages 
	for {
		select {
		// When a probing session starts, it blocks the multicast timer from being reset.
		// This allows us to run policy updates and probing in an orderly fashion
		case <-mcTimer.C:
			ps.dispatcher.Submit(func() {
				defer mcTimer.Reset(multicastInterval)

				if ps.multicastManager.IsProbing() {
					return
				}
				
				if err := ps.multicastManager.Multicast(&multicast.Config{
					Mode: multicast.RandomMembers, 
					Members: ps.ifritMembersAddress(),
				}); err != nil {
					log.Println(err.Error())
				}
			})
		}
	}

	return nil
}

func (ps *PolicyStore) ifritMembersAddress() []string {
	memMap := ps.StorageNodes()
	members := make([]string, 0)
	for _, n := range memMap {
		members = append(members, n.GetIfritAddress())
	}
	return members
}

// Populates the in-memory maps with the data stored in the postgres database
// TODO: sync db and maps with the state of the nodes
func (ps *PolicyStore) syncPolicyMaps() error {
	return nil
}

// Invoked by ifrit message handler
// FIX ME
func (ps *PolicyStore) verifyMessageSignature(msg *pb.Message) error {
	return nil
	log.Printf("MSG: %+v\n", msg)

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

	ps.dispatcher.Submit(func() {
		if err := ps.multicastManager.ProbeNetwork(multicast.LruMembers); err != nil {
			fmt.Fprintf(w, err.Error())
		}
	})
}

func (ps *PolicyStore) stopProbe(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if !ps.multicastManager.IsProbing() {
		fmt.Fprintf(w, "Probing session is not running")
		return
	} else {
		fmt.Fprintf(w, "Stopping probing session")
	}

	ps.dispatcher.Submit(func() {
		ps.multicastManager.StopProbing()
	})
}

func (ps *PolicyStore) Stop() {
	// TODO: shutdown the servers and so on...
	ps.ifritClient.Stop()
}

// Contacts the given remote address and adds the remote's Ifrit IP address to the list of ignored IP addresses.
// The local name and local address is the local identifiers of the Ifrit client.
/*func (ps *PolicyStore) ignoreMuxIfritIPAddress() (string, string, error) {
	return "", "", nil 
	conn, err := ps.muxClient.Dial(ps.config.MuxIP)
	if err != nil {
		return "", "", err
	}
	defer conn.CloseConn()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second * 10)
	defer cancel()

	r, err := conn.IgnoreIP(ctx, &pb.Node{
		Name: "Policy store",
		IfritAddress: ps.ifritClient.Addr(),
	})

	if err != nil {
		log.Fatal(err)
		return "", "", err
	}

	defer conn.CloseConn()

	return r.GetAddress(), r.GetName(), nil
}*/

// Handshake endpoint for nodes to join the network
func (ps *PolicyStore) Handshake(ctx context.Context, node *pb.Node) (*pb.HandshakeResponse, error) {
	if _, ok := ps.StorageNodes()[node.GetName()]; !ok {
		ps.insertStorageNode(node)
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
		return ps.sendDatasetPolicy(msg.GetSender().GetName(), msg.GetPolicyRequest())

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
func (ps *PolicyStore) sendDatasetPolicy(nodeIdentifier string, pr *pb.PolicyRequest) ([]byte, error) {
	if pr == nil {
		err := errors.New("Policy request is nil")
		log.Errorln(err.Error())
		return nil, err
	}

	resp := &pb.Message{
		Sender: ps.pbNode(),
		PolicyResponse: &pb.PolicyResponse{},
	}

	var policy *pb.Policy

	// If dataset is not contained in the database, set an initial policy to the dataset
	// and return it to the node. The initial policy is "disallow". "Allow" policy should be set from 
	// the policy store client. See the else clause.
	// What happens if the PS has crashed...? Should we set FALSE then too?
	if !ps.datasetExists(pr.GetIdentifier()) {
		policy = &pb.Policy {
			Issuer: ps.name,
			ObjectIdentifier: pr.GetIdentifier(),
			Content: "FALSE",
		}

		// Store the policy in the in-memory map and in the database
		ps.storePolicy(context.Background(), nodeIdentifier, pr.GetIdentifier(), policy)

		// Prepare response to node requesting the policy
		resp.PolicyResponse.ObjectPolicy = policy
	} else {
		// TODO: fetch more complex policies if the dataset is already known to
		// the policy store.
		log.Warnf("Dataset '%s' has already been assigned an initial policy.")
		log.Warnf("In 'func (ps *PolicyStore) sendDatasetPolicy': Implement more sophisticated policy definitions")
	}

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

func (ps *PolicyStore) storePolicy(ctx context.Context, nodeIdentifier, datasetIdentifier string, policy *pb.Policy) error {
	go ps.addDatasetPolicy(datasetIdentifier, &datasetPolicyMapEntry{policy, nodeIdentifier})

	if err := ps.writePolicy(policy); err != nil {
		log.Errorln(err.Error())
	}

	if err := ps.commitPolicy(policy); err != nil {
		log.Errorln(err.Error())
	}

	return nil
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

func (ps *PolicyStore) StorageNodes() map[string]*pb.Node {
	ps.nodeMapLock.RLock()
	defer ps.nodeMapLock.RUnlock()
	return ps.nodeMap
}

func (ps *PolicyStore) insertStorageNode(node *pb.Node) {
	ps.nodeMapLock.Lock()
	defer ps.nodeMapLock.Unlock()
	ps.nodeMap[node.GetName()] = node
}

func (ps *PolicyStore) nodeExists(name string) bool {
	ps.nodeMapLock.RLock()
	defer ps.nodeMapLock.RUnlock()
	_, ok := ps.nodeMap[name]
	return ok
}

func (ps *PolicyStore) pbNode() *pb.Node {
	return &pb.Node{
		Name:    		ps.name,
		IfritAddress: 	ps.ifritClient.Addr(),
		Role:    		"policy store",
		Id: 			[]byte(ps.ifritClient.Id()),
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