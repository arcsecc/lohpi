package policy

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
	"math/rand"
	"context"
	"net"
	"net/http"

	"crypto"
	"crypto/rsa"
	"crypto/x509/pkix"

	"github.com/tomcat-bit/lohpi/internal/comm"
	"github.com/tomcat-bit/lohpi/internal/core/cache"
	"github.com/tomcat-bit/lohpi/internal/core/message"
	"github.com/tomcat-bit/lohpi/internal/core/policy/multicast"
	"github.com/tomcat-bit/lohpi/internal/netutil"
	pb "github.com/tomcat-bit/lohpi/protobuf"
_	"google.golang.org/grpc/peer"

	"github.com/joonnna/workerpool"
	"github.com/golang/protobuf/proto"
	"github.com/go-git/go-git"
	"github.com/go-git/go-git/plumbing/object"
	"github.com/joonnna/ifrit"
	"github.com/gorilla/mux"
	"github.com/golang/protobuf/ptypes/empty"
)

type Config struct {
	// Policy store specific
	PolicyStoreIP				string 		`default:"127.0.1.1:8082"`
	BatchSize 					int 		`default:10`
	ProbeInterval				int32 		`default:10`
	GossipInterval				int32 		`default:10`
	HttpPort					int 		`default:8083`
	MulticastAcceptanceLevel	float64		`default:0.5`

	// Other parameters
	MuxIP						string		`default:"127.0.1.1:8081"`
	LohpiCaAddr 				string		`default:"127.0.1.1:8301"`
	RecIP 						string 		`default:"127.0.1.1:8084"`
	PolicyStoreGitRepository  	string 		`required:"true`
}

type service interface {
	Handshake(*pb.Node)
	StudyList(*empty.Empty)
	SetPolicy(*pb.Policy)
}

/*type multicastConfig struct {
	multicastDirectRecipients int 		// sigma	Dynamic
	multicastInterval time.Duration		// tau	Dynamic
	acceptanceLevel float64				// phi. Constant 
}*/

type PolicyStore struct {
	// Policy store's configuration
	config *Config

	// The underlying Fireflies client
	ifritClient *ifrit.Client

	// MUX's Ifrit ip
	muxIfritIP string

	// Cache manager
	cache *cache.Cache

	// Go-git
	repository *git.Repository

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
	s service

	// gRPC client towards the Mux
	muxClient *comm.MuxGRPCClient

	// Multicast specifics
	multicastManager *multicast.MulticastManager
	multicastConfig *multicast.MulticastConfig

	// Probing specifics
	probeManager *multicast.ProbeManager
	//probeConfig *probe.ProbeConfig

	// Keeps track of when messages are multicasted
	multicastTimer 	*time.Timer

	// HTTP server stuff
	httpListener net.Listener
	httpServer   *http.Server
}

func NewPolicyStore(config *Config) (*PolicyStore, error) {
	c, err := ifrit.NewClient()
	if err != nil {
		return nil, err
	}

	repository, err := initializeGitRepository(config.PolicyStoreGitRepository)
	if err != nil {
		return nil, err
	}

	portString := strings.Split(config.PolicyStoreIP, ":")[1]
	port, err := strconv.Atoi(portString)
	if err != nil {
		return nil, err
	}
	netutil.ValidatePortNumber(&port)
	listener, err := netutil.ListenOnPort(port)
	if err != nil {
		return nil, err
	}

	pk := pkix.Name{
		Locality: []string{listener.Addr().String()},
	}

	cu, err := comm.NewCu(pk, config.LohpiCaAddr)
	if err != nil {
		return nil, err
	}

	s, err := newPolicyStoreGRPCServer(cu.Certificate(), cu.CaCertificate(), cu.Priv(), listener)
	if err != nil {
		return nil, err
	}

	muxClient, err := comm.NewMuxGRPCClient(cu.Certificate(), cu.CaCertificate(), cu.Priv())
	if err != nil {
		return nil, err
	}

	cache := cache.NewCache(c)

	// Listener used for interactions
	httpListener, err := netutil.ListenOnPort(config.HttpPort)
	if err != nil {
		return nil, err
	}

	mc := &multicast.MulticastConfig{
		MulticastDirectRecipients: 1,
		MulticastInterval: 1000 * time.Millisecond,
	}

	multicastManager, err := multicast.NewMulticastManager(c, mc)
	if err != nil {
		return nil, err
	}

	probeManager := multicast.NewProbeManager(c, config.MulticastAcceptanceLevel)

	// TODO: remove some of the variables into other interfaces/packages?
	ps := &PolicyStore{
		ifritClient:  		c,
		repository:   		repository,
		exitChan:     		make(chan bool, 1),
		cache:        		cache,
		cu:           		cu,
		grpcs:		  		s,
		dispatcher: 		workerpool.NewDispatcher(50),
		muxClient:			muxClient,
		config: 			config,
		httpListener:		httpListener,
		multicastManager:	multicastManager,
		probeManager:		probeManager,
	}

	ps.grpcs.Register(ps)

	return ps, nil
}

func (ps *PolicyStore) Start() {
	go ps.grpcs.Start()
	go ps.ifritClient.Start()
	go ps.HttpHandler()
	go ps.probeManager.Start()
	ps.ifritClient.RegisterMsgHandler(ps.messageHandler)
	ps.ifritClient.RegisterGossipHandler(ps.gossipHandler)

	ps.dispatcher.Start()
	log.Println("Policy store running gRPC server at", ps.grpcs.Addr(), "and Ifrit client at", ps.ifritClient.Addr())
	
	// Initialize the Ifrit IP addresses that should be ignored
	// when using Ifrit message passing
	ignoredIPs := make(map[string]string)
	remoteIfritAddr, remoteName, err := ps.ignoreIfritIPAddress(ps.config.MuxIP, "Policy store", ps.ifritClient.Addr())
	if err != nil {
		log.Fatal(err)
	}

	// Make sure we ignore the Mux and compliance engine before
	// multicasting stuff
	ignoredIPs[remoteIfritAddr] = remoteName
	ps.multicastManager.SetIgnoredIfritNodes(ignoredIPs)
	ps.probeManager.SetIgnoredIfritNodes(ignoredIPs)

	// Main event loop for multicasting messages 
	for {
		select {
		// When a probing session starts, it blocks the multicast timer from being reset.
		// This allows us to run policy updates and probing in an orderly fashion
		case <-ps.multicastManager.MulticastTimer().C:
			ps.dispatcher.Submit(func() {
				if ps.probeManager.IsProbing() {
					return
				}

				// Use the latest number of direct recipients (probing sessiong might have changed n)
				//sigma := ps.multicastManager.MulticastConfiguration().MulticastDirectRecipients
				/*members, err := ps.memManager.LruMembers(sigma)
				if err != nil {
					log.Println(err.Error())
					panic(err)
				}*/
				
				if err := ps.multicastManager.Multicast(multicast.LruMembers); err != nil {
					log.Println(err.Error())
				}

				//ps.multicastManager.SetMulticastConfiguration(newMulticastConfiguration)
			})
		}
	}
}

func (ps *PolicyStore) HttpHandler() error {
	r := mux.NewRouter()
	log.Printf("Policy store: started HTTP server on port %d\n", ps.config.HttpPort)

	// Public methods exposed to data users (usually through cURL)
	r.HandleFunc("/probe", ps.probeHandler)

	ps.httpServer = &http.Server{
		Handler: r,
		// use timeouts?
	}

	err := ps.httpServer.Serve(ps.httpListener)
	if err != nil {
		log.Fatalf(err.Error())
		return err
	}
	return nil
}

// Initializes a probing procedures that awaits for n acknowledgements 
// using the current network settings
// TODO: fix queuing 
func (ps *PolicyStore) probeHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if ps.probeManager.IsProbing() {
		fmt.Fprintf(w, "Probing session is already running")
		return
	} else {
		fmt.Fprintf(w, "Starting probing session")
	}

	ps.dispatcher.Submit(func() {
		defer log.Println("Probing session done")

		// Same configuration as when pushing policy updates
		mcConfig := ps.multicastManager.MulticastConfiguration()
		/*members, err := ps.memManager.LruMembers(mcConfig.MulticastDirectRecipients)
		if err != nil {
			log.Println(err.Error())
			return
		}*/

		// Session ID
		id := make([]byte, 4)
    	rand.Read(id)

		// Might not be enough members in the network
		n := len(ps.ifritClient.Members()) - 1 		// Subtract Mux from the set of nodes
		if n <= 0 {
			return
		}

		// Update the probing configuration using the current multicast configuration
		pConfig := multicast.ProbeConfig{
			NumMembers:					n,
			AcceptanceLevel: 			ps.config.MulticastAcceptanceLevel,
			SessionID: 					id,
			MulticastInterval:  		mcConfig.MulticastInterval,
			MulticastDirectRecipients:	mcConfig.MulticastDirectRecipients,
			Mode:						multicast.LruMembers,
		}

		// Probe the network using the current settings
		newMulticastConfig, err := ps.probeManager.ProbeNetwork(pConfig)
		if err != nil {
			log.Println(err.Error())
			return
		}
		
		// Fetch the latest muilticast configuration based on the probing results
		ps.multicastManager.SetMulticastConfiguration(newMulticastConfig)
		log.Println("newMulticastConfig:", newMulticastConfig)
	})
}

// Invoked by ifrit message handler
func (ps *PolicyStore) verifyAcknowledgeMessage(msg *pb.Message) error {
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
		return errors.New("Could not securely verify the integrity of the probe ackowledgment")
	}

	return nil
}

func (ps *PolicyStore) Stop() {
	ps.ifritClient.Stop()
}

// Contacts the given remote address and adds the remote's Ifrit IP address to the list of ignored IP addresses.
// The local name and local address is the local identifiers of the Ifrit client.
func (ps *PolicyStore) ignoreIfritIPAddress(remoteAddr, localName, localAddr string) (string, string, error) {
	conn, err := ps.muxClient.Dial(remoteAddr)
	if err != nil {
		return "", "", err
	}
	defer conn.CloseConn()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second * 10)
	defer cancel()

	r, err := conn.IgnoreIP(ctx, &pb.Node{
		Name: "Policy store",
		Address: ps.ifritClient.Addr(),
	})

	if err != nil {
		log.Fatal(err)
		return "", "", err
	}

	defer conn.CloseConn()

	return r.GetAddress(), r.GetName(), nil
}

// Handshake endpoint for nodes to join the network
func (ps *PolicyStore) Handshake(ctx context.Context, node *pb.Node) (*pb.HandshakeResponse, error) {
	hr := &pb.HandshakeResponse{}
	if !ps.cache.NodeExists(node.GetName()) {
		ps.cache.InsertNodes(node.GetName(), &pb.Node{
			Name: node.GetName(),
			Address: node.GetAddress(),
			Role: node.GetRole(),
			ContactEmail: node.GetContactEmail(),
			Id: node.GetId(),
		})
		hr.Ip = ps.ifritClient.Addr()
		hr.Id = []byte(ps.ifritClient.Id())
		log.Printf("Policy store added %s to map with IP %s\n", node.GetName(), node.GetAddress())
	} else {
		errMsg := fmt.Sprintf("Node '%s' already exists in network\n", node.GetName())
		return nil, errors.New(errMsg)
	}
	return hr, nil
}

// Ifrit message handler
func (ps *PolicyStore) messageHandler(data []byte) ([]byte, error) {
	msg := &pb.Message{}
	if err := proto.Unmarshal(data, msg); err != nil {
		panic(err)
	}

	if err := ps.verifyAcknowledgeMessage(msg); err != nil {
		log.Println(err.Error())
		//return nil, err
	}

	switch msgType := msg.GetType(); msgType {

	// When a node is loaded, it sends its latest study list to the policy store
	case message.MSG_TYPE_SET_STUDY_LIST:
		ps.cache.UpdateStudies(msg.GetSender().GetName(), msg.GetStudies().GetStudies())

	case message.MSG_TYPE_PROBE_ACK:
		//log.Println("POLICY STORE: received DM acknowledgment from node:", *msg)
		ps.probeManager.ProbeChan <- *msg

	default:
		fmt.Printf("Unknown message at policy store: %s\n", msgType)
	}

	return []byte(message.MSG_TYPE_OK), nil
}

// Ifrit gossip handler
func (ps *PolicyStore) gossipHandler(data []byte) ([]byte, error) {
	msg := &pb.Message{}
	if err := proto.Unmarshal(data, msg); err != nil {
		panic(err)
	}

	// TODO: verify message
	/*if err := ps.verifyGossipMessage(msg); err != nil {
		log.Println(err.Error())
	}*/

	switch msgType := msg.GetType(); msgType {
	case message.MSG_TYPE_PROBE:
		ps.probeManager.ProbeChan <- *msg
		ps.ifritClient.SetGossipContent(data)
	default:
		fmt.Printf("Unknown message at policy store: %s\n", msgType)
	}

	return []byte(message.MSG_TYPE_OK), nil
}

// Returns the list of studies stored in the network
func (ps *PolicyStore) StudyList(context.Context, *empty.Empty) (*pb.Studies, error) {
	ps.cache.FetchRemoteStudyLists()
	studies := &pb.Studies{
		Studies: make([]*pb.Study, 0),
	}

	for s := range ps.cache.Studies() {
		study := pb.Study{
			Name: s,
		}

		studies.Studies = append(studies.Studies, &study)
	}
	return studies, nil
}

// Main entry point for setting the policies 
func (ps *PolicyStore) SetPolicy(ctx context.Context, p *pb.Policy) (*empty.Empty, error) {
	// TODO: only REC can set study-centric policies

	ps.dispatcher.Submit(func() {
		_, ok := ps.cache.Studies()[p.GetStudyName()]
		if !ok {
			log.Println("No such study", p.GetStudyName(), "is known to policy store")
			return
		}

		if err := ps.storePolicy(p); err != nil {
			log.Fatalf(err.Error())
		}
		
		if err := ps.commitPolicy(p); err != nil {
			log.Fatalf(err.Error())
		}

		// Submit policy to be commited and gossiped
		ps.multicastManager.PolicyChan <- *p
	})

	// Gossip the policy update to the network
	return &empty.Empty{}, nil
}

// Because the policy store is a part of the Ifrit network, we need to verify its own probing message
// coming from the adjacent nodes.
func (ps *PolicyStore) verifyGossipMessage(msg *pb.Message) error {
	r := msg.GetSignature().GetR()
	s := msg.GetSignature().GetS()

	msg.Signature = nil
	
	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	if !ps.ifritClient.VerifySignature(r, s, data, string(ps.ifritClient.Id())) {
		msg.Signature = &pb.Signature{
			R: r,
			S: s,
		}
		return errors.New("Policy store could not verify the integrity of the gossip message")
	}

	// Restore message
	msg.Signature = &pb.Signature{
		R: r,
		S: s,
	}

	return nil
}

// Stores the given policy on disk
func (ps *PolicyStore) storePolicy(p *pb.Policy) error {
	fullPath := filepath.Join(ps.config.PolicyStoreGitRepository, p.GetFilename())
	return ioutil.WriteFile(fullPath, p.GetContent(), 0644)
}

// Commit the policy model to the Git repository
func (ps *PolicyStore) commitPolicy(p *pb.Policy) error {
	// Get the current worktree
	worktree, err := ps.repository.Worktree()
	if err != nil {
		panic(err)
	}

	// Add the file to the staging area
	_, err = worktree.Add(p.GetFilename())
	if err != nil {
		panic(err)
	}

	status, err := worktree.Status()
	if err != nil {
		panic(err)
	}

	// Check status and abort commit if staging changes don't differ from HEAD.
	// TODO: might need to re-consider this one! What if we need to reorder commits?
	if status.File(p.GetFilename()).Staging == git.Untracked {
		if err := worktree.Checkout(&git.CheckoutOptions{}); err != nil {
			panic(err)
		}
		return nil
	}

	log.Println("Committing...")
	// Commit the file
	// TODO: use RECs attributes when commiting the file
	c, err := worktree.Commit(p.GetFilename(), &git.CommitOptions{
		Author: &object.Signature{
			Name: p.GetIssuer(),
			//Email: "john@doe.org",
			When:  time.Now(),
		},
		Committer: &object.Signature{
			Name: "Policy store",
			//Email: "john@doe.org",
			When:  time.Now(),
		},
	})

	if err != nil {
		return err
	}

	obj, err := ps.repository.CommitObject(c)
	if err != nil {
		return err
	}

	//fmt.Println(obj)
	_ = obj
	return nil
}

// Shut down all resources assoicated with the policy store
func (ps *PolicyStore) Shutdown() {
	log.Println("Shutting down policy store")
	ps.ifritClient.Stop()
}

// Sets up the Git resources in an already-existing Git repository
func initializeGitRepository(path string) (*git.Repository, error) {
	ok, err := exists(path)
	if err != nil {
		log.Fatalf(err.Error())
		return nil, err
	}

	if !ok {
		errMsg := fmt.Sprintf("Directory '%s' does not exist", path)
		return nil, errors.New(errMsg)
	}

	policiesDir := path + "/" + "policies"
	if err := os.MkdirAll(policiesDir, os.ModePerm); err != nil {
		return nil, err
	}

	return git.PlainOpen(path)
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


/*



	/*	msg := &message.NodeMessage {
		MessageType: message.MSG_TYPE_SET_REC_POLICY,
		Study: 		"study_0",
		Filename: 	"model.conf",
		Extras: 	[]byte(`[request_definition]
		r = sub, obj, act
		[policy_definition]
		p = sub, obj, act
		[policy_effect]
		e = some(where (p.eft == allow))
		[matchers]
		m=r.sub.Country==r.obj.Country && r.sub.Network==r.obj.Network && r.sub.Purpose==r.obj.Purpose
		`),
	}*/
