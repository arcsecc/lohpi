package policy

import (
	"errors"
	"fmt"
_	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
//	"math/rand"
	"context"
	"net"
	"net/http"

	"crypto"
	"crypto/rsa"
	"crypto/x509/pkix"

	"github.com/tomcat-bit/lohpi/core/comm"
	"github.com/tomcat-bit/lohpi/core/cache"
	"github.com/tomcat-bit/lohpi/core/message"
	"github.com/tomcat-bit/lohpi/core/policy/multicast"
	"github.com/tomcat-bit/lohpi/core/netutil"
	pb "github.com/tomcat-bit/lohpi/protobuf"
_	"google.golang.org/grpc/peer"

	"github.com/joonnna/workerpool"
	"github.com/golang/protobuf/proto"
	"github.com/go-git/go-git/v5"
_	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/joonnna/ifrit"
	"github.com/golang/protobuf/ptypes/empty"
)

type PolicyStoreConfig struct {
	// Policy store specific
	PolicyStoreIP				string 		`default:"127.0.1.1:8082"`
	BatchSize 					int 		`default:10`
	GossipInterval				int 		`default:10`
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

var (
	policiesStringToken = "policies"
)

type PolicyStore struct {
	// Policy store's configuration
	config *PolicyStoreConfig
	configLock sync.RWMutex

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

	// HTTP server stuff
	httpListener net.Listener
	httpServer   *http.Server
}

func NewPolicyStore(config *PolicyStoreConfig) (*PolicyStore, error) {
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

	// Set a proper port on which the gRPC server listens
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

	// Setup X509 parameters
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

	/*muxClient, err := comm.NewMuxGRPCClient(cu.Certificate(), cu.CaCertificate(), cu.Priv())
	if err != nil {
		return nil, err
	}*/

	// In-memory cache
	cache := cache.NewCache(c)

	t := time.Duration(config.GossipInterval) * time.Second
	multicastManager, err := multicast.NewMulticastManager(c, config.MulticastAcceptanceLevel, t)
	if err != nil {
		return nil, err
	}

	// TODO: remove some of the variables into other interfaces/packages?
	ps := &PolicyStore{
		ifritClient:  		c,
		repository:   		repository,
		exitChan:     		make(chan bool, 1),
		cache:        		cache,
		cu:           		cu,
		grpcs:		  		s,
		dispatcher: 		workerpool.NewDispatcher(50),
		//muxClient:			muxClient,
		config: 			config,
		configLock:			sync.RWMutex{},
		multicastManager:	multicastManager,
	}

	ps.grpcs.Register(ps)

	return ps, nil
}

func (ps *PolicyStore) Start() {
	go ps.grpcs.Start()
	go ps.ifritClient.Start()
	go ps.startHttpListener(ps.config.HttpPort)
	
	// Set Ifrit callbacks
	ps.ifritClient.RegisterMsgHandler(ps.messageHandler)
	ps.ifritClient.RegisterGossipHandler(ps.gossipHandler)

	ps.dispatcher.Start()
	log.Println("Policy store running gRPC server at", ps.grpcs.Addr(), "and Ifrit client at", ps.ifritClient.Addr())
	
	// Initialize the Ifrit IP addresses that should be ignored
	// when using Ifrit message passing
	ignoredIPs := make(map[string]string)
	remoteIfritAddr, remoteName, err := ps.ignoreMuxIfritIPAddress()
	if err != nil {
		log.Fatal(err)
	}

	// Make sure we ignore the Mux before multicasting
	ignoredIPs[remoteIfritAddr] = remoteName
	ps.multicastManager.SetIgnoredIfritNodes(ignoredIPs)

	// Main event loop for multicasting messages 
	for {
		select {
		// When a probing session starts, it blocks the multicast timer from being reset.
		// This allows us to run policy updates and probing in an orderly fashion
		case <-ps.multicastManager.MulticastTimer().C:
			ps.dispatcher.Submit(func() {
				// Reset timer 
				defer func() {
					ps.multicastManager.ResetMulticastTimer()
				}()
				
				if ps.multicastManager.IsProbing() {
					return
				}
				
				if err := ps.multicastManager.Multicast(multicast.LruMembers); err != nil {
					log.Println(err.Error())
				}
			})
		}
	}
}

// Invoked by ifrit message handler
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
		return errors.New("Policy store could not securely verify the gossip  of the message")
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
	ps.ifritClient.Stop()
}

// Contacts the given remote address and adds the remote's Ifrit IP address to the list of ignored IP addresses.
// The local name and local address is the local identifiers of the Ifrit client.
func (ps *PolicyStore) ignoreMuxIfritIPAddress() (string, string, error) {
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
		ps.cache.InsertNode(node.GetName(), &pb.Node{
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

	// ENABLE ME!
	/*if err := ps.verifyMessageSignature(msg); err != nil {
		panic(err)
		return nil, err
	}*/

	switch msgType := msg.GetType(); msgType {

	// When a node is loaded, it sends its latest study list to the policy store
	case message.MSG_TYPE_OBJECT_HEADER_LIST:
		ps.updateObjectHeaders(msg.GetObjectHeaders())

	case message.MSG_TYPE_PROBE_ACK:
		//log.Println("POLICY STORE: received DM acknowledgment from node:", *msg)
		//log.Println("MSG HANDLER IN PS:", msg)
		ps.multicastManager.RegisterProbeMessage(msg)

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

// Returns the list of studies stored in the network
/*
func (ps *PolicyStore) StudyList(context.Context, *empty.Empty) (*pb.ObjectHeaders, error) {
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
}*/

// Main entry point for setting the policies
func (ps *PolicyStore) SetPolicy(ctx context.Context, p *pb.Policy) (*empty.Empty, error) {
	// TODO: only REC can set study-centric policies

	ps.dispatcher.Submit(func() {
		if !ps.cache.ObjectExists(p.GetObjectName()) {
			log.Println("No such object", p.GetObjectName(), "is known to policy store")
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

func (ps *PolicyStore) StoreObjectHeader(ctx context.Context, objectHeader *pb.ObjectHeader) (*empty.Empty, error) {
	ps.dispatcher.Submit(func() {
		objectName := objectHeader.GetName()
		ps.cache.InsertObjectHeader(objectName, objectHeader)
		
		policy := objectHeader.GetPolicy()
		if policy == nil {
			panic(errors.New("Policy must be supplied!"))
		}

		if err := ps.storePolicy(policy); err != nil {
			log.Fatalf(err.Error())
		}
		
		if err := ps.commitPolicy(policy); err != nil {
			log.Fatalf(err.Error())
		}

		// Submit policy to be commited and gossiped
		ps.multicastManager.PolicyChan <- *objectHeader.GetPolicy()
	})

	// Gossip the policy update to the network
	return &empty.Empty{}, nil
}

func (ps *PolicyStore) GetObjectHeaders(ctx context.Context, e *empty.Empty) (*pb.ObjectHeaders, error) {
	objectHeaders := &pb.ObjectHeaders{
		ObjectHeaders: make([]*pb.ObjectHeader, 0),
	}

	for _, objectValue := range ps.cache.ObjectHeaders() {
		objectHeaders.ObjectHeaders = append(objectHeaders.ObjectHeaders, objectValue)
	}
	
	return objectHeaders, nil
}

// Stores the given policy on disk
func (ps *PolicyStore) storePolicy(p *pb.Policy) error {
	// Check if directory exists for the given study and subject
	// Use "git-repo/study/subject/policy/policy.conf" paths 
	dirPath := filepath.Join(ps.config.PolicyStoreGitRepository, policiesStringToken, p.GetObjectName())
	ok, err := exists(dirPath)
	if err != nil {
		log.Fatalf(err.Error())
	}

	// Create study directory if it doesn't exists
	if !ok {
		if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
			return err
		}
	}

/*	fullPath := filepath.Join(ps.config.PolicyStoreGitRepository, policiesStringToken, p.GetObjectName(), p.GetFilename())
	return ioutil.WriteFile(fullPath, p.GetContent(), 0644)*/
	return nil
}

// Commit the policy model to the Git repository
func (ps *PolicyStore) commitPolicy(p *pb.Policy) error {
	// Get the current worktree
	/*worktree, err := ps.repository.Worktree()
	if err != nil {
		panic(err)
	}

	// Add the file to the staging area
	path := filepath.Join(policiesStringToken, p.GetObjectName(), p.GetFilename())
	_, err = worktree.Add(path)
	if err != nil {
		panic(err)
	}

	status, err := worktree.Status()
	if err != nil {
		panic(err)
	}

	// Check status and abort commit if staging changes don't differ from HEAD.
	// TODO: might need to re-consider this one! What if we need to reorder commits?
	if status.File(path).Staging == git.Untracked {
		if err := worktree.Checkout(&git.CheckoutOptions{}); err != nil {
			panic(err)
		}
		return nil
	}

	// Commit the file
	// TODO: use RECs attributes when commiting the file
	c, err := worktree.Commit(path, &git.CommitOptions{
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
	_ = obj*/
	return nil
}

func (ps *PolicyStore) PsConfig() PolicyStoreConfig {
	ps.configLock.RLock()
	defer ps.configLock.RUnlock()
	return *ps.config
}

// Shut down all resources assoicated with the policy store
func (ps *PolicyStore) Shutdown() {
	log.Println("Shutting down policy store")
	ps.ifritClient.Stop()
}

// Updates the internal cache by assigning the object headers' ids to the object headers themselves
// TODO: update only deltas. Sync with node'S implementation
func (ps *PolicyStore) updateObjectHeaders(objectHeaders *pb.ObjectHeaders) {
	for _, header := range objectHeaders.GetObjectHeaders() {
		objectID := header.GetName()
		ps.cache.InsertObjectHeader(objectID, header)
	}
}

// Sets up the Git resources in an already-existing Git repository
func initializeGitRepository(path string) (*git.Repository, error) {
	ok, err := exists(path)
	if err != nil {
		log.Fatalf(err.Error())
		return nil, err
	}

	// Plain init if the repository doesn't exist
	if !ok {
		errMsg := "No such Git repository at " + path
		return nil, errors.New(errMsg)
	} else {
		log.Println("Git repository already exists at", path)
	}

	policiesDir := path + "/" + policiesStringToken
	ok, err = exists(policiesDir)
	if err != nil {
		log.Fatalf(err.Error())
	}

	if !ok {
		if err := os.MkdirAll(policiesDir, os.ModePerm); err != nil {
			return nil, err
		}
	} else {
		log.Println("Policies directory in Git repository already exists at", policiesDir)
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
