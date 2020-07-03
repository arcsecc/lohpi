package policy

import (
	"errors"
	"sort"
	"fmt"
	"math"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
	"context"

	"crypto"
	"crypto/rsa"
	"crypto/x509/pkix"

	"github.com/tomcat-bit/lohpi/internal/comm"
	"github.com/tomcat-bit/lohpi/internal/core/cache"
	"github.com/tomcat-bit/lohpi/internal/core/message"
	"github.com/tomcat-bit/lohpi/internal/netutil"
	pb "github.com/tomcat-bit/lohpi/protobuf"
_	"google.golang.org/grpc/peer"

	"github.com/joonnna/workerpool"
	"github.com/golang/protobuf/proto"
	"github.com/go-git/go-git"
	"github.com/go-git/go-git/plumbing/object"
	"github.com/joonnna/ifrit"
	"github.com/spf13/viper"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/empty"
)

type Config struct {
	MuxIP					string		`default:"127.0.1.1:8080"`
	LohpiCaAddr 			string		`default:"127.0.1.1:8301"`
	RecIP 					string 		`default:"127.0.1.1:8083"`
	BatchSize 				int 		`default:10`
	ProbeInterval			int32 		`default:10`
	GossipInterval			int32 		`default:10`
}

type service interface {
	Handshake(*pb.Node)
	StudyList(*empty.Empty)
	SetPolicy(*pb.Policy)
}

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
	wg       	*sync.WaitGroup
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

	// Policy distribution
	policyQueue chan pb.Policy
	commitQueue chan pb.Policy

	// Multicast stuff
	ignoredIPAddresses map[string]string
	lruNodes map[string]int					// ifrit address -> number of invocations
	recipientPercentage float64
}

func NewPolicyStore(config *Config) (*PolicyStore, error) {
	c, err := ifrit.NewClient()
	if err != nil {
		return nil, err
	}

	repository, err := initializeGitRepository(viper.GetString("policy_store_repo"))
	if err != nil {
		return nil, err
	}

	portString := strings.Split(viper.GetString("policy_store_addr"), ":")[1]
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

	cu, err := comm.NewCu(pk, viper.GetString("lohpi_ca_addr"))
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

	cache, err := cache.NewCache(c)
	if err != nil {
		return nil, err
	}

	ps := &PolicyStore{
		ifritClient:  	c,
		repository:   	repository,
		exitChan:     	make(chan bool, 1),
		wg:           	&sync.WaitGroup{},
		cache:        	cache,
		cu:           	cu,
		grpcs:		  	s,
		dispatcher: 	workerpool.NewDispatcher(3),
		muxClient:		muxClient,
		config: 		config,
		policyQueue:	make(chan pb.Policy, config.BatchSize),
		commitQueue: 	make(chan pb.Policy, config.BatchSize),
		lruNodes: 		make(map[string]int),		// node identifier -> number of times it has been receiving messages
		ignoredIPAddresses: make(map[string]string),
		recipientPercentage: 10.0,			// find a proper value for this one
	}

	ps.grpcs.Register(ps)

	return ps, nil
}

func (ps *PolicyStore) Start() {
	ps.ifritClient.RegisterMsgHandler(ps.messageHandler)
	go ps.grpcs.Start()
	go ps.ifritClient.Start()
	ps.dispatcher.Start()
	log.Println("Policy store running gRPC server at", ps.grpcs.Addr(), "and Ifrit client at", ps.ifritClient.Addr())

	// TODO: fix time configs
	//probeTimer := time.Tick(time.Duration(ps.config.ProbeInterval) * time.Second)
	probeTimer := time.Tick(10 * time.Second)
	gossipTimer := time.Tick(5 * time.Second)
	//gossipTimer := time.Tick(time.Duration(ps.config.GossipInterval) * time.Second)

	ps.initializeLruMap()

	for {
		select {
		case p := <-ps.policyQueue:
			if err := ps.storePolicy(&p); err != nil {
				log.Fatalf(err.Error())
			}

			if err := ps.commitPolicy(&p); err != nil {
				log.Fatalf(err.Error())
			}
		case <-gossipTimer:
			if err := ps.sendBatch(); err != nil {
				log.Fatalf(err.Error())
			}
		case <-probeTimer:
			ps.probeNetwork()
		}
	}
}

func (ps *PolicyStore) Stop() {
}

// Contacts the given HTTP address and ignore its Ifrit address
func (ps *PolicyStore) ignoreIfritIPAddress(remoteAddr, localName, localAddr string) (string, string, error) {
	log.Println("Remote addr:", remoteAddr)
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

func (ps *PolicyStore) messageHandler(data []byte) ([]byte, error) {
	msg := &pb.Message{}
	if err := proto.Unmarshal(data, msg); err != nil {
		panic(err)
	}

	switch msgType := msg.Type; msgType {
	case message.MSG_TYPE_SET_STUDY_LIST:
		ps.cache.UpdateStudies(msg.GetSender().GetName(), msg.GetStudies().GetStudies())

	case message.MSG_TYPE_PROBE_ACK:
		//ps.ps.AcknowledgeMessage(msg)

	default:
		fmt.Printf("Unknown message: %s\n", msgType)
	}

	return []byte(message.MSG_TYPE_OK), nil
}

// Handshake endpoint for nodes to join the network
func (ps *PolicyStore) Handshake(ctx context.Context, node *pb.Node) (*pb.HandshakeResponse, error) {
	hr := &pb.HandshakeResponse{}
	if !ps.cache.NodeExists(node.GetName()) {
		ps.cache.UpdateNodes(node.GetName(), node.GetAddress())
		hr.Ip = ps.ifritClient.Addr()
		hr.Id = []byte(ps.ifritClient.Id())
		//fmt.Printf("Policy store added %s to map with IP %s\n", node.GetName(), node.GetAddress())
	} else {
		errMsg := fmt.Sprintf("Node '%s' already exists in network\n", node.GetName())
		return nil, errors.New(errMsg)
	}
	return hr, nil
}

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

		// Submit policy to be commited and gossiped
		ps.policyQueue <- *p
	})

	// Gossip the policy update to the network
	return &empty.Empty{}, nil
}

// Stores the given policy on disk
func (ps *PolicyStore) storePolicy(p *pb.Policy) error {
	fullPath := filepath.Join(viper.GetString("policy_store_repo"), p.GetFilename())
	return ioutil.WriteFile(fullPath, p.GetContent(), 0644)
}

// Commit the policy model to the Git repository
func (ps *PolicyStore) commitPolicy(p *pb.Policy) error {
	log.Println("Committing...")
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

	// Check status and abort commit if staging changes don't differ from HEAD
	if status.File(p.GetFilename()).Staging == git.Untracked {
		if err := worktree.Checkout(&git.CheckoutOptions{}); err != nil {
			panic(err)
		} 
	}

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

// TODO finish me
func (ps *PolicyStore) sendBatch() error {	
	// Fetch all messages from the queue
	gossipChunks := make([]*pb.GossipMessageBody, 0)
	for p := range ps.policyQueue {
		if len(ps.policyQueue) == 0 {
			break
		}	

		gossipChunk := &pb.GossipMessageBody{
			//string object = 1;          // subject or study. Appliy the policy to the object
    		//uint64 version = 2;         // Version numbering 
			Policy: &p,
			//google.protobuf.Timestamp timestamp = 4; // Time at policy store at the time of arrival
		}

		gossipChunks = append(gossipChunks, gossipChunk)
	}

	gmb := &pb.GossipMessage{
		Sender: 				"Policy store",				// Do we really need it?
		MessageType: 			message.GOSSIP_MSG_TYPE_POLICY,
		Timestamp: 				ptypes.TimestampNow(),
		GossipMessageBody:		gossipChunks,
	}

	data, err := proto.Marshal(gmb)
	if err != nil {
		return err
	}

	// Sign the chunks
	r, s, err := ps.ifritClient.Sign(data)

	// Prepare the message
	msg := &pb.Message{
		Type: message.MSG_TYPE_POLICY_STORE_UPDATE,
		Signature: &pb.Signature{
			R: r,
			S: s,
		},
		GossipMessage: gmb,
	}

	// Marshalled message to be multicasted
	data, err = proto.Marshal(msg)
	if err != nil {
		return nil
	}

	//Multicast the message to a random set of LRU nodes
	members, err := ps.getLruMembers()
	if err != nil {
		return err
	}

	// Multicast messages in parrallel
	ps.networkLock.Lock()
	wg := sync.WaitGroup{}
	for _, m := range members {
		member := m
		wg.Add(1)
		go func() {
			ps.ifritClient.SendTo(member, data)
			wg.Done()
		}()
	}

	wg.Wait()
	ps.networkLock.Unlock()
	return nil 
}


func (ps *PolicyStore) initializeLruMap() error {
	remoteIfritAddr, remoteName, err := ps.ignoreIfritIPAddress(ps.config.MuxIP, "Policy store", ps.ifritClient.Addr())
	if err != nil {
		return err
	}

	// Add ignored IP address 
	ps.ignoredIPAddresses[remoteIfritAddr] = remoteName

	// Initialize the list of LRU members
	for _, c := range ps.ifritClient.Members() {
		if !ps.ipIsIgnored(c) {
			ps.lruNodes[remoteIfritAddr] = 0
		}
	}
	return nil
}

// Update LRU members list by mirroring the underlying Ifrit network membership list
func (ps *PolicyStore) updateLruMembers() {
	tempMap := make(map[string]int)

	// Copy the current map
	for k, v := range ps.lruNodes {
		tempMap[k] = v
	}

	// Hacky...
	ps.lruNodes = make(map[string]int)

	// Update the map according to the underlying Ifrit members list
	for _, ifritMember := range ps.ifritClient.Members() {
		if !ps.ipIsIgnored(ifritMember) {
			if count, ok := tempMap[ifritMember]; !ok {
				ps.lruNodes[ifritMember] = 0
			} else {
				ps.lruNodes[ifritMember] = count
			}
		}
	}
}

// Returns the least recently used members of the group of avaiable recipients
func (ps *PolicyStore) getLruMembers() ([]string, error) {
	ps.updateLruMembers()

	// Fetch some percentage of recipients
	numRecipients := float64(len(ps.lruNodes)) / ps.recipientPercentage
	recipients := make([]string, 0, int(numRecipients))

	// Special case: too few network members
	if numRecipients < 1.0 {
		numRecipients = 1
	}

	if int(numRecipients) > len(ps.lruNodes) {
		return nil, errors.New("numRecipients is greater than the number of possible recipients")
	}

	numRecipients = math.Round(float64(numRecipients))

	// List of counts fetched from the LRU map. Sort it in ascending order so that
	// the members that have sent the least number of times will be used as recipients
	counts := make([]int, 0)
	for  _, value := range ps.lruNodes {
		counts = append(counts, value)
	}

	sort.Ints(counts)
	lowest := counts[0]
	iterations := 0

	// Always keep the number of times a recipient has been contacted as low as possible
	for k, v := range ps.lruNodes {
		if v <= lowest {
			lowest = v
			
			recipients = append(recipients, k)
			iterations += 1
			ps.lruNodes[k] += 1
			if iterations >= int(numRecipients) {
				break
			}
		}
	}
	return recipients, nil
}

// Returns true if the given ip address is ignored, returns false otherwise
func (ps *PolicyStore) ipIsIgnored(ip string) bool {
	_, ok := ps.ignoredIPAddresses[ip]
	return ok
}

// Initializes a probing procedures that awaits for n acknowledgements 
// using the current network settings
func (ps *PolicyStore) probeNetwork() {
	ps.networkLock.Lock()
	defer ps.networkLock.Unlock()

	// Table of recipients-to-ticker entries.
	// Start the clock when we have sent the probe and stop it when the ack returns
	ackTable := make(map[string]time.Time)

	wg := sync.WaitGroup{}
	for _, m := range ps.ifritClient.Members() {
		member := m
		wg.Add(1)
		go func() {
			ch := ps.ifritClient.SendTo(member, []byte(""))
			select {
			case response := <-ch:
				
			}
			wg.Done()
		}()
	}
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

	policiesDir := viper.GetString("policy_store_repo") + "/" + "policies"
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
