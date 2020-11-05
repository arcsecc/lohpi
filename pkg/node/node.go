package node

import (
	"bytes"
	"os"
	"path/filepath"
	"crypto/ecdsa"
	"crypto/tls"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"log"
	"context"
	"time"
	"sync"
	"io/ioutil"
	"net"
	"net/http"

	"github.com/tomcat-bit/lohpi/pkg/comm"
	"github.com/tomcat-bit/lohpi/pkg/session"
//	"github.com/tomcat-bit/lohpi/pkg/core/mux"
	"github.com/tomcat-bit/lohpi/pkg/message"
	"github.com/tomcat-bit/lohpi/pkg/node/fuse"
	"github.com/golang/protobuf/proto"
	pb "github.com/tomcat-bit/lohpi/protobuf" 
	"github.com/pkg/xattr"
	"github.com/joonnna/ifrit"

	logger "github.com/inconshreveable/log15"
)

type Msgtype string

const (
	MSG_TYPE_NEW_STUDY = "MSG_TYPE_NEW_STUDY"
)

var (
	errNoAddr = errors.New("No certificate authority address provided, can't continue")
	logging   = logger.New("module", "node/main")
)

// TODO move me somewhere else. ref gossip.go
type gossipMessage struct {
	Content 	[][]byte
	Hash 		[]byte
	Addr	 	string
}

const XATTR_PREFIX = "user."

type Config struct {
	MuxIP					string		`default:"127.0.1.1:8081"`
	PolicyStoreIP 			string		`default:"127.0.1.1:8082"`
	LohpiCaAddr 			string		`default:"127.0.1.1:8301"`
	RecIP 					string 		`default:"127.0.1.1:8084"`
	FileDigesters			int 		`default:20`
	Root					string		`required:true`
	FuseOn					bool		`required:true`
	ServeHttp				bool		`default:false`
	HttpPort				int			`required:false`

	// Fuse configuration
	FuseConfig fuse.Config
}

type objectFile struct {
	path string
	attributes []byte
	content []byte
	err  error
}

type Node struct {
	// Fuse file system
	fs *fuse.Ptfs

	// Underlying ifrit client
	ifritClient *ifrit.Client

	// Stringy identifier of this node
	name string

	// The IP address of the Lohpi mux. Used when invoking ifrit.Client.SendTo()
	MuxIP         string
	PolicyStoreIP string

	// HTTP-related variables
	clientConfig *tls.Config

	// Policy store's public key
	psPublicKey ecdsa.PublicKey

	// Crypto unit
	cu *comm.CryptoUnit

	// Config
	config *Config

	// gRPC client towards the Mux
	muxClient *comm.MuxGRPCClient

	// Policy store 
	psClient *comm.PolicyStoreGRPCClient

	// REC client
	recClient *comm.RecGRPCClient

	// Used for identifying data coming from policy store
	MuxID []byte
	policyStoreID []byte

	// Used to set extended attribute
	attrKey string

	// Remote session handler
	rsHandler *session.Manager

	// Object id -> object header						
	objectHeadersMap  		map[string]*pb.ObjectHeader
	objectHeadersMapLock 	sync.RWMutex

	subjectsMap   	map[string][]string
	subjectsMapLock	sync.RWMutex

	// Directory where data is stored
	rootDir string

	httpListener net.Listener
	httpServer   *http.Server
}

func NewNode(name string, config *Config) (*Node, error) {
	ifritClient, err := ifrit.NewClient()
	if err != nil {
		panic(err)
	}

	logger.Info(ifritClient.Addr(), " spawned")

	pk := pkix.Name{
		//CommonName: nodeName,
		Locality: []string{ifritClient.Addr()},
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

	recClient, err := comm.NewRecClient(cu.Certificate(), cu.CaCertificate(), cu.Priv())
	if err != nil {
		return nil, err
	}

	node := &Node{
		name:     		name,
		ifritClient:  	ifritClient,
		muxClient: 		muxClient, 
		recClient:		recClient,
		config: 		config,
		psClient: 		psClient,
		attrKey:		"XATTR",
		
		objectHeadersMap:		make(map[string]*pb.ObjectHeader),
		objectHeadersMapLock: 	sync.RWMutex{},
		
		subjectsMap:			make(map[string][]string),
		subjectsMapLock:		sync.RWMutex{},

		rootDir: 			config.Root,
		rsHandler:			session.NewManager(),
	}

	if config.FuseOn {
		fs, err := fuse.NewFuseFS(name, &config.FuseConfig)
		if err != nil {
			return nil, err
		}
		node.fs = fs
	}

	if config.ServeHttp {
		if err := node.setHttpListener(); err != nil {
			return nil, err
		}

		go node.startHttpHandler()
	}

	return node, nil
}

func (n *Node) Start() {
	n.ifritClient.RegisterMsgHandler(n.messageHandler)
	n.ifritClient.RegisterGossipHandler(n.gossipHandler)
	go n.ifritClient.Start()
}

func (n *Node) FireflyClient() *ifrit.Client {
	return n.ifritClient
}

func (n *Node) Shutdown() {
	log.Printf("Shutting down Lohpi node\n")
	n.ifritClient.Stop()
	fuse.Shutdown() // might fail...
}

// Main entry point for handling Ifrit direct messaging
func (n *Node) messageHandler(data []byte) ([]byte, error) {
	msg := &pb.Message{}
	if err := proto.Unmarshal(data, msg); err != nil {
		panic(err)
	}

	log.Printf("Node '%s' got message %s\n", n.name, msg.GetType())

	switch msgType := msg.Type; msgType {
	case message.MSG_TYPE_LOAD_NODE:
		// Create study files as well, regardless of wether or not the subject exists.
		// If the study exists, we still add the subject's at the node and link to them using
		// 'ln -s'. The operations performed by this call sets the finite state of the
		// study. This means that any already existing files are deleted.
/*		if err := n.generateData(msg.GetLoad()); err != nil {
			panic(err)*/
		//	return nil, err
		//}

		// Send REC the newest updates
		// TODO MOVE ME
		/*if err := n.sendRecUpdate(msg.GetLoad().GetObjectHeader()); err != nil {
			panic(err)
		}*/

		// Notify the policy store about the newest changes
		// TODO: avoid sending all updates - only send the newest data
		// TODO: remove this because we send the list twice!
		/*if err := n.sendObjectHeaderList(n.PolicyStoreIP); err != nil {
			panic(err)
		}*/

		// TODO: send PS initial policy

		if err := n.sendObjectHeaderList(n.MuxIP); err != nil {
			panic(err)
		}

	case message.MSG_TYPE_GET_OBJECT_HEADER_LIST:
		return n.marshalledObjectHeaderList()

	case message.MSG_TYPE_GET_NODE_INFO:
		return n.nodeInfo()

	case message.MSG_TYPE_GET_META_DATA:
		//return n.studyMetaData(msg)

	case message.MSG_TYPE_GET_OBJECT:
		return n.objectData(msg)

	case message.MSG_TYPE_POLICY_STORE_UPDATE:
		log.Println("Got new policy from policy store!")
		n.setPolicy(msg)

	case message.MSG_TYPE_PROBE:
		return n.acknowledgeProbe(msg, data)

	default:
		fmt.Printf("Unknown message type: %s\n", msg.GetType())
	}

	return n.acknowledgeMessage()
}

func (n *Node) acknowledgeMessage() ([]byte, error) {
	msg := &pb.Message{Type: message.MSG_TYPE_OK}
	data, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}

	r, s, err := n.ifritClient.Sign(data)
	if err != nil {
		panic(err)
	}
	
	msg.Signature = &pb.MsgSignature{R: r, S: s}
	return proto.Marshal(msg)
}

func (n *Node) gossipHandler(data []byte) ([]byte, error) {
	msg := &pb.Message{}
	if err := proto.Unmarshal(data, msg); err != nil {
		panic(err)
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
	str := fmt.Sprintf("Info about node '%s':\n-> Ifrit IP: %s\n-> Stored studies: %s\n",
		n.name, n.address(), n.objectHeaders())
	return []byte(str), nil
}

func (n *Node) address() string {
	return n.ifritClient.Addr()
}

func (n *Node) NodeName() string {
	return n.name
}

func (n *Node) studyMetaData(msg message.NodeMessage) ([]byte, error) {
	//return n.fs.StudyMetaData(msg)
	return []byte{}, nil
}

func (n *Node) setPolicy(msg *pb.Message) {
	/*if err := n.verifyPolicyStoreMessage(msg); err != nil {
		log.Println(err.Error())
		return
	}
/*
	go func() {
		data, err := proto.Marshal(msg)
			if err != nil {
				log.Fatalf(err.Error())
			}
		n.ifritClient.SetGossipContent(data)
	}()*/

	// Determine if the object is a subject or study
	//gossipMessage := msg.GetGossipMessage()

	// Fetch the gossip chunks and apply the segments that 
	// concern this node
/*	policyChunks := msg.GetGossipMessage().GetGossipMessageBody()
	for _, chunk := range policyChunks {
		if !n.objectHeaderExists(chunk.GetObjectId()) {
			continue
		}

		if err := n.set
	}*/
}

// Move to bootstrap module?
func (n *Node) sendRecUpdate(header *pb.ObjectHeader) error {
	conn, err := n.recClient.Dial(n.config.RecIP)
	if err != nil {
		log.Println(err.Error())
		return err
	}
	defer conn.CloseConn()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	
	// Metadata to be sent to REC
	_, err = conn.StoreObjectHeader(ctx, header)

	if err != nil {
		log.Println(err.Error())
		return err
	}
	
	return nil 
}

func (n *Node) objectData(msg *pb.Message) ([]byte, error) {
	// TODO: verify the policy attributes here..?!
	/*log.Println("Client request:", msg.GetDataUserRequest())

	// Enter the 'studies' (objects) sub-tree. Walk every subject folder in the object file tree
	dirPath := fmt.Sprintf("%s/%s/%s/subjects", n.rootDir, STUDIES_DIR, msg.GetDataUserRequest().GetObjectName())
	log.Println("dirPath:", dirPath)

	// TODO: check if object header exist
	clientAttr := msg.GetDataUserRequest().GetClient().GetPolicyAttribute()
	//log.Println("clientAttr:", clientAttr)

	objectFiles, err := n.findObjectFiles(clientAttr, dirPath)
	if err != nil {
		panic(err)
		return nil, err
	}

	// TODO: optimize checking out files to client. Simple solution for now
	/*objectId := msg.GetDataUserRequest().GetObjectName()
	e := &session.Entry{Client: msg.GetDataUserRequest().GetClient()}*/

/*	if err := n.rsHandler.CheckoutObjct(objectId, e); err != nil {
		panic(err)
	}*/

	// checkout files to the client. 
	// The client subscribes on file policies.
	// depending on the enforcement mode, 

	return proto.Marshal(&pb.Message{})
}

func (n *Node) findObjectFiles(clientAttr []byte, root string) (*pb.ObjectFiles, error) {
	files := make([]*pb.ObjectFile, 0)
	
	done := make(chan struct{})
	defer close(done)

	paths, errc := n.walkDirectoryTree(done, root)
	log.Println("ROOT:", root)

	// Start a fixed number of goroutines to read and digest files.
	c := make(chan objectFile) // HLc
	var wg sync.WaitGroup
	wg.Add(n.config.FileDigesters)
	for i := 0; i < n.config.FileDigesters; i++ {
		go func() {
			n.digester(done, paths, c, clientAttr) // HLc
			wg.Done()
		}()
	}
	go func() {
		wg.Wait()
		close(c) // HLc
	}()
	// End of pipeline. OMIT

	for r := range c {
		if r.err != nil {
			panic(r.err)
			return nil, r.err
		}
		
		f := &pb.ObjectFile{
			Path: r.path,
			Attributes: r.attributes,
			Content: r.content,
		}
		files = append(files, f)
	}
	// Check whether the Walk failed.
	if err := <-errc; err != nil { // HLerrc
		panic(err)
		return nil, err
	}

	//log.Println("FILES:", files)
	return &pb.ObjectFiles{ObjectFiles: files}, nil
}

// digester reads path names from paths and sends digests of the corresponding
// files on c until either paths or done is closed.
func (n *Node) digester(done <-chan struct{}, paths <-chan string, c chan<- objectFile, clientAttr []byte) {
	for path := range paths { // HLpaths
		filexAttr, _ := xattr.Get(path, XATTR_PREFIX + n.attrKey)
		
		log.Println("filexAttr:", filexAttr, "clientAttr:", clientAttr)

		if bytes.Compare(filexAttr, clientAttr) != 0 {
			log.Println("Not equal. Continuing...")
			continue
		}

		log.Println("Reading file", path)
		data, err := ioutil.ReadFile(path)
		
		select {
		case c <- objectFile{path, filexAttr, data, err}:
			log.Println("len(paths):", len(paths))
		case <-done:
			return
		}
	}
}

// Starts at 'root' and reads all filenames
func (n *Node) walkDirectoryTree(done <-chan struct{}, root string) (<-chan string, <-chan error) {
	paths := make(chan string)
	errc := make(chan error, 1)
	go func() { // HL
		// Close the paths channel after Walk returns.
		defer close(paths) // HL
		// No select needed for this send, since errc is buffered.
		errc <- filepath.Walk(root, func(path string, info os.FileInfo, err error) error { // HL
			if err != nil {
				panic(err)
				return err
			}

			// Ensure that the path is a symlink
			if info.Mode().IsDir() {
				//panic(errors.New(path +  " is not a symlink file!"))
				return nil
			}

			select {
			case paths <- path: 		
			case <-done: // HL
				return errors.New("walk canceled")
			}
			return nil
		})
	}()
	return paths, errc
}

// Sends the newest object header list to the Ifrit node at 'addr'
func (n *Node) sendObjectHeaderList(addr string) error {
	data, err := n.marshalledObjectHeaderList()
	if err != nil {
		panic(err)
	}

	ch := n.ifritClient.SendTo(addr, data)
	select {
	case response := <-ch:
		msgResp := &pb.Message{}
		if err := proto.Unmarshal(response, msgResp); err != nil {
			panic(err)
		}

		// Verify signature too
		if string(msgResp.GetType()) != message.MSG_TYPE_OK {
			panic(errors.New("Sending study list failed"))
		}
	}

	return nil
}

// Returns a signed and marshalled list of object headers
func (n *Node) marshalledObjectHeaderList() ([]byte, error) {
	objectHeaders := &pb.ObjectHeaders{
		ObjectHeaders: make([]*pb.ObjectHeader, 0),
	}

	for _, header := range n.objectHeaders() {
		objectHeaders.ObjectHeaders = append(objectHeaders.ObjectHeaders, header)
	}	
	
	msg := &pb.Message{
		Type: message.MSG_TYPE_OBJECT_HEADER_LIST,
		Sender: &pb.Node{
			Name: n.name, 
			Address: n.ifritClient.Addr(),
			Role: "Storage node",
			Id:	[]byte(n.ifritClient.Id()),
		},
		ObjectHeaders: objectHeaders,
	}
	
	data, err := proto.Marshal(msg)
	if err != nil {
		panic(err) // return here
	}

	// Sign the message
	r, s, err := n.ifritClient.Sign(data)
	if err != nil {
		panic(err)
	}

	// Append the signature to the message
	msg.Signature = &pb.MsgSignature{R: r, S: s}
	return proto.Marshal(msg)
}

// Acknowledges the given probe message.  TODO MORE HERE
func (n *Node) acknowledgeProbe(msg *pb.Message, d []byte) ([]byte, error) {
	if err := n.verifyPolicyStoreMessage(msg); err != nil {
		panic(err)
	}

	// Spread the probe message onwards through the network
	gossipContent, err := proto.Marshal(msg)
	if err != nil {
		log.Fatal(err)
	}

	n.ifritClient.SetGossipContent(gossipContent)

	// Acknowledge the probe message
	resp := &pb.Message{
		Type: message.MSG_TYPE_PROBE_ACK,
		Sender: &pb.Node{
			Name: n.name, 
			Address: n.ifritClient.Addr(),
			Role: "Storage node",
			Id:	[]byte(n.ifritClient.Id()),
		},
		Probe: msg.GetProbe(),
	}

	data, err := proto.Marshal(resp)
	if err != nil {
		panic(err)
	}

	// Sign the acknowledgment response
	r, s, err := n.ifritClient.Sign(data)
	if err != nil {
		panic(err)
	}
	
	// Message with signature appended to it
	resp = &pb.Message{
		Type: message.MSG_TYPE_PROBE_ACK,
		Sender: &pb.Node{
			Name: n.name, 
			Address: n.ifritClient.Addr(),
			Role: "Storage node",
			Id:	[]byte(n.ifritClient.Id()),
		},
		Signature: &pb.MsgSignature{
			R: r,
			S: s,
		},
		Probe: msg.GetProbe(),
	}

	data, err = proto.Marshal(resp)
	if err != nil {
		panic(err)
	}

	log.Println(n.name, "sending ack to Policy store")
	n.ifritClient.SendTo(n.PolicyStoreIP, data)
	return nil, nil
}

func (n *Node) PolicyStoreHandshake() error {
	conn, err := n.psClient.Dial(n.config.PolicyStoreIP)
	if err != nil {
		return err
	}
	defer conn.CloseConn()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second * 20)
	defer cancel()

	r, err := conn.Handshake(ctx, &pb.Node{
		Name: n.name, 
		Address: n.ifritClient.Addr(),
		Role: "Storage node",
		Id:	[]byte(n.ifritClient.Id()),
	})
	if err != nil {
		log.Fatal(err)
	}

	defer conn.CloseConn()

	n.PolicyStoreIP = r.GetIp()
	n.policyStoreID = r.GetId()
	return nil 
}

func (n *Node) MuxHandshake() error {
	conn, err := n.muxClient.Dial(n.config.MuxIP)
	if err != nil {
		return err
	}
	
	defer conn.CloseConn()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second * 20)
	defer cancel()

	r, err := conn.Handshake(ctx, &pb.Node{
		Name: n.name, 
		Address: n.ifritClient.Addr(),
		Role: "Storage node",
		Id:	[]byte(n.ifritClient.Id()),
	})
	if err != nil {
		log.Fatal(err)
	}

	n.MuxIP = r.GetIp()
	n.MuxID = r.GetId()
	return nil 
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

func (n *Node) insertObjectHeader(objectId string, header *pb.ObjectHeader) {
	n.objectHeadersMapLock.Lock()
	defer n.objectHeadersMapLock.Unlock()
	n.objectHeadersMap[objectId] = header
}

func (n *Node) objectHeaders() map[string]*pb.ObjectHeader {
	n.objectHeadersMapLock.RLock()
	defer n.objectHeadersMapLock.RUnlock()
	return n.objectHeadersMap
}

// Test functions below. Not to be used in production
func (n *Node) eraseObjectsFromSubject(subjectId string) {
	n.subjectsMapLock.RLock()
	defer n.subjectsMapLock.RUnlock()
	n.subjectsMap[subjectId] = make([]string, 0)
}

func (n *Node) insertSubject(subject string, objectId string) {
	n.subjectsMapLock.RLock()
	defer n.subjectsMapLock.RUnlock()
	if n.subjectsMap[subject] == nil {
		n.subjectsMap[subject] = make([]string, 0)
	}

	n.subjectsMap[subject] = append(n.subjectsMap[subject], objectId)
}

func (n *Node) objectHeaderExists(objectId string) bool {
	n.objectHeadersMapLock.RLock()
	defer n.objectHeadersMapLock.RUnlock()
	_, ok := n.objectHeadersMap[objectId] 
	return ok
}

func (n *Node) deleteObjectHeader(objectId string) {
	n.objectHeadersMapLock.Lock()
	defer n.objectHeadersMapLock.Unlock()
	_, ok := n.objectHeadersMap[objectId] 
	if ok {
		delete(n.objectHeadersMap, objectId);
	}
}

func (n *Node) setInitialPolicy(h *pb.ObjectHeader) error {
	// TOOD: verify signature of mux's message
	return nil 
}

func (n *Node) pbNode() *pb.Node {
	return &pb.Node{
		Name: n.NodeName(), 
		Address: n.ifritClient.Addr(),
		Role: "storage node", 
		//ContactEmail
		Id: []byte(n.ifritClient.Id()),
	}
}