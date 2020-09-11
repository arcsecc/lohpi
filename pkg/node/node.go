package node

import (
	"crypto/ecdsa"
	"crypto/tls"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"log"
	"context"
	"time"

	"github.com/tomcat-bit/lohpi/pkg/comm"
//	"github.com/tomcat-bit/lohpi/pkg/core/mux"
	"github.com/tomcat-bit/lohpi/pkg/message"
	"github.com/tomcat-bit/lohpi/pkg/node/fuse"
	"github.com/golang/protobuf/proto"
	pb "github.com/tomcat-bit/lohpi/protobuf" 

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

type Config struct {
	MuxIP					string		`default:"127.0.1.1:8081"`
	PolicyStoreIP 			string		`default:"127.0.1.1:8082"`
	LohpiCaAddr 			string		`default:"127.0.1.1:8301"`
	RecIP 					string 		`default:"127.0.1.1:8084"`

	// Fuse configuration
	FuseConfig fuse.Config
}

type Node struct {
	// Fuse file system
	fs *fuse.Ptfs

	// Underlying ifrit client
	ifritClient *ifrit.Client

	// Stringy identifier of this node
	nodeName string

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
}

func NewNode(nodeName string, config *Config) (*Node, error) {
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

	return &Node{
		nodeName:     	nodeName,
		ifritClient:  	ifritClient,
		muxClient: 		muxClient, 
		recClient:		recClient,
		config: 		config,
		psClient: 		psClient,
	}, nil
}

func (n *Node) StartIfritClient() {
	n.ifritClient.RegisterMsgHandler(n.messageHandler)
	n.ifritClient.RegisterGossipHandler(n.gossipHandler)
	go n.ifritClient.Start()
}

func (n *Node) MountFuse() error {
	fs, err := fuse.NewFuseFS(n.nodeName, &n.config.FuseConfig)
	if err != nil {
		return err
	}
	n.fs = fs
	return nil
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

	log.Printf("Node '%s' got message %s\n", n.nodeName, msg.GetType())

	switch msgType := msg.Type; msgType {
	case message.MSG_TYPE_LOAD_NODE:
		// Create study files as well, regardless of wether or not the subject exists.
		// If the study exists, we still add the subject's at the node and link to them using
		// 'ln -s'. The operations performed by this call sets the finite state of the
		// study. This means that any already existing files are deleted.
		if err := n.generateData(msg.GetLoad()); err != nil {
			log.Fatal(err)
		//	return nil, err
		}

		// Send REC the newest updates
		// TODO MOVE ME
		/*if err := n.sendRecUpdate(msg.GetLoad().GetObjectHeader()); err != nil {
			panic(err)
		}*/

		// Notify the policy store about the newest changes
		// TODO: avoid sending all updates - only send the newest data
		// TODO: remove this because we send the list twice!
		if err := n.sendObjectHeaderList(n.PolicyStoreIP); err != nil {
			panic(err)
		}

		if err := n.sendObjectHeaderList(n.MuxIP); err != nil {
			panic(err)
		}

	case message.MSG_TYPE_GET_OBJECT_HEADER_LIST:
		return n.marshalledObjectHeaderList()

	case message.MSG_TYPE_GET_NODE_INFO:
		return n.nodeInfo()

	case message.MSG_TYPE_GET_META_DATA:
		//return n.studyMetaData(msg)

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
		n.nodeName, n.address(), n.fs.ObjectHeaders())
	return []byte(str), nil
}

func (n *Node) address() string {
	return n.ifritClient.Addr()
}

func (n *Node) NodeName() string {
	return n.nodeName
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

	// Apply the changes in fuse

	/*fileName := msg.Filename
	fileName = msg.Study + "_model.conf"
	modelText := string(msg.Extras)*

	// Fetch the gossip chunks and apply the segments that 
	// concern this node
	policyChunks := msg.GetGossipMessage().GetGossipMessageBody()
	log.Println("Len of batch:", len(policyChunks))
	for _, chunk := range policyChunks {
		policy := chunk.GetPolicy()
		//fileName := policy.GetFilename()
		//policyText := policy.GetContent()
		obectName := policy.GetObjectName()

		if !n.fs.ObjectExists(obectName) {
			continue
		}

		/*if err := n.fs.SetStudyPolicy(studyName, fileName, string(policyText)); err != nil {
			log.Println(err.Error())
		}*
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


func (n *Node) StudyData(msg message.NodeMessage) ([]byte, error) {
	//	requestPolicy := msg.Populator.MetaData.Meta_data_info.PolicyAttriuteStrings()
	fmt.Printf("TODO: Implement StudyData()\n")
	return []byte(""), nil
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

	for _, header := range n.fs.ObjectHeaders() {
		objectHeaders.ObjectHeaders = append(objectHeaders.ObjectHeaders, header...)
	}	
	
	msg := &pb.Message{
		Type: message.MSG_TYPE_OBJECT_HEADER_LIST,
		Sender: &pb.Node{
			Name: n.nodeName, 
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
			Name: n.nodeName, 
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
			Name: n.nodeName, 
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

	log.Println(n.nodeName, "sending ack to Policy store")
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
		Name: n.nodeName, 
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
		Name: n.nodeName, 
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