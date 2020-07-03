package node

import (
	"crypto/ecdsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"log"
	"context"
	"time"

	"github.com/tomcat-bit/lohpi/internal/comm"
//	"github.com/tomcat-bit/lohpi/internal/core/mux"
	"github.com/tomcat-bit/lohpi/internal/core/message"
	"github.com/tomcat-bit/lohpi/internal/core/node/fuse"
	"github.com/golang/protobuf/proto"
	pb "github.com/tomcat-bit/lohpi/protobuf" 

	"github.com/joonnna/ifrit"
	"github.com/spf13/viper"

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
	MuxIP					string		`default:"127.0.1.1:8080"`
	PolicyStoreIP 			string		`default:"127.0.1.1:8082"`
	LohpiCaAddr 			string		`default:"127.0.1.1:8301"`
	RecIP 					string 		`default:"127.0.1.1:8083"`
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
	conf *Config

	// gRPC client towards the Mux
	muxClient *comm.MuxGRPCClient

	// Policy store 
	psClient *comm.PolicyStoreGRPCClient

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

	cu, err := comm.NewCu(pk, viper.GetString("lohpi_ca_addr"))
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

	return &Node{
		nodeName:     	nodeName,
		ifritClient:  	ifritClient,
		muxClient: 		muxClient, 
		conf: 			config,
		psClient: 		psClient,
	}, nil
}

func (n *Node) StartIfritClient() {
	n.ifritClient.RegisterMsgHandler(n.messageHandler)
	n.ifritClient.RegisterGossipHandler(n.gossipHandler)
	go n.ifritClient.Start()
}

func (n *Node) MountFuse() error {
	fs, err := fuse.NewFuseFS(n.nodeName)
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
// TODO: Use go-routines to avoid clients waiting for replies
// TODO: use protobuf
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
		if err := n.fs.FeedBulkData(msg.GetLoad()); err != nil {
			return nil, err
		}

		// Notify the policy store about the newest changes
		// TODO: avoid sending all updates - only send the newest data
		// TODO: remove this because we send the list twice!
		if err := n.sendStudyList(n.PolicyStoreIP); err != nil {
			return nil, err
		}

		// Returns updates studies to the mux
		return n.studyList()

	case message.MSG_TYPE_GET_STUDY_LIST:
		return n.studyList()

	case message.MSG_TYPE_GET_NODE_INFO:
		return n.nodeInfo()

	case message.MSG_TYPE_GET_META_DATA:
		//return n.studyMetaData(msg)

	case message.MSG_TYPE_POLICY_STORE_UPDATE:
		log.Println("Got new policy from policy store!")
		n.setPolicy(msg)

	default:
		fmt.Printf("Unknown message type: %s\n", msg.GetType())
	}

	resp, err := proto.Marshal(&pb.Message{Type: message.MSG_TYPE_OK})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (n *Node) gossipHandler(data []byte) ([]byte, error) {
	log.Println("Gossiper here")
	msg := &pb.Message{}
	if err := proto.Unmarshal(data, msg); err != nil {
		panic(err)
	}

	if err := n.verifyPolicyStoreMessage(msg); err != nil {
		log.Fatalf(err.Error())
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
		n.nodeName, n.address(), n.fs.Studies())
	return []byte(str), nil
}

func (n *Node) address() string {
	return n.ifritClient.Addr()
}

func (n *Node) NodeName() string {
	return n.nodeName
}

func (n *Node) studyList() ([]byte, error) {
	// Studies known to this node
	studies := &pb.Studies{
		Studies: make([]*pb.Study, 0),
	}

	for _, s := range n.fs.Studies() {
		study := pb.Study{
			Name: s,
		}
		studies.Studies = append(studies.Studies, &study)
	}	
	return proto.Marshal(studies)
}

func (n *Node) studyMetaData(msg message.NodeMessage) ([]byte, error) {
	return n.fs.StudyMetaData(msg)
}

func (n *Node) setPolicy(msg *pb.Message) {
	if err := n.verifyPolicyStoreMessage(msg); err != nil {
		log.Fatalf(err.Error())
	}	

	// Determine if the object is a subject or study
	
	// Store the file on disk
	
	// Apply the changes in fuse

	/*fileName := msg.Filename
	fileName = msg.Study + "_model.conf"
	modelText := string(msg.Extras)

	if err := n.fs.SetStudyPolicy(msg.Study, fileName, modelText); err != nil {
		return []byte(message.MSG_TYPE_ERROR), err
	}*/

	// Inspect the content of the 
	
	data, err := proto.Marshal(msg)
	if err != nil {
		log.Fatalf(err.Error())
	}
	n.ifritClient.SetGossipContent(data)
}

func (n *Node) StudyData(msg message.NodeMessage) ([]byte, error) {
	//	requestPolicy := msg.Populator.MetaData.Meta_data_info.PolicyAttriuteStrings()
	fmt.Printf("TODO: Implement StudyData()\n")
	return []byte(""), nil
}

func (n *Node) sendStudyList(addr string) error {
	studies := &pb.Studies{
		Studies: make([]*pb.Study, 0),
	}

	for _, s := range n.fs.Studies() {
		study := pb.Study{
			Name: s,
		}

		studies.Studies = append(studies.Studies, &study)
	}	
	
	msg := &pb.Message{
		Type: message.MSG_TYPE_SET_STUDY_LIST,
		Sender: &pb.Node{
			Name: n.nodeName,
		},
		Studies: studies,
	}
	
	data, err := proto.Marshal(msg)
	if err != nil {
		panic(err) // return here
	}

	// TODO: return error from response instead?
	ch := n.ifritClient.SendTo(addr, data)
	select {
	case response := <-ch:
		if string(response) != message.MSG_TYPE_OK {
			return errors.New("Sending study list failed")
		}
	}
	log.Println("Done sending study list")
	return nil
}

func (n *Node) PolicyStoreHandshake() error {
	conn, err := n.psClient.Dial(viper.GetString("policy_store_addr"))
	if err != nil {
		return err
	}
	defer conn.CloseConn()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	r, err := conn.Handshake(ctx, &pb.Node{
		Name: n.nodeName, 
		Address: n.ifritClient.Addr(),
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
	conn, err := n.muxClient.Dial(viper.GetString("lohpi_mux_addr"))
	if err != nil {
		return err
	}
	
	defer conn.CloseConn()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	r, err := conn.Handshake(ctx, &pb.Node{
		Name: n.nodeName, 
		Address: n.ifritClient.Addr(),
	})
	if err != nil {
		log.Fatal(err)
	}

	n.MuxIP = r.GetIp()
	return nil 
}

func (n *Node) verifyPolicyStoreMessage(msg *pb.Message) error {
	r := msg.GetSignature().GetR()
	s := msg.GetSignature().GetS()

	data, err := proto.Marshal(msg.GetGossipMessage())
	if err != nil {
		return err
	}

	if !n.ifritClient.VerifySignature(r, s, []byte(data), string(n.policyStoreID)) {
		return errors.New("Could not securely verify the integrity of the new policy from policy store")
	}
	return nil
}

// Not pretty at all
func decodePublicKey(data []byte) (*ecdsa.PublicKey, error) {
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, errors.New("failed to parse PEM block containing the key")
	}

	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, err
	}

	switch pub := pub.(type) {
	case *ecdsa.PublicKey:
		return pub, nil
	default:
		break // fall through
	}
	return nil, errors.New("Key type is not RSA")
}
