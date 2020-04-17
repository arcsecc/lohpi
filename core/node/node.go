package node

import (
	"fmt"
	"strconv"
_	"io/ioutil"
	"encoding/json"
	"encoding/gob"
	"net/http"
	"bytes"
	"errors"
	"log"
	"crypto/x509/pkix"
	"crypto/tls"
	"firestore/core/node/fuse"
	"firestore/comm"
	"firestore/core/message"

	"github.com/joonnna/ifrit"
	"github.com/spf13/viper"

	logger "github.com/inconshreveable/log15"
)

type Msgtype string 

const (
	MSG_TYPE_NEW_STUDY = "MSG_TYPE_NEW_STUDY"
)
//This is a nice comment fordi vi må dokumentere koden!
var (
	errNoAddr = errors.New("No certificate authority address provided, can't continue")
	logging   = logger.New("module", "node/main")
)

type Node struct {
	// Fuse file system
	fs *fuse.Ptfs
	
	// Underlying ifrit client
	c *ifrit.Client

	// Stringy identifier of this node
	nodeName string

	// The IP address of the Lohpi mux. Used when invoking ifrit.Client.SendTo()
	muxIP string

	// HTTP-related variables
	//Listener   net.Listener
	//httpServer *http.Server
	clientConfig *tls.Config
}

func NewNode(nodeName string) (*Node, error) {
	if err := readConfig(); err != nil {
		panic(err)
	}

	c, err := ifrit.NewClient()
	if err != nil {
		panic(err)
	}

	logger.Info(c.Addr(), " spawned")

	pk := pkix.Name{
		Locality: []string{c.Addr()},
	}

	cu, err := comm.NewCu(pk, viper.GetString("lohpi_ca_addr"))
	if err != nil {
		return nil, err
	}

	// Used for HTTPS requests towards the MUX
	clientConfig := comm.ClientConfig(cu.Certificate(), cu.CaCertificate(), cu.Priv())

	node := &Node {
		nodeName: 		nodeName,
		c: 				c,
		clientConfig: 	clientConfig,
	}

	// Join the network by telling the MUX about its presence.
	// By doing this, the response from the mux is the Ifrit IP address used by Ifrit.Client.SendTo()
	/*if err := node.joinLohpi(viper.GetString("lohpi_mux_addr")); err != nil {
		return nil, err
	}*/

	return node, nil
}

func (n *Node) joinLohpi(muxAddr string) error {
	/*
	URL := "https://" + muxAddr + "/join"
	var msg struct {
		Node 	string 		`json:"node"`
	}

	msg.Node = n.nodeName
	jsonStr, err := json.Marshal(msg)
	if err != nil {
        return err
	}

	req, err := http.NewRequest("POST", URL, bytes.NewBuffer(jsonStr))
	req.Header.Set("Content-Type", "application/json")

	transport := &http.Transport{
		TLSClientConfig: n.clientConfig,
	}
	client := &http.Client{
		Transport: transport,
	}

	response, err := client.Do(req)
    if err != nil {
        return err
	}

	defer response.Body.Close()
	if response.StatusCode != int(http.StatusOK) {
		errMsg := fmt.Sprintf("%s", response.Body)
		return errors.New(errMsg)
	}

	bodyBytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return err
	}

	n.muxIP = bodyBytes*/
	return nil
}

func (n *Node) StartIfritClient() {
	n.c.RegisterMsgHandler(n.messageHandler)
	go n.c.Start()
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
	return n.c
}

func (n *Node) Shutdown() {
	log.Printf("Shutting down...\n")
	n.c.Stop()
	fuse.Shutdown() // might fail...
}

// Main entry point for handling Ifrit direct messaging
func (n *Node) messageHandler(data []byte) ([]byte, error) {
	var msg message.NodeMessage
	err := json.Unmarshal(data, &msg)
	if err != nil {
		panic(err)
	}

	log.Printf("Node %s got message %s\n", n.nodeName, msg.MessageType)

	switch msgType := msg.MessageType; msgType {
	case message.MSG_TYPE_LOAD_NODE: 		
		// Create study files as well, regardless of wether or not the subject exists. 
		// If the study exists, we still add the subject's at the node and link to them using
		// 'ln -s'. The operations performed by this call sets the finite state of the 
		// study. This means that any already existing files are deleted.
		if err := n.fs.FeedBulkData(&msg); err != nil {
			return nil, err
		}

		// TODO: Broadcast/gossip about our data collection?
		// TODO: Only send the updated subset of the data - do not 
		// send the entire thing if we don't have to!
		return n.StudyList()
		
	case message.MSG_TYPE_MUX_HANDSHAKE:
		n.muxIP = msg.MuxIP
		resp := fmt.Sprintf("%sADDRESS_DELIMITER%s", n.nodeName, n.Addr())
		return []byte(resp), nil
		
	case message.MSG_TYPE_GET_STUDY_LIST:
		return n.StudyList()

	case message.MSG_TYPE_GET_NODE_INFO:
		return n.NodeInfo()
	
	case message.MSG_TYPE_GET_DATA:
		return n.StudyData(msg)

	case message.MSG_TYPE_MONITORING_NODE:
		fmt.Println("TODO: implement access monitoring")

	default:
		fmt.Printf("Unknown message type: %s\n", msg.MessageType)
		return []byte("ERROR"), nil
	}

	return []byte(message.MSG_TYPE_OK), nil
}

func (n *Node) NodeInfo() ([]byte, error) {
	str := fmt.Sprintf("Info about node '%s':\n->Ifrit IP: %s\n->Stored studies: %s\n",
	n.nodeName, n.Addr(), n.fs.Studies())
	return []byte(str), nil
}

func (n *Node) Addr() string {
	return n.c.Addr()
}

func (n *Node) NodeName() string {
	return n.nodeName
}

func (n *Node) StudyList() ([]byte, error) {
	// Studies known to this node
	studies := n.fs.Studies()

	// Prepare binary message and send it
	buffer := &bytes.Buffer{}
	encoder := gob.NewEncoder(buffer)
	if err := encoder.Encode(studies); err != nil {
		return nil, err
	}
	bytesMsg := buffer.Bytes()
	return bytesMsg, nil
}

func (n *Node) StudyData(msg message.NodeMessage) ([]byte, error) {
	fmt.Printf("MSG: %s\n", msg)
	return []byte(""), nil
}

func (n *Node) SendPortNumber(nodeName, addr string, muxPort uint) error {
	URL := "https://127.0.1.1:" + strconv.Itoa(int(muxPort)) + "/set_port"
	fmt.Printf("URL:> %s\n", URL)

	var msg struct {
		Node 	string 		`json:"node"`
		Address string 		`json:"address"`
	}

	msg.Node = nodeName
	msg.Address = addr
	jsonStr, err := json.Marshal(msg)
	if err != nil {
        return err
    }

	req, err := http.NewRequest("POST", URL, bytes.NewBuffer(jsonStr))
	req.Header.Set("Content-Type", "application/json")

	transport := &http.Transport{
		TLSClientConfig: n.clientConfig,
	}
	client := &http.Client{Transport: transport}
	response, err := client.Do(req)
    if err != nil {
        return err
	}
	defer response.Body.Close()
	if response.StatusCode != int(http.StatusOK) {
		errMsg := fmt.Sprintf("%s", response.Body)
		return errors.New(errMsg)
	}
	return nil
}

func readConfig() error {
	viper.SetConfigName("lohpi_config")
	viper.AddConfigPath("/var/tmp")
	viper.AddConfigPath(".")
	viper.SetConfigType("yaml")

	err := viper.ReadInConfig()
	if err != nil {
		return err
	}

	// Behavior variables
	viper.SetDefault("fuse_mount", "/home/thomas/go/src/firestore")
	viper.SetDefault("lohpi_ca_addr", "127.0.1.1:8301")
	viper.SetDefault("lohpi_mux_addr", "127.0.1.1:8080")
	viper.SafeWriteConfig()
	return nil
}




