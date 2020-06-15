package node

import (
	"fmt"
	_"crypto"
	"crypto/x509"
	"crypto/ecdsa"
_	"crypto/sha256"
_	"strconv"
_	"io/ioutil"
	"encoding/json"
	"encoding/gob"
	"encoding/pem"
	"net/http"
	"bytes"
	"errors"
	"log"
	"io/ioutil"
	"crypto/tls"
	"firestore/core/node/fuse"
	"firestore/core/message"
	"firestore/comm"

	"crypto/x509/pkix"
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

type Node struct {
	// Fuse file system
	fs *fuse.Ptfs
	
	// Underlying ifrit client
	ifritClient *ifrit.Client

	// Stringy identifier of this node
	nodeName string

	// The IP address of the Lohpi mux. Used when invoking ifrit.Client.SendTo()
	MuxIP 			string
	PolicyStoreIP	string

	// HTTP-related variables
	clientConfig *tls.Config

	// Policy store's public key
	psPublicKey 	ecdsa.PublicKey

	// Crypto unit
	cu				*comm.CryptoUnit
}

func NewNode(nodeName string) (*Node, error) {
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
	clientConfig := comm.ClientConfig(cu.Certificate(), cu.CaCertificate(), cu.Priv())

	return &Node {
		nodeName: 		nodeName,
		ifritClient:	ifritClient,
		clientConfig: 	clientConfig,
	}, nil
}

func (n *Node) StartIfritClient() {
	n.ifritClient.RegisterMsgHandler(n.messageHandler)
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
func (n *Node) messageHandler(data []byte) ([]byte, error) {
	var msg message.NodeMessage
	err := json.Unmarshal(data, &msg)
	if err != nil {
		panic(err)
	}

	log.Printf("Node '%s' got message %s\n", n.nodeName, msg.MessageType)

	switch msgType := msg.MessageType; msgType {
	case message.MSG_TYPE_LOAD_NODE: 		
		// Create study files as well, regardless of wether or not the subject exists. 
		// If the study exists, we still add the subject's at the node and link to them using
		// 'ln -s'. The operations performed by this call sets the finite state of the 
		// study. This means that any already existing files are deleted.
		if err := n.fs.FeedBulkData(msg.Populator); err != nil {
			return nil, err
		}

		// Notify the policy store about the newest changes
		// TODO: avoid sending all updates - only send the newest data
		// TODO: remove this because we send the list twice!
		if err := n.sendStudyList(n.PolicyStoreIP); err != nil {
			return nil, err
		}
		return n.studyList()

	case message.MSG_TYPE_GET_STUDY_LIST:
		return n.studyList()

	case message.MSG_TYPE_GET_NODE_INFO:
		return n.nodeInfo()
	
	case message.MSG_TYPE_GET_META_DATA:
		return n.studyMetaData(msg)

	case message.MSG_TYPE_SET_POLICY:
		return n.setPolicy(&msg)
		
	default:
		fmt.Printf("Unknown message type: %s\n", msg.MessageType)
		return []byte("ERROR"), nil
	}
	return []byte(message.MSG_TYPE_OK), nil
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

func (n *Node) studyMetaData(msg message.NodeMessage) ([]byte, error) {
	return n.fs.StudyMetaData(msg)
}

func (n *Node) setPolicy(msg *message.NodeMessage) ([]byte, error) {
	/*fileName := msg.Filename
	fileName = msg.Study + "_model.conf"
	modelText := string(msg.Extras)

	if err := n.fs.SetStudyPolicy(msg.Study, fileName, modelText); err != nil {
		return []byte(message.MSG_TYPE_ERROR), err
	}*/

	if err := n.verifyPolicyStoreMessage(msg); err != nil {
		panic(err)
	}

	return []byte(message.MSG_TYPE_OK), nil
}

/*
func (n *Node) SetSubjectPolicy(msg message.NodeMessage) ([]byte, error) {
	modelText := string(msg.Extras)
	if err := n.fs.SetSubjectPolicy(msg.Subject, msg.Study, msg.Filename, modelText); err != nil {
		return []byte(message.MSG_TYPE_ERROR), err
	}
	return []byte(message.MSG_TYPE_OK), nil
}

func (n *Node) SetRECPolicy(msg message.NodeMessage) ([]byte, error) {
	modelText := string(msg.Extras)
	if err := n.fs.SetStudyPolicy(msg.Study, msg.Filename, modelText); err != nil {
		return []byte(message.MSG_TYPE_ERROR), err
	}
	return []byte(message.MSG_TYPE_OK), nil
}
*/
func (n *Node) StudyData(msg message.NodeMessage) ([]byte, error) {
//	requestPolicy := msg.Populator.MetaData.Meta_data_info.PolicyAttriuteStrings()
	fmt.Printf("TODO: Implement StudyData()\n")
	return []byte(""), nil
}

func (n *Node) sendStudyList(addr string) error {
	studies, err := n.studyList()
	if err != nil {
		return err
	}

	msg := &message.NodeMessage{
		MessageType: 	message.MSG_TYPE_SET_STUDY_LIST,
		Node:			n.nodeName,
		Extras: 		studies,
	}

	serializedMsg, err := msg.Encode()
	if err != nil {
		return err
	}

	// TODO: return error from response instead?
	ch := n.ifritClient.SendTo(addr, serializedMsg)
	select {
		case response := <- ch: 
			if string(response) != message.MSG_TYPE_OK {
				return errors.New("Sending study list failed")
			}
	}
	return nil
}

// TODO: refine this and MuxHandshake to remove boilerplate code
func (n *Node) PolicyStoreHandshake() error {
	remoteAddr := viper.GetString("policy_store_addr")
	//URL := "https://" + remoteAddr + "/set_port"
	URL := "http://" + remoteAddr + "/set_port"
	fmt.Printf("URL:> %s\n", URL)

	var msg struct {
		Node 	string 		`json:"node"`
		Address string 		`json:"address"`
	}

	var resp struct {
		PolicyStoreIP 	string
		PublicKey		[]byte
	}

	msg.Node = n.nodeName
	msg.Address = n.ifritClient.Addr()
	jsonStr, err := json.Marshal(msg)
	if err != nil {
		panic(err)
        return err
    }

	req, err := http.NewRequest("POST", URL, bytes.NewBuffer(jsonStr))
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{
		Transport: &http.Transport {
			TLSClientConfig: n.clientConfig,
		},
	}
	response, err := client.Do(req)
    if err != nil {
		panic(err)
        return err
	}

	defer response.Body.Close()
	if response.StatusCode == int(http.StatusOK) {
		bodyBytes, err := ioutil.ReadAll(response.Body)
		if err != nil {
			panic(err)
		}

		buf := bytes.NewBuffer(bodyBytes)
		dec := gob.NewDecoder(buf)
		
		if err := dec.Decode(&resp); err != nil {
			panic(err)
		}
	
		n.PolicyStoreIP = resp.PolicyStoreIP
		fmt.Printf("resp.pubKey: %s\n", resp.PolicyStoreIP)
		pubKey, err := n.cu.DecodePublicKey(resp.PublicKey)
		if err != nil {
			panic(err)
		}
		n.psPublicKey = *pubKey
		fmt.Println("Pubkey at node:", pubKey)
		return nil
	}
	return errors.New("Node could not perform handshake with 'Policy store'")
}

func (n *Node) MuxHandshake() error {
	remoteAddr := viper.GetString("lohpi_mux_addr")
	URL := "https://" + remoteAddr + "/set_port"
	
	var msg struct {
		Node 	string 		`json:"node"`
		Address string 		`json:"address"`
	}

	msg.Node = n.nodeName
	msg.Address = n.ifritClient.Addr()
	jsonStr, err := json.Marshal(msg)
	if err != nil {
        return err
    }

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: n.clientConfig,
		},
	}

	req, err := http.NewRequest("POST", URL, bytes.NewBuffer(jsonStr))
	req.Header.Set("Content-Type", "application/json")
	response, err := client.Do(req)
    if err != nil {
        return err
	}

	defer response.Body.Close()
	if response.StatusCode == int(http.StatusOK) {
		bodyBytes, err := ioutil.ReadAll(response.Body)
		if err != nil {
			return err
		}

		n.MuxIP = string(bodyBytes)
		return nil
	}
	return errors.New("Node could not perform handshake with 'MUX'")
}

// TODO: overhaul this
func (n *Node) verifyPolicyStoreMessage(msg *message.NodeMessage) error {
	if !n.cu.Verify([]byte(msg.ModelText), msg.R, msg.S, &n.psPublicKey) {
		return errors.New("Could not securely verify the integrity of the new policy from policy store")
	} else {
		log.Println("Verified new signature from policy store")
	}
	return nil
}

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

