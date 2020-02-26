package node

import (
	"fmt"
//	"os"
	"log"
	"firestore/node/mux"
	"firestore/node/fuse"
	"firestore/core/messages"
_	"firestore/core/file"
	"ifrit"
)

type Node struct {
	// Used to direct messages to either FUSE or node application
	mux *mux.Mux

	// Underlying Firefly node
	IfritClient *ifrit.Client

	// Fuse daemon. We only need to keep a reference to it, although
	// it runs in a separate goroutine
	fs *fuse.Ptfs

	// Identifier
	NodeID string
}

func NewNode(ID string) (*Node, error) {
	ifritClient, err := ifrit.NewClient()
	if err != nil {
		return nil, err
	}

	node := &Node {
		NodeID: ID,
	}

	// Initialize FUSE daemon along with its directories
	node.fs = fuse.NewFuseFS(ID)

	// Initialize Ifrit client and callback functions
	node.IfritClient = ifritClient
	node.IfritClient.RegisterGossipHandler(node.GossipMessageHandler)
	//node.ifritClient.RegisterResponseHandler(node.GossipResponseHandler)
	node.IfritClient.RegisterMsgHandler(node.MuxMessageReceiver)
	go ifritClient.Start()
	
	// When the node resources have been initialized, yield control to the node app
	// so that it can run too.
	// app "main" somewhere around here...
	return node, nil
}

func (n *Node) FireflyClient() *ifrit.Client {
	return n.IfritClient
}

func (n *Node) ID() string {
	return n.NodeID
}

// Callback functions used by the Ifrit network
// Chooses the correct procedure to execute based on the message type and 
// and message content being received
func (n *Node) MuxMessageReceiver(data []byte) ([]byte, error) {
	var response string
	msg := messages.DecodedInternalMessage(data)
	/*if isValidInternalMessage(msg.Type, *msg) == false {
		return []byte("Invalid internal message"), nil
	}*/

	switch msgType := msg.Type; msgType {
		case messages.MSG_TYPE_GET_NODE_INFO:
			fmt.Printf("Message received: %s\n", msg.Type)
			response = n.string()
		case messages.MSG_TYPE_SET_SUBJECT_NODE_PERMISSION:
			fmt.Printf("Message received: %s\n", msg.Type)
			fuseDaemon := n.fuseDaemon()
			err := fuseDaemon.SetNodePermission(msg.Permission)
			if err != nil {
				log.Fatalf("Error while setting node permission: %s\n", err)
				return nil, nil
			}
		case messages.MSG_TYPE_SET_NODE_PERMISSION:
			fmt.Printf("Message received: %s\n", msg.Type)
		default:
			log.Printf("Unkown message")
			return nil, nil
	}
    return []byte(response), nil
}

func (n *Node) GossipMessageHandler(data []byte) ([]byte, error) {
	var response string
	msg := messages.DecodedInternalMessage(data)

	switch msgType := msg.Type; msgType {
		case messages.MSG_TYPE_SET_SUBJECT_PERMISSION: 
			fmt.Printf("Message received: %s\n", msg.Type)
		default:
			log.Printf("Unknwon message: %s\n", msg.Type)
			return nil, nil
	}

	return []byte(response), nil
}

// Sets the new permission for the entire collection of files owned by 'subject'
func (n *Node) setNewSubjectPermission(permission, subject string) error {

	fmt.Printf("setNewSubjectPermission\n")
	return nil
}

func (n *Node) string() string {
	s := fmt.Sprintf("-----> %s running at network address %s\n", n.ID(), n.FireflyClient().Addr());
	//s += fmt.Sprintf("%s has %d files listed below:\n");
	// files here...
	return s
}

func (n *Node) Shutdown() {
	fireflyClient := n.FireflyClient()
	log.Printf("Node", fireflyClient.Addr(), "shutting down...")
	fireflyClient.Stop()
//	n.fs.Shutdown()
}

func isValidInternalMessage(msgType messages.Msgtype, msg messages.Internalmessage) bool {
	// Check that all mandatory fields are present. Return false is something fails
	//if msg.Permission != file.FILE_ALLOWED || msg.Permission != file.FILE_DISALLOWED {
	//	return false
	//}

	//msg.SetPermission != file.FILE_PERMISSION_SET || msg.SetPermission != file.FILE_PERMISSION_UNSET {

	// Check the message type and some of the fields according to the message type
	switch msgType {
	case messages.MSG_TYPE_GET_NODE_INFO:
		return true
	default:
		log.Printf("Invalid message type: %s\n", msgType)
	}

	return false
}

func (n *Node) fuseDaemon() *fuse.Ptfs {
	return n.fs
}