package node

import (
	"fmt"
	"os"
	"log"
	"firestore/node/mux"
	"firestore/node/fuse"
	"firestore/core/messages"
_	"firestore/core/file"
	"ifrit"
_	"errors"
)

const STUDIES_DIR = "studies"
const SUBJECTS_DIR = "subjects"

/** Note: When the files are set, there is a tiny window in which the application
 * can gain access to the files because the files need to be assigned to the node.
 * The permissions are elevated to make this possible, thus 
*/
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
	//node.createSubDirectories()
	// Initialize Ifrit client and callback functions
	node.IfritClient = ifritClient
	node.IfritClient.RegisterGossipHandler(node.GossipMessageHandler)
	//node.ifritClient.RegisterResponseHandler(node.GossipResponseHandler)
	node.IfritClient.RegisterMsgHandler(node.MuxMessageReceiver)
	//node.createStudiesDirectoryTree()
	go ifritClient.Start()
	
	// When the node resources have been initialized, yield control to the node app
	// so that it can run too.
	// app "main" somewhere around here...
	return node, nil
}

func (n *Node) createSubDirectories() {
	//pwd, err := os.Getwd()
	//if err != nil {
	//	panic(err)
	//}

	//nodePath := fmt.Sprintf("%s/%s/%s", pwd, fuse.NODE_DIR, n.ID())
	// Create subject file directory
	//_ = os.Mkdir(nodePath + "/subjects", 0755)
		
	// Create study directory 
	//_ = os.Mkdir(nodePath + "/studies", 0755)
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
	msg := messages.NewMessage(data)
	fmt.Printf("Message received: %s\n", msg)

	switch msgType := msg.MessageType; msgType {
		case messages.MSG_TYPE_GET_NODE_INFO:
			response = n.string()

		case messages.MSG_TYPE_SET_SUBJECT_NODE_PERMISSION:
			fuseDaemon := n.fuseDaemon()
			err := fuseDaemon.SetNodePermission(msg.Permission)
			if err != nil {
				log.Fatalf("Error while setting node permission: %s\n", err)
				return nil, nil
			}

		case messages.MSG_TYPE_SET_NODE_PERMISSION:
			break

		case messages.MSG_TYPE_SET_NODE_FILES:
			log.Fatalf("Setting files from cURL is not implemented. Exiting...\n")

		case messages.MSG_TYPE_NEW_STUDY:
			err := n.createNewStudyDirectory(msg.Study)
			if err != nil {
				return nil, err
			}
		
		case messages.MSG_TYPE_LOAD_NODE:
//			n.LoadNodeState()

		default:
			log.Printf("Unkown message type: %s\n", msg.MessageType)
			return nil, nil
	}
    return []byte(response), nil
}

func (n *Node) GossipMessageHandler(data []byte) ([]byte, error) {
/*	var response string
	msg := messages.DecodedInternalMessage(data)

	switch msgType := msg.Type; msgType {
		case messages.MSG_TYPE_SET_SUBJECT_PERMISSION: 
			fmt.Printf("Message received: %s\n", msg.Type)
		default:
			log.Printf("Unknwon message: %s\n", msg.Type)
			return nil, nil
	}

	return []byte(response), nil*/
	return nil, nil
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

func isValidInternalMessage(msgType messages.Msgtype, msg messages.InternalMessage) bool {
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

func (n *Node) StorageDirectory() string {
	return n.fs.GetLocalMountPoint()
}

func (n *Node) ParentFusePoint() string {
	return fuse.NODE_DIR
}

func (n *Node) createNewStudyDirectory(studyName string) error {
	/*
	pwd, err := os.Getwd()
	if err != nil {
		return err
	}

	studyPath := fmt.Sprintf("%s/%s/%s", pwd, STUDIES_DIR, studyName)
	dirExists, err := n.exists(studyPath)
	if err != nil {
		panic(err)
	}

	if !dirExists {
		err = os.Mkdir(studyPath, 0755)
		if err != nil {
			panic(err)
		}
	} else {
		return errors.New("Study already exists")
	}*/
	return nil
}

// might move this one to 'daemon.go' in this package
func (n *Node) exists(path string) (bool, error) {
    _, err := os.Stat(path)
    if err == nil { 
		return true, nil 
	}
    if os.IsNotExist(err) { 
		return false, nil 
	}
    return true, err
}

func (n *Node) createStudiesDirectoryTree() {
	dirPath := fmt.Sprintf("%s/%s", n.StorageDirectory(), STUDIES_DIR)
	fmt.Printf("Studies path = %s\n", dirPath)
}