package core

import (
	"fmt"
	"os"
	"ifrit"
	"errors"
	log "github.com/inconshreveable/log15"
	"path/filepath"
	"firestore/core/file"
	"firestore/core/messages"
	"strings"
	//"net/http"
//	"encoding/gob"
	"bytes"
//	"time"
)

var (
	errCreateRandomClientData 	= errors.New("Could not set client data.")

)

const NODE_DIR = "storage_nodes"

// Storage node used to store "research data"
type Node struct {
	// Files stored in the node 
	Files []*file.File	
	
	// The directory in which the node stores data (node-locally)
	StorageDirectoryPath string		
	
	// Underlying Firefly client
	FireflyClient *ifrit.Client
	
	// Its unique ID (might change it later to something more bulletproof?)
	NodeID string
	

	GlobalUsagePermission map[string]*file.File
}

/** Node interface. Remote firestore node */
func NewNode(ID string) (*Node, error) {
	permissionMap := make(map[string]*file.File) 	// subject name -> files that can be read
	fireflyClient, err := ifrit.NewClient()
	if err != nil {
		return nil, err
	}

	dirPath := storageDirectoryPath(ID)
	createStorageDirectory(dirPath)

	node := &Node {
		FireflyClient: fireflyClient,
		NodeID: ID,
		GlobalUsagePermission: permissionMap,
		StorageDirectoryPath: dirPath,
	}

	node.FireflyClient.RegisterGossipHandler(node.GossipMessageHandler)
	node.FireflyClient.RegisterResponseHandler(node.GossipResponseHandler)
	node.FireflyClient.RegisterMsgHandler(node.StorageNodeMessageHandler)
	go fireflyClient.Start()
	return node, nil
}

func (n *Node) NodeInfo() string {
	var b bytes.Buffer
	str := fmt.Sprintf("****** Node info ******\nLocal directory path: %s\nNode ID: %s\nIfrit address: %s\n\n",
		n.StorageDirectoryPath, n.NodeID, n.IfritClient().Addr())
	_, err := b.WriteString(str)
	if err != nil {
		panic(err)
	}

	for _, f := range n.Files {
		str := string(fmt.Sprintf("Path: %s\nOwner: %s\nStorage node: %s\nPermission: %s\n\n\n", f.AbsolutePath, f.SubjectID, f.OwnerID, f.FilePermission()))
		_, err = b.WriteString(str)
		if err != nil {
			panic(err)
		}
	}

	return string(b.Bytes())
}

func (n *Node) Name() string {
	return n.NodeID
}

func (n *Node) IfritClient() *ifrit.Client {
	return n.FireflyClient
}

func (n *Node) StorageDirectory() string {
	return n.StorageDirectoryPath
}

// Invoked when this client receives a message
func (n *Node) StorageNodeMessageHandler(data []byte) ([]byte, error) {
	msg := messages.DecodedInternalMessage(data)
	
	if (msg.Type == messages.PERMISSION_SET) {
		for _, file := range n.Files {
			if msg.SetPermission == "set" {
				//fmt.Printf("set permission %s to true", msg.Permission)
				file.SetPermission(msg.Permission, true)
			} else if msg.SetPermission == "unset" {
				file.RemovePermission(msg.Permission)
			} else {
				fmt.Errorf("%s switch is not valid\n", msg.SetPermission)
			}
		}
	} else {
		fmt.Errorf("%s type is not valid\n", msg.Type)
	} 

	// Put this into if branch...
    return nil, nil
}

func (n *Node) createFileTree(absFilePath string) {
	directory, _ := filepath.Split(absFilePath)
	err := os.MkdirAll(directory, os.ModePerm)
	if err != nil {
		log.Error("Could not create file tree for a new file")
		panic(err)
	}
}

func (n *Node) HasFile(fileMapKey string) bool {
	/*filenameTable := n.LocalFileNameTable()
	if _, ok := filenameTable[fileMapKey]; ok {
		return true
	}*/
	return false
}

// Returns the absolute path of the node's top-level storage directory
func storageDirectoryPath(nodeName string) string {
	cwd, err := os.Getwd()
	if err != nil {
		log.Error("Could not create directory for storage node")
		panic(err)
	}

	return fmt.Sprintf("%s/%s/%s", cwd, NODE_DIR, nodeName)
}

func createStorageDirectory(dirPath string) {
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		os.MkdirAll(dirPath, 0755)
	}
}

func (n *Node) AbsFilePath(fileAbsPath string) string {
	return filepath.Join(n.StorageDirectoryPath, fileAbsPath)
}

/*func (n *Node) SetAbsoluteMessagePath(msg *file.Message) {
	hash := msg.FilePathHash
	absMsgPath := fmt.Sprintf("%s/%x", n.StorageDirectoryPath, hash)
	msg.RemoteAbsolutePath = absMsgPath
}*/

// This callback will be invoked on each received gossip message.
func (n *Node) GossipMessageHandler(data []byte) ([]byte, error) {
	msg := messages.DecodedInternalMessage(data)
	targetSubject := msg.Subject
	
	// Find file in the node. If it exists, set new permission
	if (msg.Type == messages.PERMISSION_SET) {
		for _, file := range n.Files {
			if strings.Contains(file.AbsolutePath, targetSubject) {
				if msg.SetPermission == "set" {
					//fmt.Printf("set permission %s to true", msg.Permission)
					file.SetPermission(msg.Permission, true)
				} else if msg.SetPermission == "unset" {
					file.RemovePermission(msg.Permission)
				} else {
					fmt.Errorf("%s switch is not valid\n", msg.SetPermission)
				}
			}
		}
	} else {
		fmt.Errorf("%s type is not valid\n", msg.Type)
	} 

	// Put this into if branch
    return []byte(messages.MSG_NEW_PERM_SET), nil
}

// This callback will be invoked on each received gossip response.
func (n *Node) GossipResponseHandler(data []byte) {
}

// Should be called from elsewhere to assign subjects to files,
// and in turn, files to this node
func (n *Node) SetSubjectFiles(files []*file.File) {
	n.Files = files	
}

func (n *Node) AppendSubjectFile(file *file.File) {
	n.Files = append(n.Files, file)
}

func (n *Node) SubjectFiles() []*file.File {
	return n.Files	
}