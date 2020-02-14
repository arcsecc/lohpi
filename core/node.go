package core

import (
	"fmt"
	"os"
	"ifrit"
	"errors"
	log "github.com/inconshreveable/log15"
	"path/filepath"
	"firestore/core/file"
	"firestore/core/fuse"
	"firestore/core/messages"
_	"strings"
	//"net/http"
//	"encoding/gob"
	"bytes"
//	"time"
)

var (
	errCreateRandomClientData 	= errors.New("Could not set client data.")

)

//ons 26.02 kl 1445
//5w 30 full syntetisk

const NODE_DIR = "storage_nodes"

// Storage node used to store "research data"
type Node struct {
	// Files stored in the node 
	Files []*file.File	
	
	// The directory in which the node stores data (node-locally)
	MountPointDir string		
	
	// The path to /tmp/*
	DestinationPointDir string
	
	// Underlying Firefly client
	FireflyClient *ifrit.Client
	
	// Its unique ID (might change it later to something more bulletproof?)
	NodeID string

	// TO BE DETERMINED LATER WHAT TO DO WITH THIS ONE
	GlobalUsagePermission map[string]*file.File

	// Local FUSE-mounted file system
	fs *fuse.Ptfs
}

/** Node interface. Remote firestore node */
func NewNode(ID string) (*Node, error) {
	permissionMap := make(map[string]*file.File) 	// subject name -> files that can be read
	fireflyClient, err := ifrit.NewClient()
	if err != nil {
		return nil, err
	}

	localMountPoint := getLocalMountPoint(ID)
	destinationMountPoint := getDestinationMountPoint(ID)
	createDirectory(localMountPoint)
	createDirectory(destinationMountPoint)

	node := &Node {
		FireflyClient: fireflyClient,
		NodeID: ID,
		GlobalUsagePermission: permissionMap,
		MountPointDir: localMountPoint,
		DestinationPointDir: destinationMountPoint,
	}

	fuse.NewFuseFS(localMountPoint, destinationMountPoint)
	node.FireflyClient.RegisterGossipHandler(node.GossipMessageHandler)
	if err != nil {
		panic(err)
	}

	node.FireflyClient.RegisterResponseHandler(node.GossipResponseHandler)
	node.FireflyClient.RegisterMsgHandler(node.StorageNodeMessageHandler)
	go fireflyClient.Start()
	return node, nil
}

func (n *Node) NodeInfo() string {
	var b bytes.Buffer
	str := fmt.Sprintf("****** Node info ******\nLocal directory path: %s\nNode ID: %s\nIfrit address: %s\n\n",
		n.MountPointDir, n.NodeID, n.IfritClient().Addr())
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
	return n.MountPointDir
}

// Invoked when this client receives a message
func (n *Node) StorageNodeMessageHandler(data []byte) ([]byte, error) {
	msg := messages.DecodedInternalMessage(data)

	if (msg.Type == messages.PERMISSION_SET) {
		for _, file := range n.Files {
			if msg.Subject == file.SubjectID || msg.Subject == "ALL" {
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

// Returns the path for the local entry point for the FUSE file system
func getLocalMountPoint(nodeName string) string {
	cwd, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf("%s/%s/%s", cwd, NODE_DIR, nodeName)
}

// Returns the location to which changes in the node's local storage directory
// are reflected
func getDestinationMountPoint(nodeName string) string {
	return fmt.Sprintf("/tmp/%s/%s", NODE_DIR, nodeName)
}

// Creates a directory
func createDirectory(dirPath string) {
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(dirPath, 0755); err != nil {
			panic(err)
		}
	}
}

func (n *Node) AbsFilePath(fileAbsPath string) string {
	return filepath.Join(n.MountPointDir, fileAbsPath)
}

/*func (n *Node) SetAbsoluteMessagePath(msg *file.Message) {
	hash := msg.FilePathHash
	absMsgPath := fmt.Sprintf("%s/%x", n.StorageDirectoryPath, hash)
	msg.RemoteAbsolutePath = absMsgPath
}*/

// This callback will be invoked on each received gossip message.
func (n *Node) GossipMessageHandler(data []byte) ([]byte, error) {
/*	msg := messages.DecodedInternalMessage(data)
	targetSubject := msg.Subject
	
	// Find file in the node. If it exists, set new permission
	if (msg.Type == messages.PERMISSION_SET) {
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
	} else {
		fmt.Errorf("%s type is not valid\n", msg.Type)
	}

	// Put this into if branch*/
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

func (n *Node) Shutdown() {
	ifritClient := n.IfritClient()
	log.Debug("Node", ifritClient.Addr(), "shutting down...")
	ifritClient.Stop()
//	n.fs.Shutdown()
}