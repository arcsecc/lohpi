package core

import (
	"fmt"
	"os"
	"ifrit"
	"errors"
	log "github.com/inconshreveable/log15"
	"path/filepath"
	"firestore/core/file"
	"firestore/core/message"
	"strings"
//	"encoding/gob"
//	"bytes"
//	"time"
)

var (
	errCreateRandomClientData 	= errors.New("Could not set client data.")

)

const NODE_DIR = "storage_nodes"

// Storage node used to store "research data"
type Node struct {
	SubjectFiles []*file.File		// one-to-one mapping between subject and 
	StorageDirectoryPath string		// directory into which to store files (node-locally)
	IfritClient *ifrit.Client
	NodeID string
	AbsoluteStorageDirectoryPath string 
	GlobalUsagePermission map[string]*file.File
}

/** Node interface. Remote firestore node */
func NewNode(ID string) (*Node, error) {
	permissionMap := make(map[string]*file.File) 	// subject name -> files that can be read
	ifritClient, err := ifrit.NewClient()
	if err != nil {
		return nil, err
	}

	dirPath := GetDirAbsolutePath(ID)
	createStorageDirectory(dirPath)

	node := &Node {
		IfritClient: ifritClient,
		NodeID: ID,
		GlobalUsagePermission: permissionMap,
		AbsoluteStorageDirectoryPath: dirPath,
	}

	node.IfritClient.RegisterGossipHandler(node.GossipMessageHandler)
	node.IfritClient.RegisterResponseHandler(node.GossipResponseHandler)
	node.IfritClient.RegisterMsgHandler(node.StorageNodeMessageHandler)
	go ifritClient.Start()
	return node, nil
}

func (n *Node) Name() string {
	return n.NodeID
}

func (n *Node) FireflyClient() *ifrit.Client {
	return n.IfritClient
}

func (n *Node) StoragePath() string {
	return n.AbsoluteStorageDirectoryPath
}

// Invoked when this client receives a message
func (n *Node) StorageNodeMessageHandler(data []byte) ([]byte, error) {
	msg := message.DecodedMessage(data)
	targetSubject := msg.SubjectID
	// if msg type...
	for _, file := range n.SubjectFiles {
		if strings.Contains(file.AbsolutePath, targetSubject) {
			file.SetPermission(msg.Permission)
			//fmt.Printf("File: %s\n", file.AbsolutePath)
		}
	}

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

func (n *Node) storageNodeHasFile(fileMapKey string) bool {
	/*filenameTable := n.LocalFileNameTable()
	if _, ok := filenameTable[fileMapKey]; ok {
		return true
	}*/
	return false
}

func GetDirAbsolutePath(nodeName string) string {
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

func (n *Node) getAbsFilePath(fileAbsPath string) string {
	return filepath.Join(n.AbsoluteStorageDirectoryPath, fileAbsPath)
}

/*func (n *Node) SetAbsoluteMessagePath(msg *file.Message) {
	hash := msg.FilePathHash
	absMsgPath := fmt.Sprintf("%s/%x", n.AbsoluteStorageDirectoryPath, hash)
	msg.RemoteAbsolutePath = absMsgPath
}*/

// This callback will be invoked on each received gossip message.
func (n *Node) GossipMessageHandler(data []byte) ([]byte, error) {
	msg := message.DecodedMessage(data)
	targetSubject := msg.SubjectID
	
	for _, file := range n.SubjectFiles {
		if strings.Contains(file.AbsolutePath, targetSubject) {
			file.SetPermission(msg.Permission)
			//fmt.Printf("File: %s\n", file.AbsolutePath)
		}
	}

    return nil, nil
}

// This callback will be invoked on each received gossip response.
func (n *Node) GossipResponseHandler(data []byte) {
}

// Should be called from elsewhere to assign subjects to files,
// and in turn, files to this node
func (n *Node) SetSubjectFiles(files []*file.File) {
	n.SubjectFiles = files	
}

func (n *Node) AppendSubjectFile(file *file.File) {
	n.SubjectFiles = append(n.SubjectFiles, file)
}

func (n *Node) NodeSubjectFiles() []*file.File {
	return n.SubjectFiles	
}