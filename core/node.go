package core

import (
	"fmt"
	"os"
	"ifrit"
	"errors"
	log "github.com/inconshreveable/log15"
	"path/filepath"
	"firestore/core/file"
//	"encoding/gob"
//	"bytes"
//	"time"
)

var (
	errCreateRandomClientData 	= errors.New("Could not set client data.")
)

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

	// TODO: recover from crash here... check files on disk and insert them into table
	dir, err := setStorageDirectory(ID)
	if err != nil {
		log.Error("Could not create directory for storage node")
		panic(err)
	}

	/*
	ifritClient.RegisterMsgHandler(node.StorageNodeMessageHandler)
	go ifritClient.Start()*/

	node := &Node {
		IfritClient: ifritClient,
		NodeID: ID,
		GlobalUsagePermission: permissionMap,
		AbsoluteStorageDirectoryPath: dir,
	}
	
	return node, nil
}

func (n *Node) Name() string {
	return n.NodeID
}

func (n *Node) FireflyClient() *ifrit.Client {
	return n.IfritClient
}

// Invoked when this client receives a message
func (n *Node) StorageNodeMessageHandler(data []byte) ([]byte, error) {
	decodedMessage := file.DecodedMessage(data)	
	n.SetAbsoluteMessagePath(decodedMessage)
	localFileMapKey := decodedMessage.FilePathHash

	//fmt.Printf("Path = %s\n", decodedMessage.RemoteAbsolutePath)

	if n.storageNodeHasFile(localFileMapKey) == true {
		fmt.Printf("File is already in node. Should update it...\n")
	} else {
		fmt.Printf("File is not contained in the node. Inserts it...\n")
		n.insertNewFileIntoStorageNode(decodedMessage)
		n.broadcastStorageState()
		// gossip newest update of files to the entire network
	}
	return nil, nil
}

func (n *Node) broadcastStorageState() {
	
}

func (n *Node) insertNewFileIntoStorageNode(msg *file.Message) {
	/*newFile, err := file.CreateFileFromBytes(msg.RemoteAbsolutePath, msg.FileContents)
	if err != nil {
		fmt.Errorf("File exists!!1! It should not exist!")
		panic(err)
	}*/
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


func setStorageDirectory(nodeName string) (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		panic(err)
	}

	absoluteDirPath := fmt.Sprintf("%s/%s", cwd, nodeName)
	if _, err := os.Stat(absoluteDirPath); os.IsNotExist(err) {
		os.Mkdir(absoluteDirPath, 0755)
	}

	return absoluteDirPath, nil
}

func (n *Node) getAbsFilePath(fileAbsPath string) string {
	return filepath.Join(n.AbsoluteStorageDirectoryPath, fileAbsPath)
}

func (n *Node) SetAbsoluteMessagePath(msg *file.Message) {
	hash := msg.FilePathHash
	absMsgPath := fmt.Sprintf("%s/%x", n.AbsoluteStorageDirectoryPath, hash)
	msg.RemoteAbsolutePath = absMsgPath
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

/** Masternode interface */
/*
func NewMasterNode(nodeID string) (*Masternode, error) {
	ifritClient, err := ifrit.NewClient()
	if err != nil {
		return nil, err
	}

	masterNode := &Masternode {
		IfritClient: ifritClient,
		NodeID: nodeID,
	}
	go ifritClient.Start()
	return masterNode, nil
}*/

/*
func (n *Masternode) FileNameTable() map[string]*ifrit.Client {
	return n.Filemap
}*/

/*
func (n *Masternode) MasterNodeMessageHandler(data []byte) ([]byte, error) {
	//fmt.Println(data)
	return nil, nil
}

func (n *Masternode) FireflyClient() *ifrit.Client {
	return n.IfritClient
}*/