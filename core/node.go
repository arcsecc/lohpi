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

// Firestore node
type Node struct {
	Filemap map[string]*os.File		// hash of client's file path -> *os.File
	IfritClient *ifrit.Client
	NodeID string
	AbsoluteStorageDirectoryPath string // directory into which to store files (node-locally)
}

/** Node interface. Remote firestore node */
func NewNode(ID string) (*Node, error) {
	ifritClient, err := ifrit.NewClient()
	if err != nil {
		return nil, err
	}

	node := &Node {}
	node.IfritClient = ifritClient
	node.NodeID = ID
	node.Filemap = make(map[string]*os.File)

	// TODO: recover from crash here... check files on disk and insert them into table
	node.AbsoluteStorageDirectoryPath, err = setStorageDirectory(node)
	if err != nil {
		log.Error("Could not create directory for storage node")
		panic(err)
	}

	ifritClient.RegisterMsgHandler(node.StorageNodeMessageHandler)
	go ifritClient.Start()
	return node, nil
}

func (n *Node) ID() string {
	return n.NodeID
}

func (n *Node) FireflyClient() *ifrit.Client {
	return n.IfritClient
}

// Invoked when this client receives a message
func (n *Node) StorageNodeMessageHandler(data []byte) ([]byte, error) {
	decodedFile := file.Decoded(data)
	n.SetAbsoluteFilePath(decodedFile)

	fmt.Printf("Path = %s\n", decodedFile.GetAbsoluteFilePath())

	if n.storageNodeHasFile(decodedFile) == true {
		fmt.Printf("File is already in node. Should update it...\n")
	} else {
		fmt.Printf("File is not contained in the node. Inserts it...\n")
		n.insertNewFileIntoStorageNode(decodedFile)
		// gossip newest update of files to the entire network
	}
	return nil, nil
}

func (n *Node) insertNewFileIntoStorageNode(f *file.File) {
	fmt.Printf("%s\n", f.GetFileAsBytes())
	/*_, err := file.CreateFileFromBytes(f.GetAbsoluteFilePath(), f.GetFileAsBytes())
	if err != nil {
		fmt.Errorf("File exists!!1! It should not exist!")
		panic(err)
	}

	fileKey := m.FileHash*/
//	n.Filemap[fileKey] = file
}

func (n *Node) createFileTree(absFilePath string) {
	directory, _ := filepath.Split(absFilePath)
	err := os.MkdirAll(directory, os.ModePerm)
	if err != nil {
		log.Error("Could not create file tree for a new file")
		panic(err)
	}
}

func (n *Node) storageNodeHasFile(file *file.File) bool {
	filenameTable := n.FileNameTable()
	idx := file.FilePathHash
	if _, ok := filenameTable[idx]; ok {
		return true
	}
	return false
}

func (n *Node) FileNameTable() map[string]*os.File {
	return n.Filemap
}

func setStorageDirectory(n *Node) (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		panic(err)
	}

	absoluteDirPath := fmt.Sprintf("%s/%s", cwd, n.NodeID)
	if _, err := os.Stat(absoluteDirPath); os.IsNotExist(err) {
		os.Mkdir(absoluteDirPath, 0755)
	}

	return absoluteDirPath, nil
}

func (n *Node) getAbsFilePath(fileAbsPath string) string {
	return filepath.Join(n.AbsoluteStorageDirectoryPath, fileAbsPath)
}

func (n *Node) SetAbsoluteFilePath(file *file.File) {
	hash := file.FilePathHash
	absFilePath := fmt.Sprintf("%s/%x", n.AbsoluteStorageDirectoryPath, hash)
	file.AbsolutePath = absFilePath
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