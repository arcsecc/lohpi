package main

import (
	"fmt"
	"os"
	"ifrit"
	"errors"
	log "github.com/inconshreveable/log15"
	"path/filepath"
//	"encoding/gob"
//	"bytes"
//	"time"
)

var (
	errCreateRandomClientData 	= errors.New("Could not set client data.")
)

// Firestore node
type Node struct {
	Filemap map[string]*os.File		// string -> *os.File
	IfritClient *ifrit.Client
	NodeID string
	AbsoluteStorageDirectoryPath string
}

// Might need to change the value's type
type Masternode struct {
	Filemap map[string]string		// hash -> absolute filepath
	IfritClient *ifrit.Client
	NodeID string
}

/** Node interface */
func NewNode(ID string) (*Node, error) {
	ifritClient, err := ifrit.NewClient()
	if err != nil {
		return nil, err
	}

	node := &Node {}
	node.IfritClient = ifritClient
	node.NodeID = ID
	node.Filemap = make(map[string]*os.File)
	node.AbsoluteStorageDirectoryPath, err = setStorageDirectory(node)
	if err != nil {
		log.Error("Could not create directory for storage node")
		panic(err)
	}

	go ifritClient.Start()
	return node, nil
}

func (n *Node) ID() string {
	return n.NodeID
}

func (n *Node) FireflyClient() *ifrit.Client {
	if n.IfritClient == nil {
		fmt.Printf("OIAJSDOISOSIJDSO\n");
	}
	return n.IfritClient
}

// Invoked when this client receives a message
func (n *Node) StorageNodeMessageHandler(data []byte) ([]byte, error) {
	fmt.Println("StorageNodeMessageHandler")

	message, _ := DecodedMessage(data)

	if n.storageNodeHasFile(message) == true {
		fmt.Printf("File is already in node. Should update it...\n")
	} else {
		fmt.Printf("File is not contained in the node. Inserts it...\n")
		
		// sender's file tree is differen from out file tree!
		absPath := n.getAbsFilePath(message.RelativeFilePath)
		message.SetAbsoluteFilePath(absPath)
		
		fmt.Printf("new absolute file path = %s\n", message.AbsoluteFilePath)
		n.insertNewFileIntoStorageNode(message)
		
	}
	return nil, nil
}

func (n *Node) insertNewFileIntoStorageNode(m *Message) {
	n.createFileTree(m.AbsoluteFilePath)
	file, err := CreateFileFromBytes(m.AbsoluteFilePath, m.FileContents) 		// pass permission as well!
	if err != nil {
		fmt.Errorf("File exists!!1! It should not exist!")
		panic(err)
	}

	fileKey := m.FileHash
	n.Filemap[fileKey] = file
}

func (n *Node) createFileTree(absFilePath string) {
	directory, _ := filepath.Split(absFilePath)
	err := os.MkdirAll(directory, os.ModePerm)
	if err != nil {
		log.Error("Could not create file tree for a new file")
		panic(err)
	}
}

func (n *Node) storageNodeHasFile(m *Message) bool {
	filenameTable := n.FileNameTable()
	idx := m.FileHash
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

func (n *Node) getAbsFilePath(relPath string) string {
	return filepath.Join(n.AbsoluteStorageDirectoryPath, relPath)
}

/** Masternode interface */
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
}

func (n *Masternode) FileNameTable() map[string]string {
	return n.Filemap
}

func (n *Masternode) MasterNodeMessageHandler(data []byte) ([]byte, error) {
	//fmt.Println(data)
	return nil, nil
}

func (n *Masternode) FireflyClient() *ifrit.Client {
	return n.IfritClient
}