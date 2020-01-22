package main

import (
	"fmt"
	"time"
	"math/rand"
	"github.com/spf13/viper"
	log "github.com/inconshreveable/log15"
)

type Firestore struct {
	FirestoreNodes []*Node
	MasterNode *Masternode
}

// might put this one somewhere else...
type FSMessage struct {
	FileMetadata []byte
	FileContents []byte
}

func NewFirestore() (*Firestore, error) {
	numFirestoreNodes := viper.GetInt("firestore_nodes")
	nodes, err := createFirestoreNodes(numFirestoreNodes)
	if err != nil {
		log.Error("Could not create Firestore nodes")
		return nil, err
	}

	masterNode, err := newFirestoreMasterNode()
	if err != nil {
		log.Error("Could not create Firestore master node")
		return nil, err
	}

	fs := &Firestore {
		FirestoreNodes: nodes,
		MasterNode: masterNode,
	}

	return fs, nil
}

func (fs *Firestore) StoreFiles(files []*File) chan []byte{
	/*
	for _, file := range files {
		if file != nil {
			
			if fs.fileExists(file) {
				fmt.Printf("File exists\n");
			} else {
				fmt.Printf("File does not exists. Is about to insert file...\n");
				destinationFSNode := fs.randomFirestoreNode()
			}
		}	
	}*/

	
	file := files[0]
	destinationFSNode := fs.randomFirestoreNode()
	return fs.storeDataIntoFSNode(file, destinationFSNode)
	//return nil
}

func (fs *Firestore) storeDataIntoFSNode(file *File, fsNode *Node) chan []byte {
	encodedFile, _ := file.Encoded()
	masterIfritClient := fs.MasterNode.FireflyClient()
	destinationNode := fs.randomFirestoreNode().FireflyClient()

	return masterIfritClient.SendTo(destinationNode.Addr(), encodedFile)
}

func (fs *Firestore) fileExists(file *File) bool {
	filenameTable := fs.MasterNode.FileNameTable()
	idx := file.GetFileHash()
	if _, ok := filenameTable[idx]; ok {
		return true
	}
	return false
}

func (fs *Firestore) randomFirestoreNode() *Node {
	rand.Seed(time.Now().UnixNano())
	idx := rand.Intn(len(fs.FirestoreNodes))
	//fmt.Println(fs.FirestoreNodes)
	return fs.FirestoreNodes[idx]
}

func createFirestoreNodes(numNodes int) ([]*Node, error) {
	nodes := make([]*Node, 0) 

	for i := 0; i < numNodes; i++ {
		nodeID := fmt.Sprintf("node_%d", i + 1)
		node, err := NewNode(nodeID)
		if err != nil {
			log.Error("Could not create Firestore node")
			return nil, err
		}

		nodes = append(nodes, node)
		ifritClient := node.FireflyClient()
		ifritClient.RegisterMsgHandler(node.StorageNodeMessageHandler)
	}

	return nodes, nil
}

func newFirestoreMasterNode() (*Masternode, error) {
	masterNode, err := NewMasterNode("MasterNode")
	if err != nil {
		log.Error("Could not create Firestore node")
		return nil, err
	}

	ifritClient := masterNode.FireflyClient()
	ifritClient.RegisterMsgHandler(masterNode.MasterNodeMessageHandler)
	return masterNode, nil
}



