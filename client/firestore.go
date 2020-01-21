package main

import (
	"fmt"
	"time"
	"math/rand"
	"github.com/spf13/viper"
	log "github.com/inconshreveable/log15"
	"encoding/gob"
	"bytes"
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

func (fs *Firestore) StoreFiles(files []*File) {
	//destinationNode := fs.randomFirestoreNode()
	//fmt.Println(destinationNode.ID())
	for _, file := range files {
		if file != nil {
			
			if fs.fileExists(file) {
				fmt.Printf("File exists\n");
			} else {
				fmt.Printf("File does not exists. Is about to insert file...\n");
				destinationFSNode := fs.randomFirestoreNode()
				
				for {
					select {
						case <- time.After(0):
							fs.sendFileToFSNode(file, destinationFSNode)
							response := <- channel
							fmt.Printf("Resp = %s\n", response)
						}
					}
			}
		}
	}
}

func (fs *Firestore) sendFileToFSNode(file *File, fsNode *Node) chan []byte {
	masterIfritClient := fs.MasterNode.FireflyClient()
	destinationNode := fs.randomFirestoreNode().FireflyClient()
	message := NewFirestoreMessage(file, fsNode)
	encodedMessage := message.Encoded()

	return masterIfritClient.SendTo(destinationNode.Addr(), encodedMessage)
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
	fmt.Println(fs.FirestoreNodes)
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

/** Filecontents interface here */
func NewFirestoreMessage(file *File, fsNode *Node) *FSMessage {
	fileMetadata, _ := decomposeFileInformation(file)
	fileContents := file.GetFileAsBytes()

	message := &FSMessage {
		FileMetadata: fileMetadata,
		FileContents: fileContents,		
	}

	return message
}

func (fsm *FSMessage) Encoded() []byte {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	if err := encoder.Encode(fsm); err != nil {
	   panic(err)
	}
	return buffer.Bytes()
}

func decomposeFileInformation(file *File) ([]byte, error) {
	//fileInfoBuffer, _ := file.DecodeFileInfo()
	fileHashBuffer, _ := file.DecodeHashString()
	//bufferLength := len(fileInfoBuffer.Bytes()) + len(fileHashBuffer.Bytes())
	bufferLength := len(fileHashBuffer.Bytes())
	
	buffer := make([]byte, bufferLength)
	//buffer = append(buffer, fileInfoBuffer.Bytes()...)
	buffer = append(buffer, fileHashBuffer.Bytes()...)
	return buffer, nil
}




