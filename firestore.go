package main

import (
	"fmt"
	"time"
	"math/rand"
	"github.com/spf13/viper"
	log "github.com/inconshreveable/log15"
	"firestore/core/file"
	"firestore/core"
	"errors"
)

var (
	errUserExists = errors.New("User already exists in network!")
	errUserDoesNotExist = errors.New("User does not exist in network")
)

type Firestore struct {
	FirestoreNodes []*core.Node	// storage nodes in the network
	Users []*User // front-end nodes in the network that the users conntect with
}

func NewFirestore() (*Firestore, error) {
	numFirestoreNodes := viper.GetInt("firestore_nodes")
	nodes, err := createFirestoreNodes(numFirestoreNodes)
	if err != nil {
		log.Error("Could not create Firestore nodes")
		return nil, err
	}

	/*
	masterNode, err := newFirestoreMasterNode()
	if err != nil {
		log.Error("Could not create Firestore master node")
		return nil, err
	}*/

	fs := &Firestore {
		FirestoreNodes: nodes,
		//ClientNodes: ,
	}

	return fs, nil
}

func (fs *Firestore) AddUser(user *User) error {
	if fs.UserExists(user) == false {
		return errUserExists
	}

	nodeID := fmt.Sprintf("%s_clientNode", user.Name())
	clientNode := core.NewClientNode(nodeID)
	user.SetAPIFrontEnd(clientNode)
	fs.Users = append(fs.Users, user)
	return nil
}

func (fs *Firestore) StoreFile(file *file.File, user *User) chan []byte {
	if fs.UserExists(user) == false {
		panic(errUserDoesNotExist)
	}

	if fs.fileExists(file) {
		fmt.Printf("File exists\n");
	} else {
		fmt.Printf("File does not exists. Is about to insert file...\n");
		user.
		return fs.storeDataIntoFSNode(file, user)
	}

	return nil
}

func (fs *Firestore) storeDataIntoFSNode(file *file.File, user *User) chan []byte {
	encodedFile, _ := file.Encoded()
	clientNode := user.GetAPIFrontEnd()
	ifritClientNode := clientNode.GetIfritClient()
	destinationNode := fs.randomFirestoreNode().FireflyClient()
	return ifritClientNode.SendTo(destinationNode.Addr(), encodedFile)
}

func (fs *Firestore) fileExists(file *file.File, user *User) bool {
	filenameTable := user.GetAPIFrontEnd.FileNameTable()
	idx := file.GetFileHash()
	if _, ok := filenameTable[idx]; ok {
		return true
	}
	return false
}

func (fs *Firestore) randomFirestoreNode() *core.Node {
	rand.Seed(time.Now().UnixNano())
	idx := rand.Intn(len(fs.FirestoreNodes))
	//fmt.Println(fs.FirestoreNodes)
	return fs.FirestoreNodes[idx]
}

func createFirestoreNodes(numNodes int) ([]*core.Node, error) {
	nodes := make([]*core.Node, 0) 

	for i := 0; i < numNodes; i++ {
		nodeID := fmt.Sprintf("node_%d", i + 1)
		node, err := core.NewNode(nodeID)
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

// somewhat stupid -- TODO: do not change another user's 'behaviour' from this module
func (fs *Firestore) addFileToClientFrontEnd() {

}

func (fs *Firestore) UserExists(user *User) bool {
	for _, u := range fs.Users {
		if u == user {
			return true
		}
	}

	return false
}

