package core

import (	
	"fmt"
	"ifrit"
	"firestore/core/file"
	log "github.com/inconshreveable/log15"
	"math/rand"
//	"time"
)

// Notes: This file describes the client's front-end entry point API.
// All application that connect to the network create an instance of this type
// to use the network

type Clientnode struct {
	GlobalFileNodeMap map[string]string
	IfritClient *ifrit.Client
	UserNodeID string
}

func NewClientNode(NodeID string) *Clientnode {
	globalFileNodeMap := make(map[string]string, 0)
	ifritClient, err := ifrit.NewClient()
	if err != nil {
		panic(err)
	}

	self := &Clientnode {
		GlobalFileNodeMap: globalFileNodeMap,
		IfritClient: ifritClient,
	}

	ifritClient.RegisterMsgHandler(self.DirectMessageHandler)
	go ifritClient.Start()
	// gossip handlers too
	return self
}

func (cn *Clientnode) StoreFileRemotely(file *file.File, storageNodes []*Node) chan []byte {
	if cn.fileExistsInRemoteStorage(file) == true {
		log.Error("File already exists. Should we overwrite it?")
		panic(nil)
		return nil
	}

	// Map the user file's absolute file path to the storage node that stores is
	fileContentHash := file.GetFileContentHash()
	randomStorageNode := storageNodes[rand.Int() % len(storageNodes)]
	cn.GlobalFileNodeMap[fileContentHash] = randomStorageNode.FireflyClient().Addr()

	// Actually send the file to the node by encoding it as a file.Message
	return cn.IfritClient.SendTo(randomStorageNode.FireflyClient().Addr(), file.Encoded())
}

func (cn *Clientnode) DeleteFile(file *file.File) error {
	return nil
}

func (cn *Clientnode) fileExistsInRemoteStorage(file *file.File) bool {
	mapKey := file.GetFilePathHash()
	if _, ok := cn.GlobalFileNodeMap[mapKey]; ok {
		return true
	} else {
		return false
	}
}

func (cn *Clientnode) GetIfritClient() *ifrit.Client {
	return cn.IfritClient
}

func (cn *Clientnode) DirectMessageHandler(data []byte) ([]byte, error) {
	fmt.Printf("%s\n", data)
	return data, nil
}