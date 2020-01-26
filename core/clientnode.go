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
	Filemap map[string]*Node
	IfritClient *ifrit.Client
	UserNodeID string
}

func NewClientNode(NodeID string) *Clientnode {
	fileMap := make(map[string]*Node, 0)
	ifritClient, err := ifrit.NewClient()
	if err != nil {
		panic(err)
	}

	self := &Clientnode {
		Filemap: fileMap,
		IfritClient: ifritClient,
	}

	ifritClient.RegisterMsgHandler(self.DirectMessageHandler)
	go ifritClient.Start()
	// gossip handlers too
	return self
}

func (cn *Clientnode) StoreFileRemotely(file *file.File, storageNodes []*Node) chan []byte {
	if cn.fileExists(file) == true {
		log.Error("File already exists. Should we overwrite it?")
		panic(nil)
		return nil
	}

	// Map the user file's absolute file path to the storage node that stores is
	fileContentHash := file.GetFileContentHash()
	randomStorageNode := storageNodes[rand.Int() % len(storageNodes)]
	cn.Filemap[fileContentHash] = randomStorageNode

	// Actually send the file to the node by encoding it as a Message)
	return cn.IfritClient.SendTo(randomStorageNode.FireflyClient().Addr(), file.Encoded())
}

func (cn *Clientnode) DeleteFile(file *file.File) error {
	return nil
}

func (cn *Clientnode) fileExists(file *file.File) bool {
	mapKey := file.GetFilePathHash()
	if _, ok := cn.Filemap[mapKey]; ok {
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