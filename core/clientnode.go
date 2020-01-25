package core

import (
	"ifrit"
	"firestore/core/file"
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

	return self
}

func (cn *Clientnode) StoreFile(file *file.File) {
	/*hash := file.GetFileHash()
	cn.Filemap[hash] = file*/
}

func (cn *Clientnode) DeleteFile(file *file.File) error {
	return nil
}

func (cn *Clientnode) FileExists(file *file.File) bool {
	return false
}

func (cn *Clientnode) GetIfritClient() *ifrit.Client {
	return cn.IfritClient
}