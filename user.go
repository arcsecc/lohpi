package main

import (
	"errors"
//	"math"
	//"math/rand"
	"firestore/core/file"
	"firestore/core"
)

var (
	errCreateRandomData			= errors.New("Could not create random")
	errHashClientData 			= errors.New("Could not hash client's data")
	errAddClientNode 			= errors.New("Could not add ClientNode to User")
)

type User struct {
	// RSA key here..? need authenticion of some sort...
	// do precomputations -> func (*PrivateKey) Precompute!
	//PrivateKey *rsa.PrivateKey
	//PublicKey *rsa.PublicKey
	Files []*file.File
	ClientNode *core.Clientnode
	OwnerName string

	// should remove this ugly hack 
	StorageNodes []*core.Node
}

func NewUser(ownerName string) *User {
	clientNode := core.NewClientNode(ownerName)
	self := &User {
		OwnerName: ownerName,
		ClientNode: clientNode,
	}

	return self
}

func (u *User) SetFiles(files []*file.File) {
	u.Files = files
}

func (u *User) UserFiles() []*file.File {
	return u.Files
}

func (u *User) Name() string {
	return u.OwnerName
}

func (u *User) StoreFileRemotely(file *file.File) chan []byte {
	return u.ClientNode.StoreFileRemotely(file, u.StorageNodes)
}

/*func (u *User) FileExists(file *file.File) bool {
	return u.ClientNode.FileExists(file)
}*/

func (u *User) DeleteFile(file *file.File) {
	u.ClientNode.DeleteFile(file)
}

// HACK
func (u *User) SetStorageNodesList(storageNodes []*core.Node) {
	u.StorageNodes = storageNodes
}

/*
func (u *User) UpdateFile(file *File, data []byte) error {
	return u.ClientNode.UpdateFile(file, data)
}*/
