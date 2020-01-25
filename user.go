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
)

type User struct {
	// RSA key here..? need authenticion of some sort...
	// do precomputations -> func (*PrivateKey) Precompute!
	//PrivateKey *rsa.PrivateKey
	//PublicKey *rsa.PublicKey
	Files []*file.File
	ClientNode *core.Clientnode
	OwnerName string
}

func NewUser(ownerName string) (*User, error) {
	self := &User {}
	self.OwnerName = ownerName
	return self, nil
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

func (u *User) SetAPIFrontEnd(cn *core.Clientnode) {
	u.ClientNode = cn
}

func (u *User) GetAPIFrontEnd() *core.Clientnode {
	return u.ClientNode
}
