package main

import (
//	"fmt"
	"errors"
//	"math"
	//"math/rand"
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
	Files []*File
	OwnerName string
}

func NewUser(ownerName string) (*User, error) {
	self := &User {
	}
	
	self.OwnerName = ownerName
	return self, nil
}

func (u *User) SetFiles(files []*File) {
	u.Files = files
}

func (u *User) UserFiles() []*File {
	return u.Files
}

func (u *User) Name() string {
	return u.OwnerName
}