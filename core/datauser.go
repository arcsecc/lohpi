package core

import (
	"firestore/core/file"
)

type Datauser struct {
	Files []*file.File
	Name string
}

func NewDataUser(name string) *Datauser {
	self := &Datauser {
		Name: name,
	}

	return self
}