package core

import (
//	"fmt"
"firestore/core/file"
)

type Subject struct {
	SubjectName string
	Files []*file.File
	// perhaps some form of secure ID. A certificate?
}

func NewSubject(ID string) *Subject {
	self := &Subject {}
	self.SubjectName = ID

	return self
}

func (s *Subject) Name() string {
	return s.SubjectName
}

func (s *Subject) SetFiles(files []*file.File) {
	s.Files = files
}
