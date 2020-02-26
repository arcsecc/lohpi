package core

import (
//	"fmt"
	"firestore/core/file"
//	"log"
//	"os"
)

type Subject struct {
	SubjectName string
	dirPath string
	files []*file.File
	// perhaps some form of secure ID. A certificate?
}

func NewSubject(ID, absDirPath string) *Subject {
	self := &Subject {
		SubjectName: ID,
		dirPath: absDirPath,
	}

	/*if err := os.MkdirAll(self.dirPath, 0755); err != nil {
		log.Fatal(err)
	}*/

	return self
}

func (s *Subject) ID() string {
	return s.SubjectName
}

func (s *Subject) SetFiles(files []*file.File) {
	s.files = files
}

func (s *Subject) Files() []*file.File {
	return s.files
}

func (s *Subject) DirPath() string {
	return s.dirPath
}