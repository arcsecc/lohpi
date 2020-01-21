
package main

import (
	"os"
	"bytes"
	"encoding/gob"
	"fmt"
	log "github.com/inconshreveable/log15"
	"math/rand"
	"crypto/sha256"
	"io"
	"io/ioutil"
)

var FILE_DIRECTORY = "files"

// gRPC does not allow messages to exceed this size in bytes
const MAX_SHARD_SIZE = 1000000

type File struct {
	File *os.File
	Fileinfo os.FileInfo
	Owner *User
	Hash string
}

func NewFile(fileSize uint64, order int, user *User) (*File, error) {	
	customFile := &File {
		Owner: user,
	}

	currentWorkingDirectoy, err := os.Getwd()
	if err != nil {
		log.Error("Could not get current working directory")
		panic(err)
	}

	createUserDirectory(currentWorkingDirectoy, user.Name())
	path := fmt.Sprintf("%s/%s/%s/%d_file.txt", currentWorkingDirectoy, FILE_DIRECTORY, user.Name(), order)
	emptyFile, err := os.OpenFile(path, os.O_RDONLY|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
		log.Error("Could not open a new file")
        return nil, err
	}
	
	defer emptyFile.Close()
	if err != nil {
		panic(err)
	}
	
	customFile.File = emptyFile
	fillEmptyFile(fileSize, emptyFile)
	customFile.Hash = customFile.computeFileHash()
	customFile.Fileinfo, _ = os.Stat(path)
	return customFile, nil
}

func (f *File) GetFile() (*os.File) {
	return f.File
}

func (f *File) GetFileAsBytes() []byte {
	// open...
	bytes, err := ioutil.ReadFile(f.File.Name()) // just pass the file name
	if err != nil {
		log.Error("Could not read file")
	}
	buffer := make([]byte, len(bytes))
	copy(buffer, bytes)
	// close
	return buffer
}

func (f *File) GetFileInfo() (os.FileInfo) {
	return f.Fileinfo
}

func (f *File) GetFileOwner() (*User) {
	return f.Owner
}

func (f *File) computeFileHash() string {
	// open file...
	file, err := os.Open(f.File.Name())
	if err != nil {
		panic(err)
	}

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		log.Error("Could not copy from file to hasher")
	}
	defer file.Close()
	// close file...
	return string(hash.Sum(nil)[:])
}

func (f *File) GetFileHash() string {
	return f.Hash
}

func (f *File) DecodeFileInfo() (bytes.Buffer, error) {
	// open file...
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	if err := encoder.Encode(&f.Fileinfo); err != nil {
	   panic(err)
	}

	// close file...
	return buffer, nil
}

func (f *File) DecodeHashString() (bytes.Buffer, error) {
	// open file...
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	if err := encoder.Encode(f.Hash); err != nil {
	   panic(err)
	}

	// close file...
	return buffer, nil
}

func createUserDirectory(wd, userDir string) {
	fullPath := fmt.Sprintf("%s/%s/%s/", wd, FILE_DIRECTORY, userDir)
	_ = os.Mkdir(fullPath, 0774)
}

func fillEmptyFile(fileSize uint64, file *os.File) {
	// open file...
	fileContents := make([]byte, fileSize)

	_, err := rand.Read(fileContents)
	if err != nil { 
		log.Error("Could not read random data from seed")
		panic(err)
	}

	n, err := file.Write(fileContents)
	if err != nil {
		fmt.Errorf("Should write %d bytes -- wrote %d instead\n", fileSize, n)
		panic(err)
		log.Error("Could not write random data to file")
	}
	// close file...
}
