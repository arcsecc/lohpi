package file

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
//	"path/filepath"
)

var FILE_DIRECTORY = "files"

// gRPC does not allow messages to exceed this size in bytes
const MAX_SHARD_SIZE = 1000000

// API used by clients and storage nodes to handle files

type File struct {
	File *os.File
	Fileinfo os.FileInfo
	ContentHash string				// sha256 hash of the os.File's contents
	OwnerID string
	RelativePath string		// extend os.FileInfo a little bit
}

// used to send files from a client to the network.
type Message struct {
	FileContents []byte
	FileContentHash string
	OwnerID string
	AbsoluteFilePath string
	RelativeFilePath string 
	LocalModTime int64
	PreviousModifier string
}

func NewFile(fileSize uint64, order int, ownerID string) (*File, error) {	
	customFile := &File {
		OwnerID: ownerID,
	}

	currentWorkingDirectoy, err := os.Getwd()
	if err != nil {
		log.Error("Could not get current working directory")
		panic(err)
	}

	createUserDirectory(currentWorkingDirectoy, ownerID)
	absolutePath := fmt.Sprintf("%s/%s/%s/%d_file.txt", currentWorkingDirectoy, FILE_DIRECTORY, ownerID, order)
	relativePath := fmt.Sprintf("./%s/%s/%d_file.txt", FILE_DIRECTORY, ownerID, order)
	emptyFile, err := os.OpenFile(absolutePath, os.O_RDONLY|os.O_CREATE|os.O_WRONLY, 0644)
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
	customFile.ContentHash = customFile.computeFileContentHash()
	customFile.PathHash = customFile.computeFilePathHash()
	customFile.Fileinfo, _ = os.Stat(absolutePath)
	customFile.RelativePath = relativePath
	return customFile, nil
}

func (f *File) GetFile() (*os.File) {
	return f.File
}

func (f *File) GetFileAsBytes() []byte {
	file, _ := os.Open(f.File.Name())
	bytes, err := ioutil.ReadFile(file.Name())
	if err != nil {
		log.Error("Could not read file")
	}
	defer file.Close()

	buffer := make([]byte, len(bytes))
	copy(buffer, bytes)
	
	return buffer
}

func (f *File) GetFileInfo() (os.FileInfo) {
	return f.Fileinfo
}

func (f *File) GetFileContentHash() string {
	return f.ContentHash
}

func (f *File) GetFileHash() string {
	return f.PathHash
}

func (f *File) DecodeFileInfo() (bytes.Buffer, error) {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	if err := encoder.Encode(&f.Fileinfo); err != nil {
	   panic(err)
	}

	return buffer, nil
}

func (f *File) DecodeHashString() (bytes.Buffer, error) {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	if err := encoder.Encode(f.Hash); err != nil {
	   panic(err)
	}

	return buffer, nil
}

func (f *File) Encoded() ([]byte, error) {
	message, _ := NewMessage(f)
	encodedMessage := message.Encoded()
	return encodedMessage, nil
}

// TODO: need to set appropriate mode too
func CreateFileFromBytes(remoteRelativePath string, fileContents []byte) (*os.File, error) {
	if _, err := os.Stat(remoteRelativePath); err == nil {
		log.Error("File exists on remote node! It shouldn't...")
	}

	file, err := os.Create(remoteRelativePath)
	if err != nil {
		panic(err)
	}

	_, err = file.Write(fileContents)
	if err != nil {
		log.Error("Could not write raw bytes to file on storage node")
		panic(err)
	}

	file.Sync()

	defer file.Close()
	return file, nil
}

/** Private methods */
func (f *File) computeFileContentHash() string {
	file, err := os.Open(f.File.Name())
	if err != nil {
		panic(err)
	}

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		log.Error("Could not copy from file to hasher")
	}
	defer file.Close()
	return string(hash.Sum(nil)[:])
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
