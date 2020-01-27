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
	"strings"
//	"path/filepath"
)

var FILE_DIRECTORY = "files"

// gRPC does not allow messages to exceed this size in bytes
const MAX_SHARD_SIZE = 1000000

// API used by clients and storage nodes to handle files
type File struct {
	File *os.File
	Fileinfo os.FileInfo
	FileContentHash string				// sha256 hash of the os.File's contents
	FilePathHash string		// sha256 hash of the absolute path
	OwnerID string
	RelativePath string		// extend os.FileInfo a little bit
	AbsolutePath string
}

func NewFile(fileSize uint64, directoryLocalID int, ownerID string) (*File, error) {	
	customFile := &File {
		OwnerID: ownerID,
	}

	currentWorkingDirectoy, err := os.Getwd()
	if err != nil {
		log.Error("Could not get current working directory")
		panic(err)
	}

	// Create FILES dir too...
	createUserDirectory(currentWorkingDirectoy, ownerID)
	absolutePath := fmt.Sprintf("%s/%s/%s/%d_file.txt", currentWorkingDirectoy, FILE_DIRECTORY, ownerID, directoryLocalID)
	relativePath := fmt.Sprintf("./%s/%s/%d_file.txt", FILE_DIRECTORY, ownerID, directoryLocalID)
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
	customFile.FileContentHash = customFile.computeFileContentHash()
	customFile.FilePathHash = customFile.computeFilePathHash()
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
	return f.FileContentHash
}

func (f *File) GetFilePathHash() string {
	return f.FilePathHash
}

func (f *File) GetAbsoluteFilePath() string {
	return f.AbsolutePath
}

func (f *File) EncodeFileInfo() (bytes.Buffer, error) {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	if err := encoder.Encode(&f.Fileinfo); err != nil {
	   panic(err)
	}

	return buffer, nil
}

func (f *File) EncodeHashString() (bytes.Buffer, error) {
	var buffer bytes.Buffer
	_ = gob.NewEncoder(&buffer)
	/*if err := encoder.Encode(f.Hash); err != nil {
	   panic(err)
	}*/

	return buffer, nil
}

func (f *File) Encoded() []byte {
	message := NewMessage(f)
	encodedMessage := message.Encoded()
	return encodedMessage
}

func Decoded(data []byte) *File {
	var message Message
	dataBytes := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(dataBytes)
	if err := decoder.Decode(&message); err != nil {
	   panic(err)
	}

	file := &File {
		FileContentHash: message.FileContentHash,
		FilePathHash: message.FilePathHash,
		OwnerID: message.OwnerID,
	}

	return file
}

// TODO: need to set appropriate mode too
func CreateFileFromBytes(absFilePath string, fileContents []byte) (*os.File, error) {
	if _, err := os.Stat(absFilePath); err == nil {
		log.Error("File exists on remote node! It shouldn't...")
	}

	file, err := os.Create(absFilePath)
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

/******************** Private methods ***********************/
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

func (f *File) computeFilePathHash() string {
	filePath := strings.NewReader(f.File.Name())

	hash := sha256.New()
	if _, err := io.Copy(hash, filePath); err != nil {
		log.Error("Could not copy from file to hasher")
	}
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
