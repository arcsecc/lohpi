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
	"github.com/pkg/xattr"
	"path/filepath"
	"strconv"
)

var FILE_SHARE 			= "SHARE"	// Cannot be used by data users/analysers
var FILE_READ 			= "READ"	
var FILE_ANALYSIS 		= "ANALYSIS"

// gRPC does not allow messages to exceed this size in bytes
const MAX_SHARD_SIZE = 1000000

// API used by clients and storage nodes to handle files
type File struct {
	File *os.File
	FileInfo os.FileInfo
	FileContentHash string				// sha256 hash of the os.File's contents
	FilePathHash string		// sha256 hash of the absolute path
	SubjectID string
	OwnerID string
	RelativePath string		// extend os.FileInfo a little bit for remote storage purposes
	AbsolutePath string
	FilePermissionString string 
}

func NewFile(fileSize int, absolutePath, subjectID, ownerID, permission string) (*File, error) {	
	createDirectoryTree(absolutePath)
	emptyFile, err := os.OpenFile(absolutePath, os.O_RDONLY|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
		log.Error("Could not open a new file")
        return nil, err
	}
	
	defer emptyFile.Close()
	fillEmptyFile(fileSize, emptyFile)
	fileStat, err := os.Stat(absolutePath)
	if err != nil {
		panic(err)
	}

	fileContentHash := computeFileContentHash(absolutePath)
	file := &File {
		File: emptyFile,
		FileInfo: fileStat,
		FileContentHash: fileContentHash,
		SubjectID: subjectID,
		OwnerID: ownerID,
		AbsolutePath: absolutePath,
		FilePermissionString: permission,
	}
	
	file.setExtendedFileAttribute(true)
	return file, nil
}

func (f *File) setExtendedFileAttribute(flag bool) {
	const prefix = "user."			// must be used as leading token
	absFilePath := f.AbsolutePath
	attribute := fmt.Sprintf("%s%s - %s", prefix, f.OwnerID, f.FilePermission())
	if err := xattr.Set(absFilePath, attribute, []byte(strconv.FormatBool(flag))); err != nil {
		panic(err)
	}

	//data, err := xattr.Get(absFilePath, attribute); 
	//if err != nil {
	//	panic(err)
//	}
	
  	//fmt.Printf("%s\n", data)
}

func (f *File) RemovePermission(permission string) {
	const prefix = "user."			// must be used as leading token
	absFilePath := f.AbsolutePath
	attribute := fmt.Sprintf("%s%s - %s", prefix, f.OwnerID, permission)
	xattr.Remove(absFilePath, attribute)
}

func (f *File) AllPermissions() {
	
	list, err := xattr.List(f.AbsolutePath); 
	if err != nil {
		panic(err)
	}
	
	for _, attribute := range list {
		data, err := xattr.Get(f.AbsolutePath, attribute);
		if err != nil {
	  		panic(err)
		}

		fmt.Printf("%s\n", string(data[1:]))
	}
}

func (f *File) SetPermission(permission string, flag bool) {
	f.FilePermissionString = permission
	f.setExtendedFileAttribute(flag)
}

func (f *File) FileSubject() string {
	return f.SubjectID
}

func (f *File) FilePermission() string {
	return f.FilePermissionString
}

func (f *File) OSFile() (*os.File) {
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
	return f.FileInfo
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
	if err := encoder.Encode(&f.FileInfo); err != nil {
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
//	message := NewMessage(f)
//	encodedMessage := message.Encoded()
//	return encodedMessage
return nil
}

func Decoded(data []byte) *File {
	/*var message Message
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
*/
	return nil
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
func computeFileContentHash(filePath string) string {
	file, err := os.Open(filePath)
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

/*
func createUserDirectory(wd, userDir string) {
	fullPath := fmt.Sprintf("%s/%s/%s/", wd, FILE_DIRECTORY, userDir)
	_ = os.Mkdir(fullPath, 0774)
}*/

func fillEmptyFile(fileSize int, file *os.File) {
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

func createDirectoryTree(absolutePath string) {
	dir, _ := filepath.Split(absolutePath)
	err := os.MkdirAll(dir, 0755) 
	if err != nil {
		panic(err)
	}
}