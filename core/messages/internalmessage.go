package messages

/** This API is used for message passing between master node and storage node */
import (
//	"fmt"
	"bytes"
	"encoding/gob"
	//"firestore/core/file"
)

type Msgtype string 

const (
	PERMISSION_SET Msgtype = "PERMISSION_SET"
	PERMISSION_GET Msgtype = "PERMISSION_GET"
	FILE_GET Msgtype = "FILE_GET"
	FILE_DISTRIBUTE Msgtype = "FILE_DISTRIBUTE"
)

const (
	MSG_EMPTY_FIELD = "Empty message field"
	MSG_EMPTY_MESSAGE = "Empty message"
	MSG_NEW_PERM_SET = "new permission set"
	
)

type Internalmessage struct {
	Subject string
	Node string
	Permission string
	SetPermission string
	Type Msgtype
}

func NewInternalMessage(subjectID string, messageType Msgtype, permission, setPermission string) *Internalmessage {
	self := &Internalmessage {
		Subject: subjectID,
		Type: messageType,
		Permission: permission,
		SetPermission: setPermission,
	}

	return self
}

func (m *Internalmessage) EncodedInternal() []byte {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	if err := encoder.Encode(m); err != nil {
	   panic(err)
	}
	
	return buffer.Bytes()
}

func DecodedInternalMessage(encodedMessage []byte) *Internalmessage {
	var decodedMessage Internalmessage
	buffer := bytes.NewBuffer(encodedMessage)
	decoder := gob.NewDecoder(buffer)
	if err := decoder.Decode(&decodedMessage); err != nil {
	   panic(err)
	}

	return &decodedMessage
}


/*
// used to send files from a client to the network.
type Message struct {
	MessageType Msgtype
	FileContents []byte
	FileContentHash string
	FilePathHash string
	OwnerID string
	LocalModTime int64
	PreviousModifier string
	RemoteAbsolutePath string
}


* Message interface *
func NewMessage(file *File) *Message {
	fileContents := file.GetFileAsBytes()
	fileContentHash := file.GetFileContentHash()
	filePathHash := file.GetFilePathHash()
	fileOwner := file.OwnerID
	//absolutePath := file.File.Name()
	//relativePath := file.RelativePath
	modTime := file.FileInfo.ModTime().Unix()
	
	message := &Message {
		FileContents: fileContents,
		FileContentHash: fileContentHash,
		FilePathHash: filePathHash,
		OwnerID: fileOwner,
		//AbsoluteFilePath: absolutePath,
		//RelativeFilePath: relativePath,
		LocalModTime: modTime,
		PreviousModifier: fileOwner,
	}

	return message
}

func (m *Message) Encoded() []byte {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	if err := encoder.Encode(m); err != nil {
	   panic(err)
	}
	
	return buffer.Bytes()
}

func DecodedMessage(encodedMessage []byte) *Message {
	var decodedMessage Message
	buffer := bytes.NewBuffer(encodedMessage)
	decoder := gob.NewDecoder(buffer)
	if err := decoder.Decode(&decodedMessage); err != nil {
	   panic(err)
	}

	return &decodedMessage
}
*/