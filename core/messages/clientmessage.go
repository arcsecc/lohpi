package messages

/** This API is used for message passing between master node and storage node */
import (
//	"fmt"
	//"bytes"
	//"encoding/gob"
	"firestore/core/file"
	"errors"
)

type Clientmessage struct {
	Subject string
	Node string
	Permission string
	SetPermission string
}

func (c *Clientmessage) IsValidFormat() error {
	if c.Permission == file.FILE_STORE || c.Permission == file.FILE_ANALYSIS || c.Permission == file.FILE_SHARE {
		if c.SetPermission == "set" || c.SetPermission == "unset" {
			return nil
		}
	}
	return errors.New("Client message is invalid")
}

func (c *Clientmessage) IsValidSubjectQuery() error {
	if c.IsValidFormat() == nil {
		if len(c.Subject) > 0 {
			return nil
		}
	}
	return errors.New("Subject query is invalid")
}

func (c *Clientmessage) IsValidNodeQuery() error {
	if c.IsValidFormat() == nil {
		if len(c.Node) > 0 {
			return nil
		}
	}
	return errors.New("Node query is invalid")
}

/*func NewClientMessage(subjectID string, messageType Msgtype, permission, setPermission string) *Message {
	self := &Message {
		Subject: subjectID,
		//Type: messageType,
		Permission: permission,
		SetPermission: setPermission,
	}

	return self
}*/

/*
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