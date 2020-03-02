package messages

/** This API is used for message passing between master node and storage node */
import (
//	"fmt"
//	"bytes"
	"encoding/json"
	//"firestore/core/file"
)

type Msgtype string 

const (
	PERMISSION_SET Msgtype = "PERMISSION_SET"
	PERMISSION_GET Msgtype = "PERMISSION_GET"
	FILE_GET Msgtype = "FILE_GET"
	FILE_DISTRIBUTE Msgtype = "FILE_DISTRIBUTE"
	
	MSG_TYPE_GET_NODE_INFO = "MSG_TYPE_GET_NODE_INFO"
	MSG_TYPE_SET_SUBJECT_NODE_PERMISSION = "MSG_TYPE_SET_SUBJECT_NODE_PERMISSION"
	MSG_TYPE_SET_NODE_PERMISSION = "MSG_TYPE_SET_NODE_PERMISSION"
	MSG_TYPE_SET_SUBJECT_PERMISSION = "MSG_TYPE_SET_SUBJECT_PERMISSION"
	MSG_TYPE_SET_NODE_FILES = "MSG_TYPE_SET_NODE_FILES"
	MSG_TYPE_NEW_STUDY = "MSG_TYPE_NEW_STUDY"
	MSG_TYPE_LOAD_NODE = "MSG_TYPE_LOAD_NODE"
)

const (
	MSG_EMPTY_FIELD = "Empty message field"
	MSG_EMPTY_MESSAGE = "Empty message"
	MSG_NEW_PERM_SET = "new permission set"	
)

type Message struct {
	MessageType Msgtype 
	Node string 			`json:",omitempty"`
	Study string 			`json:",omitempty"`
	Subject string 			`json:",omitempty"`
	Permission string 		`json:",omitempty"`
}

// Message used to create a new study
/*type StudyMessage struct {
	Node string
	Study string
	Type Msgtype
}*/

func NewMessage(data []byte) *Message {
	var msg Message
	err := json.Unmarshal(data, &msg)
	if err != nil {
		panic(err)
	}

	return &msg
}

/*func NewStudyMessage(node, study string, messageType Msgtype) *StudyMessage {
	return &StudyMessage {
		Node: node,
		Study: study,
		Type: messageType,
	}
}

func (s *StudyMessage) Encode() []byte {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	if err := encoder.Encode(s); err != nil {
	   panic(err)
	}
	
	return buffer.Bytes()
}*/

/*func DecodedStudyMessage(encodedMessage []byte) *StudyMessage {
	var msg StudyMessage
	buffer := bytes.NewBuffer(encodedMessage)
	decoder := gob.NewDecoder(buffer)
	if err := decoder.Decode(&msg); err != nil {
	   panic(err)
	}

	return &msg
}*/

type InternalMessage struct {
	Subject string
	Node string
	Type Msgtype
	Permission string
}

func NewInternalMessage(subjectID string, nodeID string, messageType Msgtype, permission string) *InternalMessage {
	self := &InternalMessage {
		Subject: subjectID,
		Node: nodeID,
		Type: messageType,
		Permission: permission,
	//	SetPermission: setPermission,
	}

	return self
}

/*
func (m *InternalMessage) Encoded() []byte {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	if err := encoder.Encode(m); err != nil {
	   panic(err)
	}
	
	return buffer.Bytes()
}

func DecodedInternalMessage(encodedMessage []byte) *InternalMessage {
	var decodedMessage InternalMessage
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