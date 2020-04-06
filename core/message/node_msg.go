package message

/** This API is used for message passing between mux and storage nodes */
import (
	"fmt"
	"errors"
//	"bytes"
	//"firestore/core/file"
)

type MsgType string 

const (
	PERMISSION_SET MsgType = "PERMISSION_SET"
	PERMISSION_GET MsgType = "PERMISSION_GET"
	FILE_GET MsgType = "FILE_GET"
	FILE_DISTRIBUTE MsgType = "FILE_DISTRIBUTE"
	
	MSG_TYPE_GET_NODE_INFO = "MSG_TYPE_GET_NODE_INFO"
	MSG_TYPE_SET_SUBJECT_NODE_PERMISSION = "MSG_TYPE_SET_SUBJECT_NODE_PERMISSION"
	MSG_TYPE_SET_NODE_PERMISSION = "MSG_TYPE_SET_NODE_PERMISSION"
	MSG_TYPE_SET_SUBJECT_PERMISSION = "MSG_TYPE_SET_SUBJECT_PERMISSION"
	MSG_TYPE_SET_NODE_FILES = "MSG_TYPE_SET_NODE_FILES"
	MSG_TYPE_NEW_STUDY = "MSG_TYPE_NEW_STUDY"
	MSG_TYPE_LOAD_NODE = "MSG_TYPE_LOAD_NODE"
	MSG_TYPE_GET_NODE_ID = "MSG_TYPE_GET_NODE_ID"
)

const (
	MSG_EMPTY_FIELD = "Empty message field"
	MSG_EMPTY_MESSAGE = "Empty message"
	MSG_NEW_PERM_SET = "new permission set"	
)

var errInvalidMessageType = errors.New("Invalid message type. Expected map[string]interface{}")

type BulkDataCreator struct {
	MessageType MsgType
	Node 	string 				`json:"node"`
	Subject string 				`json:"subject"`
	Study string 				`json:"study"`
	Attributes map[string]interface {}	 	`json:"allowed_attributes"`
	DefaultAccess bool  		`json:"default_access"`
	NumFiles float64 				`json:"num_files"`
	FileSize float64 				`json:"file_size"`
}

type NodeMessage struct {
	MessageType MsgType
}

// Returns a map of attributes -> [value1, value2, ...]. Can extended even further!!1!
func FilterStringAttributes(attributes map[string]interface{}) (map[string]string, error) {
	attrCollection := make(map[string]string)
	
	for attrKey, attrArray := range attributes {
		fmt.Printf("%s\t%s\n", attrKey, attrArray)
		/*if attrArray, ok := attrArray.([]interface{}); ok {
			attrStringArray := make([]string, 0)
			for _, key:= range attrArray {
				s := fmt.Sprintf("%s", key)
				attrStringArray = append(attrStringArray, s)
			}
			attrCollection[attrKey] = attrStringArray
		} else {
			return nil, errInvalidMessageType
		}*/
	}

	return attrCollection, nil
}

// Used for arbitrarily long map[string]interface{} collections
/*func FilterStringAttributes(attributes map[string]interface{}) (map[string][]string, error) {
	
	attrCollection := make(map[string][]string)
	
	for attrKey, attrArray := range attributes {
		if attrArray, ok := attrArray.([]interface{}); ok {
			attrStringArray := make([]string, 0)
			for _, key:= range attrArray {
				s := fmt.Sprintf("%s", key)
				attrStringArray = append(attrStringArray, s)
			}
			attrCollection[attrKey] = attrStringArray
		} else {
			return nil, errInvalidMessageType
		}
	}

	return attrCollection, nil
}
*/
/*func NewStudyMessage(node, study string, messageType MsgType) *StudyMessage {
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
	Type MsgType
	Permission string
}

func NewInternalMessage(subjectID string, nodeID string, messageType MsgType, permission string) *InternalMessage {
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