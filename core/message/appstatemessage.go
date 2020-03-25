package message
/*
import (
	"bytes"
	"encoding/gob"
)

// Used to populate the 
type AppStateMessage struct {
	Type Msgtype
	Node string
	Subject string
	Study string
	Permission string
	NumFiles int
	FileSize int

}

func NewAppStateMessage(msgType Msgtype, node, subject, study, permission string, numFiles, fileSize int) *AppStateMessage {
	return &AppStateMessage {
		Type: msgType, 
		Node: node,
		Subject: subject,
		Study: study,
		Permission: permission,
		NumFiles: numFiles,
		FileSize: fileSize, 
	}
}

func (m *AppStateMessage) Encoded() []byte {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	if err := encoder.Encode(m); err != nil {
	   panic(err)
	}
	
	return buffer.Bytes()
}*/