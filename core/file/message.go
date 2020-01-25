package file

import (
	"bytes"
	"encoding/gob"
)

/** Message interface */
func NewMessage(file *File) (*Message, error) {
	fileContents := file.GetFileAsBytes()
	fileHash := file.GetFileHash()
	fileOwner := file.OwnerID
	absolutePath := file.File.Name()
	relativePath := file.RelativePath
	modTime := file.Fileinfo.ModTime().Unix()
	
	message := &Message {
		FileContents: fileContents,
		FileHash: fileHash,
		OwnerID: fileOwner,
		AbsoluteFilePath: absolutePath,
		RelativeFilePath: relativePath,
		LocalModTime: modTime,
		PreviousModifier: fileOwner,
	}

	return message, nil
}

func (m *Message) Encoded() []byte {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	if err := encoder.Encode(m); err != nil {
	   panic(err)
	}
	
	return buffer.Bytes()
}

func DecodedMessage(encodedMessage []byte) (*Message, error) {
	var decodedMessage Message
	buffer := bytes.NewBuffer(encodedMessage)
	decoder := gob.NewDecoder(buffer)
	if err := decoder.Decode(&decodedMessage); err != nil {
	   panic(err)
	}

	return &decodedMessage, nil
}

func (m *Message) SetAbsoluteFilePath(absFilePath string) {
	m.AbsoluteFilePath = absFilePath
} 