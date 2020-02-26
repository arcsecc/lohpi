package mux

import (
//	"fmt"
//	"os"
//	"ifrit"
	"errors"
//	log "github.com/inconshreveable/log15"
	"log"
//	"path/filepath"
//	"firestore/node/fuse"
	"firestore/core/file"
	"firestore/core/messages"
_	"strings"
	//"net/http"
//	"encoding/gob"
_	"bytes"
//	"time"
)

var (
	errCreateRandomClientData 	= errors.New("Could not set client data.")

)

//ons 26.02 kl 1445
//5w 30 full syntetisk

const NODE_DIR = "storage_nodes"

// A Mux is an entity that multiplexes incoming and outgoing messages to the correct 
// recipient based on some rules. It either passes the messages to  
type Mux struct {	
	// Use locks? in case of concurrency...
	// The current message in 
	MessageType string
}

func NewMux() *Mux {
	return &Mux{}
}

// Invoked when this client receives a message
func (m *Mux) MuxMessageReceiver(data []byte) ([]byte, error) {
	msg := messages.DecodedInternalMessage(data)
	switch msgType := msg.Type; msgType {
		case messages.PERMISSION_GET:
			log.Printf("Message: PERMISSION_GET")
		default:
			log.Printf("Unkown message")
	}
    return []byte("kake"), nil
}

// This callback will be invoked on each received gossip message.
func (m *Mux) GossipMessageHandler(data []byte) ([]byte, error) {
/*	msg := messages.DecodedInternalMessage(data)
	targetSubject := msg.Subject
	
	// Find file in the node. If it exists, set new permission
	if (msg.Type == messages.PERMISSION_SET) {
		if strings.Contains(file.AbsolutePath, targetSubject) {
			if msg.SetPermission == "set" {
				//fmt.Printf("set permission %s to true", msg.Permission)
				file.SetPermission(msg.Permission, true)
			} else if msg.SetPermission == "unset" {
				file.RemovePermission(msg.Permission)
			} else {
				fmt.Errorf("%s switch is not valid\n", msg.SetPermission)
			}
		}
	} else {
		fmt.Errorf("%s type is not valid\n", msg.Type)
	}

	// Put this into if branch*/
    return []byte(messages.MSG_NEW_PERM_SET), nil
}

// This callback will be invoked on each received gossip response.
func (m *Mux) GossipResponseHandler(data []byte) {
}

// Should be called from elsewhere to assign subjects to files,
// and in turn, files to this node
func (m *Mux) SetSubjectFiles(files []*file.File) {

}

func (m *Mux) AppendSubjectFile(file *file.File) {

}

func (m *Mux) SubjectFiles() []*file.File {
	return nil
}

func (m *Mux) Shutdown() {
//	ifritClient := m.IfritClient()
//	log.Printf("Node", ifritClient.Addr(), "shutting down...")
//	ifritClient.Stop()
//	n.fs.Shutdown()
}