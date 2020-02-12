package main

import (
_	"fmt"
	log "github.com/inconshreveable/log15"
//	"github.com/spf13/viper"
	"ifrit"
	"firestore/core/file"
	"firestore/core/messages"
	"firestore/core"
	"errors"
)

var (
	errInvalidPermission = errors.New("Requested to send an invalid permission")
)

// In order for us to simulate a read-world scenario, we do not maintain
// any application-like data structures in the masternode. This is because the state of
// the permissions is supposed to be held by the storage nodes and NOT master. Otherwise,
// we could simply delegate the enitre 
type Masternode struct {
	IfritClient *ifrit.Client
}

func NewMasterNode() (*Masternode, error) {
	ifritClient, err := ifrit.NewClient()
	if err != nil {
		return nil, err
	}

	go ifritClient.Start()
	self := &Masternode {
		IfritClient: ifritClient,
	}

	ifritClient.RegisterGossipHandler(self.GossipMessageHandler)
	ifritClient.RegisterResponseHandler(self.GossipResponseHandler)
	return self, nil
}

func (m *Masternode) GossipSubjectPermission(msg *messages.Clientmessage) error {
	if err := msg.IsValidSubjectQuery(); err != nil {
		log.Error("ClientMessage (subject-centric) is invalid")
		return err
	}
	internalMsg := messages.NewInternalMessage(msg.Subject, "PERMISSION_SET", msg.Permission, msg.SetPermission)
	encodedMsg := internalMsg.EncodedInternal()
	m.IfritClient.SetGossipContent([]byte(encodedMsg))
	return nil
}

func (m *Masternode) UpdateNodePermission(node *core.Node,  msg *messages.Clientmessage) error {
	if err := msg.IsValidNodeQuery(); err != nil {
		log.Error("ClientMessage (node-centric) is invalid")
		return err
	}
	dst := node.IfritClient().Addr()
	internalMsg := messages.NewInternalMessage(msg.Subject, "PERMISSION_SET", msg.Permission, msg.SetPermission)
	encodedMsg := internalMsg.EncodedInternal()
	<- m.FireflyClient().SendTo(dst, encodedMsg)
	return nil
}

func (m *Masternode) SendMessage(s *core.Subject, node *core.Node, permission string) {
	/*dst := node.FireflyClient().Addr()
	msg := message.NewMessage(s.Name(), message.PERMISSION_GET, permission)
	encodedMsg := msg.Encoded()
	<- m.FireflyClient().SendTo(dst, encodedMsg)*/
}

func (m *Masternode) MessageHandler(data []byte) ([]byte, error) {

	return nil, nil
}

func (m *Masternode) GossipMessageHandler(data []byte) ([]byte, error) {
	//fmt.Printf("masternode gossip msg handler: %s\n", string(data))
	return nil, nil
}

func (m *Masternode) GossipResponseHandler(data []byte) {
	//fmt.Printf("masternode gossip response handler: %s\n", string(data))
	//m.IfritClient.SetGossipContent([]byte("kake"))
}

func (m *Masternode) FireflyClient() *ifrit.Client {
	return m.IfritClient
}

func (m *Masternode) IsValidPermission(permission string) bool {
	if permission == file.FILE_STORE || permission == file.FILE_ANALYSIS || permission == file.FILE_SHARE {
		return true
	}
	return false
}

func (m *Masternode) Shutdown() {
	ifritClient := m.FireflyClient()
	log.Info("Master node shutting down...")
	ifritClient.Stop()
}
