package core

import (
	"fmt"
//	log "github.com/inconshreveable/log15"
//	"github.com/spf13/viper"
	"ifrit"
//	"firestore/core/file"
	"firestore/core/message"
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

// All public API calls change the state as provided by the parameters.
// This is important to keep in mind because the order in which the methods
// are invoked does not matter. Also, remember that the operation are idempotent.

// Permit use to both storage nodes and data users 
func (m *Masternode) GossipStorageNetwork(s *Subject, permission string) {
	msg := message.NewMessage(s.Name(), message.PERMISSION_GET, permission)
	encodedMsg := msg.Encoded()
	m.IfritClient.SetGossipContent([]byte(encodedMsg))
}

// Permit only to a subset of data users
func (m *Masternode) SendStoragePermission(s *Subject, node *Node, permission string) {
	dst := node.FireflyClient().Addr()
	msg := message.NewMessage(s.Name(), message.PERMISSION_GET, permission)
	encodedMsg := msg.Encoded()
	<- m.FireflyClient().SendTo(dst, encodedMsg)
}

func (m *Masternode) MessageHandler(data []byte) ([]byte, error) {

	return nil, nil
}

func (m *Masternode) GossipMessageHandler(data []byte) ([]byte, error) {
	fmt.Printf("masternode gossip msg handler: %s\n", string(data))
	return nil, nil
}

func (m *Masternode) GossipResponseHandler(data []byte) {
	fmt.Printf("masternode gossip response handler: %s\n", string(data))
}

func (m *Masternode) FireflyClient() *ifrit.Client {
	return m.IfritClient
}