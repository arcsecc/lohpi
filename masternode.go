package main

import (
	"fmt"
//	log "github.com/inconshreveable/log15"
//	"github.com/spf13/viper"
	"ifrit"
//	"firestore/core/file"
	"firestore/core/message"
	"firestore/core"
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

func (m *Masternode) GossipSubjectPermission(s *core.Subject, permission string) {
	msg := message.NewMessage(s.Name(), message.PERMISSION_SET, permission)
	encodedMsg := msg.Encoded()
	m.IfritClient.SetGossipContent([]byte(encodedMsg))
}

func (m *Masternode) UpdateNodePermission(node *core.Node, permission string) {
	dst := node.FireflyClient().Addr()
	msg := message.NewMessage(message.MSG_EMPTY_FIELD, message.PERMISSION_SET, permission)
	encodedMsg := msg.Encoded()
	<- m.FireflyClient().SendTo(dst, encodedMsg)
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
	fmt.Printf("masternode gossip msg handler: %s\n", string(data))
	return nil, nil
}

func (m *Masternode) GossipResponseHandler(data []byte) {
	fmt.Printf("masternode gossip response handler: %s\n", string(data))
	//m.IfritClient.SetGossipContent([]byte("kake"))
}

func (m *Masternode) FireflyClient() *ifrit.Client {
	return m.IfritClient
}