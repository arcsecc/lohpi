package main

import (
	"fmt"
	log "github.com/inconshreveable/log15"
	//	"github.com/spf13/viper"
	"errors"
	"firestore/core"
	"firestore/core/file"
	"firestore/core/messages"
	"firestore/node"
	"ifrit"
	"encoding/json"
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
	Nodes       []*node.Node
	Subjects    []*core.Subject
}

func NewMasterNode() (*Masternode, error) {
	ifritClient, err := ifrit.NewClient()
	if err != nil {
		return nil, err
	}

	go ifritClient.Start()
	self := &Masternode{
		IfritClient: ifritClient,
	}

	ifritClient.RegisterGossipHandler(self.GossipMessageHandler)
	ifritClient.RegisterResponseHandler(self.GossipResponseHandler)
	return self, nil
}

/** PUBLIC METHODS */
func (m *Masternode) SetNetworkNodes(nodes []*node.Node) {
	m.Nodes = nodes
}

func (m *Masternode) SetNetworkSubjects(subjects []*core.Subject) {
	m.Subjects = subjects
}

func (m *Masternode) FireflyClient() *ifrit.Client {
	return m.IfritClient
}

/* PUBLIC METHODS USED TO RESPOND TO HTTP CALLBACK METHODS */
// Sets a new permission to be applied to a specific node and a specific client
func (m *Masternode) SetSubjectNodePermission(nodeID, subjectID, permission string) error {
	// Verify the contents of the HTTP client's message
	if string(permission) != file.FILE_ALLOWED && string(permission) != file.FILE_DISALLOWED {
		return fmt.Errorf("Invalid permission constant: %s", permission)
	}

	// Find subject from message, if any. Return if no subject is found
	if subject := m.GetSubjectById(subjectID); subject == nil {
		return fmt.Errorf("No such subject known to the network: %s", subjectID)
	}

	node := m.GetNodeById(nodeID)
	if node == nil {
		return fmt.Errorf("No such node known to the network: %s", nodeID)
	} 

	msg := &messages.Message {
		MessageType: messages.MSG_TYPE_SET_SUBJECT_NODE_PERMISSION,
		Node: nodeID,
		Subject: subjectID,
		Permission: permission,
	}

	b, err := json.Marshal(msg)
	if err != nil {
		panic(err)
	}

	recipient := node.FireflyClient().Addr()
	<-m.FireflyClient().SendTo(recipient, b)
	return nil
}

// Sets a new permission to be applied globally to a specific node. Ie. all files maintained
// by this node are set to the new permission
func (m *Masternode) SetNodePermission(msg *messages.Clientmessage) error {
	// Verify the contents of the HTTP client's message
/*	if msg.Permission != file.FILE_ALLOWED && msg.Permission != file.FILE_DISALLOWED {
		return fmt.Errorf("Invalid permission constant: %s", msg.Permission)
	}

	// If no such node is found, return error
	var internalMsg *messages.InternalMessage
	node := m.GetNodeById(msg.Node)
	if node != nil {
		internalMsg = messages.NewInternalMessage(msg.Subject, node.ID(), messages.MSG_TYPE_SET_NODE_PERMISSION, msg.Permission)
	} else {
		return fmt.Errorf("No such node known to the network: %s", msg.Node)
	}

	encodedMsg := internalMsg.Encoded()
	recipient := node.FireflyClient().Addr()
	<-m.FireflyClient().SendTo(recipient, encodedMsg)
*/	return nil
}

// Returns a verbose string about a node
func (m *Masternode) GetNodeInfo(nodeID string) (string, error) {
	node := m.GetNodeById(nodeID)
	if node == nil {
		return "", errors.New("No such node in network");
	}

	dst := node.FireflyClient().Addr()
	msg := &messages.Message {
		MessageType: messages.MSG_TYPE_GET_NODE_INFO,
		Node: nodeID,
	}

	b, err := json.Marshal(msg)
	if err != nil {
		panic(err)
	}

	response := <-m.FireflyClient().SendTo(dst, b)
	return string(response), nil
}

// Gossip the new subject permission to the entire network
func (m *Masternode) SetSubjectPermission(msg *messages.Clientmessage) error {
	// Verify the contents of the HTTP client's message
	/*if msg.Permission != file.FILE_ALLOWED && msg.Permission != file.FILE_DISALLOWED {
		return fmt.Errorf("Invalid permission constant: %s", msg.Permission)
	}

	// If no such node is found, return error
	var internalMsg *messages.InternalMessage
	subject := m.GetSubjectById(msg.Subject)
	if subject != nil {
		internalMsg = messages.NewInternalMessage(msg.Subject, "", messages.MSG_TYPE_SET_SUBJECT_PERMISSION, msg.Permission)
	} else {
		return fmt.Errorf("No such subject known to the network: %s", msg.Subject)
	}

	encodedMsg := internalMsg.Encoded()
	m.FireflyClient().SetGossipContent(encodedMsg)
	*/return nil
}

func (m *Masternode) SendMessage(s *core.Subject, node *node.Node, permission string) {
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
	//m.IfritClien*node.Node {
	/*for _, n := range m.Nodes {
		if n.ID() == id {
			return n, nil
		}
	}*/
}

func (m *Masternode) Shutdown() {
	ifritClient := m.FireflyClient()
	log.Info("Master node shutting down...")
	ifritClient.Stop()
}

func (m *Masternode) GetNodeById(nodeID string) *node.Node {
	for _, node := range m.Nodes {
		if nodeID == node.ID() {
			return node
		}
	}
	return nil
}

func (m *Masternode) GetSubjectById(subjectID string) *core.Subject {
	for _, s := range m.Subjects {
		if subjectID == s.ID() {
			return s
		}
	}
	return nil
}

// Need only to pass strings 
func (m *Masternode) NodeCreateStudy(nodeName, study string) error {
	node := m.GetNodeById(nodeName)
	if node == nil {
		return fmt.Errorf("No such node known to network: %s", nodeName)
	}

	msg := &messages.Message {
		MessageType: messages.MSG_TYPE_NEW_STUDY,
		Node: nodeName,
		Study: study,
	}

	b, err := json.Marshal(msg)
	if err != nil {
		panic(err)
	}

	recipient := node.FireflyClient().Addr()
	<-m.FireflyClient().SendTo(recipient, b)
	return nil	
}

// Function primarily used by the user to assign files to nodes and to maintain a proper relation
// between files, subjects, nodes (and users)
func (m *Masternode) SetNodeFiles(msg *messages.AppStateMessage) error {
	subject := m.GetSubjectById(msg.Subject)
	node := m.GetNodeById(msg.Node)

	// Verify the message format
	if subject == nil || node == nil || msg.Study == "" {
		return errors.New("Invalid application state message format")
	}

	// Verify permissions
	if msg.Permission != file.FILE_ALLOWED && msg.Permission != file.FILE_DISALLOWED {
		return errors.New("Invalid application state permission")
	}

	if msg.NumFiles < 1 {
		msg.NumFiles = 1
	}

	if msg.FileSize < 10 {
		msg.FileSize = 10
	}

	appState := messages.NewAppStateMessage(messages.MSG_TYPE_SET_NODE_FILES, 
		msg.Node, 
		msg.Subject, 
		msg.Study,
		msg.Permission,
		msg.NumFiles,
		msg.FileSize)
	encodedAppState := appState.Encoded()
	recipient := node.FireflyClient().Addr()
	<-m.FireflyClient().SendTo(recipient, encodedAppState)
	return nil
}

func (m *Masternode) LoadNodeState(nodeID string) error {
	node := m.GetNodeById(nodeID)
	if node == nil {
		return fmt.Errorf("No such node known to network: %s\n", nodeID)
	}

	msg := &messages.Message {
		MessageType: messages.MSG_TYPE_LOAD_NODE,
		Node: nodeID,
	}

	b, err := json.Marshal(msg)
	if err != nil {
		panic(err)
	}

	recipient := node.FireflyClient().Addr()
	<-m.FireflyClient().SendTo(recipient, b)
	return nil
}