package mux

import (
	"fmt"
	logging "github.com/inconshreveable/log15"
	//	"github.com/spf13/viper"
	"errors"
	"net"
	"net/http"
	"log"
	//"firestore/core/file"
	//"firestore/core/message"
	"firestore/core/node"
	"firestore/netutil"
	"ifrit"
//	"encoding/json"
)

var (
	errInvalidPermission = errors.New("Requested to send an invalid permission")
)

// In order for us to simulate a read-world scenario, we do not maintain
// any application-like data structures in the masternode. This is because the state of
// the permissions is supposed to be held by the storage nodes and NOT master. Otherwise,
// we could simply delegate the enitre...
type Mux struct {
	IfritClient *ifrit.Client
	nodes       map[string]*node.Node

	// Port number the on which we can reach the application
	// Other HTTP-related stuff as well
	portNum int
	listener   net.Listener
	httpServer *http.Server
}

func NewMux(portNum int) (*Mux, error) {
	ifritClient, err := ifrit.NewClient()
	if err != nil {
		return nil, err
	}

	go ifritClient.Start()
	//ifritClient.RegisterGossipHandler(self.GossipMessageHandler)
	//ifritClient.RegisterResponseHandler(self.GossipResponseHandler)

	listener, err := netutil.ListenOnPort(portNum)
	if err != nil {
		panic(err)
	}

	log.Printf("Mux started at port %d\n", portNum)
	return &Mux{
		portNum: 		portNum,
		IfritClient: 	ifritClient,
		listener: 		listener,
	}, nil
}

/** PUBLIC METHODS */
func (m *Mux) AddNetworkNodes(numNodes int) {
	m.nodes = make(map[string]*node.Node, 0)
	for i := 0; i < numNodes; i++ {
		nodeName := fmt.Sprintf("node_%d", i)
		node := node.NewNode(nodeName)
		m.nodes[nodeName] = node
		//node.Start()
	}
}

func (m *Mux) HttpHandler() error {
	mux := http.NewServeMux()

	// Public methods exposed to data users (usually through cURL)
//	mux.HandleFunc("/kake", m.kake)

	// Utilities used in experiments
	mux.HandleFunc("/create_study", m.CreateStudy)
	//	mux.HandleFunc("/subjects", n.Subjects)
	//mux.HandleFunc("/network", )
	
	// Subject-related getters and setters
	//mux.HandleFunc("/create_subject", )
	//mux.HandleFunc("/delete_subject", )
	//mux.HandleFunc("/move_subject", )
	
	m.httpServer = &http.Server{
		Handler: mux,
	}

	err := m.httpServer.Serve(m.listener)
	if err != nil {
		logging.Error(err.Error())
		return err
	}
	return nil
}

// Used primarily for 
func (m *Mux) CreateStudy(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if r.Method != http.MethodPost {
		http.Error(w, "Expected POST method", http.StatusMethodNotAllowed)
		return
	}

	/*
	if r.Header.Get("Content-type") != "application/json" {
		logger.Error("Require header to be application/json")
	}

	var msg struct {
		Node string `json:"node"`
		Subject string `json:"subject"`
		Permission string `json:"permission"`
	}

	var body bytes.Buffer
	io.Copy(&body, r.Body)
	err := json.Unmarshal(body.Bytes(), &msg)
	if err != nil {
		panic(err)
	}


	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		str := fmt.Sprintf("%s\n", err)
		fmt.Fprintf(w, "%s", str)
	} else {
		w.WriteHeader(http.StatusOK)
		str := fmt.Sprintf("%s", subjects)
		fmt.Fprintf(w, "%s", str)
	}*/
}


/*func (m *Masternode) SetNetworkSubjects(subjects []*core.Subject) {
	m.Subjects = subjects
}

func (m *Masternode) FireflyClient() *ifrit.Client {
	return m.IfritClient
}*/

/* PUBLIC METHODS USED TO RESPOND TO HTTP CALLBACK METHODS *
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
/	return nil
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
	/return nil
}

func (m *Masternode) SendMessage(s *core.Subject, node *node.Node, permission string) {
	/*dst := node.FireflyClient().Addr()
	msg := message.NewMessage(s.Name(), message.PERMISSION_GET, permission)
	encodedMsg := msg.Encoded()
	<- m.FireflyClient().SendTo(dst, encodedMsg)*
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
	}*

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
}*/