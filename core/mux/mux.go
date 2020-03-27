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
	//"firestore/core/node"
	"os/exec"
	"syscall"
	"firestore/netutil"
	"ifrit"
	"bytes"
	"io"
	"os"
	"bufio"
	"encoding/json"
_	"encoding/binary"
_	"io/ioutil"
)

var (
	errInvalidPermission = errors.New("Requested to send an invalid permission")
)

// In order for us to simulate a read-world scenario, we do not maintain
// any application-like data structures in the masternode. This is because the state of
// the permissions is supposed to be held by the storage nodes and NOT master. Otherwise,
// we could simply delegate the enitre...
type Mux struct {
	ifritClient *ifrit.Client
	nodeProcs 		map[string]*exec.Cmd
	nodes			map[string]string
	execPath	string

	// Port number the on which we can reach the application
	// Other HTTP-related stuff as well
	portNum 	int
	listener   	net.Listener
	httpServer 	*http.Server
}

func NewMux(portNum int, execPath string) (*Mux, error) {
	ifritClient, err := ifrit.NewClient()
	if err != nil {
		return nil, err
	}

	//ifritClient.RegisterGossipHandler(self.GossipMessageHandler)
	//ifritClient.RegisterResponseHandler(self.GossipResponseHandler)

	listener, err := netutil.ListenOnPort(portNum)
	if err != nil {
		panic(err)
	}


	log.Printf("Mux started at port %d\n", portNum)
	return &Mux{
		nodes:			make(map[string]string),
		portNum: 		portNum,
		execPath: 		execPath,
		ifritClient: 	ifritClient,
		listener: 		listener,
	}, nil
}

/** PUBLIC METHODS */
func (m *Mux) AddNetworkNodes(numNodes int) {
	
	m.nodeProcs = make(map[string]*exec.Cmd, 0)

	// Start the child processes aka. the internal Fireflies nodes
	for i := 0; i < numNodes; i++ {
		
		nodeName := fmt.Sprintf("node_%d", i)
		logfileName := fmt.Sprintf("%s_logfile", nodeName)
		nodeProc := exec.Command(m.execPath, "-name", nodeName, "-logfile", logfileName)
		
		m.nodeProcs[nodeName] = nodeProc
		nodeProc.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		go func() {
			if err := nodeProc.Start(); err != nil {
				panic(err)
			}

		/*	if err := nodeProc.Wait(); err != nil {
				panic(err)
			}*/
		}()
	}
}

func (m *Mux) Start() {
	log.Printf("Started node %s\n", m.ifritClient.Addr())
	go m.ifritClient.Start()
}

func (m *Mux) ShutdownNodes() {
	fmt.Printf("KILL: %v\n",m.nodeProcs )
	for _, cmd := range m.nodeProcs {
		if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM); err != nil {
			// might allow ourselves to fail silently here instead of panicing...
			panic(err)
		}
	}

	m.ifritClient.Stop()
}

func (m *Mux) HttpHandler() error {
	mux := http.NewServeMux()

	// Public methods exposed to data users (usually through cURL)
//	mux.HandleFunc("/kake", m.kake)

	// Utilities used in experiments
	mux.HandleFunc("/populate_node", m.PopulateNode)
	mux.HandleFunc("/network", m.Network)
	mux.HandleFunc("/debug", m.DebugNode)
	
	// Subject-related getters and setters
	mux.HandleFunc("/set_port", m.SetPortNumber)
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

func (m *Mux) Network(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if r.Method != http.MethodGet {
		http.Error(w, "Expected GET method", http.StatusMethodNotAllowed)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Flireflies nodes in this network:\nMux: %s\n", m.ifritClient.Addr())
	for n, addr := range m.nodes {
		fmt.Fprintf(w, "String identifier: %s\tIP address: %s\n", n, addr)
	}
}

func (m *Mux) PopulateNode(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if r.Method != http.MethodPost {
		http.Error(w, "Expected POST method", http.StatusMethodNotAllowed)
		return
	}

	if r.Header.Get("Content-type") != "application/json" {
		http.Error(w, "Require header to be application/json", http.StatusUnprocessableEntity)
		return
	}

	var msg struct {
		Node 	string 	`json:"node"`
		Subject string 	`json:"subject"`
		StudyID string 	`json:"study"`
	}

	var body bytes.Buffer
	io.Copy(&body, r.Body)
	err := json.Unmarshal(body.Bytes(), &msg)
	if err != nil {
		panic(err)
	}

	if node, exists := m.nodes[msg.Node]; exists {
		fmt.Printf("Sending msg to %s\n", node)
		ch := m.ifritClient.SendTo(node, []byte("kake"))
		select {
			case msg := <- ch: 
				fmt.Printf("Response: %s\n", msg)
			}

		//w.WriteHeader(http.StatusOK)
		//str := fmt.Sprintf("Populated node %s\n", msg.Node)
		/fmt.Fprintf(w, "%s", str)
	} else {
		/*w.WriteHeader(http.StatusNotFound)
		str := fmt.Sprintf("No such node: %s\n", msg.Node)
		fmt.Fprintf(w, "%s", str)*
	}*/
}

func (m *Mux) DebugNode(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if r.Method != http.MethodPost {
		http.Error(w, "Expected POST method", http.StatusMethodNotAllowed)
		return
	}

	if r.Header.Get("Content-type") != "application/json" {
		http.Error(w, "Require header to be application/json", http.StatusUnprocessableEntity)
		return
	}

	var msg struct {
		Node string 	`json:"node"`
	}

	var body bytes.Buffer
	io.Copy(&body, r.Body)
	err := json.Unmarshal(body.Bytes(), &msg)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Sending msg to %s\n", msg.Node)

	ch := m.ifritClient.SendTo(msg.Node, []byte("kake"))
	select {
		case msg := <- ch: 
			fmt.Printf("Response: %s\n", msg)
	}

	w.WriteHeader(http.StatusOK)
	str := fmt.Sprintf("Populated node %s\n", msg.Node)
	fmt.Fprintf(w, "%s", str)
}

func (m *Mux) SetPortNumber(w http.ResponseWriter, r *http.Request) {
	fmt.Printf("kake\n")
	defer r.Body.Close()
	if r.Method != http.MethodPost {
		http.Error(w, "Expected POST method", http.StatusMethodNotAllowed)
		return
	}

	if r.Header.Get("Content-type") != "application/json" {
		http.Error(w, "Require header to be application/json", http.StatusUnprocessableEntity)
	}

	var msg struct {
		Node 	string 		`json:"node"`
		Address string 		`json:"address"`
	}

	decoder := json.NewDecoder(r.Body)
    err := decoder.Decode(&msg)
    if err != nil {
		errMsg := fmt.Sprintf("Error: %s\n", err)
		http.Error(w, errMsg, http.StatusBadRequest)
	}

	if _, ok := m.nodeProcs[msg.Node]; ok {
		m.nodes[msg.Node] = msg.Address
	} else {
		errMsg := fmt.Sprintf("No such node: %s\n", msg.Node)
		http.Error(w, errMsg, http.StatusNotFound)
	}
	w.WriteHeader(http.StatusOK)
}

func (m *Mux) getNetworkPorts(path string) ([]string, error) {
    file, err := os.Open(path)
    if err != nil {
        return nil, err
    }
    defer file.Close()

    var lines []string
    scanner := bufio.NewScanner(file)
    for scanner.Scan() {
		lines = append(lines, scanner.Text())
		fmt.Printf("Line: %s\n", scanner.Text())
    }
    return lines, scanner.Err()
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