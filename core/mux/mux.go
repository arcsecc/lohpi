package mux

import (
	"fmt"
	logging "github.com/inconshreveable/log15"
	//	"github.com/spf13/viper"
	"errors"
	"net"
	"net/http"
	"crypto/tls"
_	"log"
	//"firestore/core/file"
	//"firestore/core/message"
	"firestore/comm"
	"os/exec"
	"syscall"
	"firestore/netutil"
	"ifrit"
_	"bytes"
_	"io"
	"os"
	"bufio"
_	"encoding/json"
_	"encoding/binary"
_	"io/ioutil"

	//"crypto/x509"
	"crypto/x509/pkix"
	//"crypto/ecdsa"

	"github.com/spf13/viper"
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
	subjects 		map[string][]string		// subjectID -> {node_1, node_2, ...}
	execPath	string

	// HTTPS-related
	portNum 	int
	listener   	net.Listener
	httpServer 	*http.Server
	serverConfig *tls.Config

	// HTTP-related stuff. Used by the demonstrator using cURL
	_httpPortNum 	int
	_httpListener   net.Listener
	_httpServer 	*http.Server
}

// Consider using 
func NewMux(portNum, _portNum int, execPath string) (*Mux, error) {
	if err := readConfig(); err != nil {
		panic(err)
	}

	ifritClient, err := ifrit.NewClient()
	if err != nil {
		return nil, err
	}

	//ifritClient.RegisterGossipHandler(self.GossipMessageHandler)
	//ifritClient.RegisterResponseHandler(self.GossipResponseHandler)

	// Initiate HTTPS connection using TLS configuration
	listener, err := netutil.ListenOnPort(portNum)
	if err != nil {
		panic(err)
	}

	logging.Debug("addrs", "rpc", listener.Addr().String(), "udp")

	pk := pkix.Name{
		Locality: []string{listener.Addr().String()},
	}

	cu, err := comm.NewCu(pk, viper.GetString("lohpi_ca_addr"))
	if err != nil {
		return nil, err
	}

	// Used for HTTPS purposes
	serverConfig := comm.ServerConfig(cu.Certificate(), cu.CaCertificate(), cu.Priv())

	// Initiate HTTP connection without TLS. Used by demonstrator
	_httpListener, err := netutil.ListenOnPort(_portNum)
	if err != nil {
		panic(err)
	}

	return &Mux{
		ifritClient: 	ifritClient,
		nodes:			make(map[string]string),
		execPath: 		execPath,

		// HTTPS
		portNum: 		portNum,
		listener: 		listener,
		serverConfig: 	serverConfig,

		// HTTP
		_httpPortNum: 	_portNum,
		_httpListener:  _httpListener,
	}, nil
}

func (m *Mux) Start() {
	go m.ifritClient.Start()
}

func (m *Mux) RunServers() {
	go m.HttpHandler()
	go m.HttpsHandler()
}

/** PUBLIC METHODS */
func (m *Mux) AddNetworkNodes(numNodes int) {
	m.nodeProcs = make(map[string]*exec.Cmd)
	m.nodes 	= make(map[string]string)
	
	// Start the child processes aka. the internal Fireflies nodes
	for i := 0; i < numNodes; i++ {
		
		nodeName := fmt.Sprintf("node_%d", i)
		fmt.Printf("Adding %s\n", nodeName)
		logfileName := fmt.Sprintf("%s_logfile", nodeName)
		nodeProc := exec.Command(m.execPath, "-name", nodeName, "-logfile", logfileName)
		
		// Process tree map
		m.nodeProcs[nodeName] = nodeProc
		nodeProc.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

		/*go func() {
			if err := nodeProc.Start(); err != nil {
				panic(err)
			}

			if err := nodeProc.Wait(); err != nil {
				panic(err)
			}
		}()*/
	}
}

func (m *Mux) Stop() {
	fmt.Printf("KILL: %v\n",m.nodeProcs )
	for _, cmd := range m.nodeProcs {
		if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM); err != nil {
			// might allow ourselves to fail silently here instead of panicing...
			panic(err)
		}
	}

	// Stop the underlying Ifrit client
	m.ifritClient.Stop()
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

func readConfig() error {
	viper.SetConfigName("firestore_config")
	viper.AddConfigPath("/var/tmp")
	viper.AddConfigPath(".")
	viper.SetConfigType("yaml")

	err := viper.ReadInConfig()
	if err != nil {
		return err
	}

	// Behavior variables
	viper.SetDefault("num_subjects", 2)
	viper.SetDefault("num_studies", 10)
	viper.SetDefault("data_users", 1)
	viper.SetDefault("files_per_study", 2)
	viper.SetDefault("file_size", 256)
	viper.SetDefault("fuse_mount", "/home/thomas/go/src/firestore")
	viper.SetDefault("set_files", true)
	viper.SetDefault("lohpi_ca_addr", "127.0.1.1:8301")
	viper.SafeWriteConfig()
	return nil
}

/*
func (n *Node) MuxMessageReceiver(data []byte) ([]byte, error) {
	var response string
	msg := messages.NewMessage(data)
	fuseDaemon := n.fuseDaemon()
	fmt.Printf("Message received: %s\n", msg)

	switch msgType := msg.MessageType; msgType {
		case messages.MSG_TYPE_GET_NODE_INFO:
			response = n.string()

		case messages.MSG_TYPE_SET_SUBJECT_NODE_PERMISSION:
			fuseDaemon.SetSubjectNodePermission(msg.Subject, msg.Permission)

		case messages.MSG_TYPE_SET_NODE_PERMISSION:
			log.Fatalf("Not implemented. Exiting...\n")

		case messages.MSG_TYPE_SET_NODE_FILES:
			log.Fatalf("Not implemented. Exiting...\n")

		case messages.MSG_TYPE_NEW_STUDY:
			err := n.createNewStudyDirectory(msg.Study)
			if err != nil {
				return nil, err
			}
		
		case messages.MSG_TYPE_LOAD_NODE:
//			n.LoadNodeState()

		default:
			log.Printf("Unkown message type: %s\n", msg.MessageType)
			return nil, nil
	}
    return []byte(response), nil
}*/

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