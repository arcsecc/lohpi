package main

import (
	"fmt"
	///"github.com/joonnna/ifrit"
	//	"ifrit"
	_ "bytes"
	_ "encoding/gob"
	"errors"
	logger "github.com/inconshreveable/log15"
	_ "math/rand"
	//"time"
	"bytes"
	"encoding/json"
	"firestore/core"
	"firestore/core/file"
	"firestore/core/messages"
	"firestore/netutil"
	"firestore/node"
	"github.com/spf13/viper"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
//	"log"
)

var (
	errCannotCreateApplication = errors.New("Cannot create application")
	errCannotAddClients        = errors.New("Adding one or more clients to Application.Clients failed")
)

type Application struct {
	// Data owners
	Subjects []*core.Subject

	// Data storage units
	Nodes []*node.Node

	// Single point-of-entry node for API calls. Stateless too
	MasterNode *Masternode

	// Data users requesting data
	DataUsers []*core.Datauser

	// All files. Only used to print file permissions
	Files []*file.File

	// Http server stuff
	Listener   net.Listener
	httpServer *http.Server

	// Pass exit signals
	ExitChan chan bool
}

// Used to populate the 
type AppStateMessage struct {
	Node string
	Subject string
	Study string
	NumFiles int
	FileSize int
}

func main() {
	// Main entry point. Read configuration to set initial parameters
	readConfig()

	app, err := NewApplication()
	if err != nil {
		panic(err)
		logger.Error("Could not create several clients")
	}

	setupApplicationKiller(app)
	app.Start()
	//app.Run()
	//app.Stop()
}

func NewApplication() (*Application, error) {
	app := &Application{}

	l, err := netutil.ListenOnPort(viper.GetInt("server_port"))
	if err != nil {
		return nil, err
	}

	//numFiles := viper.GetInt("files_per_subject")
	numNodes := viper.GetInt("firestore_nodes")
	fmt.Printf("Port num: %d\n", viper.GetInt("server_port"))
	//numDataUsers := viper.GetInt("data_users")
	numSubjects := viper.GetInt("num_subjects")
	masterNode, err := NewMasterNode()
	app.MasterNode = masterNode
	if err != nil {
		panic(err)
	}

	nodes, err := CreateNodes(numNodes)
	app.Nodes = nodes
	if err != nil {
		panic(err)
	}

	app.Subjects = CreateApplicationSubject(numSubjects)
	//app.DataUsers = CreateDataUsers(numDataUsers)
	app.MasterNode.SetNetworkNodes(nodes)
	app.MasterNode.SetNetworkSubjects(app.Subjects)

	//app.assignSubjectFilesToStorageNodes(app.StorageNodes, app.Subjects, numFiles)
	app.Listener = l
	return app, nil
}

// Main entry point for running the application
func (app *Application) Run() {
	//app.PrintFilePermissions()
	//app.TestGossiping()
	//app.TestMulticast()
	//app.waitForPropagation()
	//app.PrintFilePermissions()
}

func (app *Application) Start() error {
	logger.Info("Started application server", "addr", app.Listener.Addr().String())
	return app.httpHandler()
}

/**** HTTP end-point functions */
func (app *Application) httpHandler() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/print_files", app.PrintFiles)
	mux.HandleFunc("/print_node", app.PrintNode)
	mux.HandleFunc("/subject_node_set_perm", app.SubjectNodeSetPermission)
	mux.HandleFunc("/subject_set_perm", app.SubjectSetPermission)
	mux.HandleFunc("/node_set_perm", app.NodeSetPermission)
	mux.HandleFunc("/subjects", app.PrintSubjects)
	mux.HandleFunc("/shutdown", app.Shutdown)
	mux.HandleFunc("/set_system_state", app.SetApplicationState)
	//mux.HandleFunc()
	/*user_set_perm
	mux.HandleFunc("/shutdown", app.Shutdown)
	mux.HandleFunc("/subjects", app.PrintSubjects)*/
	/*mux.HandleFunc("/analysers", app.Shutdown)*/

	app.httpServer = &http.Server{
		Handler: mux,
	}

	err := app.httpServer.Serve(app.Listener)
	if err != nil {
		logger.Error(err.Error())
		return err
	}
	return nil
}

func (app *Application) PrintNode(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if r.Method != http.MethodPost {
		msg := "Requires GET method"
		http.Error(w, msg, http.StatusMethodNotAllowed)
		return
	}

	if r.Header.Get("Content-type") != "application/json" {
		http.Error(w, "Require header to be application/json", http.StatusUnprocessableEntity)
		return
	}

	var incomingMsg messages.Clientmessage
	var body bytes.Buffer
	io.Copy(&body, r.Body)

	err := json.Unmarshal(body.Bytes(), &incomingMsg)
	if err != nil {
		panic(err)
	}

	node := app.MasterNode.GetNodeById(incomingMsg.Node)
	if node == nil {
		w.WriteHeader(http.StatusNotFound)
		str := fmt.Sprintf("No such node in network: %s\n", incomingMsg.Node)
		fmt.Fprintf(w, "%s", str)
		return
	}

	nodeInfo := app.MasterNode.GetNodeInfo(node)
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "%s", nodeInfo)
}

func (app *Application) PrintFiles(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
	}

	w.WriteHeader(http.StatusOK)
	if len(app.Files) < 1 {
		str := fmt.Sprintf("There are no files in the storage network\n")
		fmt.Fprintf(w, "%s", str)
		return
	}

	str := string(fmt.Sprintf("**** All known files in the system ****\n\n"))
	fmt.Fprintf(w, "%s", str)

	for _, file := range app.Files {
		str := string(fmt.Sprintf("Path: %s\nOwner: %s\nStorage node: %s\nPermission: %s\n\n\n", file.AbsolutePath, file.SubjectID, file.OwnerID, file.FilePermission()))
		fmt.Fprintf(w, "%s", str)
	}
}

func (app *Application) SubjectNodeSetPermission(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if r.Method != "POST" {
		respString := fmt.Sprintf("Invalid method: %s for /subject_set_perm\n", r.Method)
		_, err := w.Write([]byte(respString))
		if err != nil {
			logger.Error(err.Error())
			return
		}
	}

	if r.Header.Get("Content-type") != "application/json" {
		logger.Error("Require header to be application/json")
	}

	var body bytes.Buffer
	io.Copy(&body, r.Body)

	var incomingMsg messages.Clientmessage
	err := json.Unmarshal(body.Bytes(), &incomingMsg)
	if err != nil {
		panic(err)
	}

	err = app.MasterNode.SetSubjectNodePermission(&incomingMsg)
	if err != nil {
		w.WriteHeader(http.StatusNotAcceptable)
		str := fmt.Sprintf("%s\n", err)
		fmt.Fprintf(w, "%s", str)
	} else {
		w.WriteHeader(http.StatusOK)
	}
}

func (app *Application) SubjectSetPermission(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if r.Method != "POST" {
		respString := fmt.Sprintf("Invalid method: %s for /subject_set_perm\n", r.Method)
		_, err := w.Write([]byte(respString))
		if err != nil {
			logger.Error(err.Error())
			return
		}
	}

	if r.Header.Get("Content-type") != "application/json" {
		logger.Error("Require header to be application/json")
	}

	var body bytes.Buffer
	io.Copy(&body, r.Body)

	var incomingMsg messages.Clientmessage
	err := json.Unmarshal(body.Bytes(), &incomingMsg)
	if err != nil {
		panic(err)
	}

	err = app.MasterNode.SetSubjectPermission(&incomingMsg)
	if err != nil {
		w.WriteHeader(http.StatusNotAcceptable)
		str := fmt.Sprintf("%s\n", err)
		fmt.Fprintf(w, "%s", str)
	} else {
		w.WriteHeader(http.StatusOK)
	}
}

func (app *Application) NodeSetPermission(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	if r.Method != "POST" {
		respString := fmt.Sprintf("Invalid method: %s for /node_set_perm\n", r.Method)
		_, err := w.Write([]byte(respString))
		if err != nil {
			logger.Error(err.Error())
			return
		}
	}

	if r.Header.Get("Content-type") != "application/json" {
		logger.Error("Require header to be application/json")
	}

	var message messages.Clientmessage
	var body bytes.Buffer
	io.Copy(&body, r.Body)
	err := json.Unmarshal(body.Bytes(), &message)
	if err != nil {
		panic(err)
	}

	err = app.MasterNode.SetNodePermission(&message)
	if err != nil {
		w.WriteHeader(http.StatusNotAcceptable)
		str := fmt.Sprintf("%s\n", err)
		fmt.Fprintf(w, "%s", str)
	} else {
		w.WriteHeader(http.StatusOK)
	}
}

func (app *Application) Shutdown(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	app.Cleanup()
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "System shutdown OK\n")
}

func (app *Application) PrintSubjects(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	w.WriteHeader(http.StatusOK)
	for _, subject := range app.Subjects {
		str := fmt.Sprintf("Subject's name: %s\n", subject.ID())
		fmt.Fprintf(w, "%s", str)
		for _, file := range subject.Files() {
			if subject.ID() == file.SubjectID {
				str := string(fmt.Sprintf("Subject's name: %s\nOwner name: %s\nStorage network path: %s\n\n", subject.ID(), file.OwnerID, file.AbsolutePath))
				fmt.Fprintf(w, "%s", str)
			}
		}
	}
}

// Set the entire application state to using a JSON file passed in the POST request
func (app *Application) SetApplicationState(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	if r.Method != http.MethodGet {
		http.Error(w, "Require POST method", http.StatusMethodNotAllowed)
		return
	}

	if r.Header.Get("Content-type") != "application/json" {
		http.Error(w, "Require header to be application/json", http.StatusUnprocessableEntity)
		return
	}

	var message AppStateMessage
	var body bytes.Buffer
	io.Copy(&body, r.Body)
	err := json.Unmarshal(body.Bytes(), &message)
	if err != nil {
		panic(err)
	}

	if !message.isValidMessage() {
		http.Error(w, "Require header to be application/json", http.StatusUnprocessableEntity)
		return
	}
}

func (msg *AppStateMessage) isValidMessage() bool {
	return true
}

/*func (app *Application) assignSubjectFilesToStorageNodes(nodes []*core.Node, subjects []*core.Subject, numFiles int) {
	fileSize := viper.GetInt("file_size")
	app.Files = make([]*file.File, 0)

	for _, subject := range subjects {
		for i := 0; i < numFiles; i++ {
			for _, node := range nodes {


				path := fmt.Sprintf("%s/%s/file%d.txt", node.StorageDirectory(), subject.Name(), i)
				file, err := file.NewFile(fileSize, path, subject.Name(), node.Name(), file.FILE_ALLOWED)
				if err != nil {
					panic(err)
				}
				node.AppendSubjectFile(file)
				app.Files = append(app.Files, file)
			}
		}
	}
}*/

func CreateApplicationSubject(numSubjects int) []*core.Subject {
	subjects := make([]*core.Subject, 0)

	for i := 0; i < numSubjects; i++ {
		subjectName := fmt.Sprintf("subject_%d", i+1)
		subjectDirPath := fmt.Sprintf("/tmp/subject_data/subject_%d", i+1)
		subject := core.NewSubject(subjectName, subjectDirPath)
		subjects = append(subjects, subject)
	}
	return subjects
}

func CreateDataUsers(numDataUsers int) []*core.Datauser {
	dataUsers := make([]*core.Datauser, 0)
	for i := 0; i < numDataUsers; i++ {
		id := fmt.Sprintf("dataUser_%d\n", i+1)
		user := core.NewDataUser(id)
		dataUsers = append(dataUsers, user)
	}
	return dataUsers
}

func CreateNodes(numStorageNodes int) ([]*node.Node, error) {
	nodes := make([]*node.Node, 0)
	for i := 0; i < numStorageNodes; i++ {
		nodeID := fmt.Sprintf("node_%d", i+1)
		node, err := node.NewNode(nodeID)
		if err != nil {
			logger.Error("Could not create storage node")
			return nil, err
		}
		nodes = append(nodes, node)
	}
	return nodes, nil
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
	viper.SetDefault("firestore_nodes", 1)
	viper.SetDefault("num_subjects", 2)
	viper.SetDefault("data_users", 1)
	viper.SetDefault("files_per_subject", 1)
	viper.SetDefault("file_size", 256)
	viper.SetDefault("server_port", 8080)
	viper.SafeWriteConfig()
	return nil
}

func (app *Application) Cleanup() {
	for _, node := range app.Nodes {
		node.Shutdown()
	}
	app.MasterNode.Shutdown()
}

func setupApplicationKiller(app *Application) {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		app.Cleanup()
		os.Exit(0)
	}()
}

/*func (app *Application) SendData(client *Client, payload *Clientdata) chan []byte {
	// Get needed application clients
	ifritClient := client.FirefzlyClient()
	masterIfritClient := app.MasterClient.FireflyClient()

	// Set payload properties prior to sending it
	payload.SetEpochTimestamp(time.Now().UnixNano())
	payload.SetInTransit(true)

	var b bytes.Buffer
	e := gob.NewEncoder(&b)
	if err := e.Encode(payload); err != nil {
	   panic(err)
	}
	//fmt.Println("Encoded Struct ", b)

	return masterIfritClient.SendTo(ifritClient.Addr(), b.Bytes())
}*/

/** Gossip messaging here */
/*
func (app *Application) RunGossipMessaging() {
	for {
		select {

			case <- time.After(time.Second * 0):
				//randomClient := app.Clients[rand.Int() % len(app.Clients)]
				s := fmt.Sprintf("Client %s sends message\n", app.Clients[0].Addr())
				app.Clients[0].SetGossipContent([]byte(s))

				//ch := ap'p.Clients[0].SendTo(randomClient.Addr(), []byte(s))
				//response := <- ch
		}
	}
}*/
/*	StorageNodes []*core.Node

	// Single point-of-entry node for API calls. Stateless too
	MasterNode *Masternode
func (app *Application) gossipMessageHandler(data []byte) ([]byte, error) {
	fmt.Printf("In simpleGossipHandler() -- message: %s\n", string(data))
	return data, nil
}*/
/*
func (app *Application) gossipResponseHandler(data []byte) {
	fmt.Printf("In gossipResponseHandler() -- message: %s\n", string(data))
}
*/

/****** simple message passing here ******/
/*
func (app *Application) simpleMessageHandler(data []byte) ([]byte, error) {
	fmt.Printf("Message: %s\n", string(data));
	return data, nil
}

func (app *Application) RunSimpleMessaging(nClients int) {
	fmt.Printf("This node's address = %s\n", app.Clients[0].Addr());

	for {
		select {
			case <- app.ExitChan:
				return

			case <- time.After(time.Second * 1):

				//fmt.Printf("Sending 8 messages...\n")
				for i := 1; i < nClients; i++ {
					//idx := rand.Int() % len(app.Clients)
					recipient := app.Clients[i]
					//fmt.Printf("Sends message to %s\n", randomClient.Addr());
					s := fmt.Sprintf("recipient: %s", recipient.Addr())
					ch := app.Clients[0].SendTo(recipient.Addr(), []byte(s))

					response := <- ch
					fmt.Printf("Response: %s\n", response)
				}
				//fmt.Println()
		}
	}
}


*/
