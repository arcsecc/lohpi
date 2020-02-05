package main

import (
	"fmt"
	///"github.com/joonnna/ifrit"
//	"ifrit"
	log "github.com/inconshreveable/log15"
_	"math/rand"
	"errors"
_	"encoding/gob"
_	"bytes"
	"time"
	"firestore/core/file"
	"firestore/core"
	"firestore/core/messages"
//	"firestore/utils"
	"firestore/netutil"
	"github.com/spf13/viper"
	"net"
	"net/http"
	"bytes"
	"io"
_	"io/ioutil"
	"encoding/json"

)

var (
	errCannotCreateApplication 		= errors.New("Cannot create application")
	errCannotAddClients 			= errors.New("Adding one or more clients to Application.Clients failed")
)

type Application struct {
	// Data owners
	Subject *core.Subject
	
	// Data storage units
	StorageNodes []*core.Node
	
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

// used by /print_node endpoint
type Msg struct {
	Node string
	Subject string
	Permission string
	SetPermission string
}

func main() {
	// Main entry point. Read configuration to set initial parameters
	readConfig()

	app, err := NewApplication()
	if err != nil {
		panic(err)
		log.Error("Could not create several clients")
	}

	app.Start()
	//app.Run()
	//app.Stop()
}


func NewApplication() (*Application, error) {
	app := &Application {}

	l, err := netutil.ListenOnPort(viper.GetInt("server_port"))
	if err != nil {
		return nil, err
	}

	numFiles := viper.GetInt("files_per_subject")
	numStorageNodes := viper.GetInt("firestore_nodes")
	numDataUsers := viper.GetInt("data_users")
	masterNode, err := NewMasterNode()
	if err != nil {
		panic(err)
	}
	
	storageNodes, err := CreateStorageNodes(numStorageNodes)
	if err != nil {
		panic(err)
	}

	app.StorageNodes = storageNodes
	app.MasterNode = masterNode
	app.DataUsers = CreateDataUsers(numDataUsers)
	app.Subject = CreateApplicationSubject()
	app.assignSubjectFilesToStorageNodes(app.StorageNodes, app.Subject, numFiles)
	app.Listener = l
	time.After(5)
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
	log.Info("Started application server", "addr", app.Listener.Addr().String())
	return app.httpHandler()
}
 
func (app *Application) httpHandler() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/print_files", app.PrintFiles)
	mux.HandleFunc("/print_node", app.PrintNode)
	mux.HandleFunc("/subject_set_perm", app.SubjectSetPermission)
	mux.HandleFunc("/node_set_perm", app.NodeSetPermission)
	/*user_set_perm
	mux.HandleFunc("/shutdown", app.Shutdown)
	mux.HandleFunc("/subjects", app.PrintSubjects)*/
	/*mux.HandleFunc("/analysers", app.Shutdown)*/

	app.httpServer = &http.Server{
		Handler: mux,
	}

	err := app.httpServer.Serve(app.Listener)
	if err != nil {
		log.Error(err.Error())
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
		log.Error("Require header to be application/json")
	}

	var incomingMsg messages.Clientmessage
	var body bytes.Buffer
	io.Copy(&body, r.Body)

	err := json.Unmarshal(body.Bytes(), &incomingMsg)
	if err != nil {
		panic(err)
	}

	for _, n := range app.StorageNodes {
		if incomingMsg.Node == n.Name() {
			str := n.NodeInfo()
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, "%s", str) 
			return
		}
	}

	w.WriteHeader(http.StatusNotFound)
	log.Error("Node not found")
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
	}

	str := string(fmt.Sprintf("**** All known files in the system ****\n\n"))
	fmt.Fprintf(w, "%s", str) 

	for _, file := range app.Files {
		str := string(fmt.Sprintf("Path: %s\nOwner: %s\nStorage node: %s\nPermission: %s\n\n\n", file.AbsolutePath, file.SubjectID, file.OwnerID, file.FilePermission()))
		fmt.Fprintf(w, "%s", str) 
	}
}

// TODO: Handle multiple subjects too
func (app *Application) SubjectSetPermission(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if r.Method != "POST" {
		respString := fmt.Sprintf("Invalid method: %s for /subject_set_perm\n", r.Method)
		_, err := w.Write([]byte(respString))
		if err != nil {
			log.Error(err.Error())
			return
		}
	}

	defer r.Body.Close()
	if r.Header.Get("Content-type") != "application/json" {
		log.Error("Require header to be application/json")
	}

	var body bytes.Buffer
	io.Copy(&body, r.Body)

	var incomingMsg messages.Clientmessage
	err := json.Unmarshal(body.Bytes(), &incomingMsg)
	if err != nil {
		panic(err)
	}

	// change this when adding several subjects
	err = app.MasterNode.GossipSubjectPermission(&incomingMsg); 
	if err != nil {
		w.WriteHeader(http.StatusNotAcceptable)
		str := fmt.Sprintf("Invalid request body\n")
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
			log.Error(err.Error())
			return
		}
	}

	if r.Header.Get("Content-type") != "application/json" {
		log.Error("Require header to be application/json")
	}

	var message messages.Clientmessage
	var body bytes.Buffer
	io.Copy(&body, r.Body)
	err := json.Unmarshal(body.Bytes(), &message)
	if err != nil {
		panic(err)
	}

	for _, node := range app.StorageNodes {
		if node.Name() == message.Node {
			fmt.Printf("found node\n");
			app.MasterNode.UpdateNodePermission(node, &message)
			w.WriteHeader(http.StatusOK)
			return
		}
	}

	w.WriteHeader(http.StatusNotFound)
	str := fmt.Sprintf("Could not find node %s\n", message.Node)
	fmt.Fprintf(w, "%s", str)
}

func (app *Application) Shutdown(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	for _, node := range app.StorageNodes {
		node.IfritClient().Stop()
	}
	r.Body.Close()
	app.MasterNode.FireflyClient().Stop()
	app.Listener.Close()
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "System shutdown OK\n") 
}

// TODO: Handle multiple subjects too
func (app *Application) PrintSubjects(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
	}

	w.WriteHeader(http.StatusOK)

	for _, file := range app.Files {
		if (app.Subject.Name() == file.SubjectID) {
			str := string(fmt.Sprintf("Subject's name: %s\nOwner name: %s\nStorage network path: %s\n\n", app.Subject.Name(), file.OwnerID, file.AbsolutePath))
			fmt.Fprintf(w, "%s", str) 	
		}
	}
}

func (app *Application) TestMulticast() {
//	nodes := app.StorageNodes[:len(app.StorageNodes) / 2]
//	for _, node := range nodes {
//		app.MasterNode.SendStoragePermission(app.Subject, node, file.FILE_APPEND)
//	}
}

func (app *Application) TestGossiping() {
	//app.MasterNode.GossipStorageNetwork(app.Subject, file.FILE_APPEND)
}

func (app *Application) assignSubjectFilesToStorageNodes(nodes []*core.Node, subject *core.Subject, numFiles int) {
	fileSize := viper.GetInt("file_size")
	app.Files = make([]*file.File, 0)

	// might add more subjects too...
	for i := 0; i < numFiles; i++ {
		for _, node := range nodes {
			path := fmt.Sprintf("%s/%s/file%d.txt", node.StorageDirectory(), subject.Name(), i)
			file, err := file.NewFile(fileSize, path, subject.Name(), node.Name(), file.FILE_READ)
			if err != nil {
				panic(err)
			}
			node.AppendSubjectFile(file)
			app.Files = append(app.Files, file)
		}
	}
}

func CreateApplicationSubject() *core.Subject {
	subjectName := fmt.Sprintf("subject%d", 1)
	return core.NewSubject(subjectName)
	/*subjects := make([]*core.Subject, 0)

	for i := 0; i < numSubjects; i++ {
		subjectName := fmt.Sprintf("subject_%d", i + 1)
		subject := core.NewSubject(subjectName)
		subjects = append(subjects, subject)
	}
	return subjects*/
}

func CreateDataUsers(numDataUsers int) ([]*core.Datauser) {
	dataUsers := make([]*core.Datauser, 0)

	for i := 0; i < numDataUsers; i++ {
		id := fmt.Sprintf("dataUser_%d\n", i + 1)
		user := core.NewDataUser(id)
		dataUsers = append(dataUsers, user)
	}

	return dataUsers
}

func CreateStorageNodes(numStorageNodes int) ([]*core.Node, error) {
	nodes := make([]*core.Node, 0)

	for i := 0; i < numStorageNodes; i++ {
		nodeID := fmt.Sprintf("node_%d", i + 1)
		node, err := core.NewNode(nodeID)
		if err != nil {
			log.Error("Could not create storage node")
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
	viper.SafeWriteConfig()

	return nil
}

func (m *Msg) ValidFormat() bool {
	if m.SetPermission == "set" || m.SetPermission == "unset" {
		if m.Permission == file.FILE_READ || m.Permission == file.FILE_ANALYSIS || m.Permission == file.FILE_SHARE {
			return true
		}
	}

	return false
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
/*
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