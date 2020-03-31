package node

import (
	"fmt"
	"strconv"
	"encoding/json"
	"net/http"
	"bytes"
	"errors"
	"time"
	"log"
	"crypto/x509/pkix"
	"crypto/tls"
	"firestore/core/node/fuse"
	"firestore/comm"

	"github.com/joonnna/ifrit"
	"github.com/spf13/viper"

	logger "github.com/inconshreveable/log15"
)

type Msgtype string 

const (
	MSG_TYPE_NEW_STUDY = "MSG_TYPE_NEW_STUDY"
)

var (
	errNoAddr = errors.New("No certificate authority address provided, can't continue")
	logging   = logger.New("module", "node/main")
)

type Node struct {
	// Fuse file system
	fs *fuse.Ptfs
	
	// Underlying ifrit client
	c *ifrit.Client

	// Stringy identifier of this node
	nodeName string

	// HTTP-related variables
	//Listener   net.Listener
	//httpServer *http.Server
	clientConfig *tls.Config
}

func NewNode(nodeName string) (*Node, error) {
	if err := readConfig(); err != nil {
		panic(err)
	}

	c, err := ifrit.NewClient()
	if err != nil {
		panic(err)
	}

	logger.Info(c.Addr(), " spawned")

	pk := pkix.Name{
		Locality: []string{c.Addr()},
	}

	cu, err := comm.NewCu(pk, viper.GetString("lohpi_ca_addr"))
	if err != nil {
		return nil, err
	}

	clientConfig := comm.ClientConfig(cu.Certificate(), cu.CaCertificate(), cu.Priv())

	node := &Node {
		nodeName: 		nodeName,
		c: 				c,
		clientConfig: 	clientConfig,
	}

	return node, nil
}

func (n *Node) StartIfritClient() {
	n.c.RegisterMsgHandler(n.messageHandler)
	go n.c.Start()
}

func (n *Node) MountFuse() error {
	fs, err := fuse.NewFuseFS(n.nodeName)
	if err != nil {
		return err
	}
	n.fs = fs
	return nil
}

func (n *Node) FireflyClient() *ifrit.Client {
	return n.c
}

func (n *Node) Addr() string {
	return n.c.Addr()
}

func (n *Node) NodeName() string {
	return n.nodeName
}

func (n *Node) Shutdown() {
	log.Printf("Shutting down...\n")
	n.c.Stop()
	fuse.Shutdown() // might fail...
}

func (n *Node) messageHandler(data []byte) ([]byte, error) {
	<-time.After(time.Second * 1)
	logger.Debug(string(data))
	logger.Debug("kake asuidaiosuoij\n")
	return []byte("koaspkdpaos\n"), nil
}

func (n *Node) SendPortNumber(nodeName, addr string, muxPort uint) error {
	URL := "https://127.0.1.1:" + strconv.Itoa(int(muxPort)) + "/set_port"
	fmt.Printf("URL:> %s\n", URL)

	var msg struct {
		Node 	string 		`json:"node"`
		Address string 		`json:"address"`
	}

	msg.Node = nodeName
	msg.Address = addr
	jsonStr, err := json.Marshal(msg)
	if err != nil {
        return err
    }

	req, err := http.NewRequest("POST", URL, bytes.NewBuffer(jsonStr))
	req.Header.Set("Content-Type", "application/json")

	transport := &http.Transport{
		TLSClientConfig: n.clientConfig,
	}
	client := &http.Client{Transport: transport}
	response, err := client.Do(req)
    if err != nil {
        return err
	}
	defer response.Body.Close()
	if response.StatusCode != int(http.StatusOK) {
		errMsg := fmt.Sprintf("%s", response.Body)
		return errors.New(errMsg)
	}
	return nil
}

/*
func (n *Node) SetNodeSubjectPermission(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if r.Method != http.MethodPost {
		http.Error(w, "Expected POST method", http.StatusMethodNotAllowed)
		return
	}

	if r.Header.Get("Content-type") != "application/json" {
		logger.Error("Require header to be application/json")
	}

	var msg struct {
		Subject string `json:"subject"`
		Permission string `json:"permission"`
	}

	// Assert JSON format somewhere around here...

	var body bytes.Buffer
	io.Copy(&body, r.Body)
	err := json.Unmarshal(body.Bytes(), &msg)
	if err != nil {
		panic(err)
	}

	err = n.fs.SetSubjectPermission(msg.Subject, msg.Permission)
	if err != nil {
		w.WriteHeader(http.StatusNotAcceptable)
		str := fmt.Sprintf("%s\n", err)
		fmt.Fprintf(w, "%s", str)
	} else {
		w.WriteHeader(http.StatusOK)
	}
}

func (n *Node) PrintFiles(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Method not implemented")
}

func (n *Node) CreateSubject(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if r.Method != http.MethodPost {
		http.Error(w, "Expected POST method", http.StatusMethodNotAllowed)
		return
	}

	if r.Header.Get("Content-type") != "application/json" {
		logger.Error("Require header to be application/json")
	}

	var msg struct {
		Subject string `json:"subject"`
		Permission string `json:"permission"`
	}

	// Assert JSON format somewhere around here...

	var body bytes.Buffer
	io.Copy(&body, r.Body)
	err := json.Unmarshal(body.Bytes(), &msg)
	if err != nil {
		panic(err)
	}

	err = n.fs.CreateSubject(msg.Subject)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		str := fmt.Sprintf("%s\n", err)
		fmt.Fprintf(w, "%s", str)
		return
	}

	err = n.fs.SetSubjectPermission(msg.Subject, msg.Permission)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		str := fmt.Sprintf("%s\n", err)
		fmt.Fprintf(w, "%s", str)
		return
	}
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Created subject %s succsessfully", msg.Subject)
}

func (n *Node) Subjects(w http.ResponseWriter, r *http.Request) {
	/*
	defer r.Body.Close()
	if r.Method != http.MethodPost {
		http.Error(w, "Expected POST method", http.StatusMethodNotAllowed)
		return
	}

	if r.Header.Get("Content-type") != "application/json" {
		logger.Error("Require header to be application/json")
	}

	subjects, err := n.fs.Subjects()
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		str := fmt.Sprintf("%s\n", err)
		fmt.Fprintf(w, "%s", str)
	} else {
		w.WriteHeader(http.StatusOK)
		str := fmt.Sprintf("%s", subjects)
		fmt.Fprintf(w, "%s", str)
	}*
}
*/

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
func NewApplication() (*Application, error) {
	app := &Application{}

	l, err := netutil.ListenOnPort(viper.GetInt("server_port"))
	if err != nil {
		return nil, err
	}

	//numFiles := viper.GetInt("files_per_subject")
	numNodes := viper.GetInt("firestore_nodes")
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

/**** HTTP end-point functions *
func (app *Application) httpHandler() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/print_files", app.PrintFiles) // nope
	mux.HandleFunc("/print_node", app.PrintNode)	// 
	mux.HandleFunc("/subject_node_set_perm", app.SubjectNodeSetPermission)
	mux.HandleFunc("/subject_set_perm", app.SubjectSetPermission)
	mux.HandleFunc("/node_set_perm", app.NodeSetPermission)
	mux.HandleFunc("/subjects", app.PrintSubjects)
	mux.HandleFunc("/shutdown", app.Shutdown)
	mux.HandleFunc("/set_app_state", app.SetApplicationState)
	mux.HandleFunc("/node_create_study", app.NodeCreateStudy)
	mux.HandleFunc("/load_node", app.LoadNodeState)

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

	var msg struct {
		Node string `json:"node"`
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "%s", nodeInfo)
}

// Nope
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
		http.Error(w, "Require header to be application/json", http.StatusUnprocessableEntity)
		return
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

	err = app.MasterNode.SetSubjectNodePermission(msg.Node, msg.Subject, msg.Permission)
	if err != nil {
		w.WriteHeader(http.StatusNotAcceptable)
		str := fmt.Sprintf("%s\n", err)
		fmt.Fprintf(w, "%s", str)
	} else {
		w.WriteHeader(http.StatusOK)
	}
}

func (app *Application) SubjectSetPermission(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()g
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
// TODO: finish later
func (app *Application) SetApplicationState(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	if r.Method != http.MethodPost {
		http.Error(w, "Require POST method", http.StatusMethodNotAllowed)
		return
	}

	if r.Header.Get("Content-type") != "application/json" {
		http.Error(w, "Require header to be application/json", http.StatusUnprocessableEntity)
		return
	}

	var message messages.AppStateMessage
	var body bytes.Buffer
	io.Copy(&body, r.Body)
	err := json.Unmarshal(body.Bytes(), &message)
	if err != nil {
		panic(err)
	}

	err = app.MasterNode.SetNodeFiles(&message) 
	if err != nil {
		errorString := fmt.Sprintf("Could not assign files to nodes. Error: %s\n", err.Error())
		http.Error(w, errorString, http.StatusUnprocessableEntity)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Assigned files to nodes\n")
}

func (app *Application) NodeCreateStudy(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if r.Method != http.MethodPost {
		http.Error(w, "Require POST method", http.StatusMethodNotAllowed)
		return
	}

	if r.Header.Get("Content-type") != "application/json" {
		http.Error(w, "Require header to be application/json", http.StatusUnprocessableEntity)
		return
	}

	var msg struct {
		Node string `json:"node"`
		Study string `json:"study"`
	}

	var body bytes.Buffer
	io.Copy(&body, r.Body)
	err := json.Unmarshal(body.Bytes(), &msg)
	if err != nil {
		panic(err)
	}

	// FIX THIS LATER TO MAKE IT PRETTIER
	if msg.Node == "" || msg.Study == "" {
		errorString := fmt.Sprintf("Invalid input: %v\n", msg);
		http.Error(w, errorString, http.StatusUnprocessableEntity)
		return
	}

	err = app.MasterNode.NodeCreateStudy(msg.Node, msg.Study)
	if err != nil {
		errorString := fmt.Sprintf("Could not create study. Error: %s\n", err.Error())
		http.Error(w, errorString, http.StatusUnprocessableEntity)
		return
	}
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Succsessfully created a new study in node %s\n", msg.Node)
}

func (app *Application) LoadNodeState(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if r.Method != http.MethodPost {
		http.Error(w, "Require POST method", http.StatusMethodNotAllowed)
		return
	}

	if r.Header.Get("Content-type") != "application/json" {
		http.Error(w, "Require header to be application/json", http.StatusUnprocessableEntity)
		return
	}

	var msg struct {
		Node string `json:"node"`
	}

	log.Printf("%s is primarily meant to set the node's state after populating it with files and subjects.", r.URL)
	log.Printf("Use with cation\n")

	var body bytes.Buffer
	io.Copy(&body, r.Body)
	err := json.Unmarshal(body.Bytes(), &msg)
	if err != nil {
		panic(err)
	}

	// FIX THIS LATER TO MAKE IT PRETTIEEEEEEEER
	if msg.Node == "" {
		errorString := fmt.Sprintf("Invalid input: %v\n", msg);
		http.Error(w, errorString, http.StatusUnprocessableEntity)
		return
	}

	err = app.MasterNode.LoadNodeState(msg.Node)
	if err != nil {
		errorString := fmt.Sprintf("Could not load node state. Error: %s\n", err.Error())
		http.Error(w, errorString, http.StatusUnprocessableEntity)
		return
	}
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Succsessfully loaded %s state from disk\n", msg.Node)
}

/*
func (app *Application) assignSubjectFilesToStorageNodes(nodes []*node.Node, subjects []*core.Subject, numFiles int) {
	fileSize := viper.GetInt("file_size")
	app.Files = make([]*file.File, 0)

	for _, subject := range subjects {
		for i := 0; i < numFiles; i++ {
			for _, node := range nodes {
				path := fmt.Sprintf("%s/%s/file%d.txt", node.StorageDirectory(), subject.ID(), i)
				file, err := file.NewFile(fileSize, path, subject.ID(), node.ID(), file.FILE_ALLOWED)
				if err != nil {
					panic(err)
				}
				node.AppendSubjectFile(file)
				app.Files = append(app.Files, file)
			}
		}
	}
}
*
func CreateApplicationSubject(numSubjects int) []*core.Subject {
	subjects := make([]*core.Subject, 0)
	for i := 1; i <= numSubjects; i++ {
		subjectName := fmt.Sprintf("subject_%d", i)
		subjectDirPath := fmt.Sprintf("/tmp/subject_data/subject_%d", i)
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
	for i := 1; i < numStorageNodes; i++ {
		nodeID := fmt.Sprintf("node_%d", i)
		node, err := node.NewNode(nodeID)
		<-time.After(time.Second * 1)
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
	viper.SetDefault("num_studies", 10)
	viper.SetDefault("data_users", 1)
	viper.SetDefault("files_per_study", 2)
	viper.SetDefault("file_size", 256)
	viper.SetDefault("server_port", 8080)
	viper.SetDefault("fuse_mount", "/home/thomas/go_workspace/src/firestore")
	viper.SetDefault("set_files", true)
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

/********** Initialization of file trees for each node prior to interactions with the system */
//func createSubjectDirectories(numSubjects int, nodePath string) {
	/*for subject_id := 1; subject_id <= numSubjects; subject_id++ {
		subjectDirPath := fmt.Sprintf("%s/%s/subject_%d", nodePath, node.SUBJECTS_DIR, subject_id)
		err := os.Mkdir(subjectDirPath, 0755)
		if err != nil {
			panic(err)
		}
*/
//		numStudies := viper.GetInt("num_studies")
		//createStudyDirectoriesInNodes(numStudies, subjectDirPath)
	//}


//func createStudyDirectoriesInNodes(numStudies int, subjectPath string) {
/*	for study_id := 1; study_id <= numStudies; study_id++ {
		studyDirPath := fmt.Sprintf("%s/study_%d", subjectPath, study_id)
		err := os.Mkdir(studyDirPath, 0755)
		if err != nil {
			panic(err)
		}

		numFiles := viper.GetInt("files_per_subject")
		createStudyFiles(numFiles, studyDirPath)	
	}*
}

func createStudyFiles(numFiles int, studyDirPath string) {
/*	for file_id := 1; file_id <= numFiles; file_id++ {
		filePath := fmt.Sprintf("%s/file_%d", studyDirPath, file_id)

		// Fill file with noise
		file, err := os.Create(filePath)
		if err != nil {
			panic(err)
		}

		fileSize := viper.GetInt("file_size")
		fileContents := make([]byte, fileSize)
		_, err = rand.Read(fileContents)
		if err != nil { 
			panic(err)
		}
		
		n, err := file.Write(fileContents)
		if err != nil {
			fmt.Errorf("Should write %d bytes -- wrote %d instead\n", fileSize, n)
			panic(err)
		}
		err = file.Close()
		if err != nil {
			panic(err)
		}
	}*
}

// Setup the directories in 'subjects' directory
func setupStudyRelations(nodePath string, numStudies int) {
/*	numSubjects := viper.GetInt("num_subjects")
	for i := 1; i <= numStudies; i++ {
		studyDirPath := fmt.Sprintf("%s/%s/study_%d/subjects", nodePath, node.STUDIES_DIR, i)
		err := os.MkdirAll(studyDirPath, 0755)
		if err != nil {
			panic(err)
		}

		// Insert a symlink pointing from 'studies' directory to 'subjects' directory
		for s := 1; s < numSubjects; s++ {
			// Path of symlink
			symlinkPath := fmt.Sprintf("%s/subject_%d", studyDirPath, s)
			targetPath := fmt.Sprintf("%s/%s/subject_%d/study_%d",nodePath, node.SUBJECTS_DIR, i, i)
			//fmt.Printf("%s -> %s\n", symlinkPath, targetPath)

			err = os.Symlink(targetPath, symlinkPath)
			if err != nil {
				panic(err)
			}
		}
	}
}

func exists(path string) (bool, error) {
    _, err := os.Stat(path)
    if err == nil { 
		return true, nil 
	}
    if os.IsNotExist(err) { 
		return false, nil 
	}
    return true, err
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
}
if err != nil {
	panic(err)
}
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

