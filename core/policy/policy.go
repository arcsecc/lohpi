package policy

import (
	"fmt"
	"log"
	"os"
	"errors"
	"encoding/json"
	"encoding/gob"
	"net"
	"net/http"
	"mime/multipart"
	"strings"
	"sync"
	"time"
	"strconv"
	"io/ioutil"
	"bytes"
	"path/filepath"
	
	"crypto"
	"crypto/rsa"
	"crypto/x509/pkix"
	"crypto/tls"

	"firestore/core/message"
	"firestore/core/cache"
	"firestore/netutil"
	"firestore/comm"
	
	"github.com/joonnna/ifrit"
	"github.com/go-git/go-git"
	"github.com/go-git/go-git/plumbing/object"
	"github.com/spf13/viper"
	"github.com/gorilla/mux"
)

type PolicyStore struct {
	// The underlying Fireflies client
	ifritClient *ifrit.Client

	// Cache manager 
	cache 		*cache.Cache

	// Go-git 
	repository *git.Repository

	listener 		net.Listener
	httpServer 		*http.Server
	port	 		int
	serverConfig 	*tls.Config
	
	exitChan 		chan bool
	wg       		*sync.WaitGroup

	cu 				*comm.CryptoUnit
	publicKey 		crypto.PublicKey
	privateKey 		*rsa.PrivateKey
}

func NewPolicyStore() (*PolicyStore, error) {
	c, err := ifrit.NewClient()
	if err != nil {
		panic(err)
	}
	
	portString := strings.Split(viper.GetString("policy_store_addr"), ":")[1]
	port, err := strconv.Atoi(portString)
	if err != nil {
		return nil, err
	}

	netutil.ValidatePortNumber(&port)	

	repository, err := initializeGitRepository(viper.GetString("policy_store_repo"))
	if err != nil {
		return nil, err
	}

	listener, err := netutil.ListenOnPort(port)
	if err != nil {
		return nil, err
	}

	pk := pkix.Name{
		Locality: []string{listener.Addr().String()},
	}
	
	cu, err := comm.NewCu(pk, viper.GetString("lohpi_ca_addr"))
	if err != nil {
		return nil, err
	}
	serverConfig := comm.ServerConfig(cu.Certificate(), cu.CaCertificate(), cu.Priv())

	cache, err := cache.NewCache(c)
	if err != nil {
		return nil, err
	}

	return &PolicyStore {
		ifritClient: 	c,
		repository: 	repository,
		listener: 		listener,
		serverConfig: 	serverConfig,
		port: 			port,
		exitChan:		make(chan bool, 1),
		wg:             &sync.WaitGroup{},
		cache:			cache,
		cu:				cu,
	}, nil
}

func (ps *PolicyStore) Start() {
	ps.ifritClient.RegisterMsgHandler(ps.messageHandler)
	go ps.ifritClient.Start()
	go ps.HttpHandler()
}

func (ps *PolicyStore) Stop() {
}

func (ps *PolicyStore) messageHandler(data []byte) ([]byte, error) {
	var msg message.NodeMessage
	err := json.Unmarshal(data, &msg)
	if err != nil {
		panic(err)
	}

	fmt.Println("Got ", msg.MessageType, "in PS")

	switch msgType := msg.MessageType; msgType {
		case message.MSG_TYPE_SET_STUDY_LIST:
			studies := make([]string, 0)
			reader := bytes.NewReader(msg.Extras)
			dec := gob.NewDecoder(reader)
			if err := dec.Decode(&studies); err != nil {
				return nil, err
			}

			ps.cache.UpdateStudies(msg.Node, studies)
		default:
			fmt.Printf("Unknown message: %s\n", msgType)
	}
	
	return []byte(message.MSG_TYPE_OK), nil 
}

func (ps *PolicyStore) HttpHandler() error {
	r := mux.NewRouter()

	//r := .NewRouter()
	log.Printf("Policy store: Started HTTP server on port %d\n", ps.port)
	r.HandleFunc("/set_port", ps.NodeHandshake)
	r.HandleFunc("/rec/set_policy", ps.SetRecPolicy)

	ps.httpServer = &http.Server{
		Handler: 	r,
		TLSConfig:	ps.serverConfig,
	}

	//err := ps.httpServer.ServeTLS(ps.listener, "", "") 
	err := ps.httpServer.Serve(ps.listener)
	if err != nil {
		log.Fatal(err.Error())
	}
	return nil
}

// Handshake endpoint for nodes to join the network
// TODO: move handshakes to a better location (mux has the exact same function)
func (ps *PolicyStore) NodeHandshake(w http.ResponseWriter, r *http.Request) {
	log.Println("New node joining policy store")
	defer r.Body.Close()
	if r.Method != http.MethodPost {
		http.Error(w, "Expected POST method", http.StatusMethodNotAllowed)
		return
	}

	if r.Header.Get("Content-type") != "application/json" {
		http.Error(w, "Require header to be application/json", http.StatusUnprocessableEntity)
	}

	// Incoming message from node
	var msg struct {
		Node 	string 		`json:"node"`
		Address string 		`json:"address"`
	}

	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(&msg)
    if err != nil {
		errMsg := fmt.Sprintf("Error: could not decode node handshake")
		log.Printf("Error: %s\n", errMsg)
		http.Error(w, errMsg, http.StatusBadRequest)
		return
	}

	var resp struct {
		PolicyStoreIP	string
		PublicKey		[]byte
	}

	// Insert node if it doesn't exist. Respond to node with IP address and public key
	if !ps.cache.NodeExists(msg.Node) {
		ps.cache.UpdateNodes(msg.Node, msg.Address)

		pubKey, err := ps.cu.EncodePublicKey()
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest) // use internal error
			return
		}

		resp.PolicyStoreIP = ps.ifritClient.Addr()
		resp.PublicKey = pubKey

		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		if err := enc.Encode(&resp); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write(buf.Bytes())
		fmt.Printf("Added %s to map with IP %s\n", msg.Node, msg.Address)
	} else {
		errMsg := fmt.Sprintf("Node '%s' already exists in network\n", msg.Node)
		http.Error(w, errMsg, http.StatusBadRequest)
	}
}

// Sets a study's policy. The policy originates from REC and it is applied to 
// all subjects in the study
func (ps *PolicyStore) SetRecPolicy(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	err := r.ParseMultipartForm(32 << 20)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if r.MultipartForm == nil || r.MultipartForm.File == nil {
		http.Error(w, "expecting multipart form file", http.StatusBadRequest)
		return
	}

	modelFile, fileHeader, err := r.FormFile("model")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	
	study := r.PostFormValue("study")
	if err := ps.setRECPolicy(modelFile, fileHeader, study); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	fmt.Fprintf(w, "REC sets new access policy for study '%s'\n", r.PostFormValue("study"))
}

func (ps *PolicyStore) setRECPolicy(file multipart.File, fileHeader *multipart.FileHeader, study string) error {
	if !ps.cache.StudyExists(study) {
		errMsg := fmt.Sprintf("Study '%s' does not exist in the network", study)
		return errors.New(errMsg)
	}
	
	fileContents, err := ioutil.ReadAll(file)
	if err != nil {
		return err
	}

	// Store the file on disk
	fullPath := filepath.Join(viper.GetString("policy_store_repo"), fileHeader.Filename)
	if err := ioutil.WriteFile(fullPath, fileContents, 0644); err != nil {
		return err
	}

	// Commit the file to the local Git log 
	if err := ps.commit(fileHeader.Filename); err != nil {
		return err
	}

	// send policy directoly to the node that stores the study
	node := ps.cache.Studies()[study]
	if err := ps.sendStudyPolicy(node, fileContents); err != nil {
		return err
	}

	return nil 
}

// Send the policy update to the correct node
func (ps *PolicyStore) sendStudyPolicy(node string, fileContents []byte) error {
	// Sign the new policy using ECDSA
	r, s, err := ps.cu.Sign(fileContents)
	if err != nil {
		return err
	}

	msg := &message.NodeMessage{
		MessageType: 	message.MSG_TYPE_SET_POLICY,
		R:				r,
		S:				s,
		ModelText: 		string(fileContents),
	}

	serializedMsg, err := msg.Encode()
	if err != nil {
		return err
	}

	// TODO: What should we do if the node does not respond?
	ch := ps.ifritClient.SendTo(ps.cache.NodeAddr(node), serializedMsg)
	select {
		case response := <- ch: 
			fmt.Println("Resp in PS: ", response)
	}
	return nil
}

// Commit the policy model to the Git repository
func (ps *PolicyStore) commit(filePath string) error {
	fmt.Println("Commiting file at path ", filePath)

	// Get the current worktree
	wt, err := ps.repository.Worktree()
	if err != nil {
		panic(err)
	}

	// Add the file to the staging area
	_, err = wt.Add(filePath)
	if err != nil {
		panic(err)
	}

	// Commit the file
	// TODO: use RECs attributes when commiting the file
	c, err := wt.Commit(filePath, &git.CommitOptions{
		Author: &object.Signature{
			Name:  "John Doe",
			Email: "john@doe.org",
			When:  time.Now(),
		},
	})

	if err != nil {
		return err
	}

	obj, err := ps.repository.CommitObject(c)
	if err != nil {
		return err
	}
	//fmt.Println(obj)
	_ = obj
	return nil
}

// Shut down all resources assoicated with the policy store
func (ps *PolicyStore) Shutdown() {
	log.Println("Shutting down policy store")
	ps.ifritClient.Stop()
}

// Sets up the Git resources in an already-existing Git repository
func initializeGitRepository(path string) (*git.Repository, error) {
	ok, err := exists(path); 
	if err != nil {
		log.Fatalf(err.Error())
		return nil, err
	}

	if !ok {
		errMsg := fmt.Sprintf("Directory '%s' does not exist")
		return nil, errors.New(errMsg)
	}

	return git.PlainOpen(path)
}

// TODO: put me into common utils
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


/*

	
	
	/*	msg := &message.NodeMessage {
		MessageType: message.MSG_TYPE_SET_REC_POLICY,
		Study: 		"study_0",
		Filename: 	"model.conf",
		Extras: 	[]byte(`[request_definition]
		r = sub, obj, act
		[policy_definition]
		p = sub, obj, act
		[policy_effect]
		e = some(where (p.eft == allow))
		[matchers]
		m=r.sub.Country==r.obj.Country && r.sub.Network==r.obj.Network && r.sub.Purpose==r.obj.Purpose
		`),
	}*/
	