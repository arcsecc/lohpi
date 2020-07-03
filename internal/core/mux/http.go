package mux

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"context"
	"time"

	"github.com/tomcat-bit/lohpi/internal/core/message"
	pb "github.com/tomcat-bit/lohpi/protobuf"

	"github.com/gorilla/mux"
	logging "github.com/inconshreveable/log15"
)

func (m *Mux) HttpHandler() error {
	r := mux.NewRouter()
	log.Printf("MUX: Started HTTP server on port %d\n", m.httpPortNum)

	// Public methods exposed to data users (usually through cURL)
	r.HandleFunc("/network", m.network)

	// Node API
	r.HandleFunc("/node/info", m.GetNodeInfo).Methods("GET")
	r.HandleFunc("/node/load", m.LoadNode).Methods("POST")

	// Study API
	r.HandleFunc("/study/metadata", m.GetMetaData).Methods("GET") // MORE TODO
	r.HandleFunc("/study/data", m.GetData).Methods("POST")        // MORE TODO

	m._httpServer = &http.Server{
		Handler: r,
		// use timeouts?
	}

	err := m._httpServer.Serve(m._httpListener)
	if err != nil {
		logging.Error(err.Error())
		return err
	}
	return nil
}

// Returns human-readable network information and studies known to the network
func (m *Mux) network(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if r.Method != http.MethodGet {
		http.Error(w, "Expected GET method", http.StatusMethodNotAllowed)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Mux's HTTP server running on port %d\n", m.httpPortNum)
	fmt.Fprintf(w, "Mux's HTTPS server running on port %d\n", m.portNum)
	fmt.Fprintf(w, "Flireflies nodes in this network:\nMux: %s\n", m.ifritClient.Addr())
	m.cache.FetchRemoteStudyLists()
	for nodeID, addr := range m.cache.Nodes() {
		fmt.Fprintf(w, "String identifier: %s\tIP address: %s\n", nodeID, addr)
	}

	fmt.Fprintf(w, "Studies stored in the network:\n")
	for study, node := range m.cache.Studies() {
		fmt.Fprintf(w, "Study identifier: '%s'\tstorage node: '%s'\n", study, node)
	}
}

// End-point used to load the node dummy-data. The target node stores the meta-data
// and generates random data from the POST payload
func (m *Mux) LoadNode(w http.ResponseWriter, r *http.Request) {
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

	if r.Method != http.MethodPost {
		http.Error(w, "Expected POST method", http.StatusMethodNotAllowed)
		return
	}
	
	mdFile, _, err := r.FormFile("metadata")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer mdFile.Close()
	
	// Read the multipart file from the client
	buf := bytes.NewBuffer(nil)
	if _, err := io.Copy(buf, mdFile); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}

	studyName := r.PostFormValue("study")
	subjects := r.MultipartForm.Value["subjects"]
	node := r.PostFormValue("node")

	if studyName == "" || subjects == nil || node == "" {
		http.Error(w, "Missing fields when loading node.", http.StatusMethodNotAllowed)
		return
	}

	// Send metadata and loading information to the node. It might fail
	// (node doesn't exist) 
	if err := m.loadNode(studyName, node, buf.Bytes(), r.MultipartForm.Value["subjects"]); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	
	// Send metadata to rec
	if err := m.sendRecMetadata(studyName, node, buf.Bytes(), r.MultipartForm.Value["subjects"]); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	fmt.Fprintf(w, "Loaded node '%s' with study name '%s'\n", node, studyName)
}

func (m *Mux) sendRecMetadata(studyName, node string, md []byte, subjects []string) error {
	conn, err := m.recClient.Dial(m.config.RecIP)
	defer conn.CloseConn()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Metadata to be sent to REC
	_, err = conn.SetStudy(ctx, &pb.Study{
		Name: studyName,
		Node: &pb.Node{
			Name: node,
			Address: m.cache.Nodes()[node],
		},
		Metadata: &pb.Metadata{
			Content: md,
			Subjects: subjects,
		},
	})
	if err != nil {
		return err
	}

	return nil 
}

// Sets a study's policy. The policy originates from a subject
// TODO move to policy store
func (m *Mux) SetSubjectStudyPolicy(w http.ResponseWriter, r *http.Request) {
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
	if err := m._setSubjectStudyPolicy(modelFile, fileHeader, study); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	fmt.Fprintf(w, "REC sets new access policy for study '%s'\n", r.PostFormValue("study"))
}

// Returns human-readable information about a particular node
func (m *Mux) GetNodeInfo(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if r.Method != http.MethodGet {
		http.Error(w, "Expected GET method", http.StatusMethodNotAllowed)
		return
	}

	var msg message.NodeMessage
	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(&msg)
	if err != nil {
		errMsg := fmt.Sprintf("Error: %s\n", err)
		log.Printf("%s", errMsg)
		http.Error(w, errMsg, http.StatusBadRequest)
		return
	}

	nodeInfo, err := m.getNodeInfo(msg.Node)
	if err != nil {
		errMsg := fmt.Sprintf("Error: %s\n", err)
		log.Printf("%s", errMsg)
		http.Error(w, errMsg, http.StatusBadRequest)
		return
	}
	fmt.Fprintf(w, nodeInfo)
}

// Given a node identifier and a study name, return the meta-data about a particular study at that node.
// DUMMY IMPLEMENTATION
func (m *Mux) GetMetaData(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	/*
		countries := []string{`["Norway"`, `"kake country]"`}
		network := []string{`["network1"`, `"network2]"`}
		purpose := []string{`["non-commercial"]`}

		msg := &message.NodeMessage{
			MessageType: 	message.MSG_TYPE_GET_META_DATA,
			Study: "Sleeping and Diet patterns in Northern Norway",
			Node: "node_0",
		}

		statusCode, result, err := m._getMetaData(*msg)
		if err != nil {
			http.Error(w, err.Error(), statusCode)
			return
		}

		w.WriteHeader(statusCode)
		fmt.Fprintf(w, "Status code: %d\tresult: %s\n", statusCode, result)*/
}

// Given a node identifier and a study name, return the data at that node
// DUMMY IMPLEMENTATION
func (m *Mux) GetData(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	countries := []string{`["Norway"`, `"kake country]"`}
	network := []string{`["network1"`, `"network2]"`}
	purpose := []string{`["non-commercial"]`}

	msg := &message.NodeMessage{
		MessageType: message.MSG_TYPE_GET_META_DATA,
		Study:       "Sleeping and Diet patterns in Northern Norway",
		Node:        "node_0",
		Attributes: map[string][]string{"country": countries,
			"research_network": network,
			"purpose":          purpose},
	}

	statusCode, result, err := m._getStudyData(*msg)
	if err != nil {
		http.Error(w, err.Error(), statusCode)
		return
	}

	w.WriteHeader(statusCode)
	fmt.Fprintf(w, "Status code: %d\tresult: %s\n", statusCode, result)
}



/*
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

	// Send policy directoly to the node that stores the study
	node := ps.cache.Studies()[study]
	if err := ps.sendStudyPolicy(node, fileContents); err != nil {
		return err
	}

	// Gossip the policy to the network
	ps.gm.Submit(fileContents)
	ps.gm.Submit([]byte("pokpaosdkpoaskpokdpoaskpod"))

	return nil
}
*/