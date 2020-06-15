package mux

import (
	"net/http"
	"fmt"
	"io"
	"bytes"
	_"strings"
	"encoding/json"
	_"encoding/gob"
	"log"

	"firestore/core/message"

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
	r.HandleFunc("/study/metadata", m.GetMetaData).Methods("GET")		// MORE TODO
	r.HandleFunc("/study/data", m.GetData).Methods("POST")				// MORE TODO
	
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
	if r.Method != http.MethodPost {
		http.Error(w, "Expected POST method", http.StatusMethodNotAllowed)
		return
	}

	if r.Header.Get("Content-type") != "application/json" {
		http.Error(w, "Require header to be application/json", http.StatusUnprocessableEntity)
		return
	}

	// Used to prepare data packets to be sent to the node
	var body bytes.Buffer
	io.Copy(&body, r.Body)
	data := body.Bytes()

	nodePopulator, err := message.NewNodePopulator(data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}

	if err := m.loadNode(nodePopulator); err != nil {
		errMsg := fmt.Sprintf("Error: %s", err)
		http.Error(w, errMsg, http.StatusUnprocessableEntity)
		return
	}
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Created bulk data at node '%s'. Study name: '%s'\n", nodePopulator.Node, nodePopulator.MetaData.Meta_data_info.StudyName)
}

// Sets a study's policy. The policy originates from a subject
func (m *Mux) SetSubjectStudyPolicy(w http.ResponseWriter, r *http.Request)  {
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
		MessageType: 	message.MSG_TYPE_GET_META_DATA,
		Study: "Sleeping and Diet patterns in Northern Norway",
		Node: "node_0",
		Attributes: map[string][]string{"country": 			countries, 
										"research_network": network, 
										"purpose": 			purpose},
	}

	statusCode, result, err := m._getStudyData(*msg)
	if err != nil {
		http.Error(w, err.Error(), statusCode)
		return
	}

	w.WriteHeader(statusCode)
	fmt.Fprintf(w, "Status code: %d\tresult: %s\n", statusCode, result)
}