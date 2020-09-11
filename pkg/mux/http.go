package mux

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
_	"context"
_	"time"

	"github.com/tomcat-bit/lohpi/pkg/message"
//	pb "github.com/tomcat-bit/lohpi/protobuf"

	"github.com/gorilla/mux"
	logging "github.com/inconshreveable/log15"
)

func (m *Mux) HttpHandler() error {
	r := mux.NewRouter()
	log.Printf("MUX: Started HTTP server on port %d\n", m.config.MuxHttpPort)

	// Public methods exposed to data users (usually through cURL)
	r.HandleFunc("/network", m.network)

	// Node API
	r.HandleFunc("/node/info", m.GetNodeInfo).Methods("GET")
	r.HandleFunc("/node/load", m.LoadNode).Methods("POST")

	// Study API
	r.HandleFunc("/study/metadata", m.GetMetaData).Methods("GET") // MORE TODO
	r.HandleFunc("/study/data", m.GetData).Methods("POST")        // MORE TODO

	m.httpServer = &http.Server{
		Handler: r,
		// use timeouts?
	}

	err := m.httpServer.Serve(m.httpListener)
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
	fmt.Fprintf(w, "Mux's HTTP server running on port %d\n", m.config.MuxHttpPort)
	fmt.Fprintf(w, "Mux's gRPC server running on address %s\n", m.grpcs.Addr())
	fmt.Fprintf(w, "Mux's Ifrit client's IP address: %s\n", m.ifritClient.Addr())
	fmt.Fprintf(w, "Flireflies nodes in this network:\n")
	m.cache.FetchRemoteObjectHeaders()
	for nodeID, node := range m.cache.Nodes() {
		fmt.Fprintf(w, "String identifier: %s\tIP address: %s\n", nodeID, node.GetAddress())
	}

	fmt.Fprintf(w, "Studies stored in the network:\n")
	for study, header := range m.cache.ObjectHeaders() {
		fmt.Fprintf(w, "Study identifier: '%s'\tstorage node: '%s'\n", study, header.GetNode().GetName())
	}
}

// End-point used to load the node dummy-data. The target node stores the meta-data
// and generates random data from the POST payload
func (m *Mux) LoadNode(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	err := r.ParseMultipartForm(32 << 20)
	if err != nil {
		panic(err)
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
		panic(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer mdFile.Close()
	
	policyFile, policyFileHeader, err := r.FormFile("policy")
	if err != nil {
		panic(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer policyFile.Close()

	// Read the multipart file from the client
	buf := bytes.NewBuffer(nil)
	if _, err := io.Copy(buf, mdFile); err != nil {
		panic(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
	}

	// Read the policy file
	policyBuf := bytes.NewBuffer(nil)
	if _, err := io.Copy(policyBuf, policyFile); err != nil {
		panic(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
	}

	studyName := r.PostFormValue("study")
	subjects := r.MultipartForm.Value["subjects"]
	node := r.PostFormValue("node")

	if studyName == "" || subjects == nil || node == "" {
		http.Error(w, "Missing fields when loading node.", http.StatusMethodNotAllowed)
		return
	}

	// Send metadata and loading information to the node. It might fail (ie. node doesn't exist) 
	if err := m.loadNode(studyName, node, policyFileHeader.Filename, buf.Bytes(), r.MultipartForm.Value["subjects"], policyBuf.Bytes()); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	fmt.Fprintf(w, "Loaded node '%s' with study name '%s'\n", node, studyName)
}

// Sets a study's policy. The policy originates from a subject
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