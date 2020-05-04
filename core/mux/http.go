package mux

import (
	"net/http"
	"fmt"
	"io"
	"bytes"
	"encoding/json"
	_"encoding/gob"
	"log"

	"firestore/core/message"

	logging "github.com/inconshreveable/log15"
)

func (m *Mux) HttpHandler() error {
	log.Printf("MUX: Started HTTP server on port %d\n", m._httpPortNum)
	mux := http.NewServeMux()

	// Public methods exposed to data users (usually through cURL)
	mux.HandleFunc("/network", m.Network)
	mux.HandleFunc("/load_node", m.StoreNodeData)
	mux.HandleFunc("/node_info", m.GetNodeInfo)
	mux.HandleFunc("/get_meta_data", m.GetMetaData)
	//mux.HandleFunc("/get_data", m.NodeRun)

	// Subject-related getters and setters
	//mux.HandleFunc("/delete_subject", )
	//mux.HandleFunc("/move_subject", )
	
	m._httpServer = &http.Server{
		Handler: mux,
	}

	err := m._httpServer.Serve(m._httpListener)
	if err != nil {
		logging.Error(err.Error())
		return err
	}
	return nil
}

// Returns network information and studies known to the network
func (m *Mux) Network(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if r.Method != http.MethodGet {
		http.Error(w, "Expected GET method", http.StatusMethodNotAllowed)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Mux's HTTP server running on port %d\n", m._httpPortNum)
	fmt.Fprintf(w, "Mux's HTTPS server running on port %d\n", m.portNum)
	fmt.Fprintf(w, "Flireflies nodes in this network:\nMux: %s\n", m.ifritClient.Addr())
	for nodeID, addr := range m.nodes {
		fmt.Fprintf(w, "String identifier: %s\tIP address: %s\n", nodeID, addr)
	}

	fmt.Fprintf(w, "Studies stored in the network:\n")
	for study, nodes := range m.studyToNode {
		fmt.Fprintf(w, "Study identifier: '%s'\tstorage node: ", study)
		for _, node := range nodes {
			fmt.Fprintf(w, "'%s' ", node)
		}
		fmt.Fprintf(w, "\n")
	}
}

// An end-point used to tell a node to generate data and associated policies that
// originate from a subject. The format of the JSON struct is pre-defined and may be subject to change in 
// the future.
func (m *Mux) StoreNodeData(w http.ResponseWriter, r *http.Request) {
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
		panic(err)
	}

	if err := m._storeNodeData(nodePopulator); err != nil {
		errMsg := fmt.Sprintf("Error: %s", err)
		http.Error(w, errMsg, http.StatusUnprocessableEntity)
		return
	}
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Created bulk data at node '%s'\n", nodePopulator.Node)
}

// Returns human-readable information about a particular node
func (m *Mux) GetNodeInfo(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if r.Method != http.MethodGet {
		http.Error(w, "Expected GET method", http.StatusMethodNotAllowed)
		return
	}

	if r.Header.Get("Content-type") != "application/json" {
		http.Error(w, "Require header to be application/json", http.StatusUnprocessableEntity)
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

	nodeInfo, err := m._getNodeInfo(msg.Node)
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

	statusCode, result, err := m._getMetaData(*msg)
	if err != nil {
		http.Error(w, err.Error(), statusCode)
		return
	}

	w.WriteHeader(statusCode)
	fmt.Fprintf(w, "Status code: %d\tresult: %s\n", statusCode, result)
}

// Given a node identifier and a study name, return the data at that node
// DUMMY IMPLEMENTATION
func (m *Mux) GetStudyData(w http.ResponseWriter, r *http.Request) {
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