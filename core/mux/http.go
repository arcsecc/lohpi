package mux

import (
	"net/http"
	"fmt"
	"io"
	"bytes"
	"encoding/json"
	"log"

	"firestore/core/message"

	logging "github.com/inconshreveable/log15"
)

func (m *Mux) HttpHandler() error {
	log.Printf("MUX: Started HTTP server on port %d\n", m._httpPortNum)
	mux := http.NewServeMux()

	// Public methods exposed to data users (usually through cURL)
	mux.HandleFunc("/network", m.Network)
	mux.HandleFunc("/populate_node", m.PopulateNode)
	mux.HandleFunc("/make_bulk_data", m.CreateBulkData)
	
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
	//	fmt.Fprintf(w, "IP address: %s\n", addr)
		fmt.Fprintf(w, "String identifier: %s\tIP address: %s\n", nodeID, addr)
	}
	
	/*
	defer r.Body.Close()
	if r.Method != http.MethodGet {
		http.Error(w, "Expected GET method", http.StatusMethodNotAllowed)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Mux's HTTP server running on port %d\n", m._httpPortNum)
	fmt.Fprintf(w, "Mux's HTTPS server running on port %d\n", m.portNum)
	fmt.Fprintf(w, "Flireflies nodes in this network:\nMux: %s\n", m.ifritClient.Addr())
	for n, addr := range m.nodes {
		fmt.Fprintf(w, "String identifier: %s\tIP address: %s\n", n, addr)
	}*/
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

		w.WriteHeader(http.StatusOK)
		str := fmt.Sprintf("Populated node %s\n", msg.Node)
		fmt.Fprintf(w, "%s", str)
	} else {
		w.WriteHeader(http.StatusNotFound)
		str := fmt.Sprintf("No such node: %s\n", msg.Node)
		fmt.Fprintf(w, "%s", str)
	}
}

func (m *Mux) CreateBulkData(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if r.Method != http.MethodPost {
		http.Error(w, "Expected POST method", http.StatusMethodNotAllowed)
		return
	}

	if r.Header.Get("Content-type") != "application/json" {
		http.Error(w, "Require header to be application/json", http.StatusUnprocessableEntity)
		return
	}

	var msg message.NodeMessage 
	var body bytes.Buffer
	io.Copy(&body, r.Body)
	var v interface{}

	err := json.Unmarshal(body.Bytes(), &v)
	if err != nil {
		panic(err)
	}

	data := v.(map[string]interface{})

	for k, v := range data {
		switch k {
			case message.Node:
				msg.Node = v.(string)
			case message.Subject:
				msg.Subject = v.(string)
			case message.Study:
				msg.Study = v.(string)
			case message.Required_attributes:
				// Check if the incoming collection of attributes is valid
				attrMap, ok := v.(map[string]interface{})
				if ok {
					fmt.Printf("k\tv: %v\t%v\n", k, v)
				} else {
					errMsg := fmt.Sprintf("Error: unknown attribute collection: %s", attrMap)
					http.Error(w, errMsg, http.StatusUnprocessableEntity)
					return
				}

				// Create the attributes collection to be used internally
				msg.Attributes = make(map[string]string)

				// Iterate the collection coming from the client.
				// Make sure that the type and the string value of the key are valid
				// Return error to the client if something fails
				for attrKey, attr := range attrMap {
					switch attrKey {
					
					// We might adjust the agility of the program here...
					case message.Country, message.Research_network, message.Purpose:
						attrValue, ok := attr.(string)
						if ok {
							msg.Attributes[attrKey] = attrValue
							continue
						}
					default:
						errMsg := fmt.Sprintf("Error: invalid attribute pair: %v\t%v", attrKey, attr)
						http.Error(w, errMsg, http.StatusUnprocessableEntity)
						return
					}
				}
			case message.Num_files:
				msg.NumFiles = v.(float64)
			case message.File_size:
				msg.FileSize = v.(float64)
			default:
				errMsg := fmt.Sprintf("Error: unknown key: %s", k)
				http.Error(w, errMsg, http.StatusUnprocessableEntity)
				return
		}
	}

	if !m.nodeExists(msg.Node) {
		errMsg := fmt.Sprintf("Error: unknown node: %s", msg.Node)
		http.Error(w, errMsg, http.StatusUnprocessableEntity)
		return
	}

	// Set the proper message type and marshall it to json
	msg.MessageType = message.MSG_TYPE_LOAD_NODE
	serialized, err := json.Marshal(msg)
	if err != nil {
		panic(err)
	}

	ch := m.ifritClient.SendTo(m.nodes[msg.Node], serialized)
	select {
		case resp := <- ch: 
			fmt.Printf("Response: %s\n", resp)
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Populated node OK")
}