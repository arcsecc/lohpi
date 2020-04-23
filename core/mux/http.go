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
	mux.HandleFunc("/load_node", m.CreateBulkData)
	mux.HandleFunc("/node_info", m.NodeInfo)

	// TMP function: used to trigger different functionalities in Lohpi
	mux.HandleFunc("/node_run", m.NodeRun)


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

	// Used to prepare data packets to be sent to the node
	var msg message.NodeMessage 
	var body bytes.Buffer
	io.Copy(&body, r.Body)
	//var v interface{}

	// Unmarshal the request body from the client
	/*err := json.Unmarshal(body.Bytes(), &v)
	if err != nil {
		panic(err)
	}*/

	// Client data is stored by 'data'. We need to use interface{} because we don't know 
	// the format of the data
	//data := v.(map[string]interface{})
	data := body.Bytes()

	nodePopulator, err := message.NewNodePopulator(data)
	if err != nil {
		panic(err)
	}

	if err := m.StoreNodeData(nodePopulator); err != nil {
		errMsg := fmt.Sprintf("Error: %s", err)
		http.Error(w, errMsg, http.StatusUnprocessableEntity)
		return
	}
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Created bulk data at node '%s'\n", msg.Node)
}

// Returns human-readable information about a particular node
func (m *Mux) NodeInfo(w http.ResponseWriter, r *http.Request) {
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

	nodeInfo, err := m.GetNodeInfo(msg.Node)
	if err != nil {
		errMsg := fmt.Sprintf("Error: %s\n", err)
		log.Printf("%s", errMsg)
		http.Error(w, errMsg, http.StatusBadRequest)
		return
	}
	fmt.Fprintf(w, nodeInfo)
}

// DUMMY TEMP FUNC used to test different functionalities. only used in development
func (m *Mux) NodeRun(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if r.Method != http.MethodPost {
		http.Error(w, "Expected GET method", http.StatusMethodNotAllowed)
		return
	}

	if r.Header.Get("Content-type") != "application/json" {
		http.Error(w, "Require header to be application/json", http.StatusUnprocessableEntity)
		return
	}

	// Used to prepare data packets to be sent to the node
	var msg message.NodeMessage 
	var body bytes.Buffer
	io.Copy(&body, r.Body)
	var v interface{}

	// Unmarshal the request body from the client
	err := json.Unmarshal(body.Bytes(), &v)
	if err != nil {
		panic(err)
	}

	// Client data is stored by 'data'. We need to use interface{} because we don't know 
	// the format of the data
	data := v.(map[string]interface{})

	// Iterate the collection of key-value pairs comming from the client.
	// As of now, we require a pre-defined format of the bulk data generator parameters.
	// This may be subject to change in the future.
	for key, value := range data {

		// The key needs to be spelled correctly. Use type assertion as well
		// to ensure correctness
		switch key {
			case message.Node:
				msg.Node = value.(string)
			case message.Study:
				msg.Study = value.(string)
			// 'required_attributes' is a map[string]string type.
			case message.Required_attributes:

				// Check if the incoming collection of attributes is invalid. If they are,
				// return an error message to the client
				attrMap, ok := value.(map[string]interface{})
				if !ok {
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
			default:
				errMsg := fmt.Sprintf("Error: unknown key: %s", key)
				http.Error(w, errMsg, http.StatusUnprocessableEntity)
				return
		}
	}

	statusCode, result, err := m.GetStudyDataFromNode(msg)
	if err != nil {
		http.Error(w, err.Error(), statusCode)
		return
	}

	w.WriteHeader(statusCode)
	fmt.Fprintf(w, "%s\n", result)
}