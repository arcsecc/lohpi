package mux

import (
	"net/http"
	"fmt"
	"encoding/json"
	"log"

	logging "github.com/inconshreveable/log15"
)

func (m *Mux) HttpsHandler() error {
	log.Printf("MUX: Started HTTPS server on port %d\n", m.portNum)
	mux := http.NewServeMux()

	// Utilities used in experiments
	mux.HandleFunc("/set_port", m.SetPortNumber)
	mux.HandleFunc("/network", m.Network)
	mux.HandleFunc("/get_studies", m.GetStudies)
	//mux.HandleFunc("/get_study_data", m.GetStudyData)
	mux.HandleFunc("/join", m.Join)

	m.httpServer = &http.Server{
		Handler: mux,
		TLSConfig: m.serverConfig,
	}

	err := m.httpServer.ServeTLS(m.listener, "", "")
	if err != nil {
		logging.Error(err.Error())
		return err
	}
	return nil
}

func (m *Mux) Join(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if r.Method != http.MethodPost {
		http.Error(w, "Expected POST method", http.StatusMethodNotAllowed)
		return
	}

	if r.Header.Get("Content-type") != "application/json" {
		http.Error(w, "Require header to be application/json", http.StatusUnprocessableEntity)
	}

	var msg struct {
		Node 	string 		`json:"node"`
	}

	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(&msg)
    if err != nil {
		errMsg := fmt.Sprintf("Error kake\n")
		log.Printf("Error: %s\n", errMsg)
		http.Error(w, errMsg, http.StatusBadRequest)
		return
	}

	// Simply add it to the collection of nodes
	m.nodes[msg.Node] = ""

	// Return the mux's IP address
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "%s", m.ifritClient.Addr())
}

func (m *Mux) SetPortNumber(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if r.Method != http.MethodPost {
		http.Error(w, "Expected POST method", http.StatusMethodNotAllowed)
		return
	}

	if r.Header.Get("Content-type") != "application/json" {
		http.Error(w, "Require header to be application/json", http.StatusUnprocessableEntity)
	}

	var msg struct {
		Node 	string 		`json:"node"`
		Address string 		`json:"address"`
	}

	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(&msg)
    if err != nil {
		errMsg := fmt.Sprintf("Error kake\n")
		log.Printf("Error: %s\n", errMsg)
		http.Error(w, errMsg, http.StatusBadRequest)
		return
	}

	if _, ok := m.nodeProcs[msg.Node]; ok {
		m.nodes[msg.Node] = msg.Address
		w.WriteHeader(http.StatusOK)
		fmt.Printf("Added %s to map\n", msg.Node)
	} else {
		errMsg := fmt.Sprintf("No such node: %s\n", msg.Node)
		http.Error(w, errMsg, http.StatusNotFound)
	}
}

// Streams a list of studies available to the network. The studies are identified by
// strings. TODO: broadcast a pull to the network?
func (m *Mux) GetStudies(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if r.Method != http.MethodGet {
		http.Error(w, "Expected GET method", http.StatusMethodNotAllowed)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Studies known to the network\n--> ")
	for study := range m.studyNode {
		fmt.Fprintf(w, "%s ", study)
	}
}

// Given a study, GetStudyData returns a stream of files available to the 
// requester. The invoker 
/*func (m *Mux) GetStudyData(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if r.Method != http.MethodGet {
		http.Error(w, "Expected GET method", http.StatusMethodNotAllowed)
		return
	}

	//attrMap := map[string]string
	fmt.Printf("INFO: %s\n", r.TLS.PeerCertificates[0])
}*/
