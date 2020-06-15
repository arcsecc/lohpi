package mux

import (
	"net/http"
	"fmt"
	"encoding/json"
	"log"

	"github.com/gorilla/mux"
	logging "github.com/inconshreveable/log15"
)

func (m *Mux) HttpsHandler() error {
	log.Printf("MUX: Started HTTPS server on port %d\n", m.portNum)
	r := mux.NewRouter()

	// Utilities used in experiments
	r.HandleFunc("/set_port", m.SetPortNumber)
	r.HandleFunc("/network", m.network)
	r.HandleFunc("/get_studies", m.GetStudies)
	//mux.HandleFunc("/get_study_data", m.GetStudyData)

	m.httpServer = &http.Server{
		Handler: r,
		TLSConfig: m.serverConfig,
	}

	err := m.httpServer.ServeTLS(m.listener, "", "")
	if err != nil {
		logging.Error(err.Error())
		return err
	}
	return nil
}

// Handshake endpoint for nodes to join the network
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
		errMsg := fmt.Sprintf("Error: could not decode node handshake")
		http.Error(w, errMsg, http.StatusBadRequest)
		return
	}

	if !m.cache.NodeExists(msg.Node) {
		m.cache.UpdateNodes(msg.Node, msg.Address)
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "%s", m.ifritClient.Addr()) 
	} else {
		errMsg := fmt.Sprintf("Node '%s' already exists in network\n", msg.Node)
		http.Error(w, errMsg, http.StatusBadRequest)
	}
	log.Printf("Added %s to map with IP %s\n", msg.Node, msg.Address)
}

// Streams the newest list of studies available to the network
func (m *Mux) GetStudies(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if r.Method != http.MethodGet {
		http.Error(w, "Expected GET method", http.StatusMethodNotAllowed)
		return
	}

	m.cache.FetchRemoteStudyLists()

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Studies known to the network\n--> ")
	for study := range m.cache.Studies() {
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
