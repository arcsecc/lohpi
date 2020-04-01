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