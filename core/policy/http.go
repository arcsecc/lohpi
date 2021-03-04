package policy

import (
	"fmt"
	"bytes"
	"encoding/json"
	"context"
	"errors"
	log "github.com/sirupsen/logrus"
	pb "github.com/arcsecc/lohpi/protobuf"
	"net/http"
	"github.com/arcsecc/lohpi/core/util"
	"time"
	"github.com/gorilla/mux"
	"github.com/arcsecc/lohpi/core/comm"
	"strconv"
)

func (ps *PolicyStore) startHttpServer(addr string) error {
	router := mux.NewRouter()
	dRouter := router.PathPrefix("/dataset").Schemes("HTTP").Subrouter()
	dRouter.HandleFunc("/identifiers", ps.getDatasetIdentifiers).Methods("GET")
	dRouter.HandleFunc("/metadata/{id:.*}", ps.getDatasetMetadata).Methods("GET")
	dRouter.HandleFunc("/getpolicy/{id:.*}", ps.getObjectPolicy).Methods("GET")
	dRouter.HandleFunc("/setpolicy/{id:.*}", ps.setObjectPolicy).Methods("PUT")
	

	ps.httpServer = &http.Server{
		Addr: 		  	addr,
		Handler:      	router,
		WriteTimeout: 	time.Second * 30,
		ReadTimeout:  	time.Second * 30,
		IdleTimeout:  	time.Second * 60,
		TLSConfig: 		comm.ServerConfig(ps.cu.Certificate(), ps.cu.CaCertificate(), ps.cu.Priv()),
	}

	return ps.httpServer.ListenAndServe()
}

// Returns the dataset identifiers stored in the network
func (ps *PolicyStore) getDatasetIdentifiers(w http.ResponseWriter, r *http.Request) {
	log.Infoln("Got request to", r.URL.String())	
	defer r.Body.Close()

	respBody := struct {
		Identifiers []string 
	}{
		Identifiers: make([]string, 0),
	}

	for i := range ps.getDatasetPolicyMap() {
		respBody.Identifiers = append(respBody.Identifiers, i)
	}

	b := new(bytes.Buffer)
	if err := json.NewEncoder(b).Encode(respBody); err != nil {
		log.Errorln(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	r.Header.Add("Content-Length", strconv.Itoa(len(b.Bytes())))

	_, err := w.Write(b.Bytes())
	if err != nil { 
		log.Errorln(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError) + ": " + err.Error(), http.StatusInternalServerError)
		return
	}
}

// Returns the policy associated with the dataset
func (ps *PolicyStore) getObjectPolicy(w http.ResponseWriter, r *http.Request) {
	log.Infoln("Got request to", r.URL.String())	
	defer r.Body.Close()

	datasetId := mux.Vars(r)["id"]
	if datasetId == "" {
		err := errors.New("Missing dataset identifier")
		log.Infoln(err.Error())
		http.Error(w, http.StatusText(http.StatusBadRequest) + ": " + err.Error(), http.StatusBadRequest)
		return
	}

	// Get the node that stores the dataset
	datasetEntry, exists := ps.getDatasetPolicyMap()[datasetId]
	if !exists {
		err := fmt.Errorf("Dataset '%s' was not found", datasetId)
		log.Infoln(err.Error())
		http.Error(w, http.StatusText(http.StatusNotFound) + ": " + err.Error(), http.StatusNotFound)
		return
	}

	// Destination struct
	resp := struct  {
		Policy string
	}{}

	resp.Policy = datasetEntry.policy.GetContent()

	b := new(bytes.Buffer)
	if err := json.NewEncoder(b).Encode(resp); err != nil {
		log.Errorln(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	r.Header.Add("Content-Length", strconv.Itoa(len(b.Bytes())))

	_, err := w.Write(b.Bytes())
	if err != nil { 
		log.Errorln(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError) + ": " + err.Error(), http.StatusInternalServerError)
		return
	}
}

// Returns the metadata assoicated with the dataset
func (ps *PolicyStore) getDatasetMetadata(w http.ResponseWriter, r *http.Request) {
	log.Infoln("Got request to", r.URL.String())
	defer r.Body.Close()
	
	err := errors.New("getDatasetMetadata is not implemented")
	log.Warnln(err.Error())
	http.Error(w, http.StatusText(http.StatusNotImplemented) + ": " + err.Error(), http.StatusNotImplemented)
}

// Assigns a new policy to the dataset
func (ps *PolicyStore) setObjectPolicy(w http.ResponseWriter, r *http.Request) {
	log.Infoln("Got request to", r.URL.String())	
	defer r.Body.Close()

	datasetId := mux.Vars(r)["id"]
	if datasetId == "" {
		err := errors.New("Missing dataset identifier")
		log.Infoln(err.Error())
		http.Error(w, http.StatusText(http.StatusBadRequest) + ": " + err.Error(), http.StatusBadRequest)
		return
	}

	// Get the node that stores the dataset
	datasetEntry, exists := ps.getDatasetPolicyMap()[datasetId]
	if !exists {
		err := fmt.Errorf("Dataset '%s' was not found", datasetId)
		log.Infoln(err.Error())
		http.Error(w, http.StatusText(http.StatusNotFound) + ": " + err.Error(), http.StatusNotFound)
		return
	}

	// Destination struct
	reqBody := struct  {
		Policy string
	}{}

	if err := util.DecodeJSONBody(w, r, "application/json", &reqBody); err != nil {
		var e *util.MalformedParserReponse
		if errors.As(err, &e) {
			log.Errorln(err.Error())
			http.Error(w, e.Msg, e.Status)
		} else {
			log.Errorln(err.Error())
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		}
		return
	}

	policy := &pb.Policy{
		Issuer: ps.name,
		ObjectIdentifier: datasetId,
		Content: reqBody.Policy,
	}

	ps.storePolicy(context.Background(), datasetEntry.node, datasetId, policy) // if err...

	respMsg := "Successfully set a new policy for " + datasetId + "\n"
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/text")
	w.Header().Set("Content-Length", strconv.Itoa(len(respMsg)))
	w.Write([]byte(respMsg))
}