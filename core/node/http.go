package node

import (
	"bytes"
	"encoding/json"
	"strconv"
	"errors"
	log "github.com/sirupsen/logrus"
	"net/http"
	"fmt"
	"time"
	
	"github.com/arcsecc/lohpi/core/util"
	"github.com/gorilla/mux"
	"github.com/arcsecc/lohpi/core/comm"
)

const PROJECTS_DIRECTORY = "projects"

func (n *Node) startHttpServer(addr string) error {
	router := mux.NewRouter()
	log.Infof("%s: Started HTTP server on port %s\n", n.name, addr)

	dRouter := router.PathPrefix("/dataset").Schemes("HTTP").Subrouter()
	dRouter.HandleFunc("/ids", n.getDatasetIdentifiers).Methods("GET")

	// Combine these two guys into the same method
	dRouter.HandleFunc("/info/{id:.*}", n.getDatasetSummary).Methods("GET")
	//dRouter.HandleFunc("/metadata/{id:.*}", n.getDatasetSummary).Methods("GET")
	dRouter.HandleFunc("/new_policy/{id:.*}", n.setDatasetPolicy).Methods("PUT")

	// Middlewares used for validation
	//dRouter.Use(m.middlewareValidateTokenSignature)
	//dRouter.Use(m.middlewareValidateTokenClaims)

	n.httpServer = &http.Server{
		Addr: 		  	addr,
		Handler:      	router,
		WriteTimeout: 	time.Second * 30,
		ReadTimeout:  	time.Second * 30,
		IdleTimeout:  	time.Second * 60,
		TLSConfig: 		comm.ServerConfig(n.cu.Certificate(), n.cu.CaCertificate(), n.cu.Priv()),
	}

	return n.httpServer.ListenAndServe()
}

// Returns the dataset identifiers stored at this node
func (n *Node) getDatasetIdentifiers(w http.ResponseWriter, r *http.Request) {
	log.Infoln("Got request to", r.URL.String())
	defer r.Body.Close()

	ids := n.datasetIdentifiers()
	b := new(bytes.Buffer)
	if err := json.NewEncoder(b).Encode(ids); err != nil {
		log.Errorln(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	r.Header.Add("Content-Length", strconv.Itoa(len(ids)))

	_, err := w.Write(b.Bytes())
	if err != nil { 
		log.Errorln(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError) + ": " + err.Error(), http.StatusInternalServerError)
		return
	}
}

// Returns a JSON object containing the metadata assoicated with a dataset
func (n *Node) getDatasetSummary(w http.ResponseWriter, r *http.Request) {
	log.Infoln("Got request to", r.URL.String())
	defer r.Body.Close()

	dataset := mux.Vars(r)["id"]
	if dataset == "" {
		err := errors.New("Missing dataset identifier")
		log.Infoln(err.Error())
		http.Error(w, http.StatusText(http.StatusBadRequest) + ": " + err.Error(), http.StatusBadRequest)
		return
	}

	if !n.dbDatasetExists(dataset) {
		err := fmt.Errorf("Dataset '%s' is not indexed by the server", dataset)
		log.Infoln(err.Error())
		http.Error(w, http.StatusText(http.StatusNotFound) + ": " + err.Error(), http.StatusNotFound)
		return 
	}

	// Fetch the policy
	policy, err := n.dbGetObjectPolicy(dataset)
	if err != nil {
		log.Errorln(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return 
	}

	// Fetch the clients that have checked out the dataset
	checkouts, err := n.dbGetCheckoutList(dataset)
	if err != nil {
		log.Errorln(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	// Destination struct
	resp := struct  {
		Dataset string
		Policy string
		Checkouts []CheckoutInfo
	}{
		Dataset: dataset,
		Policy: policy,
		Checkouts: checkouts,
	}

	b := new(bytes.Buffer)
	if err := json.NewEncoder(b).Encode(resp); err != nil {
		log.Errorln(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	r.Header.Add("Content-Length", strconv.Itoa(len(b.Bytes())))

	_, err = w.Write(b.Bytes())
	if err != nil { 
		log.Errorln(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError) + ": " + err.Error(), http.StatusInternalServerError)
		return
	}
}

/* Assigns a new policy to the dataset. The request body must be a JSON object with a string similar to this:
 *{
 *	"policy": true/false	
 *}
 */
// TODO: restrict this function only to be used to set the initial dataset policy.
// SHOULD NOT BE USED
func (n *Node) setDatasetPolicy(w http.ResponseWriter, r *http.Request) { 
	log.Infoln("Got request to", r.URL.String())
	defer r.Body.Close()

	// Enforce application/json MIME type
	if r.Header.Get("Content-Type") != "application/json" {
        msg := "Content-Type header is not application/json"
        http.Error(w, http.StatusText(http.StatusUnsupportedMediaType) + ": " + msg, http.StatusUnsupportedMediaType)
		return
	}

	// Fetch the dataset if
	dataset := mux.Vars(r)["id"]
	if dataset == "" {
		err := errors.New("Missing dataset identifier")
		log.Infoln(err.Error())
		http.Error(w, http.StatusText(http.StatusBadRequest) + ": " + err.Error(), http.StatusBadRequest)
		return
	}

	// Check if dataset exists in the external repository
	if !n.datasetExists(dataset) {
		err := fmt.Errorf("Dataset '%s' is not stored in this node", dataset)
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

	if err := n.dbSetObjectPolicy(dataset, reqBody.Policy); err != nil {
		log.Warnln(err.Error())
	}

	respMsg := "Successfully set a new policy for " + dataset + "\n"
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/text")
	w.Header().Set("Content-Length", strconv.Itoa(len(respMsg)))
	w.Write([]byte(respMsg))
}

