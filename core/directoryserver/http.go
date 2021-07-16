package directoryserver

import (
	"github.com/rs/cors"
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/arcsecc/lohpi/core/comm"
	"github.com/arcsecc/lohpi/core/util"
	"github.com/gorilla/mux"
	"github.com/lestrrat-go/jwx/jwa"
	"github.com/lestrrat-go/jwx/jwk"
	"github.com/lestrrat-go/jwx/jws"
	log "github.com/sirupsen/logrus"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	pb "github.com/arcsecc/lohpi/protobuf"
	pbtime "google.golang.org/protobuf/types/known/timestamppb"
	"time"
	"sort"
)

type checkoutInfo struct {
	ClientId string	
	ClientName string
	MacAddress string
	EmailAddress string
	Timestamp string
	Dataset string
}

type azureConfig struct {
	appId  string // Validate me!
	issuer string // Validate me?
	nonce  string // Validate me!
	roles  []string
}

func (d *DirectoryServerCore) startHttpServer(addr string) error {
	r := mux.NewRouter()
	log.Infoln("Started directory server on port", d.config.HTTPPort)

	// Main dataset router exposed to the clients
	dRouter := r.PathPrefix("/dataset").Schemes("HTTP").Subrouter().SkipClean(false)
	dRouter.HandleFunc("/ids", d.getNetworkDatasetIdentifiers).Methods("GET", "OPTIONS")
	dRouter.HandleFunc("/metadata/{id:.*}", d.getDatasetMetadata).Methods("GET")
	dRouter.HandleFunc("/data/{id:.*}", d.getDataset).Methods("GET")
	dRouter.HandleFunc("/verify/{id:.*}", d.getDatasetPolicyVerification).Methods("GET")
	dRouter.HandleFunc("/set_project_description/{id:.*}", d.setProjectDescription).Methods("POST")
	dRouter.HandleFunc("/get_project_description/{id:.*}", d.getProjectDescription).Methods("GET")
	dRouter.HandleFunc("/node_ids", d.nodeIds).Methods("GET")
	dRouter.HandleFunc("/num_nodes", d.numNodes).Methods("GET")
	dRouter.HandleFunc("/node_info/{id:.*}", d.nodeInfo).Methods("GET")
	dRouter.HandleFunc("/checkouts/{id:.*}", d.datasetCheckouts).Methods("GET")
	dRouter.HandleFunc("/num_checkouts/{id:.*}", d.numCheckouts).Methods("GET")
	dRouter.HandleFunc("/num_datasets", d.numDatasets).Methods("GET")
	
	handler := cors.AllowAll().Handler(r)

//	networkRouter := r.PathPrefix("/network").Schemes("HTTP").Subrouter()

	// Middlewares used for validation
	//dRouter.Use(d.middlewareValidateTokenSignature)
	//dRouter.Use(d.middlewareValidateTokenClaims)

	d.httpServer = &http.Server{
		Addr:         addr,
		Handler:      handler,
		WriteTimeout: time.Hour * 1,
		//ReadTimeout:  time.Second * 30,
		//IdleTimeout:  time.Second * 60,
		TLSConfig: comm.ServerConfig(d.cm.Certificate(), d.cm.CaCertificate(), d.cm.PrivateKey()),
	}

	if err := d.setPublicKeyCache(); err != nil {
		log.Errorln(err.Error())
		return err
	}

	return d.httpServer.ListenAndServe()
}
// Gets number of datasets available
func (d *DirectoryServerCore) numDatasets(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	//	ctx, cancel := context.WithDeadline(r.Context(), time.Now().Add(time.Second * 10))
	//	defer cancel()

	// Destination struct
	
	var resp int = 0
	resp = len(d.networkService.DatasetIdentifiers())
	
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
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+err.Error(), http.StatusInternalServerError)
		return
	}
}

func (d *DirectoryServerCore) numCheckouts(w http.ResponseWriter, r *http.Request) {
	dataset := mux.Vars(r)["id"]	
	arr, err := d.dsManager.DatasetCheckouts()
	if err != nil {
		errMsg := fmt.Errorf("Checkout error")
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+errMsg.Error(), http.StatusInternalServerError)
		return
	}
	counter := 0
	for _, checkout := range arr {
		if checkout.DatasetIdentifier == dataset {
			counter += 1
		}
	}
	// Response : number of checkouts on this dataset
	resp := counter

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
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+err.Error(), http.StatusInternalServerError)
		return
	}

}

func (d *DirectoryServerCore) datasetCheckouts(w http.ResponseWriter, r *http.Request) {
	// Get URL params
	keys := r.URL.Query()
	indexStart, _ := strconv.Atoi(keys.Get("index_start"))
	indexRange, _ := strconv.Atoi(keys.Get("index_range"))

	dataset := mux.Vars(r)["id"]	
	arr, err := d.dsManager.DatasetCheckouts()
	if err != nil {
		errMsg := fmt.Errorf("Checkout error")
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+errMsg.Error(), http.StatusInternalServerError)
		return
	}

	res := make([]*checkoutInfo, 0)

	for _, checkout := range arr {
		if checkout.DatasetIdentifier == dataset {
			elem := &checkoutInfo{
				ClientId: checkout.GetClient().GetID(),
				ClientName: checkout.GetClient().GetName(),
				MacAddress:	checkout.GetClient().GetMacAddress(),
				EmailAddress: checkout.GetClient().GetEmailAddress(),
				Timestamp: checkout.GetDateCheckout().AsTime().Format("2006-01-02 15:04:05"),
				Dataset: checkout.GetDatasetIdentifier(),
			}
			res = append(res, elem)
		}
	}
	// Sort by name
	sort.Slice(res, func(i, j int) bool {
		return res[i].ClientName < res[j].ClientName
	})

	// Adjust slice range
	if len(res) < (indexStart + indexRange){
		indexRange = len(res) - indexStart
	}

	// Create slice of slice
	if indexRange != 0 {
		res = res[indexStart : (indexStart + indexRange)]
	}

	resp := struct {
		Arr []*checkoutInfo `json:"checkout_info"`
	}{
		Arr: res,
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
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+err.Error(), http.StatusInternalServerError)
		return
	}

}

func (d *DirectoryServerCore) nodeInfo(w http.ResponseWriter, r *http.Request) {
	// Destination struct
	resp := struct {
		Identifiers []string
		NumDataset int
		IpAddr string
		Uptime string
		LastJoined string	
	}{
		Identifiers: make([]string, 0),
	}

	nodeId := mux.Vars(r)["id"]
	if nodeId == "" {
		errMsg := fmt.Errorf("Missing node identifier")
		http.Error(w, http.StatusText(http.StatusBadRequest)+": "+errMsg.Error(), http.StatusBadRequest)
		return
	}

	// Loops through dataset-node mapping to set the counter and the list of datasets in node
	counter := 0
	for id, node := range d.networkService.DatasetNodes() {
		if node.GetName() == nodeId {
			counter += 1
			resp.Identifiers = append(resp.Identifiers, id)
		}
	}
	resp.NumDataset = counter

	// Gets the rest of the updated data from the node map itself
	for _, node := range d.memManager.NetworkNodes() {
		if node.GetName() == nodeId {
			resp.IpAddr = node.GetIfritAddress()
			resp.LastJoined = node.GetBootTime().AsTime().Format("2006-01-02 15:04:05")
			resp.Uptime = time.Now().Sub(node.GetBootTime().AsTime()).Round(time.Second).String() // Subs 'last joined' from 'time.Now()', rounds it to 'seconds' minimum, and returns a string
		}
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

	_, err := w.Write(b.Bytes())
	if err != nil {
		log.Errorln(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+err.Error(), http.StatusInternalServerError)
		return
	}

}
// returns all nodes
func (d *DirectoryServerCore) numNodes(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	// Destination struct
	resp := 0
	for _ = range d.memManager.NetworkNodes() {
		resp += 1
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

	_, err := w.Write(b.Bytes())
	if err != nil {
		log.Errorln(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+err.Error(), http.StatusInternalServerError)
		return
	}
}

// returns all nodes
func (d *DirectoryServerCore) nodeIds(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	// Gets the url query parameters if there are any
	keys := r.URL.Query()
	indexStart, _ := strconv.Atoi(keys.Get("index_start"))
	indexRange, _ := strconv.Atoi(keys.Get("index_range"))

	// Destination struct
	resp := struct {
		Identifiers []string
	}{
		Identifiers: make([]string, 0),
	}
	idSlice := make([]string, 0)
	for id := range d.memManager.NetworkNodes() {
		idSlice = append(resp.Identifiers, id)
	}
	sort.Strings(idSlice)

	if len(idSlice) < (indexStart + indexRange){
		indexRange = len(idSlice) - indexStart
	}
	if indexRange != 0 {
		idSlice = idSlice[indexStart : (indexStart + indexRange)]
	}
	resp.Identifiers = idSlice

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
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+err.Error(), http.StatusInternalServerError)
		return
	}
}


// Sets project description for the dataset given as 'id'
func (d *DirectoryServerCore) setProjectDescription(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	/* Front end needs to send in json object with project description, with application/json in header */
	clientReq := struct {
		ProjectDescription string `json:"project_description"`
	}{}

	if err := util.DecodeJSONBody(w, r, "application/json", &clientReq); err != nil {
		log.Errorln(err.Error())
		http.Error(w, http.StatusText(http.StatusBadRequest)+": "+err.Error(), http.StatusBadRequest)
		return
	}

	dataset := mux.Vars(r)["id"]
	if dataset == "" {
		errMsg := fmt.Errorf("Missing dataset identifier")
		http.Error(w, http.StatusText(http.StatusBadRequest)+": "+errMsg.Error(), http.StatusBadRequest)
		return
	}

	// Check if dataset is known to network
	if !d.networkService.DatasetNodeExists(dataset) {
		err := fmt.Errorf("Dataset '%s' is not stored in the network", dataset)
		log.Infoln(err.Error())
		http.Error(w, http.StatusText(http.StatusNotFound)+": "+err.Error(), http.StatusNotFound)
		return
	}
	
	// Project description as argument to updatePD, string?
	if err := d.updateProjectDescription(dataset, clientReq.ProjectDescription); err != nil {
		log.Errorln(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError) + ": Failed to update project description", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w,"Successfully updated description of dataset %s\n", dataset)

}

// Gets project description form the dataset given as 'id'
func (d *DirectoryServerCore) getProjectDescription(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	dataset := mux.Vars(r)["id"]
	if dataset == "" {
		errMsg := fmt.Errorf("Missing dataset identifier")
		http.Error(w, http.StatusText(http.StatusBadRequest)+": "+errMsg.Error(), http.StatusBadRequest)
		return
	}

	// Check if dataset is known to network
	if !d.networkService.DatasetNodeExists(dataset) {
		err := fmt.Errorf("Dataset '%s' is not stored in the network", dataset)
		log.Infoln(err.Error())
		http.Error(w, http.StatusText(http.StatusNotFound)+": "+err.Error(), http.StatusNotFound)
		return
	}

	// Project description as argument to updatePD, string?
	pd, err := d.getProjectDescriptionDB(dataset)
	if err != nil {
		panic(err)
	}

	response := struct {
		ProjectDescription string `json:"project_description"`
	}{
		ProjectDescription: pd,
	}

	b := new(bytes.Buffer)
	if err := json.NewEncoder(b).Encode(response); err != nil {
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
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+err.Error(), http.StatusInternalServerError)
		return
	}
}

func (d *DirectoryServerCore) setPublicKeyCache() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d.pubKeyCache = jwk.NewAutoRefresh(ctx)
	const msCerts = "https://login.microsoftonline.com/common/discovery/v2.0/keys" // TODO: config me

	d.pubKeyCache.Configure(msCerts, jwk.WithRefreshInterval(time.Minute*5))

	// Keep the cache warm
	_, err := d.pubKeyCache.Refresh(ctx, msCerts)
	if err != nil {
		log.Println("Failed to refresh Microsoft Azure JWKS")
		return err
	}
	return nil
}

func (d *DirectoryServerCore) middlewareValidateTokenSignature(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, err := getBearerToken(r)
		if err != nil {
			http.Error(w, http.StatusText(http.StatusBadRequest)+": "+err.Error(), http.StatusBadRequest)
			return
		}

		if err := d.validateTokenSignature(token); err != nil {
			http.Error(w, http.StatusText(http.StatusBadRequest)+": "+err.Error(), http.StatusBadRequest)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func (d *DirectoryServerCore) middlewareValidateTokenClaims(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// TODO: parse the claims in the access token
		// Return error if there are any mismatches. Call next.ServeHTTP(w, r) otherwise
		log.Println("in middlewareValidateTokenClaims")

		next.ServeHTTP(w, r)
	})
}

func (d *DirectoryServerCore) validateTokenSignature(token []byte) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// TODO: fetch keys again if it fails
	// TODO: add clock skew. Use jwt.WithAcceptableSkew
	set, err := d.pubKeyCache.Fetch(ctx, "https://login.microsoftonline.com/common/discovery/v2.0/keys")
	if err != nil {
		return err
	}

	// Note: see https://github.com/lestrrat-go/jwx/blob/a7f076fc6eadb44380d41b5e30eb5a85a91de864/jws/jws.go#L186
	// There is no guarantee that the algorithm is RS256
	for it := set.Iterate(context.Background()); it.Next(context.Background()); {
		_, err := jws.Verify(token, jwa.RS256, it.Pair().Value)
		if err == nil {
			return nil
		}
	}

	// Fetch the public keys again from URL if the verification failed.
	// Fetching it again will guarantee a best-effort to verify the request.
	/*ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// This doesn't work!
	// TODO: this doesn't work
	set, err := d.ar.Refresh(ctx, msCerts)
	if err != nil {
		log.Println("Failed to refresh Microsoft Azure JWKS")
		return err
	}

	// Note: see https://github.com/lestrrat-go/jwx/blob/a7f076fc6eadb44380d41b5e30eb5a85a91de864/jws/jws.go#L186
	// There is no guarantee that algorithm is RS256
	for it := set.Iterate(context.Background()); it.Next(context.Background()); {
		_, err := jws.Verify(token, jwa.RS256, it.Pair().Value)
		if err == nil {
			return nil
		}
	}*/

	return errors.New("Could not verify token")
}

func getBearerToken(r *http.Request) ([]byte, error) {
	authHeader := r.Header.Get("Authorization")
	authHeaderContent := strings.Split(authHeader, " ")
	if len(authHeaderContent) != 2 {
		return nil, errors.New("Token not provided or malformed")
	}
	return []byte(authHeaderContent[1]), nil
}

func (d *DirectoryServerCore) shutdownHttpServer() {
	// The duration for which the server wait for open connections to finish
	wait := time.Minute * 1
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()

	// Doesn't block if no connections, but will otherwise wait
	// until the timeout deadline.
	d.httpServer.Shutdown(ctx)

	// Optionally, you could run srv.Shutdown in a goroutine and block on
	// <-ctx.Done() if your application should wait for other services
	// to finalize based on context cancellation.
	log.Println("Gracefully shutting down HTTP server")
}

// Lazily fetch objects from all the nodes
func (d *DirectoryServerCore) getNetworkDatasetIdentifiers(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	
	// Gets the url query parameters if there are any
	keys := r.URL.Query()
	indexStart, _ := strconv.Atoi(keys.Get("index_start"))
	indexRange, _ := strconv.Atoi(keys.Get("index_range"))
	//	ctx, cancel := context.WithDeadline(r.Context(), time.Now().Add(time.Second * 10))
	//	defer cancel()

	identifierSlice := d.networkService.DatasetIdentifiers()
	sort.Strings(identifierSlice)

	if len(identifierSlice) < (indexStart + indexRange){
		indexRange = len(identifierSlice) - indexStart
	}
	if indexRange != 0 {
		identifierSlice = identifierSlice[indexStart : (indexStart + indexRange)]
	}

	// Destination struct
	resp := struct {
		Identifiers []string
	}{
		Identifiers: identifierSlice, 
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

	_, err := w.Write(b.Bytes())
	if err != nil {
		log.Errorln(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+err.Error(), http.StatusInternalServerError)
		return
	}
}

// Fetches the information about a dataset
func (d *DirectoryServerCore) getDatasetMetadata(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	dataset := mux.Vars(r)["id"]
	if dataset == "" {
		errMsg := fmt.Errorf("Missing dataset identifier")
		http.Error(w, http.StatusText(http.StatusBadRequest)+": "+errMsg.Error(), http.StatusBadRequest)
		return
	}

	/*ctx, cancel := context.WithDeadline(r.Context(), time.Now().Add(time.Second*10))
	defer cancel()*/

	// Check if dataset is known to network
	if !d.networkService.DatasetNodeExists(dataset) {
		err := fmt.Errorf("Dataset '%s' is not stored in the network", dataset)
		log.Infoln(err.Error())
		http.Error(w, http.StatusText(http.StatusNotFound)+": "+err.Error(), http.StatusNotFound)
		return
	}

	node := d.networkService.DatasetNode(dataset)
	if node == nil {
		err := fmt.Errorf("The network node that stores the dataset '%s' is not available", dataset)
		log.Infoln(err.Error())
		http.Error(w, http.StatusText(http.StatusGone)+": "+err.Error(), http.StatusGone)
		return
	}

	req := &http.Request{
		Method: "GET",
		URL: &url.URL{
			Scheme: "http", //https
			Host:   node.GetHttpsAddress() + ":" + strconv.Itoa(int(node.GetPort())),
			Path:   "/dataset/metadata/" + dataset,
		},
		Header: http.Header{},
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		err := fmt.Errorf("Service is not available")
		log.Infoln(err.Error())
		http.Error(w, http.StatusText(http.StatusServiceUnavailable)+": "+err.Error(), http.StatusServiceUnavailable)
		return
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Error(resp.Status)
		http.Error(w, http.StatusText(resp.StatusCode)+": "+resp.Status, resp.StatusCode)
		return
	}

	reader := bufio.NewReader(resp.Body)
	if err := util.StreamToResponseWriter(reader, w, 100*1024); err != nil {
		log.Errorln(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+err.Error(), http.StatusInternalServerError)
		return
	}
}

// Handler used to fetch an entire dataset. Writes a zip file to the client
func (d *DirectoryServerCore) getDataset(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	// Reject request at directoryu server if token is invalid
	token, err := getBearerToken(r)
	if err != nil {
		log.Infoln(err.Error())
		http.Error(w, http.StatusText(http.StatusBadRequest)+": "+err.Error(), http.StatusBadRequest)
		return
	}

	// Get dataset identifier
	dataset := mux.Vars(r)["id"]

	// Get the node that stores it
	if !d.networkService.DatasetNodeExists(dataset) {
		err := fmt.Errorf("Dataset '%s' is not stored in the network", dataset)
		log.Infoln(err.Error())
		http.Error(w, http.StatusText(http.StatusNotFound)+": "+err.Error(), http.StatusNotFound)
		return
	}

	node := d.networkService.DatasetNode(dataset)
	if node == nil {
		err := fmt.Errorf("The network node that stores the dataset '%s' is not available", dataset)
		log.Infoln(err.Error())
		http.Error(w, http.StatusText(http.StatusGone)+": "+err.Error(), http.StatusGone)
		return
	}

	pbClient, err := jwtTokenToPbClient(string(token))
	if err != nil {
		log.Infoln(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError) + ": " + err.Error(), http.StatusInternalServerError)
		return	
	}

	// Prepare the request. Beginning of pipeline
	req := &http.Request{
		Method: "GET",
		URL: &url.URL{
			Scheme: "http", //https
			Host:   node.GetHttpsAddress() + ":" + strconv.Itoa(int(node.GetPort())),
			Path:   "/dataset/data/" + dataset,
		},
		Header: http.Header{},
	}

	req.Header.Add("Authorization", "Bearer "+string(token))

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		err := fmt.Errorf("Failed to fetch dataset\n")
		log.Infoln(err.Error())
		http.Error(w, http.StatusText(http.StatusBadRequest)+": "+err.Error(), http.StatusBadRequest)
		return
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Errorln(resp.Status + ": " + resp.Status)
		http.Error(w, http.StatusText(resp.StatusCode)+": "+resp.Status, resp.StatusCode)
		return
	}

	reader := bufio.NewReader(resp.Body)
	if err := util.StreamToResponseWriter(reader, w, 1000*1024); err != nil {
		log.Errorln(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+err.Error(), http.StatusInternalServerError)
		return
	}

	dsCheckout := &pb.DatasetCheckout{
		DatasetIdentifier: dataset,
    	DateCheckout: pbtime.Now(),
    	Client: pbClient,
	}

	if err := d.dsManager.CheckoutDataset(dataset, dsCheckout); err != nil {
		log.Errorln(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+err.Error(), http.StatusInternalServerError)
		return
	}
}

func (d *DirectoryServerCore) getDatasetPolicyVerification(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	dataset := mux.Vars(r)["id"]
	isInvalidated := d.datasetIsInvalidated(dataset)
	b := new(bytes.Buffer)
	c := struct {
		IsInvalidated bool
	}{
		IsInvalidated: isInvalidated,
	}

	if err := json.NewEncoder(b).Encode(c); err != nil {
		log.Errorln(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Add("Content-Length", strconv.Itoa(len(b.Bytes())))

	_, err := w.Write(b.Bytes())
	if err != nil {
		log.Errorln(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+err.Error(), http.StatusInternalServerError)
		return
	}
}

func copyHeaders(h map[string][]string) map[string][]string {
	m := make(map[string][]string)
	for key, val := range h {
		m[key] = val
	}
	return m
}

func setHeaders(src, dest map[string][]string) {
	for k, v := range src {
		dest[k] = v
	}
}
