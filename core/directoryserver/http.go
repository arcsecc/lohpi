package directoryserver

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/arcsecc/lohpi/core/comm"
	"github.com/arcsecc/lohpi/core/util"
	pb "github.com/arcsecc/lohpi/protobuf"
	"github.com/gorilla/mux"
	"github.com/lestrrat-go/jwx/jwa"
	"github.com/lestrrat-go/jwx/jwk"
	"github.com/lestrrat-go/jwx/jws"
	log "github.com/sirupsen/logrus"
	pbtime "google.golang.org/protobuf/types/known/timestamppb"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

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
	dRouter.HandleFunc("/ids", d.getNetworkDatasetIdentifiers).Methods("GET")
	dRouter.HandleFunc("/metadata/{id:.*}", d.getDatasetMetadata).Methods("GET")
	dRouter.HandleFunc("/data/{id:.*}", d.getDataset).Methods("GET")
	dRouter.HandleFunc("/verify/{id:.*}", d.getDatasetPolicyVerification).Methods("GET")
	dRouter.HandleFunc("/set_project_description/{id:.*}", d.setProjectDescription).Methods("POST")
	dRouter.HandleFunc("/get_project_description/{id:.*}", d.getProjectDescription).Methods("GET")
	dRouter.HandleFunc("/is_available/{id:.*}", d.datasetIsAvailable).Methods("GET")
	dRouter.HandleFunc("/test/{id:.*}", d.testGetDataset).Methods("GET")

	networkRouter := r.PathPrefix("/network").Schemes("HTTP").Subrouter()
	networkRouter.HandleFunc("/ndoes", d.getNetworkNodes).Methods("GET")

	// Middlewares used for validation
	//dRouter.Use(d.middlewareValidateTokenSignature)
	//dRouter.Use(d.middlewareValidateTokenClaims)

	serverConfig, err := comm.ServerConfig(d.cm.Certificate(), d.cm.CaCertificate(), d.cm.PrivateKey())
	if err != nil {
		return err
	}

	d.httpServer = &http.Server{
		Addr:         addr,
		Handler:      r,
		WriteTimeout: time.Hour * 1,
		//ReadTimeout:  time.Second * 30,
		//IdleTimeout:  time.Second * 60,
		TLSConfig: serverConfig,
	}

	if err := d.setPublicKeyCache(); err != nil {
		log.Errorln(err.Error())
		return err
	}

	return d.httpServer.ListenAndServe()
}

func (d *DirectoryServerCore) testGetDataset(w http.ResponseWriter, r *http.Request) {
	datasetId := mux.Vars(r)["id"]
	fmt.Fprintf(w, "%v\n", d.memManager.RemoveNetworkNode(datasetId))
}

func (d *DirectoryServerCore) getNetworkNodes(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	/*nodes := struct {
		Nodes []struct {
			Name string `json:"name"`
			IpAddress string `json:"ip_address"`
			JoinedAt string `json:"joined_at"`
			Datasets []string `json:"datasets"`
			CheckedOutDatasets []string `json:"checked_out_datasets"`
		}
	}*/
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
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+err.Error(), http.StatusInternalServerError)
		return
	}

	dataset := mux.Vars(r)["id"]

	if !d.dsLookupService.DatasetNodeExists(dataset) {
		err := fmt.Errorf("Dataset '%s' is not stored in the network", dataset)
		log.Infoln(err.Error())
		http.Error(w, http.StatusText(http.StatusNotFound)+": "+err.Error(), http.StatusNotFound)
		return
	}

	// Project description as argument to updatePD, string?
	if err := d.updateProjectDescription(dataset, clientReq.ProjectDescription); err != nil {
		log.Errorln(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+err.Error(), http.StatusInternalServerError)
		return
	}
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
	if !d.dsLookupService.DatasetNodeExists(dataset) {
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

	//	ctx, cancel := context.WithDeadline(r.Context(), time.Now().Add(time.Second * 10))
	//	defer cancel()

	ids := d.dsLookupService.DatasetIdentifiers()
	sort.Strings(ids)

	// Destination struct
	resp := struct {
		Identifiers []string
	}{
		Identifiers: ids,
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
	if !d.dsLookupService.DatasetNodeExists(dataset) {
		err := fmt.Errorf("Dataset '%s' is not stored in the network", dataset)
		log.Infoln(err.Error())
		http.Error(w, http.StatusText(http.StatusNotFound)+": "+err.Error(), http.StatusNotFound)
		return
	}

	node := d.dsLookupService.DatasetLookupNode(dataset)
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

func (d *DirectoryServerCore) datasetIsAvailable(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	// Reject request at directoryu server if token is invalid
	token, err := getBearerToken(r)
	if err != nil {
		log.Infoln(err.Error())
		http.Error(w, http.StatusText(http.StatusBadRequest)+": "+err.Error(), http.StatusBadRequest)
		return
	}

	dataset := mux.Vars(r)["id"]

	// Get the node that stores it
	if !d.dsLookupService.DatasetNodeExists(dataset) {
		err := fmt.Errorf("Dataset '%s' is not stored in the network", dataset)
		log.Infoln(err.Error())
		http.Error(w, http.StatusText(http.StatusNotFound)+": "+err.Error(), http.StatusNotFound)
		return
	}

	node := d.dsLookupService.DatasetLookupNode(dataset)
	if node == nil {
		err := fmt.Errorf("The network node that stores the dataset '%s' is not available", dataset)
		panic(err)
		log.Infoln(err.Error())
		http.Error(w, http.StatusText(http.StatusGone)+": "+err.Error(), http.StatusGone)
		return
	}

	req := &http.Request{
		Method: "GET",
		URL: &url.URL{
			Scheme: "http", //https
			Host:   node.GetHttpsAddress() + ":" + strconv.Itoa(int(node.GetPort())),
			Path:   "/dataset/is_available/" + dataset,
		},
		Header: http.Header{},
	}

	req.Header.Add("Authorization", "Bearer "+string(token))

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+err.Error(), http.StatusInternalServerError)
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
		panic(err)
		log.Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+err.Error(), http.StatusInternalServerError)
		return
	}
}

// Handler used to fetch an entire dataset. TODO: fix HTTP header information
func (d *DirectoryServerCore) getDataset(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	// Get dataset identifier
	dataset := mux.Vars(r)["id"]

	// Get the node that stores it
	if !d.dsLookupService.DatasetNodeExists(dataset) {
		err := fmt.Errorf("Dataset '%s' is not stored in the network", dataset)
		log.Infoln(err.Error())
		http.Error(w, http.StatusText(http.StatusNotFound)+": "+err.Error(), http.StatusNotFound)
		return
	}

	node := d.dsLookupService.DatasetLookupNode(dataset)
	if node == nil {
		err := fmt.Errorf("The network node that stores the dataset '%s' is not available", dataset)
		log.Infoln(err.Error())
		http.Error(w, http.StatusText(http.StatusGone)+": "+err.Error(), http.StatusGone)
		return
	}

	pbClient, err := exstractPbClient(r)
	if err != nil {
		log.Infoln(err.Error())
		http.Error(w, http.StatusText(http.StatusBadRequest)+": "+err.Error(), http.StatusBadRequest)
		return
	}

	// Prepare the request. Beginning of pipeline
	datasetReq := &http.Request{
		Method: "GET",
		URL: &url.URL{
			Scheme: "http", //https
			Host:   node.GetHttpsAddress() + ":" + strconv.Itoa(int(node.GetPort())),
			Path:   "/dataset/data/" + dataset,
		},
		Header: http.Header{},
	}

	// Shallow copy headers
	datasetReq.Header = r.Header

	client := &http.Client{}
	resp, err := client.Do(datasetReq)
	if err != nil {
		log.Error(err.Error())
		http.Error(w, http.StatusText(http.StatusBadRequest)+": "+err.Error(), http.StatusBadRequest)
		return
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Errorln(resp.Status + ": " + resp.Status)
		http.Error(w, http.StatusText(resp.StatusCode)+": "+resp.Status, resp.StatusCode)
		return
	}

	// Fetch the current policy version from the node
	policyVersionReq := &http.Request{
		Method: "GET",
		URL: &url.URL{
			Scheme: "http", //https
			Host:   node.GetHttpsAddress() + ":" + strconv.Itoa(int(node.GetPort())),
			Path:   "/policy/version/" + dataset,
		},
		Header: http.Header{},
	}

	client = &http.Client{}
	resp, err = client.Do(policyVersionReq)
	if err != nil {
		log.Error(err.Error())
		http.Error(w, http.StatusText(http.StatusBadRequest)+": "+err.Error(), http.StatusBadRequest)
		return
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Errorln(resp.Status + ": " + resp.Status)
		http.Error(w, http.StatusText(resp.StatusCode)+": "+resp.Status, resp.StatusCode)
		return
	}

	policyVersionResponse := struct {
		PolicyVersion uint64 `json:"policy_version"`
	}{}

	if err := json.NewDecoder(resp.Body).Decode(&policyVersionResponse); err != nil {
		log.Error(err.Error())
		http.Error(w, http.StatusText(http.StatusBadRequest)+": "+err.Error(), http.StatusBadRequest)
		return
	}

	dsCheckout := &pb.DatasetCheckout{
		DatasetIdentifier: dataset,
		DateCheckout:      pbtime.Now(),
		Client:            pbClient,
		PolicyVersion:     policyVersionResponse.PolicyVersion,
	}

	if err := d.checkoutManager.CheckoutDataset(dataset, dsCheckout); err != nil {
		log.Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+err.Error(), http.StatusInternalServerError)
		return
	}

	reader := bufio.NewReader(resp.Body)
	if err := util.StreamToResponseWriter(reader, w, 1000*1024); err != nil {
		log.Error(err.Error())
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

func exstractPbClient(r *http.Request) (*pb.Client, error) {
	token, err := getBearerToken(r)
	if err != nil {
		return nil, err
	}

	msg, err := jws.ParseString(string(token))
	if err != nil {
		return nil, err
	}

	s := msg.Payload()
	if s == nil {
		return nil, errors.New("Payload was nil")
	}

	c := struct {
		Name         string `json:"name"`
		Oid          string `json:"oid"`
		EmailAddress string `json:"email"`
	}{}

	if err := json.Unmarshal(s, &c); err != nil {
		return nil, err
	}

	// Fetch the HTTP headers required to build a protobuf client
	if len(r.Header.Values("dns_name")) < 1 {
		return nil, errors.New("dns_name header field was not supplied")
	}

	var dnsName string = r.Header.Values("dns_name")[0]
	var macAddress string
	var ipAddress string

	if len(r.Header.Values("mac_address")) > 0 {
		macAddress = r.Header.Values("mac_address")[0]
	}

	if len(r.Header.Values("ip_address")) > 0 {
		ipAddress = r.Header.Values("ip_address")[0]
	}

	return &pb.Client{
		Name:         c.Name,
		ID:           c.Oid,
		EmailAddress: c.EmailAddress,
		MacAddress:   macAddress,
		IpAddress:    ipAddress,
		DNSName:      dnsName,
	}, nil
}
