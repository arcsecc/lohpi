package node

import (
	"bytes"
	"strings"
	"encoding/json"
	"net"
	"errors"
	pb "github.com/arcsecc/lohpi/protobuf"
	"fmt"
	"context"
//	"github.com/arcsecc/lohpi/core/comm"
	"github.com/arcsecc/lohpi/core/util"
	"github.com/gorilla/mux"
_	"google.golang.org/protobuf/types/known/timestamppb"
	"github.com/rs/cors"
	"github.com/lestrrat-go/jwx/jwk"
	"github.com/lestrrat-go/jwx/jws"
	"github.com/lestrrat-go/jwx/jwa"
	log "github.com/sirupsen/logrus"
	"net/http"
	pbtime "google.golang.org/protobuf/types/known/timestamppb"
	"strconv"
	"time"
)

var httpLogFields = log.Fields{
	"package": "core/node",
	"description": "http server",
}

var (
	errNoHTTPAddr = errors.New("HTTP address is empty")
	errNoDatasetAccessDenied = errors.New("Access to dataset is denied")
)

func (n *NodeCore) startHTTPServer() error {
	router := mux.NewRouter()
	addr := fmt.Sprintf(":%d", n.config().Port)		// check me
	
	log.WithFields(httpLogFields).
		Infof("Started HTTP server at port %d\n", n.config().Port)

	dRouter := router.PathPrefix("/dataset").Schemes("HTTP").Subrouter()
	dRouter.
		HandleFunc("/ids", n.getDatasetIdentifiers).
		Queries("cursor", "{cursor:[0-9,]+}", "limit", "{limit:[0-9,]+}").
		Methods("GET")

	dRouter.HandleFunc("/info/{id:.*}", n.getDatasetSummary).Methods("GET")
	dRouter.HandleFunc("/setpolicy/{id:.*}", n.setDatasetPolicy).Methods("PUT")
	dRouter.HandleFunc("/data/{id:.*}", n.getDataset).Methods("GET")
	dRouter.HandleFunc("/metadata/{id:.*}", n.getMetadata).Methods("GET")
	dRouter.HandleFunc("/is_available/{id:.*}", n.datasetIsAvailable).Methods("GET")
	
	dRouter.HandleFunc("/addset/{id:.*}", n.addDataset).Methods("POST")
	dRouter.HandleFunc("/removeset/{id:.*}", n.removeDataset).Methods("GET")
	dRouter.HandleFunc("/test/{id:.*}", n.test).Methods("GET")

	policyRouter := router.PathPrefix("/policy").Schemes("HTTP").Subrouter()
	policyRouter.HandleFunc("/version/{id:.*}", n.getPolicyVersion).Methods("GET")

	// Middlewares used for validation
	dRouter.Use(n.middlewareValidateTokenSignature)
	//dRouter.Use(n.middlewareValidateTokenClaims)

	handler := cors.AllowAll().Handler(router)

	n.httpServer = &http.Server{
		Addr:         addr,
		Handler:      handler,
		WriteTimeout: time.Minute * 30,
		ReadTimeout:  time.Minute * 30,
		IdleTimeout:  time.Minute * 60,
		//TLSConfig:    comm.ServerConfig(n.cu.Certificate(), n.cu.CaCertificate(), n.cu.PrivateKey()),
	}

	if err := n.setPublicKeyCache(); err != nil {
		log.WithFields(httpLogFields).Error(err.Error())
		return err
	}

	return n.httpServer.ListenAndServe()
}

func (n *NodeCore) stopHTTPServer() error {
	return nil
}

// TODO redirect HTTP to HTTPS
func redirectTLS(w http.ResponseWriter, r *http.Request) {
    //http.Redirect(w, r, "https://IPAddr:443"+r.RequestURI, http.StatusMovedPermanently)
}

func (n *NodeCore) setPublicKeyCache() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	n.pubKeyCache = jwk.NewAutoRefresh(ctx)
	const msCerts = "https://login.microsoftonline.com/common/discovery/v2.0/keys" // TODO: config me

	n.pubKeyCache.Configure(msCerts, jwk.WithRefreshInterval(time.Minute * 5))

	// Keep the cache warm
	_, err := n.pubKeyCache.Refresh(ctx, msCerts)
	if err != nil {
		log.Println("Failed to refresh Microsoft Azure JWKS")
		return err
	}
	return nil
}

// defer r.Body.Close()?

func (n *NodeCore) middlewareValidateTokenSignature(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, err := getBearerToken(r)
		if err != nil {
			http.Error(w, http.StatusText(http.StatusBadRequest)+": "+err.Error(), http.StatusBadRequest)
			return
		}
		_ = token
		/*
		if err := n.validateTokenSignature(token); err != nil {
			panic(err)
			http.Error(w, http.StatusText(http.StatusBadRequest)+": "+err.Error(), http.StatusBadRequest)
			return
		}*/

		next.ServeHTTP(w, r)
	})
}

func (n *NodeCore) validateTokenSignature(token []byte) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// TODO: fetch keys again if it fails
	// TODO: add clock skew. Use jwt.WithAcceptableSkew
	set, err := n.pubKeyCache.Fetch(ctx, "https://login.microsoftonline.com/common/discovery/v2.0/keys")
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

func (n *NodeCore) test(w http.ResponseWriter, r *http.Request) {
	
}

func (n *NodeCore) datasetIsAvailable(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	datasetId := strings.Split(r.URL.Path, "/dataset/is_available/")[1]
	if !n.dsManager.DatasetExists(datasetId) {
		err := fmt.Errorf("Dataset '%s' is not indexed by the server", datasetId)
		log.WithFields(httpLogFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusNotFound) + ": " + err.Error(), http.StatusNotFound)
		return
	}

	// TODO: use client attributes to determine access. nil here??
	policy := n.dsManager.GetDatasetPolicy(datasetId)
	response := struct {
		IsAvailable bool `json:"is_available"`
	}{
		IsAvailable: policy.GetContent(),
	}

	b := new(bytes.Buffer)
	if err := json.NewEncoder(b).Encode(response); err != nil {
		log.WithFields(httpLogFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	r.Header.Add("Content-Length", strconv.Itoa(len(b.Bytes())))

	_, err := w.Write(b.Bytes())
	if err != nil {
		log.WithFields(httpLogFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError) + ": " + err.Error(), http.StatusInternalServerError)
		return
	}
}

func (n *NodeCore) removeDataset(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	datasetId := strings.Split(r.URL.Path, "/dataset/removeset/")[1]
	if datasetId == "" {
		err := fmt.Errorf("Dataset identifier must not be empty.")
		log.WithFields(httpLogFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusBadRequest) + ": " + err.Error(), http.StatusBadRequest)
		return
	}	

	if !n.dsManager.DatasetExists(datasetId) {
		err := fmt.Errorf("Dataset '%s' is not indexed by the server", datasetId)
		log.WithFields(httpLogFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusNotFound) + ": " + err.Error(), http.StatusNotFound)
		return
	}

	if err := n.dsManager.RemoveDataset(datasetId); err != nil {
		log.WithFields(httpLogFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError) + ": " + err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Sucsessfully removed dataset '%s' to the node\n", datasetId);
}

func (n *NodeCore) addDataset(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	datasetId := strings.Split(r.URL.Path, "/dataset/addset/")[1]
	if datasetId == "" {
		err := fmt.Errorf("Dataset identifier must not be empty.")
		log.WithFields(httpLogFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusBadRequest) + ": " + err.Error(), http.StatusBadRequest)
		return
	}	

	newDataset := &pb.Dataset{
		Identifier: datasetId,
		Policy: &pb.Policy{},
	}

	if err := n.dsManager.InsertDataset(datasetId, newDataset); err != nil {
		log.WithFields(httpLogFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusBadRequest) + ": " + err.Error(), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Sucsessfully added dataset '%s' to the node\n", datasetId);
}

func (n *NodeCore) getMetadata(w http.ResponseWriter, r *http.Request) {
	datasetId := strings.Split(r.URL.Path, "/dataset/metadata/")[1]
	
	if !n.dsManager.DatasetExists(datasetId) {
		err := fmt.Errorf("Dataset '%s' is not indexed by the server", datasetId)
		log.WithFields(httpLogFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusNotFound) + ": " + err.Error(), http.StatusNotFound)
		return
	}

	// TODO: strip the headers from information about the client
	if handler := n.getMetadataHandler(); handler != nil {
		handler(datasetId, w, r)
	} else {
		err := fmt.Errorf("Metadata handler is nil")
		log.Warnln(err.Error())
		http.Error(w, http.StatusText(http.StatusNotImplemented) + ": " + err.Error(), http.StatusNotImplemented)
		return
	}
}

func getBearerToken(r *http.Request) ([]byte, error) {
	authHeader := r.Header.Get("Authorization")
	authHeaderContent := strings.Split(authHeader, " ")
	if len(authHeaderContent) != 2 {
		return nil, errors.New("Token not provided or malformed")
	}
	return []byte(authHeaderContent[1]), nil
}

// TODO use context!
func (n *NodeCore) getDataset(w http.ResponseWriter, r *http.Request) {
	log.WithFields(httpLogFields).
		WithField("URL:", r.URL.Path).
		WithField("Method:", r.Method).
		Infoln("getDataset HTTP handler invoked")

	defer r.Body.Close()

	datasetId := strings.Split(r.URL.Path, "/dataset/data/")[1]
	if !n.dsManager.DatasetExists(datasetId) {
		err := fmt.Errorf("Dataset '%s' is not indexed by the server", datasetId)
		log.WithFields(httpLogFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusNotFound) + ": " + err.Error(), http.StatusNotFound)
		return
	}

	// TODO: disallow if dataset has been checked out by same client
	policy := n.dsManager.GetDatasetPolicy(datasetId)
	if !policy.GetContent() {
		log.WithFields(httpLogFields).Error(errNoDatasetAccessDenied.Error())
		errMsg := fmt.Sprintf("You do not have access to dataset with id '%s'", datasetId)
		http.Error(w, http.StatusText(http.StatusUnauthorized) + ": " + errMsg, http.StatusUnauthorized)
		return
	}

	pbClient, err := exstractPbClient(r)
	if err != nil {
		log.Infoln(err.Error())
		http.Error(w, http.StatusText(http.StatusBadRequest)+": "+err.Error(), http.StatusBadRequest)
		return
	}

	// All information we need about the dataset checkout event is embedded in this object
	checkout := &pb.DatasetCheckout{
		DatasetIdentifier: datasetId,
		DateCheckout: pbtime.Now(),
		Client: pbClient,
		PolicyVersion: policy.GetVersion(),
	}

	if err := n.dcManager.CheckoutDataset(datasetId, checkout); err != nil {
		log.WithFields(httpLogFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError) + ": " + err.Error(), http.StatusInternalServerError)
		return
	}
	
	// TODO: strip the headers from information about the client
	if handler := n.getDatasetHandler(); handler != nil {
		handler(datasetId, w, r)
		// TODO: write to responseWriter only once. What happens if anything goes wrong at any point in time?
	} else {
		err := fmt.Errorf("Dataset download is not implemented for this service")
		log.Warnln(err.Error())
		http.Error(w, http.StatusText(http.StatusNotImplemented) + ": " + err.Error(), http.StatusNotImplemented)
		return
	}
}

// Returns the dataset identifiers stored at this node
func (n *NodeCore) getDatasetIdentifiers(w http.ResponseWriter, r *http.Request) {
	log.WithFields(httpLogFields).
		WithField("URL:", r.URL.Path).
		WithField("Method:", r.Method).
		Infoln("getDatasetIdentifiers HTTP handler invoked")

	defer r.Body.Close()

	cursor := mux.Vars(r)["cursor"]
	limit := mux.Vars(r)["limit"]

	c, err := strconv.Atoi(cursor)
	if err != nil {
		log.WithFields(httpLogFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusBadRequest) + ": " + err.Error(), http.StatusBadRequest)
		return
	}

	l, err := strconv.Atoi(limit)
	if err != nil {
		log.WithFields(httpLogFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusBadRequest) + ": " + err.Error(), http.StatusBadRequest)
		return
	}

	// TODO: require intervals here too
	ids := n.dsManager.DatasetIdentifiers(int64(c), int64(l))
	b := new(bytes.Buffer)
	if err := json.NewEncoder(b).Encode(ids); err != nil {
		log.WithFields(httpLogFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError) + ": " + err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Add("Content-Type", "application/json")
	w.Header().Add("Content-Length", strconv.Itoa(len(b.Bytes())))

	_, err = w.Write(b.Bytes())
	if err != nil {
		log.WithFields(httpLogFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError) + ": " + err.Error(), http.StatusInternalServerError)
		return
	}
}

func (n *NodeCore) DatasetIsCheckedOut(datasetId string, client *pb.Client) (bool, error) {
	//return n.dcManager.DatasetIsCheckedOut(datasetId, pbClient)
	return false, nil
}

// Returns a JSON object containing the metadata assoicated with a dataset
func (n *NodeCore) getDatasetSummary(w http.ResponseWriter, r *http.Request) {
	/*log.Infoln("Got request to", r.URL.String())
	defer r.Body.Close()

	dataset := mux.Vars(r)["id"]
	if dataset == "" {
		err := errors.New("Missing dataset identifier")
		log.WithFields(httpLogFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusBadRequest) + ": " + err.Error(), http.StatusBadRequest)
		return
	}

	if !n.dsManager.DatasetExists(dataset) {
		err := fmt.Errorf("Dataset '%s' is not indexed by the server", dataset)
		log.WithFields(httpLogFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusNotFound) + ": " + err.Error(), http.StatusNotFound)
		return
	}

	// Fetch the policy
	policy := n.dsManager.DatasetPolicy(dataset)
	if policy == nil {
		log.Errorln("Policy is nil")
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	// Fetch the clients that have checked out the dataset
	_, err := n.dcManager.DatasetCheckouts(dataset)
	if err != nil {
		log.WithFields(httpLogFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	// Destination struct
	resp := struct {
		Dataset   string
		Policy    string
		//Checkouts []CheckoutInfo
	}{
		Dataset:   dataset,
		//Policy:    policy,
		//Checkouts: checkouts,
	}

	b := new(bytes.Buffer)
	if err := json.NewEncoder(b).Encode(resp); err != nil {
		log.WithFields(httpLogFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	r.Header.Add("Content-Length", strconv.Itoa(len(b.Bytes())))

	_, err = w.Write(b.Bytes())
	if err != nil {
		log.WithFields(httpLogFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError) + ": " + err.Error(), http.StatusInternalServerError)
		return
	}*/
}

func (n *NodeCore) setDatasetPolicy(w http.ResponseWriter, r *http.Request) {
	log.Infoln("Got request to", r.URL.String())
	defer r.Body.Close()

	// Enforce application/json MIME type
	if r.Header.Get("Content-Type") != "application/json" {
		msg := "Content-Type header is not application/json"
		http.Error(w, http.StatusText(http.StatusUnsupportedMediaType)+": "+msg, http.StatusUnsupportedMediaType)
		return
	}

	// Fetch the dataset if
	datasetId := mux.Vars(r)["id"]

	// Check if dataset exists in the external repository
	if !n.dsManager.DatasetExists(datasetId) {
		err := fmt.Errorf("Dataset '%s' is not stored in this node", datasetId)
		log.WithFields(httpLogFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusNotFound) + ": " + err.Error(), http.StatusNotFound)
		return
	}

	// Destination struct
	reqBody := struct {
		Policy bool
	}{}

	if err := util.DecodeJSONBody(w, r, "application/json", &reqBody); err != nil {
		var e *util.MalformedParserReponse
		if errors.As(err, &e) {
			log.WithFields(httpLogFields).Error(err.Error())
			http.Error(w, e.Msg, e.Status)
		} else {
			log.WithFields(httpLogFields).Error(err.Error())
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		}
		return
	}

	currentDataset := n.dsManager.Dataset(datasetId)

	newPolicy := &pb.Policy{
		DatasetIdentifier: 	datasetId,
		Content:          	reqBody.Policy,
		Version:			currentDataset.GetPolicy().GetVersion() + 1,
		DateCreated:		currentDataset.GetPolicy().GetDateCreated(),
		DateUpdated:        pbtime.Now(),
	}

	if err := n.dsManager.SetDatasetPolicy(datasetId, newPolicy); err != nil {
		log.Warnln(err.Error())
	}

	respMsg := "Successfully set a new policy for " + datasetId + "\n"
	w.WriteHeader(http.StatusCreated)
	w.Header().Set("Content-Type", "application/text")
	w.Header().Set("Content-Length", strconv.Itoa(len(respMsg)))
	w.Write([]byte(respMsg))
}


func (n *NodeCore) getPolicyVersion(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	datasetId := strings.Split(r.URL.Path, "/policy/version/")[1]
	if !n.dsManager.DatasetExists(datasetId) {
		err := fmt.Errorf("Dataset '%s' is not indexed by the server", datasetId)
		log.WithFields(httpLogFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusNotFound) + ": " + err.Error(), http.StatusNotFound)
		return
	}

	// TODO: use client attributes to determine access. nil here??
	policy := n.dsManager.GetDatasetPolicy(datasetId)
	response := struct {
		PolicyVersion uint64 `json:"policy_version"`
	}{
		PolicyVersion: policy.GetVersion(),
	}

	b := new(bytes.Buffer)
	if err := json.NewEncoder(b).Encode(response); err != nil {
		log.WithFields(httpLogFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	r.Header.Add("Content-Length", strconv.Itoa(len(b.Bytes())))

	_, err := w.Write(b.Bytes())
	if err != nil {
		log.WithFields(httpLogFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError) + ": " + err.Error(), http.StatusInternalServerError)
		return
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

	// Validate IP address format
	if len(r.Header.Values("ip_address")) > 0 {
		ipAddress = r.Header.Values("ip_address")[0]
		if net.ParseIP(ipAddress) == nil {
			return nil, fmt.Errorf("IP address %s is invalid\n", ipAddress)
		}
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