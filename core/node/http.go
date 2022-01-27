package node

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/arcsecc/lohpi/core/util"
	pb "github.com/arcsecc/lohpi/protobuf"
	"github.com/gorilla/mux"
	"github.com/lestrrat-go/jwx/jwa"
	"github.com/lestrrat-go/jwx/jwk"
	"github.com/lestrrat-go/jwx/jws"
	"github.com/rs/cors"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
	"net/http"
	"strconv"
	"strings"
	"time"
)

var httpLogFields = log.Fields{
	"package":     "core/node",
	"description": "http server",
}

var (
	errNoHTTPAddr            = errors.New("HTTP address is empty")
	errNoDatasetAccessDenied = errors.New("Access to dataset is denied")
)

func (n *NodeCore) startHTTPServer() error {
	router := mux.NewRouter()
	addr := fmt.Sprintf(":%d", n.config().Port) // check me

	log.WithFields(httpLogFields).
		Infof("Started HTTP server at port %d\n", n.config().Port)

	dRouter := router.PathPrefix("/dataset").Schemes("HTTP").Subrouter()
	dRouter.HandleFunc("/identifiers", n.getDatasetIdentifiers).Queries("cursor", "{cursor:[0-9,]+}", "limit", "{limit:[0-9,]+}").Methods("GET")
	dRouter.HandleFunc("/checkouts/{id:.*}", n.getDatasetCheckouts).Queries("cursor", "{cursor:[0-9,]+}", "limit", "{limit:[0-9,]+}").Methods("GET")
	dRouter.HandleFunc("/data/{id:.*}", n.getDataset).Methods("GET")
	dRouter.HandleFunc("/metadata/{id:.*}", n.getMetadata).Methods("GET")
	dRouter.HandleFunc("/policy/{id:.*}", n.getDatasetPolicy).Methods("GET")
	dRouter.HandleFunc("/check_in/{id:.*}", n.checkinDataset).Methods("GET")
	dRouter.HandleFunc("/kake", n.test).Methods("GET")

	dRouter.HandleFunc("/addset/{id:.*}", n.addDataset).Methods("POST")
	dRouter.HandleFunc("/removeset/{id:.*}", n.removeDataset).Methods("GET")
	//dRouter.HandleFunc("/test/{id:.*}", n.test).Methods("GET")

	// Middlewares used for validation
	dRouter.Use(n.requestLogging)
	//dRouter.Use(n.middlewareValidateTokenSignature)
	//dRouter.Use(n.middlewareValidateTokenClaims)

	handler := cors.AllowAll().Handler(router)

	n.httpServer = &http.Server{
		Addr: addr,
		Handler: handler,
		WriteTimeout: time.Minute * 30,
		ReadTimeout: time.Minute * 30,
		IdleTimeout: time.Minute * 60,
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

func (n *NodeCore) requestLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.WithFields(logFields).
			WithField("URL", r.URL.String()).
			WithField("Host:", r.Host).
			WithField("Method", r.Method).
			WithField("Proto", r.Proto).
			WithField("Header", r.Header).
			WithField("Remote address", r.RemoteAddr).
			Info("Processing HTTP request...")

		next.ServeHTTP(w, r)
	})
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

	n.pubKeyCache.Configure(msCerts, jwk.WithRefreshInterval(time.Minute*5))

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
				http.Error(w, http.StatusText(http.StatusBadRequest) +  ": " + err.Error(), http.StatusBadRequest)
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

	// Fetch the public keys again from URL if the verification failen.
	// Fetching it again will guarantee a best-effort to verify the request.
	/*ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// This doesn't work!
	// TODO: this doesn't work
	set, err := n.ar.Refresh(ctx, msCerts)
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
	defer r.Body.Close()
	panic("SEEMS TO WORK")
	fmt.Fprintf(w, "Seems to work!")
}

func (n *NodeCore) getDatasetPolicy(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	datasetId := mux.Vars(r)["id"]
	exists, err := n.dsManager.DatasetExists(context.Background(), datasetId)
	if err != nil {
		log.WithFields(httpLogFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+err.Error(), http.StatusInternalServerError)
		return
	}

	if !exists {
		err := fmt.Errorf("Dataset '%s' is not indexed by the server", datasetId)
		log.WithFields(httpLogFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusNotFound)+": "+err.Error(), http.StatusNotFound)
		return
	}

	// TODO: use client attributes to determine access. nil here??
	policy, err := n.dsManager.GetDatasetPolicy(context.Background(), datasetId)
	if err != nil {
		log.WithFields(httpLogFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	policyJsonBytes, err := protojson.Marshal(policy)
	if err != nil {
		log.WithFields(httpLogFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	r.Header.Add("Content-Length", strconv.Itoa(len(policyJsonBytes)))

	_, err = w.Write(policyJsonBytes)
	if err != nil {
		log.WithFields(httpLogFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
}

func (n *NodeCore) removeDataset(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	datasetId := strings.Split(r.URL.Path, "/dataset/removeset/")[1]
	exists, err := n.dsManager.DatasetExists(context.Background(), datasetId)
	if err != nil {
		log.WithFields(httpLogFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+err.Error(), http.StatusInternalServerError)
		return
	}

	if !exists {
		err := fmt.Errorf("Dataset '%s' is not indexed by the node", datasetId)
		log.WithFields(httpLogFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusNotFound)+": "+err.Error(), http.StatusNotFound)
		return
	}

	if err := n.dsManager.RemoveDataset(context.Background(), datasetId); err != nil {
		log.WithFields(httpLogFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Sucsessfully removed dataset '%s'\n", datasetId)
}

// !!
func (n *NodeCore) addDataset(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	datasetId := strings.Split(r.URL.Path, "/dataset/addset/")[1]
	if datasetId == "" {
		err := fmt.Errorf("Dataset identifier must not be empty.")
		log.WithFields(httpLogFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusBadRequest)+": "+err.Error(), http.StatusBadRequest)
		return
	}

	newDataset := &pb.Dataset{
		Identifier: datasetId,
		Policy:     &pb.Policy{},
	}

	if err := n.dsManager.InsertDataset(context.Background(), datasetId, newDataset); err != nil {
		log.WithFields(httpLogFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusBadRequest)+": "+err.Error(), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Sucsessfully added dataset '%s' to the node\n", datasetId)
}

func (n *NodeCore) getMetadata(w http.ResponseWriter, r *http.Request) {
	datasetId := strings.Split(r.URL.Path, "/dataset/metadata/")[1]

	exists, err := n.dsManager.DatasetExists(context.Background(), datasetId)
	if err != nil {
		log.WithFields(httpLogFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+err.Error(), http.StatusInternalServerError)
		return
	}

	if !exists {
		err := fmt.Errorf("Dataset '%s' is not indexed by the server", datasetId)
		log.WithFields(httpLogFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusNotFound)+": "+err.Error(), http.StatusNotFound)
		return
	}

	// TODO: strip the headers from information about the client
	if handler := n.getMetadataHandler(); handler != nil {
		handler(datasetId, w, r)
	} else {
		err := fmt.Errorf("Metadata handler is not set")
		log.WithFields(httpLogFields).Warn(err.Error())
		http.Error(w, http.StatusText(http.StatusNotImplemented)+": "+err.Error(), http.StatusNotImplemented)
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
	exists, err := n.dsManager.DatasetExists(context.Background(), datasetId)
	if err != nil {
		log.WithFields(httpLogFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+err.Error(), http.StatusInternalServerError)
		return
	}

	if !exists {
		err := fmt.Errorf("Dataset '%s' is not indexed by the server", datasetId)
		log.WithFields(httpLogFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusNotFound)+": "+err.Error(), http.StatusNotFound)
		return
	}

	// TODO: disallow if dataset has been checked out by same client
	policy, err := n.dsManager.GetDatasetPolicy(context.Background(), datasetId)
	if err != nil {
		log.WithFields(httpLogFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+err.Error(), http.StatusInternalServerError)
		return
	}

	// TODO: parse policy with Casbin here
	if !policy.GetContent() {
		errMsg := fmt.Errorf("You do not have access to dataset with id '%s'", datasetId)
		log.WithFields(httpLogFields).Error(errMsg.Error())
		http.Error(w, http.StatusText(http.StatusUnauthorized)+": "+errMsg.Error(), http.StatusUnauthorized)
		return
	}

	pbClient, err := util.ExstractPbClient(r)
	if err != nil {
		log.WithFields(httpLogFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusBadRequest)+": "+err.Error(), http.StatusBadRequest)
		return
	}

	// All information we need about the dataset checkout event is embedded in this object
	checkout := &pb.DatasetCheckout{
		DatasetIdentifier:      datasetId,
		DateCheckout:           timestamppb.Now(),
		Client:                 pbClient,
		ClientPolicyVersion:    policy.GetVersion(),
		DateLatestVerification: timestamppb.Now(),
	}

	if err := n.checkoutManager.CheckoutDataset(context.Background(), datasetId, checkout, policy.GetVersion(), policy.GetContent()); err != nil {
		log.WithFields(httpLogFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+err.Error(), http.StatusInternalServerError)
		return
	}

	// TODO: strip the headers from information about the client
	if handler := n.getDatasetHandler(); handler != nil {
		handler(datasetId, w, r)
		// TODO: write to responseWriter only once. What happens if anything goes wrong at any point in time?
	} else {
		err := fmt.Errorf("Dataset download is not implemented for this service")
		log.Warnln(err.Error())
		http.Error(w, http.StatusText(http.StatusNotImplemented)+": "+err.Error(), http.StatusNotImplemented)
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
		http.Error(w, http.StatusText(http.StatusBadRequest)+": "+err.Error(), http.StatusBadRequest)
		return
	}

	l, err := strconv.Atoi(limit)
	if err != nil {
		log.WithFields(httpLogFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusBadRequest)+": "+err.Error(), http.StatusBadRequest)
		return
	}

	// TODO: require intervals here too
	ids, err := n.dsManager.DatasetIdentifiers(context.Background(), int64(c), int64(l))
	if err != nil {
		log.WithFields(httpLogFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+err.Error(), http.StatusInternalServerError)
		return
	}
	b := new(bytes.Buffer)
	if err := json.NewEncoder(b).Encode(ids); err != nil {
		log.WithFields(httpLogFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Add("Content-Type", "application/json")
	w.Header().Add("Content-Length", strconv.Itoa(len(b.Bytes())))

	_, err = w.Write(b.Bytes())
	if err != nil {
		log.WithFields(httpLogFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+err.Error(), http.StatusInternalServerError)
		return
	}
}

func (n *NodeCore) checkinDataset(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	datasetId := mux.Vars(r)["id"]

	pbClient, err := util.ExstractPbClient(r)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Check if it is checked out at all
	isCheckedOut, err := n.checkoutManager.DatasetIsCheckedOutByClient(context.Background(), datasetId, pbClient)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+err.Error(), http.StatusInternalServerError)
		return
	}

	// It has not been checked out by this client.
	// TODO: the dataset checkout overview must be consistent with the node's view
	if !isCheckedOut {
		err := fmt.Errorf("Dataset with identifier '%s' is not checked out", datasetId)
		log.WithFields(logFields).Debug(err.Error())
		http.Error(w, http.StatusText(http.StatusNotFound)+": "+err.Error(), http.StatusNotFound)
		return
	}

	// If it was checked out, check it in
	if err := n.checkoutManager.CheckinDataset(context.Background(), datasetId, pbClient); err != nil {
		clientErr := fmt.Errorf("Could not checkin dataset with identifier '%s'")
		log.WithFields(logFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+clientErr.Error(), http.StatusInternalServerError)
		return
	}
}

func (n *NodeCore) getDatasetCheckouts(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	cursor := mux.Vars(r)["cursor"]
	limit := mux.Vars(r)["limit"]
	datasetId := mux.Vars(r)["id"]

	log.WithFields(logFields).
		WithField("cursor:", cursor).
		WithField("limit:", limit).
		Infof("Selecting dataset checkouts with identifier '%s'", datasetId)

	c, err := strconv.Atoi(cursor)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusBadRequest)+": "+err.Error(), http.StatusBadRequest)
		return
	}

	l, err := strconv.Atoi(limit)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusBadRequest)+": "+err.Error(), http.StatusBadRequest)
		return
	}

	datasetCheckouts, err := n.checkoutManager.DatasetCheckouts(context.Background(), datasetId, c, l)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	responseBytes, err := protojson.Marshal(datasetCheckouts)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	r.Header.Add("Content-Length", strconv.Itoa(len(responseBytes)))

	_, err = w.Write(responseBytes)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
}

func (n *NodeCore) DatasetIsCheckedOut(datasetId string, client *pb.Client) (bool, error) {
	//return n.checkoutManager.DatasetIsCheckedOut(datasetId, pbClient)
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
	_, err := n.checkoutManager.DatasetCheckouts(dataset)
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
