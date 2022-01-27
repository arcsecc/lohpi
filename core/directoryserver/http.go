package directoryserver

import (
	"bufio"
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
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

func (d *DirectoryServerCore) startHttpServer(addr string) error {
	r := mux.NewRouter()
	log.Infoln("Started directory server on port", d.config.HTTPPort)

	// Dataset router exposed to the clients
	router := r.PathPrefix("/dataset").Schemes("HTTP", "HTTPS").Subrouter().SkipClean(false)
	router.HandleFunc("/identifiers", d.getDatasetIdentifiers).Queries("cursor", "{cursor:[0-9,]+}", "limit", "{limit:[0-9,]+}").Methods("GET")
	router.HandleFunc("/checkouts/{id:.*}", d.getDatasetCheckouts).Queries("cursor", "{cursor:[0-9,]+}", "limit", "{limit:[0-9,]+}").Methods("GET")
	router.HandleFunc("/num_checkouts/{id:.*}", d.getNumDatasetCheckout).Methods("GET")
	router.HandleFunc("/metadata/{id:.*}", d.getDatasetMetadata).Methods("GET")
	router.HandleFunc("/data/{id:.*}", d.getDataset).Methods("GET")
	router.HandleFunc("/policy/{id:.*}", d.getDatasetPolicy).Methods("GET")
	router.HandleFunc("/comply/{id:.*}", d.getDatasetCompliance).Methods("GET").Queries("version", "{[0-9]*?}")
	router.HandleFunc("/check_in/{id:.*}", d.checkinDataset).Methods("GET")
	router.HandleFunc("/set_project_description/{id:.*}", d.setProjectDescription).Methods("POST")
	router.HandleFunc("/get_project_description/{id:.*}", d.getProjectDescription).Methods("GET")
	router.HandleFunc("/num_datasets", d.getNumDatasets).Methods("GET")
	router.HandleFunc("/num_networknodes", d.getNumNetworkNodes).Methods("GET")
	router.HandleFunc("/network_nodes", d.getNetworkNodes).Queries("cursor", "{cursor:[0-9,]+}", "limit", "{limit:[0-9,]+}").Methods("GET")

	// Test stuff
	router.HandleFunc("/test/{id:.*}", d.testGetDataset).Methods("GET")

	// Middlewares
	router.Use(d.requestLogging)
	//router.Use(d.middlewareValidateTokenSignature)
	//router.Use(d.middlewareValidateTokenClaims)

	handler := cors.AllowAll().Handler(router)

	d.httpServer = &http.Server{
		Addr: addr,
		Handler: handler,
		WriteTimeout: time.Hour * 1,
		//ReadTimeout:  time.Second * 30,
		//IdleTimeout:  time.Second * 60,
		TLSConfig: d.serverConfig,
	}

	if err := d.setPublicKeyCache(); err != nil {
		log.WithFields(logFields).Error(err.Error())
		return err
	}

	return d.httpServer.ListenAndServeTLS("", "")
}

func (d *DirectoryServerCore) requestLogging(next http.Handler) http.Handler {
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
			http.Error(w, http.StatusText(http.StatusBadRequest) + " : " + err.Error(), http.StatusBadRequest)
			log.WithFields(logFields).Error(err.Error())
			return
		}

		if err := d.validateTokenSignature(token); err != nil {
			http.Error(w, http.StatusText(http.StatusBadRequest) + " : " + err.Error(), http.StatusBadRequest)
			log.WithFields(logFields).Error(err.Error())
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

func (d *DirectoryServerCore) testGetDataset(w http.ResponseWriter, r *http.Request) {
	datasetId := mux.Vars(r)["id"]

	node, err := d.dsLookupService.DatasetLookupNode(context.Background(), datasetId)
	if err != nil {
		clientErr := fmt.Errorf("Failed to get dataset metadata of dataset '%s'", datasetId)
		log.WithFields(logFields).Error(clientErr.Error())
		log.WithFields(logFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+clientErr.Error(), http.StatusInternalServerError)
		return
	}

	if node == nil {
		err := fmt.Errorf("Dataset '%s' does not exist", datasetId)
		log.WithFields(logFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusNotFound) + " : " + err.Error(), http.StatusNotFound)
		return
	}
}

func (d *DirectoryServerCore) getNetworkNodes(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	cursor := mux.Vars(r)["cursor"]
	limit := mux.Vars(r)["limit"]

	c, err := strconv.Atoi(cursor)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusBadRequest) + " : " + err.Error(), http.StatusBadRequest)
		return
	}

	l, err := strconv.Atoi(limit)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusBadRequest) + " : " + err.Error(), http.StatusBadRequest)
		return
	}

	nodes, err := d.memManager.NetworkNodes(context.Background(), c, l)
	if err != nil {
		clientErr := fmt.Errorf("Failed to get Lopi network nodes")
		log.WithFields(logFields).Error(err.Error())
		log.WithFields(logFields).Error(clientErr.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+clientErr.Error(), http.StatusInternalServerError)
		return
	}

	responseBytes, err := protojson.Marshal(nodes)
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

// Sets project description for the dataset given as 'id'
func (d *DirectoryServerCore) setProjectDescription(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	/* Front end needs to send in json object with project description, with application/json in header */
	clientReq := struct {
		ProjectDescription string `json:"project_description"`
	}{}

	if err := util.DecodeJSONBody(w, r, "application/json", &clientReq); err != nil {
		log.WithFields(logFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError) + " : " + err.Error(), http.StatusInternalServerError)
		return
	}

	dataset := mux.Vars(r)["id"]

	exists, err := d.dsLookupService.DatasetNodeExists(context.Background(), dataset)
	if err != nil {
		clientErr := fmt.Errorf("Failed to update project description for dataset '%s'", dataset)
		log.WithFields(logFields).Error(clientErr.Error())
		log.WithFields(logFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+clientErr.Error(), http.StatusInternalServerError)
		return
	}

	if exists {
		if err := d.datasetDescriptionManager.dbSetProjectDescription(dataset, clientReq.ProjectDescription); err != nil {
			clientErr := fmt.Errorf("Failed to update project description for dataset '%s'", dataset)
			log.WithFields(logFields).Error(clientErr.Error())
			log.WithFields(logFields).Error(err.Error())
			http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+clientErr.Error(), http.StatusInternalServerError)
			return
		}
	} else {
		err := fmt.Errorf("Dataset '%s' does not exist", dataset)
		log.WithFields(logFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusNotFound) + " : " + err.Error(), http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Succsessfully updated project description for dataset '%s'", dataset)
}

// Gets project description form the dataset given as 'id'
// TODO: fail gracefully for nil project description
func (d *DirectoryServerCore) getProjectDescription(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	dataset := mux.Vars(r)["id"]

	exists, err := d.dsLookupService.DatasetNodeExists(context.Background(), dataset)
	if err != nil {
		clientErr := fmt.Errorf("Failed to get project description for dataset '%s'", dataset)
		log.WithFields(logFields).Error(clientErr.Error())
		log.WithFields(logFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+clientErr.Error(), http.StatusInternalServerError)
		return
	}

	if !exists {
		err := fmt.Errorf("Dataset '%s' does not exist", dataset)
		log.WithFields(logFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusNotFound) + " : " + err.Error(), http.StatusNotFound)
		return
	}

	// Project description as argument to updatePD, string?
	pd, err := d.datasetDescriptionManager.dbGetProjectDescription(dataset)
	if err != nil {
		clientErr := fmt.Errorf("Failed to get project description for dataset '%s'", dataset)
		log.WithFields(logFields).Error(clientErr.Error())
		log.WithFields(logFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+clientErr.Error(), http.StatusInternalServerError)
	}

	response := struct {
		ProjectDescription string `json:"project_description"`
	}{
		ProjectDescription: pd,
	}

	b := new(bytes.Buffer)
	if err := json.NewEncoder(b).Encode(response); err != nil {
		log.WithFields(logFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	r.Header.Add("Content-Length", strconv.Itoa(len(b.Bytes())))

	_, err = w.Write(b.Bytes())
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
}

// Lazily fetch objects from all the nodes
func (d *DirectoryServerCore) getDatasetIdentifiers(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	//	ctx, cancel := context.WithDeadline(r.Context(), time.Now().Add(time.Second * 10))
	//	defer cancel()

	cursor := mux.Vars(r)["cursor"]
	limit := mux.Vars(r)["limit"]

	log.WithFields(logFields).
		WithField("Cursor:", cursor).
		WithField("Limit:", limit).
		Info("Selecting dataset identifiers")

	c, err := strconv.Atoi(cursor)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusBadRequest) + " : " + err.Error(), http.StatusBadRequest)
		return
	}

	l, err := strconv.Atoi(limit)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusBadRequest) + " : " + err.Error(), http.StatusBadRequest)
		return
	}

	ids, err := d.dsLookupService.DatasetIdentifiers(context.Background(), c, l)
	if err != nil {
		clientErr := fmt.Errorf("Failed to get dataset identifiers")
		log.WithFields(logFields).Error(err.Error())
		log.WithFields(logFields).Error(clientErr.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+clientErr.Error(), http.StatusInternalServerError)
		return
	}

	// Destination struct
	resp := struct {
		Identifiers []string
	}{
		Identifiers: ids,
	}

	b := new(bytes.Buffer)
	if err := json.NewEncoder(b).Encode(resp); err != nil {
		log.WithFields(logFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	r.Header.Add("Content-Length", strconv.Itoa(len(b.Bytes())))

	_, err = w.Write(b.Bytes())
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
}

// Fetches the information about a dataset
func (d *DirectoryServerCore) getDatasetMetadata(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	datasetId := mux.Vars(r)["id"]

	node, err := d.dsLookupService.DatasetLookupNode(context.Background(), datasetId)
	if err != nil {
		clientErr := fmt.Errorf("Failed to get dataset metadata of dataset '%s'", datasetId)
		log.WithFields(logFields).Error(clientErr.Error())
		log.WithFields(logFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+clientErr.Error(), http.StatusInternalServerError)
		return
	}

	if node == nil {
		err := fmt.Errorf("Dataset '%s' does not exist", datasetId)
		log.WithFields(logFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusNotFound) + " : " + err.Error(), http.StatusNotFound)
		return
	}

	req := &http.Request{
		Method: "GET",
		URL: &url.URL{
			Scheme: "http", //https
			Host:   node.GetHttpsAddress() + ":" + strconv.Itoa(int(node.GetPort())),
			Path:   "/dataset/metadata/" + datasetId,
		},
		Header: http.Header{},
	}

	client := &http.Client{}
	metadataResp, err := client.Do(req)
	if err != nil {
		clientErr := fmt.Errorf("Service is not available")
		log.WithFields(logFields).Error(err.Error())
		log.WithFields(logFields).Error(clientErr.Error())
		http.Error(w, http.StatusText(http.StatusServiceUnavailable)+": "+clientErr.Error(), http.StatusServiceUnavailable)
		return
	}

	defer metadataResp.Body.Close()

	reader := bufio.NewReader(metadataResp.Body)

	if metadataResp.StatusCode != http.StatusOK {
		log.WithFields(logFields).Infof("Expected 200 OK, got %v", http.StatusText(metadataResp.StatusCode))
		http.Error(w, "", metadataResp.StatusCode)
		_, err := io.Copy(w, reader)
		if err != nil {
			log.WithFields(logFields).Error(err.Error())
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
		return
	}

	if err := util.StreamToResponseWriter(reader, w, 100*1024); err != nil {
		clientErr := fmt.Errorf("Failed to fetch dataset metadata of dataset '%s'", datasetId)
		log.WithFields(logFields).Error(err.Error())
		log.WithFields(logFields).Error(clientErr.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+clientErr.Error(), http.StatusInternalServerError)
		return
	}
}

// Handler used to fetch an entire dataset. It looks up the storage node, performs a stream to the client and registers the
// dataset checkout.
func (d *DirectoryServerCore) getDataset(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	// Get dataset identifier
	datasetId := mux.Vars(r)["id"]

	node, err := d.dsLookupService.DatasetLookupNode(context.Background(), datasetId)
	if err != nil {
		clientErr := fmt.Errorf("Failed to get dataset metadata of dataset '%s'", datasetId)
		log.WithFields(logFields).Error(clientErr.Error())
		log.WithFields(logFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+clientErr.Error(), http.StatusInternalServerError)
		return
	}

	if node == nil {
		err := fmt.Errorf("Dataset '%s' does not exist", datasetId)
		log.WithFields(logFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusNotFound) + " : " + err.Error(), http.StatusNotFound)
		return
	}

	// Note: a lot can go wrong between start of download until checkout has been registered
	// Download dataset
	if err := d.downloadDataset(w, r, node, datasetId); err != nil {
		clientErr := fmt.Errorf("Failed to get dataset %s", datasetId)
		log.WithFields(logFields).Error(err.Error())
		log.WithFields(logFields).Error(clientErr.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+clientErr.Error(), http.StatusInternalServerError)
		return
	}

	// Register dataset checkout
	if err := d.registerDatasetCheckout(r, node, datasetId); err != nil {
		clientErr := fmt.Errorf("Failed to get dataset %s", datasetId)
		log.WithFields(logFields).Error(err.Error())
		log.WithFields(logFields).Error(clientErr.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+clientErr.Error(), http.StatusInternalServerError)
		return
	}
}

// Registers a dataset checkout.
func (d *DirectoryServerCore) registerDatasetCheckout(r *http.Request, node *pb.Node, datasetId string) error {
	// Fetch the protobuf representation of a client, given the JWT token
	pbClient, err := util.ExstractPbClient(r)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		return err
	}

	// Fetch the current policy from the node. Register it when checking out dataset
	policyRequest := &http.Request{
		Method: "GET",
		URL: &url.URL{
			Scheme: "http", //https
			Host:   node.GetHttpsAddress() + ":" + strconv.Itoa(int(node.GetPort())),
			Path:   "/dataset/policy/" + datasetId,
		},
		Header: http.Header{},
	}

	// Prepare the HTTP request
	client := &http.Client{}
	policyResp, err := client.Do(policyRequest)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		return err
	}

	defer policyResp.Body.Close()

	if policyResp.StatusCode != http.StatusOK {
		err := fmt.Errorf("Expected 200 OK, got %d", policyResp.StatusCode)
		log.WithFields(logFields).Error(err.Error())
		return err
	}

	responseBytes, err := io.ReadAll(policyResp.Body)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		return err
	}

	policyResponse := &pb.Policy{}
	opts := protojson.UnmarshalOptions{DiscardUnknown: true}
	if err := opts.Unmarshal(responseBytes, policyResponse); err != nil {
		log.WithFields(logFields).Error(err.Error())
		return err
	}

	dsCheckout := &pb.DatasetCheckout{
		DatasetIdentifier: datasetId,
		DateCheckout: timestamppb.Now(),
		Client: pbClient,
		ClientPolicyVersion: policyResponse.GetVersion(),
		DateLatestVerification: timestamppb.Now(),
	}

	// Dataset checkout commit
	if err := d.checkoutManager.CheckoutDataset(context.Background(), datasetId, dsCheckout, policyResponse.GetVersion(), policyResponse.GetContent()); err != nil {
		log.WithFields(logFields).Error(err.Error())
		return err
	}

	return nil
}

func (d *DirectoryServerCore) getNumDatasets(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	n, err := d.dsLookupService.NumberOfDatasets(context.Background())
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError) + " : " + err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "%d", n)
}

func (d *DirectoryServerCore) downloadDataset(w http.ResponseWriter, r *http.Request, node *pb.Node, datasetId string) error {
	// Prepare the request. Beginning of pipeline
	datasetReq := &http.Request{
		Method: "GET",
		URL: &url.URL{
			Scheme: "http", //https
			Host:   node.GetHttpsAddress() + ":" + strconv.Itoa(int(node.GetPort())),
			Path:   "/dataset/data/" + datasetId,
		},
		Header: http.Header{},
	}

	// Shallow copy headers
	datasetReq.Header = r.Header

	client := &http.Client{}
	datasetResp, err := client.Do(datasetReq)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		return err
	}

	defer datasetResp.Body.Close()

	reader := bufio.NewReader(datasetResp.Body)

	// Might receive another response code than expected
	if datasetResp.StatusCode != http.StatusOK {
		log.WithFields(logFields).Infof("Expected 200 OK, got %v", http.StatusText(datasetResp.StatusCode))
		http.Error(w, "", datasetResp.StatusCode)
		_, err := io.Copy(w, reader)
		if err != nil {
			log.WithFields(logFields).Error(err.Error())
			return err
		}
		return nil
	}

	// Stream only if we got a 200 OK from the node
	if err := util.StreamToResponseWriter(reader, w, 1000*1024); err != nil {
		log.WithFields(logFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError) + " : " + err.Error(), http.StatusInternalServerError)
		return err
	}

	return nil
}

func (d *DirectoryServerCore) getDatasetPolicy(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	pbClient, err := util.ExstractPbClient(r)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError) + " : " + err.Error(), http.StatusInternalServerError)
		return
	}

	datasetId := mux.Vars(r)["id"]

	// Check if sdataset is checked out by this client
	isCheckedOut, err := d.checkoutManager.DatasetIsCheckedOutByClient(context.Background(), datasetId, pbClient)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError) + " : " + err.Error(), http.StatusInternalServerError)
		return
	}

	// It has not been checked out by this client.
	if !isCheckedOut {
		resp := fmt.Sprintf("Dataset with identifier '%s' is not checked out", datasetId)
		log.WithFields(logFields).Info(resp)
		http.Error(w, http.StatusText(http.StatusNotFound)+": "+resp, http.StatusNotFound)
		return
	}

	version, policy, err := d.checkoutManager.GetDatasetSystemPolicy(context.Background(), datasetId)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError) + " : " + err.Error(), http.StatusInternalServerError)
		return
	}

	b := new(bytes.Buffer)
	reply := struct {
		Policy        bool
		PolicyVersion uint64
	}{
		Policy:        policy,
		PolicyVersion: version,
	}

	if err := json.NewEncoder(b).Encode(reply); err != nil {
		log.WithFields(logFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Add("Content-Length", strconv.Itoa(len(b.Bytes())))

	_, err = w.Write(b.Bytes())
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError) + " : " + err.Error(), http.StatusInternalServerError)
		return
	}
}

func (d *DirectoryServerCore) checkinDataset(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	datasetId := mux.Vars(r)["id"]

	pbClient, err := util.ExstractPbClient(r)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError) + " : " + err.Error(), http.StatusInternalServerError)
		return
	}

	// Check if sdataset is checked out at all
	isCheckedOut, err := d.checkoutManager.DatasetIsCheckedOutByClient(context.Background(), datasetId, pbClient)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError) + " : " + err.Error(), http.StatusInternalServerError)
		return
	}

	// It has not been checked out by this client. However, issue the same request to the node to
	// ensure consistency.
	if isCheckedOut {
		// If it was checked out, check it in
		if err := d.checkoutManager.CheckinDataset(context.Background(), datasetId, pbClient); err != nil {
			clientErr := fmt.Errorf("Could not checkin dataset with identifier '%s'")
			log.WithFields(logFields).Error(err.Error())
			http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+clientErr.Error(), http.StatusInternalServerError)
			return
		}
	}

	// Issue dataset checkin at the node
	node, err := d.dsLookupService.DatasetLookupNode(context.Background(), datasetId)
	if err != nil {
		clientErr := fmt.Errorf("Failed to get dataset metadata of dataset '%s'", datasetId)
		log.WithFields(logFields).Error(clientErr.Error())
		log.WithFields(logFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+clientErr.Error(), http.StatusInternalServerError)
		return
	}

	if node == nil {
		err := fmt.Errorf("Dataset '%s' does not exist", datasetId)
		log.WithFields(logFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusNotFound) + " : " + err.Error(), http.StatusNotFound)
		return
	}

	req := &http.Request{
		Method: "GET",
		URL: &url.URL{
			Scheme: "http", //https
			Host:   node.GetHttpsAddress() + ":" + strconv.Itoa(int(node.GetPort())),
			Path:   "/dataset/check_in/" + datasetId,
		},
		Header: http.Header{},
	}

	// Copy headers to pass onto the node
	req.Header = r.Header

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		clientErr := fmt.Errorf("Service is not available")
		log.WithFields(logFields).Error(err.Error())
		log.WithFields(logFields).Error(clientErr.Error())
		http.Error(w, http.StatusText(http.StatusServiceUnavailable)+": "+clientErr.Error(), http.StatusServiceUnavailable)
		return
	}

	defer resp.Body.Close()

	reader := bufio.NewReader(resp.Body)

	log.WithFields(logFields).Infof("Expected 200 OK, got %v", http.StatusText(resp.StatusCode))
	http.Error(w, "", resp.StatusCode)
	_, err = io.Copy(w, reader)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
		return
	}

	fmt.Fprintf(w, "Succsessfully checked in dataset '%s'\n", datasetId)
}

func (d *DirectoryServerCore) getDatasetCheckouts(w http.ResponseWriter, r *http.Request) {
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
		http.Error(w, http.StatusText(http.StatusBadRequest) + " : " + err.Error(), http.StatusBadRequest)
		return
	}

	l, err := strconv.Atoi(limit)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusBadRequest) + " : " + err.Error(), http.StatusBadRequest)
		return
	}

	datasetCheckouts, err := d.checkoutManager.DatasetCheckouts(context.Background(), datasetId, c, l)
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

func (d *DirectoryServerCore) getNumDatasetCheckout(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	datasetId := mux.Vars(r)["id"]

	n, err := d.checkoutManager.NumberOfDatasetCheckouts(context.Background(), datasetId)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError) + " : " + err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "%d", n)
}

func (d *DirectoryServerCore) getNumNetworkNodes(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	n, err := d.memManager.NumNetworkMembers(context.Background())
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError) + " : " + err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "%d", n)
}

func (d *DirectoryServerCore) getDatasetCompliance(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	datasetId := mux.Vars(r)["id"]
	clientPolicyVersion, err := strconv.Atoi(r.URL.Query().Get("version"))
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusBadRequest) + " : " + err.Error(), http.StatusBadRequest)
		return
	}

	pbClient, err := util.ExstractPbClient(r)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError) + " : " + err.Error(), http.StatusInternalServerError)
		return
	}

	// Check if sdataset is checked out by this client
	isCheckedOut, err := d.checkoutManager.DatasetIsCheckedOutByClient(context.Background(), datasetId, pbClient)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError) + " : " + err.Error(), http.StatusInternalServerError)
		return
	}

	// Dataset hasn't been checked out by this client
	if !isCheckedOut {
		resp := fmt.Sprintf("Dataset with identifier '%s' is not checked out", datasetId)
		log.WithFields(logFields).Info(resp)
		http.Error(w, http.StatusText(http.StatusNotFound)+": "+resp, http.StatusNotFound)
		return
	}

	version, _, err := d.checkoutManager.GetDatasetSystemPolicy(context.Background(), datasetId)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError) + " : " + err.Error(), http.StatusInternalServerError)
		return
	}

	// Response to client
	resp := struct {
		Message   string
		Compliant bool
	}{}

	if version == uint64(clientPolicyVersion) {
		resp.Message = fmt.Sprintf("Supplied policy version %d is equal to expected policy version", clientPolicyVersion)
		resp.Compliant = true
	} else {
		resp.Compliant = false
		resp.Message = fmt.Sprintf("Supplied policy version %d is not equal to expected policy version", clientPolicyVersion)
	}

	b := new(bytes.Buffer)
	if err := json.NewEncoder(b).Encode(resp); err != nil {
		log.WithFields(logFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	r.Header.Add("Content-Length", strconv.Itoa(len(b.Bytes())))

	// TODO: Update verification date. See wiki for more TODOs

	_, err = w.Write(b.Bytes())
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
}
