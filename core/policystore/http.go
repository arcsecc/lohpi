package policystore

import (
	"bytes"
	"context"
	"encoding/json"
	pbtime "google.golang.org/protobuf/types/known/timestamppb"
	"errors"
	"fmt"
	"github.com/arcsecc/lohpi/core/comm"
	"github.com/arcsecc/lohpi/core/util"
	"github.com/arcsecc/lohpi/core/policystore/multicast"
	pb "github.com/arcsecc/lohpi/protobuf"
	"github.com/gorilla/mux"
	"github.com/lestrrat-go/jwx/jwa"
	"github.com/lestrrat-go/jwx/jwk"
	"github.com/lestrrat-go/jwx/jws"
	"github.com/rs/cors"
	log "github.com/sirupsen/logrus"
	"net/http"
	"strconv"
	"strings"
	"time"
//	pbtime "google.golang.org/protobuf/types/known/timestamppb"
)

func (ps *PolicyStoreCore) startHttpServer(addr string) error {
	m := mux.NewRouter()
	datasetRouter := m.PathPrefix("/dataset").Schemes("HTTP").Subrouter()
	datasetRouter.HandleFunc("/identifiers", ps.getDatasetIdentifiers).Methods("GET")
	datasetRouter.HandleFunc("/metadata/{id:.*}", ps.getDatasetMetadata).Methods("GET")
	datasetRouter.HandleFunc("/getpolicy/{id:.*}", ps.getObjectPolicy).Methods("GET")
	datasetRouter.HandleFunc("/setpolicy/{id:.*}", ps.setObjectPolicy).Methods("PUT")
	//dRouter.HandleFunc("/probe}", ps.probe).Methods("GET")

	evalRouter 	:= m.PathPrefix("/evaluation").Schemes("HTTP").Subrouter()
	evalRouter.HandleFunc("/gossip", ps.gossip).Methods("GET")

	configRouter := m.PathPrefix("/config").Schemes("HTTP").Subrouter()
	configRouter.HandleFunc("/setconfig", ps.setMulticastConfigHandler).Methods("PUT")
	configRouter.HandleFunc("/getconfig", ps.getMulticastConfigHandler).Methods("GET")

	handler := cors.AllowAll().Handler(m)

	serverConfig, err := comm.ServerConfig(ps.cm.Certificate(), ps.cm.CaCertificate(), ps.cm.PrivateKey())
	if err != nil {
		return err
	}

	ps.httpServer = &http.Server{
		Addr: addr,
		Handler: handler,
		WriteTimeout: time.Second * 30,
		ReadTimeout: time.Second * 30,
		IdleTimeout: time.Second * 60,
		TLSConfig: serverConfig,
	}

	//router.Use(ps.middlewareValidateTokenSignature)

	if err := ps.setPublicKeyCache(); err != nil {
		log.Errorln(err.Error())
		return err
	}

	log.Infoln("Started HTTP server at " + addr)
	return ps.httpServer.ListenAndServe()
}

func (ps *PolicyStoreCore) setPublicKeyCache() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ps.ar = jwk.NewAutoRefresh(ctx)
	const msCerts = "https://login.microsoftonline.com/common/discovery/v2.0/keys" // TODO: config me

	ps.ar.Configure(msCerts, jwk.WithRefreshInterval(time.Minute*5))

	// Keep the cache warm
	_, err := ps.ar.Refresh(ctx, msCerts)
	if err != nil {
		log.Println("Failed to refresh Microsoft Azure JWKS")
		return err
	}
	return nil
}

func (ps *PolicyStoreCore) middlewareValidateTokenSignature(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, err := getBearerToken(r)
		if err != nil {
			http.Error(w, http.StatusText(http.StatusBadRequest)+ ": " + err.Error(), http.StatusBadRequest)
			return
		}

		if err := ps.validateTokenSignature(token); err != nil {
			http.Error(w, http.StatusText(http.StatusBadRequest)+ ": " + err.Error(), http.StatusBadRequest)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func (ps *PolicyStoreCore) validateTokenSignature(token []byte) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// TODO: fetch keys again if it fails
	// TODO: add clock skew. Use jwt.WithAcceptableSkew
	set, err := ps.ar.Fetch(ctx, "https://login.microsoftonline.com/common/discovery/v2.0/keys")
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
	set, err := m.ar.Refresh(ctx, msCerts)
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

func (ps *PolicyStoreCore) gossip(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	log.Println("Started gossiping")
	fmt.Fprintln(w, "Started gossiping...")
	ps.ifritClient.SetGossipContent([]byte("Some gossip content"))
}

func (ps *PolicyStoreCore) setMulticastConfigHandler(w http.ResponseWriter, r *http.Request) {
	log.Infoln("Got request to", r.URL.String())
	defer r.Body.Close()

	reqBody := struct {
		Sigma int
		Tau int
		Phi float64
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

	config := &multicast.MulticastConfig{
		MulticastDirectRecipients: reqBody.Sigma,
		AcceptanceLevel: reqBody.Phi,
	}
	ps.multicastManager.SetMulticastConfiguration(config)
	ps.PolicyStoreConfig().GossipInterval = time.Duration(reqBody.Tau) * time.Second

	respString := fmt.Sprintf(`Succsessfully set new multicast parameters for policy store. 
		Sigma: %d\tTau: %d\tPhi: %f`, reqBody.Sigma, time.Duration(reqBody.Tau) * time.Second, reqBody.Phi)
	log.Info(respString)
	fmt.Fprintf(w, "%s\n", respString)
}

func (ps *PolicyStoreCore) getMulticastConfigHandler(w http.ResponseWriter, r *http.Request) {
	log.Infoln("Got request to", r.URL.String())
	defer r.Body.Close()

	config := ps.multicastManager.GetMulticastConfiguration()
	respString := fmt.Sprintf("Current multicast parameters for policy store: Sigma: %d\tTau: %f\tPhi: %f", config.MulticastDirectRecipients, ps.PolicyStoreConfig().GossipInterval.Seconds(), config.AcceptanceLevel)
	log.Info(respString)
	fmt.Fprintf(w, "%s\n", respString)
}

// Returns the dataset identifiers stored in the network
func (ps *PolicyStoreCore) getDatasetIdentifiers(w http.ResponseWriter, r *http.Request) {
	log.Infoln("Got request to", r.URL.String())
	defer r.Body.Close()

	respBody := struct {
		Identifiers []string
	}{
		Identifiers: ps.dsLookupService.DatasetIdentifiers(),
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
		http.Error(w, http.StatusText(http.StatusInternalServerError)+ ": " + err.Error(), http.StatusInternalServerError)
		return
	}
}

// Returns the policy associated with the dataset
func (ps *PolicyStoreCore) getObjectPolicy(w http.ResponseWriter, r *http.Request) {
	log.Infoln("Got request to", r.URL.String())
	defer r.Body.Close()

	datasetId := mux.Vars(r)["id"]

	// Get the node that stores the dataset
	ds := ps.dsManager.Dataset(datasetId)
	if ds == nil {
		err := fmt.Errorf("Dataset '%s' was not found", datasetId)
		log.Infoln(err.Error())
		http.Error(w, http.StatusText(http.StatusNotFound)+ ": " + err.Error(), http.StatusNotFound)
		return
	}

	policy := ds.GetPolicy()
	if policy == nil {
		err := fmt.Errorf("Policy for dataset '%s' was not found", datasetId)
		log.Infoln(err.Error())
		http.Error(w, http.StatusText(http.StatusNotFound)+ ": " + err.Error(), http.StatusNotFound)
		return
	}

	// Destination struct
	resp := struct {
		Policy string
	}{
		Policy: strconv.FormatBool(policy.GetContent()),
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
		http.Error(w, http.StatusText(http.StatusInternalServerError)+ ": " + err.Error(), http.StatusInternalServerError)
		return
	}
}

// Returns the metadata assoicated with the dataset
func (ps *PolicyStoreCore) getDatasetMetadata(w http.ResponseWriter, r *http.Request) {
	log.Infoln("Got request to", r.URL.String())
	defer r.Body.Close()

	err := errors.New("getDatasetMetadata is not implemented")
	log.Warnln(err.Error())
	http.Error(w, http.StatusText(http.StatusNotImplemented)+ ": " + err.Error(), http.StatusNotImplemented)
}

// Assigns a new policy to the dataset
// TODO: rollback everything if something fails
func (ps *PolicyStoreCore) setObjectPolicy(w http.ResponseWriter, r *http.Request) {
	log.Infoln("Got request to", r.URL.String())
	defer r.Body.Close()

	datasetId := mux.Vars(r)["id"]
	dataset := ps.dsManager.Dataset(datasetId)
	if dataset == nil {
		err := fmt.Errorf("Dataset '%s' is not stored in the network", datasetId)
		log.Error(err.Error())
		http.Error(w, http.StatusText(http.StatusNotFound)+ ": " + err.Error(), http.StatusNotFound)
		return
	}

	// Destination struct. Require only booleans for now
	reqBody := struct {
		Policy bool
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

	// Get the latest policy and increment the version number by one.
	oldPolicy := dataset.GetPolicy()
	if oldPolicy == nil {
		err := fmt.Errorf("Policy for dataset '%s' was not found", datasetId)
		log.Infoln(err.Error())
		http.Error(w, http.StatusText(http.StatusNotFound)+ ": " + err.Error(), http.StatusNotFound)
		return
	}

	newPolicy := &pb.Policy{
		DatasetIdentifier: 	datasetId,
		Content:          	reqBody.Policy,
		Version:			oldPolicy.GetVersion() + 1,
		DateCreated:		oldPolicy.GetDateCreated(),
		DateUpdated:        pbtime.Now(),
	}

	// Store the dataset entry 
	if err := ps.dsManager.SetDatasetPolicy(datasetId, newPolicy); err != nil {
		log.Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError)+ ": " + err.Error(), http.StatusInternalServerError)
		return
	}

	// Get the node's identifier
	node := ps.dsLookupService.DatasetLookupNode(datasetId)
	if node == nil {
		err := fmt.Errorf("The network node that stores the dataset is not available anymore")
		log.Errorf("A policy was set for a dataset id that does not exist. ID was %s", datasetId)
		log.Errorln(err.Error())
		http.Error(w, http.StatusText(http.StatusNotFound)+ ": " + err.Error(), http.StatusNotFound)
		return
	}

	// Store the dataset policy in Git
	if err := ps.gitStorePolicy(node.GetName(), datasetId, newPolicy); err != nil {
		log.Errorln(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError)+ ": " + err.Error(), http.StatusInternalServerError)
		return
	}

	go ps.submitPolicyForDistribution(newPolicy)

	respMsg := "Successfully set a new policy for " + datasetId + "\n"
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/text")
	w.Header().Set("Content-Length", strconv.Itoa(len(respMsg)))
	w.Write([]byte(respMsg))
}
