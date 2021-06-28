package policystore

import (
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
	"github.com/rs/cors"
	log "github.com/sirupsen/logrus"
	"net/http"
	"strconv"
	"strings"
	"time"
	pbtime "google.golang.org/protobuf/types/known/timestamppb"
)

func (ps *PolicyStoreCore) startHttpServer(addr string) error {
	m := mux.NewRouter()
	router := m.PathPrefix("/dataset").Schemes("HTTP").Subrouter()
	router.HandleFunc("/identifiers", ps.getDatasetIdentifiers).Methods("GET")
	router.HandleFunc("/metadata/{id:.*}", ps.getDatasetMetadata).Methods("GET")
	router.HandleFunc("/getpolicy/{id:.*}", ps.getObjectPolicy).Methods("GET")
	router.HandleFunc("/setpolicy/{id:.*}", ps.setObjectPolicy).Methods("PUT")
	//dRouter.HandleFunc("/probe}", ps.probe).Methods("GET")

	handler := cors.AllowAll().Handler(m)

	ps.httpServer = &http.Server{
		Addr:         addr,
		Handler:      handler,
		WriteTimeout: time.Second * 30,
		ReadTimeout:  time.Second * 30,
		IdleTimeout:  time.Second * 60,
		TLSConfig:    comm.ServerConfig(ps.cu.Certificate(), ps.cu.CaCertificate(), ps.cu.PrivateKey()),
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
			http.Error(w, http.StatusText(http.StatusBadRequest)+": "+err.Error(), http.StatusBadRequest)
			return
		}

		if err := ps.validateTokenSignature(token); err != nil {
			http.Error(w, http.StatusText(http.StatusBadRequest)+": "+err.Error(), http.StatusBadRequest)
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

// Returns the dataset identifiers stored in the network
func (ps *PolicyStoreCore) getDatasetIdentifiers(w http.ResponseWriter, r *http.Request) {
	log.Infoln("Got request to", r.URL.String())
	defer r.Body.Close()

	respBody := struct {
		Identifiers []string
	}{
		Identifiers: make([]string, 0),
	}

	for i := range ps.memCache.DatasetNodes() {
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
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+err.Error(), http.StatusInternalServerError)
		return
	}
}

// Returns the policy associated with the dataset
func (ps *PolicyStoreCore) getObjectPolicy(w http.ResponseWriter, r *http.Request) {
	log.Infoln("Got request to", r.URL.String())
	defer r.Body.Close()

	datasetId := mux.Vars(r)["id"]
	if datasetId == "" {
		err := errors.New("Missing dataset identifier")
		log.Infoln(err.Error())
		http.Error(w, http.StatusText(http.StatusBadRequest)+": "+err.Error(), http.StatusBadRequest)
		return
	}

	// Get the node that stores the dataset
	policy := ps.dsManager.DatasetPolicy(datasetId)
	if policy == nil {
		err := fmt.Errorf("Dataset '%s' was not found", datasetId)
		log.Infoln(err.Error())
		http.Error(w, http.StatusText(http.StatusNotFound)+": "+err.Error(), http.StatusNotFound)
		return
	}

	// Destination struct
	resp := struct {
		Policy string
	}{}

	resp.Policy = strconv.FormatBool(policy.GetContent())

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

// Returns the metadata assoicated with the dataset
func (ps *PolicyStoreCore) getDatasetMetadata(w http.ResponseWriter, r *http.Request) {
	log.Infoln("Got request to", r.URL.String())
	defer r.Body.Close()

	err := errors.New("getDatasetMetadata is not implemented")
	log.Warnln(err.Error())
	http.Error(w, http.StatusText(http.StatusNotImplemented)+": "+err.Error(), http.StatusNotImplemented)
}

// Assigns a new policy to the dataset
func (ps *PolicyStoreCore) setObjectPolicy(w http.ResponseWriter, r *http.Request) {
	log.Infoln("Got request to", r.URL.String())
	defer r.Body.Close()

	datasetId := mux.Vars(r)["id"]
	if datasetId == "" {
		err := errors.New("Missing dataset identifier")
		log.Infoln(err.Error())
		http.Error(w, http.StatusText(http.StatusBadRequest)+": "+err.Error(), http.StatusBadRequest)
		return
	}

	// Get the node that stores the dataset
	node, exists := ps.memCache.DatasetNodes()[datasetId]
	if !exists {
		err := fmt.Errorf("Dataset '%s' was not found", datasetId)
		log.Infoln(err.Error())
		http.Error(w, http.StatusText(http.StatusNotFound)+": "+err.Error(), http.StatusNotFound)
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

	var version uint64 = 1
	// Get the latest policy and increment the version number by one.
	if currentPolicy := ps.dsManager.DatasetPolicy(datasetId); currentPolicy != nil {
		version = currentPolicy.GetVersion()
	} 

	policy := &pb.Policy{
		Issuer:           	ps.PolicyStoreConfig().Name, // should get name of client instead
		DatasetIdentifier: 	datasetId,
		Content:          	reqBody.Policy,
		Version: 		  	version + 1,
		DateCreated: 	  	pbtime.Now(),
	}

	// Store the dataset entry in map
	if err := ps.dsManager.SetDatasetPolicy(datasetId, policy); err != nil {
		log.Infoln(err.Error())
		http.Error(w, http.StatusText(http.StatusNotFound)+": "+err.Error(), http.StatusNotFound)
		return
	}

	// Store the dataset policy in Git
	if err := ps.gitStorePolicy(node.GetName(), datasetId, policy); err != nil {
		log.Errorln(err.Error())
		http.Error(w, http.StatusText(http.StatusBadRequest)+": "+err.Error(), http.StatusBadRequest)
		return
	}

	go ps.submitPolicyForDistribution(policy)

	respMsg := "Successfully set a new policy for " + datasetId + "\n"
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/text")
	w.Header().Set("Content-Length", strconv.Itoa(len(respMsg)))
	w.Write([]byte(respMsg))
}
