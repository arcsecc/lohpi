package policystore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/arcsecc/lohpi/core/comm"
	"github.com/arcsecc/lohpi/core/policystore/multicast"
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
	"package":     "core/policy",
	"description": "http server",
}

func (ps *PolicyStoreCore) startHttpServer(addr string) error {
	router := mux.NewRouter()
	datasetRouter := router.PathPrefix("/dataset").Schemes("HTTP").Subrouter()
	datasetRouter.HandleFunc("/identifiers", ps.getDatasetIdentifiers).Queries("cursor", "{cursor:[0-9,]+}", "limit", "{limit:[0-9,]+}").Methods("GET")
	datasetRouter.HandleFunc("/getpolicy/{id:.*}", ps.getObjectPolicy).Methods("GET")
	datasetRouter.HandleFunc("/setpolicy/{id:.*}", ps.setObjectPolicy).Methods("PUT")

	configRouter := router.PathPrefix("/config").Schemes("HTTP").Subrouter()
	configRouter.HandleFunc("/setconfig", ps.setMulticastConfigHandler).Methods("PUT")
	configRouter.HandleFunc("/getconfig", ps.getMulticastConfigHandler).Methods("GET")
	configRouter.HandleFunc("/reset_timer", ps.resetMulticastTimer).Methods("GET")

	evalRouter := router.PathPrefix("/evaluation").Schemes("HTTP").Subrouter()
	evalRouter.HandleFunc("/gossip", ps.gossip).Methods("GET")

	handler := cors.AllowAll().Handler(router)

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

	router.Use(ps.requestLogging)
	//router.Use(ps.middlewareValidateTokenSignature)

	if err := ps.setPublicKeyCache(); err != nil {
		log.WithFields(httpLogFields).Error(err.Error())
		return err
	}

	log.Infoln("Started HTTP server at " + addr)
	return ps.httpServer.ListenAndServe()
}

func (ps *PolicyStoreCore) requestLogging(next http.Handler) http.Handler {
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
			log.WithFields(httpLogFields).Error(err.Error())
			http.Error(w, e.Msg, e.Status)
		} else {
			log.WithFields(httpLogFields).Error(err.Error())
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

	respString := fmt.Sprintf("Succsessfully set new multicast parameters for policy store. Sigma: %d\tTau: %d\tPhi: %f", reqBody.Sigma, time.Duration(reqBody.Tau)*time.Second, reqBody.Phi)
	log.WithFields(httpLogFields).Info(respString)
	fmt.Fprintf(w, "%s\n", respString)
}

func (ps *PolicyStoreCore) getMulticastConfigHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	config := ps.multicastManager.GetMulticastConfiguration()
	respString := fmt.Sprintf("Current multicast parameters for policy store: Sigma: %d\tTau: %f\tPhi: %f", config.MulticastDirectRecipients, ps.PolicyStoreConfig().GossipInterval.Seconds(), config.AcceptanceLevel)
	log.Info(respString)
	fmt.Fprintf(w, "%s\n", respString)
}

func (ps *PolicyStoreCore) resetMulticastTimer(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
}

// Returns the dataset identifiers stored in the network
func (ps *PolicyStoreCore) getDatasetIdentifiers(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	cursor := mux.Vars(r)["cursor"]
	limit := mux.Vars(r)["limit"]

	log.WithFields(logFields).
		WithField("Cursor:", cursor).
		WithField("Limit:", limit).
		Info("Selecting dataset identifiers")

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

	ids, err := ps.dsLookupService.DatasetIdentifiers(context.Background(), c, l)
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

// Returns the policy associated with the dataset
func (ps *PolicyStoreCore) getObjectPolicy(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	datasetId := mux.Vars(r)["id"]

	// Get the node that stores the dataset
	ds, err := ps.dsManager.Dataset(context.Background(), datasetId)
	if err != nil {
		log.WithFields(httpLogFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+err.Error(), http.StatusInternalServerError)
		return
	}

	policy := ds.GetPolicy()
	if policy == nil {
		err := fmt.Errorf("Policy for dataset '%s' was not found", datasetId)
		log.WithFields(httpLogFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusNotFound)+": "+err.Error(), http.StatusNotFound)
		return
	}

	responseBytes, err := protojson.Marshal(policy)
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

// Assigns a new policy to the dataset
// TODO: rollback everything if something fails
func (ps *PolicyStoreCore) setObjectPolicy(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	datasetId := mux.Vars(r)["id"]
	dataset, err := ps.dsManager.Dataset(context.Background(), datasetId)
	if err != nil {
		log.WithFields(httpLogFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusNotFound)+": "+err.Error(), http.StatusNotFound)
		return
	}

	// Get the node's identifier
	node, err := ps.dsLookupService.DatasetLookupNode(context.Background(), datasetId)
	if err != nil {
		log.WithFields(httpLogFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Destination struct. Require only booleans for now
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

	// Get the latest policy and increment the version number by one.
	oldPolicy := dataset.GetPolicy()
	if oldPolicy == nil {
		err := fmt.Errorf("Policy for dataset '%s' was not found", datasetId)
		log.WithFields(httpLogFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusNotFound)+": "+err.Error(), http.StatusNotFound)
		return
	}

	newPolicy := &pb.Policy{
		DatasetIdentifier: datasetId,
		Content: reqBody.Policy,
		Version: oldPolicy.GetVersion() + 1,
		DateCreated: oldPolicy.GetDateCreated(),
		DateUpdated: timestamppb.Now(),
		DateApplied: timestamppb.Now(),
	}

	// Store the dataset entry
	if err := ps.dsManager.SetDatasetPolicy(context.Background(), datasetId, newPolicy); err != nil {
		log.WithFields(httpLogFields).Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Store the dataset policy in Git
	if ps.gitRepo != nil {
		if err := ps.gitRepo.StorePolicy(node.GetName(), datasetId, newPolicy); err != nil {
			log.WithFields(httpLogFields).Error(err.Error())
			http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+err.Error(), http.StatusInternalServerError)
			return
		}
	}

	ps.submitPolicyForDistribution(newPolicy)

	respMsg := "Successfully set a new policy for " + datasetId + "\n"
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/text")
	w.Header().Set("Content-Length", strconv.Itoa(len(respMsg)))
	w.Write([]byte(respMsg))
}
