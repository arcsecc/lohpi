package mux

import (
	"context"
	"fmt"
	"net/http"
	"time"
	"errors"
	"strings"
_	"bytes"

	"github.com/gorilla/mux"
	"github.com/arcsecc/lohpi/core/comm"
	//"github.com/dgrijalva/jwt-go"
	"github.com/lestrrat-go/jwx/jwk"
	"github.com/lestrrat-go/jwx/jws"
	"github.com/lestrrat-go/jwx/jwa"

	log "github.com/sirupsen/logrus"
)

type azureConfig struct {
	appId string // Validate me!
	issuer string // Validate me?
	nonce string // Validate me!
	roles []string
}

func (m *Mux) startHttpServer(addr string) error {
	r := mux.NewRouter()
	log.Printf("MUX: Started HTTP server on port %d\n", m.config.MuxHttpPort)

	// Main dataset router exposed to the clients
	dRouter := r.PathPrefix("/dataset").Schemes("HTTP").Subrouter().SkipClean(true)
	dRouter.HandleFunc("/ids", m.getNetworkDatasetIdentifiers).Methods("GET")
	dRouter.HandleFunc("/metadata/{id:.*}", m.getDatasetMetadata).Methods("GET")
	dRouter.HandleFunc("/data/{id:.*}", m.getDataset).Methods("GET")

	// Middlewares used for validation
	//dRouter.Use(m.middlewareValidateTokenSignature)
	//dRouter.Use(m.middlewareValidateTokenClaims)

	m.httpServer = &http.Server{
		Addr: 		  	addr,
		Handler:      	r,
		WriteTimeout: 	time.Second * 30,
		ReadTimeout:  	time.Second * 30,
		IdleTimeout:  	time.Second * 60,
		TLSConfig: 		comm.ServerConfig(m.cu.Certificate(), m.cu.CaCertificate(), m. cu.Priv()),
	}

	if err := m.setPublicKeyCache(); err != nil {
		return err
	}

	return m.httpServer.ListenAndServe()
}

func (m *Mux) setPublicKeyCache() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m.ar = jwk.NewAutoRefresh(ctx)
	const msCerts = "https://login.microsoftonline.com/common/discovery/v2.0/keys" // TODO: config me

	m.ar.Configure(msCerts, jwk.WithRefreshInterval(time.Minute * 5))

	// Keep the cache warm
	_, err := m.ar.Refresh(ctx, msCerts)
	if err != nil {
		log.Println("Failed to refresh Microsoft Azure JWKS")
		return err
	}
	return nil 
}

func (m *Mux) middlewareValidateTokenSignature(next http.Handler) http.Handler {
	return http.HandlerFunc(func (w http.ResponseWriter, r *http.Request) {
		token, err := getBearerToken(r)
		if err != nil {
			http.Error(w, http.StatusText(http.StatusBadRequest) + ": " + err.Error(), http.StatusBadRequest)
			return
		}
		
		if err := m.validateTokenSignature(token); err != nil {
			http.Error(w, http.StatusText(http.StatusBadRequest) + ": " + err.Error(), http.StatusBadRequest)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func (m *Mux) middlewareValidateTokenClaims(next http.Handler) http.Handler {
	return http.HandlerFunc(func (w http.ResponseWriter, r *http.Request) {
		// TODO: parse the claims in the access token
		// Return error if there are any mismatches. Call next.ServeHTTP(w, r) otherwise
		log.Println("in middlewareValidateTokenClaims")

		next.ServeHTTP(w, r)
	})
}

func (m *Mux) validateTokenSignature(token []byte) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// TODO: fetch keys again if it fails
	// TODO: add clock skew. Use jwt.WithAcceptableSkew
	set, err := m.ar.Fetch(ctx, "https://login.microsoftonline.com/common/discovery/v2.0/keys")
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

func (m *Mux) validateTokenClaims(token []byte) error {
	return nil	
}

func getBearerToken(r *http.Request) ([]byte, error) {
	authHeader := r.Header.Get("Authorization")
	authHeaderContent := strings.Split(authHeader, " ")
	if len(authHeaderContent) != 2 {
		return nil, errors.New("Token not provided or malformed")
	}
	return []byte(authHeaderContent[1]), nil
}

func (m *Mux) shutdownHttpServer() {
	// The duration for which the server wait for open connections to finish
	wait := time.Minute * 1
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()

	// Doesn't block if no connections, but will otherwise wait
	// until the timeout deadline.
	m.httpServer.Shutdown(ctx)

	// Optionally, you could run srv.Shutdown in a goroutine and block on
	// <-ctx.Done() if your application should wait for other services
	// to finalize based on context cancellation.
	log.Println("Gracefully shutting down HTTP server")
}

// Lazily fetch objects from all the nodes
func (m *Mux) getNetworkDatasetIdentifiers(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	ctx, cancel := context.WithDeadline(r.Context(), time.Now().Add(time.Second * 10))
	defer cancel()

	errChan := make(chan error)
	setsChan := make(chan []byte)

	go func() {
		defer close(errChan)
		defer close(setsChan)
		
		r = r.WithContext(ctx)
		sets, err := m.datasetIdentifiers(ctx)	
		if err != nil {
			errChan <-err
			return
		}

		setsChan <-sets
	}()

	select {
	case <-ctx.Done():
		http.Error(w, http.StatusText(http.StatusRequestTimeout), http.StatusRequestTimeout)
		return
	case err := <-errChan:
		log.Errorln(err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	case sets := <-setsChan:
		w.WriteHeader(http.StatusOK)
		w.Header().Set("Content-Type", "application/json")
		w.Write(sets)
		return
	}
}

// Fetches the information about a dataset
func (m *Mux) getDatasetMetadata(w http.ResponseWriter, req *http.Request) {
	dataset := mux.Vars(req)["id"]
	if dataset == "" {
		errMsg := fmt.Errorf("Missing dataset identifier")
		http.Error(w, http.StatusText(http.StatusBadRequest) + ": " + errMsg.Error(), http.StatusBadRequest)
		return
	}
	
	ctx, cancel := context.WithDeadline(req.Context(), time.Now().Add(time.Second * 10))
	defer cancel()

	errChan := make(chan error)
	doneChan := make(chan bool)
	
	go func() {
		defer close(doneChan)
		defer close(errChan)

		md, err := m.datasetMetadata(w, req, dataset, ctx)
		if err != nil {
			errChan <-err
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Header().Set("Content-Type", "application/json")
		w.Write(md)
		doneChan <-true
	}()

	select {
	case <-ctx.Done():
		log.Println("Timeout!")
		http.Error(w, http.StatusText(http.StatusRequestTimeout), http.StatusRequestTimeout)
		return
	case err := <-errChan:
		log.Errorln(err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	case <-doneChan:
		return
	}
}

// Handler used to fetch an entire dataset. Writes a zip file to the client
func (m *Mux) getDataset(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()

	token, err := getBearerToken(req)
	if err != nil {
		http.Error(w, http.StatusText(http.StatusBadRequest) + ": " + err.Error(), http.StatusBadRequest)
		return
	}
	
	dataset := mux.Vars(req)["id"]
	if dataset == "" {
		err := fmt.Errorf("Missing storage identifier")
		http.Error(w, http.StatusText(http.StatusBadRequest) + ": " + err.Error(), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Minute * 5))
	defer cancel()

	m.dataset(w, req, dataset, token, ctx)
}
