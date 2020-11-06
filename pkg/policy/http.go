package policy

import (
	_"fmt"
	"encoding/json"
	"log"
	"net/http"


	"github.com/tomcat-bit/lohpi/pkg/netutil"
	"github.com/gorilla/mux"
)

func (ps *PolicyStore) startHttpListener(port int) error {
	// Initiate HTTP connection
	httpListener, err := netutil.ListenOnPort(port)
	if err != nil {
		return err
	}
	ps.httpListener = httpListener

	router := mux.NewRouter()
	router.HandleFunc("/objects", ps.getObjectHeaderNames).Methods("GET")
	/*s := router.PathPrefix("/objects").Subrouter()
	s.HandleFunc("/", ps.getObjectHeaderNames)
	s.HandleFunc("/{key}/", ps.getObjectHeader)*/
	//s.HandleFunc("/{key}/policy", ps.getObjectPolicy)

	ps.httpServer = &http.Server{
		Handler: router,
		// use timeouts?
	}

	err = ps.httpServer.Serve(ps.httpListener)
	if err != nil {
		log.Println(err.Error())
	}

	return nil
}

func (ps *PolicyStore) getObjectHeader(w http.ResponseWriter, r *http.Request) {

}

func (ps *PolicyStore) getObjectHeaderNames(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	w.WriteHeader(http.StatusOK)
	headers := ps.cache.ObjectHeaders()

	if len(headers) == 0 {
		w.WriteHeader(http.StatusOK)
		return
	}

	name := struct {
		Names []string `json:"names"`
	}{}

	name.Names = make([]string, 0)

	for _, h := range headers {
		name.Names = append(name.Names, h.GetName())
	}

	js, err := json.Marshal(name)
	if err != nil {
	  http.Error(w, err.Error(), http.StatusInternalServerError)
	  return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(js)
}