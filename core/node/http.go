package node


import (
//	"bytes"
//	"context"
	"log"
	"net/http"
_	"net/url"
_	"fmt"
_	"time"

	"github.com/gorilla/mux"
	"github.com/tomcat-bit/lohpi/core/comm"
)

const PROJECTS_DIRECTORY = "projects"

func (n *Node) startHttpServer() error {
	router := mux.NewRouter()
	log.Printf("%s: Started HTTP server on port %d\n", n.name, n.config.HttpPort)

	router.HandleFunc("/archive/{remote-url:.*}", n.remoteArchive).Methods("GET")
	router.HandleFunc("/metadata/{remote-url:.*}", n.remoteMetadata).Methods("GET")

	n.httpServer = &http.Server{
		Handler: router,
		TLSConfig: comm.ServerConfig(n.cu.Certificate(), n.cu.CaCertificate(), n.cu.Priv()), 
	}

	err := n.httpServer.Serve(n.httpListener)
	if err != nil {
		log.Println(err.Error())
		return err
	}
	return nil
}

func (n *Node) remoteArchive(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	// Fetch the URL
  	remoteUrl := r.URL.Query().Get("remote-url")
  	if remoteUrl == "" {
		log.Println("Missing URL")
		return
  	}

	http.Redirect(w, r, remoteUrl, http.StatusFound)
}

func (n *Node) remoteMetadata(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	// Fetch the URL
  	remoteUrl := r.URL.Query().Get("remote-url")
  	if remoteUrl == "" {
		log.Println("Missing URL")
		return
  	}

	http.Redirect(w, r, remoteUrl, http.StatusFound)
}
