package node


import (
	//"archive/zip"
	"bytes"
	"context"
	"log"
	"net/http"
	"net/url"
	"fmt"
	"time"

//	pb "github.com/tomcat-bit/lohpi/protobuf"
	"github.com/gorilla/mux"
	"github.com/tomcat-bit/lohpi/pkg/comm"
)

const PROJECTS_DIRECTORY = "projects"

func (n *Node) startHttpServer() error {
	router := mux.NewRouter()
	log.Printf("%s: Started HTTP server on port %d\n", n.name, n.config.HttpPort)

	router.HandleFunc("/archive/{remote-url}", n.remoteArchive).Methods("GET")

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
	remoteURL := mux.Vars(r)["remote-url"]
	if remoteURL == "" {
		errMsg := fmt.Errorf("Missing URL")
		http.Error(w, http.StatusText(http.StatusBadRequest) + ": " + errMsg.Error(), http.StatusBadRequest)
		return
	}

	finalURL, err := url.Parse(n.config.RemoteURL + "/" + remoteURL)
	if err != nil {
		panic(err)
	}

	// Request
	req := &http.Request{
        Method:   "GET",
        URL:      finalURL,
	}
	log.Println("URL:", req.URL)

	// Client 
	c := http.Client{
		Timeout: time.Duration(1 * time.Minute),		// TODO: set sane value
	}

	// Context
	ctx, cancel := context.WithTimeout(req.Context(), 1 * time.Hour)
	defer cancel()

	req = req.WithContext(ctx)

	res, err := c.Do(req)
	if err != nil {
		log.Fatalf("%v", err)
	}

	log.Println()

	//w.Header().Set("Content-Type", res.Header["Content-Type"])
	//w.Header().Set("Content-Length", res.Header["Content-Length"])
	// TODO: preserve headers and shit
	w.WriteHeader(res.StatusCode)
	log.Println("res.StatusCode:", res.StatusCode)
	buf := new(bytes.Buffer)
	buf.ReadFrom(res.Body)
	log.Println("res.Body:", buf.Bytes())
	w.Write(buf.Bytes())
}

