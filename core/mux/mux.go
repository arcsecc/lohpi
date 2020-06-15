package mux

import (
_	"fmt"
	vlog "github.com/inconshreveable/log15"
	"github.com/spf13/viper"
	"errors"
	"net"
	"net/http"
	"crypto/tls"
	"firestore/comm"
	"firestore/netutil"
	"firestore/core/cache"
	"strings"
	"sync"
	"strconv"

	"crypto/x509/pkix"

	"github.com/joonnna/ifrit"
)

var (
	errInvalidPermission = errors.New("Requested to send an invalid permission")
)

type Mux struct {
	// Underlying Ifrit client
	ifritClient 	*ifrit.Client

	// In-memory cache structures
	nodeListLock 	sync.RWMutex
	cache			*cache.Cache

	// HTTP
	portNum 		int
	listener   		net.Listener
	httpServer 		*http.Server
	serverConfig 	*tls.Config

	// HTTP-related stuff. Used by the demonstrator using cURL
	httpPortNum 	int
	_httpListener   net.Listener
	_httpServer 	*http.Server
	
	// Sync
	wg       *sync.WaitGroup
	exitChan chan bool
}

func NewMux(httpPortNum int) (*Mux, error) {
	ifritClient, err := ifrit.NewClient()
	if err != nil {
		return nil, err
	}
	
	go ifritClient.Start()
	//ifritClient.RegisterGossipHandler(self.GossipMessageHandler)
	//ifritClient.RegisterResponseHandler(self.GossipResponseHandler)

	portString := strings.Split(viper.GetString("lohpi_mux_addr"), ":")[1]
	port, err := strconv.Atoi(portString)
	if err != nil {
		return nil, err
	}

	netutil.ValidatePortNumber(&port)
	listener, err := netutil.ListenOnPort(port)
	if err != nil {
		panic(err)
	}

	vlog.Debug("addrs", "rpc", listener.Addr().String(), "udp")

	pk := pkix.Name{
		Locality: []string{listener.Addr().String()},
	}

	cu, err := comm.NewCu(pk, viper.GetString("lohpi_ca_addr"))
	if err != nil {
		return nil, err
	}

	serverConfig := comm.ServerConfig(cu.Certificate(), cu.CaCertificate(), cu.Priv())

	// Initiate HTTP connection without TLS. Used by demonstrator
	_httpListener, err := netutil.ListenOnPort(httpPortNum)
	if err != nil {
		panic(err)
	}

	cache, err := cache.NewCache(ifritClient)
	if err != nil {
		return nil, err
	}

	return &Mux{
		ifritClient: 		ifritClient,
		exitChan:       	make(chan bool, 1),

		// In-memory caches used to describe the network data
		cache: 				cache,

		// HTTPS	
		portNum: 			port,
		listener: 			listener,
		serverConfig: 		serverConfig,

		// HTTP	
		httpPortNum: 		httpPortNum,
		_httpListener:  	_httpListener,

		// Sync	
		nodeListLock: 		sync.RWMutex{},
		wg:             	&sync.WaitGroup{},
	}, nil
}

func (m *Mux) Start() {
	go m.ifritClient.Start()
	go m.HttpHandler()
	go m.HttpsHandler()
}

func (m *Mux) Stop() {
	m.ifritClient.Stop()
}