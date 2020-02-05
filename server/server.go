package core
/*
import (
	"fmt"
	"net"
	"net/http"
	log "github.com/inconshreveable/log15"
	"github.com/spf13/viper"
	"errors"
	"firestore/netutil"
)

var (
	errNoAddr         = errors.New("No network address provided in cert request.")
	errNoPort         = errors.New("No port number specified in config.")
)

type Masternode struct {
	// These two fellas are set from somewhere outside to set the initial 
	// state of the system
	NetworkStorageNodes []*Node             // data storage units encapsulated behind this master node
	Subjects []*Subject                     // Subjects known to the network. Dynamic membership
	
	listener   net.Listener
	httpServer *http.Server // it's nil 
}

func NewMasterNode() (*Masternode, error) {
	err := readConfig()
	if err != nil {
		return nil, err
	}
		
	if exists := viper.IsSet("port"); !exists {
		return nil, errNoPort
	}
		
	l, err := netutil.ListenOnPort(viper.GetInt("port"))
	if err != nil {
		return nil, err
	}
		
	self := &Masternode {
		listener: l,
	}
		
	return self, nil
}
		
func (m *Masternode) Start() error {
	log.Info("Started MasterNode", "addr", m.listener.Addr().String())
	return m.httpHandler()
}
		
func (m *Masternode) Addr() string {
	return m.listener.Addr().String()

		
// Shutsdown the certificate authority instance, will no longer serve signing requests.
func (m *Masternode) Shutdown() {
	log.Info("Shuting down master node")
	m.listener.Close()
}
		
func (m *Masternode) httpHandler() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/kake", m.kake)
		
	m.httpServer = &http.Server{
		Handler: mux,
	}
		
	err := m.httpServer.Serve(m.listener)
	if err != nil {
		log.Error(err.Error())
		return err
	}
		
	return nil
}

func (m *Masternode) kake(w http.ResponseWriter, r *http.Request) {
	fmt.Printf("Kake here\n")
}
		
func readConfig() error {
	viper.SetConfigName("mn_config")
	viper.AddConfigPath("/var/tmp")
	viper.AddConfigPath(".")
		
	viper.SetConfigType("yaml")
	
	err := viper.ReadInConfig()
	if err != nil {
		log.Error(err.Error())
		return err
	}
		
	viper.WriteConfig()
	return nil
}*/