package main

import (
	"log"
	"fmt"
	"flag"
	"os"
	"os/signal"
	"io/ioutil"
	"net"
	"crypto/x509/pkix"
	"syscall"
	"strings"
	"strconv"
	"net/http"
	"time"
	
	"github.com/tomcat-bit/lohpi/pkg/netutil"
	"github.com/gorilla/mux"
	"github.com/tomcat-bit/lohpi/pkg/comm"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/jinzhu/configor"
	du "github.com/tomcat-bit/lohpi/pkg/datauser"
)

type ApplicationConfig struct {
	MuxHttpsIP				string		`default:"127.0.1.1:8081"`
	LocalAddr 				string 		`default:"127.0.1.1:8086"`
}

type Application struct {
	config ApplicationConfig

	httpPort int 
	httpListener net.Listener
	httpServer   *http.Server

	// gRPC client towards the Mux
	muxClient *comm.MuxGRPCClient

	datausers []*du.DataUser

	// Run the application n times and exit
	n int
}

var appConfig = struct {
	config ApplicationConfig
}{}

func main() {
	var datauserDirectory string
	var configFile string
	var n int
	
	arg := flag.NewFlagSet("args", flag.ExitOnError)
	arg.StringVar(&datauserDirectory, "-d", "", "Directory of data users. Each file describes a data user.")
	arg.StringVar(&configFile, "c", "config.yml", `Configuration file for policy store. If not set, use default configuration values.`)
	arg.IntVar(&n, "n", 10, "Iterates through the set of data users n times. For each data user, use the Lohpi rpc definitions. Default value is 10.")
	arg.Parse(os.Args[1:])

	if datauserDirectory == "" {
		fmt.Fprintf(os.Stderr, "Directory path must be given")
		os.Exit(2)
	}

	if !exists(datauserDirectory) {
		fmt.Fprintf(os.Stderr, "'%s' does not exist. Exiting")
		os.Exit(2)
	}

	if !exists(configFile) {
		fmt.Fprintf(os.Stderr, "'%s' does not exist. Exiting")
		os.Exit(2)
	}

	if n < 0 {
		fmt.Fprintf(os.Stdout, "n cannot be negative -- set to 10,")
		n = 10
	}

	if err := setConfigurations(configFile); err != nil {
		panic(err)
	}

	app, err := newApplication(datauserDirectory, appConfig.config)
	if err != nil {
		log.Println(err.Error())
	}

	app.run()

	// Wait for SIGTERM signal from the environment
	channel := make(chan os.Signal, 2)
	signal.Notify(channel, os.Interrupt, syscall.SIGTERM)
	<-channel

	app.stop()
}

func newApplication(dir string, config ApplicationConfig) (*Application, error) {
	// Set a proper port on which the gRPC server listens
	portString := strings.Split(config.LocalAddr, ":")[1]
	port, err := strconv.Atoi(portString)
	if err != nil {
		return nil, err
	}
	netutil.ValidatePortNumber(&port)
	listener, err := netutil.ListenOnPort(port)
	if err != nil {
		return nil, err
	}

	// Setup X509 certificate
	pk := pkix.Name{
		Locality: []string{listener.Addr().String()},
	}

	cu, err := comm.NewCu(pk, config.LocalAddr)
	if err != nil {
		return nil, err
	}

	muxClient, err := comm.NewMuxGRPCClient(cu.Certificate(), cu.CaCertificate(), cu.Priv())
	if err != nil {
		return nil, err
	}

	return &Application{
		config:		config,
		muxClient: 	muxClient,
		httpPort:	port,
	}, nil
}

func (a *Application) run() {
	for i := 0; i < a.n; i++ {
		// Fetch the studies 
		
	}
}

func (a *Application) stop() {
	
}

func (a *Application) Start() {
	go a.httpHandler()
}

func (a *Application) httpHandler() error {
	r := mux.NewRouter()
	log.Printf("Data user: started HTTP server on port %d\n", a.httpPort)

	a.httpServer = &http.Server{
		Handler: r,
		ReadTimeout:    10 * time.Minute,
		WriteTimeout:   10 * time.Minute,
	}

	err := a.httpServer.Serve(a.httpListener)
	if err != nil {
		log.Fatalf(err.Error())
		return err
	}

	return nil
}

func (a *Application) studies() ([]string, error) {
	conn, err := n.muxClient.Dial(a.config.MuxIP)
	if err != nil {
		return err
	}
	defer conn.CloseConn()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second * 20)
	defer cancel()

	return conn.GetStudies(ctx, empty.Empty{})
}

func setConfigurations(configFile string) error {
	conf := configor.New(&configor.Config{
		ErrorOnUnmatchedKeys: true,
		Verbose: true,
		Debug: true,
	})

	return conf.Load(&appConfig, configFile)
}

func loadUsers(dir string) ([]*du.DataUser, error) {
	users := make([]*du.DataUser, 0)

	files, err := ioutil.ReadDir(".")
    if err != nil {
        log.Fatal(err)
    }
	
	for _, file := range files {
		content, err := ioutil.ReadFile(file.Name())
		if err != nil {
			log.Fatal(err)
		}
   
		u := du.NewDataUser(file.Name())
		u.SetPolicyAttributes(string(content))
		users = append(users, u)
	}
	
	return users, nil
}

func exists(name string) bool {
	if _, err := os.Stat(name); err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return true
}