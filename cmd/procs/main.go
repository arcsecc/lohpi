package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"syscall"

	logger "github.com/inconshreveable/log15"
	"github.com/tomcat-bit/lohpi/internal/core/client"
	"github.com/tomcat-bit/lohpi/internal/core/mux"
	"github.com/tomcat-bit/lohpi/internal/core/rec"
	"github.com/tomcat-bit/lohpi/internal/core/policy"
	"github.com/tomcat-bit/lohpi/internal/netutil"
	"github.com/jinzhu/configor"
)

// Global configuration for each system component. Read into the program from a .yml file
var ApplicationConfig = struct {
	MuxConfig mux.Config
	RecConfig rec.Config
	PolicyStoreConfig policy.Config
}{}

type Application struct {
	// The front-end mux exposed to the data users
	mux *mux.Mux

	// The policy store
	ps *policy.PolicyStore

	// REC
	rec *rec.Rec

	// The collection of child processes
	nodeProcs map[string]*exec.Cmd
	execPath string
	nodeConfigPath string

	// The collection of data users to query the Lohpi system
	clients map[string]*client.Client

	// Number of initial nodes in the network
	numNodes int
}

func main() {
	var numNodes int
	var httpPortNum int
	var numClients int
	var execPath string = ""
	var logfile string
	var nodeConfigPath string = ""
	var h logger.Handler
	var applicationConfigFile string

	arg := flag.NewFlagSet("args", flag.ExitOnError)
	arg.IntVar(&numNodes, "nodes", 0, "Number of initial nodes in the network.")
	arg.IntVar(&numClients, "clients", 0, "Number of initial clients to interact with the network.")
	arg.StringVar(&execPath, "exec", "", "Lohpi node's executable path.")
	arg.StringVar(&nodeConfigPath, "nodeconfig", "", "Path to node configuration file.")
	arg.IntVar(&httpPortNum, "port", -1, "Port number to interact with Lohpi. If not set or the selected port is busy, select an open port.")
	arg.StringVar(&logfile, "logfile", "", "Absolute or relative path to log file.")
	arg.StringVar(&applicationConfigFile, "config", "", `Configuration file for the application. If not set, use default configuration values.`)
	arg.Parse(os.Args[1:])

	if numNodes == 0 {
		fmt.Fprintf(os.Stderr, "Requires -n <number of initial nodes>. Exiting\n")
		os.Exit(2)
	}

	// TODO: ignore numClients -> we want a dynamic membership
	if numClients == 0 {
		fmt.Fprintf(os.Stderr, "Requires -c <number of initial client>. Exiting\n")
		os.Exit(2)
	}

	if execPath == "" {
		fmt.Fprintf(os.Stderr, "Requires -e <path to executable Fireflies node>. Exiting\n")
		os.Exit(2)
	}

	if httpPortNum == -1 {
		fmt.Fprintf(os.Stderr, "Requires -p <port number>. Exiting\n")
		os.Exit(2)
	}

	if !fileExists(execPath) {
		fmt.Fprintf(os.Stderr, "Executable path '%s' is invalid. Exiting\n", execPath)
		os.Exit(2)
	}

	if applicationConfigFile == "" {
		log.Println("Using default application configuration")
	}

	if !fileExists(nodeConfigPath) {
		fmt.Fprintf(os.Stderr, "Could not use %s as configuration file for node. Exiting\n", nodeConfigPath)
		os.Exit(2)
	}

	r := logger.Root()
	if logfile != "" {
		h = logger.CallerFileHandler(logger.Must.FileHandler(logfile, logger.LogfmtFormat()))
	} else {
		h = logger.StreamHandler(os.Stdout, logger.LogfmtFormat())
	}

	r.SetHandler(h)

	netutil.ValidatePortNumber(&httpPortNum)

	if err := setConfigurations(applicationConfigFile); err != nil {
		panic(err)
	}

	app := NewApplication(numNodes, numClients, httpPortNum, execPath, nodeConfigPath)
	app.Start()

	channel := make(chan os.Signal, 2)
	signal.Notify(channel, os.Interrupt, syscall.SIGTERM)
	<-channel

	app.Stop()
}

func NewApplication(numNodes, numClients, httpPortNum int, execPath, nodeConfigPath string) *Application {
	m, err := mux.NewMux(&ApplicationConfig.MuxConfig, httpPortNum)
	if err != nil {
		panic(err)
	}

	ps, err := policy.NewPolicyStore(&ApplicationConfig.PolicyStoreConfig)
	if err != nil {
		panic(err)
	}

	rec, err := rec.NewRec(&ApplicationConfig.RecConfig)
	if err != nil {
		panic(err)
	}

	// Clients somewhere here

	return &Application{
		mux:       		m,
		numNodes:  		numNodes,
		execPath:  		execPath,
		nodeConfigPath: nodeConfigPath,
		ps:        		ps,
		rec:			rec,
		//clients: clients,
	}
}

func setConfigurations(configFile string) error {
	conf := configor.New(&configor.Config{
		ErrorOnUnmatchedKeys: true,
		Verbose: true,
		Debug: true,
	})

	return conf.Load(&ApplicationConfig, configFile)
}

func (app *Application) Start() {
	// Assert the use of go routines here and inside the Start() methods
	go app.mux.Start()
	go app.ps.Start()
	go app.rec.Start()

	// Start the network nodes
	//time.Sleep(2 * time.Second) 
	app.nodeProcs = startStorageNodes(app.numNodes, app.execPath, app.nodeConfigPath)

	// Start the clients so that they can interact with Lohpi
	/*for _, c := range app.clients {
		go c.Run()
	}*/
}

func (app *Application) Stop() {
	log.Printf("Cleaning up Lohpi resources...\n")
	app.mux.Stop()

	for _, cmd := range app.nodeProcs {
		if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM); err != nil {
			panic(err)
		}
	}

	log.Println("Exiting Lohpi")
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

func startStorageNodes(numNodes int, execPath, nodeConfigPath string) map[string]*exec.Cmd {
	nodeProcs := make(map[string]*exec.Cmd)

	// Start the child processes aka. the internal Fireflies nodes
	for i := 0; i < numNodes; i++ {
		nodeName := fmt.Sprintf("node_%d", i)
		logfileName := fmt.Sprintf("%s_logfile", nodeName)
		nodeProc := exec.Command(execPath, "-name", nodeName, "-logfile", logfileName, "-config", nodeConfigPath)

		nodeProcs[nodeName] = nodeProc
		nodeProc.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

		// Launch several nodes
		
		p := nodeProc
		go func() {
			p.Stdout = os.Stdout
			p.Stderr = os.Stderr
		
			if err := p.Start(); err != nil {
				log.Println(err.Error())
			}

			if err := p.Wait(); err != nil {
				log.Println(err.Error())
			}
		}()
	}
	return nodeProcs
}

/*
func addNetworkClients(numClients int) (map[string]*client.Client, error) {
	clients := make(map[string]*client.Client)

	for i := 0; i < numClients; i++ {
		clientName := fmt.Sprintf("Client_%d", i+1)
		c, err := client.NewClient(clientName)
		if err != nil {
			panic(err)
			return nil, err
		}
		clients[clientName] = c
	}
	return clients, nil
}*/
