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
	"github.com/tomcat-bit/lohpi/internal/core/policy"
	"github.com/tomcat-bit/lohpi/internal/netutil"

	"github.com/spf13/viper"
)

type Application struct {
	// The front-end mux exposed to the data users
	mux *mux.Mux

	// The policy store
	ps *policy.PolicyStore

	// The collection of child processes
	nodeProcs map[string]*exec.Cmd

	// The collection of data users to query the Lohpi system
	clients map[string]*client.Client

	// The path to the executable node
	execPath string

	// Number of initial nodes in the network
	numNodes int
}

func main() {
	var numNodes int
	var httpPortNum int
	var numClients int
	var execPath string = ""
	var logfile string
	var h logger.Handler

	arg := flag.NewFlagSet("args", flag.ExitOnError)
	arg.IntVar(&numNodes, "n", 0, "Number of initial nodes in the network.")
	arg.IntVar(&numClients, "c", 0, "Number of initial clients to interact with the network.")
	arg.StringVar(&execPath, "e", "", "Lohpi node's executable path.")
	arg.IntVar(&httpPortNum, "p", -1, "Port number to interact with Lohpi. If not set or the selected port is busy, select an open port.")
	arg.StringVar(&logfile, "logfile", "", "Absolute or relative path to log file.")
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

	r := logger.Root()
	if logfile != "" {
		h = logger.CallerFileHandler(logger.Must.FileHandler(logfile, logger.LogfmtFormat()))
	} else {
		h = logger.StreamHandler(os.Stdout, logger.LogfmtFormat())
	}

	r.SetHandler(h)

	netutil.ValidatePortNumber(&httpPortNum)
	app := NewApplication(numNodes, numClients, httpPortNum, execPath)
	app.Start()

	channel := make(chan os.Signal, 2)
	signal.Notify(channel, os.Interrupt, syscall.SIGTERM)
	<-channel

	app.Stop()
}

func NewApplication(numNodes, numClients, httpPortNum int, execPath string) *Application {
	if err := readConfig(); err != nil {
		panic(err)
	}

	m, err := mux.NewMux(httpPortNum)
	if err != nil {
		panic(err)
	}

	ps, err := policy.NewPolicyStore()
	if err != nil {
		panic(err)
	}

	// Clients somewhere here
	nodes := startStorageNodes(numNodes, execPath)

	return &Application{
		mux:       m,
		numNodes:  numNodes,
		execPath:  execPath,
		nodeProcs: nodes,
		ps:        ps,
		//clients: clients,
	}
}

func (app *Application) Start() {
	app.mux.Start()
	app.ps.Start()

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

func startStorageNodes(numNodes int, execPath string) map[string]*exec.Cmd {
	nodeProcs := make(map[string]*exec.Cmd)

	// Start the child processes aka. the internal Fireflies nodes
	for i := 0; i < numNodes; i++ {
		nodeName := fmt.Sprintf("node_%d", i)
		logfileName := fmt.Sprintf("%s_logfile", nodeName)
		nodeProc := exec.Command(execPath, "-name", nodeName, "-logfile", logfileName)

		nodeProcs[nodeName] = nodeProc
		nodeProc.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

		// Launch several nodes
		/*
			go func() {
				if err := nodeProc.Start(); err != nil {
					panic(err)
				}

				if err := nodeProc.Wait(); err != nil {
					panic(err)
				}
			}()*/
	}
	return nodeProcs
}

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
}

func readConfig() error {
	viper.SetConfigName("lohpi_config")
	viper.AddConfigPath("/var/tmp")
	viper.AddConfigPath(".")
	viper.SetConfigType("yaml")

	err := viper.ReadInConfig()
	if err != nil {
		return err
	}

	// Behavior variables
	viper.SetDefault("policy_store_repo", "/home/thomas/go/src/github.com/tomcat-bit/lohpi/policy_store")
	viper.SetDefault("lohpi_mux_addr", "127.0.1.1:8080")
	viper.SetDefault("policy_store_addr", "127.0.1.1:8082")
	viper.SetDefault("lohpi_ca_addr", "127.0.1.1:8301")
	viper.SafeWriteConfig()
	return nil
}
