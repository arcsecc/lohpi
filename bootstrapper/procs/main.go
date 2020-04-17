package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"syscall"

	"firestore/core/mux"
	"firestore/core/client"
	"firestore/netutil"
)

type Application struct {
	// The front-end mux exposed to the data users
	mux *mux.Mux

	// The collection of child processes
	childProcs map[string]*exec.Cmd

	// The collection of data users to query the Lohpi system
	clients map[string]*client.Client

	// The path to the executable node
	execPath string

	// Number of initial nodes in the network
	numNodes int
}

func main() {
	var numNodes int
	var portNum int
	var httpPortNum int
	var numClients int 
	var execPath string = ""

	arg := flag.NewFlagSet("args", flag.ExitOnError)
	arg.IntVar(&numNodes, "n", 0, "Number of initial nodes in the network.")
	arg.IntVar(&numClients, "c", 0, "Number of initial clients to interact with the network.")
	arg.StringVar(&execPath, "e", "", "Lohpi node's executable path.")
	arg.IntVar(&portNum, "p", -1, "Port number at which Lohpi runs. If not set or the selected port is busy, select an open port.")
	arg.IntVar(&httpPortNum, "http_port", -1, "Port number to interact with Lohpi. If not set or the selected port is busy, select an open port.")
	arg.Parse(os.Args[1:])

	if numNodes == 0 {
		fmt.Printf("IS 0\n")
		fmt.Fprintf(os.Stderr, "Requires -n <number of initial nodes>. Exiting\n")
		os.Exit(2)
	}

	if numClients == 0 {
		fmt.Fprintf(os.Stderr, "Requires -c <number of initial client>. Exiting\n")
		os.Exit(2)
	}

	if execPath == "" {
		fmt.Fprintf(os.Stderr, "Requires -e <path to executable Fireflies node>. Exiting\n")
		os.Exit(2)
	}

	if !fileExists(execPath) {
		fmt.Fprintf(os.Stderr, "Executable path '%s' is invalid. Exiting\n", execPath)
		os.Exit(2)
	} else {
		if !fileIsExecutable(execPath) {
			fmt.Fprintf(os.Stderr, "'s' is not executable. Exiting\n", execPath)
		}
	}

	// Safely set the port number of the application
	validatePortNumber(&portNum)
	validatePortNumber(&httpPortNum)
	if portNum == httpPortNum {
		fmt.Fprintf(os.Stderr, "HTTP server port and HTTPS server port cannot be the same! Exiting\n")
		os.Exit(2)
	}
	// Life-cycle of the system here
	app := NewApplication(numNodes, numClients, portNum, httpPortNum, execPath)
	app.Start()
	app.Run()

	// Wait for SIGTERM. Clean up everything when that happpens
	channel := make(chan os.Signal, 2)
	signal.Notify(channel, os.Interrupt, syscall.SIGTERM)
	<-channel

	// Stop the entire system bye bye thx
	app.Stop()
}

func NewApplication(numNodes, numClients, portNum, httpPortNum int, execPath string) *Application {
	m, err := mux.NewMux(portNum, httpPortNum)
	if err != nil {
		panic(err)
	}

	// Call this to create a process tree from the node module. Add them to the
	// collection of known nodes in the network. We are allowed to spawn and kill other nodes
	// at later points in time
	// TODO: Remove 'AddNetworkNodes' from mux module
	//m.AddNetworkNodes(numNodes)
	
	// Wait for all nodes to become available. Notify 
	// the mux's IP address to the rest of the network
	// TODO: clean up in abstractions here! 
//	m.AssignNodeIdentifiers(numNodes)
	//m.BroadcastIP()

	// Start the clients who will query Lohpi regularly
	/*clients, err := addNetworkClients(numClients)
	if err != nil {
		panic(err)
	}*/

	return &Application{
		mux:     m,
		numNodes: numNodes,
		execPath: execPath,
		//clients: clients,
	}
}

func (app *Application) Start() {
	// Always start the mux first
	app.mux.Start()

	// Add a initial set of working Lohpi nodes
	addNetworkNodes(app.numNodes, app.execPath)

	// Since we assume a static membership for now, we need to wait for all nodes to 
	// be available.
	app.mux.AssignNodeIdentifiers(app.numNodes)

	// TODO: Add feature for dynamic membership later
}

func (app *Application) Stop() {
	log.Printf("Cleaning up Lohpi resources...\n")
	app.mux.Stop()
	log.Printf("Done cleaning up\n")
}

func (app *Application) Run() {
	app.mux.RunServers()
	
	// Start the clients so that they can interact with Lohpi
	/*for _, c := range app.clients {
		go c.Run()
	}*/
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

func addNetworkNodes(numNodes int, execPath string) {
	nodeProcs := make(map[string]*exec.Cmd)

	fmt.Printf("numNodes: %d\n", numNodes)

	// Start the child processes aka. the internal Fireflies nodes
	for i := 0; i < numNodes; i++ {
		nodeName := fmt.Sprintf("node_%d", i)
		fmt.Printf("Adding %s\n", nodeName)
		logfileName := fmt.Sprintf("%s_logfile", nodeName)
		nodeProc := exec.Command(execPath, "-name", nodeName, "-logfile", logfileName)
		
		// Process tree map
		nodeProcs[nodeName] = nodeProc
		nodeProc.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
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
}

func addNetworkClients(numClients int) (map[string]*client.Client, error) {
	clients := make(map[string]*client.Client)

	for i := 0; i < numClients; i++ {
		clientName := fmt.Sprintf("Client_%d", i + 1)
		c, err := client.NewClient(clientName)
		if err != nil {
			panic(err)
			return nil, err
		}
		fmt.Printf("Added client with identifier '%s'\n", clientName)
		clients[clientName] = c
	}
	return clients, nil
}

// TODO: complete this function
func fileIsExecutable(path string) bool {
	return true
}

// Validates the port number of the application by checking if it is free. If it is
// not, select an open port number
func validatePortNumber(portNum *int) {
	// Set portNum to a valid port if it is not set
	if *portNum == -1 {
		*portNum = netutil.GetOpenPort()
		log.Printf("Port number not set. Picking %d as port num...\n", *portNum)
	} else {
		// Check if port number is set. If the port number is not open, find another port number
		host := ":0" + strconv.Itoa(*portNum)
		l, err := net.Listen("tcp4", host)
		if err != nil {
			*portNum = netutil.GetOpenPort()
			return
		}
		if err := l.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "Can't close connection on port %d: %s", *portNum, err)
			panic(err)
		}
	}
}
