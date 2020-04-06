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
	"firestore/netutil"
)

type Application struct {
	// The front-end mux exposed to the data users
	mux *mux.Mux

	// The collection of child processes
	childProcs map[string]*exec.Cmd

	// The path to the executable node
	execPath string

	// Number of initial nodes in the network
	numNodes int
}

func main() {
	var numNodes int
	var portNum int
	var httpPortNum int
	var execPath string = ""

	arg := flag.NewFlagSet("args", flag.ExitOnError)
	arg.IntVar(&numNodes, "n", 0, "Number of initial nodes in the network.")
	arg.StringVar(&execPath, "e", "", "Lohpi node's executable path.")
	arg.IntVar(&portNum, "p", -1, "Port number at which Lohpi runs. If not set or the selected port is busy, select an open port.")
	arg.IntVar(&httpPortNum, "http_port", -1, "Port number to interact with Lohpi. If not set or the selected port is busy, select an open port.")
	arg.Parse(os.Args[1:])

	if numNodes == 0 {
		fmt.Fprintf(os.Stderr, "Requires -n <number of initial nodes>. Exiting\n")
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
	app := NewApplication(numNodes, portNum, httpPortNum, execPath)
	app.Start()
	app.Run()

	// Wait for SIGTERM. Clean up everything when that happpens
	channel := make(chan os.Signal, 2)
	signal.Notify(channel, os.Interrupt, syscall.SIGTERM)
	<-channel

	// Stop the entire system
	app.Stop()
}

func NewApplication(numNodes, portNum, httpPortNum int, execPath string) *Application {
	m, err := mux.NewMux(portNum, httpPortNum, execPath)
	if err != nil {
		panic(err)
	}

	// Call this to create a process tree from the node module. Add them to the
	// collection of known nodes in the network. We are allowed to spawn and kill other nodes
	// at later points in time
	m.AddNetworkNodes(numNodes)

	return &Application{
		mux:      m,
		numNodes: numNodes,
	}
}

func (app *Application) Start() {
	app.mux.Start()
}

func (app *Application) Stop() {
	log.Printf("Cleaning up Lohpi resources...\n")
	app.mux.Stop()
	log.Printf("Done cleaning up\n")
}

func (app *Application) Run() {
	app.mux.RunServers()
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
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
