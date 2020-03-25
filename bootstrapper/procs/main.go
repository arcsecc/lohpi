package main

import (
	"fmt"
	"flag"
	"os"
	"os/signal"
	"os/exec"
	"syscall"
	"strconv"
	"net"
	"log"

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
	var execPath string = ""
	
	arg := flag.NewFlagSet("args", flag.ExitOnError)
	arg.IntVar(&numNodes, "n", 0, "Number of initial nodes in the network.")
	arg.StringVar(&execPath, "e", "", "Lohpi node's executable path.")
	arg.IntVar(&portNum, "p", 0, "Port number at which Lohpi runs. If not set or the selected port is busy, select an open port.")
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

	// Life-cycle of the system here
	app := NewApplication(numNodes, portNum, execPath)
	app.StartNodes()
	app.Run()

	// Wait for SIGTERM. Clean up everything when that happpens
	channel := make(chan os.Signal, 2)
	signal.Notify(channel, os.Interrupt, syscall.SIGTERM)
	<-channel

	// Stop the entire system
	app.Stop()
}

func NewApplication(numNodes, portNum int, execPath string) *Application {
	m, err := mux.NewMux(portNum)
	if err != nil {
		panic(err)
	}

	return &Application {
		mux: 		m,
		numNodes: 	numNodes,
		execPath: 	execPath,
	}
}

func (app *Application) Stop() {
	StopProcesses(app.childProcs)
}

func (app *Application) Run() error {
	return app.mux.HttpHandler()
}

func (app *Application) StartNodes() {
	app.childProcs = make(map[string]*exec.Cmd, 0)
	for i := 0; i < app.numNodes; i++ {
		nodeName := fmt.Sprintf("node_%d", i)
		logfileName := fmt.Sprintf("%s_logfile", nodeName)
		nodeProc := exec.Command(app.execPath, "-name", nodeName, "-logfile", logfileName)
		app.childProcs[nodeName] = nodeProc

		if err := nodeProc.Start(); err != nil {
			panic(err)
		}

		nodeProc.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		//fmt.Printf("%v\n", nodeProc.SysProcAttr)
		go func() {
			err := nodeProc.Wait()
			if err != nil {
				panic(err)
			}
		}()
		log.Printf("Added %s to network\n", nodeName)
	}
}

func StopProcesses(childProcs map[string]*exec.Cmd) {
	for _, cmd := range childProcs {
		if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); err != nil {
			// might allow ourselves to fail silently here instead of panicing...
			panic(err)
		}
	}
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
	// Set port number. Check if it is in use -- abort if so
	if *portNum == -1 {
		*portNum = netutil.GetOpenPort()
		fmt.Printf("Port number not set. Picking %d as port num...\n", *portNum)
	} else {
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