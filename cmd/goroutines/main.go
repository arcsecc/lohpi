package main

import (
	"fmt"
	"flag"
//	"log"
	"os"
	"os/signal"
	"net"
	"net/http"
	"syscall"
	"strconv"
	//"bytes"
	//"io"
	//"encoding/json"
	
	"firestore/core/mux"
//	"firestore/core/message"
	"firestore/netutil"

	logger "github.com/inconshreveable/log15"
)

var (
	logging   = logger.New("module", "goroutines/main")
)

// All resources in the system is defined in the context of this struct.
// Nodes, mux, CA and so on happen through this entity.
type Application struct {
	// The front-end mux
	mux *mux.Mux
}

func main() {
	var numNodes int
	var portNum int
	//var setFiles bool

	arg := flag.NewFlagSet("args", flag.ExitOnError)
	arg.IntVar(&numNodes, "n", 0, "Number of initial nodes in the network.")
	arg.IntVar(&portNum, "p", -1, "Port number at which Lohpi runs. If not set or the selected port is busy, select an open port.")
	//arg.BoolVar(&setFiles, "-w", false, "If set to true, a warm start is initiated. Using the parameters in the config file, the nodes are populated with files")
	arg.Parse(os.Args[1:])

	if numNodes == 0 {
		fmt.Fprintf(os.Stderr, "Requires -n <number of initial nodes>. Exiting\n")
		os.Exit(2)
	}

	// Safely set the port number of the application
	validatePortNumber(&portNum)

	// Create the application and start it so that we can interact with it :)
	app := NewApplication(numNodes, portNum)
	if err := app.Run(); err != nil {
		panic(err)
	}

	channel := make(chan os.Signal, 2)
	signal.Notify(channel, os.Interrupt, syscall.SIGTERM)
	<-channel
}


func NewApplication(numNodes, portNum int) *Application {
	m, err := mux.NewMux(portNum)
	if err != nil {
		panic(err)
	}

	return &Application {
		mux: m,
	}
}

func (app *Application) Run() error {
	return app.mux.HttpHandler()
}

// HTTP endpoints
func (app *Application) GetStudyMetadata(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if r.Method != http.MethodGet {
		http.Error(w, "Expected GET method", http.StatusMethodNotAllowed)
		return
	}

	if r.Header.Get("Content-type") != "application/json" {
		http.Error(w, "Require header to be application/json", http.StatusUnprocessableEntity)
		return
	}

	/*var msg struct {
		StudyID int `json:"node"`
	}*/

	w.WriteHeader(http.StatusOK)
	//fmt.Fprintf(w, "%s", nodeInfo)
}

/*
func (app *Application) CreateStudy(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if r.Method != http.MethodPost {
		http.Error(w, "Expected POST method", http.StatusMethodNotAllowed)
		return
	}

	if r.Header.Get("Content-type") != "application/json" {
		http.Error(w, "Requires header to be application/json", http.StatusUnprocessableEntity)
		return
	}

	// Probably to be changed. These are rather simple metadata 
	var msg struct {
		Node string `json:"node"`
		FileSize int `json:"size"`/*
		Node string `json:"node"`
		Timestamp string `json:"time"` // Using YYYY:MM:DD 00:00:00
		*
	}

	var body bytes.Buffer
	io.Copy(&body, r.Body)
	err := json.Unmarshal(body.Bytes(), &msg)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Message: %v\n", msg)

	// Validate msg here...
	// validateMsg(msg)
	if _, ok := app.nodes[msg.Node]; ok {
		fmt.Printf("About to send msg to %s\n", msg.Node)
		app.SendNodeMessage(msg.Node, body.Bytes(), message.MSG_TYPE_NEW_STUDY)
	} else {
		errorMsg := fmt.Sprintf("Node '%s' is not found in the network.", msg.Node)
		http.Error(w, errorMsg, http.StatusNotFound)
		return		
	}
}
*/
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

/*
func (app *Application) SendNodeMessage(node string, data []byte, msg message.MsgType) {
	switch msg {
		case message.MSG_TYPE_NEW_STUDY:
			log.Printf("Message: PERMISSION_GET")
		default:
			log.Printf("Unkown message")
	}
	client := app.nodes[node].FireflyClient()
	destinationAddress := client.Addr()
	<-client.SendTo(destinationAddress, data)
}*/