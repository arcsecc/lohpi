package main

/** Launcher.go launches one executable of the 'node' package and waits for a SIGTERM signal to arrive
 * from the environment. This should be used when we want to use a process-granularity run. 
 */
import (
	"flag"
	"os"
	_"strconv"
	"fmt"
	"bytes"
	"os/signal"
	_"firestore/netutil"
	"runtime"
	"syscall"
	"encoding/json"
_	"encoding/binary"
_	"net"
	"net/http"
	"errors"
	_"io/ioutil"
	"strconv"
	"firestore/core/node"

	logger "github.com/inconshreveable/log15"
)

func main() {
	var logfile string
	var nodeName string
	var h logger.Handler
	var portNum int
	var muxPort uint

	runtime.GOMAXPROCS(runtime.NumCPU())

	// Logfile and name flags
	arg := flag.NewFlagSet("args", flag.ExitOnError)
	arg.StringVar(&logfile, "logfile", "", "Log to file.")
	arg.StringVar(&nodeName, "name", "", "Human-readable identifier of node.")
	arg.IntVar(&portNum, "port", -1, "Port to use. If not set, use a random port")
	arg.UintVar(&muxPort, "mp", 8080, "Port at which the mux runs.")

	arg.Parse(os.Args[1:])

	r := logger.Root()

	/** Check the flags. Exit appropriately */
	if logfile != "" {
		h = logger.CallerFileHandler(logger.Must.FileHandler(logfile, logger.LogfmtFormat()))
	} else {
		h = logger.StreamHandler(os.Stdout, logger.LogfmtFormat())
	}

	// Require node identifier
	if nodeName == "" {
		fmt.Fprintf(os.Stderr, "Missing node identifier\n")
        os.Exit(2)
	}
	
	r.SetHandler(h)

	// Create the new node and let it live its own life
	node := node.NewNode(nodeName)
	node.StartIfritClient()
	/*if err := node.MountFuse(); err != nil {
		panic(err)
	}*/
	
	if err := sendPortNumber(node.NodeName(), node.Addr(), muxPort); err != nil {
		panic(err)
	}

	// Wait for SIGTERM signal from the environment
	channel := make(chan os.Signal, 2)
	signal.Notify(channel, os.Interrupt, syscall.SIGTERM)
	<-channel

	// Clean-up
	node.Shutdown()
}

func sendPortNumber(nodeName, addr string, muxPort uint) error {
	URL := "http://127.0.1.1:" + strconv.Itoa(int(muxPort)) + "/set_port"
	fmt.Printf("URL:> %s\n", URL)

	var msg struct {
		Node 	string 		`json:"node"`
		Address string 		`json:"address"`
	}

	msg.Node = nodeName
	msg.Address = addr
	jsonStr, err := json.Marshal(msg)
	if err != nil {
        return err
    }

	req, err := http.NewRequest("POST", URL, bytes.NewBuffer(jsonStr))
	req.Header.Set("Content-Type", "application/json")
	
	client := &http.Client{}
	response, err := client.Do(req)
    if err != nil {
        return err
	}
	defer response.Body.Close()
	if response.StatusCode != int(http.StatusOK) {
		return errors.New("Bad request")
	}
	return nil
}


func Exists(name string) bool {
    if _, err := os.Stat(name); err != nil {
        if os.IsNotExist(err) {
            return false
        }
    }
    return true
}