package main

/** Launcher.go launches one executable of the 'node' package and waits for a SIGTERM signal to arrive
 * from the environment. This should be used when we want to use a process-granularity run.
 */
import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"errors"

	logger "github.com/inconshreveable/log15"
	"github.com/tomcat-bit/lohpi/internal/core/node"
	"github.com/jinzhu/configor"
)

var NodeConfig = struct {
	NodeConfig node.Config
}{}


func main() {
	var logfile string
	var nodeName string
	var nodeConfigFile string
	var h logger.Handler
	//var muxPort uint
	//var policyStorePort uint

	runtime.GOMAXPROCS(runtime.NumCPU())

	// Logfile and name flags
	arg := flag.NewFlagSet("args", flag.ExitOnError)
	arg.StringVar(&logfile, "logfile", "", "Absolute or relative path to log file.")
	arg.StringVar(&nodeName, "name", "", "Human-readable identifier of node.")
	arg.StringVar(&nodeConfigFile, "config", "", `Configuration file for the node. If not set, use default configuration values.`)
	//arg.UintVar(&muxPort, "mp", 8080, "HTTPS port at which the mux runs.")
	//arg.UintVar(&muxPort, "psp", 8082, "HTTPS port at which the policy store runs.")

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

	if nodeConfigFile == "" {
		panic(errors.New("No config file found"))
	}

	if err := setConfigurations(nodeConfigFile); err != nil {
		panic(err)
	}

	// Create the new node and let it live its own life
	node, err := node.NewNode(nodeName, &NodeConfig.NodeConfig)
	if err != nil {
		panic(err)
	}

	node.StartIfritClient()
	if err := node.MountFuse(); err != nil {
		panic(err)
	}

	// Connect to the mux and policy store
	if err := node.MuxHandshake(); err != nil {
		panic(err)
	}

	if err := node.PolicyStoreHandshake(); err != nil {
		panic(err)
	}

	// Wait for SIGTERM signal from the environment
	channel := make(chan os.Signal, 2)
	signal.Notify(channel, os.Interrupt, syscall.SIGTERM)
	<-channel

	// Clean-up
	node.Shutdown()
}

func setConfigurations(configFile string) error {
	conf := configor.New(&configor.Config{
		ErrorOnUnmatchedKeys: true,
		Verbose: true,
		Debug: true,
	})

	return conf.Load(&NodeConfig, configFile)
}

func Exists(name string) bool {
	if _, err := os.Stat(name); err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return true
}
