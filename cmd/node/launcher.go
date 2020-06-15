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

	logger "github.com/inconshreveable/log15"
	"github.com/spf13/viper"
	"github.com/tomcat-bit/lohpi/internal/core/node"
)

func main() {
	var logfile string
	var nodeName string
	var h logger.Handler
	//var muxPort uint
	//var policyStorePort uint

	runtime.GOMAXPROCS(runtime.NumCPU())

	if err := readConfig(); err != nil {
		panic(err)
	}

	// Logfile and name flags
	arg := flag.NewFlagSet("args", flag.ExitOnError)
	arg.StringVar(&logfile, "logfile", "", "Absolute or relative path to log file.")
	arg.StringVar(&nodeName, "name", "", "Human-readable identifier of node.")
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

	// Create the new node and let it live its own life
	node, err := node.NewNode(nodeName)
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

func Exists(name string) bool {
	if _, err := os.Stat(name); err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return true
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
	viper.SetDefault("fuse_mount", "/home/thomas/go/src/firestore")
	viper.SetDefault("lohpi_mux_addr", "127.0.1.1:8080")
	viper.SetDefault("policy_store_addr", "127.0.1.1:8082")
	viper.SetDefault("lohpi_ca_addr", "127.0.1.1:8301")
	viper.SafeWriteConfig()
	return nil
}
