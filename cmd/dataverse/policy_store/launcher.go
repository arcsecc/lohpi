package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"
	"fmt"

	log "github.com/sirupsen/logrus"
	"github.com/arcsecc/lohpi"
	"github.com/jinzhu/configor"
)

var DefaultPermission = os.FileMode(0750)

// Config contains all configurable parameters for the Ifrit CA daemon.
var psConfig = struct {
	Name         				string 	`default:"Lohpi Policy store"`
	Version      				string 	`default:"1.0.0"`
	Host         				string 	`default:"127.0.1.1"`
	Port         				int    	`default:"8083"`
	GRPCPort					int 	`default:"8084"`
	Path         				string 	`default:"./policy-store"`
	BatchSize    				uint32 	`default:"3"`
	GossipInterval 				uint32 	`default:"60"`
	LogFile      				string 	`default:""`
	MulticastAcceptanceLevel 	float64 `default:0.5`
	LohpiCaAddr 				string 	`default:"127.0.1.1:8301"`
	MuxAddress 					string 	`default:"127.0.1.1:8081"`
	PolicyStoreGitRepository  	string 	`default:"/tmp/lohpi/policy_store/policies"`
	NumDirectRecipients			int		`default:"1"`
}{}

func main() {
	var psConfigFile string
	var createNew bool

	args := flag.NewFlagSet("args", flag.ExitOnError)
	args.StringVar(&psConfigFile, "c", "lohpi_config.yaml", `Configuration file for policy store. If not set, use default configuration values.`)
	args.BoolVar(&createNew, "new", false, "Initialize new Policy store instance")
	args.Parse(os.Args[1:])

	configor.New(&configor.Config{Debug: false, ENVPrefix: "PS"}).Load(&psConfig, psConfigFile, "./lohpi_config.yaml")

	var policyStore *lohpi.PolicyStore

	if createNew {
		policyStore, err := lohpi.NewPolicyStore(lohpi.PolicyStoreWithGitRepository(psConfig.PolicyStoreGitRepository))
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}

		go policyStore.Start()
		go policyStore.RunPolicyBatcher()
	} else {
		log.Fatalln("Need to set the 'new' flag to true. Exiting")
		os.Exit(1)
	}

	channel := make(chan os.Signal, 2)
	signal.Notify(channel, os.Interrupt, syscall.SIGTERM)
	<-channel

	policyStore.Stop()
}

func setConfigurations(configFile string) error {
	conf := configor.New(&configor.Config{
		ErrorOnUnmatchedKeys: true,
		Verbose: true,
		Debug: true,
	})

	return conf.Load(&psConfig, configFile)
}

func exists(name string) bool {
	if _, err := os.Stat(name); err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return true
}
