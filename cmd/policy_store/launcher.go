package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"
	"log"

	logger "github.com/inconshreveable/log15"
	"github.com/arcsecc/lohpi/core/policy"
	"github.com/jinzhu/configor"
)

var psConfig = struct {
	PolicyStoreConfig policy.PolicyStoreConfig
}{}

func main() {
	var h logger.Handler
	var configFile string
	var logfile string

	arg := flag.NewFlagSet("args", flag.ExitOnError)
	arg.StringVar(&configFile, "c", "config.yml", `Configuration file for policy store. If not set, use default configuration values.`)
	arg.StringVar(&logfile, "log", "", "Logfile's path. If not set, print log to standard output.")
	arg.Parse(os.Args[1:])

	r := logger.Root()
	if logfile != "" {
		h = logger.CallerFileHandler(logger.Must.FileHandler(logfile, logger.LogfmtFormat()))
	} else {
		h = logger.StreamHandler(os.Stdout, logger.LogfmtFormat())
	}
	r.SetHandler(h)

	if !exists(configFile) {
		log.Println("Config file not found. Using default configurations.")
	}

	if err := setConfigurations(configFile); err != nil {
		panic(err)
	}
	
	ps, err := policy.NewPolicyStore(&psConfig.PolicyStoreConfig)
	if err != nil {
		panic(err)
	}

	go ps.Start()

	channel := make(chan os.Signal, 2)
	signal.Notify(channel, os.Interrupt, syscall.SIGTERM)
	<-channel

	ps.Stop()
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
