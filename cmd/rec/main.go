package main

import (
	"log"
	"flag"
	"os"
	"os/signal"
	"syscall"

	logger "github.com/inconshreveable/log15"
	"github.com/tomcat-bit/lohpi/pkg/rec"
	"github.com/jinzhu/configor"
)

var config = struct {
	RecConfig rec.RecConfig
}{}

func main() {
	var logfile string
	var configFile string
	var h logger.Handler

	arg := flag.NewFlagSet("args", flag.ExitOnError)
	arg.StringVar(&logfile, "log", "", "Log file path.")
	arg.StringVar(&configFile, "c", "config.yml", `Configuration file for REC. If not set, use default configuration values.`)
	arg.Parse(os.Args[1:])

	r := logger.Root()

	/** Check the flags. Exit appropriately */
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

	recInstance, err := rec.NewRec(&config.RecConfig)
	if err != nil {
		panic(err)
	}

	go recInstance.Start()

	channel := make(chan os.Signal, 2)
	signal.Notify(channel, os.Interrupt, syscall.SIGTERM)
	<-channel

	recInstance.Stop()
}

func setConfigurations(configFile string) error {
	conf := configor.New(&configor.Config{
		ErrorOnUnmatchedKeys: true,
		Verbose: true,
		Debug: true,
	})

	return conf.Load(&config, configFile)
}

func exists(name string) bool {
	if _, err := os.Stat(name); err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return true
}
