package main

import (
	//"fmt"
	"flag"
	"os"
	"os/signal"
	"syscall"

	logger "github.com/inconshreveable/log15"
	"github.com/jinzhu/configor"
	"github.com/arcsecc/lohpi/core/mux"
)

type Launcher struct {
	mux *mux.Mux
}

var config = struct {
	MuxConfig mux.MuxConfig
}{}

func main() {
	var h logger.Handler
	var configFile string
	var logfile string

	arg := flag.NewFlagSet("args", flag.ExitOnError)
	arg.StringVar(&configFile, "c", "config.yml", "Multiplexer's configuration file.")
	arg.StringVar(&logfile, "log", "", "Logfile's path. If not set, print log to standard output.")
	arg.Parse(os.Args[1:])

	r := logger.Root()
	if logfile != "" {
		h = logger.CallerFileHandler(logger.Must.FileHandler(logfile, logger.LogfmtFormat()))
	} else {
		h = logger.StreamHandler(os.Stdout, logger.LogfmtFormat())
	}
	r.SetHandler(h)

	if err := loadConfiguration(configFile); err != nil {
		panic(err)
	}

	m, err := mux.NewMux(&config.MuxConfig)
	if err != nil {
		panic(err)
	}

	go m.Start()

	channel := make(chan os.Signal, 2)
	signal.Notify(channel, os.Interrupt, syscall.SIGTERM)
	<-channel

	m.Stop()
}

func loadConfiguration(configFile string) error {
	conf := configor.New(&configor.Config{
		ErrorOnUnmatchedKeys: true,
		Verbose:              true,
		Debug:                true,
	})

	return conf.Load(&config, configFile)
}
