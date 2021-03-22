package main

import (
	"fmt"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/jinzhu/configor"
	"github.com/arcsecc/lohpi/core/mux"
	log "github.com/sirupsen/logrus"
)

var config = struct {
	HttpPort   				int     `default:"8080"`
	GRPCPort 				int     `default:"8081"`
	LohpiCaAddr 			string 	`default:"127.0.1.1:8301"`
}{}

func main() {
	var configFile string
	var createNew bool

	args := flag.NewFlagSet("args", flag.ExitOnError)
	args.StringVar(&configFile, "c", "lohpi_config.yaml", "Mux's configuration file.")
	args.BoolVar(&createNew, "new", false, "Initialize new Lohpi mux instance")
	args.Parse(os.Args[1:])

	configor.New(&configor.Config{Debug: false, ENVPrefix: "MUX"}).Load(&config, configFile)

	var m *mux.Mux
	var err error
	
	if createNew {
		c := &mux.Config{
			HttpPort: config.HttpPort,
			GRPCPort: config.GRPCPort,
			LohpiCaAddr: config.LohpiCaAddr,
		}

		log.Println("Config:", c)

		m, err = mux.NewMux(c)
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
	} else {
		log.Fatalln("Need to set the 'new' flag to true. Exiting")
	}
	//m.InitializeLogfile(logging)

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
