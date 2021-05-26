package main

import (
	"fmt"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/jinzhu/configor"
	"github.com/arcsecc/lohpi"
	log "github.com/sirupsen/logrus"
)

var config = struct {
	HTTPPort   				int     `default:"8080"`
	GRPCPort 				int     `default:"8081"`
	LohpiCaAddr 			string 	`default:"127.0.1.1:8301"`
}{}

func main() {
	var configFile string
	var createNew bool

	args := flag.NewFlagSet("args", flag.ExitOnError)
	args.StringVar(&configFile, "c", "lohpi_config.yaml", "Directory server's configuration file.")
	args.BoolVar(&createNew, "new", false, "Initialize new Lohpi directory server instance.")
	args.Parse(os.Args[1:])

	configor.New(&configor.Config{Debug: false, ENVPrefix: "DIRECTORYSERVER"}).Load(&config, configFile)

	var d *lohpi.DirectoryServer
	var err error
	
	if createNew {
		opts := getDirectoryServerConfiguration()
		d, err = lohpi.NewDirectoryServer(opts...)
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
	} else {
		log.Fatalln("Need to set the 'new' flag to true. Exiting")
	}
	//m.InitializeLogfile(logging)

	go d.Start()

	channel := make(chan os.Signal, 2)
	signal.Notify(channel, os.Interrupt, syscall.SIGTERM)
	<-channel

	d.Stop()
}

func getDirectoryServerConfiguration() []lohpi.DirectoryServerOption {
	return []lohpi.DirectoryServerOption{
		lohpi.DirectoryServerWithHTTPPort(config.HTTPPort),
	}	
}

func initializeLogging(logToFile bool) error {
	logfilePath := "DirectoryServerCore.log"

	if logToFile {
		file, err := os.OpenFile(logfilePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			log.SetOutput(os.Stdout)
			return fmt.Errorf("Could not open logfile %s. Error: %s", logfilePath, err.Error())
		}
		log.SetOutput(file)
		log.SetFormatter(&log.TextFormatter{})
	} else {
		log.Infoln("Setting logs to standard output")
		log.SetOutput(os.Stdout)
	}

	return nil
}