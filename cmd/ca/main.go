package main

import (
	"fmt"
	"strconv"
	"flag"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	lohpi_ca "github.com/arcsecc/lohpi/cauth"

	log "github.com/sirupsen/logrus"
	ifrit_ca "github.com/joonnna/ifrit/cauth"
	"github.com/jinzhu/configor"
)

var DefaultPermission = os.FileMode(0750)

// Config contains all configurable parameters for the Ifrit CA daemon.
var IfritCaConfig = struct {
	Name         string `default:"Ifrit Certificate Authority"`
	Version      string `default:"1.0.0"`
	Host         string `default:"127.0.1.1"`
	Port         int    `default:"8300"`
	Path         string `default:"./ifrit-cad"`
	NumRings     uint32 `default:"30"`
	NumBootNodes uint32 `default:"30"`
	LogFile      string `default:""`
}{}

// Config contains all configurable parameters for the Lohpi CA daemon.
var LohpiCaConfig = struct {
	Name         string `default:"Lohpi Certificate Authority"`
	Version      string `default:"1.0.0"`
	Host         string `default:"127.0.1.1"`
	Port         int    `default:"8301"`
	Path         string `default:"./lohpi-cad"`
	LogFile      string `default:"ca.log"`
}{}

func main() {
	var createNew bool
	var lohpiConfigFile string
	var ifritConfigFile string

	runtime.GOMAXPROCS(runtime.NumCPU())

	args := flag.NewFlagSet("args", flag.ExitOnError)
	args.StringVar(&ifritConfigFile, "ifritconfig", "", "Ifrit CA configuration file.")
	args.StringVar(&lohpiConfigFile, "lohpiconfig", "", "Lohpi CA configuration file.")
	args.BoolVar(&createNew, "new", false, "Initialize new Ca structures")
	args.Parse(os.Args[1:])

	configor.New(&configor.Config{Debug: false, ENVPrefix: "IFRIT_CA"}).Load(&IfritCaConfig, ifritConfigFile, "/etc/ifrit/config.yaml")
	configor.New(&configor.Config{Debug: false, ENVPrefix: "LOHPI_CA"}).Load(&LohpiCaConfig, lohpiConfigFile, "/etc/lohpi/config.yaml")

	var ifritCa *ifrit_ca.Ca
	var lohpiCa *lohpi_ca.Ca
	var err error

	log.SetFormatter(&log.TextFormatter{DisableLevelTruncation: false})

	// Attempt to load existing CAs
	if !createNew {
		ifritCa, err = ifrit_ca.LoadCa(IfritCaConfig.Path, IfritCaConfig.NumBootNodes, IfritCaConfig.NumRings)
		if err != nil {
			panic(err)
			fmt.Fprintln(os.Stderr, "Error loading Ifrit CA. Run with --new option if Ifrit CA does not exit.")
			os.Exit(1)
		}

		lohpiCa, err = lohpi_ca.LoadCa(LohpiCaConfig.Path)
		if err != nil {
			panic(err)
			fmt.Fprintln(os.Stderr, "Error loading Lohpi CA. Run with --new option if Lohpi CA does not exit.")
			os.Exit(1)
		}
	} else {
		// Create Ifrit run directory
		err := os.MkdirAll(IfritCaConfig.Path, DefaultPermission)
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}

		ifritCa, err = ifrit_ca.NewCa(IfritCaConfig.Path)
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}

		// Add initial group.
		err = ifritCa.NewGroup(IfritCaConfig.NumRings, IfritCaConfig.NumBootNodes)
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}

		// Create Lohpi run directory
		err = os.MkdirAll(LohpiCaConfig.Path, DefaultPermission)
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}

		lohpiCa, err = lohpi_ca.NewCa(LohpiCaConfig.Path)
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
	}

	saveIfritState(ifritCa)
	defer saveIfritState(ifritCa)
	go ifritCa.Start(IfritCaConfig.Host, strconv.Itoa(IfritCaConfig.Port))
	
	saveLohpiState(lohpiCa)
	defer saveLohpiState(lohpiCa)
	go lohpiCa.Start(LohpiCaConfig.Port)

	channel := make(chan os.Signal, 2)
	signal.Notify(channel, os.Interrupt, syscall.SIGTERM)
	<-channel

	ifritCa.Shutdown()
	lohpiCa.Shutdown()
}

// saveState saves ca private key and public certificates to disk.
func saveIfritState(ca *ifrit_ca.Ca) {
	err := ca.SavePrivateKey()
	if err != nil {
		panic(err)
	}

	err = ca.SaveCertificate()
	if err != nil {
		panic(err)
	}
}

// saveState saves ca private key and public certificates to disk.
func saveLohpiState(ca *lohpi_ca.Ca) {
	err := ca.SavePrivateKey()
	if err != nil {
		panic(err)
	}

	err = ca.SaveCertificate()
	if err != nil {
		panic(err)
	}
}

func initializeLogfile(logfile string) error {
	file, err := os.OpenFile(logfile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.SetOutput(os.Stdout)
		return fmt.Errorf("Could not open logfile %s. Error: %s", logfile, err.Error())
	}

	log.SetOutput(file)
	log.SetFormatter(&log.TextFormatter{})
	return nil
}
