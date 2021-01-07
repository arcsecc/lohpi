package main

import (
	"fmt"
	"flag"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	lohpi_ca "github.com/tomcat-bit/lohpi/cauth"

	log "github.com/inconshreveable/log15"
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
	NumRings     uint32 `default:"3"`
	NumBootNodes uint32 `default:"0"`
	LogFile      string `default:""`
}{}

// Config contains all configurable parameters for the Lohpi CA daemon.
var LohpiCaConfig = struct {
	Name         string `default:"Lohpi Certificate Authority"`
	Version      string `default:"1.0.0"`
	Host         string `default:"127.0.1.1"`
	Port         int    `default:"8301"`
	Path         string `default:"./lohpi-cad"`
	LogFile      string `default:""`
}{}

func main() {
	var createNew bool
	var logfile string
	var lohpiConfigFile string
	var ifritConfigFile string
	var h log.Handler

	runtime.GOMAXPROCS(runtime.NumCPU())

	args := flag.NewFlagSet("args", flag.ExitOnError)
	args.StringVar(&logfile, "logfile", "", "Log to file.")
	args.StringVar(&ifritConfigFile, "ifritconfig", "", "Ifrit CA configuration file.")
	args.StringVar(&lohpiConfigFile, "lohpiconfig", "", "Lohpi CA configuration file.")
	args.BoolVar(&createNew, "new", false, "Initialize new Ca structures")
	args.Parse(os.Args[1:])

	configor.New(&configor.Config{Debug: false, ENVPrefix: "IFRIT"}).Load(&IfritCaConfig, ifritConfigFile, "/etc/ifrit/config.yaml")
	configor.New(&configor.Config{Debug: false, ENVPrefix: "IFRIT"}).Load(&LohpiCaConfig, lohpiConfigFile, "/etc/lohpi/config.yaml")
	
	r := log.Root()

	if logfile != "" {
		h = log.CallerFileHandler(log.Must.FileHandler(logfile, log.LogfmtFormat()))
	} else {
		h = log.StreamHandler(os.Stdout, log.LogfmtFormat())
	}

	r.SetHandler(h)

	var ifritCa *ifrit_ca.Ca
	var lohpiCa *lohpi_ca.Ca
	var err error

	// Attempt to load existing CAs
	if !createNew {
		ifritCa, err = ifrit_ca.LoadCa(IfritCaConfig.Path)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Error loading CA. Run with --new option if CA does not exit.")
			os.Exit(1)
		}

		lohpiCa, err = lohpi_ca.LoadCa(LohpiCaConfig.Path)
		if err != nil {
			fmt.Println(err)
			fmt.Fprintln(os.Stderr, "Error loading CA. Run with --new option if CA does not exit.")
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
	go ifritCa.Start(IfritCaConfig.Host, IfritCaConfig.Port)
	
	saveLohpiState(lohpiCa)
	defer saveLohpiState(lohpiCa)
	go lohpiCa.Start(LohpiCaConfig.Host, LohpiCaConfig.Port)

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

func setConfiguration(configFile string, c interface{}) error {
	conf := configor.New(&configor.Config{
		ErrorOnUnmatchedKeys: true,
		Verbose: true,
		Debug: true,
	})

	return conf.Load(c, configFile)
}

func exists(name string) bool {
	if _, err := os.Stat(name); err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return true
}
