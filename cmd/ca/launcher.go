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

func main() {
	var logfile string
	var configFile string
	var h log.Handler

	var caConfig = struct {
		CaConfig lohpi_ca.CaConfig
	}{}

	runtime.GOMAXPROCS(runtime.NumCPU())

	args := flag.NewFlagSet("args", flag.ExitOnError)
	args.StringVar(&logfile, "logfile", "", "Log to file.")
	args.StringVar(&configFile, "config", "", "Configuration file.")
	args.Parse(os.Args[1:])

	if configFile == "" {
		fmt.Fprintf(os.Stderr, "Configuration file must be supplied. Exiting\n")
			os.Exit(2)
	} else {
		if !exists(configFile) {
			fmt.Fprintf(os.Stderr, "Configuration file '%s' does not exist. Exiting\n", configFile)
			os.Exit(2)
		}
		if err := setConfiguration(configFile, &caConfig); err != nil {
			panic(err)
		}
	}

	r := log.Root()

	if logfile != "" {
		h = log.CallerFileHandler(log.Must.FileHandler(logfile, log.LogfmtFormat()))
	} else {
		h = log.StreamHandler(os.Stdout, log.LogfmtFormat())
	}

	r.SetHandler(h)

	// This is the Ifrit CA
	ca, err := ifrit_ca.NewCa()
	if err != nil {
		panic(err)
	}

	saveState(ca)
	defer saveState(ca)
	go ca.Start()

	// This is the Lohpi CA
	l_ca, err := lohpi_ca.NewCa(&caConfig.CaConfig)
	if err != nil {
		panic(err)
	}

	saveLohpiState(l_ca)
	defer saveLohpiState(l_ca)
	go l_ca.Start()

	channel := make(chan os.Signal, 2)
	signal.Notify(channel, os.Interrupt, syscall.SIGTERM)
	<-channel

	ca.Shutdown()
}

// saveState saves ca private key and public certificates to disk.
func saveState(ca *ifrit_ca.Ca) {
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
