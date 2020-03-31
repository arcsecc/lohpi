package main

import (
	"flag"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	lohpi_ca "firestore/cauth"

	log "github.com/inconshreveable/log15"
	ifrit_ca "github.com/joonnna/ifrit/cauth"
)

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

func main() {
	var logfile string
	var h log.Handler

	runtime.GOMAXPROCS(runtime.NumCPU())

	args := flag.NewFlagSet("args", flag.ExitOnError)
	args.StringVar(&logfile, "logfile", "", "Log to file.")
	args.Parse(os.Args[1:])

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
	l_ca, err := lohpi_ca.NewCa()
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
