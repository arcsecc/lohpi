package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/arcsecc/lohpi"
	"github.com/jinzhu/configor"
	log "github.com/sirupsen/logrus"
)

var config = struct {
	HTTPPort             int    `default:"8080"`
	GRPCPort             int    `default:"8081"`
	LohpiCaAddr          string `default:"127.0.1.1:8301"`
	AzureKeyVaultName    string `required:"true"`
	AzureKeyVaultSecret  string `required:"true"`
	AzureClientSecret    string `required:"true"`
	AzureClientID        string `required:"true"`
	AzureKeyVaultBaseURL string `required:"true"`
	AzureTenantID        string `required:"true"`
}{}

func main() {
	var configFile string
	var createNew bool

	args := flag.NewFlagSet("args", flag.ExitOnError)
	args.StringVar(&configFile, "c", "", "Directory server's configuration file.")
	args.BoolVar(&createNew, "new", false, "Initialize new Lohpi directory server instance.")
	args.Parse(os.Args[1:])

	if configFile == "" {
		log.Errorln(")Configuration file must be provided. Exiting.")
		os.Exit(2)
	}

	configor.New(&configor.Config{
		Debug:                true,
		ENVPrefix:            "DIRECTORYSERVER",
		ErrorOnUnmatchedKeys: true}).Load(&config, configFile)

	var d *lohpi.DirectoryServer
	var err error

	if createNew {
		config := getDirectoryServerConfiguration()
		d, err = lohpi.NewDirectoryServer(&config)
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

func getDirectoryServerConfiguration() lohpi.DirectoryServerConfig {
	constring, err := getDatabaseConnectionString()
	if err != nil {
		panic(err)
	}

	return lohpi.DirectoryServerConfig{
		CaAddress:           config.LohpiCaAddr,
		Name:                "Lohpi directory server",
		SQLConnectionString: constring,
		HTTPPort:            config.HTTPPort,
		GRPCPort:            config.GRPCPort,
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

func getDatabaseConnectionString() (string, error) {
	kvClient, err := newAzureKeyVaultClient()
	if err != nil {
		return "", err
	}

	resp, err := kvClient.GetSecret(config.AzureKeyVaultBaseURL, config.AzureKeyVaultSecret)
	if err != nil {
		log.Warnln(err)
		return "", err
	}
	return resp.Value, nil
}

func newAzureKeyVaultClient() (*lohpi.AzureKeyVaultClient, error) {
	c := &lohpi.AzureKeyVaultClientConfig{
		AzureKeyVaultClientID:     config.AzureClientID,
		AzureKeyVaultClientSecret: config.AzureClientSecret,
		AzureKeyVaultTenantID:     config.AzureTenantID,
	}

	return lohpi.NewAzureKeyVaultClient(c)
}
