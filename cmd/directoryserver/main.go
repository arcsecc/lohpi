package main

import (
	"flag"
	"fmt"
	"github.com/arcsecc/lohpi"
	"github.com/jinzhu/configor"
	log "github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"syscall"	
)

var config = struct {
	Name                            string `required:"true"`
	HTTPPort                        int    `default:"8080"`
	GRPCPort                        int    `default:"8081"`
	LohpiCaAddress                  string `default:"127.0.1.1:8301"`
	AzureKeyVaultName               string `required:"true"`
	AzureKeyVaultSecret             string `required:"true"`
	AzureClientSecret               string `required:"true"`
	AzureClientID                   string `required:"true"`
	AzureKeyVaultBaseURL            string `required:"true"`
	AzureTenantID                   string `required:"true"`
	LohpiCryptoUnitWorkingDirectory string `required:"true"`
	IfritCryptoUnitWorkingDirectory string `required:"true"`
	Hostnames                       []string `required:"true"`
	IfritTCPPort                    int    `required:"true"`
	IfritUDPPort                    int    `required:"true"`
	DBConnPoolSize                  int    `required:"true"`
}{}

func main() {
	var configFile string
	var createNew bool
	var useCA bool
	var useACME bool

	args := flag.NewFlagSet("args", flag.ExitOnError)
	args.StringVar(&configFile, "c", "", "Directory server's configuration file.")
	args.BoolVar(&createNew, "new", false, "Initialize new Lohpi directory server instance.")
	args.BoolVar(&useCA, "useca", false, "If true, contact CA at a known address. Otherwise, use a self-signed certificate")
	args.BoolVar(&useACME, "useacme", false, "If true, use Let's Encrypt and override the 'useca' flag.")
	args.Parse(os.Args[1:])

	//	log.SetLevel(log.ErrorLevel)

	if configFile == "" {
		log.Errorln("Configuration file must be provided. Exiting.")
		os.Exit(2)
	}

	configor.New(&configor.Config{
		Debug: true,
		ENVPrefix: "DIRECTORYSERVER",
		ErrorOnUnmatchedKeys: true}).Load(&config, configFile)

	var d *lohpi.DirectoryServer
	var err error

	config, err := getDirectoryServerConfiguration()
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	d, err = lohpi.NewDirectoryServer(config, createNew, useCA, useACME)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	go d.Start()

	channel := make(chan os.Signal, 2)
	signal.Notify(channel, os.Interrupt, syscall.SIGTERM)
	<-channel

	d.Stop()
}

func getDirectoryServerConfiguration() (*lohpi.DirectoryServerConfig, error) {
	connString, err := getDatabaseConnectionString()
	if err != nil {
		return nil, err
	}

	return &lohpi.DirectoryServerConfig{
		CaAddress: config.LohpiCaAddress,
		Name: config.Name,
		SQLConnectionString: connString,
		HTTPPort: config.HTTPPort,
		GRPCPort: config.GRPCPort,
		Hostnames: config.Hostnames,
		LohpiCryptoUnitWorkingDirectory: config.LohpiCryptoUnitWorkingDirectory,
		IfritCryptoUnitWorkingDirectory: config.IfritCryptoUnitWorkingDirectory,
		IfritTCPPort: config.IfritTCPPort,
		IfritUDPPort: config.IfritUDPPort,
		DBConnPoolSize: config.DBConnPoolSize,
	}, nil
}

func getDatabaseConnectionString() (string, error) {
	kvClient, err := newAzureKeyVaultClient()
	if err != nil {
		return "", err
	}

	resp, err := kvClient.GetSecret(config.AzureKeyVaultBaseURL, config.AzureKeyVaultSecret)
	if err != nil {
		return "", err
	}

	return resp.Value, nil
}

func newAzureKeyVaultClient() (*lohpi.AzureKeyVaultClient, error) {
	c := &lohpi.AzureKeyVaultClientConfig{
		AzureKeyVaultClientID: config.AzureClientID,
		AzureKeyVaultClientSecret: config.AzureClientSecret,
		AzureKeyVaultTenantID: config.AzureTenantID,
	}

	return lohpi.NewAzureKeyVaultClient(c)
}
