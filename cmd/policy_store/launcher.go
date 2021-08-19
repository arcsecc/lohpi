package main

import (
	"flag"
	"time"
	"os"
	"os/signal"
	"syscall"

	log "github.com/sirupsen/logrus"
	"github.com/arcsecc/lohpi"
	"github.com/jinzhu/configor"
)

var DefaultPermission = os.FileMode(0750)

// Config contains all configurable parameters for the Ifrit CA daemon.
var config = struct {
	CaAddress 					string 	`default:"127.0.1.1:8301"`
	Name        				string 	`default:"Lohpi Policy store"`
	PolicyStoreGitRepository  	string 	`default:"/tmp/lohpi/policy_store/policies"`
	Host         				string 	`default:"127.0.1.1"`
	GossipInterval 				uint32 	`default:"60"`
	HTTPPort      				int    	`default:"8083"`
	GRPCPort					int 	`default:"8084"`
	MulticastAcceptanceLevel 	float64 `default:0.5`
	NumDirectRecipients			int		`default:"1"`
	DirectoryServerAddress      string 	`default:"127.0.1.1:8081"`
	AzureKeyVaultName       	string 	`required:"true"`
	AzureKeyVaultSecret     	string 	`required:"true"`
	AzureClientSecret       	string 	`required:"true"`
	AzureClientID           	string 	`required:"true"`
	AzureKeyVaultBaseURL    	string 	`required:"true"`
	AzureTenantID           	string 	`required:"true"`
	IfritTCPPort		 		int 	`required:"true"`
	IfritUDPPort		 		int 	`required:"true"`
}{}


func main() {
	var psConfigFile string
	var createNew bool

	args := flag.NewFlagSet("args", flag.ExitOnError)
	args.StringVar(&psConfigFile, "c", "./config/lohpi_config.yaml", `Configuration file for policy store. If not set, use default configuration values.`)
	args.BoolVar(&createNew, "new", false, "Initialize new Policy store instance")
	args.Parse(os.Args[1:])

	configor.New(&configor.Config{Debug: true, ENVPrefix: "PS"}).Load(&config, psConfigFile, psConfigFile)

	config, err := getPolicyStoreConfig()
	if err != nil {
		log.Fatalln(err.Error())
	}

	policyStore, err := lohpi.NewPolicyStore(config, createNew)
	if err != nil {
		log.Fatalln(err.Error())
	}

	go policyStore.Start()
	go policyStore.RunPolicyBatcher() // place me at a lower level?
	
	channel := make(chan os.Signal, 2)
	signal.Notify(channel, os.Interrupt, syscall.SIGTERM)
	<-channel

	policyStore.Stop()
}

func getPolicyStoreConfig() (*lohpi.PolicyStoreConfig, error) {
	dbConnString, err := getDatabaseConnectionString()
	if err != nil {
		return nil, err
	}

	return &lohpi.PolicyStoreConfig{
		Hostname: config.Host,
		CaAddress: config.CaAddress,
		Name: "Policy store",
		PolicyStoreGitRepository: config.PolicyStoreGitRepository,		
		GossipInterval: time.Duration(config.GossipInterval) * time.Second,
		HTTPPort: config.HTTPPort,
		GRPCPort: config.GRPCPort,
		MulticastAcceptanceLevel: config.MulticastAcceptanceLevel,
		NumDirectRecipients: config.NumDirectRecipients,
		DirectoryServerAddress: config.DirectoryServerAddress,
		SQLConnectionString: dbConnString,
		IfritTCPPort: config.IfritTCPPort,
		IfritUDPPort: config.IfritUDPPort,
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
		AzureKeyVaultClientID:     config.AzureClientID,
		AzureKeyVaultClientSecret: config.AzureClientSecret,
		AzureKeyVaultTenantID:     config.AzureTenantID,
	}

	return lohpi.NewAzureKeyVaultClient(c)
}