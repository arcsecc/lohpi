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
	PolicyStoreAddr         string `default:"127.0.1.1:8084"`
	MuxAddr                 string `default:"127.0.1.1:8081"`
	LohpiCaAddr             string `default:"127.0.1.1:8301"`
	AzureKeyVaultName       string `required:"true"`
	AzureKeyVaultSecret     string `required:"true"`
	AzureClientSecret       string `required:"true"`
	AzureClientID           string `required:"true"`
	AzureKeyVaultBaseURL    string `required:"true"`
	AzureTenantID           string `required:"true"`
	AzureStorageAccountName string `required:"true"`
	AzureStorageAccountKey  string `required:"true"`
}{}

/* Dummy program that spawns n nodes */
func main() {
	var configFile string
	var nNodes int

	// Logfile and name flags
	args := flag.NewFlagSet("args", flag.ExitOnError)
	args.StringVar(&configFile, "c", "", `Configuration file for the node.`)
	args.IntVar(&nNodes, "n", 1, "Number of nodes to boot.")
	args.Parse(os.Args[1:])

	configor.New(&configor.Config{Debug: false, ENVPrefix: "PS_NODE"}).Load(&config, configFile)

	if configFile == "" {
		log.Errorln("Configuration file must not be empty. Exiting.")
		os.Exit(2)
	}

	if nNodes < 1 {
		log.Errorln("Number of nodes must be greater than zero. Exiting.")
		os.Exit(2)
	}

	nodes, err := newNodes(nNodes)
	if err != nil {
		log.Errorln(err.Error())
		os.Exit(1)
	}

	// Join the network and index the dataset
	for _, node := range nodes {
		if err := node.JoinNetwork(); err != nil {
			log.Errorln(err.Error())
			os.Exit(1)
		}

		sets := datasets(node.Name(), 10)
		for _, s := range sets {
			if err := node.IndexDataset(s); err != nil {
				log.Errorln(err.Error())
				os.Exit(1)
			}
		}
	}

	// Wait for SIGTERM signal from the environment
	channel := make(chan os.Signal, 2)
	signal.Notify(channel, os.Interrupt, syscall.SIGTERM)
	<-channel

	// sync here
	for _, node := range nodes {
		go node.Shutdown()
	}

	os.Exit(0)
}

func newNodes(n int) ([]*lohpi.Node, error) {
	nodes := make([]*lohpi.Node, 0)

	for i := 0; i < n; i++ {
		name := fmt.Sprintf("node_%d", i + 1)
		opts, err := getNodeConfiguration(name)
		if err != nil {
			return nil, err
		}

		node, err := lohpi.NewNode(opts...)
		if err != nil {
			return nil, err
		}

		nodes = append(nodes, node)
	}

	// Set the http handlers

	return nodes, nil
}

func startNode(node *lohpi.Node) error {
	return node.JoinNetwork()
}

func getNodeConfiguration(name string) ([]lohpi.NodeOption, error) {
	var opts []lohpi.NodeOption

	dbConn, err := getDatabaseConnectionString()
	if err != nil {
		return nil, err
	}

	opts = []lohpi.NodeOption{
		lohpi.NodeWithPostgresSQLConnectionString(dbConn),
		lohpi.NodeWithMultipleCheckouts(true),
		lohpi.NodeWithPolicyObserverWorkingDirectory(name),
		lohpi.NodeWithName(name))
	}

	return opts, nil
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

func datasets(nodename string, nDatasets int) []string {
	sets := make([]string, 0)
	for i := 0; i < nDatasets; i++ {
		s := fmt.Sprintf("%s.dataset_%d", nodename, i+1)
		sets = append(sets, s)
	}
	return sets
}

func newAzureKeyVaultClient() (*lohpi.AzureKeyVaultClient, error) {
	c := &lohpi.AzureKeyVaultClientConfig{
		AzureKeyVaultClientID:     config.AzureClientID,
		AzureKeyVaultClientSecret: config.AzureClientSecret,
		AzureKeyVaultTenantID:     config.AzureTenantID,
	}

	return lohpi.NewAzureKeyVaultClient(c)
}
