package main

import (
	"time"
	"flag"
	"fmt"
	"github.com/arcsecc/lohpi"
_	"github.com/go-redis/redis/v8"
	"github.com/jinzhu/configor"
	log "github.com/sirupsen/logrus"
_	"github.com/inconshreveable/log15"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

var config = struct {
	PolicyStoreAddr         string `default:"127.0.1.1:8084"`
	DirectoryServerAddr     string `default:"127.0.1.1:8081"`
	LohpiCaAddr             string `default:"127.0.1.1:8301"`
	AzureKeyVaultName       string `required:"true"`
	AzureKeyVaultSecret     string `required:"true"`
	AzureClientSecret       string `required:"true"`
	AzureClientID           string `required:"true"`
	AzureKeyVaultBaseURL    string `required:"true"`
	AzureTenantID           string `required:"true"`
	AzureStorageAccountName string `required:"true"`
	AzureStorageAccountKey  string `required:"true"`
	IfritTCPPort            int    `required:"true"`
	IfritUDPPort            int    `required:"true"`
	HTTPPort                int    `required:"true"`
	LohpiCryptoUnitWorkingDirectory string `required:"true"`
	IfritCryptoUnitWorkingDirectory string `required:"true"`
}{}

type node struct {
	lohpiNode *lohpi.Node
	handler   func(id string, w http.ResponseWriter, r *http.Request)
}

type App struct {
	nodes []*node
}

func main() {
	var configFile string
	var nNodes int
	var nSets int

	// Logfile and name flags
	args := flag.NewFlagSet("args", flag.ExitOnError)
	args.StringVar(&configFile, "c", "", `Configuration file for the node.`)
	args.IntVar(&nNodes, "nodes", 0, "Number of nodes to boot.")
	args.IntVar(&nSets, "sets", 0, "Number of sets per node.")
	args.Parse(os.Args[1:])

	log.SetLevel(log.ErrorLevel)

	configor.New(&configor.Config{
		Debug:     true,
		ENVPrefix: "PS_NODE"}).Load(&config, configFile)

	if configFile == "" {
		log.Errorln("Configuration file must not be empty. Exiting.")
		os.Exit(2)
	}

	if nNodes < 1 {
		log.Errorln("Number of nodes must be greater than zero. Exiting.")
		os.Exit(2)
	}

	if nSets < 1 {
		log.Errorln("Number of setse per node must be greater than zero. Exiting.")
		os.Exit(2)
	}

	app, err := NewApp(nNodes, nSets)
	if err != nil {
		log.Errorln(err.Error())
		os.Exit(1)
	}

	// Ifrit logging
	/*r := log15.Root()
	h := log15.CallerFileHandler(log15.Must.FileHandler("ifrit.log", log15.LogfmtFormat()))
	r.SetHandler(h)*/

	// Wait for SIGTERM signal from the environment
	channel := make(chan os.Signal, 2)
	signal.Notify(channel, os.Interrupt, syscall.SIGTERM)
	<-channel

	app.Stop()
	os.Exit(0)
}

func NewApp(nNodes, nSets int) (*App, error) {
	nodes := make([]*node, 0)
	ifritTCTPort := config.IfritTCPPort
	ifritUDPPort := config.IfritUDPPort
	httpPort := config.HTTPPort

	for i := 0; i < nNodes; i++ {
		func () {
			name := fmt.Sprintf("node%d", i+1)
			log.Warnln("Booting node %s", name)
			defer func() { ifritTCTPort += 100 }()
			defer func() { ifritUDPPort += 100}()
			defer func() { httpPort += 1}()

			c, err := getNodeConfiguration(name, httpPort, ifritTCTPort, ifritUDPPort)
			if err != nil {
				log.Errorln(err.Error())
				return
			}
			
			lohpiNode, err := lohpi.NewNode(c, true)
			if err != nil {
				log.Errorln(err.Error())
				return
			}

			lohpiNode.RegisterDatasetHandler(handler)

			go lohpiNode.Start()

			if err := lohpiNode.HandshakeNetwork(config.DirectoryServerAddr, config.PolicyStoreAddr); err != nil {
				log.Errorln(err.Error())
			}

			// Create datasets
			sets := datasets(name, nSets)
			for _, s := range sets {
				if err := lohpiNode.IndexDataset(s, &lohpi.DatasetIndexingOptions{AllowMultipleCheckouts: true}); err != nil {
					log.Errorln(err.Error())
					return
				}
			}

			node := &node{
				lohpiNode: lohpiNode,
				handler:   handler,
			}

			nodes = append(nodes, node)
			time.Sleep(0 * time.Second)
		}()
	}

	return &App{
		nodes: nodes,
	}, nil
}

func (a *App) Stop() {

}

// Handler invoked by each node
func handler(id string, w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	log.Println("Invoked handler! Dataset:", id)
	fmt.Fprintln(w, "Invoked handler! Dataset:", id)
}

func getNodeConfiguration(name string, httpPort int, ifritTCPPort int, ifritUDPPort int) (*lohpi.NodeConfig, error) {
	dbConnString, err := getDatabaseConnectionString()
	if err != nil {
		return nil, err
	}

	c := &lohpi.NodeConfig{
		CaAddress: config.LohpiCaAddr,
		Name: name,
		SQLConnectionString: dbConnString,
		Port: httpPort,
		LohpiCryptoUnitWorkingDirectory: config.LohpiCryptoUnitWorkingDirectory,
		IfritCryptoUnitWorkingDirectory: config.IfritCryptoUnitWorkingDirectory,
		IfritTCPPort: ifritTCPPort,
		IfritUDPPort: ifritUDPPort,
	}
	
	return c, nil
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
		s := fmt.Sprintf("%sdataset%d", nodename, i+1)
		sets = append(sets, s)
	}
	return sets
}

func newAzureKeyVaultClient() (*lohpi.AzureKeyVaultClient, error) {
	return lohpi.NewAzureKeyVaultClient(&lohpi.AzureKeyVaultClientConfig{
		AzureKeyVaultClientID: config.AzureClientID,
		AzureKeyVaultClientSecret: config.AzureClientSecret,
		AzureKeyVaultTenantID: config.AzureTenantID})
}
