package main

/** Launcher.go launches one executable of the 'node' package and waits for a SIGTERM signal to arrive
 * from the environment. This should be used when we want to use a process-granularity run.
 */
 
import (
	"context"
	"net/http"
	"time"
	"io/ioutil"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	log "github.com/sirupsen/logrus"
	"github.com/jinzhu/configor"
	"github.com/arcsecc/lohpi/core/node"
)

var config = struct {
	Port				int 		`default:"8090"`
	PolicyStoreAddr 	string 		`default:"127.0.1.1:8084"`
	MuxAddr				string		`default:"127.0.1.1:8081"`
	LohpiCaAddr    		string 		`default:"127.0.1.1:8301"`
	RemoteBaseURL		string 		`required:"true"`
	RemotePort			string 		`required:"true"`
	AzureKeyVaultName 	string 		`required:"true"`
	AzureKeyVaultSecret	string		`required:"true"`
	AzureClientSecret	string 		`required:"true"`
	AzureClientId		string		`required:"true"`
	AzureKeyVaultBaseURL string		`required:"true"`
	AzureTenantId		string		`required:"true"`
}{}

type StorageNode struct {
	node *node.Node
}

func main() {
	var configFile string
	var createNew bool
	var nodeName string

	
	runtime.GOMAXPROCS(runtime.NumCPU())

	// Logfile and name flags
	args := flag.NewFlagSet("args", flag.ExitOnError)
	args.StringVar(&nodeName, "name", "", "Human-readable identifier of node.")
	args.StringVar(&configFile, "c", "", `Configuration file for the node`)
	args.BoolVar(&createNew, "new", false, "Initialize new Lohpi node.")
	args.Parse(os.Args[1:])

	if configFile == "" {
		log.Errorln("Configuration file needs to be set. Exiting.")
		os.Exit(2)
	}

	configor.New(&configor.Config{Debug: false, ENVPrefix: "PS_NODE"}).Load(&config, configFile)

	// Require node identifier
	if nodeName == "" {
		log.Errorln("Missing node identifier. Exiting.")
		os.Exit(2)
	}

	var sn *StorageNode
	var err error

	if createNew {
		nodeConfig := &node.Config{
			Port: config.Port,
			PolicyStoreAddr: config.PolicyStoreAddr,
			MuxAddr: config.MuxAddr,
			LohpiCaAddr: config.LohpiCaAddr,
			AzureKeyVaultName: config.AzureKeyVaultName,
			AzureKeyVaultSecret: config.AzureKeyVaultSecret,
			AzureClientSecret: config.AzureClientSecret,
			AzureKeyVaultBaseURL: config.AzureKeyVaultBaseURL,
			AzureClientId: config.AzureClientId,
			AzureTenantId: config.AzureTenantId,
		}

		env := os.Getenv("LOHPI_ENV")
		if env == "" {
			log.Errorln("LOHPI_ENV must be set. Exiting.")
			os.Exit(1)
		} else if env == "production" {
			log.Infoln("Production environment set")
		} else if env == "development" {
			log.Infoln("Development environment set")
		} else {
			log.Errorln("Unknown value for environment variable LOHPI_ENV:" + env + ". Exiting.")
			os.Exit(1)
		}
		log.Infof("Using %s as remote URL base\n", config.RemoteBaseURL)

		sn, err = newNodeStorage(nodeName, nodeConfig)
		if err != nil {
			log.Errorln(err.Error())
			os.Exit(1)
		}
	} else {
		log.Errorln("Need to set the 'new' flag to true. Exiting.")
		os.Exit(1)
	}
	
	go sn.Start()

	// Wait for SIGTERM signal from the environment
	channel := make(chan os.Signal, 2)
	signal.Notify(channel, os.Interrupt, syscall.SIGTERM)
	<-channel

	// Clean-up
	sn.Shutdown()
	os.Exit(0)
}

func exists(name string) bool {
	if _, err := os.Stat(name); err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return true
}

func newNodeStorage(name string, nodeConfig *node.Config) (*StorageNode, error) {
	n, err := node.NewNode(name, nodeConfig)
	if err != nil {
		return nil, err
	}

	sn := &StorageNode {
		node: n,
	}

	sn.node.RegisterDatasetHandler(sn.archiveHandler)
	sn.node.RegsiterMetadataHandler(sn.metadataHandler)
	
	if err := sn.node.JoinNetwork(); err != nil {
		return nil, err
	}

	return sn, nil
}

func (sn *StorageNode) Start() {
	if err := sn.initializePolicies(); err != nil {
		panic(err)
	}
}

func (sn *StorageNode) initializePolicies() error {
	ids, err := remoteDatasetIdentifiers()
	if err != nil {
		return err
	}

	for _, id := range ids {
		if err := sn.node.IndexDataset(id, context.Background()); err != nil {
			return err
		}
	}

	return nil
}

// Returns nescesarry information to fetch an external archive
func (s *StorageNode) archiveHandler(id string) (*node.ExternalDataset, error) {
	return remoteDataset(id)
}

// Returns a list of avaiable identifiers
func (s *StorageNode) datasetIdentifiersHandler() ([]string, error) {
	return remoteDatasetIdentifiers()
}

// Returns true if the given id exists in a remote location, returns false otherwise
func (s *StorageNode) identifierExistsHandler(id string) bool {
	return identifierExists(id)
}

func (s *StorageNode) metadataHandler(id string) (*node.ExternalMetadata, error) {
	return &node.ExternalMetadata{
		URL: config.RemoteBaseURL + "/api/datasets/export?exporter=dataverse_json&persistentId=" + id,
	}, nil 
}

func (s *StorageNode) Shutdown() {

}

func remoteDataset(id string) (*node.ExternalDataset, error) {
	return &node.ExternalDataset{
		URL: config.RemoteBaseURL + ":" + config.RemotePort + "/api/access/dataset/:persistentId/?persistentId=" + id,
	}, nil
}

// TODO: remove me and broadcast a request to all nodes at the mux. Might 
// need to device a smart solution into how the datasets are looked up
func identifierExists(id string) bool {
	url := config.RemoteBaseURL + ":" + config.RemotePort + "/api/datasets/:persistentId/?persistentId=" + id
	client := http.Client{
		Timeout: time.Duration(30 * time.Second),
	}

	// TODO: preserve context created at the mux
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(5 * time.Second))
	defer cancel()

 	// Create a new request using http
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		log.Println(err.Error())
		return false
	}

	errChan := make(chan error, 0)
	existsChan := make(chan bool)

	go func() {
		resp, err := client.Do(req)
		if err != nil {
			errChan <-err
			return
		}
 
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			errChan <-err
			return
		}

		//log.Println("Resp:", string(body))

		jsonMap := make(map[string](interface{}))
		err = json.Unmarshal(body, &jsonMap)
		if err != nil {
			errChan <-err
			return
		}

		_, ok := jsonMap["message"].(interface{})
		existsChan <-ok
	}()

	select {
	case ok := <-existsChan:
		return !ok // Invert ok because if we found a message the dataset was not found
	case <-ctx.Done():
		log.Println(fmt.Errorf("Checking if dataset exists timeout").Error())
		return false
	case err := <-errChan:
		log.Println(err.Error())
		return false
	}

	return false
}

func remoteDatasetIdentifiers() ([]string, error) {
	url := config.RemoteBaseURL + ":" + config.RemotePort + "/api/search?q=*&type=dataset"
	client := http.Client{
		Timeout: time.Duration(30 * time.Second),
	}

	// TODO: preserve context created at the mux
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(5 * time.Second))
	defer cancel()

 	// Create a new request using http
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	errChan := make(chan error, 0)
	doneChan := make(chan bool)
	identifiers := make([]string, 0)

	defer close(errChan)
	defer close(doneChan)

	go func() {
		resp, err := client.Do(req)
		if err != nil {
			errChan <-err
			return
		}
 
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			errChan <-err
			return
		}

		// Array of dataset identifiers
		jsonMap := make(map[string](interface{}))
		err = json.Unmarshal(body, &jsonMap)
		if err != nil {
			errChan <-err
			return
		}

		data := jsonMap["data"].(map[string]interface{})
		items := data["items"].([]interface{})

		for _, i := range items {
			id := i.(map[string]interface{})["global_id"]
			identifiers = append(identifiers, id.(string))
		}
		doneChan <-true
	}()

	select {
	case <-doneChan:
		break
	case <-ctx.Done():
		return nil, fmt.Errorf("Could not fetch identifiers from remote source")	
	case err := <-errChan:
		return nil, err
	}

	log.Println("Returning identifiers")
	return identifiers, nil
}