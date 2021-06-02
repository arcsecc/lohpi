package main

/** Launcher.go launches one executable of the 'node' package and waits for a SIGTERM signal to arrive
 * from the environment. This should be used when we want to use a process-granularity run.
 */
 
import (
	"context"
	"bufio"
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
	"github.com/arcsecc/lohpi"
	"github.com/arcsecc/lohpi/core/util"
)

// TODO: find a better way to configure stuff :))

var config = struct {
	HTTPPort				int 		`default:"8080"`
	HostName				string 		`required:"true"`
	MultipleCheckouts		bool 		`required:"true"`
	PolicyStoreAddress 		string 		`required:"true"` //`default:"127.0.1.1:8084"`
	DirectoryServerAddress 	string		`required:"true"`//	`default:"127.0.1.1:8081"`
	LohpiCaAddress 	   		string 		`required:"true"` //`default:"127.0.1.1:8301"`
	RemoteBaseURL			string 		`required:"true"`
	RemotePort				string 		`required:"true"`
	AzureKeyVaultName 		string 		`required:"true"`
	AzureKeyVaultSecret		string		`required:"true"`
	AzureClientSecret		string 		`required:"true"`
	AzureClientID			string		`required:"true"`
	AzureKeyVaultBaseURL 	string		`required:"true"`
	AzureTenantID			string		`required:"true"`
}{}

type StorageNode struct {
	node *lohpi.Node
}

func main() {
	var configFile string
	var createNew bool
	var nodeName string
	var logfile string

	runtime.GOMAXPROCS(runtime.NumCPU())

	// Logfile and name flags
	args := flag.NewFlagSet("args", flag.ExitOnError)
	args.StringVar(&nodeName, "name", "", "Human-readable identifier of node.")
	args.StringVar(&configFile, "c", "", `Configuration file for the node.`)
	args.StringVar(&logfile, "log", "", `Output file for the logging.`)
	args.BoolVar(&createNew, "new", false, "Initialize new Lohpi node.")
	args.Parse(os.Args[1:])

	configor.New(&configor.Config{
		Debug: false, 
		ENVPrefix: "PS_NODE",
		ErrorOnUnmatchedKeys: true}).Load(&config, configFile)

	if configFile == "" {
		log.Errorln("Configuration file must not be empty. Exiting.")
		os.Exit(2)
	}

	// Require node identifier
	if nodeName == "" {
		log.Errorln("Missing node identifier. Exiting.")
		os.Exit(2)
	}

	// Set logger
	if logfile != "" {
		file, err := os.OpenFile(logfile, os.O_CREATE|os.O_WRONLY, 0666)
		if err == nil {
			log.SetOutput(file)
		} else {
			log.Info("Failed to log to file, using default stderr")
		}
	}

	var sn *StorageNode
	var err error

	if createNew {
		sn, err = newNodeStorage(nodeName)
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

func newNodeStorage(name string) (*StorageNode, error) {
	opts, err := getNodeConfiguration(name)
	if err != nil {
		return nil, err
	}

	n, err := lohpi.NewNode(opts...)
	if err != nil {
		return nil, err
	}

	sn := &StorageNode {
		node: n,
	}

	// TODO: revise the call stack starting from here
	if err := sn.node.JoinNetwork(); err != nil {
		return nil, err
	}

	return sn, nil
}

func getNodeConfiguration(name string) ([]lohpi.NodeOption, error) {
	dbConn, err := getDatabaseConnectionString()
	if err != nil {
		return nil, err
	}

	opts := []lohpi.NodeOption{
		lohpi.NodeWithHTTPPort(config.HTTPPort),
		lohpi.NodeWithHostName(config.HostName),
		lohpi.NodeWithName(name),
		lohpi.NodeWithPostgresSQLConnectionString(dbConn), 
		lohpi.NodeWithMultipleCheckouts(config.MultipleCheckouts), 
		lohpi.NodeWithPolicyStoreAddress(config.PolicyStoreAddress),
		lohpi.NodeWithDirectoryServerAddress(config.DirectoryServerAddress),
		lohpi.NodeWithLohpiCaAddress(config.LohpiCaAddress),
	}

	log.WithFields(log.Fields{
		"remote-url": config.RemoteBaseURL,
		"hostname": config.HostName,
		"node-name": name,
		"policy-store-address": config.PolicyStoreAddress,
		"directory-server-address": config.DirectoryServerAddress,
		"lohpi-ca-address": config.LohpiCaAddress,
		"multiple-checkouts": config.MultipleCheckouts,
		"http-port": config.HTTPPort,
	}).Infoln("Configuration")

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


func newAzureKeyVaultClient() (*lohpi.AzureKeyVaultClient, error) {
	c := &lohpi.AzureKeyVaultClientConfig{
		AzureKeyVaultClientID:     config.AzureClientID,
		AzureKeyVaultClientSecret: config.AzureClientSecret,
		AzureKeyVaultTenantID:     config.AzureTenantID,
	}

	return lohpi.NewAzureKeyVaultClient(c)
}

func (sn *StorageNode) Start() {
	if err := sn.initializePolicies(); err != nil {
		panic(err)
	}

	sn.node.RegisterDatasetHandler(dataHandler)
	sn.node.RegisterMetadataHandler(metadataHandler)
}

func (sn *StorageNode) initializePolicies() error {
	ids, err := remoteDatasetIdentifiers()
	if err != nil {
		return err
	}

	for _, id := range ids {
		if err := sn.node.IndexDataset(id); err != nil {
			return err
		}
	}

	return nil
}

func metadataHandler(id string, w http.ResponseWriter, r *http.Request) {
	metadataUrl := config.RemoteBaseURL + ":" + config.RemotePort + "/api/datasets/export?exporter=dataverse_json&persistentId=" + id

	request, err := http.NewRequest("GET", metadataUrl, nil)
	if err != nil {
		log.Error(err.Error())
		http.Error(w, http.StatusText(http.StatusBadRequest)+": "+err.Error(), http.StatusBadRequest)
		return
	}

	httpClient := &http.Client{
		Timeout: time.Duration(20 * time.Second),
	}

	response, err := httpClient.Do(request)
	if err != nil {
		log.Error(err.Error())
		http.Error(w, http.StatusText(http.StatusBadRequest)+": "+err.Error(), http.StatusBadRequest)
		return
	}

	defer response.Body.Close()
	
	if response.StatusCode != http.StatusOK {
		log.Errorf("Response from remote data repository\n")
		http.Error(w, http.StatusText(http.StatusInternalServerError) + ": " + "Could not fetch metadata from host.", http.StatusInternalServerError)
		return
	}

	m := util.CopyHeaders(response.Header)
	util.SetHeaders(m, w.Header())
	w.WriteHeader(response.StatusCode)

	reader := bufio.NewReader(response.Body)

	// Stream from response to client
	if err := util.StreamToResponseWriter(reader, w, 100 * 1024); err != nil {
		log.Errorln(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+err.Error(), http.StatusInternalServerError)
		return
	}
}

func dataHandler(id string, w http.ResponseWriter, r *http.Request) {
	datasetUrl := config.RemoteBaseURL + ":" + config.RemotePort + "/api/access/dataset/:persistentId/?persistentId=" + id
	request, err := http.NewRequest("GET", datasetUrl, nil)
	if err != nil {
		log.Error(err.Error())
		http.Error(w, http.StatusText(http.StatusBadRequest)+": "+err.Error(), http.StatusBadRequest)
		return
	}

	httpClient := &http.Client{
		Timeout: time.Duration(20 * time.Second),
	}

	response, err := httpClient.Do(request)
	if err != nil {
		log.Error(err.Error())
		http.Error(w, http.StatusText(http.StatusBadRequest)+": "+err.Error(), http.StatusBadRequest)
		return
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		log.Errorf("Response from remote data repository\n")
		http.Error(w, http.StatusText(http.StatusInternalServerError) + ": " + "Could not fetch dataset from host.", http.StatusInternalServerError)
		return
	}

	m := util.CopyHeaders(response.Header)
	util.SetHeaders(m, w.Header())
	w.WriteHeader(response.StatusCode)

	reader := bufio.NewReader(response.Body)

	// Stream from response to client
	if err := util.StreamToResponseWriter(reader, w, 100 * 1024); err != nil {
		log.Errorln(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError)+": "+err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *StorageNode) Shutdown() {

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