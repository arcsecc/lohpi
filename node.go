package lohpi

import (
	"crypto/x509/pkix"
	"github.com/arcsecc/lohpi/core/comm"
	"github.com/arcsecc/lohpi/core/datasetmanager"
	"github.com/arcsecc/lohpi/core/policyobserver"
	"fmt"
	"github.com/arcsecc/lohpi/core/node"
	"github.com/arcsecc/lohpi/core/statesync"
	"github.com/pkg/errors"
	"github.com/go-redis/redis/v8"
_	"github.com/spf13/viper"
	log "github.com/sirupsen/logrus"
	"net/http"
	"time"
)

var (
	errNoDatasetId = errors.New("Dataset identifier is empty.")
	errNoDatasetIndexingOptions = errors.New("Dataset indexing options are nil")
)

type Node struct {
	nodeCore *node.NodeCore
	conf     *node.Config
}

type DatasetIndexingOptions struct {
	AllowMultipleCheckouts bool
}

type NodeConfig struct {
	// The address of the CA. Default value is "127.0.0.1:8301"
	CaAddress string

	// The name of this node
	Name string

	// The database connection string. Default value is "". If it is not set, the database connection
	// will not be used. This means that only the in-memory maps will be used for storage.
	SQLConnectionString string

	// Hostname of the node. Default value is "127.0.1.1".
	Hostname string

	// Output directory of gossip observation unit. Default value is the current working directory.
	PolicyObserverWorkingDirectory string

	// HTTP port number. Default value is 9000
	Port int

	// Interval between which the node will initiate a synchronization of the policies between the policy store
	// and the node itself. Default value is 60 seconds.
	PolicySyncInterval time.Duration 

	// Interval between which the node will initiate a synchronization of datasets the node stores between itself 
	// and the policy store. Default value is 60 seconds.
	DatasetSyncInterval time.Duration 

	// Interval between which the node will initiate a synchronization of the datasets available to the 
	// clients through the directory server. Default value is 60 seconds.
	DatasetIdentifiersSyncInterval time.Duration 

	// Interval between which the node will initiate a synchronization of the checked out datasets'
	// policies between itself and the directory server. Default value is 60 seconds.
	CheckedOutDatasetPolicySyncInterval time.Duration 

	// Path used to store X.509 certificate and private key
	CryptoUnitWorkingDirectory string

	// Options used by Redis.
	RedisClientOptions *redis.Options

	// Ifrit's TCP port. Default value is 5000.
	IfritTCPPort int

	// Ifrit's UDP port. Default value is 6000.
	IfritUDPPort int
}

// TODO: consider using intefaces
func NewNode(config *NodeConfig, createNew bool) (*Node, error) {
	if config == nil {
		return nil, errors.New("Node configuration is nil")
	}

	if config.CaAddress == "" {
		config.CaAddress = "127.0.1.1:8301"
	}

	if config.Hostname == "" {
		config.Hostname = "127.0.1.1"
	}

	if config.PolicyObserverWorkingDirectory == "" {
		config.PolicyObserverWorkingDirectory = "."
	}

	if config.Port == 0 {
		config.Port = 9000
	}

	if config.PolicySyncInterval <= 0 {
		config.PolicySyncInterval = 2 * time.Second
	}
	if config.DatasetSyncInterval <= 0 {
		config.DatasetSyncInterval = 2 * time.Second
	}
	if config.DatasetIdentifiersSyncInterval <= 0 {
		config.DatasetIdentifiersSyncInterval = 2 * time.Second
	}
	if config.CheckedOutDatasetPolicySyncInterval <= 0 {
		config.CheckedOutDatasetPolicySyncInterval = 2 * time.Second
	}

	if config.CryptoUnitWorkingDirectory == "" {
		config.CryptoUnitWorkingDirectory = "./crypto/lohpi"
	}

	n := &Node{
		conf: &node.Config{
			Name:                   config.Name,
			SQLConnectionString:    config.SQLConnectionString,
			Port:                   config.Port,
			PolicySyncInterval:		config.PolicySyncInterval,
			DatasetSyncInterval: 	config.DatasetSyncInterval,
			DatasetIdentifiersSyncInterval: config.DatasetIdentifiersSyncInterval,
			CheckedOutDatasetPolicySyncInterval: config.CheckedOutDatasetPolicySyncInterval,
			Hostname:				config.Hostname,
			IfritTCPPort: 			config.IfritTCPPort,
			IfritUDPPort: 			config.IfritUDPPort,
		},
	}

	// Crypto manager
	var cu *comm.CryptoUnit
	var err error

	if createNew {
		// Create a new crypto unit 
		cryptoUnitConfig := &comm.CryptoUnitConfig{
			Identity: pkix.Name{
				Country: []string{"NO"},
				CommonName: config.Name,
				Locality: []string{
					fmt.Sprintf("%s:%d", config.Hostname, config.Port), 
				},
			},
			CaAddr: config.CaAddress,
			Hostnames: []string{config.Hostname},
		}

		cu, err = comm.NewCu(config.CryptoUnitWorkingDirectory, cryptoUnitConfig)
		if err != nil {
			return nil, err
		}

		if err := cu.SaveState(); err != nil {
			return nil, err
		}	
	} else {
		cu, err = comm.LoadCu(config.CryptoUnitWorkingDirectory)
		if err != nil {
			return nil, err
		}
	}

	// Policy observer 
	gossipObsConfig := &policyobserver.PolicyObserverUnitConfig{
		SQLConnectionString: config.SQLConnectionString,
	}
	gossipObs, err := policyobserver.NewPolicyObserverUnit(config.Name, gossipObsConfig)
	if err != nil {
		return nil, err
	}

	// Dataset manager service
	datasetIndexerUnitConfig := &datasetmanager.DatasetIndexerUnitConfig{
		SQLConnectionString: config.SQLConnectionString,
		RedisClientOptions: config.RedisClientOptions,
	}
	dsManager, err := datasetmanager.NewDatasetIndexerUnit(config.Name, datasetIndexerUnitConfig)
	if err != nil {
		return nil, err
	}

	// Policy synchronization service
	stateSync, err := statesync.NewStateSyncUnit()
	if err != nil {
		return nil, err
	}

	// Checkout manager
	dsCheckoutManagerConfig := &datasetmanager.DatasetCheckoutServiceUnitConfig{
		SQLConnectionString: config.SQLConnectionString,
		// skip redis for now
	}
	dsCheckoutManager, err := datasetmanager.NewDatasetCheckoutServiceUnit(config.Name, dsCheckoutManagerConfig)
	if err != nil {
		return nil, err
	}

	nCore, err := node.NewNodeCore(cu, gossipObs, dsManager, stateSync, dsCheckoutManager, n.conf)
	if err != nil {
		return nil, err
	}

	// Connect the lower-level node to this node
	n.nodeCore = nCore

	return n, nil
}

func (n *Node) StartDatasetSyncing(remoteAddr string) error {
	return nil
}

// IndexDataset registers a dataset, given with its unique identifier. The call is blocking;
// it will return when policy requests to the policy store finish.
func (n *Node) IndexDataset(datasetId string, indexOptions *DatasetIndexingOptions) error {
	if datasetId == "" {
		return errNoDatasetId
	}

	if indexOptions == nil {
		return errNoDatasetIndexingOptions
	}

	opts := &node.DatasetIndexingOptions {
		AllowMultipleCheckouts: indexOptions.AllowMultipleCheckouts,
	}

	return n.nodeCore.IndexDataset(datasetId, opts)
}

// Registers a handler that processes the client request of datasets. The handler is only invoked if the same id
// was registered with 'func (n *Node) IndexDataset()' method. It is the caller's responsibility to
// close the request after use.
func (n *Node) RegisterDatasetHandler(f func(datasetId string, w http.ResponseWriter, r *http.Request)) {
	n.nodeCore.RegisterDatasetHandler(f)
}

// Registers a handler that processes the client request of metadata. The handler is only invoked if the same id
// was registered with 'func (n *Node) IndexDataset()' method. It is the caller's responsibility to
// close the request after use.
func (n *Node) RegisterMetadataHandler(f func(datasetId string, w http.ResponseWriter, r *http.Request)) {
	n.nodeCore.RegisterMetadataHandler(f)
}

// Removes the dataset policy from the node. The dataset will no longer be available to clients.
func (n *Node) RemoveDataset(id string) {
	if id == "" {
		log.Errorln("Dataset identifier must not be empty")
		return
	}

	n.nodeCore.RemoveDataset(id)
}

// Shuts down the node
func (n *Node) Shutdown() {
	n.nodeCore.Shutdown()
}

func (n *Node) HandshakeNetwork(directoryServerAddress, policyStoreAddress string) error {
	if err := n.nodeCore.HandshakeDirectoryServer(directoryServerAddress); err != nil {
		return err
	}

	if err := n.nodeCore.HandshakePolicyStore(policyStoreAddress); err != nil {
		return err
	}

	return nil
}

func (n *Node) Start() {
	n.nodeCore.Start()
}

// Returns the underlying Ifrit address.
func (n *Node) IfritAddress() string {
	return n.nodeCore.IfritAddress()
}

// Returns the string representation of the node.
func (n *Node) String() string {
	return ""
}

// Returns the name of the node.
func (n *Node) Name() string {
	return n.nodeCore.Name()
}
