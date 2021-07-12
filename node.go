package lohpi

import (
	"net/http"
	"github.com/pkg/errors"
	"github.com/arcsecc/lohpi/core/node"
	"crypto/x509/pkix"
	"github.com/arcsecc/lohpi/core/netutil"
	"github.com/arcsecc/lohpi/core/comm"
	"github.com/arcsecc/lohpi/core/gossipobserver"
	"github.com/arcsecc/lohpi/core/datasetmanager"
	"github.com/arcsecc/lohpi/core/statesync"
	log "github.com/sirupsen/logrus"
	"time"
)

var (
	errNoDatasetId = errors.New("Dataset identifier is empty.")
)

type Node struct {
	nodeCore *node.NodeCore
	conf *node.Config
}

type NodeConfig struct {
	// The address of the CA. Default value is "127.0.0.1:8301"
	CaAddress string

	// The name of this node
	Name string

	// The database connection string. Default value is "". If it is not set, the database connection
	// will not be used. This means that only the in-memory maps will be used for storage.
	SQLConnectionString string

	// Backup retention time. Default value is 0. If it is zero, backup retentions will not be issued. 
	// NOT USED
	BackupRetentionTime time.Time

	// If set to true, a client can checkout a dataset multiple times. Default value is false.
	AllowMultipleCheckouts bool

	// Hostname of the node. Default value is "127.0.1.1".
	HostName string

	// Output directory of gossip observation unit. Default value is the current working directory.
	PolicyObserverWorkingDirectory string
}

// TODO: consider using intefaces
func NewNode(config *NodeConfig) (*Node, error) {
	
	if config == nil {
		return nil, errors.New("Node configuration is nil")
	}
	
	if config.CaAddress == "" {
		config.CaAddress = "127.0.1.1:8301"
	}

	if config.HostName == "" {
		config.HostName = "127.0.1.1"
	}

	if config.PolicyObserverWorkingDirectory == "" {
		config.PolicyObserverWorkingDirectory = "."
	}
	
	n := &Node{
		conf: &node.Config{
			Name: config.Name,
			AllowMultipleCheckouts: config.AllowMultipleCheckouts,
			SQLConnectionString: config.SQLConnectionString,
		},
	}

	listener, err := netutil.GetListener()
	if err != nil {
		return nil, err
	}

	pk := pkix.Name{
		CommonName: n.conf.Name,
		Locality:   []string{listener.Addr().String()},
	}

	// Crypto unit
	cu, err := comm.NewCu(pk, config.CaAddress)
	if err != nil {
		return nil, err
	}

	// Policy observer
	gossipObsConfig := &gossipobserver.PolicyObserverConfig{
		OutputDirectory: config.PolicyObserverWorkingDirectory,
		LogfilePrefix: config.Name,
		Capacity: 10, //config me
	}
	gossipObs, err := gossipobserver.NewGossipObserver(gossipObsConfig)
	if err != nil {
		return nil, err
	}

	// Dataset manager
	datasetManagerConfig := &datasetmanager.DatasetManagerConfig{
		SQLConnectionString: 	config.SQLConnectionString,
		Reload: 				true,
	}
	
	dsManager, err := datasetmanager.NewDatasetManager(datasetManagerConfig)
	if err != nil {
		// Error occurs here
		return nil, err
	}

	stateSync, err := statesync.NewStateSyncUnit()
	if err != nil {
		return nil, err
	}

	nCore, err := node.NewNodeCore(cu, gossipObs, dsManager, stateSync, n.conf)
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
func (n *Node) IndexDataset(datasetId string) error {
	if datasetId == "" {
		return errNoDatasetId
	}

	return n.nodeCore.IndexDataset(datasetId)
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

func (n *Node) Start(directoryServerAddress, policyStoreAddress string) error {
	if err := n.nodeCore.HandshakeDirectoryServer(directoryServerAddress); err != nil {
		return err
	}

	if err := n.nodeCore.HandshakePolicyStore(policyStoreAddress); err != nil {
		return err
	}

	go n.nodeCore.StartDatasetSyncer(time.Second * 5, policyStoreAddress)
	
	return nil
}

func (n *Node) StartHTTPServer(port int) {
	n.nodeCore.StartHTTPServer(port)	
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