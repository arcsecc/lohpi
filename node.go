package lohpi

import (
	"context"
	"github.com/arcsecc/lohpi/core/comm"
	"github.com/arcsecc/lohpi/core/datasetmanager"
	"github.com/arcsecc/lohpi/core/policyobserver"
	"github.com/arcsecc/lohpi/core/node"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"net/http"
	"time"
)

var (
	errNoDatasetId = errors.New("Dataset identifier is empty.")
	errNoDatasetIndexingOptions = errors.New("Dataset indexing options are nil")
	errNoConnectionString = errors.New("SQL connection is not set")
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

	// HTTP port number. Default value is 9000
	Port int

	// Directory path used by Lohpi to store X.509 certificate and private key
	LohpiCryptoUnitWorkingDirectory string

	// Directory path used by Ifrit to store X.509 certificate and private key
	IfritCryptoUnitWorkingDirectory string 

	// Ifrit's TCP port. Default value is 5000.
	IfritTCPPort int

	// Ifrit's UDP port. Default value is 6000.
	IfritUDPPort int

	// Geographic zone
	GeoZone string

	// Interval between issuing set reconciliation.
	SetReconciliationInterval time.Duration

	// Database connection pool size
	DBConnPoolSize int
}

// TODO: consider using intefaces
func NewNode(config *NodeConfig, createNew bool, useCA bool, useACME bool) (*Node, error) {
	if config == nil {
		return nil, errors.New("Node configuration is nil")
	}

	if config.CaAddress == "" {
		config.CaAddress = "127.0.1.1:8301"
	}

	if config.Hostname == "" {
		config.Hostname = "127.0.1.1"
	}

	if config.Port == 0 {
		config.Port = 9000
	}

	if config.SQLConnectionString == "" {
		return nil, errNoConnectionString
	}

	n := &Node{
		conf: &node.Config{
			Name: config.Name,
			Port: config.Port,
			Hostname: config.Hostname,
			IfritTCPPort: config.IfritTCPPort,
			IfritUDPPort: config.IfritUDPPort,
			IfritCryptoUnitWorkingDirectory: config.IfritCryptoUnitWorkingDirectory,
			GeoZone: config.GeoZone,
			SetReconciliationInterval: config.SetReconciliationInterval,
		},
	}

	// Create a new crypto unit 
	cryptoUnitConfig := &comm.CryptoUnitConfig{
		CaAddr: config.CaAddress,
		Hostnames: []string{config.Hostname},
	}
	cu, err := comm.NewCu(cryptoUnitConfig, useCA)
	if err != nil {
		return nil, err
	}

	pool, err := dbPool(config.SQLConnectionString, int32(config.DBConnPoolSize))
	if err != nil {
		return nil, err
	}

	// Policy observer 
	policyObserver, err := policyobserver.NewPolicyObserverUnit(config.Name, pool)
	if err != nil {
		return nil, err
	}

	// Dataset manager service
	dsManager, err := datasetmanager.NewDatasetIndexerUnit(config.Name, pool)
	if err != nil {
		return nil, err
	}

	// Checkout manager
	dsCheckoutManager, err := datasetmanager.NewDatasetCheckoutServiceUnit(config.Name, pool)
	if err != nil {
		return nil, err
	}

	nCore, err := node.NewNodeCore(cu, policyObserver, dsManager, dsCheckoutManager, n.conf, pool, createNew)
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
func (n *Node) IndexDataset(ctx context.Context, datasetId string, indexOptions *DatasetIndexingOptions) error {
	if datasetId == "" {
		return errNoDatasetId
	}

	if indexOptions == nil {
		return errNoDatasetIndexingOptions
	}

	opts := &node.DatasetIndexingOptions {
		AllowMultipleCheckouts: indexOptions.AllowMultipleCheckouts,
	}

	return n.nodeCore.IndexDataset(ctx, datasetId, opts)
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
func (n *Node) Stop() {

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
