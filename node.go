package lohpi

import (
	"net/http"
	"github.com/pkg/errors"
	"github.com/arcsecc/lohpi/core/node"
	"time"
)

var (
	errNoDatasetId = errors.New("Dataset identifier is empty.")
)

// Defines functional options for the node.
type NodeOption func(*Node)

type Node struct {
	nodeCore *node.NodeCore
	conf *node.Config
}

type Dataset struct {
	DatasetURL string
	MetadataURL string
}

// Sets the HTTP port of the node that exposes the RESP API. If not set, the port will be chosen at random.
func NodeWithHTTPPort(port int) NodeOption {
	return func(n *Node) {
		n.conf.HTTPPort = port
	}
}

// Sets the CA's address and port number. The default values are "127.0.0.1" and 8301, respectively.
func NodeWithLohpiCaConnectionString(addr string, port int) NodeOption {
	return func(n *Node) {
		n.conf.LohpiCaAddress = addr
	}
}

// Sets the host:port pair of the policy store. Default value is "".
func NodeWithPolicyStoreAddress(addr string) NodeOption {
	return func(n *Node) {
		n.conf.PolicyStoreAddress = addr
	}
}

// Sets the host:port pair of the directory server. Default value is "".
func NodeWithDirectoryServerAddress(addr string) NodeOption {
	return func(n *Node) {
		n.conf.DirectoryServerAddress = addr
	}
}

// Sets the name of the node.
func NodeWithName(name string) NodeOption {
	return func(n *Node) {
		n.conf.Name = name
	}
}

// Sets the connection string to the database that stores the policies.
func NodeWithPostgresSQLConnectionString(s string) NodeOption {
	return func(n *Node) {
		n.conf.PostgresSQLConnectionString = s
	}
}

// Sets the backup retention time to d. At each d, the in-memory caches are flushed
// to the database. If set to 0, flushing never occurs. 
func NodeWithBackupRetentionTime(t time.Duration) NodeOption {
	return func(n *Node) {
		n.conf.DatabaseRetentionInterval = t
	}
}

// If set to true, a client can checkout a dataset multiple times. Default is false.
func NodeWithMultipleCheckouts(multiple bool) NodeOption {
	return func(n *Node) {
		n.conf.AllowMultipleCheckouts = multiple
	}
}

// Sets the hostname of this node. Default value is 127.0.1.1.
func NodeWithHostName(hostName string) NodeOption {
	return func(n *Node) {
		n.conf.HostName = hostName
	}
}

// Sets the working directory of the policy logging. Default value is the current working directory.
func NodeWithPolicyObserverWorkingDirectory(dir string) NodeOption {
	return func(n *Node) {
		n.conf.PolicyObserverWorkingDirectory = dir
	}
}

// Sets the address of the certificate authority. Default value is "127.0.1.1:8301"
func NodeWithLohpiCaAddress(addr string) NodeOption {
	return func(n *Node) {
		n.conf.LohpiCaAddress = addr
	}	
}


// Applies the options to the node.
// NOTE: no locking is performed. Beware of undefined behaviour. Check that previous connections are still valid.
// SHOULD NOT be called.
func (n *Node) ApplyConfigurations(opts ...NodeOption) {
	for _, opt := range opts {
		opt(n)
	}
}

// TODO: consider using intefaces
func NewNode(opts ...NodeOption) (*Node, error) {
	const (
		defaultHTTPPort = -1
		defaultPolicyStoreAddress = "127.0.1.1:8084"
		defaultDirectoryServerAddress = "127.0.1.1:8081"
		defaultLohpiCaAddress = "127.0.1.1:8301"
		defaultName = ""
		defaultPostgresSQLConnectionString = ""
		//defaultDatabaseRetentionInterval = time.Duration(0)	// A LOT MORE TO DO HERE
		defaultAllowMultipleCheckouts = false
		defaultHostName = "127.0.1.1"
		defaultPolicyObserverWorkingDirectory = "."
	)

	// Default configuration
	conf := &node.Config{
		HostName: defaultHostName,
		HTTPPort: defaultHTTPPort,
		PolicyStoreAddress: defaultPolicyStoreAddress,
		DirectoryServerAddress: defaultDirectoryServerAddress,
		LohpiCaAddress: defaultLohpiCaAddress,
		Name: defaultName,
		PostgresSQLConnectionString: defaultPostgresSQLConnectionString,
		//DatabaseRetentionInterval: defaultDatabaseRetentionInterval,
		AllowMultipleCheckouts: defaultAllowMultipleCheckouts,
		PolicyObserverWorkingDirectory: defaultPolicyObserverWorkingDirectory,
	}

	n := &Node{
		conf: conf,
	}

	// Apply the configuration to the higher-level node
	for _, opt := range opts {
		opt(n)
	}

	// Sanitize the configuration. Some fields need to be set before continuing.


	nCore, err := node.NewNodeCore(conf)
	if err != nil {
		return nil, err
	}

	// Connect the lower-level node to this node
	n.nodeCore = nCore

	return n, nil
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
func (n *Node) RemoveDataset(id string) error {
	if id == "" {
		return errNoDatasetId
	}
	
	return n.nodeCore.RemoveDataset(id)
}

// Shuts down the node
func (n *Node) Shutdown() {
	n.nodeCore.Shutdown()
}

// Joins the network by starting the underlying Ifrit node. It must be called before any other function;
// communicating with other nodes in the network will fail. The call performs handshakes with the policy store 
// and directory server at known addresses.
func (n *Node) JoinNetwork() error {
	return n.nodeCore.JoinNetwork()
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