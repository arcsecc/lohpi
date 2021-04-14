package lohpi

import (
	"context"
	"github.com/pkg/errors"
	"github.com/arcsecc/lohpi/core/node"
	"time"
)

var (
	errNoId = errors.New("ID must not be empty.")
)

// Defines functional options for the node.
type NodeOption func(*Node)

type Node struct {
	nodeCore *node.NodeCore
	conf *node.Config
}

// Sets the HTTP port of the node that exposes the HTTP endpoints. Default value is 8090.
func NodeWithHTTPPort(port int) NodeOption {
	return func(n *Node) {
		n.conf.HTTPPort = port
	}
}

// Sets the CA's address and port number. The default values are "127.0.0.1" and 8301, respectively.
func NodeWithLohpiCaConnectionString(addr string, port int) NodeOption {
	return func(n *Node) {
		n.conf.LohpiCaAddress = addr
		n.conf.LohpiCaPort = port
	}
}

// Sets the host:port pair of the policy store. Default value is "".
func NodeWithPolicyStoreConnectionString(addr string, port int) NodeOption {
	return func(n *Node) {
		n.conf.PolicyStoreAddress = addr
		n.conf.PolicyStoreGRPCPport = port
	}
}

// Sets the host:port pair of the directory server. Default value is "".
func NodeWithDirectoryServerConnectionString(addr string, port int) NodeOption {
	return func(n *Node) {
		n.conf.DirectoryServerAddress = addr
		n.conf.DirectoryServerGPRCPort = port
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

// If set to true, verbose logging is enabled. Default is false.
func NodeWithDebugEnabled(enabled bool) NodeOption {
	return func(n *Node) {
		n.conf.DebugEnabled = enabled
	}
}

// If set to true, TLS is enabled on the HTTP connection between the node and the clients connecting to 
// the HTTP server. Default is false.
func NodeWithTLS(enabled bool) NodeOption {
	return func(n *Node) {
		n.conf.TLSEnabled = enabled
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

func NewNode(opts ...NodeOption) (*Node, error) {
	const (
		defaultHTTPPort = 8090
		defaultPolicyStoreAddress = "127.0.1.1"
		defaultPolicyStoreGRPCPport = 8084
		defaultDirectoryServerAddress = "127.0.1.1"
		defaultDirectoryServerGPRCPort = 8081
		defaultLohpiCaAddress = "127.0.1.1"
		defaultLohpiCaPort = 8301
		defaultName = ""
		defaultPostgresSQLConnectionString = ""
		defaultDatabaseRetentionInterval = time.Duration(0)	// A LOT MORE TO DO HERE
	)

	// Default configuration
	conf := &node.Config{
		HTTPPort: defaultHTTPPort,
		PolicyStoreAddress: defaultPolicyStoreAddress,
		PolicyStoreGRPCPport: defaultPolicyStoreGRPCPport,
		DirectoryServerAddress: defaultDirectoryServerAddress,
		DirectoryServerGPRCPort: defaultDirectoryServerGPRCPort,
		LohpiCaAddress: defaultLohpiCaAddress,
		LohpiCaPort: defaultLohpiCaPort,
		Name: defaultName,
		PostgresSQLConnectionString: defaultPostgresSQLConnectionString,
		DatabaseRetentionInterval: defaultDatabaseRetentionInterval,
	}

	n := &Node{
		conf: conf,
	}

	// Apply the configuration to the higher-level node
	for _, opt := range opts {
		opt(n)
	}

	nCore, err := node.NewNodeCore(conf)
	if err != nil {
		return nil, err
	}

	// Connect the lower-level node to this node
	n.nodeCore = nCore

	return n, nil
}

// IndexDataset requests a policy from policy store. The policies are stored in this node and can be retrieved 
// (see func (n *Node) GetPolicy(id string) bool). The policies are eventually available the API when the policy store
// has processsed the request. 
func (n *Node) IndexDataset(id string, ctx context.Context) error {
	if id == "" {
		return errNoId
	}

	return n.nodeCore.IndexDataset(id, ctx)
}

// Removes the dataset policy from the node. The dataset will no longer be available to clients.
func (n *Node) RemoveDataset(id string) error {
	if id == "" {
		return errNoId
	}
	
	return n.nodeCore.RemoveDataset(id)
}

// Shuts down the node
func (n *Node) Shutdown() {
	n.nodeCore.Shutdown()
}

// Joins the network by starting the underlying Ifrit node. It performs handshakes
// with the policy store and multiplexer at known addresses. In addition, the HTTP server will start as well.
func (n *Node) JoinNetwork() error {
	return n.nodeCore.JoinNetwork()
}

// Returns the underlying Ifrit address.
func (n *Node) IfritAddress() string {
	return n.nodeCore.IfritAddress()
}

// Registers the given function to be called whenever a dataset is requested.
// The id must be the same as the argument to 'func (n Node) IndexDataset(id string, ctx context.Context) error'. 
func (n *Node) RegisterDatasetHandler(f func(id string) (string, error)) {
	n.nodeCore.SetDatasetHandler(f)
}

// Registers the given function to be called whenever a dataset's metadata is requested.
// The id must be the same as the argument to 'func (n Node) IndexDataset(id string, ctx context.Context) error'. 
func (n *Node) RegsiterMetadataHandler(f func(id string) (string, error)) {
	n.nodeCore.SetMetadataHandler(f)
}

// Returns the string representation of the node.
func (n *Node) String() string {
	return ""
}