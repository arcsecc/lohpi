package lohpi

import (
	
	"context"
	"github.com/pkg/errors"
	"github.com/arcsecc/lohpi/core/node"
)

var (
	errNoId = errors.New("ID must not be empty.")
)

type NodeOption func(*Node)

type Node struct {
	nodeCore *node.NodeCore
	conf *node.Config
}

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

func NodeWithPolicyStoreConnectionString(addr string, port int) NodeOption {
	return func(n *Node) {
		n.conf.PolicyStoreAddress = addr
		n.conf.PolicyStoreGRPCPport = port
	}
}

func NodeWithDirectoryServerConnectionString(addr string, port int) NodeOption {
	return func(n *Node) {
		n.conf.DirectoryServerAddress = addr
		n.conf.DirectoryServerGPRCPort = port
	}
}

func NodeWithName(name string) NodeOption {
	return func(n *Node) {
		n.conf.Name = name
	}
}

func NodeWithPostgresSQLConnectionString(s string) NodeOption {
	return func(n *Node) {
		n.conf.PostgresSQLConnectionString = s
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

// RequestPolicy requests policies from policy store that are assigned to the dataset given by the id.
// It will also populate the node's database with the available identifiers and assoicate them with policies.
// All policies have a default value of nil, which leads to client requests being rejected. You must call this method to
// make the dataset available to the clients by fetching the latest dataset. The call will block until a context timeout or
// the policy is applied to the dataset. Dataset identifiers that have not been passed to this method will not
// be available to clients.
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

// Joins the network by starting the underlying Ifrit node. Further, it performs handshakes
// with the policy store and multiplexer at known addresses.
func (n *Node) JoinNetwork() error {
	return n.nodeCore.JoinNetwork()
}

// Returns the underlying Ifrit address.
func (n *Node) IfritAddress() string {
	return n.nodeCore.IfritAddress()
}

//
func (n *Node) RegisterDatasetHandler(f func(id string) (string, error)) {
	n.nodeCore.SetDatasetHandler(f)
}


func (n *Node) RegsiterMetadataHandler(f func(id string) (string, error)) {
	n.nodeCore.SetMetadataHandler(f)
}

func (n *Node) String() string {
	return ""
}