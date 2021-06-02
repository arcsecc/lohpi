package directoryserver

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/arcsecc/lohpi/core/node"
	log "github.com/sirupsen/logrus"
	"testing"

//	pb "github.com/arcsecc/lohpi/protobuf"
	//"github.com/stretchr/testify/assert"
)

type DirectoryServerSuite struct {
	suite.Suite

	// Actual directory server
	ds *DirectoryServerCore

	nodes []*node.NodeCore
}

func (suite *DirectoryServerSuite) SetupTest() {
	// Test with nil config
	ds, err := NewDirectoryServerCore(nil)
	require.Error(suite.T(), err)
	require.Empty(suite.T(), ds)

	// Generate configuration object
	c := &Config{
		HTTPPort:       8080,
		GRPCPort:       8081,
		LohpiCaAddress: "127.0.1.1",
		LohpiCaPort:    8301,
	}

	ds, err = NewDirectoryServerCore(c)
	require.NoError(suite.T(), err, "Failed to create new directory server")

	suite.ds = ds
	suite.ds.Start()

	// Dummy nodes, in terms of protobuf definitions
	nodes, err := nodes()
	if err != nil {
		log.Fatal(err)
	}

	suite.nodes = nodes
}

// Returns a set of dummy nodes used to test the directory server. Assume the 
// bootstrap parameters to be correct.
func nodes() ([]*node.NodeCore, error) {
	nodes := make([]*node.NodeCore, 0)
	
	nodeConfigs := []*node.Config{
		&node.Config {
			HostName: "127.0.1.1",
			HTTPPort: 8020,
			PolicyStoreAddress: "127.0.1.1:8081",
			LohpiCaAddress: "127.0.1.1:8301",
			Name: "First node",
			AllowMultipleCheckouts: true,
			PolicyObserverWorkingDirectory: ".", 
		},
		&node.Config{
			HostName: "127.0.1.1",
			HTTPPort: 8022,
			PolicyStoreAddress: "127.0.1.1:8081",
			LohpiCaAddress: "127.0.1.1:8301",
			Name: "Second node",
			AllowMultipleCheckouts: true,
			PolicyObserverWorkingDirectory: ".", 
		},
	}

	for _, c := range nodeConfigs {
		n, err := node.NewNodeCore(c)
		if err != nil {
			return nil, err
		}

		nodes = append(nodes, n)
	}

	return nodes, nil
}

func (suite *DirectoryServerSuite) TestDirectoryServerHandshake() {
	// Add nil node
	resp, err := suite.ds.Handshake(context.Background(), nil)
	require.Empty(suite.T(), resp)
	require.Error(suite.T(), err)

	// Add random nodes and check the response
	for _, n := range suite.nodes {
		resp, err := suite.ds.Handshake(context.Background(), n.PbNode())
		require.NoError(suite.T(), err)
		require.NotEqual(suite.T(), resp.Ip, nil)
		require.NotEqual(suite.T(), resp.Id, nil)
	}

	// Add same node to trigger duplicate error
	resp, err = suite.ds.Handshake(context.Background(), suite.nodes[0].PbNode())
	require.Empty(suite.T(), resp)
	require.Error(suite.T(), err)
}

func TestDirectoryServerSuite(t *testing.T) {
	suite.Run(t, new(DirectoryServerSuite))
}

func (suite *DirectoryServerSuite) TestStart() {
	suite.ds.Start()
}

func (suite *DirectoryServerSuite) TestShutdownDirectoryServer() {
	//suite.ifritCa.Shutdown()
	//suite.lohpiCa.Shutdown()
}
