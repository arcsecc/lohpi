package datasetmanager

import (
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type DirectoryServerSuite struct {
	suite.Suite
}

func init() {

}

// Run before each test
func (suite *DirectoryServerSuite) SetupTest() {
	// Create the checkout service
	dsCheckoutManagerConfig := &datasetmanager.DatasetCheckoutServiceUnitConfig{
		SQLConnectionString: config.SQLConnectionString,
	}
	
	dsCheckoutManager, err := datasetmanager.NewDatasetCheckoutServiceUnit("directory_server", dsCheckoutManagerConfig)
	if err != nil {
		return nil, err
	}
}