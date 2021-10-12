package lohpi

import (
	"crypto/x509/pkix"
	"errors"
	"github.com/arcsecc/lohpi/core/setsync"
	"github.com/arcsecc/lohpi/core/comm"
	"github.com/arcsecc/lohpi/core/datasetmanager"
	"github.com/arcsecc/lohpi/core/membershipmanager"
	"github.com/arcsecc/lohpi/core/directoryserver"
	"github.com/arcsecc/lohpi/core/policyobserver"
	"time"
	"fmt"
)

type DirectoryServerConfig struct {
	// The address of the CA. Default value is "127.0.0.1:8301"
	CaAddress string

	// The name of this node
	Name string

	// The database connection string. Default value is "". If it is not set, the database connection
	// will not be used. This means that only the in-memory maps will be used for storage.
	SQLConnectionString string

	// Hostname of the node. Default value is "127.0.1.1".
	HostName string

	// Output directory of gossip observation unit. Default value is the current working directory.
	PolicyObserverWorkingDirectory string

	// HTTP port used by the server. Default value is 8080.
	HTTPPort int

	// TCP port used by the gRPC server. Default value is 8081.
	GRPCPort int

	// Directory path used by Lohpi to store X.509 certificate and private key
	LohpiCryptoUnitWorkingDirectory string

	// Directory path used by Ifrit to store X.509 certificate and private key
	IfritCryptoUnitWorkingDirectory string 

	// Ifrit's TCP port. Default value is 5000.
	IfritTCPPort int

	// Ifrit's UDP port. Default value is 6000.
	IfritUDPPort int

	// Interval in seconds between each dataset identifiers synchronization procedure.
	DatasetIdentifiersSyncInterval time.Duration
}

type DirectoryServer struct {
	dsCore *directoryserver.DirectoryServerCore
	conf   *directoryserver.Config
}

// Returns a new DirectoryServer using the given directory server options. Returns a non-nil error, if any.
func NewDirectoryServer(config *DirectoryServerConfig, new bool) (*DirectoryServer, error) {
	if config == nil {
		return nil, errors.New("Directory server configuration is nil")
	}

	if config.Name == "" {
		config.Name = "Lohpi directory server"
	}

	if config.CaAddress == "" {
		config.CaAddress = "127.0.1.1:8301"
	}

	if config.HostName == "" {
		config.HostName = "127.0.1.1"
	}

	if config.HTTPPort == 0 {
		config.HTTPPort = 8080
	}

	if config.GRPCPort == 0 {
		config.GRPCPort = 8081
	}

	if config.LohpiCryptoUnitWorkingDirectory == "" {
		return nil, errors.New("Lohpi crypto working directory is nil")
	}

	if config.IfritTCPPort == 0 {
		config.IfritTCPPort = 5000
	}

	if config.IfritUDPPort == 0 {
		config.IfritUDPPort = 6000
	}

	if config.IfritCryptoUnitWorkingDirectory == "" {
		return nil, errors.New("Ifrit crypto working directory is nil")
	}

	ds := &DirectoryServer{
		conf: &directoryserver.Config{
			Name:                 config.Name,
			HTTPPort:             config.HTTPPort,
			GRPCPort:             config.GRPCPort,
			SQLConnectionString:  config.SQLConnectionString,
			Hostname:			  config.HostName,
			IfritCryptoUnitWorkingDirectory: config.IfritCryptoUnitWorkingDirectory,
			IfritTCPPort: 		 config.IfritTCPPort,
			IfritUDPPort: 		 config.IfritUDPPort,
			DatasetIdentifiersSyncInterval: config.DatasetIdentifiersSyncInterval,
		},
	}

	// Crypto manager
	var cu *comm.CryptoUnit
	var err error

	if new {
		// Create a new crypto unit 
		cryptoUnitConfig := &comm.CryptoUnitConfig{
			Identity: pkix.Name{
				Country: []string{"NO"},
				CommonName: ds.conf.Name,
				Locality: []string{
					fmt.Sprintf("%s:%d", config.HostName, config.HTTPPort), 
					fmt.Sprintf("%s:%d", config.HostName, config.GRPCPort),
				},
			},
			CaAddr: config.CaAddress,
			Hostnames: []string{config.HostName},
		}
		cu, err = comm.NewCu(config.LohpiCryptoUnitWorkingDirectory, cryptoUnitConfig)
		if err != nil {
			return nil, err
		}

		if err := cu.SaveState(); err != nil {
			return nil, err
		}	
	} else {
		cu, err = comm.LoadCu(config.LohpiCryptoUnitWorkingDirectory)
		if err != nil {
			return nil, err
		}
	}

	// Dataset manager
	datasetLookupServiceConfig := &datasetmanager.DatasetLookupServiceConfig{
		SQLConnectionString: config.SQLConnectionString,
	}
	datasetLookupService, err := datasetmanager.NewDatasetLookupService("directory_server", datasetLookupServiceConfig)
	if err != nil {
		return nil, err
	}

	// Membership manager
	memManagerConfig := &membershipmanager.MembershipManagerUnitConfig{
		SQLConnectionString: config.SQLConnectionString,
		UseDB: true,
	}
	memManager, err := membershipmanager.NewMembershipManager("directory_server", memManagerConfig)
	if err != nil {
		return nil, err
	}

	// Checkout manager
	dsCheckoutManagerConfig := &datasetmanager.DatasetCheckoutServiceUnitConfig{
		SQLConnectionString: config.SQLConnectionString,
	}
	dsCheckoutManager, err := datasetmanager.NewDatasetCheckoutServiceUnit("directory_server", dsCheckoutManagerConfig)
	if err != nil {
		return nil, err
	}

	// Policy observer 
	gossipObsConfig := &policyobserver.PolicyObserverUnitConfig{
		SQLConnectionString: config.SQLConnectionString,
	}

	gossipObs, err := policyobserver.NewPolicyObserverUnit("directory_server", gossipObsConfig)
	if err != nil {
		return nil, err
	}

	// Policy synchronization service
	stateSync, err := setsync.NewSetSyncUnit()
	if err != nil {
		return nil, err
	}

	dsCore, err := directoryserver.NewDirectoryServerCore(cu, gossipObs, datasetLookupService, memManager, dsCheckoutManager, stateSync, ds.conf, new)
	if err != nil {
		return nil, err
	}

	ds.dsCore = dsCore

	return ds, nil
}

// Starts the directory server by running the Ifrit server, gRPC server and HTTP server. The call will return when
// these services have been started.
func (d *DirectoryServer) Start() {
	d.dsCore.Start()
}

// Performs a graceful shutdown of the directory server.
func (d *DirectoryServer) Stop() {
	d.dsCore.Stop()
}