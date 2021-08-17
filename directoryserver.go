package lohpi

import (
	"crypto/x509/pkix"
	"errors"
	"github.com/arcsecc/lohpi/core/comm"
	"github.com/arcsecc/lohpi/core/datasetmanager"
	"github.com/arcsecc/lohpi/core/membershipmanager"
	"github.com/arcsecc/lohpi/core/directoryserver"
	"github.com/go-redis/redis"

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

	// Backup retention time. Default value is 0. If it is zero, backup retentions will not be issued.
	// NOT USED
	BackupRetentionTime time.Time

	// Hostname of the node. Default value is "127.0.1.1".
	HostName string

	// Output directory of gossip observation unit. Default value is the current working directory.
	PolicyObserverWorkingDirectory string

	// HTTP port used by the server. Default value is 8080.
	HTTPPort int

	// TCP port used by the gRPC server. Default value is 8081.
	GRPCPort int

	// Path used to store X.509 certificate and private key
	CryptoUnitWorkingDirectory string

	// Ifrit's TCP port. Default value is 5000.
	IfritTCPPort int

	// Ifrit's UDP port. Default value is 6000.
	IfritUDPPort int

	// Ifrit's X.509 certificate path. An error is returned if the string is empty.
	IfritCertPath string
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

	if config.PolicyObserverWorkingDirectory == "" {
		config.PolicyObserverWorkingDirectory = "."
	}

	if config.HTTPPort == 0 {
		config.HTTPPort = 8080
	}

	if config.GRPCPort == 0 {
		config.GRPCPort = 8081
	}

	if config.CryptoUnitWorkingDirectory == "" {
		config.CryptoUnitWorkingDirectory = "./secrets"
	}

	if config.IfritTCPPort == 0 {
		config.IfritTCPPort = 5000
	}

	if config.IfritUDPPort == 0 {
		config.IfritUDPPort = 6000
	}

	if config.IfritCertPath == "" {
		return nil, fmt.Errorf("Certificate path is empty")
	}

	ds := &DirectoryServer{
		conf: &directoryserver.Config{
			Name:                config.Name,
			HTTPPort:            config.HTTPPort,
			GRPCPort:            config.GRPCPort,
			CaAddress:           config.CaAddress,
			SQLConnectionString: config.SQLConnectionString,
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

	// Dataset manager
	datasetLookupServiceConfig := &datasetmanager.DatasetLookupServiceConfig{
		SQLConnectionString: config.SQLConnectionString,
		RedisClientOptions: &redis.Options{
			Network: "tcp",
			Addr: fmt.Sprintf("%s:%d", "127.0.0.1", 6302),
			Password: "",
			DB: 0,
		},
	}
	datasetLookupService, err := datasetmanager.NewDatasetLookupService("directoryserver", datasetLookupServiceConfig)
	if err != nil {
		return nil, err
	}

	// Membership manager
	memManagerConfig := &membershipmanager.MembershipManagerUnitConfig{
		SQLConnectionString: config.SQLConnectionString,
		UseDB: true,
	}
	memManager, err := membershipmanager.NewMembershipManager(memManagerConfig)
	if err != nil {
		return nil, err
	}

	// Checkout manager
	dsCheckoutManagerConfig := &datasetmanager.DatasetCheckoutServiceUnitConfig{
		SQLConnectionString: config.SQLConnectionString,
	}
	dsCheckoutManager, err := datasetmanager.NewDatasetCheckoutServiceUnit("directoryserver", dsCheckoutManagerConfig)
	if err != nil {
		return nil, err
	}

	dsCore, err := directoryserver.NewDirectoryServerCore(cu, datasetLookupService, memManager, dsCheckoutManager, ds.conf)
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