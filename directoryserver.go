package lohpi

import (
	"errors"
	"github.com/arcsecc/lohpi/core/comm"
	"github.com/arcsecc/lohpi/core/datasetmanager"
	"github.com/arcsecc/lohpi/core/directoryserver"
	"github.com/arcsecc/lohpi/core/membershipmanager"
	"github.com/arcsecc/lohpi/core/policyobserver"
	"time"
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
	Hostnames []string

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

	// Database connection pool size
	DBConnPoolSize int
}

type DirectoryServer struct {
	dsCore *directoryserver.DirectoryServerCore
	conf   *directoryserver.Config
}

// Returns a new DirectoryServer using the given directory server options. Returns a non-nil error, if any.
func NewDirectoryServer(config *DirectoryServerConfig, new bool, useCA bool, useACME bool) (*DirectoryServer, error) {
	if config == nil {
		return nil, errors.New("Directory server configuration is nil")
	}

	if config.Name == "" {
		config.Name = "Lohpi directory server"
	}

	if config.CaAddress == "" {
		config.CaAddress = "127.0.1.1:8301"
	}

	if config.Hostnames == nil {
		return nil, errors.New("Hostnames list is nil")
	}

	if len(config.Hostnames) == 0 {
		return nil, errors.New("Hostnames list is empty")
	}

	if config.HTTPPort == 0 {
		config.HTTPPort = 8080
	}

	if config.GRPCPort == 0 {
		config.GRPCPort = 8081
	}

	if config.IfritTCPPort == 0 {
		config.IfritTCPPort = 5000
	}

	if config.IfritUDPPort == 0 {
		config.IfritUDPPort = 6000
	}

	if config.DBConnPoolSize <= 0 {
		return nil, errors.New("Database connection pool size must be greater than zero")
	}

	if config.LohpiCryptoUnitWorkingDirectory == "" {
		return nil, errors.New("Lohpi crypto unit working directory is nil")
	}

	if config.IfritCryptoUnitWorkingDirectory == "" {
		return nil, errors.New("Ifrit crypto unit working directory is nil")
	}

	ds := &DirectoryServer{
		conf: &directoryserver.Config{
			Name: config.Name,
			HTTPPort: config.HTTPPort,
			GRPCPort: config.GRPCPort,
			SQLConnectionString: config.SQLConnectionString,
			Hostname: config.Hostnames[0],
			IfritCryptoUnitWorkingDirectory: config.IfritCryptoUnitWorkingDirectory,
			IfritTCPPort: config.IfritTCPPort,
			IfritUDPPort: config.IfritUDPPort,
		},
	}

	var cu *comm.CryptoUnit
	var err error 

	// NOTE: when using autocert, the certManager interface is basically useless... 
	// please refine the logic behind this
	if !useACME {
		// Create a new crypto unit
		cryptoUnitConfig := &comm.CryptoUnitConfig{
			CaAddr: config.CaAddress,
			Hostnames: config.Hostnames,
		}

		cu, err = comm.NewCu(cryptoUnitConfig, useCA)
		if err != nil {
			return nil, err
		}
	}

	pool, err := dbPool(config.SQLConnectionString, int32(config.DBConnPoolSize))
	if err != nil {
		return nil, err
	}

	// Dataset manager
	datasetLookupService, err := datasetmanager.NewDatasetLookupService(config.Name, pool)
	if err != nil {
		return nil, err
	}

	// Membership manager
	memManager, err := membershipmanager.NewMembershipManager(config.Name, pool)
	if err != nil {
		return nil, err
	}

	// Checkout manager
	dsCheckoutManager, err := datasetmanager.NewDatasetCheckoutServiceUnit(config.Name, pool)
	if err != nil {
		return nil, err
	}

	// Policy observer
	gossipObserver, err := policyobserver.NewPolicyObserverUnit(config.Name, pool)
	if err != nil {
		return nil, err
	}

	dsCore, err := directoryserver.NewDirectoryServerCore(cu, gossipObserver, datasetLookupService, memManager, dsCheckoutManager, ds.conf, pool, new, useACME)
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
