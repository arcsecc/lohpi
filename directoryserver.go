package lohpi

import (
	"errors"
	"github.com/arcsecc/lohpi/core/directoryserver"
	"time"
	"github.com/arcsecc/lohpi/core/datasetmanager"
	"github.com/arcsecc/lohpi/core/membershipmanager"
	"github.com/arcsecc/lohpi/core/netutil"
	"github.com/arcsecc/lohpi/core/comm"
	"crypto/x509/pkix"
)

type DirectoryServerOption func(*DirectoryServer)

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

	CertificateFile string
	PrivateKey 	string
}

type DirectoryServer struct {
	dsCore *directoryserver.DirectoryServerCore
	conf *directoryserver.Config
}

// Returns a new DirectoryServer using the given directory server options. Returns a non-nil error, if any. 
func NewDirectoryServer(config *DirectoryServerConfig) (*DirectoryServer, error) {
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

	ds := &DirectoryServer{
		conf: &directoryserver.Config{
			Name: config.Name,
			HTTPPort: config.HTTPPort,
			GRPCPort: config.GRPCPort,
			CaAddress: config.CaAddress,
			SQLConnectionString: config.SQLConnectionString,
		},
	}

	listener, err := netutil.GetListener()
	if err != nil {
		return nil, err
	}

	pk := pkix.Name{
		CommonName: ds.conf.Name,
		Locality:   []string{listener.Addr().String()},
	}

	// Crypto unit
	cu, err := comm.NewCu(pk, config.CaAddress)
	if err != nil {
		return nil, err
	}

	// Dataset manager
	datasetManagerConfig := &datasetmanager.DatasetManagerConfig{
		SQLConnectionString: 	config.SQLConnectionString,
		Reload: 				true,
	}
	dsManager, err := datasetmanager.NewDatasetLookup(datasetManagerConfig)
	if err != nil {
		return nil, err
	}

	memManager, err := membershipmanager.NewMembershipManager()
	if err != nil {
		return nil, err
	}

	dsCore, err := directoryserver.NewDirectoryServerCore(cu, dsManager, memManager, ds.conf)
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
