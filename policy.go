package lohpi

import (
	"errors"
	"fmt"
	"github.com/arcsecc/lohpi/core/datasetmanager"
	"github.com/arcsecc/lohpi/core/policyobserver"
	"github.com/arcsecc/lohpi/core/membershipmanager"
	"github.com/arcsecc/lohpi/core/policystore"
	"github.com/arcsecc/lohpi/core/setsync"
	"github.com/arcsecc/lohpi/core/comm"
	"crypto/x509/pkix"
	//log "github.com/sirupsen/logrus"
	"time"
)

var (
	errNoSQLConnectionString = errors.New("SQL connection is not set")
)

type PolicyStoreConfig struct {
	// The address of the CA. Default value is "127.0.0.1:8301"
	CaAddress string

	// The name of this node
	Name string

	// The location of the Git repository. Default value is "./policy_store_repository".
	PolicyStoreGitRepository string

	// Hostname of the policy store. Default value is "127.0.1.1".
	Hostname string

	// Gossip interval in seconds. Default value is 60 seconds.
	GossipInterval time.Duration

	// HTTP port used by the http server. Default value is 8083
	HTTPPort int

	// TCP port used by the gRPC server. Default value is 8084
	GRPCPort int

	// Mutlicast acceptance level. Default value is 0.5.
	MulticastAcceptanceLevel float64

	// Number of direct recipients. Default value is 1.
	NumDirectRecipients int

	// Directory server address. Default value is "127.0.1.1:8081".
	DirectoryServerAddress string

	// The address of the CA. Default value is "127.0.0.1:8301"
	LohpiCaAddr string

	// The database connection string. Default value is "". If it is not set, the database connection
	// will not be used. This means that only the in-memory maps will be used for storage.
	SQLConnectionString string

	// Directory path used by Lohpi to store X.509 certificate and private key
	LohpiCryptoUnitWorkingDirectory string

	// Directory path used by Ifrit to store X.509 certificate and private key
	IfritCryptoUnitWorkingDirectory string 

	// Ifrit's TCP port. Default value is 5000.
	IfritTCPPort int

	// Ifrit's UDP port. Default value is 6000.
	IfritUDPPort int

	// Ifrit's X.509 certificate path. An error is returned if the string is empty.
	IfritCertPath string
}

type PolicyStore struct {
	psCore *policystore.PolicyStoreCore
	config *policystore.Config
}

func NewPolicyStore(config *PolicyStoreConfig, new bool) (*PolicyStore, error) {
	if config == nil {
		return nil, errors.New("Policy store configuration is nil")
	}

	if config.Name == "" {
		config.Name = "policy_store"
	}

	if config.CaAddress == "" {
		config.CaAddress = "127.0.1.1:8301"
	}

	if config.Hostname == "" {
		config.Hostname = "127.0.1.1"
	}

	if config.HTTPPort <= 0 {
		config.HTTPPort = 8083
	}

	if config.GRPCPort <= 0 {
		config.GRPCPort = 8084
	}

	if config.GossipInterval <= 0 {
		config.GossipInterval = 60 * time.Second
	}

	if config.MulticastAcceptanceLevel <= 0 {
		config.MulticastAcceptanceLevel = 0.5
	}

	if config.NumDirectRecipients <= 0 {
		config.NumDirectRecipients = 1
	}

	if config.DirectoryServerAddress == "" {
		config.DirectoryServerAddress = "127.0.1.1:8081"
	}

	if config.CaAddress == "" {
		config.CaAddress = "127.0.1.1:8301"
	}

	if config.PolicyStoreGitRepository == "" {
		config.PolicyStoreGitRepository = "./policy_store_repository"
	}

	if config.SQLConnectionString == "" {
		return nil, errNoSQLConnectionString
	}

	if config.LohpiCryptoUnitWorkingDirectory == "" {
		return nil, errors.New("Lohpi crypto configuration is nil")
	}

	p := &PolicyStore{
		config: &policystore.Config{
			Name:                     config.Name,
			Hostname:                 config.Hostname,
			GossipInterval:           config.GossipInterval,
			HTTPPort:                 config.HTTPPort,
			GRPCPort:                 config.GRPCPort,
			MulticastAcceptanceLevel: config.MulticastAcceptanceLevel,
			NumDirectRecipients:      config.NumDirectRecipients,
			CaAddress:                config.CaAddress,
			DirectoryServerAddress:   config.DirectoryServerAddress,
			GitRepositoryPath:        config.PolicyStoreGitRepository,
			IfritTCPPort: 		 	  config.IfritTCPPort,
			IfritUDPPort: 		 	  config.IfritUDPPort,
			IfritCryptoUnitWorkingDirectory: config.IfritCryptoUnitWorkingDirectory,
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
				CommonName: config.Name,
				Locality: []string{
					fmt.Sprintf("%s:%d", config.Hostname, config.HTTPPort), 
					fmt.Sprintf("%s:%d", config.Hostname, config.GRPCPort),
				},
			},
			CaAddr: config.CaAddress,
			Hostnames: []string{config.Hostname},
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
	datasetLookupService, err := datasetmanager.NewDatasetLookupService(config.Name, datasetLookupServiceConfig)
	if err != nil {
		return nil, err
	}

	// Membership manager
	memManagerConfig := &membershipmanager.MembershipManagerUnitConfig{
		SQLConnectionString: config.SQLConnectionString,
		UseDB: true,
	}
	memManager, err := membershipmanager.NewMembershipManager(config.Name, memManagerConfig)
	if err != nil {
		return nil, err
	}

	// Dataset manager service
	datasetIndexerUnitConfig := &datasetmanager.DatasetIndexerUnitConfig{
		SQLConnectionString: config.SQLConnectionString,
	}
	dsManager, err := datasetmanager.NewDatasetIndexerUnit(config.Name, datasetIndexerUnitConfig)
	if err != nil {
		return nil, err
	}
	
	// Policy synchronization service
	stateSync, err := setsync.NewSetSyncUnit()
	if err != nil {
		return nil, err
	}

	if err := stateSync.InitializeReconciliationDatabaseConnection(config.SQLConnectionString); err != nil {
		return nil, err
	}

	if err := stateSync.InitializePolicyReconciliationTable(config.Name); err != nil {
		return nil, err
	}

	// Policy observer 
	policyObserverConfig := &policyobserver.PolicyObserverUnitConfig{
		SQLConnectionString: config.SQLConnectionString,
	}
	policyObserver, err := policyobserver.NewPolicyObserverUnit(config.Name, policyObserverConfig)
	if err != nil {
		return nil, err
	}

	psCore, err := policystore.NewPolicyStoreCore(cu, datasetLookupService, stateSync, memManager, dsManager, p.config, policyObserver, new)
	if err != nil {
		return nil, err
	}

	p.psCore = psCore

	return p, nil
}

func (ps *PolicyStore) Start() error {
	return ps.psCore.Start()
}

func (ps *PolicyStore) IfritAddress() string {
	return ps.psCore.IfritAddress()
}

func (ps *PolicyStore) Stop() {
	ps.psCore.Stop()
}

func (ps *PolicyStore) RunPolicyBatcher() {
	ps.psCore.RunPolicyBatcher()
}

func (ps *PolicyStore) StopPolicyBatcher() {
	ps.psCore.StopPolicyBatcher()
}
