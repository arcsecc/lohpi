package lohpi

import (
	"errors"
	"github.com/arcsecc/lohpi/core/datasetmanager"
	"github.com/arcsecc/lohpi/core/membershipmanager"
	"github.com/arcsecc/lohpi/core/policystore"
	"github.com/arcsecc/lohpi/core/statesync"
	log "github.com/sirupsen/logrus"
	"time"
)

type PolicyStoreConfig struct {
	// The address of the CA. Default value is "127.0.0.1:8301"
	CaAddress string

	// The name of this node
	Name string

	// The location of the Git repository. Default value is "./policy_store_repository".
	PolicyStoreGitRepository string

	// Hostname of the policy store. Default value is "127.0.1.1".
	Host string

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
}

type PolicyStore struct {
	psCore *policystore.PolicyStoreCore
	config *policystore.Config
}

func NewPolicyStore(config *PolicyStoreConfig) (*PolicyStore, error) {
	if config == nil {
		return nil, errors.New("Policy store configuration is nil")
	}

	if config.Name == "" {
		config.Name = "Lohpi directory server"
	}

	if config.CaAddress == "" {
		config.CaAddress = "127.0.1.1:8301"
	}

	if config.Host == "" {
		config.Host = "127.0.1.1"
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
		log.Warnln("SQL connection string is not set")
	}

	p := &PolicyStore{
		config: &policystore.Config{
			Name:                     config.Name,
			Host:                     config.Host,
			GossipInterval:           config.GossipInterval,
			HTTPPort:                 config.HTTPPort,
			GRPCPort:                 config.GRPCPort,
			MulticastAcceptanceLevel: config.MulticastAcceptanceLevel,
			NumDirectRecipients:      config.NumDirectRecipients,
			CaAddress:                config.CaAddress,
			DirectoryServerAddress:   config.DirectoryServerAddress,
			GitRepositoryPath:        config.PolicyStoreGitRepository,
		},
	}

	// Dataset manager
	datasetLookupServiceConfig := &datasetmanager.DatasetLookupServiceConfig{
		SQLConnectionString: config.SQLConnectionString,
		UseDB:               true,
	}
	datasetLookupService, err := datasetmanager.NewDatasetLookupService(datasetLookupServiceConfig)
	if err != nil {
		return nil, err
	}

	// State syncer
	stateSync, err := statesync.NewStateSyncUnit()
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

	// Dataset manager service
	datasetServiceUnitConfig := &datasetmanager.DatasetServiceUnitConfig{
		SQLConnectionString: config.SQLConnectionString,
		UseDB: true,
	}
	dsManager, err := datasetmanager.NewDatasetServiceUnit(datasetServiceUnitConfig)
	if err != nil {
		return nil, err
	}
	
	psCore, err := policystore.NewPolicyStoreCore(datasetLookupService, stateSync, memManager, dsManager, p.config)
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
