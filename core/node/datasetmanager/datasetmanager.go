package datasetmanager

import (
	pb "github.com/arcsecc/lohpi/protobuf"
	"sync"
	"database/sql"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// Note: the in-memory maps are the single source of truth, as seen by the consumer of
// this API. If the element does not exist in the map, it does not exist in the database. 
// Also, if an element exist in the map, it also exists in the database.

// Database-related consts
var (
	schemaName           = "nodedbschema"
	datasetPolicyTable   = "policy_table"
	datasetCheckoutTable = "dataset_checkout_table"
)

// policy_table attributes
var (
	dataset_id = "dataset_id"
	allowed = "allowed"
)

var (
	errNilCheckout = errors.New("Dataset checkout is nil")
	errNilPolicy = errors.New("Dataset policy is nil")
	errNilConfig = errors.New("Configuration is nil")
	errNoPolicyFound = errors.New("No such policy exists")
)

// TODO: logging everywhere

// Configuration struct for the dataset manager
type DatasetManagerConfig struct {
	// The database connection string used to back the in-memory data structures. 
	// If this is left empty, the in-memory data structures will not be backed by persistent storage.
	// TODO: use timeouts to flush the data structures to the db at regular intervals :))
	SQLConnectionString string

	// Fill the in-memory data structures with the contents in the database. It will only take effect 
	// if DatabaseConnectionString is set.
	Reload bool
}

type DatasetManager struct {
	datasetPolicyMap     map[string]*pb.Policy
	datasetPolicyMapLock sync.RWMutex

	datasetCheckoutMap map[string]*pb.DatasetCheckout
	datasetCheckoutMapLock sync.RWMutex

	// Mirroring of datsetMap
	datasetPolicyDB   *sql.DB
	datasetCheckoutDB *sql.DB

	//datasetCheckoutDB *sql.DB
	// TODO: retry options if db transactions fail. See exp backoff with jitter 
	// Only applicable in high load environments, but we should take a look into it at some point.
}

// Returns a new DatasetManager, given the configuration
func NewDatasetManager(config *DatasetManagerConfig) (*DatasetManager, error) {
	if config == nil {
		return nil, errNilConfig
	}

	dm := &DatasetManager{
		datasetPolicyMap: make(map[string]*pb.Policy),
		datasetCheckoutMap: make(map[string]*pb.DatasetCheckout),
	}

	if config.SQLConnectionString != "" {
		if err := dm.createSchema(config.SQLConnectionString); err != nil {
			return nil, err
		}

		if err := dm.createDatasetPolicyTable(config.SQLConnectionString); err != nil {
			return nil, err
		}

		if err := dm.createDatasetCheckoutTable(config.SQLConnectionString); err != nil {
			return nil, err
		}

		if config.Reload {
			if err := dm.reloadMaps(); err != nil {
				return nil, err
			}
		}
	} else {
		log.Debugln("Dataset manager is not using database")
	}

	return dm, nil
}

func (dm *DatasetManager) DatasetIdentifiers() []string {
	ids := make([]string, 0)
	dm.datasetPolicyMapLock.RLock()
	defer dm.datasetPolicyMapLock.RUnlock()
	for id := range dm.datasetPolicyMap {
		ids = append(ids, id)
	}
	return ids
}

func (dm *DatasetManager) DatasetPolicyExists(dataset string) bool {
	dm.datasetPolicyMapLock.RLock()
	defer dm.datasetPolicyMapLock.RUnlock()
	_, ok := dm.datasetPolicyMap[dataset]
	return ok
}

func (dm *DatasetManager) RemoveDatasetPolicy(dataset string) {
	dm.datasetPolicyMapLock.Lock()
	defer dm.datasetPolicyMapLock.Unlock()
	delete(dm.datasetPolicyMap, dataset)

	if dm.datasetCheckoutDB != nil {
	// TODO: is this an append-only system? Should we completely remove elements or 
	// still keep them?
	//	dm.dbDeleteDatasetPolicy(dataset)
	}
}

func (dm *DatasetManager) DatasetIsAvailable(dataset string) (bool, error) {
	dm.datasetPolicyMapLock.RLock()
	defer dm.datasetPolicyMapLock.RUnlock()
	
	policy, ok := dm.datasetPolicyMap[dataset]
	if !ok {
		return false, errNoPolicyFound
	}

	return policy.GetContent(), nil
}

func (dm *DatasetManager) DatasetIsCheckedOut(dataset string, client *pb.Client) bool {
	dm.datasetCheckoutMapLock.RLock()
	defer dm.datasetCheckoutMapLock.RUnlock()
	_, ok := dm.datasetCheckoutMap[dataset]
	return ok
}

func (dm *DatasetManager) DatasetPolicy(dataset string) *pb.Policy {
	dm.datasetPolicyMapLock.RLock()
	defer dm.datasetPolicyMapLock.RUnlock()
	return dm.datasetPolicyMap[dataset]
}

func (dm *DatasetManager) CheckoutDataset(dataset string, checkout *pb.DatasetCheckout) error {
	if checkout == nil {
		return errNilCheckout
	}

	dm.datasetCheckoutMapLock.Lock()
	defer dm.datasetCheckoutMapLock.Unlock()
	dm.datasetCheckoutMap[dataset] = checkout

	if dm.datasetCheckoutDB != nil {
		return dm.dbInsertDatasetCheckout(checkout)
	}

	return nil
}

func (dm *DatasetManager) DatasetCheckouts(dataset string) ([]*pb.Dataset, error) {
	return nil, nil
}

func (dm *DatasetManager) InsertDatasetPolicy(dataset string, policy *pb.Policy) error {
	if policy == nil {
		return errNilPolicy
	}

	dm.datasetPolicyMapLock.Lock()
	defer dm.datasetPolicyMapLock.Unlock()
	dm.datasetPolicyMap[dataset] = policy

	if dm.datasetPolicyDB != nil {
		return dm.dbInsertPolicyIntoTable(policy)
	}

	return nil
}

// Populates the in-memory maps with the data in the databases
func (dm *DatasetManager) reloadMaps() error {
	return nil
}

