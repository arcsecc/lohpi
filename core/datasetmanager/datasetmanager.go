package datasetmanager

import (
	"fmt"
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
	datasetTable 		 = "dataset_table"
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
	errNoIfritClient = errors.New("Ifrit client is nil")
	errNoPolicyStoreAddress = errors.New("Policy store address is empty")
	errNoDatasetMap = errors.New("Dataset map is nil")
)

// TODO: logging everywhere
// TODO: rename tables by using the node's name

// Configuration struct for the dataset manager
type DatasetManagerConfig struct {
	// The database connection string used to back the in-memory data structures. 
	// If this is left empty, the in-memory data structures will not be backed by persistent storage.
	// TODO: use timeouts to flush the data structures to the db at regular intervals :))
	// TODO: configure SQL connection pools and timeouts. The timeout values must be sane
	SQLConnectionString string

	// Fill the in-memory data structures with the contents in the database. It will only take effect 
	// if DatabaseConnectionString is set.
	Reload bool
}

type DatasetManager struct {	
	datasetMap     map[string]*pb.Dataset
	datasetMapLock sync.RWMutex

	datasetCheckoutMap map[string]*pb.DatasetCheckout
	datasetCheckoutMapLock sync.RWMutex

	// Mirroring of datsetMap
	datasetPolicyDB   *sql.DB
	datasetCheckoutDB *sql.DB

	exitChan chan bool
	//datasetCheckoutDB *sql.DB
	// TODO: retry options if db transactions fail. See exp backoff with jitter 
	// Only applicable in high load environments, but we should take a look into it at some point.
}

type DatasetLookup struct {
	datasetNodesMap map[string]*pb.Node
	datasetNodesMapLock sync.RWMutex
	datasetCheckoutDB *sql.DB
}

// Returns a new DatasetManager, given the configuration
func NewDatasetManager(config *DatasetManagerConfig) (*DatasetManager, error) {
	if config == nil {
		return nil, errNilConfig
	}

	dm := &DatasetManager{
		datasetMap: 		make(map[string]*pb.Dataset),
		datasetCheckoutMap: make(map[string]*pb.DatasetCheckout),
	}

	if config.SQLConnectionString != "" {
		if err := dm.createSchema(config.SQLConnectionString); err != nil {
			return nil, err
		}

		if err := dm.createDatasetTable(config.SQLConnectionString); err != nil {
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

func NewDatasetLookup(config *DatasetManagerConfig) (*DatasetLookup, error) {
	if config == nil {
		return nil, errNilConfig
	}

	dm := &DatasetLookup{
		datasetNodesMap: make(map[string]*pb.Node),
	}

	if config.SQLConnectionString != "" {
		// A LOT MORE TO DO HERE
	}

	return dm, nil
}

func (dl *DatasetLookup) InsertDatasetNode(datasetId string, node *pb.Node) error {
	if node == nil {
		return errors.New("protobuf node is nil")
	}
	dl.datasetNodesMapLock.Lock()
	defer dl.datasetNodesMapLock.Unlock()
	dl.datasetNodesMap[datasetId] = node
	return nil
}

func (dl *DatasetLookup) DatasetNode(datasetId string) *pb.Node {
	dl.datasetNodesMapLock.RLock()
	defer dl.datasetNodesMapLock.RUnlock()
	return dl.datasetNodesMap[datasetId]
}

func (dl *DatasetLookup) DatasetNodes() map[string]*pb.Node {
	dl.datasetNodesMapLock.RLock()
	defer dl.datasetNodesMapLock.RUnlock()
	return dl.datasetNodesMap
}

func (dl *DatasetLookup) CheckoutDataset(datasetId string, checkout *pb.DatasetCheckout) error {
	return fmt.Errorf("Implement CheckoutDataset!")
}

func (dl *DatasetLookup) DatasetIdentifiers() []string {
	ids := make([]string, 0)
	dl.datasetNodesMapLock.RLock()
	defer dl.datasetNodesMapLock.RUnlock()
	for id := range dl.datasetNodesMap {
		ids = append(ids, id)
	}
	return ids
}

func (dl *DatasetLookup) DatasetExists(datasetId string) bool {
	dl.datasetNodesMapLock.RLock()
	defer dl.datasetNodesMapLock.RUnlock()
	_, ok := dl.datasetNodesMap[datasetId]
	return ok	
}

func (dl *DatasetLookup) RemoveDataset(datasetId string) {
	dl.datasetNodesMapLock.Lock()
	defer dl.datasetNodesMapLock.Unlock()
	delete(dl.datasetNodesMap, datasetId)
}



func (dm *DatasetManager) Dataset(datasetId string) *pb.Dataset {
	dm.datasetMapLock.RLock()
	defer dm.datasetMapLock.RUnlock()
	return dm.datasetMap[datasetId]
}

func (dm *DatasetManager) Datasets() map[string]*pb.Dataset {
	dm.datasetMapLock.RLock()
	defer dm.datasetMapLock.RUnlock()
	return dm.datasetMap
}

func (dm *DatasetManager) DatasetIdentifiers() []string {
	ids := make([]string, 0)
	dm.datasetMapLock.RLock()
	defer dm.datasetMapLock.RUnlock()
	for id := range dm.datasetMap {
		ids = append(ids, id)
	}
	return ids
}

func (dm *DatasetManager) DatasetExists(datasetId string) bool {
	dm.datasetMapLock.RLock()
	defer dm.datasetMapLock.RUnlock()
	_, ok := dm.datasetMap[datasetId]
	return ok
}

func (dm *DatasetManager) RemoveDataset(datasetId string) {
	dm.datasetMapLock.Lock()
	defer dm.datasetMapLock.Unlock()
	delete(dm.datasetMap, datasetId)

	if dm.datasetCheckoutDB != nil {
	// TODO: is this an append-only system? Should we completely remove elements or 
	// still keep them?
	//	dm.dbDeleteDatasetPolicy(dataset)
	}
}

func (dm *DatasetManager) DatasetIsAvailable(datasetId string) (bool, error) {
	dm.datasetMapLock.RLock()
	defer dm.datasetMapLock.RUnlock()
	
	dataset, ok := dm.datasetMap[datasetId]
	if !ok {
		return false, errNoPolicyFound
	}

	return dataset.GetPolicy().GetContent(), nil
}

func (dm *DatasetManager) DatasetIsCheckedOut(datasetId string, client *pb.Client) bool {
	dm.datasetCheckoutMapLock.RLock()
	defer dm.datasetCheckoutMapLock.RUnlock()
	_, ok := dm.datasetCheckoutMap[datasetId]
	return ok
}

func (dm *DatasetManager) DatasetPolicy(datasetId string) *pb.Policy {
	dm.datasetMapLock.RLock()
	defer dm.datasetMapLock.RUnlock()
	return dm.datasetMap[datasetId].GetPolicy()
}

func (dm *DatasetManager) SetDatasetPolicy(datasetId string, policy *pb.Policy) error {
	if policy == nil {
		return errNilPolicy
	}
	
	dm.datasetMapLock.Lock()
	defer dm.datasetMapLock.Unlock()
	_, ok := dm.datasetMap[datasetId]
	if !ok {
		return fmt.Errorf("No such dataset '%s'", datasetId)
	}
	newDataset := &pb.Dataset{
		Identifier: datasetId,
		Policy: policy,
	}
	dm.datasetMap[datasetId] = newDataset
	return nil
}

func (dm *DatasetManager) CheckoutDataset(datasetId string, checkout *pb.DatasetCheckout) error {
	if checkout == nil {
		return errNilCheckout
	}

	dm.datasetCheckoutMapLock.Lock()
	defer dm.datasetCheckoutMapLock.Unlock()
	dm.datasetCheckoutMap[datasetId] = checkout

	if dm.datasetCheckoutDB != nil {
		return dm.dbInsertDatasetCheckout(checkout)
	}

	return nil
}

func (dm *DatasetManager) SetDatasetPolicies(datasetMap map[string]*pb.Dataset) error {
	if datasetMap == nil {
		return errNoDatasetMap
	}

	dm.datasetMapLock.Lock()
	defer dm.datasetMapLock.Unlock()
	dm.datasetMap = datasetMap
	return nil
}

func (dm *DatasetManager) DatasetCheckouts(datasetId string) ([]*pb.Dataset, error) {
	return nil, nil
}

func (dm *DatasetManager) InsertDataset(datasetId string, dataset *pb.Dataset) error {
	if dataset == nil {
		return errNilPolicy
	}

	dm.datasetMapLock.Lock()
	defer dm.datasetMapLock.Unlock()
	dm.datasetMap[datasetId] = dataset

	if dm.datasetPolicyDB != nil {
		return dm.dbInsertDatasetIntoTable(dataset)
	}

	return nil
}

// Populates the in-memory maps with the data in the databases
func (dm *DatasetManager) reloadMaps() error {
	log.Printf("Reloading maps...")
	return nil
}

