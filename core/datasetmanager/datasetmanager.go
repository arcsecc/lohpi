package datasetmanager

import (
	"database/sql"
	"fmt"
	pb "github.com/arcsecc/lohpi/protobuf"
	"github.com/pkg/errors"
//	log "github.com/sirupsen/logrus"
	"sync"
)

// policy_table attributes
var (
	dataset_id = "dataset_id"
	allowed    = "allowed"
)

var (
	errNilCheckout          = errors.New("Dataset checkout is nil")
	errNilPolicy            = errors.New("Dataset policy is nil")
	errNilConfig            = errors.New("Configuration is nil")
	errNoPolicyFound        = errors.New("No such policy exists")
	errNoIfritClient        = errors.New("Ifrit client is nil")
	errNoPolicyStoreAddress = errors.New("Policy store address is empty")
	errNoDatasetMap         = errors.New("Dataset map is nil")
	errNilDataset 			= errors.New("Dataset is nil")
)

// TODO: logging everywhere
// TODO: rename tables by using the node's name

// Configuration struct for the dataset manager
type DatasetLookupServiceConfig struct {
	// The database connection string used to back the in-memory data structures.
	// If this is left empty, the in-memory data structures will not be backed by persistent storage.
	// TODO: use timeouts to flush the data structures to the db at regular intervals :))
	// TODO: configure SQL connection pools and timeouts. The timeout values must be sane
	SQLConnectionString string

	UseDB bool
}

type DatasetLookupService struct {
	// Dataset id -> protobuf node
	datasetNodeMap map[string]*pb.Node
	datasetNodeMapLock sync.RWMutex

	// Mirroring of datsetMap
	datasetLookupDB *sql.DB

	config *DatasetLookupServiceConfig
}

// Returns a new DatasetIndexerService, given the configuration
func NewDatasetLookupService(config *DatasetLookupServiceConfig) (*DatasetLookupService, error) {
	if config == nil {
		return nil, errNilConfig
	}

	d := &DatasetLookupService{
		datasetNodeMap: make(map[string]*pb.Node),
		config: config,
	}

	if config.UseDB {
		if config.SQLConnectionString == "" {
			return nil, errNoConnectionString
		}

		if err := d.createSchema(config.SQLConnectionString); err != nil {
			return nil, err
		}

		if err := d.createDatasetLookupTable(config.SQLConnectionString); err != nil {
			return nil, err
		}

		if err := d.reloadMaps(); err != nil {
			return nil, err
		}
	}

	return d, nil
}

func (d *DatasetLookupService) DatasetNode(datasetId string) *pb.Node {
	d.datasetNodeMapLock.RLock()
	defer d.datasetNodeMapLock.RUnlock()
	return d.datasetNodeMap[datasetId]
}

func (d *DatasetLookupService) InsertDatasetNode(datasetId string, node *pb.Node) error {
	d.datasetNodeMapLock.Lock()
	defer d.datasetNodeMapLock.Unlock()
	d.datasetNodeMap[datasetId] = node

	if d.datasetLookupDB != nil {
		return d.dbInsertDatasetNode(datasetId, node)
	}

	return nil
}

func (d *DatasetLookupService) DatasetNodeExists(datasetId string) bool {
	d.datasetNodeMapLock.RLock()
	defer d.datasetNodeMapLock.RUnlock()
	_, ok := d.datasetNodeMap[datasetId]
	return ok
}

func (d *DatasetLookupService) RemoveDatasetNode(datasetId string) error {
	d.datasetNodeMapLock.Lock()
	defer d.datasetNodeMapLock.Unlock()
	delete(d.datasetNodeMap, datasetId)

	if d.datasetLookupDB != nil {
		return d.dbRemoveDatasetNode(datasetId)
	}
	return nil
}

func (d *DatasetLookupService) DatasetIdentifiers() []string {
	ids := make([]string, 0)
	d.datasetNodeMapLock.RLock()
	defer d.datasetNodeMapLock.RUnlock()
	for id := range d.datasetNodeMap {
		ids = append(ids, id)
	}
	return ids
}

func (d *DatasetLookupService) DatasetStorageNode(datasetId string) *pb.Node {
	d.datasetNodeMapLock.RLock()
	defer d.datasetNodeMapLock.RUnlock()
	return d.datasetNodeMap[datasetId]
}

func (d *DatasetLookupService) reloadMaps() error {
	maps, err := d.dbGetAllDatasetNodes()
	if err != nil {
		return err
	}

	d.datasetNodeMapLock.Lock()
	defer d.datasetNodeMapLock.Unlock()
	d.datasetNodeMap = maps
	return nil
}

type DatasetServiceUnitConfig struct {
	SQLConnectionString string
	UseDB bool
}

type DatasetServiceUnit struct {
	datasetMap     map[string]*pb.Dataset
	datasetMapLock sync.RWMutex
	datasetIndexDB *sql.DB
}

func NewDatasetServiceUnit(config *DatasetServiceUnitConfig) (*DatasetServiceUnit, error) {
	if config == nil {
		return nil, errNilConfig
	}

	d := &DatasetServiceUnit{
		datasetMap: make(map[string]*pb.Dataset),
	}

	if config.UseDB {
		if config.SQLConnectionString == "" {
			return nil, errNoConnectionString
		}

		if err := d.createSchema(config.SQLConnectionString); err != nil {
			return nil, err
		}

		if err := d.createDatasetIndexerTable(config.SQLConnectionString); err != nil {
			return nil, err
		}

		if err := d.reloadMaps(); err != nil {
			return nil, err
		}
	}

	return d, nil
}

func (d *DatasetServiceUnit) Dataset(datasetId string) *pb.Dataset {
	d.datasetMapLock.RLock()
	defer d.datasetMapLock.RUnlock()
	return d.datasetMap[datasetId]
}

func (d *DatasetServiceUnit) Datasets() map[string]*pb.Dataset {
	d.datasetMapLock.RLock()
	defer d.datasetMapLock.RUnlock()
	return d.datasetMap
}

func (d *DatasetServiceUnit) DatasetIdentifiers() []string {
	ids := make([]string, 0)
	d.datasetMapLock.RLock()
	defer d.datasetMapLock.RUnlock()
	for id := range d.datasetMap {
		ids = append(ids, id)
	}
	return ids
}

func (d *DatasetServiceUnit) DatasetExists(datasetId string) bool {
	d.datasetMapLock.RLock()
	defer d.datasetMapLock.RUnlock()
	_, exists := d.datasetMap[datasetId]
	return exists
}

func (d *DatasetServiceUnit) RemoveDataset(datasetId string) error  {
	d.datasetMapLock.Lock()
	defer d.datasetMapLock.Unlock()
	delete(d.datasetMap, datasetId)

	if d.datasetIndexDB != nil {
		return d.dbRemoveDataset(datasetId)
	}
	
	return nil
}

func (d *DatasetServiceUnit) InsertDataset(datasetId string, dataset *pb.Dataset) error {
	if dataset == nil {
		return errNilDataset
	}
	d.datasetMapLock.Lock()
	defer d.datasetMapLock.Unlock()
	d.datasetMap[datasetId] = dataset

	if d.datasetIndexDB != nil {
		return d.dbInsertDataset(datasetId, dataset)
	}

	return nil
}

func (d *DatasetServiceUnit) SetDatasetPolicy(datasetId string, policy *pb.Policy) error {
	if policy == nil {
		return errNilPolicy
	}

	d.datasetMapLock.Lock()
	defer d.datasetMapLock.Unlock()
	_, ok := d.datasetMap[datasetId]
	if !ok {
		return fmt.Errorf("No such dataset '%s'", datasetId)
	}
	newDataset := &pb.Dataset{
		Identifier: datasetId,
		Policy: policy,
	}
	d.datasetMap[datasetId] = newDataset

	if d.datasetIndexDB != nil {
		return d.dbInsertDatasetPolicy(datasetId, policy)
	}

	return nil
}

func (d *DatasetServiceUnit) GetDatasetPolicy(datasetId string) *pb.Policy {
	d.datasetMapLock.RLock()
	defer d.datasetMapLock.RUnlock()
	return d.datasetMap[datasetId].GetPolicy()
}

func (d *DatasetServiceUnit) reloadMaps() error {
	maps, err := d.dbGetAllDatasets()
	if err != nil {
		return err
	}

	d.datasetMapLock.Lock()
	defer d.datasetMapLock.Unlock()
	d.datasetMap = maps
	return nil
}

type DatasetCheckoutServiceUnitConfig struct {
	SQLConnectionString string
}

type DatasetCheckoutServiceUnit struct {
	datasetCheckoutDB *sql.DB	
}

// We don't use in-memory maps because of the hassle of indexing it
func NewDatasetCheckoutServiceUnit(config *DatasetCheckoutServiceUnitConfig) (*DatasetCheckoutServiceUnit, error) {
	if config == nil {
		return nil, errNilConfig
	}

	d := &DatasetCheckoutServiceUnit{}

	if config.SQLConnectionString == "" {
		return nil, errNoConnectionString
	}
	
	if err := d.createSchema(config.SQLConnectionString); err != nil {
		return nil, err
	}

	if err := d.createTable(config.SQLConnectionString); err != nil {
		return nil, err
	}
	
	return d, nil
}

func (d *DatasetCheckoutServiceUnit) CheckoutDataset(datasetId string, checkout *pb.DatasetCheckout) error {
	if checkout == nil {
		return errors.New("Dataset checkout is nil")
	}
	return d.dbCheckoutDataset(datasetId, checkout)
}

func (d *DatasetCheckoutServiceUnit) DatasetIsCheckedOut(datasetId string, client *pb.Client) (bool, error) {
	if datasetId == "" {
		err := fmt.Errorf("Dataset identifier must not be empty")
		return false, err
	}
	return d.dbDatasetIsCheckedOutByClient(datasetId, client)
}

func (d *DatasetCheckoutServiceUnit) CheckinDataset(datasetId string, client *pb.Client) error {
	return d.dbCheckinDataset(datasetId, client)
}

func (d *DatasetCheckoutServiceUnit) DatasetCheckouts() ([]*pb.DatasetCheckout, error) {
	return d.dbGetAllDatasetCheckouts()
}

// idea: create in-memory cache of clients that have checked out a dataset?