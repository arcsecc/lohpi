package datasetmanager

import (
	"database/sql"
	"fmt"
	"github.com/go-redis/redis"
	pb "github.com/arcsecc/lohpi/protobuf"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/golang/protobuf/proto"
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
	errNoId 				= errors.New("Id must be set")
)

type DatasetIndexerUnitConfig struct {
	SQLConnectionString string
	RedisClientOptions *redis.Options
}

type DatasetIndexerUnit struct {
	redisClient *redis.Client
	datasetIndexDB *sql.DB
	config *DatasetIndexerUnitConfig
	datasetStorageSchema string
	datasetStorageTable string
}

func NewDatasetIndexerUnit(id string, config *DatasetIndexerUnitConfig) (*DatasetIndexerUnit, error) {
	if config == nil {
		return nil, errNilConfig
	}

	if config.SQLConnectionString == "" {
		return nil, errNoConnectionString
	}

	if id == "" {
		return nil, errNoId
	}

	d := &DatasetIndexerUnit{
		config: config,			
		datasetStorageSchema: id + "_dataset_storage_schema",
		datasetStorageTable: id + "_dataset_storage_table",
	}
	
	if err := d.createSchema(config.SQLConnectionString); err != nil {
		return nil, err
	}

	if err := d.createDatasetIndexerTable(config.SQLConnectionString); err != nil {
		return nil, err
	}

	if config.RedisClientOptions != nil {
		d.redisClient = redis.NewClient(config.RedisClientOptions)
		pong, err := d.redisClient.Ping().Result()
		if err != nil {
			return nil, err
		}
		
		if pong != "PONG" {
			return nil, fmt.Errorf("Value of Redis pong was wrong")
		}

		if err := d.flushAll(); err != nil {
			return nil, err
		}

		/*errc := d.reloadRedis()
		if err := <-errc; err != nil {
			return nil, err
		}*/
	}

	return d, nil
}

func (d *DatasetIndexerUnit) Dataset(datasetId string) *pb.Dataset {
	if d.redisClient != nil {
		dataset, err := d.cacheDataset(datasetId)
		if dataset != nil && err == nil {
			return dataset
		}

		if err != nil {
			log.Error(err.Error())
		}
	}

	return d.dbSelectDataset(datasetId)
}

func (d *DatasetIndexerUnit) cacheDataset(datasetId string) (*pb.Dataset, error) {
	log.Println("Using cache -- cacheDataset")
	cmd := d.redisClient.MGet(datasetId)
	if err := cmd.Err(); err != nil {
		return nil, err
	}

	datasetBytes, err := cmd.Result()
	if err != nil {
		return nil, err
	}

	if len(datasetBytes) > 0 {
		if datasetBytes[0] != nil {
			dataset := &pb.Dataset{}
			if err := proto.Unmarshal([]byte(datasetBytes[0].(string)), dataset); err != nil {
				return nil, err
			}
			return dataset, nil
		}
	}

	return nil, fmt.Errorf("Dataset '%s' was not found in cache", datasetId)
}

func (d *DatasetIndexerUnit) Datasets() map[string]*pb.Dataset {
	if d.redisClient != nil {
		datasets, err := d.cacheDatasets()
		if datasets != nil && err == nil {
			return datasets
		}

		if err != nil {
			log.Error(err.Error())
		}
	}

	sets, err := d.dbGetAllDatasets()
	if err != nil {
		log.Error(err.Error())
		return nil
	}

	return sets
}

// TODO: add range search
func (d *DatasetIndexerUnit) cacheDatasets() (map[string]*pb.Dataset, error) {
	log.Println("Using cache -- cacheDatasets")

	sets := make(map[string]*pb.Dataset)

	scanCmd := d.redisClient.Scan(0, "", 0)
	if err := scanCmd.Err(); err != nil {
		return nil, err
	}

	iter := scanCmd.Iterator()
	for iter.Next() {
		dataset := &pb.Dataset{}

		if err := proto.Unmarshal([]byte(iter.Val()), dataset); err != nil {
			return nil, err
		}

		sets[dataset.GetIdentifier()] = dataset
	}

	if err := iter.Err(); err != nil {
		return nil, err
	}

	return sets, nil	
}

func (d *DatasetIndexerUnit) DatasetIdentifiers() []string {
	if d.redisClient != nil {
		ids, err := d.cacheDatasetIdentifiers()
		if ids != nil && err == nil {
			return ids
		}

		if err != nil {
			log.Error(err.Error)
		}
	}

	return d.dbSelectDatasetIdentifiers()
}

// TODO: add ranges
func (d *DatasetIndexerUnit) cacheDatasetIdentifiers() ([]string, error) {
	log.Println("Using cache -- cacheDatasetIdentifiers")

	ids := make([]string, 0)
	iter := d.redisClient.Scan(0, "", 0).Iterator()
	for iter.Next() {
		ids = append(ids, iter.Val())
	}
	
	if err := iter.Err(); err != nil {
	    return nil, err
	}
	
	return ids, nil
}

func (d *DatasetIndexerUnit) DatasetExists(datasetId string) bool {
	if d.redisClient != nil {
		exists, err := d.cacheDatasetNodeExists(datasetId)
		if exists && err == nil {
			return exists
		}

		if err != nil {
			log.Error(err.Error())
		}
	}

	// If there was a cache miss but a hit in PSQL, insert it into Redis
	exists := d.dbDatasetExists(datasetId)
	if exists && d.redisClient != nil {
		dataset := d.dbSelectDataset(datasetId)
		if dataset != nil {
			if err := d.InsertDataset(datasetId, dataset); err != nil {
				log.Error(err.Error())
			}
		}
	}

	return exists
}

func (d *DatasetIndexerUnit) cacheDatasetNodeExists(datasetId string) (bool, error) {
	log.Println("Using cache -- cacheDatasetNodeExists")

	cmd := d.redisClient.Exists(datasetId)
	if err := cmd.Err(); err != nil {
		return false, err
	}

	r, err := cmd.Result()
	if err != nil {
		return false, err
	}

	if r == 1 {
		return true, nil
	} else {
		return false, nil
	}
}

func (d *DatasetIndexerUnit) RemoveDataset(datasetId string) error  {
	if d.redisClient != nil {
		if err := d.cacheRemoveDatasetNode(datasetId); err != nil {
			log.Error(err.Error())
		}
	}
	
	return d.dbRemoveDataset(datasetId)
}

func (d *DatasetIndexerUnit) cacheRemoveDatasetNode(datasetId string) error {
	log.Println("Using cache -- cacheRemoveDatasetNode")

	cmd := d.redisClient.Del(datasetId)
	if cmd.Err() != nil {
		return cmd.Err()
	}

	r, err := cmd.Result()
	if err != nil {
		return err
	}

	if r == 1 {
		return nil
	} else {
		return fmt.Errorf("Dataset with identifier '%s' was not found", datasetId)
	}

	return nil
}

func (d *DatasetIndexerUnit) InsertDataset(datasetId string, dataset *pb.Dataset) error {
	if dataset == nil {
		return errNilDataset
	}

	if d.redisClient != nil {
		if err := d.cacheInsertDataset(datasetId, dataset); err != nil {
			log.Error(err.Error())
		}
	}

	return d.dbInsertDataset(datasetId, dataset)
}

func (d *DatasetIndexerUnit) cacheInsertDataset(datasetId string, dataset *pb.Dataset) error {
	log.Println("Using cache -- cacheInsertDataset")

	datasetBytes, err := proto.Marshal(dataset)
	if err != nil {
		return err
	}

	return d.redisClient.Set(datasetId, datasetBytes, 0).Err()
}


func (d *DatasetIndexerUnit) SetDatasetPolicy(datasetId string, policy *pb.Policy) error {
	if policy == nil {
		return errNilPolicy
	}

	if !d.DatasetExists(datasetId) {
		return fmt.Errorf("No such dataset '%s'", datasetId)
	}
	
	return d.dbInsertDatasetPolicy(datasetId, policy)
}

func (d *DatasetIndexerUnit) GetDatasetPolicy(datasetId string) *pb.Policy {
	// use cache here...
	return d.dbSelectDatasetPolicy(datasetId)
}

func (d *DatasetIndexerUnit) flushAll() error {
	cmd := d.redisClient.FlushAll()
	return cmd.Err()
}

func (d *DatasetIndexerUnit) reloadRedis() chan error {
	ch := make(chan error, 1)
	
	go func() {
		ch <- nil
	}()

	return ch
}