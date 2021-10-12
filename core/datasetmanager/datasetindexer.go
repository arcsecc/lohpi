package datasetmanager

import (
	"sync"
	"fmt"
	"strconv"
	"github.com/go-redis/redis/v8"
	pb "github.com/arcsecc/lohpi/protobuf"
	"github.com/jackc/pgx/v4/pgxpool"
	"time"
	"context"
	log "github.com/sirupsen/logrus"
	"github.com/golang/protobuf/proto"
)

var indexerLogFields = log.Fields{
	"package": "datasetmanager",
	"description": "dataset indexing",
}

type DatasetIndexerUnitConfig struct {
	SQLConnectionString string
	RedisClientOptions *redis.Options
}

type DatasetIndexerUnit struct {
	redisClient *redis.Client
	config *DatasetIndexerUnitConfig
	datasetStorageSchema string
	datasetStorageTable string
	datasetLookupTable string
	pool *pgxpool.Pool
}

func init() { 
	log.SetReportCaller(true)
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

	// Database connection pool
	poolConfig, err := pgxpool.ParseConfig(config.SQLConnectionString)
	if err != nil {
		log.WithFields(indexerLogFields).Error(err.Error())
		return nil, err
	}

	poolConfig.MaxConnLifetime = time.Second * 10
	poolConfig.MaxConnIdleTime = time.Second * 4
	poolConfig.MaxConns = 100
	poolConfig.HealthCheckPeriod = time.Second * 1
	poolConfig.LazyConnect = false

	pool, err := pgxpool.ConnectConfig(context.Background(), poolConfig)
	if err != nil {
		log.WithFields(indexerLogFields).Error(err.Error())
		return nil, err
	}

	d := &DatasetIndexerUnit{
		pool: pool,
		config: config,			
		datasetStorageSchema: id + "_schema",
		datasetStorageTable: id + "_policy_table",
		datasetLookupTable: id + "_dataset_lookup_table",
	}
	
	if config.RedisClientOptions != nil {
		d.redisClient = redis.NewClient(config.RedisClientOptions)
		pong, err := d.redisClient.Ping(context.Background()).Result()
		if err != nil {
			log.WithFields(indexerLogFields).Error(err.Error())
			return nil, err
		}
		
		if pong != "PONG" {
			return nil, ErrNoRedisPong
		}

		if err := d.flushAll(); err != nil {
			log.WithFields(indexerLogFields).Error(err.Error())
			return nil, err
		}

		go func() {
			errc := d.reloadRedis()
			if err := <-errc; err != nil {
				log.WithFields(indexerLogFields).Error(err.Error())
				return
			}
		}()
	}

	return d, nil
}

func (d *DatasetIndexerUnit) Dataset(datasetId string) *pb.Dataset {
	if d.redisClient != nil {
		/*dataset, err := d.cacheDataset(datasetId)
		if dataset != nil && err == nil {
			return dataset
		}

		if err != nil {
			log.Error(err.Error())
		}*/
	}

	dataset, err := d.dbSelectDataset(datasetId)
	if err != nil {
		log.WithFields(indexerLogFields).Error(err.Error())
		return nil
	}

	return dataset
}

func (d *DatasetIndexerUnit) cacheDataset(datasetId string) (*pb.Dataset, error) {
	log.WithFields(indexerLogFields).
		WithField("storage", "redis").
		Infof("Fetching dataset with key %s\n", datasetId)

	cmd := d.redisClient.MGet(context.Background(), datasetId)
	if err := cmd.Err(); err != nil {
		log.WithFields(indexerLogFields).Error(err.Error())
		return nil, err
	}

	datasetBytes, err := cmd.Result()
	if err != nil {
		log.WithFields(indexerLogFields).Error(err.Error())
		return nil, err
	}

	if len(datasetBytes) > 0 {
		if datasetBytes[0] != nil {
			dataset := &pb.Dataset{}
			if err := proto.Unmarshal([]byte(datasetBytes[0].(string)), dataset); err != nil {
				log.WithFields(indexerLogFields).Error(err.Error())
				return nil, err
			}
			return dataset, nil
		}
	}

	return nil, fmt.Errorf("Dataset '%s' was not found in cache", datasetId)
}

// TODO: add ranges
func (d *DatasetIndexerUnit) Datasets() map[string]*pb.Dataset {
	if d.redisClient != nil {
		datasets, err := d.cacheDatasets()
		if datasets != nil && err == nil {
			return datasets
		}

		if err != nil {
			log.WithFields(indexerLogFields).Error(err.Error())
			return nil
		}
	}

	sets := make(map[string]*pb.Dataset)
	dsChan, errChan, doneChan := d.dbSelectAllDatasets()
	
	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			select {
			case dataset, ok := <-dsChan:
				if ok {
					sets[dataset.GetIdentifier()] = dataset
				}
			case <-doneChan:
				return
			case err, ok := <-errChan:
				if ok {
					log.WithFields(indexerLogFields).Error(err.Error())
				}
			}
		}
	}()

	wg.Wait()

	return sets
}

// TODO: add range search
func (d *DatasetIndexerUnit) cacheDatasets() (map[string]*pb.Dataset, error) {
	log.WithFields(indexerLogFields).
		WithField("storage", "redis").
		Infoln("Fetching dataset map containing all datasets")
		
	sets := make(map[string]*pb.Dataset)

	scanCmd := d.redisClient.Scan(context.Background(), 0, "", 0)
	if err := scanCmd.Err(); err != nil {
		log.WithFields(indexerLogFields).Error(err.Error())
		return nil, err
	}

	iter := scanCmd.Iterator()
	for iter.Next(context.Background()) {
		dataset := &pb.Dataset{}

		if err := proto.Unmarshal([]byte(iter.Val()), dataset); err != nil {
			log.WithFields(indexerLogFields).Error(err.Error())
			return nil, err
		}

		sets[dataset.GetIdentifier()] = dataset
	}

	if err := iter.Err(); err != nil {
		log.WithFields(indexerLogFields).Error(err.Error())
		return nil, err
	}

	return sets, nil	
}

func (d *DatasetIndexerUnit) DatasetIdentifiers(fromIdx int64, length int64) []string {
	/*if d.redisClient != nil {
		ids, err := d.cacheDatasetIdentifiers(fromIdx, length)
		if ids != nil && err == nil {
			return ids
		}

		if err != nil {
			log.Error(err.Error)
		}
	}*/

	ids, err := d.dbSelectDatasetIdentifiers()
	if err != nil {
		log.WithFields(indexerLogFields).Error(err.Error())
		return nil
	}
	return ids
}

// TODO: add ranges
func (d *DatasetIndexerUnit) cacheDatasetIdentifiers(fromIdx int64, length int64) ([]string, error) {
	log.WithFields(indexerLogFields).
		WithField("storage", "redis").
		Infof("Fetching dataset identifiers in the range %d:%d\n", fromIdx, length)

	sets, err := d.redisClient.LRange(context.Background(), datasetList, fromIdx, length).Result()
	if err != nil {
		log.WithFields(indexerLogFields).Error(err.Error())
		return nil, err
	}

	keys := make([]string, 0)
	for _, s := range sets {
		dataset := &pb.Dataset{}
		if err := proto.Unmarshal([]byte(s), dataset); err != nil {
			log.WithFields(indexerLogFields).Error(err.Error())
			continue
		}
		keys = append(keys, dataset.GetIdentifier())
	}

	return keys, nil
}

func (d *DatasetIndexerUnit) DatasetExists(datasetId string) bool {
	if d.redisClient != nil {
		exists, err := d.cacheDatasetNodeExists(datasetId)
		if exists && err == nil {
			return exists
		}

		if err != nil {
			log.WithFields(indexerLogFields).Error(err.Error())
		}
	}

	// If there was a cache miss but a hit in PSQL, insert it into Redis
	exists, err := d.dbDatasetExists(datasetId)
	if err != nil {
		log.WithFields(indexerLogFields).Error(err.Error())
		return false
	}

	if exists && d.redisClient != nil {
		dataset, err := d.dbSelectDataset(datasetId)
		if err != nil {
			log.WithFields(indexerLogFields).Error(err.Error())
			return false
		}

		if dataset != nil {
			if err := d.InsertDataset(datasetId, dataset); err != nil {
				log.WithFields(indexerLogFields).Error(err.Error())
			}
		}
	}

	return exists
}

func (d *DatasetIndexerUnit) cacheDatasetNodeExists(datasetId string) (bool, error) {
	log.WithFields(indexerLogFields).
		WithField("storage", "redis").
		Infof("Checking if dataset with id %s exists\n", datasetId)

	cmd := d.redisClient.Exists(context.Background(), datasetId)
	if err := cmd.Err(); err != nil {
		log.WithFields(indexerLogFields).Error(err.Error())
		return false, err
	}

	r, err := cmd.Result()
	if err != nil {
		log.WithFields(indexerLogFields).Error(err.Error())
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
			log.WithFields(indexerLogFields).Error(err.Error())
		}
	}
	
	if err := d.dbRemoveDataset(datasetId); err != nil {
		log.WithFields(indexerLogFields).Error(err.Error())
		return ErrRemoveDataset
	}
	
	return nil
}

func (d *DatasetIndexerUnit) cacheRemoveDatasetNode(datasetId string) error {
	log.WithFields(indexerLogFields).
		WithField("storage", "redis").
		Infof("Checking if dataset with id %s exists\n", datasetId)

	cmd := d.redisClient.Del(context.Background(), datasetId)
	if err := cmd.Err(); err != nil {
		log.WithFields(indexerLogFields).Error(err.Error())
		return err
	}

	r, err := cmd.Result()
	if err != nil {
		log.WithFields(indexerLogFields).Error(err.Error())
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
			log.WithFields(indexerLogFields).Error(err.Error())
		}
	}

	if err := d.dbInsertDataset(datasetId, dataset); err != nil {
		log.WithFields(indexerLogFields).Error(err.Error())
		return ErrInsertDataset
	}

	return nil
}

func (d *DatasetIndexerUnit) cacheInsertDataset(datasetId string, dataset *pb.Dataset) error {
	log.WithFields(indexerLogFields).
		WithField("storage", "redis").
		Infof("inserting dataset with id %s\n", datasetId)

	exists, err := d.cacheDatasetNodeExists(datasetId)
	if err != nil {
		log.WithFields(indexerLogFields).Error(err.Error())
		return err
	}

	datasetBytes, err := proto.Marshal(dataset)
	if err != nil {
		log.WithFields(indexerLogFields).Error(err.Error())
		return err
	}

	// The element already exists. Find the existing index and replace it in the list
	if exists {
		cmd := d.redisClient.MGet(context.Background(), datasetId)
		if err := cmd.Err(); err != nil {
			log.WithFields(indexerLogFields).Error(err.Error())
			return err
		}

		idx, err := cmd.Result()
		if err != nil {
			log.WithFields(indexerLogFields).Error(err.Error())
			return err
		}
		
		i, err := strconv.Atoi(idx[0].(string))
		if err != nil {
			log.WithFields(indexerLogFields).Error(err.Error())
			return err
		}

		intCmd := d.redisClient.LSet(context.Background(), datasetList, int64(i), datasetBytes)
		if err := intCmd.Err(); err != nil {
			log.WithFields(indexerLogFields).Error(err.Error())
			return err
		}
	} else {
		// Get index of new element
		intCmd := d.redisClient.LLen(context.Background(), datasetList)
		if err := intCmd.Err(); err != nil {
			log.WithFields(indexerLogFields).Error(err.Error())
			return err
		}
	
		len, err := intCmd.Uint64()
		if err != nil {
			log.WithFields(indexerLogFields).Error(err.Error())
			return err
		}

		if err := d.redisClient.Set(context.Background(), datasetId, len, 0).Err(); err != nil {
			log.WithFields(indexerLogFields).Error(err.Error())
			return err
		}

		if err := d.redisClient.RPush(context.Background(), "dsList", string(datasetBytes)).Err(); err != nil {
			log.WithFields(indexerLogFields).Error(err.Error())
			return err
		}
	}

	return nil
}


func (d *DatasetIndexerUnit) SetDatasetPolicy(datasetId string, policy *pb.Policy) error {
	if policy == nil {
		return errNilPolicy
	}

	if !d.DatasetExists(datasetId) {
		err := fmt.Errorf("No such dataset '%s' was found", datasetId)
		log.WithFields(indexerLogFields).Error(err.Error())
		return err
	}
	
	if err := d.dbInsertDatasetPolicy(datasetId, policy); err != nil {
		log.WithFields(indexerLogFields).Error(err.Error())
		return ErrInsertPolicy
	}

	return nil
}

func (d *DatasetIndexerUnit) GetDatasetPolicy(datasetId string) *pb.Policy {
	policy, err := d.dbSelectDatasetPolicy(datasetId)
	if err != nil {
		log.WithFields(indexerLogFields).Error(err.Error())
		return nil
	}

	return policy
}

func (d *DatasetIndexerUnit) DatasetsAtNode(node *pb.Node) []*pb.Dataset {
	datasets, err := d.dbSelectAllDatasetsAtNode(node)
	if err != nil {
		log.WithFields(indexerLogFields).Error(err.Error())
		return nil
	}

	return datasets
}

func (d *DatasetIndexerUnit) flushAll() error {
	return d.redisClient.FlushAll(context.Background()).Err()
}

func (d *DatasetIndexerUnit) reloadRedis() chan error {
	errc := make(chan error, 1)

	// Reload Redis by fetching the datasets from a channel.
	// Insert each dataset into Redis. 
	go func() {
		defer close(errc)
		dsChan, errChan, doneChan := d.dbSelectAllDatasets()
		pipe := d.redisClient.TxPipeline()
		idx := -1
		for {
			idx ++
			select {
			case dataset, ok := <-dsChan:
				if ok {
					marshalled, err := proto.Marshal(dataset)
					if err != nil {
						log.WithFields(indexerLogFields).Error(err.Error())
						continue
					}
			
					// Add the dataset to the list
					if err := d.redisClient.RPush(context.Background(), datasetList, marshalled).Err(); err != nil {
						log.WithFields(indexerLogFields).Error(err.Error())
						continue
					}

					// Add the index too
					if err := d.redisClient.Set(context.Background(), dataset.GetIdentifier(), idx, 0).Err(); err != nil {
						log.WithFields(indexerLogFields).Error(err.Error())
						continue
					}
		
					if _, err := pipe.Exec(context.Background()); err != nil {
						log.WithFields(indexerLogFields).Error(err.Error())
						continue
					}
				} else {
					return
				}
			case err, ok := <-errChan:
				if ok {
					log.WithFields(indexerLogFields).Error(err.Error())
					errc <-err
				}

			case <-doneChan:
				return
			}
		}
		errc <- nil
	}()

	return errc
}