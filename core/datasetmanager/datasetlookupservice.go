package datasetmanager

import (
	"fmt"
	"os"
	"context"
	pb "github.com/arcsecc/lohpi/protobuf"
	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
	"errors"
	"github.com/jackc/pgx/v4/pgxpool"
	"time"
	"github.com/golang/protobuf/proto"
)

var nodeLookupServiceLogFields = log.Fields{
	"package": "datasetmanager",
	"description": "storage node lookup service",
}

// Configuration struct for the dataset manager
type DatasetLookupServiceConfig struct {
	// The database connection string used to back the in-memory data structures.
	// If this is left empty, the in-memory data structures will not be backed by persistent storage.
	// TODO: use timeouts to flush the data structures to the db at regular intervals :))
	// TODO: configure SQL connection pools and timeouts. The timeout values must be sane
	SQLConnectionString string

	RedisClientOptions *redis.Options
}

// TODO: if db transation fails, do not insert into redis!

type DatasetLookupService struct {
	redisClient *redis.Client
	config *DatasetLookupServiceConfig
	datasetLookupSchema string
	datasetLookupTable string
	storageNodeTable string
	pool *pgxpool.Pool
}

var datasetResolverLogFields = log.Fields{
	"package": "datasetmanager",
	"description": "dataset lookup service",
}

func init() { 
	log.SetReportCaller(true)
}

// Returns a new DatasetIndexerService, given the configuration
func NewDatasetLookupService(id string, config *DatasetLookupServiceConfig) (*DatasetLookupService, error) {
	if config == nil {
		return nil, errNilConfig
	}

	if config.SQLConnectionString == "" {
		return nil, errNoConnectionString			
	}

	if id == "" {
		return nil, errNoId
	}

	poolConfig, err := pgxpool.ParseConfig(config.SQLConnectionString)
	if err != nil {
		log.WithFields(datasetResolverLogFields).Error(err.Error())
		return nil, err
	}

	poolConfig.MaxConnLifetime = time.Second * 10
	poolConfig.MaxConnIdleTime = time.Second * 4
	poolConfig.MaxConns = 100
	poolConfig.HealthCheckPeriod = time.Second * 1
	poolConfig.LazyConnect = false

	pool, err := pgxpool.ConnectConfig(context.Background(), poolConfig)
	if err != nil {
		log.Errorln(err.Error())
		os.Exit(1)
	}

	d := &DatasetLookupService{
		pool: pool,
		config: config,
		datasetLookupSchema: id + "_schema",
		datasetLookupTable: id + "_dataset_lookup_table",
		storageNodeTable: id + "_storage_node_table",
	}

	// Initialize Redis cache. Use Redis only locally
	if config.RedisClientOptions != nil {
		d.redisClient = redis.NewClient(config.RedisClientOptions)
		pong, err := d.redisClient.Ping(context.Background()).Result()
		if err != nil {
			log.WithFields(datasetResolverLogFields).Error(err.Error())
			return nil, err
		}
		
		if pong != "PONG" {
			log.Error(errors.New("Value of Redis pong was wrong"))
		}

		/*if err := d.flushAll(); err != nil {
			log.Errorln(err.Error())
		}*/

		go func() {
			/*errc := d.reloadRedis()
			if err := <-errc; err != nil {
				log.Error(err.Error())
			}*/
		}()
	}

	log.WithFields(datasetResolverLogFields).Infof("Succsessfully created new dataset lookup service\n")

	return d, nil
}

func (d *DatasetLookupService) DatasetLookupNode(datasetId string) *pb.Node {
	if datasetId == "" {
		log.WithFields(datasetResolverLogFields).Error(errNoDatasetId.Error())
	}

	/*if d.redisClient != nil {
		node, err := d.cacheDatasetNode(datasetId)
		if node != nil && err == nil {
			return node
		}

		if err != nil {
			log.WithFields(datasetResolverLogFields).Error(err.Error())
		}
	}*/

	node, err := d.dbSelectDatasetNode(datasetId)
	if err != nil {
		log.WithFields(datasetResolverLogFields).Error(err.Error())
		return nil
	}
	return node
}

func (d *DatasetLookupService) cacheDatasetNode(datasetId string) (*pb.Node, error) {
	if datasetId == "" {
		log.WithFields(datasetResolverLogFields).Error(errNoDatasetId.Error())
	}

	cmd := d.redisClient.MGet(context.Background(), datasetId)
	if err := cmd.Err(); err != nil {
		log.WithFields(datasetResolverLogFields).Error(err.Error())
		return nil, err
	}

	nodeBytes, err := cmd.Result()
	if err != nil {
		log.WithFields(datasetResolverLogFields).Error(err.Error())
		return nil, err
	}
	
	if len(nodeBytes) > 0 {
		if nodeBytes[0] != nil {
			node := &pb.Node{}
			if err := proto.Unmarshal([]byte(nodeBytes[0].(string)), node); err != nil {
				log.WithFields(datasetResolverLogFields).Error(err.Error())
				return nil, err
			}
			return node, nil
		}
	}
	
	err = fmt.Errorf("Storage node with identifier '%s' was not found in cache")
	log.WithFields(datasetResolverLogFields).Error(err.Error())
	return nil, err
}

func (d *DatasetLookupService) InsertDatasetLookupEntry(datasetId string, nodeName string) error {
	if datasetId == "" {
		log.WithFields(datasetResolverLogFields).Error(errNoDatasetId.Error())
		return errNoDatasetId
	}

	if nodeName == "" {
		log.WithFields(datasetResolverLogFields).Error(errNoNodeName.Error())
		return errNoNodeName
	}

	if err := d.dbInsertDatasetLookupEntry(datasetId, nodeName); err != nil {
		log.WithFields(datasetResolverLogFields).Error(err.Error())
		return err
	}

	/*if d.redisClient != nil {
		if err := d.cacheInsertDatasetNode(datasetId, node); err != nil {
			log.WithFields(datasetResolverLogFields).Error(err.Error())
			return err
		}
	}*/
	
	return nil
}

func (d *DatasetLookupService) cacheInsertDatasetNode(datasetId string, node *pb.Node) error {
	if datasetId == "" {
		log.WithFields(datasetResolverLogFields).Error(errNoDatasetId.Error())
	}

	if node == nil {
		log.WithFields(datasetResolverLogFields).Error(errNilNode.Error())
		return errNilNode
	}

	nodeBytes, err := proto.Marshal(node)
	if err != nil {
		return err
	}
	
	return d.redisClient.Set(context.Background(), datasetId, nodeBytes, 0).Err()
}

// TODO: return errors from db interface as well
func (d *DatasetLookupService) DatasetNodeExists(datasetId string) bool {
	if datasetId == "" {
		log.WithFields(datasetResolverLogFields).Error(errNoDatasetId.Error())
		return false
	}

	/*if d.redisClient != nil {
		exists, err := d.cacheDatasetNodeExists(datasetId)
		if exists && err == nil {
			return exists
		}

		if err != nil {
			log.WithFields(datasetResolverLogFields).Error(err.Error())
		}
	}*/

	// If there was a cache miss but a hit in PSQL, insert it into Redis
	exists := d.dbDatasetNodeExists(datasetId)
	if exists && d.redisClient != nil {
		log.WithFields(datasetResolverLogFields).
			Infof(`Found entry in database but not in Redis. Inserting '%s' into redis`, datasetId)

		node, err := d.dbSelectDatasetNode(datasetId)
		if err != nil {
			log.WithFields(datasetResolverLogFields).Error()
		}

		if node != nil {
			nodeBytes, err := proto.Marshal(node)
			if err != nil {
				panic(err)
				log.WithFields(datasetResolverLogFields).Error()
			}

			if err := d.redisClient.Set(context.Background(), datasetId, nodeBytes, 0).Err(); err != nil {
				log.WithFields(datasetResolverLogFields).Error()
			}
		}
	}

	return exists
}

func (d *DatasetLookupService) cacheDatasetNodeExists(datasetId string) (bool, error) {
	cmd := d.redisClient.Exists(context.Background(), datasetId)
	if cmd.Err() != nil {
		return false, cmd.Err()
	}

	r, err := cmd.Result()
	if err != nil {
		log.WithFields(datasetResolverLogFields).Error(cmd.Err().Error())
		return false, cmd.Err()
	}

	if r == 1 {
		return true, nil
	} else {
		return false, nil
	}
}

func (d *DatasetLookupService) RemoveDatasetLookupEntry(datasetId string) error {
	if datasetId == "" {
		log.WithFields(datasetResolverLogFields).Error(errNoDatasetId.Error())
		return errNoDatasetId
	}

	/*if d.redisClient != nil {
		if err := d.cacheRemoveDatasetNode(datasetId); err != nil {
			log.Error(err.Error())
		}
	}*/

	if err := d.dbRemoveDatasetNode(datasetId); err != nil {
		log.WithFields(datasetResolverLogFields).Error(err.Error())
		return err
	}
	return nil
}

func (d *DatasetLookupService) cacheRemoveDatasetNode(datasetId string) error {
	cmd := d.redisClient.Del(context.Background(), datasetId)
	if cmd.Err() != nil {
		return cmd.Err()
	}

	r, err := cmd.Result()
	if err != nil {
		log.WithFields(datasetResolverLogFields).Error(cmd.Err().Error())
		return cmd.Err()
	}

	if r == 1 {
		return nil
	} else {
		return fmt.Errorf("Dataset node with identifier '%s' was not found in cache", datasetId)
	}

	return nil
}

func (d *DatasetLookupService) DatasetIdentifiers() []string {
	/*if d.redisClient != nil {
		ids, err := d.cacheDatasetIdentifiers()
		if ids != nil && err == nil {
			return ids
		}

		if err != nil {
			log.WithFields(datasetResolverLogFields).Error(err.Error())
		}
	}*/

	return d.dbSelectDatasetIdentifiers()
}

// TODO: add ranges
func (d *DatasetLookupService) cacheDatasetIdentifiers() ([]string, error) {
	ids := make([]string, 0)
	iter := d.redisClient.Scan(context.Background(), 0, "*", 0).Iterator()
	for iter.Next(context.Background()) {
		ids = append(ids, iter.Val())
	}
	
	if err := iter.Err(); err != nil {
		log.WithFields(datasetResolverLogFields).Error(err.Error())
	    return nil, err
	}
	
	return ids, nil
}

func (d *DatasetLookupService) DatasetIdentifiersAtNode(nodeName string) []string {
	if nodeName == "" {
		log.WithFields(datasetResolverLogFields).Error(errNoNodeName.Error())
		return nil
	}

	ids, err := d.dbSelectDatasetIdentifiersAtNode(nodeName)
	if err != nil {
		log.WithFields(datasetResolverLogFields).Error(err.Error())
		return nil
	}
	return ids
}

func (d *DatasetLookupService) DatasetNodeName(datasetId string) string {
	return ""
}

func (d *DatasetLookupService) ResolveDatasetIdentifiers(newIdentifiers []string, staleIdentifiers []string, node *pb.Node) error {
	if node == nil {
		return errNilNode
	}

	if len(newIdentifiers) == 0 {
		log.WithFields(datasetResolverLogFields).Infoln("No dataset identifiers to add")
	}

	if len(staleIdentifiers) == 0 {
		log.WithFields(datasetResolverLogFields).Infoln("No dataset identifiers to remove")
	}

	if err := d.dbRemoveDatasetIdentifiers(staleIdentifiers, node); err != nil {
		log.WithFields(datasetResolverLogFields).Error(err.Error())
		return ErrResolveDatasetIdentifiers	
	}

	if err := d.dbInsertDatasetIdentifiers(newIdentifiers, node); err != nil {
		log.WithFields(datasetResolverLogFields).Error(err.Error())
		return ErrInsertDatasetIdentifiers	
	}

	return nil
}

func (d *DatasetLookupService) flushAll() error {
	log.Println("FlushAll!")
	return d.redisClient.FlushAll(context.Background()).Err()
}
 // TODO: optimize this by adding missing entries and removing stale ones one at a time.
func (d *DatasetLookupService) reloadRedis() chan error {
	errc := make(chan error, 1)

	go func() {
		defer close(errc)
	
		// TODO: don't load everyting into memory!
		maps, err := d.dbGetAllDatasetNodes()
		if err != nil {
			log.WithFields(datasetResolverLogFields).Error(err.Error())
			errc <- err
			return
		}

		pipe := d.redisClient.TxPipeline()
		for k, v := range maps {
			marshalled, err := proto.Marshal(v)
			if err != nil {
				log.WithFields(datasetResolverLogFields).Error(err.Error())
				continue
			}
			
			if err := d.redisClient.MSet(context.Background(), k, marshalled).Err(); err != nil {
				log.WithFields(datasetResolverLogFields).Error(err.Error())
				errc <- err
				return
			}
		}

		if _, err := pipe.Exec(context.Background()); err != nil {
			log.WithFields(datasetResolverLogFields).Error(err.Error())
			errc <- err
		}
		errc <- nil

		
	}()

	return errc
}
