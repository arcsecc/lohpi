package datasetmanager

import (
	"database/sql"
	pb "github.com/arcsecc/lohpi/protobuf"
	"github.com/go-redis/redis"
	"errors"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"fmt"
)

type DatasetCheckoutServiceUnitConfig struct {
	SQLConnectionString string
	RedisClientOptions *redis.Options
}

type DatasetCheckoutServiceUnit struct {
	redisClient *redis.Client
	datasetCheckoutDB *sql.DB	
	config *DatasetCheckoutServiceUnitConfig
	datasetCheckoutSchema string
	datsetCheckoutTable string
}

// We don't use in-memory maps because of the hassle of indexing it
func NewDatasetCheckoutServiceUnit(id string, config *DatasetCheckoutServiceUnitConfig) (*DatasetCheckoutServiceUnit, error) {
	if config == nil {
		return nil, errNilConfig
	}

	if config.SQLConnectionString == "" {
		return nil, errNoConnectionString
	}

	if id == "" {
		return nil, errNoId
	}

	d := &DatasetCheckoutServiceUnit{
		config: config,
		datasetCheckoutSchema: id + "_dataset_checkout_schema",
		datsetCheckoutTable: id + "_datset_checkout_table",

	}
	
	if err := d.createSchema(config.SQLConnectionString); err != nil {
		return nil, err
	}

	if err := d.createTable(config.SQLConnectionString); err != nil {
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

		/*errc := d.reloadRedis()
		if err := <-errc; err != nil {
			return nil, err
		}*/
	}

	return d, nil
}

func (d *DatasetCheckoutServiceUnit) CheckoutDataset(datasetId string, checkout *pb.DatasetCheckout) error {
	if checkout == nil {
		return errors.New("Dataset checkout is nil")
	}

	if d.redisClient != nil {
		if err := d.cacheCheckoutDataset(datasetId, checkout); err != nil {
			log.Error(err.Error())
		}
	}

	return d.dbCheckoutDataset(datasetId, checkout)
}

func (d *DatasetCheckoutServiceUnit) cacheCheckoutDataset(datasetId string, checkout *pb.DatasetCheckout) error {
	checkoutBytes, err := proto.Marshal(checkout)
	if err != nil {
		return err
	}

	return d.redisClient.Set(datasetId, checkoutBytes, 0).Err()
}

func (d *DatasetCheckoutServiceUnit) DatasetIsCheckedOut(datasetId string, client *pb.Client) (bool, error) {
	if datasetId == "" {
		err := fmt.Errorf("Dataset identifier must not be empty")
		return false, err
	}
	
	if client == nil {
		err := fmt.Errorf("Client cannot be nil")
		return false, err
	}

	return d.dbDatasetIsCheckedOutByClient(datasetId, client)
}

func (d *DatasetCheckoutServiceUnit) DatasetCheckouts() ([]*pb.DatasetCheckout, error) {
	return d.dbGetAllDatasetCheckouts()
}

func (d *DatasetCheckoutServiceUnit) DatasetCheckout(datasetId string, client *pb.Client) *pb.DatasetCheckout {
	if datasetId == "" {
		err := fmt.Errorf("Dataset identifier must not be empty")
		log.Error(err.Error())
		return nil
	}

	if client == nil {
		err := fmt.Errorf("Client cannot be nil")
		log.Error(err.Error())
		return nil
	}

	dsCheckout, err := d.dbSelectDatasetCheckout(datasetId, client)
	if err != nil {
		log.Error(err.Error())
		return nil
	}

	return dsCheckout
}


func (d *DatasetCheckoutServiceUnit) reloadRedis() chan error {
	ch := make(chan error, 1)
	
	go func() {
		ch <- nil
	}()

	return ch}