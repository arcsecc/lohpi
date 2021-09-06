package datasetmanager

import (
	"context"
	pb "github.com/arcsecc/lohpi/protobuf"
	"errors"
	//"github.com/golang/protobuf/proto"
	"fmt"
	"github.com/jackc/pgx/v4/pgxpool"
	log "github.com/sirupsen/logrus"
)

type DatasetCheckoutServiceUnitConfig struct {
	SQLConnectionString string
}

type DatasetCheckoutServiceUnit struct {
	config *DatasetCheckoutServiceUnitConfig
	datasetCheckoutSchema string
	datsetCheckoutTable string
	pool *pgxpool.Pool
}

var (
	ErrNilDatasetCheckoutConfig = errors.New("No configuration for dataset checkout manager")
	ErrDatasetCheckoutFailure = errors.New("Dataset checkout failed")
	ErrNoClient = errors.New("Client is nil")
)

var checkoutLogFields = log.Fields{
	"package": "datasetmanager",
	"description": "dataset checkout manager",
}

func init() { 
	log.SetReportCaller(true)
}

// We don't use in-memory maps because of the hassle of indexing it
func NewDatasetCheckoutServiceUnit(id string, config *DatasetCheckoutServiceUnitConfig) (*DatasetCheckoutServiceUnit, error) {
	if config == nil {
		return nil, ErrNilDatasetCheckoutConfig
	}

	if config.SQLConnectionString == "" {
		return nil, errNoConnectionString
	}

	if id == "" {
		return nil, errNoId
	}

	pool, err := pgxpool.Connect(context.Background(), config.SQLConnectionString)
	if err != nil {
		return nil, err
	}

	d := &DatasetCheckoutServiceUnit{
		config: config,
		datasetCheckoutSchema: id + "_schema",
		datsetCheckoutTable: id + "_dataset_checkout_table",
		pool: pool,
	}
	
	return d, nil
}

func (d *DatasetCheckoutServiceUnit) CheckoutDataset(datasetId string, checkout *pb.DatasetCheckout) error {
	if datasetId == "" {
		return errNoDatasetId
	}

	if checkout == nil {
		return errNilCheckout
	}

	if err := d.dbCheckoutDataset(datasetId, checkout); err != nil {
		log.WithFields(checkoutLogFields).Error(err.Error())
		return ErrDatasetCheckoutFailure
	}

	return nil
}

func (d *DatasetCheckoutServiceUnit) DatasetIsCheckedOut(datasetId string, client *pb.Client) bool {
	if datasetId == "" {
		log.WithFields(checkoutLogFields).Error(errNoDatasetId.Error())
		return false
	}
	
	if client == nil {
		log.WithFields(checkoutLogFields).Error(ErrNoClient.Error())
		return false
	}

	exists, err := d.dbDatasetIsCheckedOutByClient(datasetId, client)
	if err != nil {
		log.WithFields(checkoutLogFields).Error(err.Error())
	}

	return exists
}

func (d *DatasetCheckoutServiceUnit) DatasetCheckouts(datasetId string) ([]*pb.DatasetCheckout, error) {
	if datasetId == "" {
		err := fmt.Errorf("Dataset identifier must not be empty")
		log.Error(err.Error())
		return nil, err
	}

	dsChan, errChan := d.dbSelectDatasetCheckouts(datasetId)
	dsCheckouts := make([]*pb.DatasetCheckout, 0)

	for {
		select {
		case checkout, ok := <-dsChan:
			if ok {
				dsCheckouts = append(dsCheckouts, checkout)
			} else {
				break
			}
		case err, ok := <-errChan:
			if ok {
				log.Error(err.Error())
			} else {
				break
			}
		}
	}

	return dsCheckouts, nil
}