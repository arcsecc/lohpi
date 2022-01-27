package datasetmanager

import (
	"context"
	pb "github.com/arcsecc/lohpi/protobuf"
	"github.com/jackc/pgx/v4/pgxpool"
	log "github.com/sirupsen/logrus"
)

type DatasetCheckoutServiceUnit struct {
	datasetCheckoutSchema      string
	datsetCheckoutTable        string
	datasetCheckoutPolicyTable string
	pool                       *pgxpool.Pool
}

var checkoutLogFields = log.Fields{
	"package":     "datasetmanager",
	"description": "dataset checkout manager",
}

func init() {
	log.SetReportCaller(true)
}

// We don't use in-memory maps because of the hassle of indexing it
func NewDatasetCheckoutServiceUnit(id string, pool *pgxpool.Pool) (*DatasetCheckoutServiceUnit, error) {
	if id == "" {
		log.WithFields(datasetLookupLogFields).Error(ErrEmptyInstanceIdentifier.Error())
		return nil, ErrEmptyInstanceIdentifier
	}

	if pool == nil {
		log.WithFields(datasetLookupLogFields).Error(ErrNilConnectionPool.Error())
		return nil, ErrNilConnectionPool
	}

	d := &DatasetCheckoutServiceUnit{
		datasetCheckoutSchema:      id + "_schema",
		datsetCheckoutTable:        id + "_dataset_checkout_table",
		datasetCheckoutPolicyTable: id + "_dataset_checkout_policy_table",
		pool:                       pool,
	}

	return d, nil
}

func (d *DatasetCheckoutServiceUnit) CheckoutDataset(ctx context.Context, datasetId string, checkout *pb.DatasetCheckout, systemPolicyVersion uint64, policy bool) error {
	if checkout == nil {
		return ErrNoDatasetCheckout
	}

	if datasetId == "" {
		return ErrEmptyDatasetIdentifier
	}

	if err := d.dbCheckoutDataset(ctx, datasetId, checkout, systemPolicyVersion, policy); err != nil {
		return ErrDatasetCheckout
	}

	return nil
}

func (d *DatasetCheckoutServiceUnit) DatasetIsCheckedOutByClient(ctx context.Context, datasetId string, client *pb.Client) (bool, error) {
	if datasetId == "" {
		return false, ErrEmptyDatasetIdentifier
	}

	if client == nil {
		return false, ErrNilDatasetCheckoutClient
	}

	exists, err := d.dbDatasetIsCheckedOutByClient(ctx, datasetId, client)
	if err != nil {
		log.WithFields(checkoutLogFields).Error(err.Error())
		return false, ErrDatasetIsCheckedOutByClient
	}

	return exists, nil
}

func (d *DatasetCheckoutServiceUnit) DatasetIsCheckedOut(ctx context.Context, datasetId string) (bool, error) {
	if datasetId == "" {
		return false, ErrEmptyDatasetIdentifier
	}

	exists, err := d.dbDatasetIsCheckedOut(ctx, datasetId)
	if err != nil {
		log.WithFields(checkoutLogFields).Error(err.Error())
		return false, ErrDatasetIsCheckedOut
	}

	return exists, nil
}

func (d *DatasetCheckoutServiceUnit) DatasetCheckouts(ctx context.Context, datasetId string, cursor int, limit int) (*pb.DatasetCheckouts, error) {
	if datasetId == "" {
		return nil, ErrEmptyDatasetIdentifier
	}

	checkouts, err := d.dbSelectDatasetCheckouts(ctx, datasetId, cursor, limit)
	if err != nil {
		log.WithFields(checkoutLogFields).Error(err.Error())
		return nil, ErrDatasetCheckouts
	}

	return checkouts, nil
}

func (d *DatasetCheckoutServiceUnit) SetDatasetSystemPolicy(ctx context.Context, datasetId string, version uint64, policy bool) error {
	if datasetId == "" {
		return ErrEmptyDatasetIdentifier
	}

	if err := d.dbSetDatasetSystemPolicy(ctx, datasetId, version, policy); err != nil {
		log.WithFields(checkoutLogFields).Error(err.Error())
		return ErrSetDatasetCheckoutAccess
	}

	return nil
}

func (d *DatasetCheckoutServiceUnit) GetDatasetSystemPolicy(ctx context.Context, datasetId string) (uint64, bool, error) {
	if datasetId == "" {
		return 0, false, ErrEmptyDatasetIdentifier
	}

	version, policy, err := d.dbGetDatasetSystemPolicy(ctx, datasetId)
	if err != nil {
		log.WithFields(checkoutLogFields).Error(err.Error())
		return 0, false, ErrGetDatasetCheckoutAccess
	}

	return version, policy, nil
}

func (d *DatasetCheckoutServiceUnit) CheckinDataset(ctx context.Context, datasetId string, client *pb.Client) error {
	if datasetId == "" {
		return ErrEmptyDatasetIdentifier
	}

	if client == nil {
		return ErrNilDatasetCheckoutClient
	}

	if err := d.dbCheckinDataset(ctx, datasetId, client); err != nil {
		return ErrDatasetCheckin
	}

	return nil
}

func (d *DatasetCheckoutServiceUnit) NumberOfDatasetCheckouts(ctx context.Context, datasetId string) (int, error) {
	if datasetId == "" {
		return 0, ErrEmptyDatasetIdentifier
	}

	n, err := d.dbNumberOfDatasetCheckouts(ctx, datasetId)
	if err != nil {
		log.WithFields(datasetLookupLogFields).Error(err.Error())
		return 0, ErrNumDatasetCheckouts
	}

	return n, nil
}
