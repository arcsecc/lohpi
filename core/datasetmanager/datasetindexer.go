package datasetmanager

import (
	"context"
	"fmt"
	pb "github.com/arcsecc/lohpi/protobuf"
	"github.com/jackc/pgx/v4/pgxpool"
	log "github.com/sirupsen/logrus"
)

var indexerLogFields = log.Fields{
	"package":     "datasetmanager",
	"description": "dataset indexing",
}

type DatasetIndexerUnit struct {
	datasetStorageSchema string
	datasetStorageTable string
	datasetLookupTable string
	pool *pgxpool.Pool
}

func init() {
	log.SetReportCaller(true)
}

func NewDatasetIndexerUnit(id string, pool *pgxpool.Pool) (*DatasetIndexerUnit, error) {
	if id == "" {
		return nil, ErrEmptyDatasetIdentifier
	}

	if pool == nil {
		return nil, ErrNilConnectionPool
	}

	return &DatasetIndexerUnit{
		pool: pool,
		datasetStorageSchema: id + "_schema",
		datasetStorageTable: id + "_policy_table",
		datasetLookupTable: id + "_dataset_lookup_table",
	}, nil
}

func (d *DatasetIndexerUnit) Dataset(ctx context.Context, datasetId string) (*pb.Dataset, error) {
	if datasetId == "" {
		return nil, ErrEmptyDatasetIdentifier
	}

	dataset, err := d.dbSelectDataset(datasetId)
	if err != nil {
		log.WithFields(indexerLogFields).Error(err.Error())
		return nil, ErrGetDataset
	}

	return dataset, nil
}

// TODO: add ranges
func (d *DatasetIndexerUnit) Datasets(ctx context.Context) (map[string]*pb.Dataset, error) {
	sets, err := d.dbSelectAllDatasets()
	if err != nil {
		log.WithFields(indexerLogFields).Error(err.Error())
		return nil, ErrGetDatasets
	}

	return sets, nil
}

func (d *DatasetIndexerUnit) DatasetIdentifiers(ctx context.Context, fromIdx int64, length int64) ([]string, error) {
	ids, err := d.dbSelectDatasetIdentifiers()
	if err != nil {
		log.WithFields(indexerLogFields).Error(err.Error())
		return nil, ErrLookupDatasetIdentifiers
	}
	return ids, nil
}

func (d *DatasetIndexerUnit) DatasetExists(ctx context.Context, datasetId string) (bool, error) {
	if datasetId == "" {
		return false, ErrEmptyDatasetIdentifier
	}

	exists, err := d.dbDatasetExists(datasetId)
	if err != nil {
		log.WithFields(indexerLogFields).Error(err.Error())
		return false, ErrDatasetExists
	}

	return exists, nil
}

func (d *DatasetIndexerUnit) RemoveDataset(ctx context.Context, datasetId string) error {
	if datasetId == "" {
		return ErrEmptyDatasetIdentifier
	}

	if err := d.dbRemoveDataset(datasetId); err != nil {
		log.WithFields(indexerLogFields).Error(err.Error())
		return ErrRemoveDataset
	}

	return nil
}

func (d *DatasetIndexerUnit) InsertDataset(ctx context.Context, datasetId string, dataset *pb.Dataset) error {
	if datasetId == "" {
		return ErrEmptyDatasetIdentifier
	}

	if dataset == nil {
		return ErrNoDataset
	}

	if err := d.dbInsertDataset(datasetId, dataset); err != nil {
		log.WithFields(indexerLogFields).Error(err.Error())
		return ErrInsertDataset
	}

	return nil
}

func (d *DatasetIndexerUnit) SetDatasetPolicy(ctx context.Context, datasetId string, policy *pb.Policy) error {
	if datasetId == "" {
		return ErrEmptyDatasetIdentifier
	}

	if policy == nil {
		log.WithFields(datasetLookupLogFields).Error(ErrNoPolicy.Error())
		return ErrNoPolicy
	}

	exists, err := d.DatasetExists(ctx, datasetId)
	if err != nil {
		log.WithFields(indexerLogFields).Error(err.Error())
		return ErrInsertPolicy
	}

	if !exists {
		err := fmt.Errorf("Dataset with identifier '%s' was not found", datasetId)
		log.WithFields(dbLogFields).Error(err.Error())
		return ErrDatasetNodeExists
	}

	if err := d.dbInsertDatasetPolicy(datasetId, policy); err != nil {
		log.WithFields(indexerLogFields).Error(err.Error())
		return ErrInsertPolicy
	}

	return nil
}

func (d *DatasetIndexerUnit) GetDatasetPolicy(ctx context.Context, datasetId string) (*pb.Policy, error) {
	if datasetId == "" {
		return nil, ErrEmptyDatasetIdentifier
	}

	policy, err := d.dbSelectDatasetPolicy(datasetId)
	if err != nil {
		log.WithFields(indexerLogFields).Error(err.Error())
		return nil, ErrGetDatasetPolicy
	}

	return policy, nil
}

func (d *DatasetIndexerUnit) DatasetsAtNode(ctx context.Context, node *pb.Node) ([]*pb.Dataset, error) {
	if node == nil {
		return nil, ErrNilNode
	}

	datasets, err := d.dbSelectAllDatasetsAtNode(node)
	if err != nil {
		log.WithFields(indexerLogFields).Error(err.Error())
		return nil, ErrDatasetExistsAtNode
	}

	return datasets, nil
}
