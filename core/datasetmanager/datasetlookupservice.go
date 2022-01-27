package datasetmanager

import (
	"context"
	pb "github.com/arcsecc/lohpi/protobuf"
	"github.com/jackc/pgx/v4/pgxpool"
	log "github.com/sirupsen/logrus"
)

var nodeLookupServiceLogFields = log.Fields{
	"package":     "datasetmanager",
	"description": "storage node lookup service",
}

type DatasetLookupService struct {
	datasetLookupSchema string
	datasetLookupTable string
	storageNodeTable string
	pool *pgxpool.Pool
}

var datasetLookupLogFields = log.Fields{
	"package":     "datasetmanager",
	"description": "dataset lookup service",
}

func init() {
	log.SetReportCaller(true)
}

// Returns a new DatasetIndexerService, given the configuration
func NewDatasetLookupService(id string, pool *pgxpool.Pool) (*DatasetLookupService, error) {
	if id == "" {
		return nil, ErrEmptyInstanceIdentifier
	}

	if pool == nil {
		return nil, ErrNilConnectionPool
	}

	d := &DatasetLookupService{
		pool: pool,
		datasetLookupSchema: id + "_schema",
		datasetLookupTable: id + "_dataset_lookup_table",
		storageNodeTable: id + "_storage_node_table",
	}

	return d, nil
}

func (d *DatasetLookupService) DatasetLookupNode(ctx context.Context, datasetId string) (*pb.Node, error) {
	if datasetId == "" {
		return nil, ErrEmptyDatasetIdentifier
	}

	node, err := d.dbSelectDatasetNode(ctx, datasetId)
	if err != nil {
		log.WithFields(datasetLookupLogFields).Error(err.Error())
		return nil, ErrLookupDatasetNode
	}

	return node, nil
}

func (d *DatasetLookupService) InsertDatasetLookupEntry(ctx context.Context, datasetId string, nodeName string) error {
	if datasetId == "" {
		return ErrEmptyDatasetIdentifier
	}

	if nodeName == "" {
		return ErrNilNodeIdentifier
	}

	if err := d.dbInsertDatasetLookupEntry(ctx, datasetId, nodeName); err != nil {
		log.WithFields(datasetLookupLogFields).Error(err.Error())
		return ErrInsertDataset
	}

	return nil
}

// TODO: return errors from db interface as well
func (d *DatasetLookupService) DatasetNodeExists(ctx context.Context, datasetId string) (bool, error) {
	if datasetId == "" {
		return false, ErrEmptyDatasetIdentifier
	}

	// If there was a cache miss but a hit in PSQL, insert it into Redis
	exists, err := d.dbDatasetNodeExists(ctx, datasetId)
	if err != nil {
		log.WithFields(datasetLookupLogFields).Error(err.Error())
		return false, ErrDatasetNodeExists
	}

	return exists, nil
}

func (d *DatasetLookupService) RemoveDatasetLookupEntry(ctx context.Context, datasetId string) error {
	if datasetId == "" {
		return ErrEmptyDatasetIdentifier
	}

	if err := d.dbRemoveDatasetNode(ctx, datasetId); err != nil {
		log.WithFields(datasetLookupLogFields).Error(err.Error())
		return ErrRemoveDataset
	}

	return nil
}

func (d *DatasetLookupService) DatasetIdentifiers(ctx context.Context, cursor int, limit int) ([]string, error) {
	ids, err := d.dbSelectDatasetIdentifiers(ctx, cursor, limit)
	if err != nil {
		log.WithFields(datasetLookupLogFields).Error(err.Error())
		return nil, ErrLookupDatasetIdentifiers
	}

	return ids, nil
}

func (d *DatasetLookupService) DatasetIdentifiersAtNode(ctx context.Context, nodeName string) ([]string, error) {
	if nodeName == "" {
		return nil, ErrEmptyNodeIdentifier
	}

	ids, err := d.dbSelectDatasetIdentifiersAtNode(ctx, nodeName)
	if err != nil {
		log.WithFields(datasetLookupLogFields).Error(err.Error())
		return nil, ErrLookupNodeDatasets
	}

	return ids, nil
}

func (d *DatasetLookupService) DatasetExistsAtNode(ctx context.Context, datasetId string, nodeName string) (bool, error) {
	if datasetId == "" {
		return false, ErrEmptyDatasetIdentifier
	}

	if nodeName == "" {
		return false, ErrEmptyNodeIdentifier
	}

	exists, err := d.dbDatasetExistsAtNode(ctx, datasetId, nodeName)
	if err != nil {
		log.WithFields(datasetLookupLogFields).Error(err.Error())
		return false, ErrDatasetExistsAtNode
	}

	return exists, nil
}

func (d *DatasetLookupService) NumberOfDatasets(ctx context.Context) (int64, error) {
	n, err := d.dbNumberOfDatasets(ctx)
	if err != nil {
		log.WithFields(datasetLookupLogFields).Error(err.Error())
		return 0, ErrCountDatasets
	}

	return n, nil
}
