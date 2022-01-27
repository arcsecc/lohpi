package datasetmanager

import (
	"context"
	"fmt"
	pb "github.com/arcsecc/lohpi/protobuf"
	"github.com/jackc/pgx/v4"
	log "github.com/sirupsen/logrus"
	pbtime "google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

func (d *DatasetLookupService) dbInsertDatasetLookupEntry(ctx context.Context, datasetId string, nodeName string) error {
	q := `INSERT INTO ` + d.datasetLookupSchema + `.` + d.datasetLookupTable + `
	(dataset_id, node_name) VALUES ($1, $2)
	ON CONFLICT (dataset_id) DO UPDATE	
	SET 
		dataset_id = $1,
		node_name = $2
	WHERE ` + d.datasetLookupSchema + `.` + d.datasetLookupTable + `.dataset_id = $1;`

	log.WithFields(dbLogFields).Infof("Running PSQL query %s", q)

	if _, err := d.pool.Exec(ctx, q, datasetId, nodeName); err != nil {
		log.WithFields(dbLogFields).Error(err.Error())
		return err
	}
	return nil
}

func (d *DatasetLookupService) dbDatasetNodeExists(ctx context.Context, datasetId string) (bool, error) {
	var exists bool
	q := `SELECT EXISTS ( SELECT 1 FROM ` + d.datasetLookupSchema + `.` + d.datasetLookupTable + ` WHERE dataset_id = $1);`

	log.WithFields(dbLogFields).Infof("Running PSQL query %s\n", q)

	err := d.pool.QueryRow(ctx, q, datasetId).Scan(&exists)
	if err != nil {
		log.WithFields(dbLogFields).Error(err.Error())
		return false, err
	}

	return exists, nil
}

func (d *DatasetLookupService) dbRemoveDatasetNode(ctx context.Context, datasetId string) error {
	exists, err := d.dbDatasetNodeExists(ctx, datasetId)
	if err != nil {
		log.WithFields(dbLogFields).Error(err.Error())
		return err
	}

	if !exists {
		err := fmt.Errorf("Dataset with identifier '%s' does not exist", datasetId)
		log.WithFields(dbLogFields).Error(err.Error())
		return err
	}

	q := `DELETE FROM ` + d.datasetLookupSchema + `.` + d.datasetLookupTable + ` WHERE dataset_id = $1;`
	log.WithFields(dbLogFields).Infof("Running PSQL query %s\n", q)

	cmdTag, err := d.pool.Exec(ctx, q, datasetId)
	if err != nil {
		log.WithFields(dbLogFields).Error(err.Error())
		return err
	}

	if cmdTag.RowsAffected() == 0 {
		log.Infoln("Zero rows were affected after deletion of dataset with identifier", datasetId)
	}

	return nil
}

func (d *DatasetLookupService) dbSelectDatasetNode(ctx context.Context, datasetId string) (*pb.Node, error) {
	q := `SELECT ` +
		d.datasetLookupSchema + `.` + d.storageNodeTable + `.node_name, ` +
		d.datasetLookupSchema + `.` + d.storageNodeTable + `.ip_address, ` +
		d.datasetLookupSchema + `.` + d.storageNodeTable + `.public_id, ` +
		d.datasetLookupSchema + `.` + d.storageNodeTable + `.https_address, ` +
		d.datasetLookupSchema + `.` + d.storageNodeTable + `.port, ` +
		d.datasetLookupSchema + `.` + d.storageNodeTable + `.boottime 
		FROM ` + d.datasetLookupSchema + `.` + d.storageNodeTable + ` INNER JOIN ` +
		d.datasetLookupSchema + `.` + d.datasetLookupTable + ` ON (` +
		d.datasetLookupSchema + `.` + d.datasetLookupTable + `.node_name = ` +
		d.datasetLookupSchema + `.` + d.storageNodeTable + `.node_name AND ` +
		d.datasetLookupSchema + `.` + d.datasetLookupTable + `.dataset_id = $1);`

	log.WithFields(dbLogFields).Infof("Running PSQL query %s\n", q)

	var nodeName, ipAddress, httpsAddress, boottime string
	var port int32
	var publicId []byte

	err := d.pool.QueryRow(ctx, q, datasetId).Scan(&nodeName, &ipAddress, &publicId, &httpsAddress, &port, &boottime)
	switch err {
	case pgx.ErrNoRows:
		log.WithFields(dbLogFields).
			WithField("database query", fmt.Sprintf("could not find '%s' among storage nodes", datasetId)).
			Error(err.Error())
		return nil, nil
	case nil:
	default:
		log.WithFields(dbLogFields).Error(err.Error())
		return nil, err
	}

	bTime, err := time.Parse(time.RFC1123Z, boottime)
	if err != nil {
		log.WithFields(dbLogFields).Error(err.Error())
	}

	log.WithFields(dbLogFields).Infof("Successfully selected dataset node with dataset identifier '%s'\n", datasetId)

	return &pb.Node{
		Name:         nodeName,
		IfritAddress: ipAddress,
		Id:           publicId,
		HttpsAddress: httpsAddress,
		Port:         port,
		BootTime:     pbtime.New(bTime),
	}, nil
}

// TODO add ranges
func (d *DatasetLookupService) dbSelectDatasetIdentifiers(ctx context.Context, cursor int, limit int) ([]string, error) {
	q := `SELECT dataset_id FROM ` + d.datasetLookupSchema + `.` + d.datasetLookupTable + ` WHERE id > $1 ORDER BY dataset_id LIMIT $2`
	log.WithFields(dbLogFields).Infof("Running PSQL query %s", q)

	rows, err := d.pool.Query(ctx, q, cursor, limit)
	if err != nil {
		log.WithFields(dbLogFields).Error(err.Error())
		return nil, err
	}
	defer rows.Close()

	ids := make([]string, 0)

	for rows.Next() {
		var datasetId string
		if err := rows.Scan(&datasetId); err != nil {
			log.WithFields(dbLogFields).Error(err.Error())
			continue
		}

		ids = append(ids, datasetId)
	}

	log.WithFields(dbLogFields).Info("Successfully selected all dataset identifiers")

	return ids, nil
}

// TODO add ranges
func (d *DatasetLookupService) dbSelectDatasetIdentifiersAtNode(ctx context.Context, nodeName string) ([]string, error) {
	q := `SELECT dataset_id FROM ` + d.datasetLookupSchema + `.` + d.datasetLookupTable + `
		WHERE node_name = $1;`
	log.WithFields(dbLogFields).Infof("Running PSQL query %s\n", q)

	rows, err := d.pool.Query(ctx, q, nodeName)
	if err != nil {
		log.WithFields(dbLogFields).Error(err.Error())
		return nil, err
	}
	defer rows.Close()

	ids := make([]string, 0)

	for rows.Next() {
		var datasetId string
		if err := rows.Scan(&datasetId); err != nil {
			log.WithFields(dbLogFields).Error(err.Error())
			return nil, err
		}

		ids = append(ids, datasetId)
	}

	log.WithFields(dbLogFields).Infof("Successfully selected dataset identifiers stored by '%s'", nodeName)

	return ids, nil
}

func (d *DatasetLookupService) dbInsertDatasetIdentifiers(ctx context.Context, identifiers []string, node *pb.Node) error {
	for _, i := range identifiers {
		q := `INSERT INTO ` + d.datasetLookupSchema + `.` + d.datasetLookupTable + `
		(dataset_id, node_name) VALUES ($1, $2)
		ON CONFLICT (dataset_id) DO UPDATE	
		SET 
			dataset_id = $1,
			node_name = $2
		WHERE ` + d.datasetLookupSchema + `.` + d.datasetLookupTable + `.dataset_id = $1;`

		log.WithFields(dbLogFields).Infof("Running PSQL query %s\n", q)

		if _, err := d.pool.Exec(ctx, q, i, node.GetName()); err != nil {
			log.WithFields(dbLogFields).Error(err.Error())
			continue
		}
	}

	return nil
}

func (d *DatasetLookupService) dbDatasetExistsAtNode(ctx context.Context, datasetId string, nodeName string) (bool, error) {
	var exists bool
	q := `SELECT EXISTS (SELECT 1 FROM ` + d.datasetLookupSchema + `.` + d.datasetLookupTable + `WHERE dataset_id = $1 AND node_name = $2);`

	log.WithFields(dbLogFields).Infof("Running PSQL query %s\n", q)

	err := d.pool.QueryRow(ctx, q, datasetId).Scan(&exists)
	if err != nil {
		log.WithFields(dbLogFields).Error(err.Error())
		return false, err
	}

	return exists, nil
}

// TODO: optimize me!
func (d *DatasetLookupService) dbRemoveDatasetIdentifiers(ctx context.Context, identifiers []string, node *pb.Node) error {
	for _, i := range identifiers {
		q := `DELETE FROM ` + d.datasetLookupSchema + `.` + d.datasetLookupTable + ` WHERE dataset_id = $1`

		cmdTag, err := d.pool.Exec(ctx, q, i)
		if err != nil {
			log.WithFields(dbLogFields).Error(err.Error())
			continue
		}

		if cmdTag.RowsAffected() == 0 {
			log.Infoln("Zero rows were affected after deletion of dataset with identifier", i)
		}
	}
	return nil
}

func (d *DatasetLookupService) dbNumberOfDatasets(ctx context.Context) (int64, error) {
	q := `SELECT count(id) FROM ` + d.datasetLookupSchema + `.` + d.datasetLookupTable + `;`

	var result int64
	err := d.pool.QueryRow(ctx, q).Scan(&result)
	if err != nil {
		log.WithFields(dbLogFields).Error(err.Error())
		return 0, err
	}

	return result, nil
}
