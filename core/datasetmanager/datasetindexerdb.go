package datasetmanager

import (
	"context"
	"errors"
	"fmt"
	pb "github.com/arcsecc/lohpi/protobuf"
	log "github.com/sirupsen/logrus"
	pbtime "google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

var (
	errNoConnectionString = errors.New("No connection string specified")
)

func (d *DatasetIndexerUnit) dbSelectAllDatasets() (map[string]*pb.Dataset, error) {
	rows, err := d.pool.Query(context.Background(), `SELECT * FROM `+d.datasetStorageSchema+`.`+d.datasetStorageTable+`;`)
	if err != nil {
		log.WithFields(dbLogFields).Error(err.Error())
		return nil, err
	}

	defer rows.Close()

	datasets := make(map[string]*pb.Dataset)

	for rows.Next() {
		var id, policyVersion uint64
		var datasetId, dateCreated, dateUpdated string
		var policyContent, allowMultipleCheckout bool
		if err := rows.Scan(&id, &datasetId, &allowMultipleCheckout, &policyContent, &policyVersion, &dateCreated, &dateUpdated); err != nil {
			log.WithFields(dbLogFields).Error(err.Error())
			continue
		}

		created, err := time.Parse(time.RFC1123Z, dateCreated)
		if err != nil {
			log.WithFields(dbLogFields).Error(err.Error())
			continue
		}

		updated, err := time.Parse(time.RFC1123Z, dateUpdated)
		if err != nil {
			log.WithFields(dbLogFields).Error(err.Error())
			continue
		}

		ds := &pb.Dataset{
			Identifier: datasetId,
			Policy: &pb.Policy{
				DatasetIdentifier: datasetId,
				Content:           policyContent,
				Version:           policyVersion,
				DateCreated:       pbtime.New(created),
				DateUpdated:       pbtime.New(updated),
			},
		}

		datasets[datasetId] = ds
	}

	log.WithFields(dbLogFields).Infoln("Succsessfully selected a collection of datasets")

	return datasets, nil
}

func (d *DatasetIndexerUnit) dbRemoveDataset(datasetId string) error {
	q := `DELETE FROM ` + d.datasetStorageSchema + `.` + d.datasetStorageTable + ` WHERE dataset_id=$1;`
	commangTag, err := d.pool.Exec(context.Background(), q, datasetId)
	if err != nil {
		log.WithFields(dbLogFields).Error(err.Error())
		return err
	}

	if commangTag.RowsAffected() != 1 {
		err := fmt.Errorf("Could not delete dataset with id '%s'", datasetId)
		log.WithFields(dbLogFields).Error(err.Error())
		return err
	}

	log.WithFields(dbLogFields).Infof("Succsessfully deleted dataset with id '%s'", datasetId)

	return nil
}

func (d *DatasetIndexerUnit) dbInsertDataset(datasetId string, dataset *pb.Dataset) error {
	q := `INSERT INTO ` + d.datasetStorageSchema + `.` + d.datasetStorageTable + `
	(dataset_id, allow_multiple_checkout, policy_content, policy_version, date_created, date_updated) VALUES ($1, $2, $3, $4, $5, $6)
	ON CONFLICT (dataset_id) DO UPDATE	
	SET 
		dataset_id = $1,
		allow_multiple_checkout = $2,
		policy_content = $3, 
		policy_version = $4, 
		date_created = $5, 
		date_updated = $6
	WHERE ` + d.datasetStorageSchema + `.` + d.datasetStorageTable + `.dataset_id = $1;`

	if idxOpts := dataset.GetIndexOption(); idxOpts != nil {
		if policy := dataset.GetPolicy(); policy != nil {
			_, err := d.pool.Exec(context.Background(), q,
				datasetId,
				idxOpts.GetAllowMultipleCheckouts(),
				policy.GetContent(),
				policy.GetVersion(),
				policy.GetDateCreated().AsTime().Format(time.RFC1123Z),
				policy.GetDateUpdated().AsTime().Format(time.RFC1123Z))
			if err != nil {
				log.WithFields(dbLogFields).Error(err.Error())
				return err
			}
		} else {
			err := fmt.Errorf("Policy was empty for dataset %s", datasetId)
			log.WithFields(dbLogFields).Error(err.Error())
			return err
		}
	} else {
		err := fmt.Errorf("Policy was empty for dataset %s", datasetId)
		log.WithFields(dbLogFields).Error(err.Error())
		return err
	}

	log.WithFields(dbLogFields).Infof("Succsessfully inserted dataset with id '%s'", datasetId)

	return nil
}

// TODO not done
func (d *DatasetIndexerUnit) dbInsertDatasetPolicy(datasetId string, policy *pb.Policy) error {
	timeUpdated := policy.GetDateUpdated().AsTime().Format(time.RFC1123Z)

	q := `UPDATE ` + d.datasetStorageSchema + `.` + d.datasetStorageTable + ` SET policy_content = $2, policy_version = $3, date_updated = $4
	WHERE dataset_id = $1;`
	_, err := d.pool.Exec(context.Background(), q,
		datasetId,
		policy.GetContent(),
		policy.GetVersion(),
		timeUpdated)
	if err != nil {
		log.WithFields(dbLogFields).Error(err.Error())
		return err
	}

	log.WithFields(dbLogFields).
		WithField("dataset id", datasetId).
		WithField("policy content", policy.GetContent()).
		WithField("policy version", policy.GetVersion()).
		WithField("date updated", timeUpdated).
		Infof("Succsessfully updated the policy of dataset '%s'", datasetId)

	return nil
}

func (d *DatasetIndexerUnit) dbSelectDataset(datasetId string) (*pb.Dataset, error) {
	q := `SELECT dataset_id, allow_multiple_checkout, policy_content, policy_version, date_created, date_updated FROM ` +
		d.datasetStorageSchema + `.` + d.datasetStorageTable + ` WHERE dataset_id = $1 ;`

	log.WithFields(dbLogFields).
		WithField("context", "select one dataset").
		Debugf("Running PSQL query %s", q)

	var policyVersion uint64
	var dataset, dateCreated, dateUpdated string
	var policyContent, allowMultipleCheckout bool

	if err := d.pool.QueryRow(context.Background(), q, datasetId).Scan(&dataset, &allowMultipleCheckout, &policyContent, &policyVersion, &dateCreated, &dateUpdated); err != nil {
		log.WithFields(dbLogFields).Error(err.Error())
		return nil, err
	}

	created, err := time.Parse(time.RFC1123Z, dateCreated)
	if err != nil {
		log.WithFields(dbLogFields).Error(err.Error())
		return nil, err
	}

	updated, err := time.Parse(time.RFC1123Z, dateUpdated)
	if err != nil {
		log.WithFields(dbLogFields).Error(err.Error())
		return nil, err
	}

	log.WithFields(dbLogFields).Infof("Succsessfully selected dataset '%s'", datasetId)

	return &pb.Dataset{
		Identifier: datasetId,
		Policy: &pb.Policy{
			DatasetIdentifier: datasetId,
			Content:           policyContent,
			Version:           policyVersion,
			DateCreated:       pbtime.New(created),
			DateUpdated:       pbtime.New(updated),
		},
		IndexOption: &pb.IndexOption{
			AllowMultipleCheckouts: allowMultipleCheckout,
		},
	}, nil
}

func (d *DatasetIndexerUnit) dbSelectDatasetIdentifiers() ([]string, error) {
	rows, err := d.pool.Query(context.Background(), `SELECT dataset_id FROM `+d.datasetStorageSchema+`.`+d.datasetStorageTable+`;`)
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

	log.WithFields(dbLogFields).Infoln("Succsessfully selected a colllection of datasets")

	return ids, nil
}

func (d *DatasetIndexerUnit) dbDatasetExists(datasetId string) (bool, error) {
	var exists bool
	q := `SELECT EXISTS ( SELECT 1 FROM ` + d.datasetStorageSchema + `.` + d.datasetStorageTable + ` WHERE dataset_id = $1);`
	err := d.pool.QueryRow(context.Background(), q, datasetId).Scan(&exists)
	if err != nil {
		log.WithFields(dbLogFields).
			WithField("database query", fmt.Sprintf("could not find dataset with identifier  '%s' in database", datasetId)).
			Error(err.Error())
		return false, err
	}
	return exists, nil
}

func (d *DatasetIndexerUnit) dbSelectDatasetPolicy(datasetId string) (*pb.Policy, error) {
	q := `SELECT dataset_id, policy_content, policy_version, date_created, date_updated FROM ` +
		d.datasetStorageSchema + `.` + d.datasetStorageTable + ` WHERE dataset_id = $1 ;`

	var datasetIdentifier, dateCreated, dateUpdated string
	var content bool
	var version uint64

	if err := d.pool.QueryRow(context.Background(), q, datasetId).Scan(&datasetIdentifier, &content, &version, &dateCreated, &dateUpdated); err != nil {
		log.WithFields(dbLogFields)
		return nil, err
	}

	created, err := time.Parse(time.RFC1123Z, dateCreated)
	if err != nil {
		log.WithFields(dbLogFields).Error(err.Error())
		return nil, err
	}

	updated, err := time.Parse(time.RFC1123Z, dateUpdated)
	if err != nil {
		log.WithFields(dbLogFields).Error(err.Error())
		return nil, err
	}

	log.WithFields(dbLogFields).Infof("Succsessfully selected the policy of dataset '%s'", datasetId)

	return &pb.Policy{
		DatasetIdentifier: datasetId,
		Content:           content,
		Version:           version,
		DateCreated:       pbtime.New(created),
		DateUpdated:       pbtime.New(updated),
	}, nil
}

func (d *DatasetIndexerUnit) dbSelectAllDatasetsAtNode(node *pb.Node) ([]*pb.Dataset, error) {
	q := `SELECT ` +
		d.datasetStorageSchema + `.` + d.datasetStorageTable + `.dataset_id, ` +
		d.datasetStorageSchema + `.` + d.datasetStorageTable + `.allow_multiple_checkout, ` +
		d.datasetStorageSchema + `.` + d.datasetStorageTable + `.policy_content, ` +
		d.datasetStorageSchema + `.` + d.datasetStorageTable + `.policy_version, ` +
		d.datasetStorageSchema + `.` + d.datasetStorageTable + `.date_created, ` +
		d.datasetStorageSchema + `.` + d.datasetStorageTable + `.date_updated
	FROM ` + d.datasetStorageSchema + `.` + d.datasetStorageTable + ` INNER JOIN ` +
		d.datasetStorageSchema + `.` + d.datasetLookupTable + ` ON (` +
		d.datasetStorageSchema + `.` + d.datasetStorageTable + `.dataset_id = ` +
		d.datasetStorageSchema + `.` + d.datasetLookupTable + `.dataset_id AND ` +
		d.datasetStorageSchema + `.` + d.datasetLookupTable + `.node_name = $1);`

	rows, err := d.pool.Query(context.Background(), q, node.GetName())
	if err != nil {
		log.WithFields(dbLogFields).Error(err.Error())
		return nil, err
	}

	sets := make([]*pb.Dataset, 0)

	for rows.Next() {
		var policyVersion uint64
		var datasetId, dateCreated, dateUpdated string
		var policyContent, allowMultipleCheckout bool
		if err := rows.Scan(&datasetId, &allowMultipleCheckout, &policyContent, &policyVersion, &dateCreated, &dateUpdated); err != nil {
			log.WithFields(dbLogFields).Error(err.Error())
			continue
		}

		created, err := time.Parse(time.RFC1123Z, dateCreated)
		if err != nil {
			log.WithFields(dbLogFields).Error(err.Error())
			continue
		}

		updated, err := time.Parse(time.RFC1123Z, dateUpdated)
		if err != nil {
			log.WithFields(dbLogFields).Error(err.Error())
			continue
		}

		set := &pb.Dataset{
			Identifier: datasetId,
			Policy: &pb.Policy{
				DatasetIdentifier: datasetId,
				Content:           policyContent,
				Version:           policyVersion,
				DateCreated:       pbtime.New(created),
				DateUpdated:       pbtime.New(updated),
			},
			IndexOption: &pb.IndexOption{
				AllowMultipleCheckouts: allowMultipleCheckout,
			},
		}

		sets = append(sets, set)
	}

	return sets, nil
}
