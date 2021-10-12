package datasetmanager

import (
	"context"
	"errors"
	"fmt"
	pb "github.com/arcsecc/lohpi/protobuf"
	"time"
	pbtime "google.golang.org/protobuf/types/known/timestamppb"
	log "github.com/sirupsen/logrus"
)

var (
	errNoConnectionString = errors.New("No connection string specified")
)

var dbLogFields = log.Fields{
	"package": "core/datasetmanager",
	"action": "database client",
}

func (d *DatasetIndexerUnit) dbSelectAllDatasets() (chan *pb.Dataset, chan error, chan struct{}) {
	dsChan := make(chan *pb.Dataset, 1)
	errChan := make(chan error, 1)
	doneChan := make(chan struct{})

	go func() {
		defer close(dsChan)
		defer close(errChan)
		defer close(doneChan)
		
		rows, err := d.pool.Query(context.Background(), `SELECT * FROM ` + d.datasetStorageSchema + `.` + d.datasetStorageTable + `;`)
    	if err != nil {
			log.WithFields(dbLogFields).Error(err.Error())
    	    errChan <- err
			return
    	}
    	defer rows.Close()
	
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
			}
	  
			updated, err := time.Parse(time.RFC1123Z, dateUpdated)
			if err != nil {
				log.WithFields(dbLogFields).Error(err.Error())
			}
			
			dsChan <- &pb.Dataset{
				Identifier: datasetId,
				Policy: &pb.Policy{
					DatasetIdentifier: datasetId,
					Content: policyContent,
					Version: policyVersion,
					DateCreated: pbtime.New(created),
					DateUpdated: pbtime.New(updated),
				},
			}
    	}

		doneChan <-struct{}{}

		log.WithFields(dbLogFields).Infoln("Succsessfully selected a collection of datasets")
	}()

	return dsChan, errChan, doneChan
}

func (d *DatasetIndexerUnit) dbRemoveDataset(datasetId string) error {
	if datasetId == ""{
		return errNoDatasetId
	}

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
	
	log.WithFields(dbLogFields).Infof("Succsessfully deleted dataset with id '%s'\n'", datasetId)

	return nil		
}

func (d *DatasetIndexerUnit) dbInsertDataset(datasetId string, dataset *pb.Dataset) error {
	if datasetId == ""{
		return errNoDatasetId
	}

	if dataset == nil {
		return errNilDataset
	}

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

	_, err := d.pool.Exec(context.Background(), q, 
		datasetId, 
		dataset.GetAllowMultipleCheckouts(),
		dataset.GetPolicy().GetContent(), 
		dataset.GetPolicy().GetVersion(),
		dataset.GetPolicy().GetDateCreated().AsTime().Format(time.RFC1123Z),
		dataset.GetPolicy().GetDateUpdated().AsTime().Format(time.RFC1123Z))
	if err != nil {
		log.WithFields(dbLogFields).Error(err.Error())
		return err
	}

	log.WithFields(dbLogFields).Infof("Succsessfully inserted dataset with id '%s'\n'", datasetId)

	return nil
}

// TODO not done
func (d *DatasetIndexerUnit) dbInsertDatasetPolicy(datasetId string, policy *pb.Policy) error {
	if datasetId == ""{
		return errNoDatasetId
	}

	if policy == nil {
		return errNilPolicy
	}

	updated := policy.GetDateUpdated().AsTime().Format(time.RFC1123Z)

	q:= `UPDATE ` + d.datasetStorageSchema + `.` + d.datasetStorageTable + ` SET policy_content = $2, policy_version = $3, date_updated = $4
	WHERE dataset_id = $1;`
	_, err := d.pool.Exec(context.Background(), q, 
		datasetId,
		policy.GetContent(), 
		policy.GetVersion(),
		updated)
	if err != nil {
		log.WithFields(dbLogFields).Error(err.Error())
		return err
	}
	
	log.WithFields(dbLogFields).
		WithField("Dataset id", datasetId).
		WithField("Policy content",	policy.GetContent()).
		WithField("Policy version",	policy.GetVersion()).
		WithField("Date updated",	policy.GetDateUpdated().String()).
		Infof("Succsessfully updated the policy of dataset '%s'\n", datasetId)

	return nil
}

func (d *DatasetIndexerUnit) dbSelectDataset(datasetId string) (*pb.Dataset, error) {
	if datasetId == ""{
		return nil, errNoDatasetId
	}

	q := `SELECT dataset_id, allow_multiple_checkout, policy_content, policy_version, date_created, date_updated FROM ` + 
		d.datasetStorageSchema + `.` + d.datasetStorageTable + ` WHERE dataset_id = $1 ;`

	log.WithFields(dbLogFields).
		WithField("context", "select one dataset").
		Debugf("Running PSQL query %s\n", q)

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
  
	log.WithFields(dbLogFields).Infof("Succsessfully selected dataset '%s'\n", datasetId)

	return &pb.Dataset{
		Identifier: datasetId,
		Policy: &pb.Policy{
			DatasetIdentifier: datasetId,
			Content: policyContent,
			Version: policyVersion,
			DateCreated: pbtime.New(created),
			DateUpdated: pbtime.New(updated),
		},
		AllowMultipleCheckouts: allowMultipleCheckout,
	}, nil
}

func (d *DatasetIndexerUnit) dbSelectDatasetIdentifiers() ([]string, error) {
	rows, err := d.pool.Query(context.Background(), `SELECT dataset_id FROM ` + d.datasetStorageSchema + `.` + d.datasetStorageTable + `;`)
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
	if datasetId == ""{
		return false, errNoDatasetId
	}
	
	var exists bool
	q := `SELECT EXISTS ( SELECT 1 FROM ` + d.datasetStorageSchema + `.` + d.datasetStorageTable + ` WHERE dataset_id = $1);`
	err := d.pool.QueryRow(context.Background(), q, datasetId).Scan(&exists)
	if err != nil {
		log.WithFields(dbLogFields).
			WithField("database query", fmt.Sprintf("could not find '%s' in database", datasetId)).
			Errorln(err.Error())
		return false, err
	}
	return exists, nil
}

func (d *DatasetIndexerUnit) dbSelectDatasetPolicy(datasetId string) (*pb.Policy, error) {
	if datasetId == ""{
		return nil, errNoDatasetId
	}

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
	}
  
	updated, err := time.Parse(time.RFC1123Z, dateUpdated)
	if err != nil {
		log.WithFields(dbLogFields).Error(err.Error())
	}

	log.WithFields(dbLogFields).Infof("Succsessfully selected the policy of dataset '%s'\n", datasetId)

	return &pb.Policy{
		DatasetIdentifier: datasetId,
		Content: content,
		Version: version,
		DateCreated: pbtime.New(created),
		DateUpdated: pbtime.New(updated),
	}, nil
}

func (d *DatasetIndexerUnit) dbSelectAllDatasetsAtNode(node *pb.Node) ([]*pb.Dataset, error) {
	if node == nil {
		return nil, errNilNode
	}

	q := `SELECT ` + 
	d.datasetStorageSchema + `.` + d.datasetStorageTable + `.dataset_id, ` + 
	d.datasetStorageSchema + `.` + d.datasetStorageTable + `.allow_multiple_checkout, ` + 
	d.datasetStorageSchema + `.` + d.datasetStorageTable + `.policy_content, ` + 
	d.datasetStorageSchema + `.` + d.datasetStorageTable + `.policy_version, ` + 
	d.datasetStorageSchema + `.` + d.datasetStorageTable + `.date_created, ` + 
	d.datasetStorageSchema + `.` + d.datasetStorageTable + `.date_updated
	FROM ` + d.datasetStorageSchema + `.` + d.datasetStorageTable + ` INNER JOIN ` + 
	d.datasetStorageSchema + `.` + d.datasetLookupTable  + ` ON (` +
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
		}
  
		updated, err := time.Parse(time.RFC1123Z, dateUpdated)
		if err != nil {
			log.WithFields(dbLogFields).Error(err.Error())
		}

		set := &pb.Dataset{
			Identifier: datasetId,
			AllowMultipleCheckouts: allowMultipleCheckout,
			Policy: &pb.Policy{
				DatasetIdentifier: datasetId,
				Content: policyContent,
				Version: policyVersion,
				DateCreated: pbtime.New(created),
				DateUpdated: pbtime.New(updated),
			},
		}

		sets = append(sets, set)
	}

	return sets, nil
}