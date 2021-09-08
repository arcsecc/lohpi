package datasetmanager

import (
	"context"
	"errors"
	"fmt"
	pb "github.com/arcsecc/lohpi/protobuf"
	"regexp"
	"time"
	//"github.com/sirupsen/logrus"
	"strconv"
	pbtime "google.golang.org/protobuf/types/known/timestamppb"
	log "github.com/sirupsen/logrus"
)

var (
	errNoConnectionString = errors.New("No connection string specified")
)

var dbLogFields = log.Fields{
	"package": "datasetmanagers",
	"action": "database client",
}

func (d *DatasetIndexerUnit) dbSelectAllDatasets() (chan *pb.Dataset, chan error) {
	dsChan := make(chan *pb.Dataset, 1)
	errChan := make(chan error, 1)

	go func() {
		defer close(dsChan)
		defer close(errChan)
		
		rows, err := d.pool.Query(context.Background(), `SELECT * FROM ` + d.datasetStorageSchema + `.` + d.datasetStorageTable + `;`)
    	if err != nil {
			log.WithFields(dbLogFields).Error(err.Error())
    	    errChan <- err
			return
    	}
    	defer rows.Close()
	
    	for rows.Next() {
			var id, policyVersion uint64
    	    var datasetId, dateCreated, dateApplied string
			var policyContent, allowMultipleCheckout bool
    	    if err := rows.Scan(&id, &datasetId, &allowMultipleCheckout, &policyContent, &policyVersion, &dateCreated, &dateApplied); err != nil {
				log.WithFields(dbLogFields).Error(err.Error())
				continue
    	    }

			created, err := toTimestamppb(dateCreated)
			if err != nil {
				log.WithFields(dbLogFields).Error(err.Error())
				continue
			}

			applied, err := toTimestamppb(dateApplied)
			if err != nil {
				log.WithFields(dbLogFields).Error(err.Error())
				continue
			}	
			
			dsChan <- &pb.Dataset{
				Identifier: datasetId,
				Policy: &pb.Policy{
					DatasetIdentifier: datasetId,
					Content: policyContent,
					Version: policyVersion,
					DateCreated: created,
					DateApplied: applied,
				},
			}
    	}

		log.WithFields(dbLogFields).Infoln("Succsessfully selected a collection of datasets")
	}()

	return dsChan, errChan 
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
		log.WithFields(dbLogFields).Error(err)
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
	(dataset_id, allow_multiple_checkout, policy_content, policy_version, date_created, date_applied) VALUES ($1, $2, $3, $4, $5, $6)
	ON CONFLICT (dataset_id) DO NOTHING;		`

	_, err := d.pool.Exec(context.Background(), q, 
		datasetId, 
		dataset.GetAllowMultipleCheckouts(),
		dataset.GetPolicy().GetContent(), 
		dataset.GetPolicy().GetVersion(),
		dataset.GetPolicy().GetDateCreated().String(),
		dataset.GetPolicy().GetDateApplied().String())
	if err != nil {
		log.WithFields(dbLogFields).Error(err)
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

	q:= `UPDATE ` + d.datasetStorageSchema + `.` + d.datasetStorageTable + ` SET policy_content = $2, policy_version = $3, date_applied = $4
	WHERE dataset_id = $1;`
	_, err := d.pool.Exec(context.Background(), q, 
		datasetId,
		policy.GetContent(), 
		policy.GetVersion(),
		policy.GetDateApplied().String())
	if err != nil {
		log.WithFields(dbLogFields).Error(err)
		return err
	}
	
	log.WithFields(dbLogFields).
		WithField("Dataset id", datasetId).
		WithField("Policy content",	policy.GetContent()).
		WithField("Policy version",	policy.GetVersion()).
		WithField("Date applied",	policy.GetDateApplied().String()).
		Infof("Succsessfully updated the policy of dataset '%s'\n", datasetId)

	return nil
}

func (d *DatasetIndexerUnit) dbSelectDataset(datasetId string) (*pb.Dataset, error) {
	if datasetId == ""{
		return nil, errNoDatasetId
	}

	q := `SELECT dataset_id, allow_multiple_checkout, policy_content, policy_version, date_created, date_applied FROM ` + 
		d.datasetStorageSchema + `.` + d.datasetStorageTable + ` WHERE dataset_id = $1 ;`

	var policyVersion uint64
	var dataset, dateCreated, dateApplied string
	var policyContent, allowMultipleCheckout bool

	if err := d.pool.QueryRow(context.Background(),q, datasetId).Scan(&dataset, &allowMultipleCheckout, &policyContent, &policyVersion, &dateCreated, &dateApplied); err != nil {
		log.WithFields(dbLogFields).Error(err)
		return nil, err
  	}
	
	created, err := toTimestamppb(dateCreated)
	if err != nil {
		log.WithFields(dbLogFields).Error(err)
	}

	applied, err := toTimestamppb(dateApplied)
	if err != nil {
		log.WithFields(dbLogFields).Error(err)
	}
  
	log.WithFields(dbLogFields).Infof("Succsessfully selected dataset '%s'\n", datasetId)

	return &pb.Dataset{
		Identifier: datasetId,
		Policy: &pb.Policy{
			DatasetIdentifier: datasetId,
			Content: policyContent,
			Version: policyVersion,
			DateCreated: created,
			DateApplied: applied,
		},
		AllowMultipleCheckouts: allowMultipleCheckout,
	}, nil
}

func (d *DatasetIndexerUnit) dbSelectDatasetIdentifiers() ([]string, error) {
	rows, err := d.pool.Query(context.Background(), `SELECT dataset_id FROM ` + d.datasetStorageSchema + `.` + d.datasetStorageTable + `;`)
    if err != nil {
		log.WithFields(dbLogFields).Error(err)
        return nil, err
    }
    defer rows.Close()
    
	ids := make([]string, 0)

    for rows.Next() {
        var datasetId string
        if err := rows.Scan(&datasetId); err != nil {
			log.WithFields(dbLogFields).Error(err)
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
		panic(err)
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

	q := `SELECT dataset_id, policy_content, policy_version, date_created, date_applied FROM ` + 
	d.datasetStorageSchema + `.` + d.datasetStorageTable + ` WHERE dataset_id = $1 ;`

	var datasetIdentifier, dateCreated, dateApplied string
	var content bool
	var version uint64
	
	if err := d.pool.QueryRow(context.Background(), q, datasetId).Scan(&datasetIdentifier, &content, &version, &dateCreated, &dateApplied); err != nil {
		log.WithFields(dbLogFields)
		return nil, err
  	}
	
	created, err := toTimestamppb(dateCreated)
	if err != nil {
		log.WithFields(dbLogFields)
		return nil, err
	}

	applied, err := toTimestamppb(dateApplied)
	if err != nil {
		log.WithFields(dbLogFields)
		return nil, err
	}

	log.WithFields(dbLogFields).Infof("Succsessfully selected the policy of dataset '%s'\n", datasetId)

	return &pb.Policy{
		DatasetIdentifier: datasetId,
		Content: content,
		Version: version,
		DateCreated: created,
		DateApplied: applied,
	}, nil
}

func toTimestamppb(tString string) (*pbtime.Timestamp, error) {
	if tString == "" {
		return nil, fmt.Errorf("Timestamp string is empty!")
	}

	rxp := regexp.MustCompile("[0-9]+").FindAllStringSubmatch(tString, -1)
	if len(rxp) == 0 {
		err := fmt.Errorf("Regexp string is empty!")
		log.Error(err.Error())
		return nil, err
	}

	secsAsString := rxp[0][0]
	nanosAsString := rxp[1][0]		

	seconds, err := strconv.ParseInt(secsAsString, 10, 64)
	if err != nil {
		return nil, err
	}

	nanos, err := strconv.ParseInt(nanosAsString, 10, 64)
	if err != nil {
		return nil, err
	}

	return pbtime.New(time.Unix(seconds, nanos)), nil
}
