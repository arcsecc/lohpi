package datasetmanager

import (
	"database/sql"
	"errors"
	"fmt"
	pb "github.com/arcsecc/lohpi/protobuf"
	"regexp"
	"time"
	log "github.com/sirupsen/logrus"
	_ "github.com/lib/pq"
	"strconv"
	pbtime "google.golang.org/protobuf/types/known/timestamppb"
)

var (
	errNoConnectionString = errors.New("No connection string specified")
)

// Creates the table in the database that indexes
func (d *DatasetIndexerUnit) createDatasetIndexerTable(connectionString string) error {
	if connectionString == "" {
		return errNoConnectionString
	}

	q := `CREATE TABLE IF NOT EXISTS ` + d.datasetStorageSchema + `.` + d.datasetStorageTable + ` (
		id SERIAL PRIMARY KEY,
		dataset_id VARCHAR NOT NULL UNIQUE,
		allow_multiple_checkout BOOLEAN NOT NULL,
		policy_content BOOLEAN NOT NULL,
		policy_version INT NOT NULL,
		date_created VARCHAR NOT NULL,
		date_applied VARCHAR NOT NULL
	);`	

	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return err
	}

	if err := db.Ping(); err != nil {
		return err
	}

	_, err = db.Exec(q)
	if err != nil {
		return err
	}

	d.datasetIndexDB = db
	return nil
}

func (d *DatasetIndexerUnit) createSchema(connectionString string) error {
	if connectionString == "" {
		return errNoConnectionString
	}

	q := `CREATE SCHEMA IF NOT EXISTS ` + d.datasetStorageSchema + `;`
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return err
	}

	if err := db.Ping(); err != nil {
		return err
	}

	_, err = db.Exec(q)
	
	return err
}

func (d *DatasetIndexerUnit) dbGetAllDatasets() (map[string]*pb.Dataset, error) {
	rows, err := d.datasetIndexDB.Query(`SELECT * FROM ` + d.datasetStorageSchema + `.` + d.datasetStorageTable + `;`)
    if err != nil {
        return nil, err
    }
    defer rows.Close()
    
	datasets := make(map[string]*pb.Dataset)

    for rows.Next() {
        var id, datasetId, policyVersion, dateCreated, dateApplied string
		var policyContent bool
        if err := rows.Scan(&id, &datasetId, &policyContent, &policyVersion, &dateCreated, &dateApplied); err != nil {
            log.Errorln(err.Error())
			panic(err)

			continue
        }

		created, err := toTimestamppb(dateCreated)
		if err != nil {
			return nil, err
		}

		applied, err := toTimestamppb(dateApplied)
		if err != nil {
			return nil, err
		}

		v, err := strconv.ParseUint(policyVersion, 10, 64)
		if err != nil {
			return nil, err
		}
	
		dataset := &pb.Dataset{
			Identifier: datasetId,
			Policy: &pb.Policy{
				DatasetIdentifier: datasetId,
				Content: policyContent,
				Version: v,
				DateCreated: created,
				DateApplied: applied,
			},
		}
		datasets[datasetId] = dataset
    } 

	return datasets, nil
}

func (d *DatasetIndexerUnit) dbRemoveDataset(datasetId string) error {
	if datasetId == ""{
		return fmt.Errorf("Dataset id is empty")
	}

	q := `DELETE FROM ` + d.datasetStorageSchema + `.` + d.datasetStorageTable + ` WHERE
		dataset_id='` + datasetId + `';`

	_, err := d.datasetIndexDB.Exec(q)
	if err != nil {
		return err
	}
	
	return nil		
}

func (d *DatasetIndexerUnit) dbInsertDataset(datasetId string, dataset *pb.Dataset) error {
	if dataset == nil {
		return fmt.Errorf("Dataset is nil")
	}

	q := `INSERT INTO ` + d.datasetStorageSchema + `.` + d.datasetStorageTable + `
	(dataset_id, allow_multiple_checkout, policy_content, policy_version, date_created, date_applied) VALUES ($1, $2, $3, $4, $5, $6)
	ON CONFLICT (dataset_id) DO NOTHING;`

	_, err := d.datasetIndexDB.Exec(q, 
		datasetId, 
		dataset.GetAllowMultipleCheckouts(),
		dataset.GetPolicy().GetContent(), 
		dataset.GetPolicy().GetVersion(),
		dataset.GetPolicy().GetDateCreated().String(),
		dataset.GetPolicy().GetDateApplied().String())
	if err != nil {
		panic(err)
		return err
	}

	return nil
}

// TODO not done
func (d *DatasetIndexerUnit) dbInsertDatasetPolicy(datasetId string, policy *pb.Policy) error {
	if policy == nil {
		return fmt.Errorf("Policy is nil")
	}

	q:= `UPDATE ` + d.datasetStorageSchema + `.` + d.datasetStorageTable + ` SET policy_content = $2, policy_version = $3, date_applied = $4
	WHERE dataset_id = $1;`
	_, err := d.datasetIndexDB.Exec(q, 
		datasetId,
		policy.GetContent(), 
		policy.GetVersion(),
		policy.GetDateApplied().String())
	if err != nil {
		panic(err)
		return err
	}
	
	return nil
}

func (d *DatasetIndexerUnit) dbSelectDataset(datasetId string) *pb.Dataset {
	if datasetId == ""	{
		err := fmt.Errorf("Dataset identifier must not be an empty string")
		log.Error(err.Error())
		return nil
	}

	q := `SELECT dataset_id, allow_multiple_checkout, policy_content, policy_version, date_created, date_applied FROM ` + 
		d.datasetStorageSchema + `.` + d.datasetStorageTable + ` WHERE dataset_id = $1 ;`
	
	var dataset, dateCreated, dateApplied, policyVersion string
	var policyContent, allowMultipleCheckout bool

	err := d.datasetIndexDB.QueryRow(q, datasetId).Scan(&dataset, &allowMultipleCheckout, &policyContent, &policyVersion, &dateCreated, &dateApplied)
	switch err {
	case sql.ErrNoRows:
		log.Infoln("No rows were returned")
		return nil
  	case nil:
  	default:
		log.Error(err.Error())
		return nil
  	}
	
	created, err := toTimestamppb(dateCreated)
	if err != nil {
		log.Errorln(err.Error())
		return nil
	}

	applied, err := toTimestamppb(dateApplied)
	if err != nil {
		log.Errorln(err.Error())
		return nil
	}

	v, err := strconv.ParseUint(policyVersion, 10, 64)
	if err != nil {
		log.Errorln(err.Error())
		return nil
	}
  
	return &pb.Dataset{
		Identifier: datasetId,
		Policy: &pb.Policy{
			DatasetIdentifier: datasetId,
			Content: policyContent,
			Version: v,
			DateCreated: created,
			DateApplied: applied,
		},
		AllowMultipleCheckouts: allowMultipleCheckout,
	}
}

func (d *DatasetIndexerUnit) dbSelectDatasetIdentifiers() []string {
	rows, err := d.datasetIndexDB.Query(`SELECT dataset_id FROM ` + d.datasetStorageSchema + `.` + d.datasetStorageTable + `;`)
    if err != nil {
		log.Errorln(err.Error())
        return nil
    }
    defer rows.Close()
    
	ids := make([]string, 0)

    for rows.Next() {
        var datasetId string
        if err := rows.Scan(&datasetId); err != nil {
            log.Errorln(err.Error())
			continue
		}
		
		ids = append(ids, datasetId)
    } 

	return ids
}

func (d *DatasetIndexerUnit) dbDatasetExists(datasetId string) bool {
	if datasetId == "" {
		log.Errorln("Dataset id must not be empty")
		return false
	}

	var exists bool
	q := `SELECT EXISTS ( SELECT 1 FROM ` + d.datasetStorageSchema + `.` + d.datasetStorageTable +`
			WHERE dataset_id = '` + datasetId + `');`
	err := d.datasetIndexDB.QueryRow(q).Scan(&exists)
	if err != nil {
		log.Errorln(err.Error())
	}
	return exists
}

func (d *DatasetIndexerUnit) dbSelectDatasetPolicy(datasetId string) *pb.Policy {
	if datasetId == "" {
		log.Errorln("Dataset id must not be empty")
		return nil
	}

	q := `SELECT dataset_id, policy_content, policy_version, date_created, date_applied FROM ` + 
	d.datasetStorageSchema + `.` + d.datasetStorageTable + ` WHERE dataset_id = $1 ;`

	var datasetIdentifier, dateCreated, dateApplied string
	var content bool
	var version uint64
	
	err := d.datasetIndexDB.QueryRow(q, datasetId).Scan(&datasetIdentifier, &content, &version, &dateCreated, &dateApplied)
	switch err {
	case sql.ErrNoRows:
		log.Infoln("No rows were returned")
		return nil
  	case nil:
  	default:
		panic(err)
		log.Error(err.Error())
		return nil
  	}
	
	created, err := toTimestamppb(dateCreated)
	if err != nil {
		log.Errorln(err.Error())
		return nil
	}

	applied, err := toTimestamppb(dateApplied)
	if err != nil {
		log.Errorln(err.Error())
		return nil
	}

	return &pb.Policy{
		DatasetIdentifier: datasetId,
		Content: content,
		Version: version,
		DateCreated: created,
		DateApplied: applied,
	}
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
