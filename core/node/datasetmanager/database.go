package datasetmanager

import (
	"errors"
	"database/sql"
	pb "github.com/arcsecc/lohpi/protobuf"
	log "github.com/sirupsen/logrus"
	_ "github.com/lib/pq"
)

var (
	errNoConnectionString = errors.New("No connection string specified")
)

func (dm *DatasetManager) createSchema(connectionString string) error {
	if connectionString == "" {
		return errNoConnectionString
	}

	q := `CREATE SCHEMA IF NOT EXISTS ` + schemaName + `;`
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

	return nil
}

// Creates the table in the database that tracks which client has checked out datasets
func (dm *DatasetManager) createDatasetCheckoutTable(connectionString string) error {
	if connectionString == "" {
		return errNoConnectionString
	}

	q := `CREATE TABLE IF NOT EXISTS ` + schemaName + `.` + datasetCheckoutTable + ` (
		id SERIAL PRIMARY KEY, 
		client_id VARCHAR(200) NOT NULL, 
		client_name VARCHAR(200) NOT NULL,
		dataset_id VARCHAR(200) NOT NULL,
		tstamp TIMESTAMP NOT NULL);` // consider using timezone as well

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

	dm.datasetCheckoutDB = db
	return nil
}

// Creates the table in the database that assigns policies to datasets
func (dm *DatasetManager) createDatasetPolicyTable(connectionString string) error {
	if connectionString == "" {
		return errNoConnectionString
	}

	q := `CREATE TABLE IF NOT EXISTS ` + schemaName + `.` + datasetPolicyTable + ` (
		id SERIAL PRIMARY KEY,
		dataset_id VARCHAR(200) NOT NULL UNIQUE,
		issuer VARCHAR(20) NOT NULL,
		version INTEGER NOT NULL,
		date_created TIMESTAMP NOT NULL,
		content BOOLEAN NOT NULL);`

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

	dm.datasetPolicyDB = db
	return nil
}

func (dm *DatasetManager) dbInsertPolicyIntoTable(p *pb.Policy) error {
	if p == nil {
		return errNilPolicy
	}

	q := `INSERT INTO ` + schemaName + `.` + datasetPolicyTable + `
	(dataset_id, issuer, version, date_created, content) VALUES ($1, $2, $3, current_timestamp, $4)
	ON CONFLICT (dataset_id) 
	DO
		UPDATE SET content = $4;`

	_, err := dm.datasetPolicyDB.Exec(q, p.GetDatasetIdentifier(), p.GetIssuer(), p.GetVersion(), p.GetContent())
	if err != nil {
		log.Errorln("SQL insert error:", err.Error())
	}
	return nil
}

func (dm *DatasetManager) dbInsertDatasetCheckout(d *pb.DatasetCheckout) error {
	if d == nil {
		return errNilCheckout
	}

	return nil
}