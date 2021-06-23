package directoryserver

import (
	"database/sql"
	//"encoding/json"
	//"errors"
	//"fmt"
	//"github.com/lestrrat-go/jwx/jws"
	_ "github.com/lib/pq"
	log "github.com/sirupsen/logrus"
)



var (
	dbName          = "directory_server_db"
	schemaName      = "directory_server_schema"
	checkoutTable   = "checkout_table"
	datasetTable	= "dataset_table"
)


// Main entry point for initializing the database schema and its tables on Microsoft Azure
func (d *DirectoryServerCore) initializeDirectorydb(connectionString string) error {
	if connectionString == "" {
		log.Warnln("Connection string is empty")
	}
	
	// Create schema
	if err := d.createSchema(connectionString); err != nil {  
		return err
	}

	// Dataset table
	if err := d.initializeDatasetTable(connectionString); err != nil { 
		return err
	}

	// Client table
	if err := d.initializeCheckoutTable(connectionString); err != nil {
		return err
	}
	
	return nil
}

// Creates the table in the database that assigns policies to datasets
// TODO: refine this to perform boolean operations as a temp fix
func (d *DirectoryServerCore) initializeDatasetTable(connectionString string) error {
	q := `CREATE TABLE IF NOT EXISTS ` + schemaName + `.` + datasetTable + ` (
		dataset_id VARCHAR(200) PRIMARY KEY,
		project_description VARCHAR(10000),	
		size INT
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

	d.datasetDB = db
	return nil
}

// Creates the table in the database that tracks which client has checked out datasets
func (d *DirectoryServerCore) initializeCheckoutTable(connectionString string) error {
	q := `CREATE TABLE IF NOT EXISTS ` + schemaName + `.` + checkoutTable + ` (
		c_id VARCHAR(200) PRIMARY KEY, 
		c_name VARCHAR(200), 
		c_phonenumber VARCHAR(12),
		c_mail VARCHAR(200),
		c_ipaddr VARCHAR(200),
		dataset_id VARCHAR(200),
		t_stamp TIMESTAMP
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

	d.checkoutDB = db
	return nil
}

// Creates the schema, given the connection string
func (d *DirectoryServerCore) createSchema(connectionString string) error {

	q := `CREATE SCHEMA IF NOT EXISTS ` + schemaName + `;`
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return err
	}
	if err := db.Ping(); err != nil {		// Error here ("SSL is not enabled on the server")
		panic(err)
		return err
	}
	_, err = db.Exec(q)
	if err != nil {
		return err
	}

	return nil
}


// Insert datasetId into database, called from `directoryservercore.go, line 162`
func (d *DirectoryServerCore) dbInsertDataset(dataSetId string) error {
	q := `INSERT INTO ` + schemaName + `.` + datasetTable + `
		(dataset_id) VALUES ($1)
		ON CONFLICT (dataset_id) 
		DO
			UPDATE SET dataset_id = $1;`
	_, err := d.datasetDB.Exec(q, dataSetId)
	if err != nil {
		return err
	}

	return nil
}

func (d *DirectoryServerCore) updateProjectDescription(id string, project_description string) error {

	q := `INSERT INTO ` + schemaName + `.` + datasetTable + ` 
	(dataset_id, project_description) VALUES ($1, $2)
	ON CONFLICT (dataset_id)
	DO
		UPDATE SET project_description = $2;`


	_, err := d.datasetDB.Exec(q, id, project_description)
	if err != nil {
		return err
	}

	return nil
}

func (d *DirectoryServerCore) getProjectDescriptionDB(id string) (string, error) {

	q := `SELECT project_description FROM ` + schemaName + `.` + datasetTable + ` 
	WHERE dataset_id = $1;`

	rows, err := d.datasetDB.Query(q, id)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	if err := rows.Err(); err != nil {
		log.Errorln(err.Error())
		return "", err
	}

	var project_description sql.NullString
	for rows.Next() {
		if err := rows.Scan(&project_description); err != nil {
			log.Errorln(err.Error())
			return "", err
		}
	}

	return project_description.String, nil
}