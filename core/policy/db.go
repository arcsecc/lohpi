package policy

import (
	"fmt"
	"database/sql"
	log "github.com/sirupsen/logrus"
	"os"
	"errors"
	pb "github.com/arcsecc/lohpi/protobuf"
)

// MAJOR TODO: use database/sql function to avoid SQL injections :)
// Initializes the underlying policy store's database using 'id' as the unique identifier for the relation.
func (ps *PolicyStore) initializePolicyDb() error {
	var connectionString = fmt.Sprintf(os.Getenv("POLICY_STORE_DB_CONNECTION_STRING"))
	if connectionString == "" {
		return errors.New("Tried to fetch 'POLICY_STORE_DB_CONNECTION_STRING' from environment but it was not set.")
	}

	log.Infoln("Using POLICY_STORE_DB_CONNECTION_STRING environment variable")

	return ps.initializePostgreSQLdb(connectionString)
}

// Main entry point for initializing the database schema and its tables on Microsoft Azure
func (ps *PolicyStore)  initializePostgreSQLdb(connectionString string) error {
	// Create schema 
	if err := ps.createSchema(connectionString); err != nil {
		panic(err)
		return err
	}

	// Policy table
	if err := ps.initializeDatasetPolicyTable(connectionString); err != nil {
		panic(err)
		return err
	}
	
	return nil 
}

// Creates the table in the database that assigns policies to datasets
func (ps *PolicyStore) initializeDatasetPolicyTable(connectionString string) error {
	q := `CREATE TABLE IF NOT EXISTS ` + schemaName + `.` + datasetPolicyTable + ` (
		id SERIAL PRIMARY KEY, 
		dataset_id VARCHAR(200) NOT NULL UNIQUE, 
		storage_node VARCHAR(200) NOT NULL,
		allowed BOOLEAN NOT NULL);`		// TODO: to be changed. for now we do things simple

	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		panic(err)
		return err
	}

	if err := db.Ping(); err != nil {
		panic(err)
		return err
	}

	_, err = db.Exec(q)
	if err != nil {
		return err
	}

	ps.datasetDB = db
	return nil
}

// Creates the schema, given the connection string 
func (ps *PolicyStore) createSchema(connectionString string) error {
	q := `CREATE SCHEMA IF NOT EXISTS ` + schemaName + `;`
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		panic(err)
		return err
	}

	if err := db.Ping(); err != nil {
		panic(err)
		return err
	}

	_, err = db.Exec(q)
	if err != nil {
		panic(err)
		return err
	}

	return nil
}

// Returns true if the given data object is registered in the database, 
// returns false otherwise.
func (ps *PolicyStore) datasetExistsInDb(id string) bool {
	exists := false
	q := `SELECT EXISTS ( SELECT 1 FROM ` + schemaName + `.` + datasetPolicyTable + ` WHERE dataset_id = "` + id + `");`
	err := ps.datasetDB.QueryRow(q).Scan(&exists)
	if err != nil {
		log.Warnln(err.Error())
	}
	return exists
}

// Sets the policy for the dataset given by the datasetId. The objectAttribute is the policy
// associated with the dataset.
func (ps *PolicyStore) insertDatasetPolicy(datasetId, nodeIdentifier string, policy *pb.Policy) error {
	//q := `INSERT INTO ` + schemaName + `.` + datasetPolicyTable + `(` + n.policyIdentifier + `, policyAttributes) VALUES ($1, $2)`
	q := `INSERT INTO ` + schemaName + `.` + datasetPolicyTable + `
	(dataset_id, storage_node, allowed) VALUES ($1, $2, $3)
	ON CONFLICT (dataset_id) 
	DO
		UPDATE SET allowed = $3;`

	_, err := ps.datasetDB.Exec(q, datasetId, nodeIdentifier, policy.GetContent())
	if err != nil {
  		log.Warnln("Insert error:", err.Error())
	}
	return nil
}

func (ps *PolicyStore) getAllDatasetPolicies() (map[string]*datasetPolicyMapEntry, error) {
	q := `SELECT * FROM ` + schemaName + `.` + datasetPolicyTable + `;`

	rows, err := ps.datasetDB.Query(q)
	if err != nil {
  		log.Errorln(err.Error())
		return nil, err
	}
	defer rows.Close()
	
	datasetPolicyMap := make(map[string]*datasetPolicyMapEntry)

	for rows.Next() {
    	var id, datasetId, nodeIdentifier, allowed string
    	if err := rows.Scan(&id, &datasetId, &nodeIdentifier, &allowed); err != nil {
			log.Errorln(err.Error())
            return nil, err
    	}

		entry := &datasetPolicyMapEntry{
			policy: &pb.Policy{
				Issuer: ps.name,
				ObjectIdentifier: datasetId,
				Content: allowed,
			},
			node: nodeIdentifier,
		}

		datasetPolicyMap[datasetId] = entry
	}
	if err := rows.Err(); err != nil {
		log.Errorln(err.Error())
	    return nil, err
	}
	return datasetPolicyMap, nil
}

func (ps *PolicyStore) allowAccess(id string) error {
	return nil
}

func (ps *PolicyStore) disallowAccess(id string) error {
	return nil
}