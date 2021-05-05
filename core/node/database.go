package node

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/lestrrat-go/jwx/jws"
	_ "github.com/lib/pq"
	log "github.com/sirupsen/logrus"
)

// TODO: change oid to something else? OID comes from Azure AD
type Client struct {
	Name string `json:"name"`
	Oid  string `json:"oid"`
}

type CheckoutInfo struct {
	ClientId   string `json:"client_id"`
	ClientName string `json:"client_name"`
	DatasetId  string `json:"dataset_id"`
	Timestamp  string `json:"timestamp"`
}

// Database-related consts
var (
	dbName               = "dataset_policy_db"
	schemaName           = "nodedbschema"
	datasetPolicyTable   = "policy_table"
	datasetCheckoutTable = "dataset_checkout_table"
)

/* MAJOR TODO: use db.Prepare and friends to prevent SQL injection!!1! */
// Also: make db columns into config variables

// Main entry point for initializing the database schema and its tables on Microsoft Azure
func (n *NodeCore) initializePostgreSQLdb(connectionString string) error {
	if connectionString == "" {
		log.Warnln("Connection string is empty")
	}
	
	// Create schema
	if err := n.createSchema(connectionString); err != nil {
		return err
	}

	// Policy table
	if err := n.initializePolicyTable(connectionString); err != nil {
		return err
	}

	// Client table
	if err := n.initializeDatasetCheckoutTable(connectionString); err != nil {
		return err
	}

	return nil
}

// Creates the table in the database that assigns policies to datasets
// TODO: refine this to perform boolean operations as a temp fix
func (n *NodeCore) initializePolicyTable(connectionString string) error {
	q := `CREATE TABLE IF NOT EXISTS ` + schemaName + `.` + datasetPolicyTable + ` (
		id SERIAL PRIMARY KEY,
		dataset_id VARCHAR(200) NOT NULL UNIQUE, 
		allowed BOOLEAN NOT NULL);`

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

	n.datasetPolicyDB = db
	return nil
}

// Creates the table in the database that tracks which client has checked out datasets
func (n *NodeCore) initializeDatasetCheckoutTable(connectionString string) error {
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

	n.datasetCheckoutDB = db
	return nil
}

// Creates the schema, given the connection string
func (n *NodeCore) createSchema(connectionString string) error {
	log.Println("connectionString:", connectionString)
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

// Sets the policy for the dataset given by the datasetId. The objectAttribute is the policy
// associated with the dataset.
func (n *NodeCore) dbSetObjectPolicy(datasetId, allowed string) error {
	//q := `INSERT INTO ` + schemaName + `.` + datasetPolicyTable + `(` + n.policyIdentifier + `, policyAttributes) VALUES ($1, $2)`
	q := `INSERT INTO ` + schemaName + `.` + datasetPolicyTable + `
	(dataset_id, allowed) VALUES ($1, $2)
	ON CONFLICT (dataset_id) 
	DO
		UPDATE SET allowed = $2;`

	_, err := n.datasetPolicyDB.Exec(q, datasetId, allowed)
	if err != nil {
		log.Errorln("SQL insert error:", err.Error())
	}
	return nil
}

// Returns the policy assoicated with the dataset. If the dataset identifier is not stored in the database,
// it returns an empty string and a nil error.
func (n *NodeCore) dbGetObjectPolicy(datasetId string) (string, error) {
	q := `SELECT * FROM ` + schemaName + `.` + datasetPolicyTable + ` WHERE dataset_id = $1;`

	var id, dataset_id, allowed string

	row := n.datasetPolicyDB.QueryRow(q, datasetId)
	switch err := row.Scan(&id, &dataset_id, &allowed); err {
	case sql.ErrNoRows:
		fmt.Println("No rows were returned!")
	case nil:
		return allowed, nil
	default:
		return "", err
	}
	return "", nil
}

// Returns true if the given subjectAttribute (the attributes that are associated with the clients)
// matches the objectAttribute (the attributes that are assoicated with ) Should be called from ifrit handler
// THIS FUNCTION IS NOT USED
/*func (n *NodeCore) subjectIsAllowedAccess(clientAttribute, objectAttribute string) bool {
	allowed := false
	q := `SELECT EXISTS ( SELECT 1 FROM ` + schemaName + `.` + policyTable + ` WHERE ` + clientAttribute + ` = '` + objectAttribute + `');`
	err := n.datasetPolicyDB.QueryRow(q).Scan(&allowed)
	if err != nil {
		log.Warnln(err.Error())
	}
	return allowed
}*/

/* Returns true if the dataset is publicly available, returns false otherwise.
 * Note: this function looks for the "allowed" attribute only. We need to find a better way to specify policies.
 */
func (n *NodeCore) dbDatasetIsAvailable(id string) bool {
	var allowed bool
	q := `SELECT EXISTS ( SELECT 1 FROM ` + schemaName + `.` + datasetPolicyTable + ` 
		WHERE dataset_id = '` + id + `' AND allowed = 't');`
	err := n.datasetPolicyDB.QueryRow(q).Scan(&allowed)
	if err != nil && err != sql.ErrNoRows {
		log.Errorln("error checking if row exists:", err.Error())
		//log.Warnln(err.Error())
	}
	return allowed
}

// Returns true if the given data object is registered in the database,
// returns false otherwise.
func (n *NodeCore) dbDatasetExists(id string) bool {
	var exists bool
	q := `SELECT EXISTS ( SELECT 1 FROM ` + schemaName + `.` + datasetPolicyTable + ` WHERE dataset_id = '` + id + `');`
	err := n.datasetPolicyDB.QueryRow(q).Scan(&exists)
	if err != nil {
		log.Warnln(err.Error())
	}
	return exists
}

// Returns a (client, dataset, timestamp) tuple that shows the name of the client that checked out
// the daataset at the timestamp (point in time)
/*func (n *NodeCore) dbGetDatasetCheckout(id string) (string, string, string, error) {

}*/

// Returns a list of records displaying the dataset being checked out and
func (n *NodeCore) dbGetCheckoutList(id string) ([]CheckoutInfo, error) {
	q := `SELECT * FROM ` + schemaName + `.` + datasetCheckoutTable + ` WHERE dataset_id='` + id + `';`

	rows, err := n.datasetCheckoutDB.Query(q)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}
	defer rows.Close()

	arr := make([]CheckoutInfo, 0)

	for rows.Next() {
		var id, clientId, clientName, datasetId, timestamp string
		if err := rows.Scan(&id, &clientId, &clientName, &datasetId, &timestamp); err != nil {
			panic(err)
			log.Errorln(err.Error())
			return nil, err
		}

		c := CheckoutInfo{
			ClientId:   clientId,
			ClientName: clientName,
			DatasetId:  datasetId,
			Timestamp:  timestamp,
		}

		arr = append(arr, c)
	}
	if err := rows.Err(); err != nil {
		log.Errorln(err.Error())
		return nil, err
	}
	return arr, nil
}

// TODO: add azure id as parameter
func (n *NodeCore) dbDatasetIsCheckedOutByClient(id string) bool {
	var exists bool
	q := `SELECT EXISTS ( SELECT 1 FROM ` + schemaName + `.` + datasetCheckoutTable + ` WHERE 
		dataset_id = '` + id + `');`
	err := n.datasetPolicyDB.QueryRow(q).Scan(&exists)
	if err != nil {
		log.Warnln(err.Error())
	}
	return exists
}

// TODO: create a table for past checkouts and checkins
func (n *NodeCore) dbCheckinDataset(id string) error {
	q := `DELETE FROM ` + schemaName + `.` + datasetCheckoutTable + ` WHERE
		dataset_id='` + id + `';`

	_, err := n.datasetCheckoutDB.Exec(q)
	if err != nil {
		panic(err)
		return err
	}

	return nil
}

func (n *NodeCore) dbCheckoutDataset(token, datasetId string) error {
	msg, err := jws.ParseString(string(token))
	if err != nil {
		return err
	}

	s := msg.Payload()
	if s == nil {
		return errors.New("Payload was nil")
	}

	c := Client{}
	if err := json.Unmarshal(s, &c); err != nil {
		return err
	}

	clientID := c.Oid
	clientName := c.Name

	q := `INSERT INTO ` + schemaName + `.` + datasetCheckoutTable + `(
		client_id, client_name, dataset_id, tstamp) VALUES ($1, $2, $3, current_timestamp);`

	_, err = n.datasetCheckoutDB.Exec(q, clientID, clientName, datasetId)
	if err != nil {
		return err
	}

	return nil
}

func (n *NodeCore) dbGetDatasetIdentifiers() ([]string, error) {
	q := `SELECT * FROM ` + schemaName + `.` + datasetPolicyTable + `;`

	rows, err := n.datasetCheckoutDB.Query(q)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}
	defer rows.Close()

	arr := make([]string, 0)

	for rows.Next() {
		var id, datasetId, allowed string
		if err := rows.Scan(&id, &datasetId, &allowed); err != nil {
			panic(err)
			log.Errorln(err.Error())
			return nil, err
		}

		arr = append(arr, datasetId)
	}
	if err := rows.Err(); err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	return arr, nil
}

// Should we acutally reset the entire database? Consider an alternative to this solution
// TODO: remove only all fields that were not included in this execution (delete IDS from previous execution)
func (n *NodeCore) dbResetDatasetIdentifiers() error {
	// Delete all records
	q := `DELETE FROM ` + schemaName + `.` + datasetPolicyTable + `;`

	_, err := n.datasetPolicyDB.Exec(q)
	if err != nil {
		log.Errorln(err.Error())
		return err
	}

	// Reset squence counter
	q = `ALTER SEQUENCE ` + schemaName + `.policy_table_id_seq RESTART WITH 1;` // UPDATE ` + schemaName + `.` + datasetPolicyTable + ` SET id = DEFAULT;`
	_, err = n.datasetPolicyDB.Exec(q)
	if err != nil {
		log.Errorln(err.Error())
		return err
	}

	return nil
}