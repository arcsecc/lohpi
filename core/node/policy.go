package node

import (
	"database/sql"
	"encoding/json"

	"fmt"
	"github.com/lestrrat-go/jwx/jws"
	pb "github.com/arcsecc/lohpi/protobuf"
	log "github.com/sirupsen/logrus"
	_ "github.com/lib/pq"
)

type Client struct {
	Name string		`json:"name"`
	Oid string		`json:"oid"`
}

func (n *Node) initializePostgreSQLdb(connectionString string) error {
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

func (n *Node) initializePolicyTable(connectionString string) error {
	q := `CREATE TABLE IF NOT EXISTS ` + schemaName + `.` + policyTable + ` (
		id SERIAL PRIMARY KEY, "` + 
		n.policyIdentifier + `" VARCHAR(200) NOT NULL UNIQUE, policyAttributes VARCHAR(200) NOT NULL);`

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

	n.policyDB = db
	return nil
}

func (n *Node) initializeDatasetCheckoutTable(connectionString string) error {
	q := `CREATE TABLE IF NOT EXISTS ` + schemaName + `.` + datasetCheckoutTable + ` (
		id SERIAL PRIMARY KEY, 
		client_id VARCHAR(200) NOT NULL, 
		client_name VARCHAR(200) NOT NULL,
		doi VARCHAR(200) NOT NULL,
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

	n.clientCheckoutTable = db
	return nil
}

func (n *Node) createSchema(connectionString string) error {
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

// Sets the policy for the external datasets
func (n *Node) setObjectPolicy(id, objectAttribute string) error {
	q := `INSERT INTO ` + schemaName + `.` + policyTable + `(` + n.policyIdentifier + `, policyAttributes) VALUES ($1, $2)`
	_, err := n.policyDB.Exec(q, id, objectAttribute)
	if err != nil {
  		log.Println(err.Error())
	}
	return nil
}

func (n *Node) subjectIsAllowedAccess(subjectId, objectId string) bool {
	allowed := false
	q := `SELECT EXISTS ( SELECT 1 FROM ` + schemaName + `.` + policyTable + ` WHERE ` + subjectId + ` = '` + objectId + `');`
	err := n.policyDB.QueryRow(q).Scan(&allowed)
	if err != nil {
		log.Println(err.Error())
	}
	return allowed
}

// Returns true if the given data object is registered in the database, 
// returns false otherwise
func (n *Node) datasetExistsInDb(id string) bool {
	exists := false
	q := `SELECT EXISTS ( SELECT 1 FROM ` + schemaName + `.` + policyTable + ` WHERE ` + n.policyIdentifier + ` = '` + id + `');`
	err := n.policyDB.QueryRow(q).Scan(&exists)
	if err != nil {
		log.Println(err.Error())
	}
	return exists
}

// TODO: implement me
func (n *Node) removeObjectPolicy(id string) {

}

func (n *Node) checkoutDataset(r *pb.DatasetRequest) error {
	token := r.GetClientToken()
	msg, err := jws.ParseString(string(token))
	if err != nil {
		fmt.Printf("failed to parse payload: %s\n", err)
	}

	s := msg.Payload()
	if s == nil {
		return fmt.Errorf("Payload was nil")
	}

	c := Client{}
	if err := json.Unmarshal(s, &c); err != nil {
    	return err
	}

	log.Println("C:", c)
	clientID := c.Oid
	clientName := c.Name
	doi := r.GetIdentifier()

	q := `INSERT INTO ` + schemaName + `.` + datasetCheckoutTable + `(
		client_id, client_name, doi, tstamp) VALUES ($1, $2, $3, current_timestamp);`

	_, err = n.clientCheckoutTable.Exec(q, clientID, clientName, doi)
	if err != nil {
  		log.Println(err.Error())
	}

	return nil
}
