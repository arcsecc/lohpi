package node

import (
	"database/sql"
	
	log "github.com/sirupsen/logrus"
	_ "github.com/lib/pq"
)

func (n *Node) initializePostgreSQLdb(primaryKey, connectionString string) error {
	q := `CREATE TABLE IF NOT EXISTS ` + schemaName + `.` + policyTable + ` (
		id SERIAL PRIMARY KEY, "` + 
		primaryKey + `" VARCHAR(200) NOT NULL UNIQUE, policyAttributes VARCHAR(200) NOT NULL);`

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

	n.db = db
	return nil 
}

// Sets the policy for the external datasets
func (n *Node) setObjectPolicy(id, objectAttribute string) error {
	q := `INSERT INTO ` + schemaName + `.` + policyTable + `(` + n.pk + `, policyAttributes) VALUES ($1, $2)`
	_, err := n.db.Exec(q, id, objectAttribute)
	if err != nil {
  		log.Println(err.Error())
	}
	return nil
}

func (n *Node) subjectIsAllowedAccess(subjectId, objectId string) bool {
	allowed := false
	q := `SELECT EXISTS ( SELECT 1 FROM ` + schemaName + `.` + policyTable + ` WHERE ` + subjectId + ` = '` + objectId + `');`
	err := n.db.QueryRow(q).Scan(&allowed)
	if err != nil {
		log.Println(err.Error())
	}
	return allowed
}

// Returns true if the given data object is registered in the database, 
// returns false otherwise
func (n *Node) datasetExistsInDb(id string) bool {
	exists := false
	q := `SELECT EXISTS ( SELECT 1 FROM ` + schemaName + `.` + policyTable + ` WHERE ` + n.pk + ` = '` + id + `');`
	err := n.db.QueryRow(q).Scan(&exists)
	if err != nil {
		log.Println(err.Error())
	}
	return exists
}

func (n *Node) removeObjectPolicy(id string) {

}
