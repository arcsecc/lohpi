package datasetmanager

import (
	"database/sql"
	"errors"
	"fmt"
	pb "github.com/arcsecc/lohpi/protobuf"
	_"regexp"
	log "github.com/sirupsen/logrus"
	_ "github.com/lib/pq"
	//pbtime "google.golang.org/protobuf/types/known/timestamppb"
)

func (d *DatasetLookupService) createSchema(connectionString string) error {
	if connectionString == "" {
		return errNoConnectionString
	}

	q := `CREATE SCHEMA IF NOT EXISTS ` + d.datasetLookupSchema + `;`
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

func (d *DatasetLookupService) createDatasetLookupTable(connectionString string) error {
	if connectionString == "" {
		return errNoConnectionString
	}

	q := `CREATE TABLE IF NOT EXISTS ` + d.datasetLookupSchema + `.` + d.datasetLookupTable + ` (
		id SERIAL PRIMARY KEY,
		dataset_id VARCHAR NOT NULL UNIQUE,
		node_id VARCHAR NOT NULL,
		ip_address VARCHAR NOT NULL,
		public_id BYTEA NOT NULL,
		httpsAddress VARCHAR NOT NULL,
		port INT NOT NULL,
		boottime VARCHAR NOT NULL
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

	d.datasetLookupDB = db
	return nil
}

func (d *DatasetLookupService) dbInsertDatasetNode(datasetId string, node *pb.Node) error {
	if node == nil {
		return fmt.Errorf("Node is nil")
	}

	// TODO: update all fields
	q := `INSERT INTO ` + d.datasetLookupSchema + `.` + d.datasetLookupTable + `
	(dataset_id, node_id, ip_address, public_id, httpsAddress, port, boottime) VALUES ($1, $2, $3, $4, $5, $6, $7)
	ON CONFLICT (dataset_id) DO UPDATE
	SET 
		node_id = $2,
		ip_address = $3,
		public_id = $4,
		httpsAddress = $5,
		port = $6,
		boottime = $7
	WHERE ` + d.datasetLookupSchema + `.` + d.datasetLookupTable + `.dataset_id = $1;`

	_, err := d.datasetLookupDB.Exec(q, 
		datasetId, 
		node.GetName(),
		node.GetIfritAddress(), 
		node.GetId(),
		node.GetHttpsAddress(), 
		node.GetPort(), 
		node.GetBootTime().String())
	if err != nil {
		panic(err)
		return err // UTF-8 error here!
	}
	
	return nil
}

func (d *DatasetLookupService) dbDatasetNodeExists(datasetId string) bool {
	if datasetId == "" {
		log.Errorln("Dataset id must not be empty")
		return false
	}

	var exists bool
	q := `SELECT EXISTS ( SELECT 1 FROM ` + d.datasetLookupSchema + `.` + d.datasetLookupTable + `
			WHERE dataset_id = '` + datasetId + `');`
	err := d.datasetLookupDB.QueryRow(q).Scan(&exists)
	if err != nil {
		panic(err)
		log.Errorln(err.Error())
	}
	return exists
}

func (d *DatasetLookupService) dbRemoveDatasetNode(datasetId string) error {
	if datasetId == ""{
		return fmt.Errorf("Dataset id is empty")
	}

	if !d.dbDatasetNodeExists(datasetId) {
		return fmt.Errorf("SQL: No such dataset '%s' was deleted", datasetId)
	}

	q := `DELETE FROM ` + d.datasetLookupSchema + `.` + d.datasetLookupTable + ` WHERE
		dataset_id='` + datasetId + `';`

	r, err := d.datasetLookupDB.Exec(q)
	if err != nil {
		panic(err)
		return err
	}
	
	n, err := r.RowsAffected()
	if err != nil {
		return err
	}

	if n == 0 {
		return errors.New("Zero rows were affected after deletion")
	}

	return nil		
}

func (d *DatasetLookupService) dbGetAllDatasetNodes() (map[string]*pb.Node, error) {
	rows, err := d.datasetLookupDB.Query(`SELECT * FROM ` + d.datasetLookupSchema + `.` + d.datasetLookupTable + `;`)
    if err != nil {
        return nil, err
    }
    defer rows.Close()
    
	nodes := make(map[string]*pb.Node)

    for rows.Next() {
        var id, datasetId, nodeId, ipAddress, httpsAddress, boottime string
		var port int32
		var publicId []byte
        if err := rows.Scan(&id, &datasetId, &nodeId, &ipAddress, &publicId, &httpsAddress, &port, &boottime); err != nil {
            log.Errorln(err.Error())
			panic(err)
			continue
        }

		bTime, err := toTimestamppb(boottime)
		if err != nil {
			return nil, err
		}

		node := &pb.Node{
			Name: nodeId,
			IfritAddress: ipAddress,
			Id: publicId,
			HttpsAddress: httpsAddress,
			Port: port,
			BootTime: bTime,
		}

		nodes[datasetId] = node
    } 

	return nodes, nil
}

func (d *DatasetLookupService) dbSelectDatasetNode(datasetId string) *pb.Node {
	if datasetId == "" {
		err := errors.New("Dataset identifier cannot be empty")
		log.Error(err.Error())
		return nil
	}

	q := `SELECT dataset_id, node_id, ip_address, public_id, httpsAddress, port, boottime FROM ` + d.datasetLookupSchema + `.` + d.datasetLookupTable + ` WHERE dataset_id = $1 ;`
	
	var dataset, nodeId, ipAddress, httpsAddress, boottime string
	var port int32
	var publicId []byte

	err := d.datasetLookupDB.QueryRow(q, datasetId).Scan(&dataset, &nodeId, &ipAddress, &publicId, &httpsAddress, &port, &boottime)
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
	
	bTime, err := toTimestamppb(boottime)
	if err != nil {
		log.Errorln(err.Error())
		return nil
	}

	return &pb.Node{
		Name: nodeId,
		IfritAddress: ipAddress,
		Id: publicId,
		HttpsAddress: httpsAddress,
		Port: port,
		BootTime: bTime,
	}
}

func (d *DatasetLookupService) dbSelectDatasetIdentifiers() []string {
	rows, err := d.datasetLookupDB.Query(`SELECT dataset_id FROM ` + d.datasetLookupSchema + `.` + d.datasetLookupTable + `;`)
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
			panic(err)

			continue
        }

		ids = append(ids, datasetId)
    } 

	return ids
}

