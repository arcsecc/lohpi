package datasetmanager

import (
	"context"
	"github.com/jackc/pgx/v4"
	"fmt"
	pb "github.com/arcsecc/lohpi/protobuf"
	_"regexp"
	log "github.com/sirupsen/logrus"
	"time"
	pbtime "google.golang.org/protobuf/types/known/timestamppb"
)

func (d *DatasetLookupService) dbInsertDatasetLookupEntry(datasetId string, nodeName string) error {
	q := `INSERT INTO ` + d.datasetLookupSchema + `.` + d.datasetLookupTable + `
	(dataset_id, node_name) VALUES ($1, $2)
	ON CONFLICT (dataset_id) DO UPDATE	
	SET 
		dataset_id = $1,
		node_name = $2
	WHERE ` + d.datasetLookupSchema + `.` + d.datasetLookupTable + `.dataset_id = $1;`

	log.WithFields(dbLogFields).Infof("Running PSQL query %s\n", q)

	if _, err := d.pool.Exec(context.Background(), q, datasetId, nodeName); err != nil {
		log.WithFields(dbLogFields).Error(err.Error())
		return err
	}
	return nil
}

func (d *DatasetLookupService) dbDatasetNodeExists(datasetId string) bool {
	var exists bool
	q := `SELECT EXISTS ( SELECT 1 FROM ` + d.datasetLookupSchema + `.` + d.datasetLookupTable + ` WHERE dataset_id = $1);`

	log.WithFields(dbLogFields).Infof("Running PSQL query %s\n", q)

	err := d.pool.QueryRow(context.Background(), q, datasetId).Scan(&exists)
	if err != nil {
		log.WithFields(dbLogFields).Error(err.Error())
		return false
	}
	return exists
}

func (d *DatasetLookupService) dbRemoveDatasetNode(datasetId string) error {
	if !d.dbDatasetNodeExists(datasetId) {
		err := fmt.Errorf("No such dataset '%s' exists in the database", datasetId)
		log.WithFields(dbLogFields).Error(err.Error())
		return err
	}

	q := `DELETE FROM ` + d.datasetLookupSchema + `.` + d.datasetLookupTable + ` WHERE dataset_id = $1;`
	log.WithFields(dbLogFields).Infof("Running PSQL query %s\n", q)

	cmdTag, err := d.pool.Exec(context.Background(), q, datasetId)
	if err != nil {
		log.WithFields(dbLogFields).Error(err.Error())
		return err
	}
	
	if cmdTag.RowsAffected() == 0 {
		log.Infoln("Zero rows were affected after deletion of dataset with identifier", datasetId)
	}

	return nil		
}

// TODO finish me
func (d *DatasetLookupService) dbGetAllDatasetNodes() (map[string]*pb.Node, error) {
	panic("dbGetAllDatasetNodes not implemented")
	rows, err := d.pool.Query(context.Background(), `SELECT * FROM ` + d.datasetLookupSchema + `.` + d.datasetLookupTable + `;`)
    if err != nil {
        return nil, err
    }
    defer rows.Close()
    
	nodes := make(map[string]*pb.Node)

    for rows.Next() {
		var id int
        var datasetId, nodeId, ipAddress, httpsAddress, boottime string
		var port int32
		var publicId []byte
        if err := rows.Scan(&id, &datasetId, &nodeId, &ipAddress, &publicId, &httpsAddress, &port, &boottime); err != nil {
			log.WithFields(dbLogFields).Error()
			continue
        }

		bTime, err := time.Parse(time.RFC1123Z, boottime)
		if err != nil {
			log.WithFields(dbLogFields).Error(err.Error())
		}

		node := &pb.Node{
			Name: nodeId,
			IfritAddress: ipAddress,
			Id: publicId,
			HttpsAddress: httpsAddress,
			Port: port,
			BootTime: pbtime.New(bTime),
		}

		nodes[datasetId] = node
    } 

	return nodes, nil
}

func (d *DatasetLookupService) dbSelectDatasetNode(datasetId string) (*pb.Node, error) {
	q := `SELECT ` + 
		d.datasetLookupSchema + `.` + d.storageNodeTable + `.node_name, ` +
		d.datasetLookupSchema + `.` + d.storageNodeTable + `.ip_address, ` + 
		d.datasetLookupSchema + `.` + d.storageNodeTable + `.public_id, ` + 
		d.datasetLookupSchema + `.` + d.storageNodeTable + `.https_address, `  +
		d.datasetLookupSchema + `.` + d.storageNodeTable + `.port, ` +
		d.datasetLookupSchema + `.` + d.storageNodeTable + `.boottime 
		FROM ` + d.datasetLookupSchema + `.` + d.storageNodeTable + ` INNER JOIN ` +
		d.datasetLookupSchema + `.` + d.datasetLookupTable + ` ON (` +
		d.datasetLookupSchema + `.` + d.datasetLookupTable + `.node_name = ` +
		d.datasetLookupSchema + `.` + d.storageNodeTable + `.node_name AND ` + 
		d.datasetLookupSchema + `.` + d.datasetLookupTable + `.dataset_id = $1);`

	log.WithFields(dbLogFields).Infof("Running PSQL query %s\n", q)

	var nodeName, ipAddress, httpsAddress, boottime string
	var port int32
	var publicId []byte

	err := d.pool.QueryRow(context.Background(), q, datasetId).Scan(&nodeName, &ipAddress, &publicId, &httpsAddress, &port, &boottime)
	switch err {
	case pgx.ErrNoRows:
		log.WithFields(dbLogFields).
			WithField("database query", fmt.Sprintf("could not find '%s' among storage nodes", datasetId)).
			Error(err.Error())
		return nil, nil
	case nil:
	default:
		log.WithFields(dbLogFields).Error(err.Error())
		return nil, err
	}

	bTime, err := time.Parse(time.RFC1123Z, boottime)
	if err != nil {
		log.WithFields(dbLogFields).Error(err.Error())
	}

	log.WithFields(dbLogFields).Infof("Successfully selected dataset node with dataset identifier '%s'\n", datasetId)

	return &pb.Node{
		Name: nodeName,
		IfritAddress: ipAddress,
		Id: publicId,
		HttpsAddress: httpsAddress,
		Port: port,
		BootTime: pbtime.New(bTime),
	}, nil
}

// TODO add ranges
func (d *DatasetLookupService) dbSelectDatasetIdentifiers() []string {
	q := `SELECT dataset_id FROM ` + d.datasetLookupSchema + `.` + d.datasetLookupTable + `;`
	log.WithFields(dbLogFields).Infof("Running PSQL query %s\n", q)

	rows, err := d.pool.Query(context.Background(), q)
    if err != nil {
		log.WithFields(dbLogFields).Error(err.Error())
        return nil
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

	log.WithFields(dbLogFields).Info("Successfully selected all dataset identifiers")

	return ids
}

// TODO add ranges
func (d *DatasetLookupService) dbSelectDatasetIdentifiersAtNode(nodeName string) ([]string, error) {
	q := `SELECT dataset_id FROM ` + d.datasetLookupSchema + `.` + d.datasetLookupTable + `
		WHERE node_name = $1;`
	log.WithFields(dbLogFields).Infof("Running PSQL query %s\n", q)

	rows, err := d.pool.Query(context.Background(), q, nodeName)
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
			return nil, err
        }

		ids = append(ids, datasetId)
    } 

	log.WithFields(dbLogFields).Infof("Successfully selected dataset identifiers stored by '%s'", nodeName)

	return ids, nil
}

func (d *DatasetLookupService) dbInsertDatasetIdentifiers(identifiers []string, node *pb.Node) error {
	for _, i := range identifiers {
		q := `INSERT INTO ` + d.datasetLookupSchema + `.` + d.datasetLookupTable + `
		(dataset_id, node_name) VALUES ($1, $2)
		ON CONFLICT (dataset_id) DO UPDATE	
		SET 
			dataset_id = $1,
			node_name = $2
		WHERE ` + d.datasetLookupSchema + `.` + d.datasetLookupTable + `.dataset_id = $1;`

		log.WithFields(dbLogFields).Infof("Running PSQL query %s\n", q)

		if _, err := d.pool.Exec(context.Background(), q, i, node.GetName()); err != nil {
			log.WithFields(dbLogFields).Error(err.Error())
			continue
		}
	}

	return nil
}

// TODO: optimize me!
func (d *DatasetLookupService) dbRemoveDatasetIdentifiers(identifiers []string, node *pb.Node) error {
	for _, i := range identifiers {
		q := `DELETE FROM ` + d.datasetLookupSchema + `.` + d.datasetLookupTable + ` WHERE dataset_id = $1`
		
		cmdTag, err := d.pool.Exec(context.Background(), q, i)
		if err != nil {
			log.WithFields(dbLogFields).Error(err.Error())
			continue
		}

		if cmdTag.RowsAffected() == 0 {
			log.Infoln("Zero rows were affected after deletion of dataset with identifier", i)
		}
	}
	return nil
}