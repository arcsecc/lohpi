package membershipmanager

import (
	"database/sql"
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
	//schemaName           = "nodedbschema"
	membershipSchema = "membership_schema"
	membershipTable  = "membership_table"
)

func (m *MembershipManagerUnit) createSchema(connectionString string) error {
	if connectionString == "" {
		return errNoConnectionString
	}

	q := `CREATE SCHEMA IF NOT EXISTS ` + membershipSchema + `;`
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

// Creates the table in the database that indexes
func (m *MembershipManagerUnit) createMembershipTable(connectionString string) error {
	if connectionString == "" {
		return errNoConnectionString
	}

	q := `CREATE TABLE IF NOT EXISTS ` + membershipSchema + `.` + membershipTable + ` (
		node_id VARCHAR PRIMARY KEY,
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

	m.networkNodeDB = db
	return nil
}

func (m *MembershipManagerUnit) dbAddNetworkNode(nodeId string, node *pb.Node) error {
	if nodeId == "" {
		return fmt.Errorf("Node identifier must not be empty")
	}

	if node == nil {
		return fmt.Errorf("Node is nil")
	}

	q := `INSERT INTO ` + membershipSchema + `.` + membershipTable + `
	(node_id, ip_address, public_id, httpsAddress, port, boottime) VALUES ($1, $2, $3, $4, $5, $6)
	ON CONFLICT (node_id) DO NOTHING;`

	_, err := m.networkNodeDB.Exec(q, 
		nodeId,
		node.GetIfritAddress(), 
		node.GetId(),
		node.GetHttpsAddress(), 
		node.GetPort(), 
		node.GetBootTime().String())
	if err != nil {
		return err // UTF-8 error here!
	}
	
	return nil
}

func (m *MembershipManagerUnit) dbRemoveNetworkNode(nodeId string) error {
	if nodeId == "" {
		return fmt.Errorf("Node id is empty")
	}

	q := `DELETE FROM ` + membershipSchema + `.` + membershipTable + ` WHERE
		node_id='` + nodeId + `';`

	_, err := m.networkNodeDB.Exec(q)
	if err != nil {
		return err
	}
	
	return nil		
}

func (m *MembershipManagerUnit) dbGetAllNetworkNodes() (map[string]*pb.Node, error) {
	rows, err := m.networkNodeDB.Query(`SELECT * FROM ` + membershipSchema + `.` + membershipTable + `;`)
    if err != nil {
        return nil, err
    }
    defer rows.Close()
    
	nodes := make(map[string]*pb.Node)

    for rows.Next() {
        var nodeId, ipAddress, publicId, httpsAddress, boottime string
		var port int32
        if err := rows.Scan(&nodeId, &ipAddress, &publicId, &httpsAddress, &port, &boottime); err != nil {
            log.Errorln(err.Error())
			continue
        }

		rxp := regexp.MustCompile("[0-9]+").FindAllStringSubmatch(boottime, -1)

		secsAsString := rxp[0][0]
		nanosAsString := rxp[1][0]		

		seconds, err := strconv.ParseInt(secsAsString, 10, 64)
		if err != nil {
			log.Errorln(err.Error())
			continue
		}

		nanos, err := strconv.ParseInt(nanosAsString, 10, 64)
		if err != nil {
			log.Errorln(err.Error())
			continue
		}

		node := &pb.Node{
			Name: nodeId,
			IfritAddress: ipAddress,
			Id: []byte(publicId),
			HttpsAddress: httpsAddress,
			Port: port,
			BootTime: pbtime.New(time.Unix(seconds, nanos)),
		}

		nodes[nodeId] = node
    } 

	return nodes, nil
}

/*
func (dm *DatasetIndexerService) dbInsertDatasetIntoTable(dataset *pb.Dataset) error {
	if dataset == nil {
		return errNilPolicy
	}

	q := `INSERT INTO ` + schemaName + `.` + datasetTable + `
	(dataset_id, issuer, version, date_created, content) VALUES ($1, $2, $3, current_timestamp, $4)
	ON CONFLICT (dataset_id)
	DO
		UPDATE SET content = $4;`

	p := dataset.GetPolicy()
	_, err := d.datasetPolicyDB.Exec(q, dataset.GetIdentifier(), p.GetIssuer(), p.GetVersion(), p.GetContent())
	if err != nil {
		log.Errorln("SQL insert error:", err.Error())
	}
	return nil
}

func (dm *DatasetIndexerService) dbInsertDatasetCheckout(d *pb.DatasetCheckout) error {
	if d == nil {
		return errNilCheckout
	}

	return nil
}*/