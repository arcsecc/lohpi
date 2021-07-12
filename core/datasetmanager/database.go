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

	datasetLookupSchema = "dataset_lookup_schema"
	datasetLookupTable  = "dataset_lookup_table"
	storageNodeTable = "storage_node_table"

	datasetStorageSchema = "dataset_storage_schema"
	datasetStorageTable = "dataset_storage_table"

	datasetCheckoutSchema = "dataset_checkout_schema"
	datsetCheckoutTable = "datset_checkout_table"
)

func (d *DatasetLookupService) createSchema(connectionString string) error {
	if connectionString == "" {
		return errNoConnectionString
	}

	q := `CREATE SCHEMA IF NOT EXISTS ` + datasetLookupSchema + `;`
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

	q := `CREATE TABLE IF NOT EXISTS ` + datasetLookupSchema + `.` + datasetLookupTable + ` (
		dataset_id VARCHAR PRIMARY KEY,
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
	q := `INSERT INTO ` + datasetLookupSchema + `.` + datasetLookupTable + `
	(dataset_id, node_id, ip_address, public_id, httpsAddress, port, boottime) VALUES ($1, $2, $3, $4, $5, $6, $7)
	ON CONFLICT (dataset_id) DO NOTHING;`

	_, err := d.datasetLookupDB.Exec(q, 
		datasetId, 
		node.GetName(),
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

func (d *DatasetLookupService) dbRemoveDatasetNode(datasetId string) error {
	if datasetId == ""{
		return fmt.Errorf("Dataset id is empty")
	}

	q := `DELETE FROM ` + datasetLookupSchema + `.` + datasetLookupTable + ` WHERE
		dataset_id='` + datasetId + `';`

	_, err := d.datasetLookupDB.Exec(q)
	if err != nil {
		return err
	}
	
	return nil		
}

func (d *DatasetLookupService) dbGetAllDatasetNodes() (map[string]*pb.Node, error) {
	rows, err := d.datasetLookupDB.Query(`SELECT * FROM ` + datasetLookupSchema + `.` + datasetLookupTable + `;`)
    if err != nil {
        return nil, err
    }
    defer rows.Close()
    
	nodes := make(map[string]*pb.Node)

    for rows.Next() {
        var datasetId, nodeId, ipAddress, publicId, httpsAddress, boottime string
		var port int32
        if err := rows.Scan(&datasetId, &nodeId, &ipAddress, &publicId, &httpsAddress, &port, &boottime); err != nil {
            log.Errorln(err.Error())
			continue
        }

		bTime, err := toTimestamppb(boottime)
		if err != nil {
			return nil, err
		}

		node := &pb.Node{
			Name: nodeId,
			IfritAddress: ipAddress,
			Id: []byte(publicId),
			HttpsAddress: httpsAddress,
			Port: port,
			BootTime: bTime,
		}

		nodes[datasetId] = node
    } 

	return nodes, nil
}

func (d *DatasetServiceUnit) createSchema(connectionString string) error {
	if connectionString == "" {
		return errNoConnectionString
	}

	q := `CREATE SCHEMA IF NOT EXISTS ` + datasetStorageSchema + `;`
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
func (d *DatasetServiceUnit) createDatasetIndexerTable(connectionString string) error {
	if connectionString == "" {
		return errNoConnectionString
	}

	q := `CREATE TABLE IF NOT EXISTS ` + datasetStorageSchema + `.` + datasetStorageTable + ` (
		dataset_id VARCHAR PRIMARY KEY,
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

func (d *DatasetServiceUnit) dbGetAllDatasets() (map[string]*pb.Dataset, error) {
	rows, err := d.datasetIndexDB.Query(`SELECT * FROM ` + datasetStorageSchema + `.` + datasetStorageTable + `;`)
    if err != nil {
        return nil, err
    }
    defer rows.Close()
    
	datasets := make(map[string]*pb.Dataset)

    for rows.Next() {
        var datasetId, policyVersion, dateCreated, dateApplied string
		var policyContent bool
        if err := rows.Scan(&datasetId, &policyContent, &policyVersion, &dateCreated, &dateApplied); err != nil {
            log.Errorln(err.Error())
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

func (d *DatasetServiceUnit) dbRemoveDataset(datasetId string) error {
	if datasetId == ""{
		return fmt.Errorf("Dataset id is empty")
	}

	q := `DELETE FROM ` + datasetStorageSchema + `.` + datasetStorageTable + ` WHERE
		dataset_id='` + datasetId + `';`

	_, err := d.datasetIndexDB.Exec(q)
	if err != nil {
		return err
	}
	
	return nil		
}

func (d *DatasetServiceUnit) dbInsertDataset(datasetId string, dataset *pb.Dataset) error {
	if dataset == nil {
		return fmt.Errorf("Dataset is nil")
	}

	q := `INSERT INTO ` + datasetStorageSchema + `.` + datasetStorageTable + `
	(dataset_id, policy_content, policy_version, date_created, date_applied) VALUES ($1, $2, $3, $4, $5)
	ON CONFLICT (dataset_id) DO UPDATE
	SET 
		policy_content = $2,
		policy_version = $3,
		date_created = $4,
		date_applied = $5
	WHERE ` + datasetStorageSchema + `.` + datasetStorageTable + `.dataset_id = $1;`

	_, err := d.datasetIndexDB.Exec(q, 
		datasetId, 
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

func (d *DatasetServiceUnit) dbInsertDatasetPolicy(datasetId string, policy *pb.Policy) error {
	if policy == nil {
		return fmt.Errorf("Policy is nil")
	}

	q := `INSERT INTO ` + datasetStorageSchema + `.` + datasetStorageTable + `
	(dataset_id, policy_content, policy_version, date_created, date_applied) VALUES ($1, $2, $3, $4, $5)
	ON CONFLICT (dataset_id) DO UPDATE
	SET 
		policy_content = $2,
		policy_version = $3,
		date_created = $4,
		date_applied = $5
	WHERE ` + datasetStorageSchema + `.` + datasetStorageTable + `.dataset_id = $1;`

	_, err := d.datasetIndexDB.Exec(q, 
		datasetId, 
		policy.GetContent(), 
		policy.GetVersion(),
		policy.GetDateCreated().String(),
		policy.GetDateApplied().String())
	if err != nil {
		return err
	}
	
	return nil
}

func toTimestamppb(tString string) (*pbtime.Timestamp, error) {
	if tString == "" {
		return nil, fmt.Errorf("Timestamp string is empty!")
	})
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

func (d *DatasetCheckoutServiceUnit) createSchema(connectionString string) error {
	if connectionString == "" {
		return errNoConnectionString
	}

	q := `CREATE SCHEMA IF NOT EXISTS ` + datasetCheckoutSchema + `;`
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

func (d *DatasetCheckoutServiceUnit) createTable(connectionString string) error {
	if connectionString == "" {
		return errNoConnectionString
	}

	q := `CREATE TABLE IF NOT EXISTS ` + datasetCheckoutSchema + `.` + datsetCheckoutTable + ` (
		id SERIAL PRIMARY KEY,
		dataset_id VARCHAR NOT NULL,
		client_id VARCHAR NOT NULL,	
		client_name VARCHAR NOT NULL,
		mac_address VARCHAR NOT NULL,
		email_address VARCHAR NOT NULL,
		date_checkout VARCHAR NOT NULL
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

	d.datasetCheckoutDB = db
	return nil
}

func (d *DatasetCheckoutServiceUnit) dbGetAllDatasetCheckouts() ([]*pb.DatasetCheckout, error) {
	rows, err := d.datasetCheckoutDB.Query(`SELECT * FROM ` + datasetCheckoutSchema + `.` + datsetCheckoutTable + `;`)
    if err != nil {
        return nil, err
    }
    defer rows.Close()
    
	allCheckouts := make([]*pb.DatasetCheckout, 0)

    for rows.Next() {
        var id, datasetId, macAddress, clientId, clientName, emailAddress, dateCheckout string
		// TODO remove 'id'
        if err := rows.Scan(&id, &datasetId, &clientId, &clientName, &macAddress, &emailAddress, &dateCheckout); err != nil {
            log.Errorln(err.Error())
			continue
        }


		timestamp, err := toTimestamppb(dateCheckout)
		if err != nil {
			return nil, err
		}
		
		datasetCheckout := &pb.DatasetCheckout{
			DatasetIdentifier: datasetId,
			DateCheckout: timestamp,
    		Client: &pb.Client{
				Name: clientName,
    			ID: clientId,
				EmailAddress: emailAddress,
				MacAddress: macAddress,
			},
		}

		allCheckouts = append(allCheckouts, datasetCheckout)
    } 

	return allCheckouts, nil
}

// Note: we dont allow muliple checkouts per device
func (d *DatasetCheckoutServiceUnit) dbCheckoutDataset(datasetId string, co *pb.DatasetCheckout) error {
	if co == nil {
		return fmt.Errorf("Dataset checkout is nil")
	}

	q := `INSERT INTO ` + datasetCheckoutSchema + `.` + datsetCheckoutTable + `
	(dataset_id, client_id, client_name, mac_address, email_address, date_checkout) VALUES ($1, $2, $3, $4, $5, $6);`

	_, err := d.datasetCheckoutDB.Exec(q, 
		datasetId,
		co.GetClient().GetID(),
		co.GetClient().GetName(),
		co.GetClient().GetMacAddress(),
		co.GetClient().GetEmailAddress(),
		co.GetDateCheckout().String())
	return err
}

func (d *DatasetCheckoutServiceUnit) dbDatasetIsCheckedOutByClient(datasetId string, client *pb.Client) (bool, error) {
	if datasetId == "" {
		return false, errors.New("Dataset id cannot be empty")
	}

	if client == nil {
		return false, errors.New("Client is nil")
	}

	var allowed bool
	q := `SELECT EXISTS ( SELECT 1 FROM ` + datasetCheckoutSchema + `.` + datsetCheckoutTable + ` WHERE dataset_id = $1 AND client_id = $2 AND
		client_name = $3 AND mac_address = $4);`
	err := d.datasetCheckoutDB.QueryRow(q, datasetId, client.GetID(), client.GetName(), client.GetMacAddress()).Scan(&allowed)
	if err != nil {
		return false, err
	}
	return allowed, nil
}

func (d *DatasetCheckoutServiceUnit) dbCheckinDataset(datasetId string, client *pb.Client) error {
	if datasetId == "" {
		return errors.New("Dataset id cannot be empty")
	}

	if client == nil {
		return errors.New("Client is nil")
	}
	
	q := `DELETE FROM ` + datasetCheckoutSchema + `.` + datsetCheckoutTable + ` WHERE dataset_id = $1 AND client_id = $2 AND
		client_name = $3 AND mac_address = $4);`

	_, err := d.datasetCheckoutDB.Exec(q, datasetId, client.GetID(), client.GetName(), client.GetMacAddress())
	return err
}