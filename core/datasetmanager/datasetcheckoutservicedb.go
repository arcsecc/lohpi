package datasetmanager

import (
	"database/sql"
	pb "github.com/arcsecc/lohpi/protobuf"
	log "github.com/sirupsen/logrus"
	"fmt"
	"errors"
)


func (d *DatasetCheckoutServiceUnit) createSchema(connectionString string) error {
	if connectionString == "" {
		return errNoConnectionString
	}

	q := `CREATE SCHEMA IF NOT EXISTS ` + d.datasetCheckoutSchema + `;`
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

	q := `CREATE TABLE IF NOT EXISTS ` + d.datasetCheckoutSchema + `.` + d.datsetCheckoutTable + ` (
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
	rows, err := d.datasetCheckoutDB.Query(`SELECT * FROM ` + d.datasetCheckoutSchema + `.` + d.datsetCheckoutTable + `;`)
    if err != nil {
        return nil, err
    }
    defer rows.Close()
    
	allCheckouts := make([]*pb.DatasetCheckout, 0)

    for rows.Next() {
        var id, datasetId, macAddress, clientId, clientName, emailAddress, dateCheckout string
        if err := rows.Scan(&id, &datasetId, &clientId, &clientName, &macAddress, &emailAddress, &dateCheckout); err != nil {
			panic(err)
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

	q := `INSERT INTO ` + d.datasetCheckoutSchema + `.` + d.datsetCheckoutTable + `
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
	q := `SELECT EXISTS ( SELECT 1 FROM ` + d.datasetCheckoutSchema + `.` + d.datsetCheckoutTable + ` WHERE dataset_id = $1 AND client_id = $2 AND
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
	
	q := `DELETE FROM ` + d.datasetCheckoutSchema + `.` + d.datsetCheckoutTable + ` WHERE dataset_id = $1 AND client_id = $2 AND
		client_name = $3 AND mac_address = $4);`

	_, err := d.datasetCheckoutDB.Exec(q, datasetId, client.GetID(), client.GetName(), client.GetMacAddress())
	return err
}

func (d *DatasetCheckoutServiceUnit) dbSelectDatasetCheckout(dataset string, client *pb.Client) (*pb.DatasetCheckout, error) {
	q := `SELECT dataset_id, client_id, client_name, mac_address, email_address, date_checkout  FROM ` + d.datasetCheckoutSchema + `.` + d.datsetCheckoutTable + `
		WHERE dataset_id = $1 AND client_id = $2 AND mac_address = $3;`
	
	var datasetId, clientId, clientName, macAddress, emailAddress, dateCheckout string
	err := d.datasetCheckoutDB.QueryRow(q, 
			dataset, 
			client.GetID(), 
			client.GetName(), 
			client.GetMacAddress()).Scan(&datasetId, &clientId, &clientName, &macAddress, &emailAddress, &dateCheckout)
    if err != nil {
        return nil, err
    }
    
	switch err {
	case sql.ErrNoRows:
		log.Infoln("No rows were returned")
		return nil, err
  	case nil:
  	default:
		panic(err)
		log.Error(err.Error())
		return nil, err
  	}

	timestamp, err := toTimestamppb(dateCheckout)
	if err != nil {
		return nil, err
	}
		
	return &pb.DatasetCheckout{
		DatasetIdentifier: datasetId,
		DateCheckout: timestamp,
    	Client: &pb.Client{
			Name: clientName,
    		ID: clientId,
			EmailAddress: emailAddress,
			MacAddress: macAddress,
		},
	}, nil
}

	 

	