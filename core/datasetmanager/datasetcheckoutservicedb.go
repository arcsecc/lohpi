package datasetmanager

import (
	"context"
	"time"
	pb "github.com/arcsecc/lohpi/protobuf"
	"fmt"
	"errors"
	log "github.com/sirupsen/logrus"
	pbtime "google.golang.org/protobuf/types/known/timestamppb"
)

func (d *DatasetCheckoutServiceUnit) dbGetAllDatasetCheckouts() ([]*pb.DatasetCheckout, error) {
	rows, err := d.pool.Query(context.Background(), `SELECT * FROM ` + d.datasetCheckoutSchema + `.` + d.datsetCheckoutTable + `;`)
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

		checkoutTime, err := time.Parse(time.RFC1123Z, dateCheckout)
		if err != nil {
			log.WithFields(dbLogFields).Error(err.Error())
			continue
		}
		
		datasetCheckout := &pb.DatasetCheckout{
			DatasetIdentifier: datasetId,
			DateCheckout: pbtime.New(checkoutTime),
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
		(dataset_id, client_id, client_name, dns_name, mac_address, ip_address, email_address, date_checkout, policy_version) VALUES 
		($1, $2, $3, $4, $5, $6, $7, $8, $9);`

	_, err := d.pool.Exec(context.Background(), q, 
		datasetId,
		co.GetClient().GetID(),
		co.GetClient().GetName(),
		co.GetClient().GetDNSName(),
		co.GetClient().GetMacAddress(),
		co.GetClient().GetIpAddress(),
		co.GetClient().GetEmailAddress(),
		co.GetDateCheckout().String(),
		co.GetPolicyVersion())
	if err != nil {
		log.WithFields(checkoutLogFields).Error(err.Error())
		return err
	}

	return nil
}

func (d *DatasetCheckoutServiceUnit) dbDatasetIsCheckedOutByClient(datasetId string, client *pb.Client) (bool, error) {
	var exists bool
	q := `SELECT EXISTS ( SELECT 1 FROM ` + d.datasetCheckoutSchema + `.` + d.datsetCheckoutTable + ` WHERE dataset_id = $1 AND client_id = $2 AND
		client_name = $3 AND mac_address = $4);`
	err := d.pool.QueryRow(context.Background(), q, datasetId, client.GetID(), client.GetName(), client.GetMacAddress()).Scan(&exists)
	if err != nil {
		log.WithFields(checkoutLogFields).Error(err.Error())
		return false, err
	}
	return exists, nil
}

func (d *DatasetCheckoutServiceUnit) dbDatasetIsCheckedOut(datasetId string) (bool, error) {
	var exists bool
	q := `SELECT EXISTS ( SELECT 1 FROM ` + d.datasetCheckoutSchema + `.` + d.datsetCheckoutTable + ` WHERE dataset_id = $1);`

	err := d.pool.QueryRow(context.Background(), q, datasetId).Scan(&exists)
	if err != nil {
		log.WithFields(checkoutLogFields).Error(err.Error())
		return false, err
	}
	return exists, nil
}

func (d *DatasetCheckoutServiceUnit) dbCheckinDataset(datasetId string, client *pb.Client) error {
	if datasetId == "" {
		return errors.New("Dataset id cannot be empty")
	}

	if client == nil {
		return errors.New("Client is nil")
	}
	
	q := `DELETE FROM ` + d.datasetCheckoutSchema + `.` + d.datsetCheckoutTable + ` WHERE dataset_id = $1 AND client_id = $2 AND
		client_name = $3 AND mac_address = $4;`

	_, err := d.pool.Exec(context.Background(), q, datasetId, client.GetID(), client.GetName(), client.GetMacAddress())
	if err != nil {
		log.WithFields(checkoutLogFields).Error(err.Error())
		return err
	}

	return nil
}

func (d *DatasetCheckoutServiceUnit) dbSelectDatasetCheckouts(dataset string) (chan *pb.DatasetCheckout, chan error) {
	dsChan := make(chan *pb.DatasetCheckout, 1)
	errChan := make(chan error, 1)	

	go func() {
		defer close(dsChan)
		defer close(errChan)

		rows, err := d.pool.Query(context.Background(), `SELECT * FROM ` + d.datasetCheckoutSchema + `.` + d.datsetCheckoutTable + ` WHERE dataset_id = $1;`)
    	if err != nil {
			log.WithFields(checkoutLogFields).Error(err.Error())
    	    errChan <- err
			return
    	}
    	defer rows.Close()

		for rows.Next() {
			var id uint64
			var datasetId, clientId, clientName, macAddress, emailAddress, dateCheckout string
			if err := rows.Scan(&id, &datasetId, &clientId, &clientName, &macAddress, &emailAddress, &dateCheckout); err != nil {
				log.WithFields(checkoutLogFields).Error(err.Error())
				errChan <- err
				return
			}

			checkoutTime, err := time.Parse(time.RFC1123Z, dateCheckout)
			if err != nil {
				log.WithFields(checkoutLogFields).Error(err.Error())
				errChan <- err
				return
			}
		
			dsChan <- &pb.DatasetCheckout{
				DatasetIdentifier: datasetId,
				DateCheckout: pbtime.New(checkoutTime),
    			Client: &pb.Client{
					Name: clientName,
    				ID: clientId,
					EmailAddress: emailAddress,
					MacAddress: macAddress,
				},
			}
		}
	}()

	return dsChan, errChan
}

	 

	