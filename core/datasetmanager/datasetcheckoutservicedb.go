package datasetmanager

import (
	"context"
	pb "github.com/arcsecc/lohpi/protobuf"
	"github.com/jackc/pgx/v4"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

// TODO: add ranges
func (d *DatasetCheckoutServiceUnit) dbGetAllDatasetCheckouts(ctx context.Context) ([]*pb.DatasetCheckout, error) {
	rows, err := d.pool.Query(ctx, `SELECT * FROM `+d.datasetCheckoutSchema+`.`+d.datsetCheckoutTable+`;`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	allCheckouts := make([]*pb.DatasetCheckout, 0)

	for rows.Next() {
		var id, datasetId, clientId, clientName, emailAddress, dateCheckout, dateLatestVerification string
		var clientPolicyVersion uint64
		if err := rows.Scan(&id, &datasetId, &clientId, &clientName, &emailAddress, &dateCheckout, &clientPolicyVersion, &dateLatestVerification); err != nil {
			log.WithFields(dbLogFields).Error(err.Error())
			continue
		}

		checkoutTime, err := time.Parse(time.RFC1123Z, dateCheckout)
		if err != nil {
			log.WithFields(dbLogFields).Error(err.Error())
			continue
		}

		latestVerificationTime, err := time.Parse(time.RFC1123Z, dateLatestVerification)
		if err != nil {
			log.WithFields(checkoutLogFields).Error(err.Error())
			continue
		}

		datasetCheckout := &pb.DatasetCheckout{
			DatasetIdentifier: datasetId,
			DateCheckout:      timestamppb.New(checkoutTime),
			Client: &pb.Client{
				Name:         clientName,
				ID:           clientId,
				EmailAddress: emailAddress,
			},
			ClientPolicyVersion:    clientPolicyVersion,
			DateLatestVerification: timestamppb.New(latestVerificationTime),
		}
		allCheckouts = append(allCheckouts, datasetCheckout)
	}

	return allCheckouts, nil
}

func (d *DatasetCheckoutServiceUnit) dbCheckoutDataset(ctx context.Context, datasetId string, co *pb.DatasetCheckout, systemPolicyVersion uint64, policy bool) error {
	// Insert into global checkout table first. By doing so, we obtain a one-to-many relationship between one dataset and instances of that dataset
	// being checked out.
	q := `INSERT INTO ` + d.datasetCheckoutSchema + `.` + d.datasetCheckoutPolicyTable + ` (dataset_id, system_policy_version, policy, date_applied) VALUES ($1, $2, $3, $4)
	ON CONFLICT (dataset_id) DO UPDATE
	SET
		dataset_id = $1,
		system_policy_version = $2,
		policy = $3, 
		date_applied = $4
	WHERE ` + d.datasetCheckoutSchema + `.` + d.datasetCheckoutPolicyTable + `.dataset_id = $1;`

	_, err := d.pool.Exec(ctx, q, datasetId, systemPolicyVersion, policy, time.Now().Format(time.RFC1123Z))
	if err != nil {
		log.WithFields(checkoutLogFields).Error(err.Error())
		return err
	}

	// Now we can checkout dataset to client
	q = `INSERT INTO ` + d.datasetCheckoutSchema + `.` + d.datsetCheckoutTable + `
		(dataset_id, client_id, client_name, ip_address, email_address, date_checkout, client_policy_version, date_latest_verification) VALUES 
		($1, $2, $3, $4, $5, $6, $7, $8);`

	if client := co.GetClient(); client != nil {
		_, err := d.pool.Exec(ctx, q,
			datasetId,
			client.GetID(),
			client.GetName(),
			client.GetIpAddress(),
			client.GetEmailAddress(),
			co.GetDateCheckout().AsTime().Format(time.RFC1123Z),
			co.GetClientPolicyVersion(),
			co.GetDateLatestVerification().AsTime().Format(time.RFC1123Z))
		if err != nil {
			log.WithFields(checkoutLogFields).Error(err.Error())
			return err
		}
	} else {
		return ErrNilDatasetCheckoutClient
	}

	return nil
}

func (d *DatasetCheckoutServiceUnit) dbDatasetIsCheckedOutByClient(ctx context.Context, datasetId string, client *pb.Client) (bool, error) {
	var exists bool
	q := `SELECT EXISTS ( SELECT 1 FROM ` + d.datasetCheckoutSchema + `.` + d.datsetCheckoutTable + ` WHERE dataset_id = $1 AND client_id = $2 AND
		client_name = $3);`
	if err := d.pool.QueryRow(ctx, q, datasetId, client.GetID(), client.GetName()).Scan(&exists); err != nil {
		log.WithFields(checkoutLogFields).Error(err.Error())
		return false, err
	}
	return exists, nil
}

func (d *DatasetCheckoutServiceUnit) dbDatasetIsCheckedOut(ctx context.Context, datasetId string) (bool, error) {
	var exists bool
	q := `SELECT EXISTS ( SELECT 1 FROM ` + d.datasetCheckoutSchema + `.` + d.datsetCheckoutTable + ` WHERE dataset_id = $1);`

	if err := d.pool.QueryRow(ctx, q, datasetId).Scan(&exists); err != nil {
		log.WithFields(checkoutLogFields).Error(err.Error())
		return false, err
	}
	return exists, nil
}

func (d *DatasetCheckoutServiceUnit) dbCheckinDataset(ctx context.Context, datasetId string, client *pb.Client) error {
	// Delete from the dataset->client checkout table
	q := `DELETE FROM ` + d.datasetCheckoutSchema + `.` + d.datsetCheckoutTable + ` WHERE dataset_id = $1 AND client_id = $2 AND client_name = $3;`

	_, err := d.pool.Exec(ctx, q, datasetId, client.GetID(), client.GetName())
	if err != nil {
		log.WithFields(checkoutLogFields).Error(err.Error())
		return err
	}

	// Completely remove the dataset checkout from the helper table.
	q = `DELETE FROM ` + d.datasetCheckoutSchema + `.` + d.datasetCheckoutPolicyTable + ` WHERE NOT EXISTS ( 
		SELECT dataset_id from ` + d.datasetCheckoutSchema + `.` + d.datsetCheckoutTable + ` WHERE dataset_id = $1);`

	_, err = d.pool.Exec(ctx, q, datasetId)
	if err != nil {
		log.WithFields(checkoutLogFields).Error(err.Error())
		return err
	}

	return nil
}

// inner join...
func (d *DatasetCheckoutServiceUnit) dbSelectDatasetCheckouts(ctx context.Context, dataset string, cursor int, limit int) (*pb.DatasetCheckouts, error) {
	q := `SELECT ` +
		d.datasetCheckoutSchema + `.` + d.datsetCheckoutTable + `.id, ` +
		d.datasetCheckoutSchema + `.` + d.datsetCheckoutTable + `.dataset_id, ` +
		d.datasetCheckoutSchema + `.` + d.datsetCheckoutTable + `.client_id, ` +
		d.datasetCheckoutSchema + `.` + d.datsetCheckoutTable + `.client_name, ` +
		d.datasetCheckoutSchema + `.` + d.datsetCheckoutTable + `.ip_address, ` +
		d.datasetCheckoutSchema + `.` + d.datsetCheckoutTable + `.email_address, ` +
		d.datasetCheckoutSchema + `.` + d.datsetCheckoutTable + `.date_checkout, ` +
		d.datasetCheckoutSchema + `.` + d.datsetCheckoutTable + `.client_policy_version, ` +
		d.datasetCheckoutSchema + `.` + d.datsetCheckoutTable + `.date_latest_verification
	FROM ` + d.datasetCheckoutSchema + `.` + d.datsetCheckoutTable + ` INNER JOIN ` +
		d.datasetCheckoutSchema + `.` + d.datasetCheckoutPolicyTable + ` ON ` +
		d.datasetCheckoutSchema + `.` + d.datsetCheckoutTable + `.dataset_id = ` +
		d.datasetCheckoutSchema + `.` + d.datasetCheckoutPolicyTable + `.dataset_id AND ` +
		d.datasetCheckoutSchema + `.` + d.datsetCheckoutTable + `.dataset_id = $1 WHERE ` +
		d.datasetCheckoutSchema + `.` + d.datsetCheckoutTable + `.id > $2 ORDER BY date_checkout LIMIT $3;`

	rows, err := d.pool.Query(ctx, q, dataset, cursor, limit)
	if err != nil {
		log.WithFields(checkoutLogFields).Error(err.Error())
		return nil, err
	}

	defer rows.Close()

	dsCheckouts := make([]*pb.DatasetCheckout, 0)

	for rows.Next() {
		var clientPolicyVersion, id uint64
		var datasetId, clientId, clientName, ipAddress, emailAddress, dateCheckout, dateLatestVerification string

		if err := rows.Scan(&id, &datasetId, &clientId, &clientName, &ipAddress, &emailAddress, &dateCheckout, &clientPolicyVersion, &dateLatestVerification); err != nil {
			log.WithFields(checkoutLogFields).Error(err.Error())
			continue
		}

		checkoutTime, err := time.Parse(time.RFC1123Z, dateCheckout)
		if err != nil {
			log.WithFields(checkoutLogFields).Error(err.Error())
			continue
		}

		latestVerificationTime, err := time.Parse(time.RFC1123Z, dateLatestVerification)
		if err != nil {
			log.WithFields(checkoutLogFields).Error(err.Error())
			continue
		}

		datasetCheckout := &pb.DatasetCheckout{
			DatasetIdentifier: datasetId,
			DateCheckout:      timestamppb.New(checkoutTime),
			Client: &pb.Client{
				Name:         clientName,
				ID:           clientId,
				EmailAddress: emailAddress,
				IpAddress:    ipAddress,
			},
			ClientPolicyVersion:    clientPolicyVersion,
			DateLatestVerification: timestamppb.New(latestVerificationTime),
		}

		dsCheckouts = append(dsCheckouts, datasetCheckout)
	}

	return &pb.DatasetCheckouts{
		DatasetCheckouts: dsCheckouts,
	}, nil
}

func (d *DatasetCheckoutServiceUnit) dbSetDatasetSystemPolicy(ctx context.Context, datasetId string, version uint64, policy bool) error {
	q := `UPDATE ` + d.datasetCheckoutSchema + `.` + d.datasetCheckoutPolicyTable + ` 
	SET 
		system_policy_version = $2,
		policy = $3,
		date_applied = $4
	WHERE dataset_id = $1;`

	_, err := d.pool.Exec(ctx, q, datasetId, version, policy, time.Now().Format(time.RFC1123Z))
	if err != nil {
		log.WithFields(dbLogFields).Error(err.Error())
		return err
	}

	return nil
}

func (d *DatasetCheckoutServiceUnit) dbGetDatasetSystemPolicy(ctx context.Context, datasetId string) (uint64, bool, error) {
	q := `SELECT system_policy_version, policy FROM ` + d.datasetCheckoutSchema + `.` + d.datasetCheckoutPolicyTable + ` WHERE dataset_id = $1;`
	var version uint64
	var result bool

	if err := d.pool.QueryRow(ctx, q, datasetId).Scan(&version, &result); err != nil {
		if err == pgx.ErrNoRows {
			log.WithFields(dbLogFields).Error(err.Error())
			return 0, result, pgx.ErrNoRows
		}
		log.WithFields(dbLogFields).Error(err.Error())
		return 0, result, err
	}

	return version, result, nil
}

func (d *DatasetCheckoutServiceUnit) dbNumberOfDatasetCheckouts(ctx context.Context, datasetId string) (int, error) {
	q := `SELECT count(id) FROM ` + d.datasetCheckoutSchema + `.` + d.datsetCheckoutTable + ` WHERE dataset_id = $1;`

	var result int
	err := d.pool.QueryRow(ctx, q, datasetId).Scan(&result)
	if err != nil {
		log.WithFields(dbLogFields).Error(err.Error())
		return 0, err
	}

	return result, nil
}
