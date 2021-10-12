package multicast

import (
	"encoding/base64"
	"context"
	"errors"
	"time"
	log "github.com/sirupsen/logrus"
	pb "github.com/arcsecc/lohpi/protobuf"
)

var dbLogFields = log.Fields{
	"package": "core/policy/multicast",
	"description": "database client",
}


func (m *MulticastManager) dbInsertGossipBatch(addr string, g *pb.GossipMessage, t time.Time, id []byte) error {
	if g == nil {
		return errors.New("Gossip message is nil")
	}

	zone, offset := t.Zone()

	q := `INSERT INTO ` + m.schema + `.` + m.policyDisseminationTable + `
		(recipient, date_sent, policy_store_id, sequence_number, local_timezone, time_offset) VALUES ($1, $2, $3, $4, $5, $6);`

	log.WithFields(dbLogFields).Debugf("Running PSQL query %s", q)

	_, err := m.pool.Exec(context.Background(), q, addr, t.String(), base64.StdEncoding.EncodeToString(id), g.GetGossipMessageID().GetSequenceNumber(), zone, offset)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		return err
	}

	log.WithFields(dbLogFields).Info("Successfully inserted gossip batch message")

	return nil
}

func (m *MulticastManager) dbInsertDirectMessage(id []byte, sequenceNumber int, datasetId string, policyVersion uint64) error {
	q := `INSERT INTO ` + m.schema + `.` + m.directMessageLookupTable + `
		(policy_store_id, sequence_number, dataset_id, policy_version) VALUES ($1, $2, $3, $4);`

	log.WithFields(dbLogFields).Debugf("Running PSQL query %s", q)

	_, err := m.pool.Exec(context.Background(), q, base64.StdEncoding.EncodeToString(id), sequenceNumber, datasetId, policyVersion)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		return err
	}

	log.WithFields(dbLogFields).Info("Successfully inserted a direct message")

	return nil
}