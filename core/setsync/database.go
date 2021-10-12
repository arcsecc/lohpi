package setsync

import (
	"context"
	"time"
	log "github.com/sirupsen/logrus"
)

var dbLogFields = log.Fields{
	"package": "core/setsync",
	"description": "database client",
}

func (d *SetSyncUnit) dbInsertPolicyReconciliation(nodeName string, timestamp time.Time, numResolved int) error {
	if d.pool == nil {
		log.WithFields(logFields).Error(ErrNoDBPool.Error())
		return ErrNoDBPool
	}

	if nodeName == "" {
		log.WithFields(logFields).Error(ErrNoNodeName.Error())
		return ErrNoNodeName
	}

	if numResolved < 0 {
		log.WithFields(logFields).Error(ErrNegativeDeltaNumber.Error())
		return ErrNegativeDeltaNumber
	}

	q := `INSERT INTO ` + d.schema + `.` + d.policyReconTable + `
	(node_name, timestamp, num_update) VALUES ($1, $2, $3);`

	log.WithFields(dbLogFields).Debugf("Running PSQL query %s", q)

	_, err := d.pool.Exec(context.Background(), q, nodeName, timestamp.String(), numResolved)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		return err
	}

	log.WithFields(dbLogFields).Info("Successfully inserted policy reconciliation result record")

	return nil
}