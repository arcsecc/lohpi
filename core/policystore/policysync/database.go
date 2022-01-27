package policysync

import (
	"context"
	log "github.com/sirupsen/logrus"
	"time"
)

var dbLogFields = log.Fields{
	"package":     "core/policystore/policysync",
	"description": "database client",
}

func (p *PolicySyncUnit) dbInsertPolicyReconciliation(nodeName string, timestamp time.Time, numResolved int) error {
	if p.pool == nil {
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

	q := `INSERT INTO ` + p.schema + `.` + p.policyReconTable + `
	(node_name, timestamp, num_update) VALUES ($1, $2, $3);`

	log.WithFields(dbLogFields).Debugf("Running PSQL query %s", q)

	_, err := p.pool.Exec(context.Background(), q, nodeName, timestamp.Format(time.RFC1123Z), numResolved)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		return err
	}

	log.WithFields(dbLogFields).Info("Successfully inserted policy reconciliation result record")

	return nil
}
