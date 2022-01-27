package policyobserver

import (
	"context"
	"encoding/base64"
	"errors"
	pb "github.com/arcsecc/lohpi/protobuf"
	"github.com/jackc/pgx/v4/pgxpool"
	log "github.com/sirupsen/logrus"
	"time"
)

var (
	ErrNoID               = errors.New("ID is empty")
	ErrNoGossip           = errors.New("Gossip message is nil")
	ErrNoMessageID        = errors.New("Gossip message ID is nil")
	ErrFailedInsertGossip = errors.New("Failed to insert gossip message")
	ErrNoPolicy           = errors.New("Policy is nil")
	ErrGossipExists       = errors.New("Failed to check if gossip exists")
	ErrNoPool             = errors.New("Pool is nil")
)

var observerLogFields = log.Fields{
	"package":     "policygossipobserver",
	"description": "gossip observer",
}

type PolicyObserverUnit struct {
	pool                 *pgxpool.Pool
	gossipObserverSchema string
	gossipObserverTable  string
	appliedPolicyTable   string
}

type observedGossip struct {
	ArrivedAt time.Time
	MessageID *pb.GossipMessageID
}

func NewPolicyObserverUnit(id string, pool *pgxpool.Pool) (*PolicyObserverUnit, error) {
	if id == "" {
		log.WithFields(observerLogFields).Error(ErrNoID.Error())
		return nil, ErrNoID
	}

	if pool == nil {
		log.WithFields(observerLogFields).Error(ErrNoPool.Error())
		return nil, ErrNoPool
	}

	po := &PolicyObserverUnit{
		pool:                 pool,
		gossipObserverSchema: id + "_schema",
		gossipObserverTable:  id + "_gossip_observation_table",
		appliedPolicyTable:   id + "_applied_policy_table",
	}

	return po, nil
}

// Returns true if the gossip message has already been observed, returns false otherwise
func (p *PolicyObserverUnit) GossipIsObserved(ctx context.Context, msg *pb.PolicyGossipUpdate) (bool, error) {
	if msg == nil {
		log.WithFields(observerLogFields).Error(ErrNoGossip.Error())
		return false, ErrNoGossip
	}

	id := msg.GetGossipMessageID()
	if id == nil {
		log.WithFields(observerLogFields).Error(ErrNoMessageID.Error())
		return false, ErrNoMessageID
	}

	var exists bool
	q := `SELECT EXISTS ( SELECT 1 FROM ` + p.gossipObserverSchema + `.` + p.gossipObserverTable + ` WHERE
	sequence_number = $1 AND policy_store_id = $2 AND date_sent = $3);`

	if err := p.pool.QueryRow(context.Background(), q, id.GetSequenceNumber(), id.GetPolicyStoreID(), msg.GetDateSent().AsTime().Format(time.RFC1123Z)).Scan(&exists); err != nil {
		log.WithFields(observerLogFields).Error(err.Error())
		return false, ErrGossipExists
	}

	log.WithFields(observerLogFields).Info("Succsessfully checked if gossip has been observed")

	return exists, nil
}

// Inserts the gossip message into the log of observed gossips
func (p *PolicyObserverUnit) InsertObservedGossip(ctx context.Context, msg *pb.PolicyGossipUpdate) error {
	if msg == nil {
		log.WithFields(observerLogFields).Error(ErrNoGossip.Error())
		return ErrNoGossip
	}

	id := msg.GetGossipMessageID()
	if id == nil {
		log.WithFields(observerLogFields).Error(ErrNoMessageID.Error())
		return ErrNoMessageID
	}

	q := `INSERT INTO ` + p.gossipObserverSchema + `.` + p.gossipObserverTable + `
	(sequence_number, date_received, policy_store_id, date_sent) VALUES ($1, $2, $3, $4);`

	_, err := p.pool.Exec(context.Background(), q,
		id.GetSequenceNumber(),
		time.Now().Format(time.RFC1123Z),
		base64.StdEncoding.EncodeToString(id.GetPolicyStoreID()),
		msg.GetDateSent().AsTime().Format(time.RFC1123Z))
	if err != nil {
		log.WithFields(observerLogFields).Error(err.Error())
		return ErrFailedInsertGossip
	}

	log.WithFields(observerLogFields).Info("Succsessfully inserted gossip observation")

	return nil
}

func (p *PolicyObserverUnit) InsertAppliedPolicy(ctx context.Context, policy *pb.Policy) error {
	if policy == nil {
		log.WithFields(observerLogFields).Error(ErrNoPolicy.Error())
		return ErrNoPolicy
	}

	q := `INSERT INTO ` + p.gossipObserverSchema + `.` + p.appliedPolicyTable + `
	(dataset_id, policy_version, date_created, date_updated, date_applied) VALUES ($1, $2, $3, $4, $5);`

	_, err := p.pool.Exec(context.Background(), q,
		policy.GetDatasetIdentifier(),
		policy.GetVersion(),
		policy.GetDateCreated().AsTime().Format(time.RFC1123Z),
		policy.GetDateUpdated().AsTime().Format(time.RFC1123Z),
		time.Now().Format(time.RFC1123Z))
	if err != nil {
		log.WithFields(observerLogFields).Error(err.Error())
		return ErrFailedInsertGossip
	}

	log.WithFields(observerLogFields).Info("Succsessfully inserted policy change")

	return nil
}
