package policyobserver

import (
	"context"
	"encoding/base64"
	"errors"
	log "github.com/sirupsen/logrus"
	pb "github.com/arcsecc/lohpi/protobuf"
	"time"
	"github.com/jackc/pgx/v4/pgxpool"
)

var (
	ErrNoConfig = errors.New("Configuration is nil")
	ErrNoID = errors.New("ID is empty")
	ErrNoConnectionString = errors.New("Connection string is empty")
	ErrNilGossip = errors.New("Gossip message is nil")
	ErrNoMessageID = errors.New("Gossip message ID is nil")
	ErrFailedInsertGossip = errors.New("Failed to insert gossip message")
	ErrNoPolicy = errors.New("Policy is nil")
)

var observerLogFields = log.Fields{
	"package": "policygossipobserver",
	"description": "gossip observer",
}

type PolicyObserverUnit struct {
	pool *pgxpool.Pool
	gossipObserverSchema string
	gossipObserverTable string
	appliedPolicyTable string
	config *PolicyObserverUnitConfig
}

type PolicyObserverUnitConfig struct {
	SQLConnectionString string
}

type observedGossip struct {
	ArrivedAt 	time.Time
	MessageID	*pb.GossipMessageID
}

func NewPolicyObserverUnit(id string, config *PolicyObserverUnitConfig) (*PolicyObserverUnit, error) {
	if config == nil {
		return nil, ErrNoConfig
	}

	if config.SQLConnectionString == "" {
		return nil, ErrNoConnectionString
	}

	if id == "" {
		return nil, ErrNoID
	}

	// Database connection pool
	poolConfig, err := pgxpool.ParseConfig(config.SQLConnectionString)
	if err != nil {
		log.WithFields(observerLogFields).Error(err.Error())
		return nil, err
	}

	poolConfig.MaxConnLifetime = time.Second * 10
	poolConfig.MaxConnIdleTime = time.Second * 4
	poolConfig.MaxConns = 100
	poolConfig.HealthCheckPeriod = time.Second * 1
	poolConfig.LazyConnect = false

	pool, err := pgxpool.ConnectConfig(context.Background(), poolConfig)
	if err != nil {
		log.WithFields(observerLogFields).Error(err.Error())
		return nil, err
	}

	po := &PolicyObserverUnit{
		pool: pool,
		config: config,			
		gossipObserverSchema: id + "_schema",
		gossipObserverTable: id + "_gossip_observation_table",
		appliedPolicyTable: id + "_applied_policy_table",
	}

	return po, nil
}

// Returns true if the gossip message has already been observed, returns false otherwise
func (p *PolicyObserverUnit) GossipIsObserved(msg *pb.GossipMessage) bool {
	if msg == nil {
		log.WithFields(observerLogFields).Error(ErrNilGossip.Error())
		return false
	}

	id := msg.GetGossipMessageID()
	if id == nil {
		log.WithFields(observerLogFields).Error(ErrNoMessageID.Error())
		return false
	}
	
	var exists bool
	q := `SELECT EXISTS ( SELECT 1 FROM ` + p.gossipObserverSchema + `.` + p.gossipObserverTable + ` WHERE
	sequence_number = $1 AND policy_store_id = $2 AND date_sent = $3);`

	err := p.pool.QueryRow(context.Background(), q, 
		id.GetSequenceNumber(), 
		id.GetPolicyStoreID(), 
		msg.GetDateSent().AsTime().Format(time.RFC1123Z)).Scan(&exists)
	if err != nil {
		log.WithFields(observerLogFields).Error(err.Error())
		return false
	}
	return exists
}

// Inserts the gossip message into the log of observed gossips
// TODO: time formats should be the same!
func (p *PolicyObserverUnit) InsertObservedGossip(msg *pb.GossipMessage) error {
	if msg == nil {
		return ErrNoMessageID
	}

	id := msg.GetGossipMessageID()
	if id == nil {
		return ErrNoMessageID
	}

	dateSent := msg.GetDateSent().AsTime().Format(time.RFC1123Z)

	q := `INSERT INTO ` + p.gossipObserverSchema + `.` + p.gossipObserverTable + `
	(sequence_number, date_received, policy_store_id, date_sent) VALUES ($1, $2, $3, $4);`

	_, err := p.pool.Exec(context.Background(), q, 
		id.GetSequenceNumber(), 
		time.Now().String(),
		base64.StdEncoding.EncodeToString(id.GetPolicyStoreID()),
		dateSent)
	if err != nil {
		log.WithFields(observerLogFields).Error(err.Error())
		return ErrFailedInsertGossip
	}

	log.WithFields(observerLogFields).Info("Succsessfully inserted gossip observation")

	return nil
}

func (p *PolicyObserverUnit) InsertAppliedPolicy(policy *pb.Policy) error {
	if policy == nil {
		return ErrNoPolicy
	}

	q := `INSERT INTO ` + p.gossipObserverSchema + `.` + p.appliedPolicyTable + `
	(dataset_id, policy_version, date_created, date_updated, date_applied) VALUES ($1, $2, $3, $4, $5);`

	_, err := p.pool.Exec(context.Background(), q, 
		policy.GetDatasetIdentifier(),
		policy.GetVersion(), 
		policy.GetDateCreated().String(),
		policy.GetDateUpdated().String(),
		time.Now().Format(time.RFC1123Z))
	if err != nil {
		log.WithFields(observerLogFields).Error(err.Error())
		return ErrFailedInsertGossip
	}
	
	log.WithFields(observerLogFields).Info("Succsessfully inserted policy change")
		
	return nil
}

func (p *PolicyObserverUnit) InsertPolicyGossip(policy *pb.Policy) error {
	return nil
}