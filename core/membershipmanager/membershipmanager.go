package membershipmanager

import (
	"errors"
	pb "github.com/arcsecc/lohpi/protobuf"
	"github.com/jackc/pgx/v4/pgxpool"
	"time"
	"github.com/sirupsen/logrus"
_	"os"
	"context"
)

var (
	errNilConfig            = errors.New("Configuration is nil")
	errNoConnectionString = errors.New("No connection string is provided")
)

var log = logrus.New()

var mainLogFields = logrus.Fields{
	"package": "membershipmanager",
	"action": "network membership handling",
}

var (
	ErrInsertNode = errors.New("Inserting node failed")
	ErrRemoveNode = errors.New("Removing node failed")
)

type MembershipManagerUnitConfig struct {
	SQLConnectionString string
	UseDB bool
}

type MembershipManagerUnit struct {
	config *MembershipManagerUnitConfig
	storageNodeSchema string
	storageNodeTable string
	pool *pgxpool.Pool
}

func init() { 
	log.SetReportCaller(true)
}

func NewMembershipManager(id string, config *MembershipManagerUnitConfig) (*MembershipManagerUnit, error) {
	if config == nil {
		return nil, errNilConfig
	}

	if config.SQLConnectionString == "" {
		return nil, errNoConnectionString
	}
	
	poolConfig, err := pgxpool.ParseConfig(config.SQLConnectionString)
	if err != nil {
		log.WithFields(mainLogFields).Error(err.Error())
		return nil, err
	}

	poolConfig.MaxConnLifetime = time.Second * 10
	poolConfig.MaxConnIdleTime = time.Second * 4
	poolConfig.MaxConns = 100
	poolConfig.HealthCheckPeriod = time.Second * 1
	poolConfig.LazyConnect = false

	pool, err := pgxpool.ConnectConfig(context.Background(), poolConfig)
	if err != nil {
		log.WithFields(mainLogFields).Error(err.Error())
		return nil, err
	}

	m := &MembershipManagerUnit{
		pool: pool,
		config: config,
		storageNodeSchema: id + "_schema",
		storageNodeTable: id + "_storage_node_table",
	}

	// redis?

	return m, nil
}

func (m *MembershipManagerUnit) NetworkNodes() map[string]*pb.Node {
	nodes, err := m.dbSelectAllNetworkNodes()
	if err != nil {
		return nil
	}
	return nodes
}

func (m *MembershipManagerUnit) NetworkNode(nodeId string) *pb.Node {
	node, err := m.dbSelectNetworkNode(nodeId)
	if err != nil {
		return nil
	}
	return node
}

func (m *MembershipManagerUnit) AddNetworkNode(nodeId string, node *pb.Node) error {
	if err := m.dbInsertNetworkNode(nodeId, node); err != nil {
		return ErrInsertNode
	}
	return nil
}

func (m *MembershipManagerUnit) NetworkNodeExists(id string) bool {
	exists, err := m.dbNetworkNodeExists(id)
	if err != nil {
		return exists
	}
	return exists
}

func (m *MembershipManagerUnit) RemoveNetworkNode(id string) error {
	if err := m.dbDeleteNetworkNode(id); err != nil {
		return ErrRemoveNode
	}
	return nil
}

func (m *MembershipManagerUnit) Stop() {
	log.WithFields(mainLogFields).Infoln("Closing database connection pool")
	m.pool.Close()
}