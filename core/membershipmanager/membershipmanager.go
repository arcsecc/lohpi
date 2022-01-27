package membershipmanager

import (
	"context"
	"errors"
	pb "github.com/arcsecc/lohpi/protobuf"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/sirupsen/logrus"
)

var log = logrus.New()

var memshipLogFields = logrus.Fields{
	"package": "membershipmanager",
	"action":  "network membership service",
}

var (
	ErrEmptyInstanceIdentifier = errors.New("No instance ID was provided")
	ErrNilConnectionPool       = errors.New("Connection pool was nil")
	ErrNetworkNodes            = errors.New("Could not fetch network nodes")
	ErrNetworkNode             = errors.New("Could not fetch network node")
	ErrNilNode                 = errors.New("Protobuf node was nil")
	ErrEmptyNodeNameIdentifier = errors.New("No node identifier was provided")
	ErrAddNetworkNode          = errors.New("Inserting node into collection failed")
	ErrRemoveNode              = errors.New("Removing node failed")
	ErrNetworkNodeExists       = errors.New("Failed to check if a network node exists")
	ErrNumNetworkMembers       = errors.New("Error while fetching number of network members")
)

type MembershipManagerUnit struct {
	storageNodeSchema string
	storageNodeTable  string
	pool              *pgxpool.Pool
}

func init() {
	log.SetReportCaller(true)
}

func NewMembershipManager(id string, pool *pgxpool.Pool) (*MembershipManagerUnit, error) {
	if id == "" {
		return nil, ErrEmptyInstanceIdentifier
	}

	if pool == nil {
		return nil, ErrNilConnectionPool
	}

	return &MembershipManagerUnit{
		pool:              pool,
		storageNodeSchema: id + "_schema",
		storageNodeTable:  id + "_storage_node_table",
	}, nil
}

func (m *MembershipManagerUnit) NetworkNodes(ctx context.Context, cursor int, limit int) (*pb.Nodes, error) {
	nodes, err := m.dbSelectNetworkNodes(ctx, cursor, limit)
	if err != nil {
		log.WithFields(memshipLogFields).Error(err.Error())
		return nil, ErrNetworkNodes
	}

	return nodes, nil
}

func (m *MembershipManagerUnit) NetworkNode(ctx context.Context, nodeName string) (*pb.Node, error) {
	if nodeName == "" {
		return nil, ErrEmptyNodeNameIdentifier
	}

	node, err := m.dbSelectNetworkNode(ctx, nodeName)
	if err != nil {
		log.WithFields(memshipLogFields).Error(err.Error())
		return nil, ErrNetworkNode
	}

	return node, nil
}

func (m *MembershipManagerUnit) AddNetworkNode(ctx context.Context, nodeName string, node *pb.Node) error {
	if nodeName == "" {
		return ErrEmptyNodeNameIdentifier
	}

	if node == nil {
		return ErrNilNode
	}

	if err := m.dbInsertNetworkNode(ctx, nodeName, node); err != nil {
		log.WithFields(memshipLogFields).Error(err.Error())
		return ErrAddNetworkNode
	}

	return nil
}

func (m *MembershipManagerUnit) NetworkNodeExists(ctx context.Context, nodeName string) (bool, error) {
	if nodeName == "" {
		return false, ErrEmptyNodeNameIdentifier
	}

	exists, err := m.dbNetworkNodeExists(ctx, nodeName)
	if err != nil {
		log.WithFields(memshipLogFields).Error(err.Error())
		return exists, ErrNetworkNodeExists
	}

	return exists, nil
}

func (m *MembershipManagerUnit) RemoveNetworkNode(ctx context.Context, nodeName string) error {
	if nodeName == "" {
		return ErrEmptyNodeNameIdentifier
	}

	if err := m.dbDeleteNetworkNode(ctx, nodeName); err != nil {
		log.WithFields(memshipLogFields).Error(err.Error())
		return ErrRemoveNode
	}

	return nil
}

func (m *MembershipManagerUnit) Stop() {
	log.WithFields(memshipLogFields).Infoln("Closing database connection pool")
	m.pool.Close()
}

func (m *MembershipManagerUnit) NumNetworkMembers(ctx context.Context) (int, error) {
	n, err := m.dbNumNetworkMembers(ctx)
	if err != nil {
		log.WithFields(memshipLogFields).Error(err.Error())
		return 0, ErrNumNetworkMembers
	}

	return n, nil
}
