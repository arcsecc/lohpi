package membershipmanager

import (
	"errors"
	"database/sql"
	pb "github.com/arcsecc/lohpi/protobuf"
	"sync"
)

var (
	errNilConfig            = errors.New("Configuration is nil")
	errNoConnectionString = errors.New("No connection string is provided")
)

type MembershipManagerUnitConfig struct {
	SQLConnectionString string
	UseDB bool
}

type MembershipManagerUnit struct {
	nodesMap map[string]*pb.Node
	nodesMapLock sync.RWMutex
	config *MembershipManagerUnitConfig
	networkNodeDB *sql.DB
}

func NewMembershipManager(config *MembershipManagerUnitConfig) (*MembershipManagerUnit, error) {
	if config == nil {
		return nil, errNilConfig
	}

	m := &MembershipManagerUnit{
		nodesMap: make(map[string]*pb.Node),
		config: config,
	}

	if config.UseDB {
		if config.SQLConnectionString == "" {
			return nil, errNoConnectionString
		}

		if err := m.createSchema(config.SQLConnectionString); err != nil {
			return nil, err
		}

		if err := m.createMembershipTable(config.SQLConnectionString); err != nil {
			return nil, err
		}

		if err := m.reloadMaps(); err != nil {
			return nil, err
		}
	}

	return m, nil
}

func (m *MembershipManagerUnit) NetworkNodes() map[string]*pb.Node {
	m.nodesMapLock.RLock()
	defer m.nodesMapLock.RUnlock()
	return m.nodesMap
}

func (m *MembershipManagerUnit) NetworkNode(nodeId string) *pb.Node {
	m.nodesMapLock.RLock()
	defer m.nodesMapLock.RUnlock()
	return m.nodesMap[nodeId]
}

func (m *MembershipManagerUnit) AddNetworkNode(nodeId string, node *pb.Node) error {
	m.nodesMapLock.Lock()
	defer m.nodesMapLock.Unlock()
	m.nodesMap[nodeId] = node

	if m.networkNodeDB != nil {
		return m.dbAddNetworkNode(nodeId, node)
	}

	return nil
}

func (m *MembershipManagerUnit) NetworkNodeExists(id string) bool {
	m.nodesMapLock.RLock()
	defer m.nodesMapLock.RUnlock()
	_, exists := m.nodesMap[id]
	return exists
}

func (m *MembershipManagerUnit) RemoveNetworkNode(id string) error {
	m.nodesMapLock.Lock()
	defer m.nodesMapLock.Unlock()
	delete(m.nodesMap, id)

	if m.networkNodeDB != nil {
		return m.dbRemoveNetworkNode(id)
	}

	return nil
}

func (m *MembershipManagerUnit) reloadMaps() error {
	maps, err := m.dbGetAllNetworkNodes()
	if err != nil {
		return err
	}

	m.nodesMapLock.Lock()
	defer m.nodesMapLock.Unlock()
	m.nodesMap = maps
	return nil
}
