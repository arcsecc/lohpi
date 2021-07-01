package membershipmanager

import (
	pb "github.com/arcsecc/lohpi/protobuf"
	"sync"
)

type MembershipManagerUnit struct {
	nodesMap map[string]*pb.Node
	nodesMapLock sync.RWMutex
}

func NewMembershipManager() (*MembershipManagerUnit, error) {
	return &MembershipManagerUnit{
		nodesMap: make(map[string]*pb.Node),
	}, nil
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

func (m *MembershipManagerUnit) AddNetworkNode(nodeId string, node *pb.Node) {
	m.nodesMapLock.Lock()
	defer m.nodesMapLock.Unlock()
	m.nodesMap[nodeId] = node
}

func (m *MembershipManagerUnit) NetworkExists(id string) bool {
	m.nodesMapLock.RLock()
	defer m.nodesMapLock.RUnlock()
	_, exists := m.nodesMap[id]
	return exists
}


func (m *MembershipManagerUnit) RemoveNetworkNode(id string) {
	m.nodesMapLock.Lock()
	defer m.nodesMapLock.Unlock()
	delete(m.nodesMap, id)
}