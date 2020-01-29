package core

import (
	"fmt"
	log "github.com/inconshreveable/log15"
	"github.com/spf13/viper"
	"ifrit"
	"firestore/core/file"
)

type Masternode struct {
	NetworkStorageNodes []*Node		// data storage units encapsulated behind this master node
	Subjects []*Subject			// Subjects known to the network. Dynamic membership
	IfritClient *ifrit.Client
}

func NewMasterNode() (*Masternode, error) {
	numFirestoreNodes := viper.GetInt("firestore_nodes")
	storage_nodes, err := createStorageNodes(numFirestoreNodes)
	if err != nil {
		log.Error("Could not create Firestore storage nodes")
		return nil, err
	}

	ifritClient, err := ifrit.NewClient()
	if err != nil {
		return nil, err
	}

	go ifritClient.Start()
	self := &Masternode {
		NetworkStorageNodes: storage_nodes,
		IfritClient: ifritClient,
	}

	return self, nil
}

// All public API calls change the state as provided by the parameters.
// This is important to keep in mind because the order in which the methods
// are invoked does not matter. Also, remember that the operation are idempotent.

// Permit use to both storage nodes and data users 
func (m *Masternode) PermitToAll(s *Subject) {

}

// Permit only to a subset of data users
func (m *Masternode) PermitDataUsers(s *Subject, users []*Datauser) {

}

// Permit only to a subset of storage nodes
func (m *Masternode) PermitStorageNodes(s *Subject, nodes []*Node) {
	for _, node := range nodes {
		for _, f := range node.NodeSubjectFiles() {
			if f.FileSubject() == s.Name() {
				f.SetPermission(file.FILE_READ)
				
			}
		}
	}
}

// Revoke all usages from storage nodes and data users
func (m *Masternode) RevokeAll(s *Subject) {

}

// Revoke permission only from a subset of data users
func (m *Masternode) RevokeDataUsers(s *Subject, users []*Datauser) {

}

// Revoke permission only from a subset of storage nodes
func (m *Masternode) RevokeStorageNodes(s *Subject, nodes []*Node) {

}

// Returns a list of storage nodes that are permitted to use the data
func (m *Masternode) GetPermittedStorageNodes(s *Subject) ([]*Node) {
	return nil
}

// Returns a list of data users that are permitted to use the data 
func (m *Masternode) GetPermittedDataUsers(s *Subject) ([]*Datauser) {
	return nil
}

func (m *Masternode) StorageNodes() []*Node {
	return m.NetworkStorageNodes
}

func (m *Masternode) StorageNodesNames() []string {
	names := make([]string, 0)
	for _, sn := range m.NetworkStorageNodes {
		names = append(names, sn.Name())
	}
	return names
}

func createStorageNodes(numStorageNodes int) ([]*Node, error) {
	nodes := make([]*Node, 0)

	for i := 0; i < numStorageNodes; i++ {
		nodeID := fmt.Sprintf("storageNode_%d", i + 1)
		node, err := NewNode(nodeID)
		if err != nil {
			log.Error("Could not create storage node")
			return nil, err
		}

		nodes = append(nodes, node)
	}

	return nodes, nil
}

func MasterNodeMessageHandler(data []byte) ([]byte, error) {
	return data, nil
}