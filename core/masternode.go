package core

import (
	"fmt"
	log "github.com/inconshreveable/log15"
	"github.com/spf13/viper"
)

type Masternode struct {
	NetworkStorageNodes []*Node		// data storage units encapsulated behind this master node
	Subjects []*Subject			// Subjects known to the network. Dynamic membership
}

func NewMasterNode() (*Masternode, error) {
	numFirestoreNodes := viper.GetInt("firestore_nodes")
	storage_nodes, err := createStorageNodes(numFirestoreNodes)
	if err != nil {
		log.Error("Could not create Firestore storage nodes")
		return nil, err
	}

	self := &Masternode {
		NetworkStorageNodes: storage_nodes,
	}

	return self, nil
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

func (m *Masternode) StorageNodes() []*Node {
	return m.NetworkStorageNodes
}