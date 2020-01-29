package core

import (
//	"fmt"
//	log "github.com/inconshreveable/log15"
//	"github.com/spf13/viper"
	"ifrit"
//	"firestore/core/file"
)

// In order for us to simulate a read-world scenario, we do not maintain
// any application-like data structures in the masternode. This is because the state of
// the permissions is supposed to be held by the storage nodes and NOT master. Otherwise,
// we could simply delegate the enitre 
type Masternode struct {
	IfritClient *ifrit.Client
}

func NewMasterNode() (*Masternode, error) {
	ifritClient, err := ifrit.NewClient()
	if err != nil {
		return nil, err
	}

	go ifritClient.Start()
	self := &Masternode {
		IfritClient: ifritClient,
	}

	return self, nil
}

// All public API calls change the state as provided by the parameters.
// This is important to keep in mind because the order in which the methods
// are invoked does not matter. Also, remember that the operation are idempotent.

// Permit use to both storage nodes and data users 
func (m *Masternode) BroadcastPermissionToStorageNetwork(s *Subject, permission string) {
	//m.IfritClient.SetGossipContent(yourGossipMsg)
}

// Permit only to a subset of data users
func (m *Masternode) PermitDataUsers(s *Subject, users []*Datauser) {

}

// Permit only to a subset of storage nodes
func (m *Masternode) BroadcastPermission(s *Subject, permission string) {

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

func MasterNodeMessageHandler(data []byte) ([]byte, error) {
	return data, nil
}