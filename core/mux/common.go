package mux

import (
	"firestore/core/message"

_	"fmt"
	"errors"
	"math/rand"
	"reflect"
	"bytes"
	"encoding/gob"
	"encoding/json"
)

/** Contains utility methods */

// Returns true if 'node' is known to the network, returns false otherwise
func (m *Mux) nodeExists(node string) bool {
	if _, ok := m.nodes[node]; ok {
		return true
	}
	return false
}

// Returns true if the mux knows about 'study'. If it does not, it broadcasts a query to the
// Lohpi network asking for the newest lists of studies known to all nodes. If the study is not in the 
// local cache after the update, it returns false
func (m *Mux) studyExists(study string) bool {
	if _, ok := m.studyToNode[study]; ok {
		return true
	}

	// If the study is unknown to the mux, ask the entire network 
	// to push their newest list of studies they store
	// TODO: Run the RPC in parallel
	for nodeName, dest := range m.nodes {
		msg := message.NodeMessage{
			MessageType: 	message.MSG_TYPE_GET_STUDY_LIST,
		}

		jsonStr, err := json.Marshal(msg)
		if err != nil {
			panic(err)
		}
	
		// Request node identfier from the node known to the underlying Ifrit network
		ch := m.ifritClient.SendTo(dest, jsonStr)
		studies := make([]string, 0)
		select {
			case response := <- ch: 
				reader := bytes.NewReader(response)
				dec := gob.NewDecoder(reader)
				if err := dec.Decode(&studies); err != nil {
					panic(err)
				}
		}
	
		// Add the node to the list of studies the node stores 
		m.addNodeToListOfStudies(nodeName, studies)
	}

	// Check the local chache again
	if _, ok := m.studyToNode[study]; ok {
		return true
	}

	return false
}

// Assign 'node' to the list of studies it stores to update the local cache. 
// Please refer to the 'Mux.studyToNode' map. 
func (m *Mux) addNodeToListOfStudies(node string, studies []string) {
	// 'studies' is the list of studies returned directory from the node.
	// First, check if the study is known to the mux. If it isn't, associate the study name (key)
	// with a list of nodes (value) that stores this particular study. 
	LOOP:
	for _, study := range studies {
		// If the study doesn't exist, create a list of nodes that stores this study
		if _, ok := m.studyToNode[study]; !ok {
			m.studyToNode[study] = make([]string, 0)
		}

		// If the node already stores this study, do not add it again
		for _, n := range m.studyToNode[study] {
			if n == node {
				continue LOOP
			}
		}

		// Add the node to the list of nodes who store this study
		m.studyToNode[study] = append(m.studyToNode[study], node)
	}
}

// Returns true if 'node' stores 'study', returns false otherwise
func (m *Mux) nodeStoresStudy(node, study string) bool {
	if !m.studyExists(study) {
		return false
	}

	nodes := m.getStudyNodes(study)
	for _, n := range nodes {
		if n == node {
			return true
		}
	}
	return false
}

// Returns a list of nodes that stores 'study'
func (m *Mux) getStudyNodes(study string) []string {
	storageNodes := make([]string, 0)
	for _, node := range m.studyToNode[study] {
		storageNodes = append(storageNodes, node)
	}
	return storageNodes
}

// Returns the node's string and IP that is selected to monitor access patterns in a given query
func ( m *Mux) getMonitorNode() (string, string) {
	keys := reflect.ValueOf(m.nodes).MapKeys()
	nodeName, ok := keys[rand.Intn(len(keys))].Interface().(string)
	if !ok {
		panic(errors.New("Not a string type"))
	}

	nodeIP, ok := m.nodes[nodeName]
	if !ok {
		panic(errors.New("Not a string type"))
	} 
	return nodeName, nodeIP
}