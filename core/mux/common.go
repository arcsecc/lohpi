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

func (m *Mux) studyExistsInCache(study string) bool {
	if _, ok := m.studyNode[study]; ok {
		return true
	}
	return false
}

// TODO: use go-routines to speed up the iterations
func (m *Mux) FetchStudyIDs() {
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
		m.updateStudyCache(nodeName, studies)
	}
}

// Assign 'node' to the list of studies it stores to update the local cache. 
// Please refer to the 'Mux.studyNode' map. 
func (m *Mux) updateStudyCache(node string, studies []string) {
	for _, study := range studies {
		// If the study doesn't exist, assign the study to the node
		if _, ok := m.studyNode[study]; !ok {
			m.studyNode[study] = node
		}
	}
}

// Returns true if 'node' stores 'study', returns false otherwise
func (m *Mux) studyInNode(node, study string) bool {
	/*
	if !m.studyExists(study) {
		return false
	}

	nodes := m.getStudyNodes(study)
	for _, n := range nodes {
		if n == node {
			return true
		}
	}*/
	return false
}

// Returns a list of nodes that stores 'study'
func (m *Mux) getStudyNodes(study string) []string {
	storageNodes := make([]string, 0)
	/*for _, node := range m.studyNode[study] {
		storageNodes = append(storageNodes, node)
	}*/
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