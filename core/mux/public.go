package mux

/* This file contains methods that use the Lohpi network for queries */

import (
	"encoding/json"
	"encoding/gob"
	"fmt"
	"errors"
	"bytes"
	"net/http"

	"firestore/core/message"
)

// Returns human-readable info about 'node'
func (m *Mux) GetNodeInfo(node string) (string, error) {
	msg := message.NodeMessage{
		MessageType: message.MSG_TYPE_GET_NODE_INFO,
	}

	if !m.nodeExists(node) {
		errMsg := fmt.Sprintf("Unknown node: %s", node)
		return "", errors.New(errMsg)
	}

	serialized, err := json.Marshal(msg)
	if err != nil {
		return "", err
	}

	ch := m.ifritClient.SendTo(m.nodes[node], serialized)
	var result string
	select {
		case response := <- ch: 
			result = string(response)
	}
	return result, nil
}

// Orders a node to create test data for it to store dummy data and assoicated data policies
func (m *Mux) StoreNodeData(np *message.NodePopulator) error {
	node := np.Node

	// Ensure that the node is known to the mux
	if !m.nodeExists(node) {
		errMsg := fmt.Sprintf("Unknown node: %s", node)
		return errors.New(errMsg)
	}

	msg := &message.NodeMessage{
		MessageType: 	message.MSG_TYPE_LOAD_NODE,
		Populator: 		np,
	}

	serializedMsg, err := msg.Encode()
	if err != nil {
		return err
	}

	ch := m.ifritClient.SendTo(m.nodes[node], serializedMsg)
	studies := make([]string, 0)
	select {
		case response := <- ch: 
			reader := bytes.NewReader(response)
			dec := gob.NewDecoder(reader)
			if err := dec.Decode(&studies); err != nil {
				return err
			}
	}

	// Add the node to the list of studies the node stores 
	m.addNodeToListOfStudies(np.Node, studies)
	fmt.Printf("Node '%s' now stores study '%s'\n", np.Node, np.MetaData.Meta_data_info.StudyName)
	return nil
}

// Returns a string of study files held by one specific node
func (m *Mux) GetStudyDataFromNode(msg message.NodeMessage) (int, string, error) {
	// Ensure that the node is known to the mux
	if !m.nodeExists(msg.Node) {
		errMsg := fmt.Sprintf("Unknown node: %s", msg.Node)
		return http.StatusNotFound, "", errors.New(errMsg)
	}

	// Ensure that the study is known to the mux
	if !m.studyExists(msg.Study) {
		errMsg := fmt.Sprintf("Unknown study: %s", msg.Study)
		return http.StatusNotFound, "", errors.New(errMsg)
	}

	// Select the monitoring node
	monitoringNodeName, monitoringNodeIP := m.getMonitorNode()
	fmt.Printf("monitoringNodeName, monitoringNodeIP: %s, %s\n", monitoringNodeName, monitoringNodeIP)

	// TODO: What if the monitoring node equals the target node?
	
	// Send monitoring node the query for monitoring
	

	// Set the proper message type and marshall it to json
	msg.MessageType = message.MSG_TYPE_GET_DATA
	serialized, err := json.Marshal(msg)
	if err != nil {
		return http.StatusInternalServerError, "", err
	}	

	ch := m.ifritClient.SendTo(m.nodes[msg.Node], serialized)
	var result string
	select {
		case response := <- ch: 
			result = string(response)
	}

	return http.StatusOK, result, nil
}

// Returns all files in the network assoicated with the given study 
func (m *Mux) GetAllStudyData(msg message.NodeMessage) (int, string, error) {
	return 0, "", nil
}