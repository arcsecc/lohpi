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
func (m *Mux) _getNodeInfo(node string) (string, error) {
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
func (m *Mux) _storeNodeData(np *message.NodePopulator) error {
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

// Given a node identifier and a study name, return the data at that node
func (m *Mux) _getStudyData(msg message.NodeMessage) (int, string, error) {
	// Ensure that the node is known to the mux
	if !m.nodeExists(msg.Node) {
		errMsg := fmt.Sprintf("Unknown node: %s", msg.Node)
		return http.StatusNotFound, "", errors.New(errMsg)
	}

	// Ensure that the node stores the study
	if !m.nodeStoresStudy(msg.Node, msg.Study) {
		errMsg := fmt.Sprintf("Study %s is not stored in %s", msg.Study, msg.Node)
		return http.StatusNotFound, "", errors.New(errMsg)
	}

	// Set the proper message type and marshall it to json
	msg.MessageType = message.MSG_TYPE_GET_DATA
	serializedMsg, err := msg.Encode()
	if err != nil {
		return http.StatusInternalServerError, "", err
	}

	ch := m.ifritClient.SendTo(m.nodes[msg.Node], serializedMsg)
	var result string
	select {
		case response := <- ch: 
			result = string(response)
	}

	return http.StatusOK, result, nil
}

// Given a node identifier and a study name, return the meta-data about a particular study at that node.
func (m *Mux) _getMetaData(msg message.NodeMessage) (int, string, error) {
	// Ensure that the node is known to the mux
	if !m.nodeExists(msg.Node) {
		errMsg := fmt.Sprintf("Unknown node: %s", msg.Node)
		return http.StatusNotFound, "", errors.New(errMsg)
	}

	// Ensure that the node stores the study
	if !m.nodeStoresStudy(msg.Node, msg.Study) {
		errMsg := fmt.Sprintf("Study %s is not stored in %s", msg.Study, msg.Node)
		return http.StatusNotFound, "", errors.New(errMsg)
	}

	// Serialize the message and wait for a reply
	serializedMsg, err := msg.Encode()
	if err != nil {
		return http.StatusInternalServerError, "", err
	}

	ch := m.ifritClient.SendTo(m.nodes[msg.Node], serializedMsg)
	var result string
	select {
		case response := <- ch: 
			result = string(response)
	}

	return http.StatusOK, result, nil
}