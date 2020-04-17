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
func (m *Mux) StoreNodeData(msg message.NodeMessage) error {
	// Ensure that the node is known to the mux
	if !m.nodeExists(msg.Node) {
		errMsg := fmt.Sprintf("Unknown node: %s", msg.Node)
		return errors.New(errMsg)
	}

	// Set the proper message type and marshall it to JSON format
	serialized, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	ch := m.ifritClient.SendTo(m.nodes[msg.Node], serialized)
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
	m.addNodeToListOfStudies(msg.Node, studies)
	return nil
}

// Returns a string of study files 
// TODO: if node is omitted, ask all nodes!
func (m *Mux) GetStudyData(msg message.NodeMessage) (int, string, error) {
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