package mux

/* This file contains methods that use the Lohpi network for queries */

import (
	"log"
	"encoding/json"
	"errors"
	"fmt"
	"mime/multipart"
	"net/http"

	pb "github.com/tomcat-bit/lohpi/protobuf"
	"github.com/golang/protobuf/proto"
	"github.com/tomcat-bit/lohpi/pkg/message"
)

// Returns human-readable info about a storage node
func (m *Mux) getNodeInfo(node string) (string, error) {
	msg := message.NodeMessage{
		MessageType: message.MSG_TYPE_GET_NODE_INFO,
	}

	if !m.cache.NodeExists(node) {
		errMsg := fmt.Sprintf("Error: unknown node '%s'", node)
		return "", errors.New(errMsg)
	}

	serialized, err := json.Marshal(msg)
	if err != nil {
		return "", err
	}

	ch := m.ifritClient.SendTo(m.cache.NodeAddr(node).GetAddress(), serialized)
	var result string
	select {
	case response := <-ch:
		result = string(response)
	}
	return result, nil
}

// Orders a node to create test data for it to store dummy data and assoicated data policies
func (m *Mux) loadNode(objectName, node, policyFileName string, md []byte, subjects []string, policyText []byte) error {
	if !m.cache.NodeExists(node) {
		errMsg := fmt.Sprintf("Unknown node: %s", node)
		return errors.New(errMsg)
	}

	// Prepare the message
	msg := &pb.Message{
		Type: message.MSG_TYPE_LOAD_NODE,
		Load: &pb.Load{
			ObjectHeader: &pb.ObjectHeader{
				Name: objectName,
				Node: &pb.Node{
					Name: node,
					Address: "",
				},
				Metadata: &pb.Metadata{
					Content: md,
					Subjects: subjects,
				},
				Policy: &pb.Policy{
					Issuer: "Mux",
					ObjectName: objectName,
					Filename: policyFileName,
					   Content: policyText,
				},
			},
			Minfiles: 2,
			Maxfiles: 10,
		},
	}

	data, err := proto.Marshal(msg)
	if err != nil {
		panic(err)
		return err
	}

	m.cache.FetchRemoteObjectHeaders()
	if m.cache.ObjectInAnyNodeThan(node, objectName) {
		errMsg := fmt.Sprintf("Study '%s' already exists in another node", objectName)
		return errors.New(errMsg)
	}

	// Load the node
	ch := m.ifritClient.SendTo(m.cache.NodeAddr(node).GetAddress(), data)
	select {
		// TODO: timeout handling
	case response := <-ch:
		msgResp := &pb.Message{}
		if err := proto.Unmarshal(response, msgResp); err != nil {
			panic(err)
		}

		if string(msgResp.GetType()) != message.MSG_TYPE_OK {
			panic(errors.New("Loading node failed"))
		}
	}

	log.Printf("Loading node OK")
	/*for _, 
	m.cache.UpdateObjectHeaders(node, msgResp.GetObjectHeaders())
	fmt.Printf("Node '%s' now stores study '%s'\n", node, objectName)*/
	return nil
}

// Given a node identifier and a study name, return the data at that node
// TODO: finish me later
func (m *Mux) _getStudyData(msg message.NodeMessage) (int, string, error) {
	// Ensure that the node is known to the mux
	if !m.cache.NodeExists(msg.Node) {
		errMsg := fmt.Sprintf("Unknown node: %s", msg.Node)
		return http.StatusNotFound, "", errors.New(errMsg)
	}

	/*
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

		return http.StatusOK, result, nil*/
	return 200, "", nil
}

// Sets a new policy for the given study
func (m *Mux) _setSubjectStudyPolicy(file multipart.File, fileHeader *multipart.FileHeader, study string) error {
	/*
		m.FetchStudyIDs()
		node, ok := m.studyNode[study];
		if !ok {
			errMsg := fmt.Sprintf("Study '%s' does not exist in the network", study)
			return errors.New(errMsg)
		}

		fileContents, err := ioutil.ReadAll(file)
		if err != nil {
			return err
		}

		msg := &message.NodeMessage {
			MessageType: 	message.MSG_TYPE_SET_SUBJECT_POLICY,
			Study: 			study,
			Filename:	 	fileHeader.Filename,
			Extras: 		fileContents,
		}

		fmt.Printf("Data to send: %s\n", msg)
		serializedMsg, err := msg.Encode()
		if err != nil {
			return err
		}

		ch := m.ifritClient.SendTo(m.nodes[node], serializedMsg)
		select {
			case response := <- ch:
				fmt.Printf("Response: %s\n", string(response))
		}
	*/
	return nil
}

// Sets the REC policy at a study and a node
/*func (m *Mux) _setRECPolicy(file multipart.File, fileHeader *multipart.FileHeader, study string) error {
	m.FetchStudyIDs()
	/*node, ok := m.studyNode[study];
	if !ok {
		errMsg := fmt.Sprintf("Study '%s' does not exist in the network", study)
		return errors.New(errMsg)
	}*

	fileContents, err := ioutil.ReadAll(file)
	if err != nil {
		return err
	}

	msg := &message.PolicyMessage {
		//Node: 		m.nodes[node],
		Node: 		"node_0",
		Study: 		study,
		Filename: 	fileHeader.Filename,
		Model: 		fileContents,
	}

	serializedMsg, err := msg.Encode()
	if err != nil {
		return err
	}

	/*ch := m.ifritClient.SendTo(m.nodes[node], serializedMsg)
	select {
		case response := <- ch:
			fmt.Printf("Response: %s\n", string(response))
	}*

	return m.ps.MessageHandler(serializedMsg)

	//return nil
}*/

// Given a node identifier and a study name, return the meta-data about a particular study at that node.
func (m *Mux) _getMetaData(msg message.NodeMessage) /*(int, string, error)*/ {
	// Ensure that the node is known to the mux
	/*if !m.nodeExists(msg.Node) {
	errMsg := fmt.Sprintf("Unknown node: %s", msg.Node)
	return http.StatusNotFound, "", errors.New(errMsg)*/
}

/*
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

	ch := m.ifritClient.SendTo(m.nodes[msg.Node], serializedMsg)*/
//var result string
/*select {
	case response := <- ch:
		result = string(response)
}*/

//return http.StatusOK, result, nil
