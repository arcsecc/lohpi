package mux

/* This file contains methods that use the Lohpi network for queries */

import (
	"log"
	"encoding/json"
	"errors"
	"fmt"
	"context"


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
					Address: m.cache.NodeAddr(node).GetAddress(),
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

// Given a node identifier and a study name, return the meta-data about a particular study at that node.
func (m *Mux) getObjectData(ctx context.Context, req *pb.DataUserRequest) (*pb.ObjectFiles, error) {
	// TODO: use streams

	if !m.sManager.ClientExists(req.GetClient().GetName()) {
		log.Println("No such client is known to the mux:", req.GetClient().GetName())
	}
	

	log.Println("req.GetObjectName()", req.GetObjectName())
	node, err := m.cache.StorageNode(req.GetObjectName())
	if err != nil {
		return nil, err
	}

	msg := &pb.Message{
		Type: message.MSG_TYPE_GET_OBJECT,
		DataUserRequest: req,
	}

	data, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}

	// Sign the chunks
	r, s, err := m.ifritClient.Sign(data)
	if err != nil {
		return nil, err
	}

	// Message with signature
	msg.Signature = &pb.MsgSignature{R: r, S: s}

	// Marshalled message to be multicasted
	data, err = proto.Marshal(msg)
	if err != nil {
		return nil, err
	}

	log.Println("Node addr:", node)
	ch := m.ifritClient.SendTo(node.GetAddress(), data)
	resp := &pb.ObjectFiles{ObjectFiles: make([]*pb.ObjectFile, 0)}

	select {
	case object := <-ch:
		log.Println("object:", object)
		respMsg := &pb.Message{}
		if err := proto.Unmarshal(object, respMsg); err != nil {
			panic(err)
		}

		// Verify message signature from node!
		for _, o := range respMsg.GetObjectFiles().GetObjectFiles() {
			resp.ObjectFiles = append(resp.ObjectFiles, o)
		}
	}

	return resp, nil 
}
