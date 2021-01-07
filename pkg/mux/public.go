package mux

/* This file contains methods that use the Lohpi network for queries */

import (
	//"log"
	"encoding/json"
	"errors"
	"fmt"
	//"context"


//	pb "github.com/tomcat-bit/lohpi/protobuf"
//	"github.com/golang/protobuf/proto"
	"github.com/tomcat-bit/lohpi/pkg/message"

)

// Returns human-readable info about a storage node
func (m *Mux) getNodeInfo(node string) (string, error) {
	msg := message.NodeMessage{
		MessageType: message.MSG_TYPE_GET_NODE_INFO,
	}

	if !m.nodeExists(node) {
		errMsg := fmt.Sprintf("Error: unknown node '%s'", node)
		return "", errors.New(errMsg)
	}

	serialized, err := json.Marshal(msg)
	if err != nil {
		return "", err
	}

	addr := m.StorageNodes()[node].GetAddress()
	ch := m.ifritClient.SendTo(addr, serialized)
	var result string
	select {
	case response := <-ch:
		result = string(response)
	}
	return result, nil
}

// Given a node identifier and a study name, return the meta-data about a particular study at that node.
/*func (m *Mux) getObjectData(ctx context.Context, req *pb.DataUserRequest) (*pb.ObjectFiles, error) {
	// TODO: use streams

	if !m.sManager.ClientExists(req.GetClient().GetName()) {
		log.Println("No such client is known to the mux:", req.GetClient().GetName())
	}
	
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
}*/
