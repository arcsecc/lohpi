package node

import (
	"errors"
	"fmt"
	pb "github.com/arcsecc/lohpi/protobuf"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"github.com/arcsecc/lohpi/core/message"
	"strconv"
)

// This file defines the message passing functions for outgoing messages from the node.
// Notice the pbXXX signature.
// TODO use context on all message passing functions. 

// Requests the newest policy from the policy store
func (n *NodeCore) pbSendPolicyStorePolicyRequest(datasetId, policyStoreIP string) (*pb.Policy, error) {
	msg := &pb.Message{
		Type:   message.MSG_TYPE_GET_DATASET_POLICY,
		Sender: n.pbNode(),
		PolicyRequest: &pb.PolicyRequest{
			Identifier: datasetId,
		},
	}

	if err := n.pbAddMessageSignature(msg); err != nil {
		return nil, err
	}

	data, err := proto.Marshal(msg)
	if err != nil {
		panic(err)
		return nil, err
	}

	ch := n.ifritClient.SendTo(policyStoreIP, data)

	select {
	case resp := <-ch:
		respMsg := &pb.Message{}
		if err := proto.Unmarshal(resp, respMsg); err != nil {
			panic(err)
			return nil, err
		}

		if err := n.verifyMessageSignature(respMsg); err != nil {
			panic(err)
			return nil, err
		}

		return respMsg.GetPolicy(), nil
	}
	return nil, fmt.Errorf("Error in pbRequestPolicyStoreUpdate")
}

// Sends the dataset identfier given bu 'id' to the recipient
func (n *NodeCore) pbSendDatsetIdentifier(id, recipient string) error {
	msg := &pb.Message{
		Type:        message.MSG_TYPE_ADD_DATASET_IDENTIFIER,
		Sender:      n.pbNode(),
		StringValue: id,
	}

	if err := n.pbAddMessageSignature(msg); err != nil {
		return err
	}

	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	log.Debug("recipient:", recipient)

	n.ifritClient.SendTo(recipient, data)
	return nil
}

// Signs the given message and sets R and S into it
func (n *NodeCore) pbAddMessageSignature(msg *pb.Message) error {
	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	r, s, err := n.ifritClient.Sign(data)
	if err != nil {
		return err
	}

	msg.Signature = &pb.MsgSignature{R: r, S: s}
	return nil
}

// Returns the protobuf message of the indexed identifiers in this node.
func (n *NodeCore) pbMarshalDatasetIdentifiers(msg *pb.Message) ([]byte, error) {
	respMsg := &pb.Message{
		StringSlice: n.datasetIdentifiers(),
	}

	if err := n.pbAddMessageSignature(respMsg); err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	return proto.Marshal(respMsg)
}

// Returns a mashalled acknowledgment
func (n *NodeCore) pbMarshalAcknowledgeMessage() ([]byte, error) {
	msg := &pb.Message{Type: message.MSG_TYPE_OK}
	data, err := proto.Marshal(msg)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	r, s, err := n.ifritClient.Sign(data)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	msg.Signature = &pb.MsgSignature{R: r, S: s}
	return proto.Marshal(msg)
}

// Returns a marshalled message notifying the recipient that the data request was unauthorized.
func (n *NodeCore) pbMarshalDisallowedDatasetResponse(errorMsg string) ([]byte, error) {
	respMsg := &pb.Message{
		DatasetResponse: &pb.DatasetResponse{
			IsAllowed:    false,
			ErrorMessage: errorMsg,
		},
	}

	if err := n.pbAddMessageSignature(respMsg); err != nil {
		log.Errorln(err.Error())
		return nil, err
	}
	return proto.Marshal(respMsg)
}

// Sends a revocation message to the directory server. The message is cached at the directory server 
// so that clients can check the state of the policy.
func (n *NodeCore) pbSendDatasetRevocationUpdate(dataset, policyContent string) error {
	if dataset == "" {
		return errors.New("Dataset identifier must not be empty")
	} else if policyContent == "" {
		return errors.New("Policy content must not be empty")
	}

	b, err := strconv.ParseBool(policyContent)
	if err != nil {
		return err
	}

	msg := &pb.Message{
		Type:        message.MSG_POLICY_REVOCATION_UPDATE,
		Sender:      n.pbNode(),
		StringValue: dataset,
		BoolValue:   b,
	}

	if err := n.pbAddMessageSignature(msg); err != nil {
		return err
	}

	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	// todo more here? Try again if something fails? timeouts? 
	n.ifritClient.SendTo(n.directoryServerIP, data)

	return nil
}