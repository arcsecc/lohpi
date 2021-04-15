package node

import (
	"fmt"
	pb "github.com/arcsecc/lohpi/protobuf"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"github.com/arcsecc/lohpi/core/message"
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

func (n *NodeCore) pbMarshalDatasetURL(msg *pb.Message) ([]byte, error) {
	// Check that the dataset is indexed by the node
	if !n.dbDatasetExists(msg.GetDatasetRequest().GetIdentifier()) {
		return n.pbMarshalDisallowedDatasetResponse(fmt.Sprintf("Dataset '%s' is not indexed by the node", msg.GetDatasetRequest().GetIdentifier()))
	}

	// If multiple checkouts are allowed, check if the client has checked it out already
	if !n.config().AllowMultipleCheckouts {
		if n.dbDatasetIsCheckedOutByClient(msg.GetDatasetRequest().GetIdentifier()) {
			return n.pbMarshalDisallowedDatasetResponse("Client has already checked out this dataset")
		}
	}

	// If the client has valid access to the dataset, fetch the URL
	if n.clientIsAllowed(msg.GetDatasetRequest()) {
		datasetUrl, err := n.fetchDatasetURL(msg.GetDatasetRequest().GetIdentifier())
		if err != nil {
			log.Errorln(err.Error())
			return nil, err
		}

		// Response message
		respMsg := &pb.Message{
			DatasetResponse: &pb.DatasetResponse{
				URL:       datasetUrl,
				IsAllowed: true,
			},
		}

		if err := n.pbAddMessageSignature(respMsg); err != nil {
			log.Errorln(err.Error())
			return nil, err
		}

		if err := n.dbCheckoutDataset(msg.GetDatasetRequest()); err != nil {
			log.Errorln(err.Error())
			return nil, err
		}

		return proto.Marshal(respMsg)

	} else {
		return n.pbMarshalDisallowedDatasetResponse("Client has invalid access attributes")
	}
}

// Returns the URL of the dataset
func (n *NodeCore) fetchDatasetURL(id string) (string, error) {
	if handler := n.getDatasetCallback(); handler != nil {
		externalArchiveUrl, err := handler(id)
		if err != nil {
			return "", err
		}

		return externalArchiveUrl, nil
	}
	
	log.Warnln("Handler in fetchDatasetURL not set")
	return "", nil
}

// Marshals the metadata URL
func (n *NodeCore) pbMarshalDatasetMetadataURL(msg *pb.Message) ([]byte, error) {
	// Check that the dataset is indexed by the node
	if !n.dbDatasetExists(msg.GetDatasetRequest().GetIdentifier()) {
		return n.pbMarshalDisallowedDatasetResponse(fmt.Sprintf("Dataset '%s' is not indexed by the node", msg.GetDatasetRequest().GetIdentifier()))
	}

	metadataUrl, err := n.fetchDatasetMetadataURL(msg.GetDatasetRequest().GetIdentifier())
	if err != nil {
		return nil, err
	}

	respMsg := &pb.Message{
		StringValue: metadataUrl,
		Sender:      n.pbNode(),
	}

	if err := n.pbAddMessageSignature(respMsg); err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	return proto.Marshal(respMsg)
}

// Fetches the URL of the external metadata, given by the dataset id
func (n *NodeCore) fetchDatasetMetadataURL(id string) (string, error) {
	if handler := n.getExternalMetadataHandler(); handler != nil {
		metadataUrl, err := handler(id)
		if err != nil {
			return "", err
		}

		return metadataUrl, nil
	}
	
	log.Warnln("Handler in fetchDatasetMetadataURL is not set")
	return "", nil
}

