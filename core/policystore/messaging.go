package policystore

import (
	"fmt"
	"errors"
	"github.com/golang/protobuf/proto"
	"github.com/arcsecc/lohpi/core/message"
	pb "github.com/arcsecc/lohpi/protobuf"
	log "github.com/sirupsen/logrus"
)

// Returns the correct policy assoicated with a dataset to the node that stores it.
func (ps *PolicyStoreCore) pbMarshalDatasetPolicy(nodeIdentifier string, policyReq *pb.PolicyRequest) ([]byte, error) {
	if policyReq == nil {
		return nil, errors.New("Policy request is nil")
	}

	if nodeIdentifier == "" {
		return nil, errors.New("Polciy identifier is empty")
	}

	// Build the response message
	resp := &pb.Message{
		Sender: ps.pbNode(),
		Policy: &pb.Policy{},
	}

	p := ps.getDatasetPolicy(policyReq.GetIdentifier())
	if p != nil {
		resp.Policy = p
	} else {
		return nil, fmt.Errorf("No such dataset '%s' indexed by policy store", policyReq.GetIdentifier())
	}

	if err := ps.pbAddMessageSignature(resp); err != nil {
		return nil, err
	}

	return proto.Marshal(resp)
}

// Returns a mashalled acknowledgment
func (ps *PolicyStoreCore) pbMarshalAcknowledgeMessage() ([]byte, error) {
	msg := &pb.Message{Type: message.MSG_TYPE_OK}
	data, err := proto.Marshal(msg)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	r, s, err := ps.ifritClient.Sign(data)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	msg.Signature = &pb.MsgSignature{R: r, S: s}
	return proto.Marshal(msg)
}

// Signs the given message and sets R and S into it
func (ps *PolicyStoreCore) pbAddMessageSignature(msg *pb.Message) error {
	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	r, s, err := ps.ifritClient.Sign(data)
	if err != nil {
		return err
	}

	msg.Signature = &pb.MsgSignature{R: r, S: s}
	return nil
}