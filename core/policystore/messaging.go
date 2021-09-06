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
func (ps *PolicyStoreCore) pbMarshalDatasetPolicy(datasetId string) ([]byte, error) {
	resp := &pb.Message{
		Sender: ps.pbNode(),
		Policy: &pb.Policy{},
	}

	ds := ps.dsManager.Dataset(datasetId)
	if ds == nil {
		err := fmt.Errorf("Dataset entry is nil")
		log.Error(err.Error())
		panic(err)
		return nil, err
	}

	log.Infoln("DS:", ds)

	policy := ds.GetPolicy()
	if policy == nil {
		err := fmt.Errorf("Policy for dataset '%s' is nil\n", datasetId)
		return nil, err
	}

	resp.Policy = policy
	if err := ps.pbAddMessageSignature(resp); err != nil {
		panic(err)
		return nil, err
	}

	return proto.Marshal(resp)
}

func (ps *PolicyStoreCore) pbMarshalDatasetsMap(datasetsMap map[string]*pb.Dataset) ([]byte, error) {
	if datasetsMap == nil {
		return nil, errors.New("Datasets map cannot be nil")
	}

	resp := &pb.Message{
		Sender: ps.pbNode(), 
		DatasetCollectionSummary: &pb.DatasetCollectionSummary{
			DatasetMap: datasetsMap,
		},
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