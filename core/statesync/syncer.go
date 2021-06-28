package statesync

import (
	"errors"
	"github.com/golang/protobuf/proto"
	"context"
	"github.com/joonnna/ifrit"
	"sync"
	"github.com/mitchellh/hashstructure/v2"
	//log "github.com/sirupsen/logrus"
	"github.com/arcsecc/lohpi/core/message"
	pb "github.com/arcsecc/lohpi/protobuf"
)

var (
	errNoRecipient = errors.New("Recipient cannot be empty")
	errNoInputMap = errors.New("No input map")
	errNoIfritClient = errors.New("Ifrit client cannot be nil")
	errSyncInterval = errors.New("Sync interval must be positive")
	errNoSummary = errors.New("Dataset summary cannot be nil")
	errNoDatasetIdentifiers = errors.New("No dataset identifiers")
	errNoPgMsg = errors.New("No protobuf message")
)

// TODO: consider separating syncing-related types into another pb package 

// Describes the synchronization of the types defined in this package.
// It uses protobuf types and syncs
type StateSyncUnit struct {
	isSyncing bool
	isSyncingMutex sync.RWMutex
	
	ifritClient *ifrit.Client
	ifritClientMutex sync.RWMutex
}

type DatasetIdentifiersSnapshot struct {
	// Map to sync with remote. Do not pass a reference to the original map; pass a deep copy of it. 
	DatasetIdentifiers []string
}	

func NewStateSyncUnit() (*StateSyncUnit, error) {
	return &StateSyncUnit{}, nil
}

func (d *StateSyncUnit) SynchronizeDatasetIdentifiers(ctx context.Context, datasetIdentifiers []string, remoteAddr string) error {
	if d.getIfritClient() == nil {
		return errNoIfritClient
	}

	d.setIsSyncing(true)
	defer d.setIsSyncing(false)

	if datasetIdentifiers == nil {
		return errNoDatasetIdentifiers
	}

	if remoteAddr == "" {
		return errNoRecipient
	}

	datasetTableHash, err := hashstructure.Hash(datasetIdentifiers, hashstructure.FormatV2, nil)
	if err != nil {
		return err
	}				

	msg := &pb.Message{
		Type: message.MSG_SYNCHRONIZE_DATASET_IDENTIFIERS,
		DatasetIdentifierStateRequest: &pb.DatasetIdentifierStateRequest{
			DatasetIdentifiersHash: datasetTableHash,
			DatasetIdentifiers: datasetIdentifiers,
		},
	}

	ch, err := d.sendToRemote(msg, remoteAddr)
	if err != nil {
		return err
	}

	select {
	case resp := <-ch:
		// verify signature of message after unmarshalling! See the node package. maybe interface them as well?
		respMsg := &pb.Message{}
		err := proto.Unmarshal(resp, respMsg)
		if err != nil {
			return err
		}

		//case context deadline...
		// case 1: identifiers are equal. nothing to do
		// case 2: send deltas: add and remove identifiers

	}

	return nil
}

// Given the inputIdentifiers, resolve any differences by parsing the DatasetIdentifierStateRequest message. Returns a list of correct identifiers. 
func (d *StateSyncUnit) ResolveDatasetIdentifiers(ctx context.Context, inputIdentifiers []string, s *pb.DatasetIdentifierStateRequest) ([]string, error) {
	if s == nil {
		return nil, errors.New("DatasetIdentifierStateRequest is nil")
	}

	h1, err := hashstructure.Hash(inputIdentifiers, hashstructure.FormatV2, nil)
	if err != nil {
		return nil, err
	}				

	h2, err := hashstructure.Hash(s.GetDatasetIdentifiers(), hashstructure.FormatV2, nil)
	if err != nil {
		return nil, err
	}				

	if h1 != h2 {
		return s.GetDatasetIdentifiers(), nil
	}

	return nil, nil 
}

func (d *StateSyncUnit) RegisterIfritClient(c *ifrit.Client) {
	d.ifritClientMutex.Lock()
	defer d.ifritClientMutex.Unlock()
	d.ifritClient = c
}

func (d *StateSyncUnit) IsSyncing() bool {
	d.isSyncingMutex.RLock()
	defer d.isSyncingMutex.RUnlock()
	return d.isSyncing
}

func (d *StateSyncUnit) setIsSyncing(syncing bool) {
	d.isSyncingMutex.Lock()
	defer d.isSyncingMutex.Unlock()	
	d.isSyncing = syncing
}

func (d *StateSyncUnit) getIfritClient() *ifrit.Client {
	d.ifritClientMutex.RLock()
	defer d.ifritClientMutex.RUnlock()
	return d.ifritClient
}

func (d *StateSyncUnit) sendToRemote(msg *pb.Message, remoteAddr string) (chan []byte, error) {
	if d.getIfritClient() == nil {
		return nil, errNoIfritClient
	}

	if msg == nil {
		return nil, errNoPgMsg
	}

	data, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}

	r, s, err := d.getIfritClient().Sign(data)
	if err != nil {
		return nil, err
	}

	msg.Signature = &pb.MsgSignature{R: r, S: s}

	data, err = proto.Marshal(msg)
	if err != nil {
		return nil, err
	}	

	ch := d.getIfritClient().SendTo(remoteAddr, data)

	return ch, nil
}

