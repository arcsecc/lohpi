package statesync

import (
	"errors"
_	"time"
	"github.com/golang/protobuf/proto"
	"context"
	"github.com/joonnna/ifrit"
	"sync"
	"github.com/arcsecc/lohpi/core/message"
	pb "github.com/arcsecc/lohpi/protobuf"
)

var (
	errNoDatasets = errors.New("Dataset map cannot be nil")
	errNoTargetAddr = errors.New("Target address cannot be empty")
	errNoInputMap = errors.New("No input map")
	errNoIfritClient = errors.New("Ifrit client cannot be nil")
	errSyncInterval = errors.New("Sync interval must be positive")
	errNoSummary = errors.New("Dataset summary cannot be nil")
	errNoDatasetIdentifiers = errors.New("No dataset identifiers")
	errNoPgMsg = errors.New("No protobuf message")
)

type Collection int

const (
    Datasets Collection = iota
	CheckoutDatasetPolicies
)

// TODO: consider separating syncing-related types into another pb package 

// Describes the synchronization of the types defined in this package.
// It uses protobuf types and syncs
type StateSyncUnit struct {
	datasetIsSyncing bool
	datasetIsSyncingMutex sync.RWMutex
	
	ifritClient *ifrit.Client
	ifritClientMutex sync.RWMutex

	syncState string
	syncStateMutex sync.RWMutex
}

func NewStateSyncUnit() (*StateSyncUnit, error) {
	return &StateSyncUnit{}, nil
}

func (d *StateSyncUnit) SynchronizeDatasets(ctx context.Context, datasets map[string]*pb.Dataset, targetAddr string) (map[string]*pb.Dataset, error) {
	if d.getIfritClient() == nil {
		return nil, errNoIfritClient
	}

	if d.getDatasetIsSyncing() {
		return nil, errors.New("Datasets are already in sync process")
	}

	d.setDatasetIsSyncing(true)
	defer d.setDatasetIsSyncing(false)

	if datasets == nil {
		return nil, errNoDatasets
	}

	if targetAddr == "" {
		return nil, errNoTargetAddr
	}

	msg := &pb.Message{
		Type: message.MSG_SYNCHRONIZE_DATASET_IDENTIFIERS,
		DatasetCollectionSummary: &pb.DatasetCollectionSummary{
			DatasetMap: datasets,
		},
	}

	ch, err := d.sendToRemote(msg, targetAddr)
	if err != nil {
		return nil, err
	}

	select {
	case resp := <-ch:
		// verify signature of message after unmarshalling! See the node package. maybe interface them as well?
		respMsg := &pb.Message{}
		err := proto.Unmarshal(resp, respMsg)
		if err != nil {
			return nil, err
		}

		return respMsg.GetDatasetCollectionSummary().GetDatasetMap(), nil
	}

	return nil, errors.New("Other error")
}

func (d *StateSyncUnit) RegisterIfritClient(c *ifrit.Client) {
	d.ifritClientMutex.Lock()
	defer d.ifritClientMutex.Unlock()
	d.ifritClient = c
}

func (d *StateSyncUnit) getDatasetIsSyncing() bool {
	d.datasetIsSyncingMutex.RLock()
	defer d.datasetIsSyncingMutex.RUnlock()
	return d.datasetIsSyncing
}

func (d *StateSyncUnit) SyncState() string {
	d.syncStateMutex.RLock()
	defer d.syncStateMutex.RUnlock()
	return d.syncState
}

func (d *StateSyncUnit) setSyncState(s string) {
	d.syncStateMutex.Lock()
	defer d.syncStateMutex.Unlock()
	d.syncState = s
}

func (d *StateSyncUnit) setDatasetIsSyncing(syncing bool) {
	d.datasetIsSyncingMutex.Lock()
	defer d.datasetIsSyncingMutex.Unlock()	
	d.datasetIsSyncing = syncing
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

