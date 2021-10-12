package setsync

import (
	"bytes"
	"fmt"
	"errors"
	"math/rand"
	"time"
	log "github.com/sirupsen/logrus"
	"github.com/golang/protobuf/proto"
	"context"
	"github.com/joonnna/ifrit"
	"github.com/joonnna/ifrit/core"
	"sync"
	pb "github.com/arcsecc/lohpi/protobuf"
	"github.com/arcsecc/lohpi/core/message"
	"github.com/jackc/pgx/v4/pgxpool"
)

var (
	errNoDatasets = errors.New("Dataset map cannot be nil")
	errNoRemoteAddr = errors.New("Address of remote server cannot be empty")
	errNoInputMap = errors.New("No input map")
	ErrNoIfritClient = errors.New("Ifrit client is nil")
	errSyncInterval = errors.New("Sync interval must be positive")
	errNoSummary = errors.New("Dataset summary cannot be nil")
	ErrNoIdentifiers = errors.New("No dataset identifiers was found in collection")
	errNoPgMsg = errors.New("No protobuf message")
	ErrNoDatasetIdentifiersAreSyncing = errors.New("Dataset identifiers are currently syncing")
	ErrDatasetIdentifiersResponse = errors.New("Dataset identifiers response is nil")
	ErrDatasetAreSyncing = errors.New("Dataset are currently syncing")
	ErrNoDBPool = errors.New("No database pool")
	ErrNoNodeName = errors.New("Node name cannot be empty")
	ErrNegativeDeltaNumber = errors.New("Negative delta number")
	ErrNilDatasetSyncronization = errors.New("Datasets from node is nil")
	ErrNilDatasetSyncRequestResponse = errors.New("Dataset synchronization request response is nil")
	ErrNoPSQLConnectionString = errors.New("No PSQL connection string")
)

var logFields = log.Fields{
	"package": "core/setsync",
	"description": "set reconciliation",
} 

// Describes the synchronization of the types defined in this package.
// It uses protobuf types and syncs
type SetSyncUnit struct {
	datasetIdentifiersInSync bool
	datasetIdentifiersInSyncMutex sync.RWMutex
	
	datasetsInSync bool
	datasetsInSyncMutex sync.RWMutex

	ifritClient *ifrit.Client
	ifritClientMutex sync.RWMutex

	datasetSyncSessionId []byte
	datasetSyncSessionIdMutex sync.RWMutex

	syncState string
	syncStateMutex sync.RWMutex

	pool *pgxpool.Pool
	schema string
	policyReconTable string
}

func NewSetSyncUnit() (*SetSyncUnit, error) {
	return &SetSyncUnit{}, nil
}

func (d *SetSyncUnit) InitializeReconciliationDatabaseConnection(connString string) error {
	if connString == "" {
		return ErrNoPSQLConnectionString
	}

	poolConfig, err := pgxpool.ParseConfig(connString)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		return err
	}

	poolConfig.MaxConnLifetime = time.Second * 10
	poolConfig.MaxConnIdleTime = time.Second * 4
	poolConfig.MaxConns = 100
	poolConfig.HealthCheckPeriod = time.Second * 1
	poolConfig.LazyConnect = false

	pool, err := pgxpool.ConnectConfig(context.Background(), poolConfig)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		return err
	}

	d.pool = pool
	return nil 
}

func (d *SetSyncUnit) InitializePolicyReconciliationTable(id string) error {
	if d.pool == nil {
		log.WithFields(logFields).Error(ErrNoDBPool.Error())
		return ErrNoDBPool
	}
	d.schema = id + "_schema"
	d.policyReconTable = id + "_policy_reconciliation_table"
	return nil
}
 
func (d *SetSyncUnit) RequestDatasetIdentifiersSync(ctx context.Context, currentIdentifiers []string, remoteAddr string) ([]string, []string, error) {
	if d.getIfritClient() == nil {
		return nil, nil, ErrNoIfritClient
	}

	if currentIdentifiers == nil {
		return nil, nil, ErrNoIdentifiers
	}

	if d.getDatasetIdentifiersAreSyncing() {
		return nil, nil, ErrNoDatasetIdentifiersAreSyncing
	}

	d.setDatasetIdentifiersAreSyncing(true)
	defer d.setDatasetIdentifiersAreSyncing(false)

	msg := &pb.Message{
		Type: message.MSG_SYNCHRONIZE_DATASET_IDENTIFIERS,
		StringSlice: currentIdentifiers,
	}

	ch, err := d.sendToRemote(msg, remoteAddr)
	if err != nil {
		return nil, nil, err
	}

	select {
	case resp := <-ch:
		respMsg := &pb.Message{}
		if resp != nil {
			if err := resp.Error; err != nil {
				return nil, nil, err
			}
			
			if err := proto.Unmarshal(resp.Data, respMsg); err != nil {
				return nil, nil, err
			}

			if respMsg.GetDatasetIdentifiersResponse() != nil {
				newIds := respMsg.GetDatasetIdentifiersResponse().GetNewIdentifiers()
				staleIds := respMsg.GetDatasetIdentifiersResponse().GetStaleIdentifiers()

				if len(newIds) > 0 {
					log.WithFields(logFields).
						WithField("collection", "dataset identifiers").
						Debugf("Adding %v to collection", newIds)
				} else {
					log.WithFields(logFields).
						WithField("collection", "dataset identifiers").
						Debugf("No identifiers to add")
				}

				if len(staleIds) > 0 {
					log.WithFields(logFields).
						WithField("collection", "dataset identifiers").
						Debugf("Removing %v from collection", staleIds)
				} else {
					log.WithFields(logFields).
						WithField("collection", "dataset identifiers").
						Debugf("No identifiers to remove")
				}
				return newIds, staleIds, nil
			}
		}
	}

	return nil, nil, ErrDatasetIdentifiersResponse
}

func (d *SetSyncUnit) ResolveDatasetIdentifiersDeltas(currentDatasets []string, incomingDatasets []string) ([]string, []string) {
	diffAdding := make([]string, 0)
	diffRemoving := make([]string, 0)

	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		defer wg.Done()
		// Find elements to add
		adding := make(map[string]struct{}, len(incomingDatasets))
		for _, x := range incomingDatasets {
			adding[x] = struct{}{}
		}
		for _, x := range currentDatasets {
			if _, found := adding[x]; !found {
				diffAdding = append(diffAdding, x)
			}
		}
	}()

	go func() {
		defer wg.Done()
		// Find elements to remove
		removing := make(map[string]struct{}, len(currentDatasets))
		for _, x := range currentDatasets {
			removing[x] = struct{}{}
		}
		for _, x := range incomingDatasets {
			if _, found := removing[x]; !found {
				diffRemoving = append(diffRemoving, x)
			}
		}
	}()

	wg.Wait()

	if len(diffAdding) > 0 {
		log.WithFields(logFields).
			WithField("collection", "dataset policies").
			Debugf("Adding %v to policy collection", diffAdding)
	} else {
		log.WithFields(logFields).
		WithField("collection", "dataset policies").
		Debugf("No dataset policies to add")
	}

	if len(diffRemoving) > 0 {
		log.WithFields(logFields).
		WithField("collection", "dataset policies").
		Debugf("Removing %v from policy collection", diffRemoving)
	} else {
		log.WithFields(logFields).
		WithField("collection", "dataset policies").
		Debugf("No dataset policies to remove")
	}

    return diffAdding, diffRemoving
}

func (d *SetSyncUnit) RequestDatasetSync(ctx context.Context, node *pb.Node, currentDatasets []*pb.Dataset) (int, error) {
	if d.getIfritClient() == nil {
		return 0, ErrNoIfritClient
	}

	if d.getDatasetIsSyncing() {
		return 0, ErrDatasetAreSyncing
	}

	d.setDatasetIsSyncing(true)
	defer d.setDatasetIsSyncing(false)

	sessionId := genSessionId()
	d.setDatasetSyncSessionId(sessionId)

	msg := &pb.Message{
		Type: message.MSG_SYNCHRONIZE_DATASETS,
		Bytes: d.getDatasetSyncSessionId(),
	}

	// Start a three-phase procedure. See below.
	ch, err := d.sendToRemote(msg, node.GetIfritAddress())
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		return 0, err
	}

	var numDelta int = 0

	// Request the datasets from the node. Respond to the node with deltas
	select {
	case resp := <-ch:
		respMsg := &pb.Message{}
		if resp != nil {
			if err := proto.Unmarshal(resp.Data, respMsg); err != nil {
				log.WithFields(logFields).Error(err.Error())
				return 0, err
			}

			if dSync := respMsg.GetDatasetSyncronization(); dSync != nil {
				requestSessionId := dSync.GetSessionID()
				if bytes.Compare(requestSessionId, d.getDatasetSyncSessionId()) != 0 {
					err := fmt.Errorf("Mismatch on session ID. Required %s, got %s", requestSessionId, d.getDatasetSyncSessionId())
					log.WithFields(logFields).Error(err.Error())
					return 0, err
				}

				if incomingDatasets := dSync.GetDatasets(); incomingDatasets != nil {
					delta, err := d.resolveDatasetDeltas(currentDatasets, incomingDatasets, d.getDatasetSyncSessionId())
					if err != nil {
						log.WithFields(logFields).Error(err.Error())
						return 0, err
					}

					msg = &pb.Message{
						Type: message.MSG_DELTA_DATASETS,
						DatasetSyncronizationDelta: delta,
					}

					numDelta = len(delta.GetDatasets())

					ch2, err := d.sendToRemote(msg, node.GetIfritAddress())
					if err != nil {
						log.WithFields(logFields).Error(err.Error())
						return 0, err
					}

					select {
					case deltaResp := <-ch2:
						respMsg = &pb.Message{}
						if resp != nil {
							if err := proto.Unmarshal(deltaResp.Data, respMsg); err != nil {
								return 0, err
							}
						}

					case <-ctx.Done():
						log.WithFields(logFields).Error("Request cancelled")
						return 0, errors.New("Dataset sync timeout inner")			
					}
				} else {
					log.WithFields(logFields).Error(ErrNilDatasetSyncronization.Error())
				}
			} else {
				log.WithFields(logFields).Error(ErrNilDatasetSyncRequestResponse.Error())
			}
		}
		case <-ctx.Done():
			log.WithFields(logFields).Error("Request cancelled")
			return 0, errors.New("Dataset sync timeout")
	}

	return numDelta, nil
}

func (d *SetSyncUnit) resolveDatasetDeltas(currentDatasets []*pb.Dataset, incomingDatasets []*pb.Dataset, sessionID []byte) (*pb.DatasetSyncronizationDelta, error) {
	if currentDatasets == nil {
		return nil, errors.New("current dataset collection is nil")
	}

	if incomingDatasets == nil {
		return nil, errors.New("incoming datasets collection is nil")
	}

	deltaSets := make([]*pb.Dataset, 0)

	// Compare the sets from the node and PS. Resolve the differences
	for _, incoming := range incomingDatasets {
		for _, current := range currentDatasets {
			if incoming.GetIdentifier() == current.GetIdentifier() {
				if incomingPolicy := incoming.GetPolicy(); incomingPolicy != nil {
					if currentPolicy := current.GetPolicy(); currentPolicy != nil {
						if incomingPolicy.GetVersion() != currentPolicy.GetVersion() {
							deltaSets = append(deltaSets, current)
						}
					}
				}
				continue
			}
		}
	}

	return &pb.DatasetSyncronizationDelta{
		SessionID: sessionID,
		Datasets: deltaSets,
	}, nil
}

func (d *SetSyncUnit) DBLogDatasetReconciliation(nodeName string, numResolved int) error {
	return d.dbInsertPolicyReconciliation(nodeName, time.Now(), numResolved)
}

func (d *SetSyncUnit) RegisterIfritClient(c *ifrit.Client) {
	d.ifritClientMutex.Lock()
	defer d.ifritClientMutex.Unlock()
	d.ifritClient = c
}

func (d *SetSyncUnit) getDatasetIdentifiersAreSyncing() bool {
	d.datasetIdentifiersInSyncMutex.RLock()
	defer d.datasetIdentifiersInSyncMutex.RUnlock()	
	return d.datasetIdentifiersInSync
}

func (d *SetSyncUnit) setDatasetIdentifiersAreSyncing(syncing bool) {
	d.datasetIdentifiersInSyncMutex.Lock()
	defer d.datasetIdentifiersInSyncMutex.Unlock()	
	d.datasetIdentifiersInSync = syncing
}

func (d *SetSyncUnit) getDatasetIsSyncing() bool {
	d.datasetsInSyncMutex.RLock()
	defer d.datasetsInSyncMutex.RUnlock()	
	return d.datasetsInSync
}

func (d *SetSyncUnit) setDatasetIsSyncing(syncing bool) {
	d.datasetsInSyncMutex.Lock()
	defer d.datasetsInSyncMutex.Unlock()	
	d.datasetsInSync = syncing
}

func (d *SetSyncUnit) getIfritClient() *ifrit.Client {
	d.ifritClientMutex.RLock()
	defer d.ifritClientMutex.RUnlock()
	return d.ifritClient
}

func (d *SetSyncUnit) setDatasetSyncSessionId(sessionId []byte) {
	d.datasetSyncSessionIdMutex.Lock()
	defer d.datasetSyncSessionIdMutex.Unlock()
	d.datasetSyncSessionId = sessionId
}

func (d *SetSyncUnit) getDatasetSyncSessionId() []byte {
	d.datasetSyncSessionIdMutex.RLock()
	defer d.datasetSyncSessionIdMutex.RUnlock()
	return d.datasetSyncSessionId
}

func (d *SetSyncUnit) sendToRemote(msg *pb.Message, remoteAddr string) (chan *core.Message, error) {
	if d.getIfritClient() == nil {
		return nil, ErrNoIfritClient
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

	return d.getIfritClient().SendTo(remoteAddr, data), nil
}

func genSessionId() []byte {
	token := make([]byte, 32)
    rand.Read(token)
	return token
}
