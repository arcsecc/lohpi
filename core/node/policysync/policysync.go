package policysync

import (
	"context"
	"errors"
	"github.com/arcsecc/lohpi/core/comm"
	pb "github.com/arcsecc/lohpi/protobuf"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

var (
	errNoDatasets = errors.New("Dataset collection is nil")
)

var logFields = log.Fields{
	"package":     "core/node/policysync",
	"description": "policy reconciliation",
}

// Note: redesign how the policy store address is set by the caller

type datasetManager interface {
	Datasets(ctx context.Context) (map[string]*pb.Dataset, error) // index and cursor here!
	SetDatasetPolicy(ctx context.Context, datasetId string, policy *pb.Policy) error
}

type policyLogger interface {
	InsertObservedGossip(ctx context.Context, g *pb.PolicyGossipUpdate) error
	GossipIsObserved(ctx context.Context, g *pb.PolicyGossipUpdate) (bool, error)
	InsertAppliedPolicy(ctx context.Context, p *pb.Policy) error
}

type PolicySyncUnit struct {
	timer        *time.Timer
	syncInterval time.Duration

	// Make gRPC invocations towards the policy store
	psClient *comm.PolicyStoreGRPCClient

	psAddr      string
	psAddrMutex sync.RWMutex

	// Required to manipulate the datasets and their policies. See node.go in this package
	dsManager datasetManager
	policyLog policyLogger

	exitChan chan bool

	origin *pb.Node
}

func NewPolicySyncUnit(origin *pb.Node, syncInterval time.Duration, psClient *comm.PolicyStoreGRPCClient, dsManager datasetManager, policyLog policyLogger) (*PolicySyncUnit, error) {
	if origin == nil {
		return nil, errors.New("Protobuf node is nil")
	}

	if syncInterval <= 0 {
		return nil, errors.New("Sync interval must be greater than zero")
	}

	if psClient == nil {
		return nil, errors.New("Policy store gRPC client is nil")
	}

	return &PolicySyncUnit{
		syncInterval: syncInterval,
		timer:        time.NewTimer(syncInterval),
		psClient:     psClient,
		dsManager:    dsManager,
		policyLog:    policyLog,
		exitChan:     make(chan bool, 1),
		origin:       origin,
	}, nil
}

// Register gossip observation
func (p *PolicySyncUnit) ResetPolicyReconciliationTimer() {
	log.WithFields(logFields).Info("Resetting policy reconciliation timer")
	p.timer.Reset(p.syncInterval)
}

func (p *PolicySyncUnit) setDuration(d time.Duration) error {
	return nil
}

func (p *PolicySyncUnit) setPolicyStoreAddress(addr string) {
	p.psAddrMutex.Lock()
	defer p.psAddrMutex.Unlock()
	p.psAddr = addr
}

func (p *PolicySyncUnit) getPolicyStoreAddress() string {
	p.psAddrMutex.RLock()
	defer p.psAddrMutex.RUnlock()
	return p.psAddr
}

func (p *PolicySyncUnit) Start(psAddr string) {
	p.setPolicyStoreAddress(psAddr)

	p.timer.Reset(p.syncInterval)

	for {
		select {
		case <-p.timer.C:
			log.WithFields(logFields).Infof("Set reconciliation timer expired at %s. Initiating set reconciliation.", time.Now().Format(time.RFC1123))

			currentDatasetsMap, err := p.dsManager.Datasets(context.Background())
			if err != nil {
				log.WithFields(logFields).Error(err.Error())
				continue
			}

			if len(currentDatasetsMap) > 0 {
				updatedpolicies, err := p.reconcilePolicies(currentDatasetsMap)
				if err != nil {
					log.WithFields(logFields).Error(err.Error())
				}

				for _, policy := range updatedpolicies {
					p.dsManager.SetDatasetPolicy(context.Background(), policy.GetDatasetIdentifier(), policy)
					if err := p.policyLog.InsertAppliedPolicy(context.Background(), policy); err != nil {
						log.WithFields(logFields).Error(err.Error())
					}
				}
			} else {
				log.WithFields(logFields).Info("No policies to reconcile")
			}

			p.timer.Reset(p.syncInterval)
		case <-p.exitChan:
			log.Info("Stopping policy synchronization unit")
			break
		}
	}

	if !p.timer.Stop() {
		<-p.timer.C
	}
}

func (p *PolicySyncUnit) reconcilePolicies(currentDatasets map[string]*pb.Dataset) ([]*pb.Policy, error) {
	if currentDatasets == nil {
		return nil, errNoDatasets
	}

	log.WithFields(logFields).Info("Reconciling policies with policy store")
	addr := p.getPolicyStoreAddress()
	if addr == "" {
		return nil, errors.New("Policy store address was not set")
	}

	conn, err := p.psClient.Dial(addr)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		return nil, err
	}
	defer conn.CloseConn()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*1)
	defer cancel()

	// Prepare request
	policies := make([]*pb.Policy, 0, len(currentDatasets))
	for _, d := range currentDatasets {
		if policy := d.GetPolicy(); policy != nil {
			policies = append(policies, policy)
		} else {
			log.WithFields(logFields).Errorf("Tried to get policy of dataset '%s' but it was nil", d.GetIdentifier())
		}
	}

	req := &pb.PolicyReconcileRequest{
		Policies: policies,
		Origin:   p.origin,
	}

	policyCollectionResponse, err := conn.ReconcilePolicies(ctx, req)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		return nil, err
	}

	defer conn.CloseConn()

	return policyCollectionResponse.GetPolicyDeltas(), nil
}

func (p *PolicySyncUnit) Stop() {
	p.exitChan <- true
	close(p.exitChan)
}
