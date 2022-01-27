package policysync

import (
	"context"
	"errors"
	"fmt"
	"github.com/arcsecc/lohpi/core/status"
	pb "github.com/arcsecc/lohpi/protobuf"
	"github.com/jackc/pgx/v4/pgxpool"
	log "github.com/sirupsen/logrus"
	"time"
)

var (
	errNoDatasets                    = errors.New("Dataset map cannot be nil")
	errNoRemoteAddr                  = errors.New("Address of remote server cannot be empty")
	errNoInputMap                    = errors.New("No input map")
	ErrNoIfritClient                 = errors.New("Ifrit client is nil")
	errSyncInterval                  = errors.New("Sync interval must be positive")
	errNoSummary                     = errors.New("Dataset summary cannot be nil")
	ErrNoIdentifiers                 = errors.New("No dataset identifiers was found in collection")
	errNoPgMsg                       = errors.New("No protobuf message")
	ErrDatasetIdentifiersResponse    = errors.New("Dataset identifiers response is nil")
	ErrDatasetAreSyncing             = errors.New("Dataset are currently syncing")
	ErrNoDBPool                      = errors.New("No database pool")
	ErrNoNodeName                    = errors.New("Node name cannot be empty")
	ErrNegativeDeltaNumber           = errors.New("Negative delta number")
	ErrNilDatasetSyncronization      = errors.New("Datasets from node is nil")
	ErrNilDatasetSyncRequestResponse = errors.New("Dataset synchronization request response is nil")
	ErrNoPSQLConnectionString        = errors.New("No PSQL connection string")
)

var logFields = log.Fields{
	"package":     "core/setsync",
	"description": "set reconciliation",
}

type datasetManager interface {
	GetDatasetPolicy(ctx context.Context, datasetId string) (*pb.Policy, error)
}

// Describes the synchronization of the types defined in this package.
// It uses protobuf types and syncs
type PolicySyncUnit struct {
	pool             *pgxpool.Pool
	schema           string
	policyReconTable string
	dsManager        datasetManager
}

func NewPolicySyncUnit(id string, pool *pgxpool.Pool, dsManager datasetManager) (*PolicySyncUnit, error) {
	if id == "" {
		return nil, errors.New("Id is required")
	}

	if pool == nil {
		return nil, ErrNoDBPool
	}

	return &PolicySyncUnit{
		schema:           id + "_schema",
		policyReconTable: id + "_policy_reconciliation_table",
		pool:             pool,
		dsManager:        dsManager,
	}, nil
}

func (p *PolicySyncUnit) ReconcilePolicies(ctx context.Context, req *pb.PolicyReconcileRequest) (*pb.PolicyReconcileResponse, error) {
	if req == nil {
		return nil, status.Errorf(status.Nil, "Policy reconcile request is nil")
	}

	var policyDeltas []*pb.Policy

	if origin := req.GetOrigin(); origin != nil {
		if policies := req.GetPolicies(); policies != nil {
			policyDeltas, err := p.resolvePolicyDeltas(ctx, policies)
			if err != nil {
				log.WithFields(logFields).Error(err.Error())
				return nil, err
			}

			if err := p.dbInsertPolicyReconciliation(origin.GetName(), time.Now(), len(policyDeltas)); err != nil {
				log.WithFields(logFields).Error(err.Error())
			}
		} else {
			err := fmt.Errorf("Policy collection from node %s is nil", req.GetOrigin().GetName())
			log.WithFields(logFields).Error(err.Error())
			return nil, status.Errorf(status.Nil, "Policy reconcile request is nil")
		}
	} else {
		err := fmt.Errorf("Origin node is nil")
		log.WithFields(logFields).Error(err.Error())
		return nil, status.Error(status.Nil, err.Error())
	}

	return &pb.PolicyReconcileResponse{
		PolicyDeltas: policyDeltas,
	}, nil
}

func (p *PolicySyncUnit) resolvePolicyDeltas(ctx context.Context, policies []*pb.Policy) ([]*pb.Policy, error) {
	deltas := make([]*pb.Policy, 0)

	for _, remotePolicy := range policies {
		datasetId := remotePolicy.GetDatasetIdentifier()
		localPolicy, err := p.dsManager.GetDatasetPolicy(ctx, datasetId)
		if err != nil {
			log.WithFields(logFields).Error(err.Error())
			return nil, err
		}

		if localPolicy.GetVersion() > remotePolicy.GetVersion() {
			deltas = append(deltas, localPolicy)
		}
	}

	return deltas, nil
}
