package datasetmanager

import (
	"errors"
	log "github.com/sirupsen/logrus"
)

var (
	// Miscellaneous errors used by the package.
	ErrNilConfig               = errors.New("Configuration is nil")
	ErrRemoveDataset           = errors.New("Failed to remove dataset")
	ErrEmptyInstanceIdentifier = errors.New("No instance ID was provided")
	ErrNilConnectionPool       = errors.New("Connection pool was nil")
	ErrEmptyDatasetIdentifier  = errors.New("Dataset identifier was not provided")
	ErrNilNode                 = errors.New("Protobuf node was nil")
	ErrNilNodeIdentifier       = errors.New("Node identifier was not provided")

	// Error types used by the dataset checkout manager
	ErrNoDataset                   = errors.New("Dataset was not provided")
	ErrDatasetCheckout             = errors.New("Dataset checkout failed")
	ErrDatasetCheckin              = errors.New("Dataset checkin failed")
	ErrDatasetIsCheckedOutByClient = errors.New("Cannot check if dataset is checked out by client")
	ErrDatasetIsCheckedOut         = errors.New("Dataset checkout client is nil")
	ErrNilDatasetCheckoutClient    = errors.New("Dataset checkout client is nil")
	ErrDatasetCheckouts            = errors.New("Could not get dataset checkouts")
	ErrSetDatasetCheckoutAccess    = errors.New("Could not set dataset availability access")
	ErrGetDatasetCheckoutAccess    = errors.New("Could not get dataset availability access")
	ErrNumDatasetCheckouts         = errors.New("Could not get dataset checkouts")

	// Error types used by the dataset lookup client

	// Error types used by the dataset indexer
	ErrGetDataset    = errors.New("Could not get dataset")
	ErrGetDatasets   = errors.New("Could not get datasets")
	ErrDatasetExists = errors.New("Failed to lookup dataset in index")
	ErrCountDatasets = errors.New("Could not count datasets")

	ErrDatasetNodeExists         = errors.New("Could not find dataset node")
	ErrEmptyNodeIdentifier       = errors.New("No node identifier was provided")
	ErrSetDatasetPolicy          = errors.New("Failed to set dataset policy")
	ErrLookupNodeDatasets        = errors.New("Could not lookup dataset identifiers at the provided node")
	ErrLookupDatasetIdentifiers  = errors.New("Could not lookup dataset identifiers")
	ErrLookupDatasetNode         = errors.New("Could not lookup dataset node")
	ErrInsertDataset             = errors.New("Failed to insert dataset")
	ErrInsertPolicy              = errors.New("Failed to insert policy")
	ErrInsertDatasetIdentifiers  = errors.New("Inserting dataset identifiers failed")
	ErrResolveDatasetIdentifiers = errors.New("Resolving dataset identifiers failed")
	ErrNoPolicy                  = errors.New("No policy was specified")
	ErrGetDatasetPolicy          = errors.New("Dataset policy could not be found")
	ErrDatasetExistsAtNode       = errors.New("Could not check if the given dataset exists at the given node")
	ErrNoDatasetCheckout         = errors.New("Dataset checkout is nil")
)

// policy_table attributes
var (
	dataset_id = "dataset_id"
	allowed    = "allowed"
)

var dbLogFields = log.Fields{
	"package": "core/datasetmanager",
	"action":  "database client",
}
