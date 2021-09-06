package datasetmanager

import (
	"errors"
)

var (
	errNilCheckout          = errors.New("Dataset checkout is nil")
	errNilPolicy            = errors.New("Dataset policy is nil")
	errNilConfig            = errors.New("Configuration is nil")
	errNoPolicyFound        = errors.New("No such policy exists")
	errNoIfritClient        = errors.New("Ifrit client is nil")
	errNoPolicyStoreAddress = errors.New("Policy store address is empty")
	errNoDatasetMap         = errors.New("Dataset map is nil")
	errNilDataset 			= errors.New("Dataset is nil")
	errNoDatasetId   		= errors.New("Dataset identifiers is empty")
	errNoId 				= errors.New("Id must be set")
	errNilNode 				= errors.New("Node is nil")
	errNoNodeName 			= errors.New("Node name is empty")
)

var (
	ErrRemoveDataset 	= errors.New("Failed to remove dataset")
	ErrSetDatasetPolicy = errors.New("Failed to set dataset policy")
	ErrInsertDataset    = errors.New("Failed to insert dataset")
	ErrNoRedisPong = errors.New("Redis' ping response was wrong")
	ErrInsertPolicy = errors.New("Failed to insert policy")
)

// policy_table attributes
var (
	dataset_id = "dataset_id"
	allowed    = "allowed"
)

// redis attributes
var (
	datasetList = "dsList" // must NEVER equal to a dataset. Assert for this :)
)