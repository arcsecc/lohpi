package cache

import (
	"sync"
	"time"

	"github.com/joonnna/ifrit"
	pb "github.com/arcsecc/lohpi/protobuf"
)

// Cache is the internal overview of objects and nodes
type Cache struct {
	ifritClient *ifrit.Client

	// Node's identifier maps to its Iffrit IP address
	nodeMap map[string]*pb.Node
	nodesMapLock sync.RWMutex

	// Dataset collection
	datasetNodesMap map[string]*pb.Node
	datasetNodesMapLock sync.RWMutex

	// Datasets that have been checked out
	datasetCheckoutMap map[string]*ClientCheckout
	datasetCheckoutMapLock sync.RWMutex

	// Datasets that are due for revocation
	invalidDatasetMap map[string]*InvalidDataset
	invalidDatasetMapLock sync.RWMutex
}

// Describes a dataset that has been checked out 
type ClientCheckout struct {
	dataset string
	clientId string
	timestamp time.Time
}

type InvalidDataset struct {
	dataset string
	timestamp time.Time
}

// Returns a new Cache. The Ifrit client needs to be in a running state.
// The cacheSize denotes how many study entries that can be in present at any time.
// If the cache size is exceeded, LRU study is evicted.
func NewCache(client *ifrit.Client) *Cache {
	return &Cache{
		nodeMap:     			make(map[string]*pb.Node),
		datasetNodesMap:		make(map[string]*pb.Node),
		datasetCheckoutMap:		make(map[string]*ClientCheckout),
		invalidDatasetMap:		make(map[string]*InvalidDataset),
		ifritClient: 			client,
	}
}

func NewClientCheckout(dataset, clientId string) *ClientCheckout {
	return &ClientCheckout{dataset,	clientId, time.Now()}
}

// Returns the storage node that stores the given object header
func (c *Cache) Nodes() map[string]*pb.Node {
	c.nodesMapLock.RLock()
	defer c.nodesMapLock.RUnlock()
	return c.nodeMap
}

// Updates the node's identifier using the address
func (c *Cache) AddNode(nodeName string, node *pb.Node) {
	c.nodesMapLock.Lock()
	defer c.nodesMapLock.Unlock()
	c.nodeMap[nodeName] = node
}

// Returns a map of datasets mapping to the node identifier
func (c *Cache) DatasetNodes() map[string]*pb.Node {
	c.datasetNodesMapLock.RLock()
	defer c.datasetNodesMapLock.RUnlock()
	return c.datasetNodesMap
}

// Assoicates a dataset identifier with a node identifier
func (c *Cache) AddDatasetNode(dataset string, node *pb.Node) {
	c.datasetNodesMapLock.Lock()
	defer c.datasetNodesMapLock.Unlock()
	c.datasetNodesMap[dataset] = node
}

func (c *Cache) ClientCheckouts() map[string]*ClientCheckout {
	c.datasetCheckoutMapLock.RLock()
	defer c.datasetCheckoutMapLock.RUnlock()
	return c.datasetCheckoutMap
}

func (c *Cache) AddClientCheckout(datasetId string, checkout *ClientCheckout) {
	c.datasetCheckoutMapLock.Lock()
	defer c.datasetCheckoutMapLock.Unlock()
	c.datasetCheckoutMap[datasetId] = checkout
}

func (c *Cache) InvalidDatasets() map[string]*InvalidDataset {
	c.invalidDatasetMapLock.RLock()
	defer c.invalidDatasetMapLock.RUnlock()
	return c.invalidDatasetMap
}

func (c *Cache) AddInvalidDataset(datasetId string, dataset *InvalidDataset) {
	c.invalidDatasetMapLock.Lock()
	defer c.invalidDatasetMapLock.Unlock()
	c.invalidDatasetMap[datasetId] = dataset
}