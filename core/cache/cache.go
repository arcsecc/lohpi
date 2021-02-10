package cache

/*import (
	"errors"
	"sync"
	"log"

	"github.com/joonnna/ifrit"
	pb "github.com/arcsecc/lohpi/protobuf"
)*/

// Cache is the internal overview of objects and nodes
type Cache struct {
	// Node's identifier maps to its IP address
	/*nodeMap    map[string]*pb.Node
	nodesMutex sync.RWMutex

	// Objects's identifier maps to the object header itself
	objectHeaderMap   map[string]*pb.ObjectMetadata
	objectHeaderMapMutex sync.RWMutex

	ifritClient *ifrit.Client

	// IP addresses that should be ignored
	ignoredIP []string

	cacheSize int*/
}

// Returns a new Cache. The Ifrit client needs to be in a running state.
// The cacheSize denotes how many study entries that can be in present at any time.
// If the cache size is exceeded, LRU study is evicted.
/*func NewCache(client *ifrit.Client) *Cache {
	return &Cache{
		nodeMap:     			make(map[string]*pb.Node),
		objectHeaderMap:    	make(map[string]*pb.ObjectMetadata),
		objectHeaderMapMutex:	sync.RWMutex{},
		ifritClient: 	client,
	}
}

// Update the cache's map of studies from all nodes in the network
func (c *Cache) FetchRemoteObjectMetadatas() {
	//c.fetchObjectMetadatas()
}

// Updates the cache with the object'd id and the object header itself
func (c *Cache) InsertObjectMetadata(objectId string, objectHeader *pb.ObjectMetadata) {
	c.objectHeaderMapMutex.Lock()
	defer c.objectHeaderMapMutex.Unlock()
	c.objectHeaderMap[objectId] = objectHeader
}

// Returns the storage node that stores the given object header
func (c *Cache) StorageNode(objectId string) (*pb.Node, error) {
/*	if header, exists := c.ObjectMetadatas()[objectId]; exists {
		log.Println("Found:", header)
		return header.GetNode(), nil
	}*
	return nil, errors.New("No such node")
}

// Sets the list of IP addresses the cache will ignore when interacting with the network
// TODO: set me elsewhere
func (c *Cache) SetIgnoredIPs(ips []string) {
	c.ignoredIP = ips
}

// Returns the map with node identifiers as keys and Node object as values
func (c *Cache) Nodes() map[string]*pb.Node {
	//c.updateNodeMembershipList()
	c.nodesMutex.RLock()
	defer c.nodesMutex.RUnlock()
	return c.nodeMap
}

// Returns the map of the object id-to-object map
func (c *Cache) ObjectMetadatas() map[string]*pb.ObjectMetadata {
	c.objectHeaderMapMutex.RLock()
	defer c.objectHeaderMapMutex.RUnlock()
	return c.objectHeaderMap
}

// Updates the node's identifier using the address
func (c *Cache) InsertNode(nodeName string, node *pb.Node) {
	c.nodesMutex.Lock()
	defer c.nodesMutex.Unlock()
	c.nodeMap[nodeName] = node
}

// Returns true if the node exists in the cache, returns false otherwise
func (c *Cache) NodeExists(node string) bool {
	c.nodesMutex.RLock()
	defer c.nodesMutex.RUnlock()
	if _, ok := c.nodeMap[node]; ok {
		return true
	}
	return false
}

// Returns true if the study exists in the cache, returns false otherwise
func (c *Cache) ObjectExists(object string) bool {
	c.objectHeaderMapMutex.RLock()
	defer c.objectHeaderMapMutex.RUnlock()

	// Check local cache. If it fails, fetch the remote lists
	if _, ok := c.objectHeaderMap[object]; ok {
		return true
	}
	//c.studyMutex.RUnlock()
	//c.fetchObjectSummaries()
	//c.objectHeaderMapMutex.RLock()

	// Check local cache again
	if _, ok := c.objectHeaderMap[object]; ok {
		return true
	}
	return false
}

// Given the node identifier, return the address of the node, 
func (c *Cache) NodeAddr(nodeName string) *pb.Node {
	c.nodesMutex.RLock()
	defer c.nodesMutex.RUnlock()
	return c.nodeMap[nodeName]
}

// Returns true if the study exists in any other node than the given node.
// Ignores if study is contained in the given node
// FIX ME
func (c *Cache) ObjectInAnyNodeThan(node, object string) bool {
/*	c.objectHeaderMapMutex.RLock()
	defer c.objectHeaderMapMutex.RUnlock()
	for objectId, objectValue := range c.objectHeaderMap {
		// If the object exists in another node than 'node', return true
		if objectId == object && objectValue.GetNode().GetName() != node {
			return true
		}
	}*
	return false
}

// Fetches all lists in the network and add them to the internal cache
/*func (c *Cache) fetchObjectMetadatas() {
	//c.updateNodeMembershipList()
	wg := sync.WaitGroup{}
	c.nodesMutex.RLock()
	defer c.nodesMutex.RUnlock()
	
	for _, node := range c.nodeMap {
		d := node.GetAddress()
		wg.Add(1)

		go func() {
			defer wg.Done()
			msg := &pb.Message{
				Type: message.MSG_TYPE_GET_OBJECT_HEADER_LIST,
			}
			
			data, err := proto.Marshal(msg)
			if err != nil {
				panic(err)
			}

			ch := c.ifritClient.SendTo(d, data)
			msgResp := &pb.Message{}

			select {
			case response := <-ch:
				if err := proto.Unmarshal(response, msgResp); err != nil {
					panic(err)
				}
				
				// Verify signature too
				for _, header := range msgResp.GetObjectMetadatas().GetObjectMetadatas() {
					objectID := header.GetName()
					c.UpdateObjectMetadata(objectID, header)
				}
			}
		}()
	}

	wg.Wait()
}*

// Updates the node map based on the underlying list of members as seen by 
// the Ifrit client. Should be invoked regulrarily to maintain a consistent view 
// of available members. TODO: make this shit work
func (c *Cache) updateNodeMembershipList() {
	return
	c.nodesMutex.Lock()
	defer c.nodesMutex.Unlock()

	log.Println("Length of map:", len(c.nodeMap))

	for nodeName, node := range c.nodeMap {
		if !c.nodeIsAlive(node.GetAddress()) {
			delete(c.nodeMap, nodeName)
			log.Println(node.GetName(), "is dead")
		}
	}
}

func (c *Cache) nodeIsAlive(addr string) bool {
	log.Println("Ifrit length:", len(c.ifritClient.Members()))
	for _, a := range c.ifritClient.Members() {
		log.Println("Ifrit address:", a)
		if a == addr {
			return true
		}
	}
	log.Println(addr, "does not exist in ifrit map")
	return false
}

// Updates the given node with a list of new studies
func (c *Cache) UpdateObjectMetadata(objectId string, objectHeader *pb.ObjectMetadata) {
	c.objectHeaderMapMutex.Lock()
	defer c.objectHeaderMapMutex.Unlock()
	c.objectHeaderMap[objectId] = objectHeader
}

func (c *Cache) ipIsIgnored(ip string) bool {
	for _, addr := range c.ignoredIP {
		log.Println("Checking ignored ip", ip, "against", addr)
		if addr == ip {
			return true
		}
	}
	return false
}*/