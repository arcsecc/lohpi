package cache

import (
	"sync"
	"log"

	"github.com/joonnna/ifrit"
	"github.com/tomcat-bit/lohpi/internal/core/message"
	pb "github.com/tomcat-bit/lohpi/protobuf"
	"github.com/golang/protobuf/proto"
)

// Cache is the internal overview of studies and nodes
type Cache struct {
	// Node's identifier maps to its IP address
	NodeMap    map[string]*pb.Node
	nodesMutex sync.RWMutex

	// Study's identifier maps to the node's identifier
	StudyMap   map[string]string
	studyMutex sync.RWMutex

	ifritClient *ifrit.Client

	ignoredIP []string
}

// Returns a new Cache. The Ifrit client needs to be in a running state
func NewCache(client *ifrit.Client) *Cache {
	return &Cache{
		NodeMap:     make(map[string]*pb.Node),
		StudyMap:    make(map[string]string), // maybe change this is pb.Study?
		ifritClient: client,
	}
}

// Update the cache's map of studies from all nodes in the network
func (c *Cache) FetchRemoteStudyLists() {
	c.fetchStudyLists()
}

// Updates the node's list of studies it stores
func (c *Cache) UpdateStudies(node string, studies []*pb.Study) {
	c.updateStudies(node, studies)
}

// Sets the list of IP addresses the cache will ignore
func (c *Cache) SetIgnoredIPs(ips []string) {
	c.ignoredIP = ips
}

// Returns the map of node identifiers
func (c *Cache) Nodes() map[string]*pb.Node {
	//c.updateNodeMembershipList()
	c.nodesMutex.RLock()
	defer c.nodesMutex.RUnlock()
	return c.NodeMap
}

// Returns the map of study storage nodes
func (c *Cache) Studies() map[string]string {
	c.studyMutex.RLock()
	defer c.studyMutex.RUnlock()
	return c.StudyMap
}

// Updates the node's identifier using the address
func (c *Cache) InsertNodes(nodeName string, node *pb.Node) {
	c.nodesMutex.Lock()
	defer c.nodesMutex.Unlock()
	c.NodeMap[nodeName] = node
}

// Returns true if the node exists in the cache, returns false otherwise
func (c *Cache) NodeExists(node string) bool {
	c.nodesMutex.RLock()
	defer c.nodesMutex.RUnlock()
	if _, ok := c.NodeMap[node]; ok {
		return true
	}
	return false
}

// Returns true if the study exists in the cache, returns false otherwise
func (c *Cache) StudyExists(study string) bool {
	defer c.studyMutex.RUnlock()

	// Check local cache. If it fails, fetch the remote lists
	c.studyMutex.RLock()
	if _, ok := c.StudyMap[study]; ok {
		return true
	}
	c.studyMutex.RUnlock()
	c.fetchStudyLists()
	c.studyMutex.RLock()

	// Check local cache again
	if _, ok := c.StudyMap[study]; ok {
		return true
	}
	return false
}

// Given the node identifier, return the address of the node, 
func (c *Cache) NodeAddr(nodeName string) *pb.Node {
	c.nodesMutex.RLock()
	defer c.nodesMutex.RUnlock()
	return c.NodeMap[nodeName]
}

// Returns true if the study exists in any other node than the given node.
// Ignores if study is contained in the given node
func (c *Cache) StudyInAnyNodeThan(node, study string) bool {
	c.studyMutex.RLock()
	defer c.studyMutex.RUnlock()
	for s, n := range c.StudyMap {
		if s == study && n != node {
			return true
		}
	}
	return false
}

// Fetches all lists in the network and add them to the internal cache
func (c *Cache) fetchStudyLists() {
	//c.updateNodeMembershipList()
	wg := sync.WaitGroup{}
	
	c.nodesMutex.RLock()
	defer c.nodesMutex.RUnlock()
	
	for nodeName, node := range c.NodeMap {
		name := nodeName
		d := node.GetAddress()
		wg.Add(1)

		go func() {
			defer wg.Done()
			msg := &pb.Message{
				Type: message.MSG_TYPE_GET_STUDY_LIST,
			}
			
			data, err := proto.Marshal(msg)
			if err != nil {
				panic(err)
			}

			ch := c.ifritClient.SendTo(d, data)
			msgResp := &pb.Studies{}

			select {
			case response := <-ch:
				if err := proto.Unmarshal(response, msgResp); err != nil {
					panic(err)
				}
			}

			// Add the node to the list of studies the node stores
			c.updateStudies(name, msgResp.GetStudies())
		}()
	}

	wg.Wait()
}

// Updates the node map based on the underlying list of members as seen by 
// the Ifrit client. Should be invoked regulrarily to maintain a consistent view 
// of available members. TODO: make this shit work
func (c *Cache) updateNodeMembershipList() {
	return
	c.nodesMutex.Lock()
	defer c.nodesMutex.Unlock()

	log.Println("Length of map:", len(c.NodeMap))

	for nodeName, node := range c.NodeMap {
		if !c.nodeIsAlive(node.GetAddress()) {
			delete(c.NodeMap, nodeName)
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
func (c *Cache) updateStudies(node string, studies []*pb.Study) {
	c.studyMutex.Lock()
	defer c.studyMutex.Unlock()
	for _, study := range studies {
		if _, ok := c.StudyMap[study.GetName()]; !ok {
			c.StudyMap[study.GetName()] = node
		}
	}
}

func (c *Cache) ipIsIgnored(ip string) bool {
	for _, addr := range c.ignoredIP {
		log.Println("Checking ignored ip", ip, "against", addr)
		if addr == ip {
			return true
		}
	}
	return false
}