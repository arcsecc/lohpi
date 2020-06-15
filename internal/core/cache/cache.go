package cache

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"log"
	"sync"

	"github.com/joonnna/ifrit"
	"github.com/tomcat-bit/lohpi/internal/core/message"
)

// Cache is the internal overview of studies and nodes
type Cache struct {
	// Study's name maps to its IP address
	NodeMap    map[string]string
	nodesMutex sync.RWMutex

	// Study's identifier maps to the node's identifier
	StudyMap   map[string]string
	studyMutex sync.RWMutex

	ifritClient *ifrit.Client
}

// Returns a new Cache
func NewCache(client *ifrit.Client) (*Cache, error) {
	return &Cache{
		NodeMap:     make(map[string]string),
		StudyMap:    make(map[string]string),
		ifritClient: client,
	}, nil
}

// Update the cache's map of studies from all nodes in the network
func (c *Cache) FetchRemoteStudyLists() {
	c.fetchStudyLists()
}

// Updates the node's list of studies it stores
func (c *Cache) UpdateStudies(node string, studies []string) {
	c.updateStudies(node, studies)
}

// Returns the map of node identifiers
func (c *Cache) Nodes() map[string]string {
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
func (c *Cache) UpdateNodes(node, addr string) {
	c.nodesMutex.Lock()
	defer c.nodesMutex.Unlock()
	c.NodeMap[node] = addr
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
	c.studyMutex.RLock()
	defer c.studyMutex.RUnlock()

	// Check local cache. If it fails, fetch the remote lists
	if _, ok := c.StudyMap[study]; ok {
		return true
	}

	// Unlock before we yield control
	c.studyMutex.RUnlock()
	c.fetchStudyLists()
	c.studyMutex.RLock()

	// Check local cache again
	if _, ok := c.StudyMap[study]; ok {
		return true
	}
	return false
}

// Returns the address of the node
func (c *Cache) NodeAddr(node string) string {
	c.nodesMutex.RLock()
	defer c.nodesMutex.RUnlock()
	return c.NodeMap[node]
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

func (c *Cache) fetchStudyLists() {
	// TODO: use goroutines in loop
	for nodeName, dest := range c.NodeMap {
		msg := message.NodeMessage{
			MessageType: message.MSG_TYPE_GET_STUDY_LIST,
		}

		jsonStr, err := json.Marshal(msg)
		if err != nil {
			log.Fatal(err)
		}

		ch := c.ifritClient.SendTo(dest, jsonStr)
		studies := make([]string, 0)
		select {
		case response := <-ch:
			reader := bytes.NewReader(response)
			dec := gob.NewDecoder(reader)
			if err := dec.Decode(&studies); err != nil {
				panic(err)
			}
		}

		// add the node to the list of studies the node stores
		c.updateStudies(nodeName, studies)
	}
}

func (c *Cache) updateStudies(node string, studies []string) {
	c.studyMutex.Lock()
	defer c.studyMutex.Unlock()
	for _, study := range studies {
		if _, ok := c.StudyMap[study]; !ok {
			c.StudyMap[study] = node
		}
	}
}
