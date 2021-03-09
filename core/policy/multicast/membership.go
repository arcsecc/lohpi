package multicast

import (
	"errors"
	"time"
	log "github.com/sirupsen/logrus"
	"sort"
	"math/rand"
	"sync"

	"github.com/joonnna/ifrit"
)

type membershipManager struct {
	// Sends probe messages to the network
	ifritClient *ifrit.Client

	// List of recipients
	lruNodesMap map[string]int				
	lruLock	sync.RWMutex

	// Multicast stuff
	ignoredIPAddresses map[string]string
	ignoredIPLock sync.RWMutex
}

func newMembershipManager(ifritClient *ifrit.Client) *membershipManager {
	return &membershipManager{
		ifritClient:	ifritClient,
		lruNodesMap:	make(map[string]int),	// Ifrit IP -> number of invocations
		lruLock:		sync.RWMutex{},
		ignoredIPLock:	sync.RWMutex{},
	}
}

// Returns a map of the least recently used nodes and the number of
// times they each have been invoked.
func (m *membershipManager) lruNodes() map[string]int {
	m.lruLock.RLock()
	defer m.lruLock.RUnlock()
	return m.lruNodesMap
}

// Returns a subset of the least recently used members. Returns an array of one element if
// and only if 0 < numDirectRecipients < 4. numDirectRecipients is a whole number (not percetage).
// TODO: numDirectRecipients tends to live its own life. Fix this!
func (m *membershipManager) lruMembers(members []string, numDirectRecipients int) ([]string, error) {
	log.Debugln("Implement lruMembers!")
	return nil, nil
	
	m.updateLruMembers()

	// BUG HERE!
	// Nothing to do
	if len(m.lruNodes()) == 0 {
		return nil, errors.New("No members in LRU list")
	}

	// Sanitize input
	if numDirectRecipients < 0 {
		log.Println("numDirectRecipients is negative! -", numDirectRecipients)
		numDirectRecipients = 1
	}

	if numDirectRecipients > len(m.lruNodes()) {
		log.Println("numDirectRecipients is greater than n! -", numDirectRecipients)
		numDirectRecipients = 1
	}

	if len(m.lruNodes()) > 4 && numDirectRecipients > 0 {
		numDirectRecipients = 1
	}

	// List of LRU recipients
	recipients := make([]string, 0, int(numDirectRecipients))

	// List of counts fetched from the LRU map. Sort it in ascending order so that
	// the members that have sent the least number of times will be used as recipients
	counts := make([]int, 0)
	for  _, value := range m.lruNodes() {
		counts = append(counts, value)
	}

	sort.Ints(counts)
	lowest := counts[0]
	iterations := 0

	// If sigma > number of nodes
	if numDirectRecipients > len(m.lruNodes()) {
		numDirectRecipients = len(m.lruNodes())
	}

	// Always keep the number of times a recipient has been contacted as low as possible
	m.lruLock.Lock()
	defer m.lruLock.Unlock()
	for k, v := range m.lruNodesMap {
		if v <= lowest {
			lowest = v
			
			recipients = append(recipients, k)
			iterations += 1
			m.lruNodesMap[k] += 1
			log.Println("In memmanger:", numDirectRecipients)
			if iterations >= int(numDirectRecipients) {
				break
			}
		}
	}

	return recipients, nil
}

// Returns all available Ifrit members. Ignored IP addresses are filtered from the result
func (m *membershipManager) AllMembers() []string {
	members := make([]string, 0)
	for _, addr := range m.ifritClient.Members() {
		if !m.ipIsIgnored(addr) {
			members = append(members, addr)
		}
	}
	return members
}

// Sets the given list as a black-list for Ifrit IP addresses that receive
// multicast messages. If the internal black-list is empty, all nodes can 
// possibly receive a message.
func (m *membershipManager) setIgnoredIfritNodes(ignoredIPs map[string]string) {
	m.ignoredIPLock.Lock()
	defer m.ignoredIPLock.Unlock()
	m.ignoredIPAddresses = ignoredIPs
}

// Update LRU members list by mirroring the underlying Ifrit network membership list
func (m *membershipManager) updateLruMembers() {
	tempMap := make(map[string]int)

	// Copy the current map
	for k, v := range m.lruNodes() {
		tempMap[k] = v
	}

	m.lruLock.Lock()
	defer m.lruLock.Unlock()
	m.lruNodesMap = make(map[string]int)

	// Update the map according to the underlying Ifrit members list.
	// Save the number of times the nodes have been invoked.
	for _, ifritMember := range m.ifritClient.Members() {
		if !m.ipIsIgnored(ifritMember) {
			if count, ok := tempMap[ifritMember]; !ok {
				// Add new member
				m.lruNodesMap[ifritMember] = 0
			} else {
				// Do nothing. Save the current number of sent messages
				m.lruNodesMap[ifritMember] = count
			}
		}
	}
}

// TODO: test me
func (m *membershipManager) randomMembers(members []string, numDirectRecipients int) ([]string, error) {
	if len(members) == 0 {
		return nil, errors.New("Members list is empty")
	}

	if numDirectRecipients < 0 {
		log.Warnln("numDirectRecipients is negative. Setting it to 1")
		numDirectRecipients = 1
	}

	if numDirectRecipients > len(members) {
		log.Warnln(`numDirectRecipients is greater than number of available members.
			Sets numDirectRecipients equal to number of available members`)
		numDirectRecipients = len(members)
	}

	if len(members) > 4 {
		numDirectRecipients = 1
	}

	// Populate 'result' from n random members from 'members'
	result := make([]string, 0, numDirectRecipients)

	rand.Seed(time.Now().UnixNano())
    p := rand.Perm(numDirectRecipients)
    for i := range p {
		result = append(result, members[i])
	}
	
	log.Println("result:", result)
	return result, nil
}

// Returns true if the given ip address is ignored, returns false otherwise
func (m *membershipManager) ipIsIgnored(ip string) bool {
	m.ignoredIPLock.RLock()
	defer m.ignoredIPLock.RUnlock()

	_, ok := m.ignoredIPAddresses[ip]
	return ok
}