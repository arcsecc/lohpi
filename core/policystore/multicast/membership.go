package multicast

import (
	"errors"
	"fmt"
	pb "github.com/arcsecc/lohpi/protobuf"
	"github.com/joonnna/ifrit"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"sort"
	"sync"
	"time"
)

type membershipManager struct {
	// Sends probe messages to the network
	ifritClient *ifrit.Client

	// List of recipients
	lruNodesMap map[string]int
	lruLock     sync.RWMutex

	// Multicast stuff
	ignoredIPAddresses map[string]string
	ignoredIPLock      sync.RWMutex

	geoRegions []*geoRegion
}

type geoRegion struct {
	regionName string
	members    []*pb.Node
}

func newMembershipManager(ifritClient *ifrit.Client) *membershipManager {
	return &membershipManager{
		ifritClient: ifritClient,
		lruNodesMap: make(map[string]int), // Ifrit IP -> number of invocations
		lruLock: sync.RWMutex{},
		ignoredIPLock: sync.RWMutex{},
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
func (m *membershipManager) lruMembers(nodes *pb.Nodes, numDirectRecipients int) ([]string, error) {
	log.Fatal("Implement lruMembers!")
	return nil, nil

	m.updateLruMembers()

	// BUG HERE!
	// Nothing to do
	if len(m.lruNodes()) == 0 {
		return nil, errors.New("No members in LRU list")
	}

	// Sanitize input
	if numDirectRecipients < 0 {
		numDirectRecipients = 1
	}

	if numDirectRecipients > len(m.lruNodes()) {
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
	for _, value := range m.lruNodes() {
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
			if iterations >= int(numDirectRecipients) {
				break
			}
		}
	}

	return recipients, nil
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

// Returns a list of random members based on the collection of nodes in the network and sigma.
func (m *membershipManager) randomMembers(nodes *pb.Nodes, numDirectRecipients int) ([]string, error) {
	if nodes == nil {
		return nil, errors.New("Nodes collection is nil")
	}

	if members := nodes.GetNodes(); members != nil {
		if len(members) == 0 {
			err := errors.New("List of members list is empty")
			log.WithFields(logFields).Error(err.Error())
			return nil, err
		}

		if numDirectRecipients < 0 {
			numDirectRecipients = 1
		}

		if numDirectRecipients > len(members) {
			log.WithFields(logFields).Warnln(
				`Number of direct recipients is greater than number of available members in the network.
				Sets the number of direct recipients equal to number of available members`)
			numDirectRecipients = len(members)
		}

		if len(members) < 4 {
			log.WithFields(logFields).Warnf("Number of members is less than 4. Setting sigma to 1")
			numDirectRecipients = 1
		}

		log.WithFields(logFields).Infof("Selected %d members as multicast recipients", numDirectRecipients)
		result, err := filterRandomMembers(members, numDirectRecipients)
		if err != nil {
			log.WithFields(logFields).Error(err.Error())
			return nil, err
		}

		return result, nil
	} else {
		return nil, errors.New("Members collection is nil")
	}
}

// Returns a list of members that are evenly distributed across all geographical regions
// TODO: fix me
func (m *membershipManager) geographicallyDistributedMembers(nodes *pb.Nodes, numDirectRecipients int) ([]string, error) {
	if nodes == nil {
		return nil, errors.New("Nodes collection is nil")
	}

	if members := nodes.GetNodes(); members != nil {
		if len(members) == 0 {
			err := errors.New("List of members list is empty")
			log.WithFields(logFields).Error(err.Error())
			return nil, err
		}

		// Note: Should define values of numDirectRecipients more clearly
		if numDirectRecipients < 0 || numDirectRecipients > len(members) {
			numDirectRecipients = len(members)
		}

		// Filter the members by geographical regions
		geoRegions := membersByGeoZone(members)
		result := make([]string, 0)

		log.Printf("Number of members: %d", len(members))
		log.Printf("Sigma: %d", numDirectRecipients)
		log.Println("Number of geo regions:", len(geoRegions))

		// Simple case: number of zones equals sigma. Select one random member from each zone
		if len(geoRegions) == numDirectRecipients {
			log.WithFields(logFields).Printf("Selecting %d members from %d geographical zones", numDirectRecipients, len(geoRegions))
			
			// Select exactly one random member from each geographical region
			for _, g := range geoRegions {
				randomMemberCollection, err := filterRandomMembers(g.members, 1)
				if err != nil {
					log.WithFields(logFields).Error(err.Error())
					return nil, err
				}
				result = append(result, randomMemberCollection...)
			}
		} else if len(geoRegions) > numDirectRecipients {
			// Get all geographical regions by string
			geoZonesKeys := make([]string, len(geoRegions))
			i := 0
			for key := range geoRegions {
				geoZonesKeys[i] = key
				i++
			}
	
			// 'geoIndices' is the collection of indices used to fetch all random geographical regions. 
			geoIndices := randomUniqueNumbers(0, len(geoRegions), numDirectRecipients)

			for _, r := range geoIndices {
				// Because len(geoMembers) > numDirectRecipients, we fetch exactly one random member from a subset of geoMembers9
				randomMemberCollection, err := filterRandomMembers(geoRegions[geoZonesKeys[r]].members, 1)
				if err != nil {
					log.WithFields(logFields).Error(err.Error())
					return nil, err
				}
				result = append(result, randomMemberCollection...)
			}
		} else {
			// Advanced shit
			panic("len(geoRegions) < numDirectRecipients")

			addedMembers := 0
			membersPerRegion := numDirectRecipients / len(geoRegions)
			log.Println("membersPerRegion:", membersPerRegion)

			// Select numDirectRecipients from geoRegions
			for _, g := range geoRegions {
				randomMemberCollection, err := filterRandomMembers(g.members, membersPerRegion)
				if err != nil {
					log.WithFields(logFields).Error(err.Error())
					return nil, err
				}
				result = append(result, randomMemberCollection...)
				addedMembers += membersPerRegion
			}

			log.Println("addedMembers:", addedMembers)

			// Add remaining members, if any
			if addedMembers < numDirectRecipients {
				panic("Missing members")
			}
			log.Println("Members:", result)
			
		}
		
		if len(result) == 0 {
			log.WithFields(logFields).Debug("No recipients in list!")
		}

		log.Println("Members:", result)

		return result, nil
	} else {
		return nil, errors.New("Network nodes collection is nil")
	}
}

// Returns a list of strings of length n from the collection of nodes.
// Returns a non-nil error if n > len(nodes)
func filterRandomMembers(nodes []*pb.Node, n int) ([]string, error) {
	if nodes == nil {
		return nil, errors.New("Network node collection is nil")
	}

	if n > len(nodes) {
		return nil, fmt.Errorf("N cannot be greater than length of members list. N = %d, number of members = %d", n, len(nodes))
	}

	// Find all keys
	keys := make([]int, len(nodes))
	for idx := range nodes {
		keys[idx] = idx
		idx++
	}

	result := make([]string, 0)
	randNums := randomUniqueNumbers(0, len(nodes), n)

	for _, i := range randNums {
		result = append(result, nodes[keys[i]].GetIfritAddress())
	}
	return result, nil
}

// Filters each node in the protobuf Node slice by regions. The result is a map
// with region as key and geoRegion as value.
func membersByGeoZone(members []*pb.Node) map[string]*geoRegion {
	regionsMap := make(map[string]*geoRegion)

	for _, m := range members {
		region, exists := regionsMap[m.GetGeoZone()]
		if !exists {
			newRegion := &geoRegion{
				regionName: m.GetGeoZone(),
				members: make([]*pb.Node, 0),
			}
			regionsMap[m.GetGeoZone()] = newRegion
			newRegion.members = append(newRegion.members, m)
			continue
		}
		region.members = append(region.members, m)
	}

	return regionsMap
}

// Returns a set of n intergers in the range [min, max>
// Note: see the Birthday problem at https://en.wikipedia.org/wiki/Birthday_problem 🥳
func randomUniqueNumbers(min, max, n int) []int {
	rand.Seed(time.Now().UnixNano())
	results := make([]int, 0, n)
	generated := make(map[int]bool)
	counter := 0

	for {
		if counter == n {
			break
		}
		i := rand.Intn(max-min) + min
		if !generated[i] {
			generated[i] = true
			results = append(results, i)
			counter++
			continue
		}
	}

	return results
}

// Returns true if the given ip address is ignored, returns false otherwise
func (m *membershipManager) ipIsIgnored(ip string) bool {
	m.ignoredIPLock.RLock()
	defer m.ignoredIPLock.RUnlock()

	_, ok := m.ignoredIPAddresses[ip]
	return ok
}
