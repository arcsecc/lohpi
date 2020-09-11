package multicast

import (
	"errors"
	"log"
	"time"
	"sync"

	"github.com/joonnna/ifrit"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/proto"
	"github.com/tomcat-bit/lohpi/pkg/message"
	pb "github.com/tomcat-bit/lohpi/protobuf"
)

type MessageMode byte

const (
	// Send direct messages to a subset of the least recently used members
	LruMembers = 'A'

	// Send direct messages to a random set of members
	RandomMembers = 'B'
)

var (
	errInvalidSigma = errors.New("Sigma must be greater than zero")
	errInvalidTau = errors.New("")
)

// configuration for network multicasts. Should not be written to from outside this package
type multicastConfig struct {
	// The number of recipients of direct messages
	multicastDirectRecipients int 		// sigma. Dynamic

	// The time between each message multicast
	multicastInterval time.Duration		// tau	Dynamic

	// Level of consistency 
	acceptanceLevel float64
}

// Maintains all gossip-related events
type MulticastManager struct {
	// String identifier used to identify the user as seen by other network entities
	id string

	// Channel into which policies are inserted to be multicasted in the next batch
	PolicyChan chan pb.Policy

	// Timer that fires each time a multicast can be executed
	multicastTimer 	*time.Timer
	mcLock			sync.RWMutex
	//networkLock 	sync.Mutex

	// Sends messages to the network
	ifritClient *ifrit.Client

	// Configuration
	configLock sync.RWMutex
	config *multicastConfig

	memManager *membershipManager

	// Probing manager
	prbManager *probeManager
}

// Used to fetch members from membership mananger 
type memberFetch struct {
	members []string 
	err error
}

// Returns a new MulticastManager, given the Ifrit node and the configuration. Sigma and tau are initial values
// that may be changed after initiating probing sessions. The values will be adjusted if they are higher than they should be.
func NewMulticastManager(ifritClient *ifrit.Client, acceptanceLevel float64, multicastInterval time.Duration) (*MulticastManager, error) {	
	m := &MulticastManager{
		PolicyChan:	make(chan pb.Policy, 100),
		multicastTimer:	time.NewTimer(1 * time.Second),
		mcLock:			sync.RWMutex{},

		ifritClient: 	ifritClient,

		config: &multicastConfig {
			multicastDirectRecipients: 	1,
			multicastInterval: 			multicastInterval,
			acceptanceLevel:			acceptanceLevel,
		},

		configLock:		sync.RWMutex{},
		memManager: 	newMembershipManager(ifritClient),
		prbManager:		newProbeManager(ifritClient),
	}

	m.prbManager.start()

	return m, nil
}

// Returns the current multicast configruation 
func (m *MulticastManager) multicastConfiguration() multicastConfig {
	m.configLock.RLock()
	defer m.configLock.RUnlock()
	return *m.config
}

// Sets a new configuration for this multicast manager
func (m *MulticastManager) setMulticastConfiguration(config *multicastConfig) {
	m.configLock.Lock()
	defer m.configLock.Unlock()
	m.config = config
}

func (m *MulticastManager) MulticastTimer() *time.Timer {
	m.mcLock.RLock()
	defer m.mcLock.RUnlock()
	return m.multicastTimer
}

// Sends a batch of messages, given the messaging mode. The call will block until
// all messages have been dispatched. The caller is responsible to wait until probing is finished
// to avoid inconsistency. See IsProbing() in this package.
func (m *MulticastManager) Multicast(mode MessageMode) error {
	return m.multicast(mode)
}

// Registers the given message as a probe message
func (m *MulticastManager) RegisterProbeMessage(msg *pb.Message) {
	m.prbManager.probeChan <- *msg
}

// Sends a batch of messages to a random set of members
func (m *MulticastManager) SendToRandomMembers() error {
	log.Println("Implement SendToRandomMembers()")
	return nil
}

func (m *MulticastManager) SetIgnoredIfritNodes(ignoredIPs map[string]string) {
	// Hacky af...
	m.memManager.setIgnoredIfritNodes(ignoredIPs)
	m.prbManager.setIgnoredIfritNodes(ignoredIPs)
}

// Probes the network to adjust the parameters
func (m *MulticastManager) ProbeNetwork(mode MessageMode) error {
	// Fetch the current probing configuration
	config := m.multicastConfiguration()
		
	// Fetch a new configuration from the probing session
	newConfig, err := m.prbManager.probeNetwork(&config, mode)
	if err != nil {
		log.Println(err.Error())
		return err
	}

	// Set a new configration gained from the probing session
	if newConfig != nil {
		m.setMulticastConfiguration(newConfig)
		log.Println("New config after probe:", m.multicastConfiguration())
	}

	return nil
}

func (m *MulticastManager) StopProbing() {
	m.prbManager.Stop()
}

// FINISH ME
func (m *MulticastManager) Stop() {
	log.Println("Stopping multicast manager")
	m.prbManager.Stop()
}

func (m *MulticastManager) ResetMulticastTimer() {
	currentConfig := m.multicastConfiguration()
	m.multicastTimer.Reset(currentConfig.multicastInterval)
}

// FINISH ME
func (m *MulticastManager) getMembers(mode MessageMode, directRecipients int) ([]string, error) {
	switch mode {
	case LruMembers:
		return m.memManager.lruMembers(directRecipients)
	case RandomMembers:
		log.Fatal("Implement me")
	default:
		log.Fatalf("Unknown mode: %s\n", mode)
	}

	return nil, errors.New("Unknown mode!")
}

// FINISH ME
func (m *MulticastManager) multicast(mode MessageMode) error {
	members, err := m.getMembers(mode, 2)
	if err != nil {
		return err
	}

	// Empty channel. Nothing to do 
	if len(m.PolicyChan) == 0 {
		return nil
	}

	gossipChunks := make([]*pb.GossipMessageBody, 0)
	// TODO: avoid sizes exceeding 4MB

	log.Println("BEfore sending batch, length is", len(m.PolicyChan))
	for p := range m.PolicyChan {
		gossipChunk := &pb.GossipMessageBody{
			//string object = 1;          // subject or study. Appliy the policy to the object
			//uint64 version = 2;         // Version numbering 
			Policy: &p,
			//google.protobuf.Timestamp timestamp = 4; // Time at policy store at the time of arrival
		}

		gossipChunks = append(gossipChunks, gossipChunk)

		if len(m.PolicyChan) == 0 {
			break
		}	
	}

	timestamp := ptypes.TimestampNow()
	msg := &pb.Message{
		Type: message.MSG_TYPE_POLICY_STORE_UPDATE,
		GossipMessage: &pb.GossipMessage{
			Sender: 				"Policy store",				// Do we really need it?
			MessageType: 			message.GOSSIP_MSG_TYPE_POLICY,
			Timestamp: 				timestamp,
			GossipMessageBody:		gossipChunks,
		},
	}

	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	// Sign the chunks
	r, s, err := m.ifritClient.Sign(data)
	if err != nil {
		return err
	}

	// Message with signature
	msg = &pb.Message{
		Type: message.MSG_TYPE_POLICY_STORE_UPDATE,
		Signature: &pb.MsgSignature{
			R: r,
			S: s,
		},
		GossipMessage: &pb.GossipMessage{
			Sender: 				"Policy store",
			MessageType: 			message.GOSSIP_MSG_TYPE_POLICY,
			Timestamp: 				timestamp,
			GossipMessageBody:		gossipChunks,
		},
	}

	// Marshalled message to be multicasted
	data, err = proto.Marshal(msg)
	if err != nil {
		return nil
	}

	wg := sync.WaitGroup{}

	// Multicast messages in parrallel
	for _, mem := range members {
		member := mem
		wg.Add(1)
		go func() {
			log.Println("Sending msg", msg)
			m.ifritClient.SendTo(member, data)
			wg.Done()
		}()
	}

	wg.Wait()

	return nil 
}

// FINISH ME
func (m *MulticastManager) IsProbing() bool {
	return m.prbManager.IsProbing()
}