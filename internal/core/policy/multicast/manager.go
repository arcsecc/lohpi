package multicast

import (
	"log"
	"time"
	"sync"

	"github.com/joonnna/ifrit"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/proto"
	"github.com/tomcat-bit/lohpi/internal/core/message"
	pb "github.com/tomcat-bit/lohpi/protobuf"
)

// configuration for network multicasts. Should not be written to from outside this package
type MulticastConfig struct {
	// The number of recipients of direct messages
	MulticastDirectRecipients int 		// sigma. Dynamic

	// The time between each message multicast
	multicastInterval time.Duration		// tau	Dynamic
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

	// Sends probe messages to the network
	ifritClient *ifrit.Client

	// Configuration
	config *MulticastConfig
	configLock sync.RWMutex
}

// Returns a new MulticastManager, given the Ifrit node and the configuration. if config is nil,
// use the default configuration.
func NewMulticastManager(ifritClient *ifrit.Client, config *MulticastConfig) *MulticastManager {	
	m := &MulticastManager{
		PolicyChan:	make(chan pb.Policy, 100),

		multicastTimer:	time.NewTimer(10000 * time.Millisecond),
		mcLock:			sync.RWMutex{},

		ifritClient: 	ifritClient,
		configLock:		sync.RWMutex{},
	}

	// Multicast configuration
	if m.config == nil {
		m.config = &MulticastConfig{
			MulticastDirectRecipients: 	1,
			multicastInterval: 			time.Duration(10 * time.Second),
		}
	}

	return m
}

// Returns the current multicast configruation 
func (m *MulticastManager) MulticastConfiguration() MulticastConfig {
	m.configLock.RLock()
	defer m.configLock.RUnlock()
	return *m.config
}

// Sets a new configuration for this multicast manager
func (m *MulticastManager) SetMulticastConfiguration(config *MulticastConfig) {
	m.configLock.Lock()
	defer m.configLock.Unlock()
	m.config = config
}

func (m *MulticastManager) MulticastTimer() *time.Timer {
	m.mcLock.RLock()
	defer m.mcLock.RUnlock()
	return m.multicastTimer
}


// Sends a batch of messages to the least recently used members in the network
func (m *MulticastManager) Multicast(members []string) error {
	// Empty channel. Nothing to do 
	if len(m.PolicyChan) == 0 {
		log.Println("Queue is empty, nothing to send")
		return nil
	}

	gossipChunks := make([]*pb.GossipMessageBody, 0)
	// TODO: avoid sizes exceeding 4MB
	for p := range m.PolicyChan {
		if len(m.PolicyChan) == 0 {
			break
		}	

		gossipChunk := &pb.GossipMessageBody{
			//string object = 1;          // subject or study. Appliy the policy to the object
    		//uint64 version = 2;         // Version numbering 
			Policy: &p,
			//google.protobuf.Timestamp timestamp = 4; // Time at policy store at the time of arrival
		}

		gossipChunks = append(gossipChunks, gossipChunk)
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
		Signature: &pb.Signature{
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
			m.ifritClient.SendTo(member, data)
			wg.Done()
		}()
	}

	wg.Wait()

	// Reset the timers when all messages have been sent
	currentConfig := m.MulticastConfiguration()
	m.multicastTimer.Reset(currentConfig.multicastInterval)
	
	return nil 
}

// Registers the given message as a probe message
func (m *MulticastManager) RegisterProbeMessage(msg *pb.Message) {
	log.Println("Implement RegisterProbeMessage()")
}

// Sends a batch of messages to a random set of members
func (m *MulticastManager) SendToRandomMembers() error {
	log.Println("Implement SendToRandomMembers()")
	return nil
}

func (m *MulticastManager) Stop() {
	
}
