package multicast

import (
	"errors"
	"github.com/arcsecc/lohpi/core/message"
	pb "github.com/arcsecc/lohpi/protobuf"
	pbtime "google.golang.org/protobuf/types/known/timestamppb"
	"github.com/golang/protobuf/proto"
	"github.com/joonnna/ifrit"
	log "github.com/sirupsen/logrus"
	"sync"
	"strings"
	"time"
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
	errInvalidTau   = errors.New("")
)

// configuration for network multicasts. Should not be written to from outside this package
type multicastConfig struct {
	// The number of recipients of direct messages
	multicastDirectRecipients int // sigma. Dynamic

	// The time between each message multicast
	multicastInterval time.Duration // tau	Dynamic

	// Level of consistency
	acceptanceLevel float64 // static
}

type Config struct {
	// Sets the behaviour of the message passing
	Mode MessageMode

	// The members at the time the message passing is invoked
	Members []string
}

// Maintains all gossip-related events
type MulticastManager struct {
	// String identifier used to identify the user as seen by other network entities
	id string

	// Channel into which policies are inserted to be multicasted in the next batch
	PolicyChan chan pb.Policy

	// Timer that fires each time a multicast can be executed
	multicastTimer *time.Timer
	mcLock         sync.RWMutex
	//networkLock 	sync.Mutex

	// Sends messages to the network
	ifritClient *ifrit.Client

	// Configuration
	configLock sync.RWMutex
	config     *multicastConfig

	memManager *membershipManager

	// Probing manager
	prbManager *probeManager

	sequenceNumber int32
}

// Used to fetch members from membership mananger
type memberFetch struct {
	members []string
	err     error
}

// Returns a new MulticastManager, given the Ifrit node and the configuration. Sigma and tau are initial values
// that may be changed after initiating probing sessions. The values will be adjusted if they are higher than they should be.
func NewMulticastManager(ifritClient *ifrit.Client, acceptanceLevel float64, multicastDirectRecipients int) (*MulticastManager, error) {
	m := &MulticastManager{
		PolicyChan:     make(chan pb.Policy, 100),
		multicastTimer: time.NewTimer(1 * time.Second),
		mcLock:         sync.RWMutex{},
		ifritClient:    ifritClient,

		config: &multicastConfig{
			multicastDirectRecipients: multicastDirectRecipients,
			acceptanceLevel:           acceptanceLevel,
		},

		configLock: sync.RWMutex{},
		memManager: newMembershipManager(ifritClient),
		prbManager: newProbeManager(ifritClient),
		sequenceNumber: 0,
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

// Sends a batch of messages, given the messaging mode. The call will block until
// all messages have been dispatched. The caller is responsible to wait until probing is finished
// to avoid inconsistency. See IsProbing() in this package.
func (m *MulticastManager) Multicast(c *Config) error {
	return m.multicast(c)
}

// Registers the given message as a probe message
func (m *MulticastManager) RegisterProbeMessage(msg *pb.Message) {
	m.prbManager.probeChan <- *msg
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

func (m *MulticastManager) getMessageRecipients(members []string, mode MessageMode, directRecipients int) ([]string, error) {
	switch mode {
	case LruMembers:
		return m.memManager.lruMembers(members, directRecipients)
	case RandomMembers:
		return m.memManager.randomMembers(members, directRecipients)
	default:
		break
	}

	return nil, errors.New("Unknown mode!")
}

// FINISH ME
func (m *MulticastManager) multicast(c *Config) error {
	if c == nil {
		return errors.New("Multicast configuration is nil")
	}

	// Empty channel. Nothing to do
	if len(m.PolicyChan) == 0 {
		log.Infoln("Multicast queue is empty. Aborting message passing")
		return nil
	}

	members, err := m.getMessageRecipients(c.Members, c.Mode, m.config.multicastDirectRecipients)
	if err != nil {
		return err
	}

	// Pack all chunks together in a batch
	gossipChunks := make([]*pb.GossipMessageBody, 0)
	// TODO: avoid sizes exceeding 4MB

	// TODO: prune reduntant chunks
	for p := range m.PolicyChan {
		gossipChunk := &pb.GossipMessageBody{
			Policy: &p,
		}

		gossipChunks = append(gossipChunks, gossipChunk)

		if len(m.PolicyChan) == 0 {
			break
		}
	}

	m.sequenceNumber += 1
	msg := &pb.Message{
		Type: message.MSG_TYPE_POLICY_STORE_UPDATE,
		GossipMessage: &pb.GossipMessage{
			Sender:				"Polciy store",
			MessageType:		message.GOSSIP_MSG_TYPE_POLICY, // Don't really need this one because the outtermost Message type already has a type specifier
			DateSent:			pbtime.Now(),
			GossipMessageBody:  gossipChunks,
			GossipMessageID: 	&pb.GossipMessageID{
				PolicyStoreID: 		strings.ToValidUTF8(m.ifritClient.Id(), ""), // use another encoding than UTF-8?
				SequenceNumber: 	m.sequenceNumber,
			},
		},
	}

	data, err := proto.Marshal(msg)
	if err != nil {
		panic(err)
		return err
	}

	// Sign the chunks
	r, s, err := m.ifritClient.Sign(data)
	if err != nil {
		panic(err)
		return err
	}

	// Message with signature
	msg.Signature = &pb.MsgSignature{R: r, S: s}

	// Marshalled message to be multicasted
	data, err = proto.Marshal(msg)
	if err != nil {
		panic(err)
		return err
	}

	wg := sync.WaitGroup{}

	// Multicast messages in parrallel
	for _, mem := range members {
		member := mem
		wg.Add(1)
		go func() {
			defer wg.Done()
			log.Infof("Sending policy batch to member '%s'\n", member)
			m.ifritClient.SendTo(member, data)
		}()
	}

	wg.Wait()

	return nil
}

// FINISH ME
func (m *MulticastManager) IsProbing() bool {
	return m.prbManager.IsProbing()
}
