package multicast

import (
	"fmt"
	"errors"
	"context"
	"github.com/arcsecc/lohpi/core/message"
	pb "github.com/arcsecc/lohpi/protobuf"
	pbtime "google.golang.org/protobuf/types/known/timestamppb"
	"github.com/golang/protobuf/proto"
	"container/list"
	"github.com/joonnna/ifrit"
	log "github.com/sirupsen/logrus"
	"github.com/jackc/pgx/v4/pgxpool"
	"sync"
	//"strings"
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
	errInvalidTau   = errors.New("Invalid value for tau")
	ErrNoSQLConnString = errors.New("PSQL connection string is empty")
	ErrNoID = errors.New("Identifier was not supplied to multicast manager")
)

var logFields = log.Fields{
	"package": "core/policy/multicast",
	"description": "message dissemination",
}

// configuration for network multicasts. Should not be written to from outside this package
type MulticastConfig struct {
	// The number of recipients of direct messages
	MulticastDirectRecipients int // sigma. Dynamic

	// Level of consistency
	AcceptanceLevel float64 // static
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

	// List of to-be-sent policies
	policyList *list.List
	policyListMutex sync.RWMutex

	// Timer that fires each time a multicast can be executed
	multicastTimer *time.Timer

	// Sends messages to the network
	ifritClient *ifrit.Client

	// Configuration
	configLock sync.RWMutex
	config     *MulticastConfig

	memManager *membershipManager

	// Probing manager
	prbManager *probeManager

	sequenceNumber int32

	// Database-related fields
	pool *pgxpool.Pool
	schema string
	policyDisseminationTable string
	directMessageLookupTable string

	isMulticasting bool
	isMulticastingMutex sync.RWMutex
}

// Used to fetch members from membership mananger
type memberFetch struct {
	members []string
	err     error
}

// Returns a new MulticastManager, given the Ifrit node and the configuration. Sigma and tau are initial values
// that may be changed after initiating probing sessions. The values will be adjusted if they are higher than they should be.
func NewMulticastManager(ifritClient *ifrit.Client, acceptanceLevel float64, MulticastDirectRecipients int, connString string, id string) (*MulticastManager, error) {
	if connString == "" {
		return nil, ErrNoSQLConnString
	}

	if id == "" {
		return nil, ErrNoID
	}

	poolConfig, err := pgxpool.ParseConfig(connString)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		return nil, err
	}

	poolConfig.MaxConnLifetime = time.Second * 10
	poolConfig.MaxConnIdleTime = time.Second * 4
	poolConfig.MaxConns = 100
	poolConfig.HealthCheckPeriod = time.Second * 1
	poolConfig.LazyConnect = false

	pool, err := pgxpool.ConnectConfig(context.Background(), poolConfig)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		return nil, err
	}

	m := &MulticastManager{
		policyList: list.New(),
		policyListMutex: sync.RWMutex{},
		ifritClient: ifritClient,

		config: &MulticastConfig{
			MulticastDirectRecipients: MulticastDirectRecipients,
			AcceptanceLevel: acceptanceLevel,
		},

		configLock: sync.RWMutex{},
		memManager: newMembershipManager(ifritClient),
		prbManager: newProbeManager(ifritClient),
		sequenceNumber: 0,
		pool: pool,
		schema: id + "_schema",
		policyDisseminationTable: id + "_policy_dissemination_table",
		directMessageLookupTable: id + "_direct_message_lookup_table",
	}

	log.WithFields(logFields).
		Debugf("Created new multicast manager with the following configuration:\nMulticast recipients: %s\nAcceptance level: %d\nInitial sequence number: %d\n", MulticastDirectRecipients, acceptanceLevel, m.sequenceNumber)

	m.prbManager.start()

	return m, nil
}

// Returns the current multicast configruation
func (m *MulticastManager) GetMulticastConfiguration() *MulticastConfig {
	m.configLock.RLock()
	defer m.configLock.RUnlock()
	return m.config
}

// Sets a new configuration for this multicast manager
func (m *MulticastManager) SetMulticastConfiguration(config *MulticastConfig) {
	m.configLock.Lock()
	defer m.configLock.Unlock()
	m.config = config
}



// Sends a batch of messages, given the messaging mode. The call will block until
// all messages have been dispatched. The caller is responsible to wait until probing is finished
// to avoid inconsistency. See IsProbing() in this package.
func (m *MulticastManager) Multicast(c *Config) error {
	m.setIsMulticasting(true)
	defer m.setIsMulticasting(false)
	return m.multicast(c)
}

// Registers the given message as a probe message
func (m *MulticastManager) RegisterProbeMessage(msg *pb.Message) {
	m.prbManager.probeChan <- *msg
}

// Probes the network to adjust the parameters
func (m *MulticastManager) ProbeNetwork(mode MessageMode) error {
	// Fetch the current probing configuration
	config := m.GetMulticastConfiguration()

	// Fetch a new configuration from the probing session
	newConfig, err := m.prbManager.probeNetwork(config, mode)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		return err
	}

	// Set a new configration gained from the probing session
	if newConfig != nil {
		m.SetMulticastConfiguration(newConfig)
		log.Println("New config after probe:", m.GetMulticastConfiguration())
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

func (m *MulticastManager) getMessageRecipients(members []string, mode MessageMode, directRecipients int) ([]string, error) {
	switch mode {
	case LruMembers:
		return m.memManager.lruMembers(members, directRecipients)
	case RandomMembers:
		return m.memManager.randomMembers(members, directRecipients)
	default:
		log.WithFields(logFields).Errorf("Unsupported mode: %s", mode)
		break
	}

	return nil, fmt.Errorf("Unsupported mode: %s", mode)
}

// FINISH ME
func (m *MulticastManager) multicast(c *Config) error {
	if c == nil {
		return errors.New("Multicast configuration is nil")
	}

	// Empty channel. Nothing to do
	if m.policyList.Len() == 0 {
		log.WithFields(logFields).Info("Policy batch list is empty -- nothing to do")
		return nil
	}

	members, err := m.getMessageRecipients(c.Members, c.Mode, m.config.MulticastDirectRecipients)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		return err
	}

	gossipChunks := make([]*pb.GossipMessageBody, 0)

	m.policyListMutex.RLock()
	defer m.policyListMutex.RUnlock()

	// TODO: avoid sizes exceeding 4MB
	for e := m.policyList.Front(); e != nil; e = e.Next() {
		p := e.Value.(*pb.Policy)
		gossipChunk := &pb.GossipMessageBody{
			Policy: p,
		}

		gossipChunks = append(gossipChunks, gossipChunk)
		if err := m.dbInsertDirectMessage([]byte(m.ifritClient.Id()), int(m.sequenceNumber), p.GetDatasetIdentifier(), p.GetVersion()); err != nil {
			log.WithFields(logFields).Error(err.Error())
		}

		m.policyList.Remove(e)
	}

	// TODO: store me somewhere persistently 
	m.sequenceNumber += 1
	msg := &pb.Message{
		Type: message.MSG_TYPE_POLICY_STORE_UPDATE,
		GossipMessage: &pb.GossipMessage{
			Sender:				"Polciy store",
			MessageType:		message.GOSSIP_MSG_TYPE_POLICY,
			DateSent:			pbtime.Now(),
			GossipMessageBody:  gossipChunks,
			GossipMessageID: 	&pb.GossipMessageID{
				PolicyStoreID: 		[]byte(m.ifritClient.Id()), // use another encoding than UTF-8?
				SequenceNumber: 	m.sequenceNumber,
			},
		},
	}

	data, err := proto.Marshal(msg)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		return err
	}

	// Sign the chunks
	r, s, err := m.ifritClient.Sign(data)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		return err
	}

	// Message with signature
	msg.Signature = &pb.MsgSignature{R: r, S: s}

	// Marshalled message to be multicasted
	data, err = proto.Marshal(msg)
	if err != nil {
		log.WithFields(logFields).Error(err.Error())
		return err
	}

	wg := sync.WaitGroup{}

	currentConfig := m.GetMulticastConfiguration()
	log.WithFields(logFields).
		WithField("action", "multicast policy batch").
		Infof("Multicast manager will send multicast messages using the following configurations:\nMulticast recipients: %s\nAcceptance level: %f\nInitial sequence number: %d\nMessage size: %d",
			members, currentConfig.AcceptanceLevel, m.sequenceNumber, len(data))

	// Multicast messages in parallel
	for _, mem := range members {
		member := mem
		wg.Add(1)
		go func() {
			defer wg.Done()
			log.WithFields(logFields).
				WithField("action", "multicast policy batch").
				Infof("Sending policy batch to remote recipient at address '%s'", member)
			
			m.ifritClient.SendTo(member, data)
			now := time.Now()
		
			if err := m.dbInsertGossipBatch(member, msg.GetGossipMessage(), now, []byte(m.ifritClient.Id())); err != nil {
				log.WithFields(logFields).Error(err.Error())
			}
		}()
	}

	wg.Wait()

	return nil
}

func (m *MulticastManager) InsertPolicy(p *pb.Policy) {
	m.policyListMutex.Lock()
	defer m.policyListMutex.Unlock()
	m.policyList.PushBack(p)
}

// FINISH ME
func (m *MulticastManager) IsProbing() bool {
	return m.prbManager.IsProbing()
}

func (m *MulticastManager) IsMulticasting() bool { 
	m.isMulticastingMutex.RLock()
	defer m.isMulticastingMutex.RUnlock()
	return m.isMulticasting
}

func (m *MulticastManager) setIsMulticasting(b bool) { 
	m.isMulticastingMutex.Lock()
	defer m.isMulticastingMutex.Unlock()
	m.isMulticasting = b	
}