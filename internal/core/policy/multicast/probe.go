package multicast

import (
	"bytes"
	"errors"
	"math"
	"sync"
	"time"
	"log"
	
	"github.com/joonnna/ifrit"
	"github.com/tomcat-bit/lohpi/internal/core/message"
	"github.com/golang/protobuf/proto"
	pb "github.com/tomcat-bit/lohpi/protobuf"
)

var (
	errNoMode 					= errors.New("No membership mode given")
	errInvalidMessageMode		= errors.New("Invalid message mode given")
)

// Describes each probing round. Should be set at the beginning of each probing round
type ackRound struct {
	// Lohpi string identifier mapped to a Probe message
	table map[string]*pb.Probe

	// Timestamp
	timestamp time.Time

	// Current configuration
	config ProbeConfig

	// Monotonically increasing order number
	order uint32

	// Network members to which probe messages are sent
	recipients []string
}

// Configuration used to probe the network. Should be set explicitly from outside. 
type ProbeConfig struct {
	// Number of storage nodes in the network
	NumMembers int

	// Percentage of network nodes needed to achieve consistency
	AcceptanceLevel float64

	// Unique session ID
	SessionID []byte

	// Time between probing rounds. Fetched from this package
	MulticastInterval time.Duration

	// The number of recipients of direct messages
	MulticastDirectRecipients int 		// sigma. Dynamic
	
	// Messaging mode
	Mode MessageMode
}

type ProbeManager struct {
	//Upon receiving an acknowledgment, insert it into the channel to register it
	ProbeChan chan pb.Message
	
	// Stop signal
	stopAck chan bool

	// Sends probe messages to the network
	ifritClient *ifrit.Client

	// Probing acknowledgment-related stuff
	rounds []*ackRound
	roundsLock sync.RWMutex

	currentRound *ackRound
	currentRoundLock sync.RWMutex

	// Signaled when phi acks have arrived
	allAcksChan chan bool

	// Configuration used when probing network
	ProbeConfig ProbeConfig
	probeConfigLock sync.RWMutex

	// Multicast configuration
	multicastConfig MulticastConfig
	mcConfigLock sync.RWMutex

	// Percentage of nodes we require to receive the messages
	acceptanceLevel float64

	isProbing bool
	isProbingMutex	sync.RWMutex

	// Membership manager 
	memManager *membershipManager
}

func NewProbeManager(ifritClient *ifrit.Client, acceptanceLevel float64) *ProbeManager {
	return &ProbeManager{
		ProbeChan:			make(chan pb.Message, 1000),
		allAcksChan:		make(chan bool, 1),
		stopAck: 			make(chan bool, 1),
		ifritClient:		ifritClient,
		rounds:				make([]*ackRound, 0),
		currentRoundLock:	sync.RWMutex{},
		probeConfigLock:	sync.RWMutex{},
		acceptanceLevel:	acceptanceLevel,
		mcConfigLock:		sync.RWMutex{},
		isProbingMutex:		sync.RWMutex{},
		memManager: newMembershipManager(ifritClient),
	}
}

func (p *ProbeManager) Start() {
	go p.probeAcknowledger()
}

func (p *ProbeManager) IsProbing() bool {
	p.isProbingMutex.RLock()
	defer p.isProbingMutex.RUnlock()
	return p.isProbing
}

// Probes the network by multicasting a probing messages to the given members. 
// It uses the current configuration assoicated with the probing manager.
func (p *ProbeManager) ProbeNetwork(config ProbeConfig) (*MulticastConfig, error) {
	p.isProbingMutex.Lock()
	p.isProbing = true
	p.isProbingMutex.Unlock()

	defer func() {
		p.isProbingMutex.Lock()
		p.isProbing = false
		p.isProbingMutex.Unlock()
	}()

	// Fetch the latest configuration for this probing session
	if config.Mode == ' ' {
		config.Mode = RandomMembers
	}

	p.setProbeConfiguration(config)

	// Timer used to wait for acks
	ackTimer := time.NewTimer(config.MulticastInterval)	
		
	log.Println("At the start of probing session: config ->", config)

	order := 1
	consecutiveSuccesses := 0

	// Main probing loop
	for {//i := 0; i < 20; i++ {
		// Fetch the latest configurations
		probeConfig := p.probeConfiguration()
		recp, err := p.probeRoundRecipients(probeConfig.Mode, probeConfig.MulticastDirectRecipients)
		if err != nil {
			log.Println(err.Error())
		}

		// Make a new probe round and multicast it
		r := &ackRound{
			table:		make(map[string]*pb.Probe),
			config: 	probeConfig,
			order:		uint32(order),
			recipients: recp,
		}

		log.Println("probing round ->", r)

		if err := p.multicast(r); err != nil {
			return nil, err
		}

		ackTimer.Reset(probeConfig.MulticastInterval)
	
		// Lower limit in time frame, as opposed to the channel
		// signal from ackTimer
		lowerLimit := time.Now().Add(probeConfig.MulticastInterval / 2)
		upperLimit := time.Now().Add(probeConfig.MulticastInterval)

		select {
		case <-ackTimer.C:
			//log.Println("AckTimer expired")
			if !p.acknowledgmentsArrivedInTime() {
				log.Println("Before increasing config:", p.probeConfiguration())
				p.increaseProbeParameters(&config)
				p.setProbeConfiguration(config)
				log.Println("After increasing config:", p.probeConfiguration())
				consecutiveSuccesses = 0
			} else {
				consecutiveSuccesses++
			}
			ackTimer.Reset(probeConfig.MulticastInterval)
		
		// Signaled when all valid acks have arrived for this round.
		// It will always trigger when all acks for this rounds have arrived.
		case <-p.allAcksChan:
			//log.Println("Channel event: all acks have arrived")
			if nowIsBeforeLimit(lowerLimit) {
				log.Println("Before decreasing config:", p.probeConfiguration())
				p.decreaseProbeParameters(&config)
				p.setProbeConfiguration(config)
				log.Println("After decreasing config:", p.probeConfiguration())
			} else if nowIsBeforeLimit(upperLimit) {
				consecutiveSuccesses++
			}
			ackTimer.Reset(probeConfig.MulticastInterval)
		
		case <-p.stopAck:
			log.Println("Stopping prober...")
			break
		}

		if consecutiveSuccesses > 3 {
			// log config
			log.Println("At the end of probing session: config ->", p.probeConfiguration())
			break
		}

		order++
	}

	pConfig := p.probeConfiguration()

	return &MulticastConfig{
		MulticastDirectRecipients: pConfig.MulticastDirectRecipients,
		MulticastInterval:	pConfig.MulticastInterval,
	}, nil
}

// Sets the probing config 
func (p *ProbeManager) setProbeConfiguration(config ProbeConfig) {
	p.probeConfigLock.Lock()
	defer p.probeConfigLock.Unlock()
	p.ProbeConfig = config
}

func (p *ProbeManager) probeConfiguration() ProbeConfig {
	p.probeConfigLock.RLock()
	defer p.probeConfigLock.RUnlock()
	return p.ProbeConfig
}

// Sets the multicast configuration
func (p *ProbeManager) SetMulticastConfiguration(config MulticastConfig) {
	p.mcConfigLock.Lock()
	defer p.mcConfigLock.Unlock()
	p.multicastConfig = config
}

func (p *ProbeManager) MulticastConfiguration() MulticastConfig {
	p.mcConfigLock.RLock()
	defer p.mcConfigLock.RUnlock()
	return p.multicastConfig
}

// Multicasts a set of probe messages given the order number of the configuration
func (p *ProbeManager) multicast(round *ackRound) error {
	config := p.probeConfiguration()

	// Prepare message
	msg := &pb.Message{
		Type: message.MSG_TYPE_PROBE,
		Probe: &pb.Probe{
			Order: round.order,
			SessionId: config.SessionID,
		},
	}

	// Encode before signing message
	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	// Sign it
	r, s, err := p.ifritClient.Sign(data)
	if err != nil {
		return err
	}

	// Final message to be sent
	msg = &pb.Message{
		Type: message.MSG_TYPE_PROBE,
		Signature: &pb.Signature{
			R: r,
			S: s,
		},
		Probe: &pb.Probe{
			Order: round.order,
			SessionId: config.SessionID,
		},
	}

	// Marshal before multicast probe messages
	data, err = proto.Marshal(msg)
	if err != nil {
		return err
	}

	p.appendRound(round)
	p.setCurrentRound(round)

	// Multicast messages in parrallel
	wg := sync.WaitGroup{}
	for _, mem := range round.recipients {
		member := mem
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.ifritClient.SendTo(member, data)
		}()
	}

	wg.Wait()
	return nil
}

// Continuously listens for acknowledgments coming from the network
func (p *ProbeManager) probeAcknowledger() {
	for {
		select {
		case msg := <-p.ProbeChan:
			if !p.IsProbing() {
				continue
			}
			
			currentRound := p.getCurrentRound()
			order := currentRound.order
			config := p.probeConfiguration()
			
			if !p.isValidAcknowledgment(msg.GetProbe(), &config, order) {
				//log.Println("Required order", order, "got order", msg.GetProbe().GetOrder())
				continue
			}

			// Insert into map
			currentRound.table[msg.GetSender().GetName()] = msg.GetProbe()

			// Check if all acks have arrived. Signal if all have arrived
			n := config.NumMembers
			a := config.AcceptanceLevel
			if len(currentRound.table) >= nAcceptanceNodes(n, a) {
				p.allAcksChan <-true
			}
		case <-p.stopAck:
			log.Println("Stopping prober...")
			return
		}
	}
}

func (p *ProbeManager) getAckRounds() []*ackRound {
	p.roundsLock.RLock()
	defer p.roundsLock.RUnlock()
	return p.rounds
}

func (p *ProbeManager) setCurrentRound(r *ackRound) {
	p.currentRoundLock.Lock()
	defer p.currentRoundLock.Unlock()
	p.currentRound = r
}

func (p *ProbeManager) getCurrentRound() *ackRound {
	p.currentRoundLock.RLock()
	defer p.currentRoundLock.RUnlock()
	return p.currentRound
}

func (p *ProbeManager) appendRound(r *ackRound) {
	p.roundsLock.Lock()
	defer p.roundsLock.Unlock()
	p.rounds = append(p.rounds, r)
}

func (p *ProbeManager) isValidAcknowledgment(probe *pb.Probe, config *ProbeConfig, order uint32) bool {
	return probe.GetOrder() == order && bytes.Compare(probe.GetSessionId(), config.SessionID) == 0 
}

// Set tau to 0.75 of its original value
func (p *ProbeManager) decreaseProbeParameters(config *ProbeConfig) {
	p.probeConfigLock.Lock()
	defer p.probeConfigLock.Unlock()
	
	ms := float32(config.MulticastInterval.Milliseconds()) * 0.75
	newDuration := time.Duration(time.Duration(ms) * time.Millisecond)
	config.MulticastInterval = newDuration
}

func (p *ProbeManager) probeRoundRecipients(mode MessageMode, directRecipients int) ([]string, error) {
	log.Println("direct recipients:", directRecipients)
	switch mode {
	case LruMembers:
		return p.memManager.LruMembers(directRecipients)
	case RandomMembers:
		return p.memManager.RandomMembers()
	default:
		return nil, errInvalidMessageMode
	}
}

func (p *ProbeManager) acknowledgmentsArrivedInTime() bool {
	config := p.probeConfiguration()
	n := config.NumMembers
	a := config.AcceptanceLevel

	table := p.getCurrentRound().table
	
	if len(table) >= nAcceptanceNodes(n, a) {
		return true	
	}
	return false
}

// Increases the multicast parameters to process more load
// TODO: sigma preceeds tau
func (p *ProbeManager) increaseProbeParameters(config *ProbeConfig) {
	p.probeConfigLock.Lock()
	defer p.probeConfigLock.Unlock()
	
	// Phi is the acceptance level. Always increase sigma until
	// it reaches its upper bound of ceil((phi * n ) / 2)
	accLevel := config.AcceptanceLevel
	n := config.NumMembers
	directRecipients := config.MulticastDirectRecipients
	tau := config.MulticastInterval

	// If sigma already is at its upper bound, double tau.
	// Else, increase sigma as much as we can
	/*log.Println("Acc level:", accLevel)
	log.Println("N:", n)
	log.Println("Direct recipients:", directRecipients)
	log.Println("Tau:", tau.String())*/
	if directRecipients >= int(math.Ceil((accLevel * float64(n)) / 2)) {
		tau *= 2 // TODO: set upper bound on tau. Multiply by 1.5
		if tau >= time.Duration(1 * time.Hour) {
			tau = time.Duration(1 * time.Hour)
		}
	} else {
		if directRecipients > int(math.Ceil((accLevel * float64(n)) / 2)) {
			directRecipients = int(math.Ceil((accLevel * float64(n)) / 2))
		} else {
			directRecipients *= 2
		}
	}

	config.MulticastDirectRecipients = directRecipients
	config.MulticastInterval = tau
	//log.Println("After increasing:")
	//log.Println("config.MulticastDirectRecipients:", config.MulticastDirectRecipients)
	//log.Println("config.MulticastInterval:", config.MulticastInterval)
}

func (p *ProbeManager) SetIgnoredIfritNodes(ignoredIPs map[string]string) {
	p.memManager.setIgnoredIfritNodes(ignoredIPs)
}

func nAcceptanceNodes(n int, acceptanceLevel float64) int {
	return int(math.Ceil(float64(n) * acceptanceLevel))
}

// Returns true if now is before limit, returns false otherwise
func nowIsBeforeLimit(limit time.Time) bool {
	return time.Now().Unix() < limit.Unix()
}

// Returns true if now is after limit, returns false otherwise
func nowIsAfterLimit(limit time.Time) bool {
	return time.Now().Unix() > limit.Unix()
}