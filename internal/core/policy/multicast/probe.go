package multicast

import (
	"bytes"
	"math"
	"sync"
	"time"
	"log"
	
	"github.com/joonnna/ifrit"
	"github.com/tomcat-bit/lohpi/internal/core/message"
	"github.com/golang/protobuf/proto"
	pb "github.com/tomcat-bit/lohpi/protobuf"
)

// Table of acknowledgments and an order number
type ackRound struct {
	table map[string]*pb.Probe
	config ProbeConfig
	order uint32
}

// Each session requires one unique set of data
type ProbeConfig struct {
	Recipients []string
	NumMembers int
	AcceptanceLevel float64
	SessionID []byte
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
func (p *ProbeManager) ProbeNetwork(members []string) error {
	p.isProbingMutex.Lock()
	p.isProbing = true
	p.isProbingMutex.Unlock()

	defer func() {
		p.isProbingMutex.Lock()
		p.isProbing = false
		log.Println("Set isprobing to false")
		p.isProbingMutex.Unlock()
	}()

	// Fetch the latest configurations
	probeConfig := p.ProbeConfiguration()
	mcConfig := p.MulticastConfiguration()

	// Timer used to wait for acks
	ackTimer := time.NewTimer(mcConfig.multicastInterval)	
		
	log.Println("At the start of probing session: config ->", probeConfig)

	order := 1
	consecutiveSuccesses := 0

	// Main probing loop
	for i := 0; i < 20; i++ {
		// Fetch the latest configurations
		probeConfig := p.ProbeConfiguration()
		mcConfig := p.MulticastConfiguration()
	
		// Make a new probe round and multicast it
		r := &ackRound{
			table:	make(map[string]*pb.Probe),
			config: probeConfig,
			order:	uint32(order),
		}

		if err := p.multicast(r); err != nil {
			return err
		}

		ackTimer.Reset(mcConfig.multicastInterval)
	
		// Lower limit in time frame, as opposed to the channel
		// signal from ackTimer
		lowerLimit := time.Now().Add(mcConfig.multicastInterval / 2)

		select {
		// Signaled when all valid acks have arrived for this round.
		// It will always trigger when all acks for this rounds have arrived.
		case <-p.allAcksChan:
			log.Println("Channel event: all acks have arrived")
			if nowIsBeforeLimit(lowerLimit) {
				log.Println("Got acks before half time has elapsed")	
				newConfig := p.decreaseMulticastParameters()
				log.Println("Decreased config:", newConfig)
				p.SetMulticastConfiguration(*newConfig)
				ackTimer.Reset(mcConfig.multicastInterval)
			}
		
		case <-ackTimer.C:
			log.Println("AckTimer expired")
			if !p.acknowledgmentsArrivedInTime() {
				newConfig := p.increaseMulticastParameters()
				log.Println("Increased config:", newConfig)
				p.SetMulticastConfiguration(*newConfig)
				consecutiveSuccesses = 0
			} else {
				consecutiveSuccesses++
			}
			ackTimer.Reset(mcConfig.multicastInterval)
		
		case <-p.stopAck:
			log.Println("Stopping prober...")
			return nil
		}

		if consecutiveSuccesses > 3 {
			// log config
			log.Println("At the end of probing session: config ->", p.MulticastConfiguration())
			return nil
		}

		order++
	}

	log.Println("Probing didn't finish successfully")
	return nil
}

// Sets the probing config 
func (p *ProbeManager) SetProbeConfiguration(config ProbeConfig) {
	p.probeConfigLock.Lock()
	defer p.probeConfigLock.Unlock()
	p.ProbeConfig = config
}

func (p *ProbeManager) ProbeConfiguration() ProbeConfig {
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
	config := p.ProbeConfiguration()

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
	for _, mem := range config.Recipients {
		member := mem
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.ifritClient.SendTo(member, data)
			log.Println("Sending probe to", member, "using order", round.order)
		}()
	}

	wg.Wait()
	return nil
}

// Continuously listens for acknowledgments coming from the network
func (p *ProbeManager) probeAcknowledger() {
	for {
		select {
		case <-p.stopAck:
			log.Println("Stopping prober...")
			return
		case msg := <-p.ProbeChan:
			log.Println("MSG:", msg)
			order := p.getCurrentRound().order
			config := p.ProbeConfiguration()

			if !p.isValidAcknowledgment(msg.GetProbe(), &config, order) {
				log.Println("Required order", order, "got order", msg.GetProbe().GetOrder())
				return
			}

			// Insert into map
			currentRound := p.getCurrentRound()
			currentRound.table[msg.GetSender().GetName()] = msg.GetProbe()

			// Check if all acks have arrived. Signal if all have arrived
			n := config.NumMembers
			a := config.AcceptanceLevel
			if len(currentRound.table) >= nAcceptanceNodes(n, a) {
				p.allAcksChan <-true
			}
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

func (p *ProbeManager) getCurrentRound() ackRound {
	p.currentRoundLock.RLock()
	defer p.currentRoundLock.RUnlock()
	return *p.currentRound
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
func (p *ProbeManager) decreaseMulticastParameters() *MulticastConfig {
	config := p.MulticastConfiguration()
	ms := float32(config.multicastInterval.Milliseconds()) * 0.75
	newDuration := time.Duration(time.Duration(ms) * time.Millisecond)

	return &MulticastConfig{
		MulticastDirectRecipients: config.MulticastDirectRecipients,
		multicastInterval: newDuration,
	}
}

func (p *ProbeManager) acknowledgmentsArrivedInTime() bool {
	config := p.ProbeConfiguration()
	n := config.NumMembers
	a := config.AcceptanceLevel

	table := p.getCurrentRound().table
	
	if len(table) >= nAcceptanceNodes(n, a) {
		return true	
	}
	return false
}

// Increases the multicast parameters to process more load
func (p *ProbeManager) increaseMulticastParameters() *MulticastConfig {
	probeConfig := p.ProbeConfiguration()
	multicastConfig := p.MulticastConfiguration()

	// Phi is the acceptance level. Always increase sigma until
	// it reaches its upper bound of ceil((phi * n ) / 2)
	accLevel := probeConfig.AcceptanceLevel
	n := probeConfig.NumMembers
	directRecipients := multicastConfig.MulticastDirectRecipients
	tau := multicastConfig.multicastInterval

	// If sigma already is at its upper bound, double tau.
	// Else, increase sigma as much as we can
	if directRecipients >= int(math.Ceil((accLevel * float64(n)) / 2)) {
		tau *= 2 // TODO: set upper bound on tau
		if tau >= time.Duration(1 * time.Hour) {
			tau = time.Duration(1 * time.Hour)
		}
	} else {
		directRecipients *= 2
		if directRecipients > int(math.Ceil((accLevel * float64(n)) / 2)) {
			directRecipients = int(math.Ceil((accLevel * float64(n)) / 2))
		}
	}

	return &MulticastConfig{
		MulticastDirectRecipients: directRecipients,
		multicastInterval: tau,
	}
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