package multicast

import (
	"bytes"
	"errors"
	"math"
	"math/rand"
	"sync"
	"time"
	"log"
	
	"github.com/joonnna/ifrit"
	"github.com/arcsecc/lohpi/core/message"
	"github.com/golang/protobuf/proto"
	pb "github.com/arcsecc/lohpi/protobuf"
)

var (
	errNoMode 					= errors.New("No membership mode given")
	errInvalidMessageMode		= errors.New("Invalid message mode given")
)

// Configuration used to probe the network. Should be set explicitly from outside. 
type probeSessionConfig struct {
	// Percentage of network nodes needed to achieve consistency
	// Phi. Static
	acceptanceLevel float64

	// Unique session ID
	sessionID []byte

	// N members for this round
	numMembers int
}

type probeManager struct {
	//Upon receiving an acknowledgment, insert it into the channel to register it
	probeChan chan pb.Message
	
	// Stop signal
	stopAck chan bool

	// Sends probe messages to the network
	ifritClient *ifrit.Client

	// Probing acknowledgment-related stuff
	rounds []*ackRound
	roundsLock sync.RWMutex

	currentRound *ackRound
	currentRoundLock sync.RWMutex

	// Signaled when phi*n acks have arrived
	allAcksChan chan bool

	// Configuration used when probing network
	prbConfig probeSessionConfig
	probeConfigLock sync.RWMutex

	// Multicast configuration
	multicastConfig multicastConfig
	mcConfigLock sync.RWMutex

	// Percentage of nodes we require to receive the messages
	acceptanceLevel float64

	isProbing bool
	isProbingMutex	sync.RWMutex

	// Membership manager 
	memManager *membershipManager
}

// Creates a new probeManager given the Ifrit client.
func newProbeManager(ifritClient *ifrit.Client) *probeManager {
	return &probeManager{
		probeChan:			make(chan pb.Message, 1000),
		allAcksChan:		make(chan bool, 1),
		stopAck: 			make(chan bool, 1),
		ifritClient:		ifritClient,
		rounds:				make([]*ackRound, 0),
		currentRoundLock:	sync.RWMutex{},
		probeConfigLock:	sync.RWMutex{},
		mcConfigLock:		sync.RWMutex{},
		isProbingMutex:		sync.RWMutex{},
		memManager: 		newMembershipManager(ifritClient),
	}
}

func (p *probeManager) start() {
	go p.probeAcknowledger()
}

func (p *probeManager) IsProbing() bool {
	p.isProbingMutex.RLock()
	defer p.isProbingMutex.RUnlock()
	return p.isProbing
}

// Returns a probe session using the given multicast configuration
func (p* probeManager) initializeProbingSession(mcConfig multicastConfig) {
	sessionID := make([]byte, 4)
	rand.Read(sessionID)

	config := &probeSessionConfig{
		acceptanceLevel: 			mcConfig.acceptanceLevel,
		sessionID:					sessionID,
		numMembers:					len(p.ifritClient.Members()) - 1,		// Subtract mux from the set
	}

	p.setProbeSessionConfiguration(config)
}

// Probes the network by multicasting a probing messages to the given members. 
// It uses the current configuration assoicated with the probing manager.
// If probing is aborted prematurely, multicastConfig is nil.
func (p *probeManager) probeNetwork(mcConfig *multicastConfig, mode MessageMode) (*multicastConfig, error) {
	p.isProbingMutex.Lock()
	p.isProbing = true
	p.isProbingMutex.Unlock()

	defer func() {
		p.isProbingMutex.Lock()
		p.isProbing = false
		p.isProbingMutex.Unlock()
	}()

	p.initializeProbingSession(*mcConfig)
	
	// Timer used to wait for acks. Using initial value from multicastConfig
	ackTimer := time.NewTimer(mcConfig.multicastInterval)	
	order := 1
	consecutiveSuccesses := 0

	sigma := mcConfig.multicastDirectRecipients
	tau := mcConfig.multicastInterval
	phi := mcConfig.acceptanceLevel

	// Main probing loop
	for {//i := 0; i < 20; i++ {
		// Start the first round. Parameter changes are done in other methods
		recipients, err := p.probeRoundRecipients(mode, sigma)
		if err != nil {
			return nil, err
		}

		log.Println("recipients", recipients)

		r := newProbeRound(&tau, &sigma, mode, &order, &phi, recipients)
		p.appendRound(r)
		p.setCurrentRound(r)
	
		lowerLimit := time.Now().Add(r.multicastInterval / 2)
		upperLimit := time.Now().Add(r.multicastInterval)	
		_ = lowerLimit
		_ = upperLimit
		
		if err := p.multicast(r); err != nil {
			return nil, err
		}

		// Timer set to the multicast interval
		ackTimer.Reset(tau)	
		
		select {
		case <-p.stopAck:
			log.Println("Stopping prober...")
			return nil, nil
		
		case <-ackTimer.C:
			log.Println("AckTimer expired")
			if p.acknowledgmentsArrivedInTime() {
				log.Println("Seems OK")
				consecutiveSuccesses++
			} else {
				log.Println("Before increasing config:", p.getCurrentRound())
				p.increaseProbeParameters(&tau, &sigma)
				log.Println("After increasing config:", tau, sigma)
				consecutiveSuccesses = 0
			}
			ackTimer.Reset(tau)
		
		// Signaled when all valid acks have arrived for this round.
		// It will always trigger when all acks for this rounds have arrived.
		/*case <-p.allAcksChan:
			log.Println("Channel event: all acks have arrived")
			if nowIsBeforeLimit(lowerLimit) {
				log.Printf("Before decreasing config: %v+\n", p.probeConfiguration())
				p.decreaseProbeSessionConfiguration(prbSessionConfig)
				p.setProbeSessionConfiguration(*prbSessionConfig)
				log.Printf("After decreasing config: %v+\n", p.probeConfiguration())
			} else if nowIsBeforeLimit(upperLimit) {
				consecutiveSuccesses++
			}
			ackTimer.Reset(prbSessionConfig.multicastInterval)
		*/
		}

		if consecutiveSuccesses > 2 {
			// log config
			//log.Printf("At the end of probing session: %v+\n", p.probeConfiguration())
			break
		}

		order++
	}

	r := p.getCurrentRound()
	
	cc := &multicastConfig{
		multicastDirectRecipients: 	r.multicastDirectRecipients,
		multicastInterval:			r.multicastInterval,
	}

	return cc, nil
}

func (p *probeManager) Stop() {
	p.stopAck <- true
}

// Sets the probing config 
func (p *probeManager) setProbeSessionConfiguration(config *probeSessionConfig) {
	p.probeConfigLock.Lock()
	defer p.probeConfigLock.Unlock()
	p.prbConfig = *config
}

func (p *probeManager) probeConfiguration() probeSessionConfig {
	p.probeConfigLock.RLock()
	defer p.probeConfigLock.RUnlock()
	return p.prbConfig
}

// Sets the multicast configuration
func (p *probeManager) SetMulticastConfiguration(config multicastConfig) {
	p.mcConfigLock.Lock()
	defer p.mcConfigLock.Unlock()
	p.multicastConfig = config
}

func (p *probeManager) MulticastConfiguration() multicastConfig {
	p.mcConfigLock.RLock()
	defer p.mcConfigLock.RUnlock()
	return p.multicastConfig
}

// Multicasts a set of probe messages given the order number of the configuration
func (p *probeManager) multicast(round *ackRound) error {
	// No recipients in network. Return
	if p.probeConfiguration().numMembers == 0 {
		return errors.New("No probe recipients in network")
	}

	config := p.probeConfiguration()

	// Prepare message
	msg := &pb.Message{
		Type: message.MSG_TYPE_PROBE,
		Probe: &pb.Probe{
			Order: round.order,
			SessionId: config.sessionID,
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
		Signature: &pb.MsgSignature{
			R: r,
			S: s,
		},
		Probe: &pb.Probe{
			Order: round.order,
			SessionId: config.sessionID,
		},
	}

	// Marshal before multicast probe messages
	binaryData, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	// Multicast messages in parrallel
	wg := sync.WaitGroup{}
	for _, mem := range round.recipients {
		member := mem
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.ifritClient.SendTo(member, binaryData)
		}()
	}

	wg.Wait()
	return nil
}

// Continuously listens for acknowledgments coming from the network
func (p *probeManager) probeAcknowledger() {
	for {
		select {
			case msg := <-p.probeChan:
				// Ignore acks if we don't probe at all
				if !p.IsProbing() {
					continue
				}
				
				currentRound := p.getCurrentRound()
				order := currentRound.order
				config := p.probeConfiguration()
				
				if !p.isValidAcknowledgment(msg.GetProbe(), &config, order) {
					log.Println("Required order", order, "got order", msg.GetProbe().GetOrder())
					continue
				}

				// Insert into map
				currentRound.acktableLock.Lock()
				//log.Printf("currentRound.table: %p\n", &currentRound.table)
				currentRound.acktable[msg.GetSender().GetName()] = msg.GetProbe()
				currentRound.acktableLock.Unlock()

				// Check if all acks have arrived. Signal if all have arrived
				/*n := config.numMembers
				a := config.acceptanceLevel*/

				/*if len(currentRound.table) >= nAcceptanceNodes(n, a) {
					p.allAcksChan <-true
				}*/
			default:
				continue
			/*case <-p.stopAck:
				log.Println("Stopping prober...")
				return*/
			}
	}
}

func (p *probeManager) getAckRounds() []*ackRound {
	p.roundsLock.RLock()
	defer p.roundsLock.RUnlock()
	return p.rounds
}

func (p *probeManager) setCurrentRound(r *ackRound) {
	p.currentRoundLock.Lock()
	defer p.currentRoundLock.Unlock()
	p.currentRound = r
}

func (p *probeManager) getCurrentRound() *ackRound {
	p.currentRoundLock.RLock()
	defer p.currentRoundLock.RUnlock()
	return p.currentRound
}

func (p *probeManager) appendRound(r *ackRound) {
	p.roundsLock.Lock()
	defer p.roundsLock.Unlock()
	p.rounds = append(p.rounds, r)
}

func (p *probeManager) isValidAcknowledgment(probe *pb.Probe, config *probeSessionConfig, order uint32) bool {
	return probe.GetOrder() == order && bytes.Compare(probe.GetSessionId(), config.sessionID) == 0 
	//return probe.GetOrder() == order
}

// Set tau to 0.75 of its original value
func (p *probeManager) decreaseProbeSessionConfiguration() {
	// Fetch newest 
	log.Println("Should decrease parameters")
	/*r := p.getCurrentRound()
	ms := float32(r.multicastInterval.Milliseconds()) * 0.75
	newDuration := time.Duration(time.Duration(ms) * time.Millisecond)
	r.multicastInterval = newDuration*/
}

func (p *probeManager) probeRoundRecipients(mode MessageMode, directRecipients int) ([]string, error) {
	switch mode {
	case LruMembers:
		return p.memManager.lruMembers(directRecipients)
//	case RandomMembers:
//		return p.memManager.RandomMembers()
	default:
		return nil, errInvalidMessageMode
	}

	return nil, nil
}

func (p *probeManager) acknowledgmentsArrivedInTime() bool {
	config := p.probeConfiguration()
	n := config.numMembers
	a := config.acceptanceLevel

	round := p.getCurrentRound()
	round.acktableLock.RLock()
	table := p.getCurrentRound().acktable
	round.acktableLock.RUnlock()
	
	if len(table) >= nAcceptanceNodes(n, a) {
		return true	
	}
	return false
}

// Increases the multicast parameters to process more load
// TODO: sigma preceeds tau
func (p *probeManager) increaseProbeParameters(tau *time.Duration, sigma *int) {
	sessionConfig := p.probeConfiguration()

	// Phi is the acceptance level. Always increase sigma until
	// it reaches its upper bound of ceil((phi * n ) / 2)
	accLevel := sessionConfig.acceptanceLevel
	n := sessionConfig.numMembers
	
	if *sigma < int(math.Ceil((accLevel * float64(n)) / 2)) {
		*sigma *= 2

		if *sigma > int(math.Ceil((accLevel * float64(n)) / 2)) {
			*sigma = int(math.Ceil((accLevel * float64(n)) / 2))
		}
	} else if *sigma == int(math.Ceil((accLevel * float64(n)) / 2)) {
		//s := float64(tau.Milliseconds()) * 1.5 // TODO: set upper bound on tau. Multiply by 1.5
		// TODO: increase by 1.5
		*tau *= 2
		if *tau >= time.Duration(1 * time.Hour) {
			*tau = time.Duration(1 * time.Hour)
		}
	} else if n > 4 {
		*sigma = 1

		*tau *= 2
		if *tau >= time.Duration(1 * time.Hour) {
			*tau = time.Duration(1 * time.Hour)
		}
	}

	// Set the new round
//	p.setCurrentRound(&config)
	//log.Println("After increasing:")
	//log.Println("config.MulticastDirectRecipients:", config.MulticastDirectRecipients)
	//log.Println("config.MulticastInterval:", config.MulticastInterval)
}

func (p *probeManager) setIgnoredIfritNodes(ignoredIPs map[string]string) {
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
