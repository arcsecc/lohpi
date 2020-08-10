package multicast
/*
import (
	"log"
	"time"
	"sync"
	"github.com/joonnna/ifrit"
	"github.com/tomcat-bit/lohpi/internal/core/message"
	"github.com/golang/protobuf/proto"

	pb "github.com/tomcat-bit/lohpi/protobuf"
)

// Used to indicate an increase or decrease in gossip parameter
const INCREASE = 1
const DECREASE = -1

// Used to indicate half time or upper bound on ack time interval 
const HALFTIME = 1
const FULLTIME = 2

// One session of gossips
type probeSession struct {
	// Used to send probe messages to network
	ifritClient *ifrit.Client

	// System parameters used for each session.
	// We set these variables at the beginning of each session to avoid
	// spending too much time in adjusting the parameters in case of a lot of fluctuations.
	nMembers int
	numExpectedAcknowledgments uint32
	lruMembers []string
	mcConfig MulticastConfig

	// Map of acknowledgments received
	acks map[string]*pb.Probe
	mapLock	sync.RWMutex

	// Signaled when done
	done chan bool

	// Pointer to a channel into which probe messages are sent
	Queue *chan pb.Message
}

// Initializes a probing session with the given system parameters and list of members
// that are to receive probing messages.
func newProbeSession(ifritClient *ifrit.Client, mcConfig MulticastConfig, n int, lruMembers []string, queue *chan pb.Message) *probeSession {
	if lruMembers == nil {
		log.Fatalf("TODO: implement random member selection. Exiting")
	}
	return &probeSession{
		ifritClient: 	ifritClient,
		nMembers: 		n,
		lruMembers: 	lruMembers,
		mcConfig: 		mcConfig,
		acks:			make(map[string]*pb.Probe),
		mapLock:		sync.RWMutex{},
		done:			make(chan bool, 1),
		Queue:			queue,
	}
}

// Run a new probe session. Returns nil if it exits succsessfully
func (ps *probeSession) run() error {
	// Monotonically increasing ordering number. We do not allow acks from a previous window
	// be accepted as a valid probe ack
	var roundId uint32 = 1

	// Set the timer that defines the window in which acks must arrive
	ackTimer := time.NewTimer(ps.mcConfig.MulticastInterval)	
	halfTimer := time.NewTimer(ps.mcConfig.MulticastInterval / 2)	

	// Main probing loop
	for {
		// Multicast probing messages to network 
		if err := ps.multicastProbeMessages(roundId); err != nil {
			log.Println(err.Error())
			return err
		}

		// When the different timers fire, check acks.
		select {
		case ack := <-*ps.Queue:
			ps.mapLock.Lock()
			ps.acks[ack.GetSender().GetName()] = ack.GetProbe()
			ps.mapLock.Unlock()

		//At half-time, we might need to set new parameters.
		// If that happens, the latter timer doesn't fire and a new iteration begins
		case <-halfTimer.C:
			log.Println("Half time inspection")
			if !round.inspectAcknowledgements(HALFTIME) {
				ps.setNewGossipConfiguration(DECREASE)
				ackTimer.Reset(ps.mcConfig.multicastInterval)
				halfTimer.Reset(ps.mcConfig.multicastInterval / 2)
			}

		// Inspect all acks. If phi acks have arrived, do nothing. If not, set new parameters and
		// repeat the process
		case <-ackTimer.C:
			log.Println("Full time inspection")
			if !round.inspectAcknowledgements(FULLTIME) {
				ps.setNewGossipConfiguration(INCREASE)
				ackTimer.Reset(ps.mcConfig.multicastInterval)
				halfTimer.Reset(ps.mcConfig.multicastInterval / 2)
			} else {
				log.Println("New parameters set")
				break
			}
		}
	}
}

// Returns true if 
func (ps *probeSession) inspectAcknowledgements() bool {

}

func (ps *probeSession) setNewGossipConfiguration(flag int) {
	switch flag {
	case INCREASE:
	}
}

func (ps *probeSession) multicastProbeMessages(order uint32) error {
	log.Println("Sending probe msg to", ps.lruMembers)

	// Prepare message
	msg := &pb.Message{
		Type: message.MSG_TYPE_PROBE,
		Probe: &pb.Probe{
			Order: order,
		},
	}

	// Encode before signing message
	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	// Sign it
	r, s, err := ps.ifritClient.Sign(data)
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
			Order: order,
		},
	}

	// Marshal before sening
	data, err = proto.Marshal(msg)
	if err != nil {
		return err
	}

	// Send to all LRU members, using the same configurations as multicasting policies
	wg := sync.WaitGroup{} 
	for _, m := range ps.lruMembers {
		member := m
		wg.Add(1)
		go func() {
			ps.ifritClient.SendTo(member, data)
			wg.Done()
		}()
	}
	
	wg.Wait()
	
	return nil
}
*/