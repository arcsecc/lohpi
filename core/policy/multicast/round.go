package multicast

import ( 
	"sync"
	"time"
	pb "github.com/tomcat-bit/lohpi/protobuf"
)

// Describes the configuration for one round
type ackRound struct {
	acktable map[string]*pb.Probe
	acktableLock sync.RWMutex

	// Time between probing rounds. Fetched from this package
	multicastInterval time.Duration		// tau. Dynamic

	// The number of recipients of direct messages
	multicastDirectRecipients int 		// sigma. Dynamic
	
	// Messaging mode
	mode MessageMode

	// Incresing ordering number
	order uint32
	orderLock sync.RWMutex

	recipients []string

	// Never changed for each session
	//prbSession probeSessionConfig
}

// use now too...
// TODO refine this 
func newProbeRound(multicastInterval *time.Duration, multicastDirectRecipients *int, mode MessageMode, order *int, acceptanceLevel *float64, recipients []string) *ackRound {
	return &ackRound{
		multicastInterval: 			*multicastInterval,
		multicastDirectRecipients: 	*multicastDirectRecipients,
		mode:						mode,
		order:						uint32(*order),
		orderLock:  				sync.RWMutex{},
		recipients: 				recipients,
		acktable:					make(map[string]*pb.Probe),
		acktableLock:				sync.RWMutex{},
	}
}

func (r *ackRound) ackTable() map[string]*pb.Probe {
	r.acktableLock.RLock()
	defer r.acktableLock.RUnlock()
	return r.acktable
}

func (r *ackRound) orderNumber() uint32 {
	r.orderLock.RLock()
	defer r.orderLock.RUnlock()
	return r.order
}
