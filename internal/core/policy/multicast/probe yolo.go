package multicast
/*
import (
	"log"
	"sync"
	"bytes"
_	"errors"
	
	"github.com/tomcat-bit/lohpi/internal/core/message"
	"github.com/golang/protobuf/proto"
	pb "github.com/tomcat-bit/lohpi/protobuf"
)

type probeConfig struct {
	recipients []string
	n int
	acceptanceLevel float64
	sessionID []byte
}

func (m *MulticastManager) probeAcknowledger() {
	// Main loop for continuously handling acknowledgments

	for {
		select {
		case <-m.stopAck:
			log.Println("Stopping prober...")
			return
		case msg := <-m.ProbeChan:
			log.Println("Got probe ack message:", msg)
			config := m.currProbeConfig()

			// Verify the incoming probe acknowledgment
			order := m.ackMap.order
			if !m.isValidAcknowledgment(msg.GetProbe(), &config, order) {
				log.Println("Required order", order, "got order", msg.GetProbe().GetOrder())
				return
			}

			// Insert into map
			m.ackMap.table[msg.GetSender().GetName()] = msg.GetProbe()

			// Check if all acks have arrived. Signal if all have arrived
			n := config.n
			a := config.acceptanceLevel
			if len(m.ackMap.table) >= nAcceptanceNodes(n, a) {
				m.allAcksChan <-true
			}
		}
	}
}

func (m *MulticastManager) acknowledgmentsArrivedInTime() bool {
	config := m.currProbeConfig()
	n := config.n
	a := config.acceptanceLevel
	
	if len(m.getAckMap().table) >= nAcceptanceNodes(n, a) {
		return true	
	}
	return false
}

func (m *MulticastManager) isValidAcknowledgment(p *pb.Probe, config *probeConfig, order uint32) bool {
	return p.GetOrder() == order && bytes.Compare(p.GetSessionId(), config.sessionID) == 0 
}
*/