package gossipmanager

import (
_	"fmt"
	"log"
	"time"
_	"container/list"
_	"crypto/sha256"
	"encoding/json"
	"sync"

	"github.com/tomcat-bit/lohpi/internal/core/message"
	"github.com/tomcat-bit/lohpi/internal/core/cache"

	"github.com/joonnna/ifrit"
)


// TODO: assure that the size does not exceed 4MB (refer to grpc library's constraints on message sizes)
// Gossiping works. However, we need gossip probing to work 
type GossipManager struct {
	// Underlying Ifrit client
	ifritClient *ifrit.Client

	// Self-adjusting interval between gossiping contents
	gossipInterval time.Duration

	// Interval between each probing session 
	probeInterval time.Duration

	// Messages submitted from the client. Read by the batcher
	messageQueue chan []byte

	// Current batch in flight
	gossipMsg *message.NodeMessage
	gossipLock sync.RWMutex

	// Cache from policy store
	cache *cache.Cache

	// Recipients required to accept the current gossip
	remainingRecipients map[string]string
	numRequiredRecipients int
	// lock it too 
}

func NewGossipManager(ifritClient *ifrit.Client, batchSize int, probeInterval time.Duration, cache *cache.Cache) (*GossipManager, error) {
	return &GossipManager{
		ifritClient: 			ifritClient,
		gossipInterval:			10 * time.Second,		// Find a reasonable value 
		probeInterval:			probeInterval,
		messageQueue: 			make(chan []byte, batchSize),
		cache:					cache,
		remainingRecipients:	make(map[string]string),
		gossipMsg:  			&message.NodeMessage{
			MessageType:			message.MSG_TYPE_GOSSIP_MESSAGE,
			Content: 				make([][]byte, 0),
		},
		gossipLock: sync.RWMutex{},
	}, nil
}

// Submits a message to the network. The message will be batched together with other messages and 
// sent to the network.
func (gm *GossipManager) Submit(data []byte) {
	log.Println("Submitting policy to batch")

	// Producer
	//lock
	gm.messageQueue <- data
	//unlock
}

// Main loop
func (gm *GossipManager) Start() {
	probeTimer := time.Tick(gm.probeInterval)
	gossipTimer := time.Tick(gm.gossipInterval)
	
	for {
		select {
		case <-gossipTimer:
			gm.gossipBatch()
		case <-probeTimer:
			gm.probeNetwork()
		case msg := <-gm.messageQueue:
			gm.insertMessage(msg)
		//case <-gm.exitChan: 
		//	return
		}
	}
}

func (gm *GossipManager) AcknowledgeMessage(msg message.NodeMessage) {
	log.Printf("GossipManager acking message with hash %s from %s\n", string(msg.Hash), msg.Node)

	if msg.Hash != gm.gossipMsg.Hash {
		log.Println("Got an unknown hash")
		return
	}

	//lock
	delete(gm.remainingRecipients, msg.Node)
	//unlock

	if len(gm.remainingRecipients) < 1 {
		log.Println("Got acks from all nodes")
	} else {
		log.Println("Missing acks from ", gm.remainingRecipients)
	}
}

// Probes the network to find the appropriate gossip interval. 
// Will always block application-level gossips until all gossips have completed
func (gm *GossipManager) probeNetwork() {
	log.Println("Probing network for diagnostics")

	// Prepare probing message. Wait 'gm.gossipInterval' seconds before
}

func (gm *GossipManager) insertMessage(msg []byte) {
	gm.gossipLock.Lock()
	defer gm.gossipLock.Unlock()
	gm.gossipMsg.Content = append(gm.gossipMsg.Content, msg)
}

func (gm *GossipManager) gossipBatch() {
	gm.gossipLock.Lock()
	defer gm.gossipLock.Unlock()
	
	// Copy the marshalled object 
	b, _ := json.Marshal(gm.gossipMsg)
	c := make([]byte, len(b))
	copy(c, b)

	log.Println("Gossiping to network")
	gm.gossipMessage(c)

	gm.gossipMsg = &message.NodeMessage{
		Content: 	make([][]byte, 0),
	}
}

// Gossips content to the Ifrit network
func (gm *GossipManager) gossipMessage(data []byte) {
	gm.ifritClient.SetGossipContent(data)
}

func (gm *GossipManager) setRecipients() {
	//lock
	gm.remainingRecipients = gm.cache.Nodes()
	gm.numRequiredRecipients = len(gm.cache.Nodes())
	//unlock
}

// Check if the in-fligh batch completed as it should. Adjusts the sleep intercal 
func (gm *GossipManager) verifyAcknowledgements() {
	// lock 
	/*numMissing := len(gm.remainingRecipients)
	if numMissing < 1 {
		pb := &pastBatch{
			recipients: gm.remainingRecipients,
			ackPercentage: (float32(numMissing) * float32(gm.numRequiredRecipients)) * 100.0,
		}

		elem := &list.Element{
			Value: pb,
		}

//		gm.pastBatchesList.PushBack(elem)
		gm.gossipInterval += 5 * time.Second
		log.Printf("Missed %f elements in this gossip round", pb.ackPercentage)
	}*/
	// unlock
}