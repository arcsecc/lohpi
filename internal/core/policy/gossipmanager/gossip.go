package gossipmanager

import (
_	"fmt"
	"log"
	"time"
	"container/list"
	"crypto/sha256"
	"encoding/json"

	"github.com/tomcat-bit/lohpi/internal/core/message"
	"github.com/tomcat-bit/lohpi/internal/core/cache"

	"github.com/joonnna/ifrit"
)


// TODO: assure that the size does not exceed 4MB (refer to grpc library's constraints on message sizes)

type GossipManager struct {
	// Underlying Ifrit client
	ifritClient *ifrit.Client

	// List of past batches to be gossiped to the network
	pastBatchesList *list.List

	// Self-adjusting interval between gossiping contents
	gossipInterval time.Duration

	// Interval between each probing session 
	probeInterval time.Duration

	// Messages submitted from the client. Read by the batcher
	messageQueue chan []byte

	// Current batch in flight
	gossipMsg *message.NodeMessage

	// Cache from policy store
	cache *cache.Cache

	// Recipients required to accept the current gossip
	remainingRecipients map[string]string
	numRequiredRecipients int
	// lock it too 
}

type pastBatch struct {
	recipients map[string]string
	ackPercentage float32
	//Contents as well?
}

func NewGossipManager(ifritClient *ifrit.Client, batchSize int, probeInterval time.Duration, cache *cache.Cache) (*GossipManager, error) {
	return &GossipManager{
		ifritClient: 			ifritClient,
		pastBatchesList: 		list.New(),
		gossipInterval:			10 * time.Second,		// Find a reasonable value 
		probeInterval:			probeInterval,
		messageQueue: 			make(chan []byte, batchSize),
		cache:					cache,
		remainingRecipients:	make(map[string]string),
		gossipMsg:  			&message.NodeMessage{
			MessageType:			message.MSG_TYPE_GOSSIP_MESSAGE,
			Content: 				make([][]byte, 0),
		},
	}, nil
}

// Submits a message to the network. The message will be batched together with other messages and 
// sent to the network.
func (gm *GossipManager) Submit(data []byte) {
	log.Println("Submitting policy to batch")

	// Producer
	gm.messageQueue <- data
}

// Main loop
func (gm *GossipManager) Start() {
	for {
		select {
		case <-time.After(gm.probeInterval):
			gm.probeNetwork()
		case <-time.After(gm.gossipInterval):
			gm.gossipContent()
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

func (gm *GossipManager) probeNetwork() {
	log.Println("Probing network. Not implemented yet!")
}

func (gm *GossipManager) insertMessage(msg []byte) {
	// Write lock
	gm.gossipMsg.Content = append(gm.gossipMsg.Content, msg)
	// Write unlock
}

// TODO: make sure hash collision between equal objects cannot occur. Seed with time?
func (gm *GossipManager) gossipContent() {
	log.Println("GOSSIP TIME!!!!")
	// Lock before sending it. 

	gm.verifyAcknowledgements()

	// Compute the hash
	h := sha256.New()
	//h.Write([]byte(fmt.Sprintf("%v", gm.gossipMsg)))
	t, _ := time.Now().MarshalJSON()
	h.Write(t)
	gm.gossipMsg.Hash = string(h.Sum(nil))

	// Copy the marshalled object 
	b, _ := json.Marshal(gm.gossipMsg)
	c := make([]byte, len(b))
	copy(c, b)

	gm.setRecipients()

	log.Printf("Gossip hash: %s\n", string(gm.gossipMsg.Hash))
	for _, b := range gm.gossipMsg.Content {
		log.Printf("Gossip content: %s\n", string(b))
	}

	log.Println("Want acks from ", gm.remainingRecipients)

	// Gossip it
	gm.ifritClient.SetGossipContent(c)

	// Nil everything 
	gm.gossipMsg = &message.NodeMessage{
		Content: 	make([][]byte, 0),
		Hash: 		"",
	}
	// Unlock
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
	numMissing := len(gm.remainingRecipients)
	if numMissing < 1 {
		pb := &pastBatch{
			recipients: gm.remainingRecipients,
			ackPercentage: (float32(numMissing) * float32(gm.numRequiredRecipients)) * 100.0,
		}

		elem := &list.Element{
			Value: pb,
		}

		gm.pastBatchesList.PushBack(elem)
		gm.gossipInterval += 5 * time.Second
		log.Printf("Missed %f elements in this gossip round", pb.ackPercentage)
	}
	// unlock
}