package gossipmanager

import (
	"fmt"
	"log"
	"time"
	"container/list"
	"crypto/sha256"
	"encoding/json"

	"github.com/tomcat-bit/lohpi/internal/core/message"
	"github.com/joonnna/ifrit"
)

type GossipManager struct {
	// Underlying Ifrit client
	ifritClient *ifrit.Client

	// List of batches to be gossiped to the network
	list *list.List

	// Self-adjusting interval between gossiping contents
	gossipInterval time.Duration

	// Interval between each probing session 
	probeInterval time.Duration

	// Messages submitted from the client. Read by the batcher
	messageQueue chan []byte

	// Current batch in flight
	gossipMsg *message.NodeMessage

	// Recipients required to accept the current gossip
	recipients map[string]string
}

func NewGossipManager(ifritClient *ifrit.Client, batchSize int, probeInterval time.Duration) (*GossipManager, error) {
	return &GossipManager{
		ifritClient: 	ifritClient,
		list: 			list.New(),
		gossipInterval:	10 * time.Second,		// Find a reasonable value 
		probeInterval:	probeInterval,
		messageQueue: 	make(chan []byte, batchSize),
		recipients:		make(map[string]string),
		gossipMsg:  	&message.NodeMessage{
			MessageType:	message.MSG_TYPE_GOSSIP_MESSAGE,
			Content: 		make([][]byte, 0),
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
	log.Printf("GossipManager acking message with hash %s from %s\n", string(msg.Hash), msg.NodeAddr)
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

	// Compute the hash
	h := sha256.New()
    h.Write([]byte(fmt.Sprintf("%v", gm.gossipMsg)))
	gm.gossipMsg.Hash = string(h.Sum(nil))

	// Copy the marshalled object 
	b, _ := json.Marshal(gm.gossipMsg)
	c := make([]byte, len(b))
	copy(c, b)

	log.Printf("Gossip hash: %s\n", string(gm.gossipMsg.Hash))
	for _, b := range gm.gossipMsg.Content {
		fmt.Printf("Gossip content: %s\n", string(b))
	}

	gm.setRecipients()

	// Gossip it
	//gm.ifritClient.SetGossipContent(c)

	// Nil everything 
	gm.gossipMsg = &message.NodeMessage{
		Content: 	make([][]byte, 0),
		Hash: 		"",
	}
	// Unlock
}

func (gm *GossipManager) setRecipients() {
	//lock
	
	//unlock
}