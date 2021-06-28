package gossipobserver

import (
	"fmt"
	"errors"
	"os"
	"github.com/tomcat-bit/fifoqueue"
	log "github.com/sirupsen/logrus"
	"encoding/json"
	pb "github.com/arcsecc/lohpi/protobuf"
	"time"
)

var (
	errNoConfig = errors.New("Configuration is nil")
)

// TODO: add logrus logging with nice fields

type GossipObserverUnit struct {
	observedGossipsList *fifoqueue.Queue
	config *PolicyObserverConfig
	cursor int
}

type PolicyObserverConfig struct {
	// Output directory of the logfiles
	OutputDirectory string

	// The prefix used on the logfile names
	LogfilePrefix string 

	// The capacity of the in-memory queue. When the capacity is reached, the
	// least recently element is evicted from the queue. 
	Capacity int
}

type observedGossip struct {
	ArrivedAt 	time.Time
	MessageID	*pb.GossipMessageID
}

func NewGossipObserver(config *PolicyObserverConfig) (*GossipObserverUnit, error) {
	if config == nil {
		return nil, errNoConfig
	}

	fq, err := fifoqueue.NewFIFOQueue(config.Capacity)
	if err != nil {
		return nil, err
	}

	po := &GossipObserverUnit{
		observedGossipsList: fq,
		config: config,
		cursor: 0,
	}

	// Create output directory 
	if _, err := os.Stat(config.OutputDirectory); os.IsNotExist(err) {
		if err := os.MkdirAll(config.OutputDirectory, 0755); err != nil {
			return nil, err
		}
	}

	return po, nil
}

// Returns true if the gossip message has already been observed, returns false otherwise
func (p *GossipObserverUnit) GossipIsObserved(msg *pb.GossipMessage) bool {
	return p.observedGossipsList.Exists(msg)
}

func (p *GossipObserverUnit) logObservedGossipBatch(entry []interface{}) error {
	if entry == nil {
		return fmt.Errorf("Entries to be logged is nil")
	}

	filename :=	fmt.Sprintf("%s/%s_%s.json", p.config.OutputDirectory, p.config.LogfilePrefix, time.Now().String())
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	
	data, err := json.MarshalIndent(entry, "", "	")
	if err != nil {
		return err
	}

	_, err = f.Write(data)
	if err != nil {
		return err
	}
	
	return f.Close()
}

// Inserts the gossip message into the log of observed gossips
func (p *GossipObserverUnit) AddGossip(msg *pb.GossipMessage) error {
	if msg == nil {
		return fmt.Errorf("Gossip message is nil")
	}

	elem := &observedGossip{
		ArrivedAt: time.Now(),
		MessageID: msg.GetGossipMessageID(),
	}

	p.observedGossipsList.Insert(elem)
	p.cursor += 1

	if p.cursor == p.config.Capacity {
		elements := p.observedGossipsList.Elements()
		if err := p.logObservedGossipBatch(elements); err != nil {
			return err
		}
		p.cursor = 0
	}

	return nil
}

// hacky af...
func (p *GossipObserverUnit) LatestGossip() *pb.GossipMessageID {
	if p.observedGossipsList.Length() == 0 {
		return nil
	}
	log.Println("KAKE:", p.observedGossipsList.Back())
	if p.observedGossipsList.Back() != nil {
		return p.observedGossipsList.Back().(*observedGossip).MessageID
	}
	return nil
}