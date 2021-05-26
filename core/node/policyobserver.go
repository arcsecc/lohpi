package node

import (
	"fmt"
	"errors"
	"os"
	"github.com/tomcat-bit/fifoqueue"
	//log "github.com/sirupsen/logrus"
	"encoding/json"
	pb "github.com/arcsecc/lohpi/protobuf"
	"time"
)

var (
	errNoConfig = errors.New("Configuration is nil")
)

type policyObserver struct {
	observedGossipsList *fifoqueue.Queue
	config *policyObserverConfig
	currentPolicyLogFile *policyLogFile
	beginLogging int
	endLogging int
	cursor int
}

type policyLogFile struct {
	file *os.File
	filename string
	capacity int
}

type policyObserverConfig struct {
	outputDirectory string
	logfilePrefix string 
	capacity int
}

func newPolicyObserver(config *policyObserverConfig) (*policyObserver, error) {
	if config == nil {
		return nil, errNoConfig
	}

	fq, err := fifoqueue.NewFIFOQueue(config.capacity)
	if err != nil {
		return nil, err
	}

	po := &policyObserver{
		observedGossipsList: fq,
		config: config,
		beginLogging: 0,
		cursor: 0,
		endLogging: config.capacity,
	}

	// Create directory
	if _, err := os.Stat(config.outputDirectory); os.IsNotExist(err) {
		if err := os.MkdirAll(config.outputDirectory, 0755); err != nil {
			return nil, err
		}
	}

	return po, nil
}

// Returns true if the gossip message has already been observed, returns false otherwise
func (p *policyObserver) gossipIsObserved(msg *pb.GossipMessage) bool {
	return p.observedGossipsList.Exists(msg)
}

func (p *policyObserver) logObservedGossipBatch(entry []interface{}) error {
	if entry == nil {
		return fmt.Errorf("Entries array is nil")
	}

	filename :=	fmt.Sprintf("%s/%s_%s.json", p.config.outputDirectory, p.config.logfilePrefix, time.Now().String())
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
func (p *policyObserver) addObservedGossip(msg *pb.GossipMessage) error {
	if msg == nil {
		return fmt.Errorf("Gossip message is nil")
	}

	pgo := &policyGossipObservation{
		ArrivedAt: time.Now(),
		MessageID: msg.GetGossipMessageID(),
	}

	p.observedGossipsList.Insert(pgo)

	p.cursor += 1

	if p.cursor == p.config.capacity {
		elements := p.observedGossipsList.Elements()
		if err := p.logObservedGossipBatch(elements); err != nil {
			return err
		}
		p.cursor = 0
	}

	return nil
}

