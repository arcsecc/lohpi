package main

import (
	"fmt"
	"os"
	"ifrit"
	"errors"
//	"encoding/gob"
//	"bytes"
//	"time"
)

var (
	errCreateRandomClientData 	= errors.New("Could not set client data.")
)

// Firestore node
type Node struct {
	Dataset map[string]*os.File		// string -> custom file in this package
	IfritClient *ifrit.Client
	NodeID string
}

// Might need to change the value's type
type Masternode struct {
	Filemap map[string]string
	IfritClient *ifrit.Client
	NodeID string
}

/** Node interface */
func NewNode(ID string) (*Node, error) {
	ifritClient, err := ifrit.NewClient()
	if err != nil {
		return nil, err
	}

	node := &Node {}
	node.IfritClient = ifritClient
	node.NodeID = ID
	go ifritClient.Start()
	return node, nil
}

func (n *Node) ID() string {
	return n.NodeID
}

func (n *Node) FireflyClient() *ifrit.Client {
	if n.IfritClient == nil {
		fmt.Printf("OIAJSDOISOSIJDSO\n");
	}
	return n.IfritClient
}

// Invoked when this client receives a message
func (n *Node) StorageNodeMessageHandler(data []byte) ([]byte, error) {
	fmt.Println(data)
/*	var decodedPayload Clientdata
	b := bytes.NewBuffer(data)
	d := gob.NewDecoder(b)
	if err := d.Decode(&decodedPayload); err != nil {
	   panic(err)
	}
  
	startTime := decodedPayload.GetEpochTimestamp()
	nSecInTransit := time.Now().UnixNano() - startTime
	fmt.Printf("Message spent %d ns in transit\n", nSecInTransit)
	*/
	return nil, nil
}

/*func (c *Client) Start() {
	go c.IfritClient.Start()
	//fmt.Printf("addr = %s\n", c.IfritClient.Addr())
}

func (c *Client) ID() string {
	return c.ClientID
}

// Use this to access the underlying Ifrit client. Should find a more sane way to 
// separate an ifrit client and an ordinary client...
func (c *Client) FireflyClient() *ifrit.Client {
	return c.IfritClient
}*/

/** Masternode interface */
func NewMasterNode(nodeID string) (*Masternode, error) {
	ifritClient, err := ifrit.NewClient()
	if err != nil {
		return nil, err
	}

	masterNode := &Masternode {
		IfritClient: ifritClient,
		NodeID: nodeID,
	}
	go ifritClient.Start()
	return masterNode, nil
}

func (n *Masternode) FileNameTable() map[string]string {
	return n.Filemap
}

func (n *Masternode) MasterNodeMessageHandler(data []byte) ([]byte, error) {
	fmt.Println(data)
	return nil, nil
}

func (n *Masternode) FireflyClient() *ifrit.Client {
	return n.IfritClient
}