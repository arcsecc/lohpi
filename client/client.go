package main

import (
	"fmt"
	"ifrit"
	"errors"
	"encoding/gob"
	"bytes"
	"time"
)

var (
	errCreateRandomClientData 	= errors.New("Could not set client data.")
)

type Client struct {
	Dataset map[string]*Clientdata		// string -> clientdata
	ClientID string
	IfritClient *ifrit.Client
}

func NewApplicationClient(clientID string) (*Client, error) {
	ifritClient, err := ifrit.NewClient()
	if err != nil {
		return nil, err
	}

	client := &Client {
		ClientID: clientID,
		IfritClient: ifritClient,
	}

	ifritClient.RegisterMsgHandler(client.ClientMessageHandler)
	return client, nil
}

// Invoked when this client receives a message
func (c *Client) ClientMessageHandler(data []byte) ([]byte, error) {
	var decodedPayload Clientdata
	b := bytes.NewBuffer(data)
	d := gob.NewDecoder(b)
	if err := d.Decode(&decodedPayload); err != nil {
	   panic(err)
	}
  
	startTime := decodedPayload.GetEpochTimestamp()
	nSecInTransit := time.Now().UnixNano() - startTime
	fmt.Printf("Message spent %d ns in transit\n", nSecInTransit)
	
	return nil, nil
}

func (c *Client) Start() {
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
}

func (c *Client) ClientData(key string) *Clientdata {
	return c.Dataset[key]
}

func (c *Client) InsertClientdata(clientData *Clientdata) {
	c.Dataset[clientData.BlobHash] = clientData
}