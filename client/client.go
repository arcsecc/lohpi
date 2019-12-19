/*package main;

import (
	"github.com/joonnna/ifrit"
	"time"
	"bytes"
	"fmt"
	"encoding/json"
)

// Mockup application
type application struct {
	ifritClient *ifrit.Client

	exitChan chan bool
	data     *appData
}

// Mockup data structure
type appData struct {
	Users map[int]*user
}

// Mockup users
type user struct {
	FirstName string
	LastName  string
	Address   string
}

// We store the client instance within the application
// such that we can communicate with it as we see fit
func newApp() (*application, error) {
	c, err := ifrit.NewClient()
	if err != nil {
		return nil, err
	}

	return &application{
		ifritClient: c,
	}, nil
}

// This callback will be invoked on each received message.
func (a *application) handleMessages(data []byte) ([]byte, error) {
	received := &appData{}

	err := json.NewDecoder(bytes.NewReader(data)).Decode(received)
	if err != nil {
		return nil, err
	}

	return []byte("kake"), nil
}

// Start the mockup application
func (a *application) Start() {
	a.ifritClient.RegisterMsgHandler(a.handleMessages)

	id := 1

	for {
		select {
			case <-a.exitChan:
				return

			case <-time.After(time.Second * 1):
				fmt.Printf("Should add user now\n");
				a.AddUser(id)
				id += 1
			}
		}
}

func (a *application) AddUser(id int) {
	c, err := ifrit.NewClient()
	if err != nil {
	    panic(err)
	}

	go c.Start()
	fmt.Printf("%p", &a.data);
}

func main() {
	a, err := newApp()
	if err != nil {
		panic(err)
	}

	a.Start()
}
*/

/*
package main

import (
	"fmt"
	"ifrit"
	//"time"
	//"math/rand"
)


func getClientInfo(client ifrit.Client, numClients int) {
	allNetworkMembers := client.Members()

	for i := range allNetworkMembers {
		fmt.Printf("Member's IP: %s\n", allNetworkMembers[i])
	}
}

func addClientsToNetwork(clients *[1]ifrit.Client, numClients int) {

	for i := 0; i < numClients; i++ {

		c, err := ifrit.NewClient()
		if err != nil {
			panic(err)
		}

		clients[i] = *c
		go c.Start()
		fmt.Printf("Started node with address %s\n", c.Addr())
	}
}

// This callback will be invoked on each received message.
func yourMessageHandler(data []byte) ([]byte, error) {
	fmt.Println("Message received");
	return data, nil
}

func main() {
	fmt.Printf("Creating a network of clients...")

	const N_CLIENTS int = 1
	var clients [N_CLIENTS]ifrit.Client

	addClientsToNetwork(&clients, N_CLIENTS)

	client := &clients[0]
	client.RegisterMsgHandler(yourMessageHandler)
	getClientInfo(*client, N_CLIENTS)
	
	// Testing connectivity in the graph
	members := client.Members()
	fmt.Printf("This node's IP = %s\n", client.Addr());
	fmt.Printf("Sending message to %s\n", string(members[0]))

	ch := client.SendTo(string(members[0]), []byte("msg"))

	for {
		select {
			case response := <-ch:
				fmt.Printf("s\n", response);
		}
	}
}*/


package main

import (
	"fmt"
	"ifrit"
	//"math/rand"
)

func addClientsToNetwork(clients *[2]ifrit.Client, numClients int) {

	for i := 0; i < numClients; i++ {

		c, err := ifrit.NewClient()
		if err != nil {
			panic(err)
		}

		clients[i] = *c
		go c.Start()
		c.RegisterMsgHandler(yourMessageHandler)
	}
}

// This callback will be invoked on each received message.
func yourMessageHandler(data []byte) ([]byte, error) {
	fmt.Println("Message received");
	return data, nil
}

func main() {
	fmt.Printf("Creating a network of clients...\n\n")

	// Client 1...
	/*
	c1, err := ifrit.NewClient()
	if err != nil {
		panic(err)
	}
	go c1.Start()

	// Client 2...
	c2, err := ifrit.NewClient()
	if err != nil {
		panic(err)
	}
	go c2.Start()*/

	const N_CLIENTS int = 2
	var clients [N_CLIENTS]ifrit.Client
	addClientsToNetwork(&clients, N_CLIENTS)

	// Testing Fireflies connectivity...
	//fmt.Printf("c1.Addr() = %s\n", c1.Addr());
	//fmt.Printf("Sending message to %s\n", string(c2.Addr()))

	ch := clients[0].SendTo(string(clients[1].Addr()), []byte("msg"))

	for {
		select {
			case response := <-ch:
				fmt.Printf("res = %s\n", string(response));
		}
	}
}