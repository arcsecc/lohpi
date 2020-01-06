package main

import (
	"fmt"
	"github.com/joonnna/ifrit"
	//"ifrit"
//	log "github.com/inconshreveable/log15"
	"math/rand"
	//"time"
)

type Application struct {
	Clients []*ifrit.Client
	ExitChan chan bool
	AppData string
}


func NewApplication() (*Application, error) {
	channel := make(chan bool, 1)
	clients := make([]*ifrit.Client, 0)
	
	app := &Application {
		ExitChan: channel,
	}

	for i := 0; i < 1; i++ {
		client, err := ifrit.NewClient()
		if err != nil {
			return nil, err
		}
	
		clients = append(clients, client)
		client.RegisterMsgHandler(app.simpleMessageHandler)
		fmt.Printf("Started node with IP = %s\n", client.Addr());
		go client.Start()
	}

	app.Clients = clients

	//fmt.Printf("len=%d cap=%d %v\n", len(clients), cap(clients), clients)
	return app, nil
}

// This callback will be invoked on each received message.
func (app *Application) simpleMessageHandler(data []byte) ([]byte, error) {

	return data, nil
}

func (app *Application) Start() {
	/*
	for {
		select {
			case <- app.ExitChan:
				return

			case <- time.After(time.Second * 0):
				idx := rand.Int() % len(app.Clients)
				randomClient := app.Clients[idx]
				//fmt.Printf("Sends message to %s\n", randomClient.Addr());
				ch := app.Clients[0].SendTo(randomClient.Addr(), []byte("msg"))
				
				//response := <- ch
				//fmt.Printf("Kake = %s\n", response)
		}
	}*/

	idx := rand.Int() % len(app.Clients)
	randomClient := app.Clients[idx]
	ch := app.Clients[0].SendTo(randomClient.Addr(), []byte("msg"))
	response := <- ch
	fmt.Printf("Kake = %s\n", response)
}

func main() {
	app, err := NewApplication()
	if err != nil {
		panic(err)
	}

	app.Start()
}

/*

func main() {
	const N_CLIENTS int = 2
	var clients [N_CLIENTS]ifrit.Client

	addClientsToNetwork(&clients, N_CLIENTS)

	// Send the first message 
	randomClient := clients[rand.Int() % len(clients)]
	ch := clients[0].SendTo(randomClient.Addr(), []byte("msg"))

	fmt.Printf("Address of this node = %s\n", clients[0].Addr())

	simpleMessagePassing(ch, &clients)
}

func simpleMessagePassing(ch <-chan []byte, clients *[2]ifrit.Client) {
	for {
		select {
			case response := <-ch:
				fmt.Printf("res = %s\n", string(response));
		
			case <-time.After(time.Second * 0):
				randomClient := clients[rand.Int() % len(clients)]
	//			fmt.Printf("Sends %s a message...\n", randomClient.Addr())
				
				for i := 0; i < 10; i++ {
					clients[0].SendTo(randomClient.Addr(), []byte("msg"))
				}
				
				randomClient.Stop()
		}
	}
}

func addClientsToNetwork(clients *[2]ifrit.Client, numClients int) {

	for i := 0; i < numClients; i++ {

		c, err := ifrit.NewClient()
		if err != nil {
			panic(err)
		}

		clients[i] = *c
		c.RegisterMsgHandler(simpleMessageHandler)
		go c.Start()
	}
}

// This callback will be invoked on each received message.
func simpleMessageHandler(data []byte) ([]byte, error) {
	//log.Info("Message received");
	//fmt.Println("Message received");
	return data, nil
}

*/
