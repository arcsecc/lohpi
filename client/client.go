package main

import (
	"fmt"
	//"github.com/joonnna/ifrit"
	"ifrit"
	log "github.com/inconshreveable/log15"
//	"math/rand"
	"time"
	"errors"
)

var (
	errCannotCreateApplication 		= errors.New("Cannot create application")
	errCannotAddClients 			= errors.New("Adding one or more clients to Application.Clients failed")
)

type Application struct {
	Clients []*ifrit.Client
	ExitChan chan bool
	AppData string
}

func main() {

	nClients := 4
	app, err := NewApplication(nClients)
	if err != nil {
		panic(err)
	}

	app.Start()
	//app.RunGossipMessaging()
	app.RunSimpleMessaging(nClients)
}

func NewApplication(nClients int) (*Application, error) {
	channel := make(chan bool, 1)

	clients, err := AddClients(nClients)
	if err != nil {
		return nil, errCannotCreateApplication
	}

	app := &Application {
		ExitChan: channel,
	}

	app.Clients = clients
	return app, nil
}

func AddClients(nClients int) ([]*ifrit.Client, error) {
	clients := make([]*ifrit.Client, 0)

	for i := 0; i < nClients; i++ {
		client, err := ifrit.NewClient()
		if err != nil {
			log.Error("Could not add client to application")
			return nil, errCannotAddClients
		}
	
		clients = append(clients, client)	
	}

	return clients, nil
}

func (app *Application) Start() {
	for _, client := range app.Clients {
		go client.Start()
		//client.RegisterGossipHandler(app.gossipMessageHandler)
		//client.RegisterResponseHandler(app.gossipResponseHandler)
		client.RegisterMsgHandler(app.simpleMessageHandler)
	}
}

/** Gossip messaging here */
func (app *Application) RunGossipMessaging() {
	for {
		select {

			// TODO: exit main application loop
			case <- app.ExitChan:
				return

			case <- time.After(time.Second * 0):
				//randomClient := app.Clients[rand.Int() % len(app.Clients)]
				s := fmt.Sprintf("Client %s sends message\n", app.Clients[0].Addr())
				app.Clients[0].SetGossipContent([]byte(s))
				
				//ch := ap'p.Clients[0].SendTo(randomClient.Addr(), []byte(s))
				//response := <- ch
		}
	}
}

func (app *Application) gossipMessageHandler(data []byte) ([]byte, error) {
	fmt.Printf("In simpleGossipHandler() -- message: %s\n", string(data))
	return data, nil
}

func (app *Application) gossipResponseHandler(data []byte) {
	fmt.Printf("In gossipResponseHandler() -- message: %s\n", string(data))
}	


/****** simple message passing here ******/
func (app *Application) simpleMessageHandler(data []byte) ([]byte, error) {
	fmt.Printf("Message: %s\n", string(data));
	return data, nil
}

func (app *Application) RunSimpleMessaging(nClients int) {
	fmt.Printf("This node's address = %s\n", app.Clients[0].Addr());

	for {
		select {
			case <- app.ExitChan:
				return

			case <- time.After(time.Second * 1):

				fmt.Printf("Sending 8 messages...\n")
				for i := 1; i < nClients; i++ {
					//idx := rand.Int() % len(app.Clients)
					recipient := app.Clients[i]
					//fmt.Printf("Sends message to %s\n", randomClient.Addr());
					s := fmt.Sprintf("recipient: %s", recipient.Addr())
					ch := app.Clients[0].SendTo(recipient.Addr(), []byte(s))
				
					response := <- ch
					fmt.Printf("%s\n", response)
				}
				fmt.Println()
		}
	}
}


