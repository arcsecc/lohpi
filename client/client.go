package main

import (
	"fmt"
	//"github.com/joonnna/ifrit"
	"ifrit"
//	log "github.com/inconshreveable/log15"
//	"math/rand"
	"time"
)

type Application struct {
	Clients []*ifrit.Client
	ExitChan chan bool
	AppData string
}

func main() {

    // Enable line numbers in logging
   // log.SetFlags(log.LstdFlags | log.Lshortfile)

    // Will print: "[date] [time] [filename]:[line]: [text]"
 //   log.Println("Logging w/ line numbers on golangcode.com")

	app, err := NewApplication()
	if err != nil {
		panic(err)
	}

	app.Start()
}


func NewApplication() (*Application, error) {
	channel := make(chan bool, 1)
	clients := make([]*ifrit.Client, 0)
	
	app := &Application {
		ExitChan: channel,
	}

	for i := 0; i < 8; i++ {
		client, err := ifrit.NewClient()
		if err != nil {
			return nil, err
		}
	
		clients = append(clients, client)
		client.RegisterMsgHandler(app.simpleMessageHandler)
		//fmt.Printf("Started node with IP = %s\n", client.Addr());
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
	
	fmt.Printf("This node's address = %s\n", app.Clients[0].Addr());

	for {
		select {
			case <- app.ExitChan:
				return

			case <- time.After(time.Second * 1):

				fmt.Printf("Sending 8 messages...\n")
				for i := 0; i < 8; i++ {
					//idx := rand.Int() % len(app.Clients)
					randomClient := app.Clients[i]
					//fmt.Printf("Sends message to %s\n", randomClient.Addr());
					s := fmt.Sprintf("recipient: %s", randomClient.Addr())
					ch := app.Clients[0].SendTo(randomClient.Addr(), []byte(s))
				
					response := <- ch
					_ = response 
					fmt.Printf("Message: %s\n", response)
				}
				fmt.Println()
		}
	}
}
