package main

import (
	"fmt"
	///"github.com/joonnna/ifrit"
//	"ifrit"
	log "github.com/inconshreveable/log15"
//	"math/rand"
	"errors"
//	"encoding/gob"
//	"bytes"
	"time"
	"firestore/core/file"
	"firestore/core"
	"github.com/spf13/viper"
)

var (
	errCannotCreateApplication 		= errors.New("Cannot create application")
	errCannotAddClients 			= errors.New("Adding one or more clients to Application.Clients failed")
)

type Application struct {
	Users []*User
	StorageNodes []*core.Node
}

func main() {
	// Main entry point. Read configuration to set initial parameters
	readConfig()

	app, err := NewApplication()
	if err != nil {
		panic(err)
		log.Error("Could not create several clients")
	}

	app.Run()
}


func NewApplication() (*Application, error) {
	/*
	numFirestoreNodes := viper.GetInt32("firestore_nodes")
	replication_degree := viper.GetInt32("replication_degree")
	
	filesPerUser := viper.GetInt32("files_per_user")
	fileSize := viper.GetInt32("file_size")
	*/

	app := &Application {}

	numUsers := viper.GetInt("num_users")
	users, err := CreateApplicationUsers(numUsers)
	if err != nil {
		log.Error("Could not create users")
		return nil, err
	}

	numFirestoreNodes := viper.GetInt("firestore_nodes")
	firestore_nodes, err := CreateFirestoreNodes(numFirestoreNodes)
	if err != nil {
		log.Error("Could not create Firestore storage nodes")
		return nil, err
	}

	app.Users = users
	app.StorageNodes = firestore_nodes
	
	// HACK
	for _, u := range app.Users {
		u.SetStorageNodesList(firestore_nodes)
	}

	return app, nil
}

// Main entry point for running the application
func (app *Application) Run() {

	user := app.Users[0]
	file := user.UserFiles()[0]
	_ = <- user.StoreFileRemotely(file)
	//fmt.Printf("Resp = %s\n", response)

	select {
		case <- time.After(0):
			//fmt.Printf("Resp = %s\n", response)
	}			
}

func CreateFirestoreNodes(numFirestoreNodes int) ([]*core.Node, error) {
	nodes := make([]*core.Node, 0)

	for i := 0; i < numFirestoreNodes; i++ {
		nodeID := fmt.Sprintf("fsNode_%d", i + 1)
		node, err := core.NewNode(nodeID)
		if err != nil {
			log.Error("Could not create Firestore storage node")
			return nil, err
		}

		nodes = append(nodes, node)
	}

	return nodes, nil
}

func CreateApplicationUsers(numUsers int) ([]*User, error) {
	users := make([]*User, 0)

	for i := 0; i < numUsers; i++ {
		username := fmt.Sprintf("user_%d", i + 1)
		user := NewUser(username)
	
		files, err := CreateFiles(1, user)
		if err != nil {
			panic(err)
		}

		user.SetFiles(files)
		users = append(users, user)
	}

	return users, nil
}

func CreateFiles(numFiles int, user *User) ([]*file.File, error) {
	files := make([]*file.File, 0)

	for i := 0; i < numFiles; i++ {
		file, err := file.NewFile(10, i, user.OwnerName)
		if err != nil {
			panic(err)
		}

		files = append(files, file)
	}

	return files, nil
}

func readConfig() error {
	viper.SetConfigName("firestore_config")
	viper.AddConfigPath("/var/tmp")
	viper.AddConfigPath(".")

	viper.SetConfigType("yaml")

	err := viper.ReadInConfig()
	if err != nil {
		return err
	}

	// Behavior variables
	viper.SetDefault("firestore_nodes", 1)
	viper.SetDefault("replication_degree", 1)
	viper.SetDefault("files_per_user", 1)
	viper.SetDefault("num_users", 1)
	viper.SetDefault("files_per_user", 1)
	viper.SetDefault("file_size", 256)
	viper.SafeWriteConfig()

	return nil
}

//Firestore: scalable, secure and sharded one-hop key-value store 

/*func (app *Application) SendData(client *Client, payload *Clientdata) chan []byte {
	// Get needed application clients
	ifritClient := client.FireflyClient()
	masterIfritClient := app.MasterClient.FireflyClient()

	// Set payload properties prior to sending it
	payload.SetEpochTimestamp(time.Now().UnixNano())
	payload.SetInTransit(true)

	var b bytes.Buffer
	e := gob.NewEncoder(&b)
	if err := e.Encode(payload); err != nil {
	   panic(err)
	}
	//fmt.Println("Encoded Struct ", b)

	return masterIfritClient.SendTo(ifritClient.Addr(), b.Bytes())
}*/

/** Gossip messaging here */
/*
func (app *Application) RunGossipMessaging() {
	for {
		select {

			case <- time.After(time.Second * 0):
				//randomClient := app.Clients[rand.Int() % len(app.Clients)]
				s := fmt.Sprintf("Client %s sends message\n", app.Clients[0].Addr())
				app.Clients[0].SetGossipContent([]byte(s))
				
				//ch := ap'p.Clients[0].SendTo(randomClient.Addr(), []byte(s))
				//response := <- ch
		}
	}
}*/
/*
func (app *Application) gossipMessageHandler(data []byte) ([]byte, error) {
	fmt.Printf("In simpleGossipHandler() -- message: %s\n", string(data))
	return data, nil
}*/
/*
func (app *Application) gossipResponseHandler(data []byte) {
	fmt.Printf("In gossipResponseHandler() -- message: %s\n", string(data))
}	
*/

/****** simple message passing here ******/
/*
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

				//fmt.Printf("Sending 8 messages...\n")
				for i := 1; i < nClients; i++ {
					//idx := rand.Int() % len(app.Clients)
					recipient := app.Clients[i]
					//fmt.Printf("Sends message to %s\n", randomClient.Addr());
					s := fmt.Sprintf("recipient: %s", recipient.Addr())
					ch := app.Clients[0].SendTo(recipient.Addr(), []byte(s))
				
					response := <- ch
					fmt.Printf("Response: %s\n", response)
				}
				//fmt.Println()
		}
	}
}


*/