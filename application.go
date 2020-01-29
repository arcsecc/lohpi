package main

import (
	"fmt"
	///"github.com/joonnna/ifrit"
//	"ifrit"
	log "github.com/inconshreveable/log15"
_	"math/rand"
	"errors"
//	"encoding/gob"
//	"bytes"
	//"time"
	"firestore/core/file"
	"firestore/core"
	"github.com/spf13/viper"
)

var (
	errCannotCreateApplication 		= errors.New("Cannot create application")
	errCannotAddClients 			= errors.New("Adding one or more clients to Application.Clients failed")
)

type Application struct {
	Subject *core.Subject				// data owners. Pub
	StorageNodes []*core.Node		// data storage units
	MasterNode *core.Masternode	// single point-of-entry node for API calls
	DataUsers []*core.Datauser		// data users. Sub
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

	numFiles := viper.GetInt("files_per_subject")
	//numSubjects := viper.GetInt("num_subjects")
	numDataUsers := viper.GetInt("data_users")

	masterNode, err := core.NewMasterNode()
	if err != nil {
		panic(err)
	}
	
	app.MasterNode = masterNode
	storageNodes := masterNode.StorageNodes()
	app.DataUsers = CreateDataUsers(numDataUsers)
	app.Subject = CreateApplicationSubject()

	initializeFilesInNetwork(storageNodes, app.Subject, numFiles)
	return app, nil
}

// Main entry point for running the application
func (app *Application) Run() {
	master := app.MasterNode

	// let's try the first subject...
	TestPermitAllStorageNodes(app.Subject, master)

}

func TestPermitAllStorageNodes(s *core.Subject, master *core.Masternode) {
	allStorageNodes := master.StorageNodes()

	node := allStorageNodes[1]
	files := node.NodeSubjectFiles()
	master.PermitStorageNodes(s, allStorageNodes)

	for _, node := range allStorageNodes {
		fmt.Printf("Node %s\n", node.Name())
		for _, f := range files {
			fmt.Printf("%s\n", f.FilePermission())
		}
	}
}	

// Environment initialization functions //
func initializeFilesInNetwork(nodes []*core.Node, subject *core.Subject, numFiles int) {
	fileSize := viper.GetInt("file_size")

	// might add more subjects too...
	for i := 0; i < numFiles; i++ {
		directoryOrder := i
		for _, node := range nodes {
			file, err := file.NewFile(fileSize, directoryOrder, subject.Name(), node.Name(), file.FILE_NO_PERMISSION)
			if err != nil {
				panic(err)
			}
			
			node.AppendSubjectFile(file)
		}
	}


	/*
	for _, subject := range subjects {
		for i := 0; i < numFiles; i++ {
			files := make([]*file.File, 0)
			for _, node := range nodes {
				directoryOrder := i
				file, err := file.NewFile(fileSize, directoryOrder, subject.Name(), node.Name(), file.FILE_NO_PERMISSION)
				if err != nil {
					panic(err)
				}
				files = append(files, file)
				node.SetSubjectFiles(files)
			}
		}
	}*/
}

func CreateApplicationSubject() *core.Subject {
	subjectName := fmt.Sprintf("subject_%d", 1)
	return core.NewSubject(subjectName)
	/*subjects := make([]*core.Subject, 0)

	for i := 0; i < numSubjects; i++ {
		subjectName := fmt.Sprintf("subject_%d", i + 1)
		subject := core.NewSubject(subjectName)
		subjects = append(subjects, subject)
	}
	return subjects*/
}

func CreateDataUsers(numDataUsers int) ([]*core.Datauser) {
	dataUsers := make([]*core.Datauser, 0)

	for i := 0; i < numDataUsers; i++ {
		id := fmt.Sprintf("dataUser_%d\n", i + 1)
		user := core.NewDataUser(id)
		dataUsers = append(dataUsers, user)
	}

	return dataUsers
}

/*
func CreateFiles(numFiles int, user *User) ([]*file.File, error) {
	files := make([]*file.File, 0)

	for i := 0; i < numFiles; i++ {
		file, err := file.NewFile(10, i, user.OwnerName)
		if err != nil {
			panic(err)
		}

		fmt.Printf("File = %s\n", file.GetFileAsBytes())
		files = append(files, file)
	}

	return files, nil
}
*/

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
	viper.SetDefault("num_subjects", 2)
	viper.SetDefault("data_users", 1)
	viper.SetDefault("files_per_subject", 1)
	viper.SetDefault("file_size", 256)
	viper.SafeWriteConfig()

	return nil
}

//Firestore: scalable, secure and sharded one-hop key-value store 

/*func (app *Application) SendData(client *Client, payload *Clientdata) chan []byte {
	// Get needed application clients
	ifritClient := client.FirefzlyClient()
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