package client

import (
	"log"
	"time"
	"io/ioutil"
	"bytes"
	"encoding/json"
	"encoding/gob"
	"net/http"
	"fmt"
	"crypto/tls"
	"crypto/x509/pkix"
	
	"firestore/comm"
	"firestore/netutil"
	"firestore/core/message"

	"github.com/spf13/viper"
)

type Client struct {
	// TLS configuration used when interacting with the MUX over HTTPS
	clientConfig *tls.Config

	// A unique identifier of the client
	clientName string

	// Attributes of the client
	attrMap map[string]string
}

func NewClient(name string) (*Client, error) {
	if err := readConfig(); err != nil {
		panic(err)
	}

	localIpAddr, err := netutil.LocalIP()
	if err != nil {
		return nil, err
	}

	attrMap := initializeAttributes()
	b := new(bytes.Buffer)
    e := gob.NewEncoder(b)

    // Encoding the map
    err = e.Encode(attrMap)
    if err != nil {
        panic(err)
    }

	pk := pkix.Name{
		Locality: []string{localIpAddr},
		CommonName: name, 
	}

	cu, err := comm.NewCu(pk, viper.GetString("lohpi_ca_addr"))
	if err != nil {
		panic(err)
		return nil, err
	}

	clientConfig := comm.ClientConfig(cu.Certificate(), cu.CaCertificate(), cu.Priv())

	return &Client {
		clientConfig: 	clientConfig,
		clientName: 	name,
		attrMap:	 	attrMap,
	}, nil
}

func (c *Client) Run() {
	log.Printf("Client %s will test some basic features of Lohpi...\n", c.clientName)	
	
	// Test the /get_studies endpoint
	//c.TestGetStudies()

	// Test the /get_study_data endpoint
	c.TestGetStudyData()
}

func (c *Client) TestGetStudyData() {
	log.Printf("Running /get_study_data a couple of times...\n")
	muxURL := "https://127.0.1.1:8080/get_study_data"

	var msg struct {
		Str string
	}

	msg.Str = "hello"
	jsonStr, err := json.Marshal(msg)
	if err != nil {
		log.Fatal(err)
    }

	req, err := http.NewRequest("GET", muxURL, bytes.NewBuffer(jsonStr))
	req.Header.Set("Content-Type", "application/json")

	transport := &http.Transport{
		TLSClientConfig: c.clientConfig,
	}
	client := &http.Client{
		Transport: transport,
	}

	response, err := client.Do(req)
    if err != nil {
		log.Fatal(err)
	}

	defer response.Body.Close()
	if response.StatusCode != int(http.StatusOK) {
		errMsg := fmt.Sprintf("%s", response.Body)
		log.Print(errMsg)
		return
	}

	bodyBytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("%s", bodyBytes)
	response.Body.Close()
	log.Printf("/get_studies seems to work :)\n")
}


func (c *Client) TestGetStudies() {
	log.Printf("Running /get_studies a couple of times...\n")
	muxURL := "https://127.0.1.1:8080/get_studies"
	req, err := http.NewRequest("GET", muxURL, nil)
	if err != nil {
		panic(err)
	}

	transport := &http.Transport{
		TLSClientConfig: c.clientConfig,
	}

	client := &http.Client{Transport: transport}
	for i := 0; i < 5; i++ {
		<-time.After(1 * time.Second)

		response, err := client.Do(req)
    	if err != nil {
	        panic(err)
		}

		if response.StatusCode != int(http.StatusOK) {
			errMsg := fmt.Sprintf("%s", response.Body)
			fmt.Printf("Error from mux: %s\n", errMsg)
			response.Body.Close()
			return
		}

		bodyBytes, err := ioutil.ReadAll(response.Body)
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("%s", bodyBytes)
		response.Body.Close()
	}
	log.Printf("/get_studies seems to work :)\n")
}

// hacky af
func initializeAttributes() map[string]string {
	// TODO: move identifiers in message module to somewhere else!
	attrMap := make(map[string]string)
	
	// TODO: initialize the map in A LOT better way than this!!
	attrMap[message.Country] = "norway"
	attrMap[message.Study] = "study1"
	attrMap[message.Research_network] = "UNN"
	return attrMap
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
	//viper.SetDefault("num_subjects", 2)
	//viper.SetDefault("num_studies", 10)          
	//viper.SetDefault("data_users", 1)
	//viper.SetDefault("files_per_study", 2)
	//viper.SetDefault("file_size", 256)
	viper.SetDefault("fuse_mount", "/home/thomas/go/src/firestore")
	//viper.SetDefault("set_files", true)
	viper.SetDefault("lohpi_ca_addr", "127.0.1.1:8301")
	viper.SafeWriteConfig()
	return nil
}
