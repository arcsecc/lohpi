package client

import (
_	"fmt"
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

	// Attributes
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

	pk := pkix.Name{
		Locality: []string{localIpAddr},
	}

	cu, err := comm.NewCu(pk, viper.GetString("lohpi_ca_addr"))
	if err != nil {
		panic(err)
		return nil, err
	}

	clientConfig := comm.ClientConfig(cu.Certificate(), cu.CaCertificate(), cu.Priv())

	return &Client {
		clientConfig: clientConfig,
		clientName: name,
		attrMap: initializeAttributes(),
	}, nil
}

func (c *Client) Run() {
		
}

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
