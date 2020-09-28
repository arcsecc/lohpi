package main

import (
	"fmt"
	"flag"
	"os"
	"log"
	"net"
	"net/http"
_	"time"
_	"context"
	
_	"github.com/golang/protobuf/ptypes/empty"
	"github.com/jinzhu/configor"
	"github.com/tomcat-bit/lohpi/pkg/client"
)

type ApplicationConfig struct {
	MuxIP				string		`default:"127.0.1.1:8081"`
	LocalAddr 				string 		`default:"127.0.1.1:8086"`
}

type Application struct {
	config ApplicationConfig

	httpPort int 
	httpListener net.Listener
	httpServer   *http.Server

	client *client.Client
}

var appConfig = struct {
	ClientConfig client.Config
}{}

func main() {
	var configFile string
	var n int
	
	arg := flag.NewFlagSet("args", flag.ExitOnError)
	arg.StringVar(&configFile, "c", "config.yml", `Configuration file for policy store. If not set, use default configuration values.`)
	arg.IntVar(&n, "n", 10, "Iterates through the set of data users n times. For each data user, use the Lohpi rpc definitions. Default value is 10.")
	arg.Parse(os.Args[1:])


	if !exists(configFile) {
		fmt.Fprintf(os.Stderr, "'%s' does not exist. Exiting.\n", configFile)
		os.Exit(2)
	}

	if n < 0 {
		fmt.Fprintf(os.Stdout, "n cannot be negative. Exiting")
		os.Exit(2)
	}

	config, err := loadConfiguration(configFile)
	if err != nil {
		panic(err)
	}

	log.Println("Config:", config)

	c, err := client.NewClient("Awesome client", config)
	if err != nil {
		panic(err)
	}

	c.Start()
	c.Stop()
}

func loadConfiguration(configFile string) (*client.Config, error) {
	var appConfig = struct {
		ClientConfig client.Config
	}{}
	

	conf := configor.New(&configor.Config{
		ErrorOnUnmatchedKeys: true,
		Verbose: true,
		Debug: true,
	})

	
	if err := conf.Load(&appConfig, configFile); err != nil {
		return nil, err
	}

	return &appConfig.ClientConfig, nil
}

func exists(name string) bool {
	if _, err := os.Stat(name); err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return true
}