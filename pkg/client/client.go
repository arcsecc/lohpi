package client

import (
	"os"
	"strings"
	"sync"
	"crypto/x509/pkix"
	"log"
	"time"
	"context"

	pb "github.com/tomcat-bit/lohpi/protobuf"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/tomcat-bit/lohpi/pkg/comm"
	"github.com/tomcat-bit/lohpi/pkg/netutil"
	"github.com/abiosoft/ishell"
)

// Configuration struct for the client. 
type Config struct {
	// The mount point of the file tree
	LocalMountPoint string 		`required:"true"`

	// Mux's remote address
	MuxIP string		`default:"127.0.1.1:8081"`

	LohpiCaAddr string	`default:"127.0.1.1:8301"`
}

type Client struct {
	name string

	muxId []byte

	config *Config
	configLock sync.RWMutex

	policyAttributes []byte
	attrLock sync.RWMutex

	muxClient *comm.MuxGRPCClient
}

// Returns a new client using the given name and configuration struct.
func NewClient(name string, config *Config) (*Client, error) {
	port := netutil.GetOpenPort()
	listener, err := netutil.ListenOnPort(port)
	if err != nil {
		return nil, err
	}

	// Setup X509 certificate
	pk := pkix.Name{
		Locality: []string{listener.Addr().String()},
	}

	cu, err := comm.NewCu(pk, config.LohpiCaAddr)
	if err != nil {
		return nil, err
	}

	muxClient, err := comm.NewMuxGRPCClient(cu.Certificate(), cu.CaCertificate(), cu.Priv())
	if err != nil {
		return nil, err
	}

	// Create working directory for the client
	if _, err := os.Stat(config.LocalMountPoint); os.IsNotExist(err) {
		if err := os.Mkdir(config.LocalMountPoint, 0755); err != nil {
			return nil, err
		}
	} else {
		if err := os.RemoveAll(config.LocalMountPoint); err != nil {
			return nil, err
		}
		if err := os.Mkdir(config.LocalMountPoint, 0755); err != nil {
			return nil, err
		}
	}

	return &Client{
		name:		name, 
		muxClient:  muxClient,
		attrLock:	sync.RWMutex{},
		config: 	config,
		configLock: sync.RWMutex{},
	}, nil
}

// Starts the client by connecting to the Lohpi network
func (c *Client) Start() {
	if err := c.joinNetwork(); err != nil {
		panic(err)
	}

	log.Println("Starts shell")
	shell := ishell.New()

	// Top-level command to fetch remote objects (here, by 'object' it refers to anything)
	getCmd := &ishell.Cmd{
		Name:    	"get",
		Help:    	"Remote get",
		LongHelp: 	`Fetches remote object by specifying a sub-command. The following sub-commands are available:
					headers`,
		Func: func(con *ishell.Context) {
			log.Println("Usage: get headers|object <object id ...>|metadata <object id ...>")
		},
	}

	// Fetch remote object headers
	getCmd.AddCmd(&ishell.Cmd{
		Name:    	"headers",
		Help:    	"Fetch remote object header IDs",
		LongHelp: 	`Fetch all remote object headers IDs and print them in the terminal`,
		Func: func(con *ishell.Context) {
			ids, err := c.fetchObjectIDs()
			if err != nil {
				panic(err)
			}
			
			for _, id := range ids {
				log.Println("Object ID:", id)
			}
		},
	})

	// Fetch metadata
	// TODO: use id and don't fetch ALL object headers
	getCmd.AddCmd(&ishell.Cmd{
		Name:    	"metadata",
		Help:    	"Fetch remote object header IDs",
		LongHelp: 	`Fetch all remote object headers IDs and print them in the terminal`,
		Func: func(con *ishell.Context) {
			metadata, err := c.fetchObjectMetadata()
			if err != nil {
				panic(err)
			}
			
			for _, md := range metadata {
				log.Println("Metadata:", md)
			}
		},
	})

	// Fetch remote object data
	getCmd.AddCmd(&ishell.Cmd{
		Name:    	"object",
		Help:    	"Fetch remote object header IDs",
		LongHelp: 	`Fetch all remote object headers IDs and print them in the terminal`,
		Func: func(con *ishell.Context) {
			for _, s := range con.Args {
				obj := strings.TrimLeft(strings.TrimRight(s, "\""),"\"")
				if err := c.fetchObjectData(obj); err != nil {
					panic(err)
				}
			}
		},
	})
	
	// Fetch remote object data
	getCmd.AddCmd(&ishell.Cmd{
		Name:    	"attributes",
		Help:    	"Fetch remote object header IDs",
		LongHelp: 	`Fetch all remote object headers IDs and print them in the terminal`,
		Func: func(con *ishell.Context) {
			log.Println(string(c.getPolicyAttributes()))
		},
	})

	shell.AddCmd(getCmd)

	// Set policy attribute. Only used for testing!!
	setCmd := &ishell.Cmd{
		Name:    	"set",
		Help:    	"Sets the policy attributes of the client. ONLY MEANT FOR DEMONSTRATION",
		Func: func(con *ishell.Context) {},
	}

	setCmd.AddCmd(&ishell.Cmd{
		Name:    	"attributes",
		Help:    	"",
		LongHelp: 	`Fetches remote object by specifying a sub-command. The following sub-commands are available:
					headers`,
		Func: func(con *ishell.Context) {
			for _, s := range con.Args {
				a := strings.TrimLeft(strings.TrimRight(s, "\""),"\"")
				c.setPolicyAttributes([]byte(a))
			}
		},
	})

	shell.AddCmd(setCmd)

	// start shell
	shell.Run()

	// teardown
	shell.Close()
}

func (c *Client) joinNetwork() error {
	conn, err := c.muxClient.Dial(c.config.MuxIP)
	if err != nil {
		panic(err)
	}

	defer conn.CloseConn()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second * 20)
	defer cancel()

	handshakeResp, err := conn.ClientHandshake(ctx, &pb.Client{Name: c.name, PolicyAttribute: c.getPolicyAttributes()})
	if err != nil {
		panic(err)
	}

	c.muxId = handshakeResp.GetId()

	return nil 
}

func (c *Client) fetchObjectData(objectId string) error {
	conn, err := c.muxClient.Dial(c.config.MuxIP)
	if err != nil {
		panic(err)
	}

	defer conn.CloseConn()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second * 20) // Sane timeout value here!
	defer cancel()

	log.Println("c.getPolicyAttributes():", c.getPolicyAttributes())

	objFiles, err := conn.GetObjectData(ctx, &pb.DataUserRequest{
		Client: &pb.Client{
			Name: c.name,
			PolicyAttribute: c.getPolicyAttributes(),
		},
		ObjectName: objectId,
	})

	if err != nil {
		log.Println(err.Error())
	}


	log.Println(objFiles)

	return nil 
}

func (c *Client) fetchObjectIDs() ([]string, error) {
	headers, err := c.fetchObjectHeaders()
	if err != nil {
		panic(err)
	}

	ids := make([]string, 0)
	for _, h := range headers.GetObjectHeaders() {
		ids = append(ids, h.GetName())
	}

	return ids, nil
}

func (c *Client) fetchObjectMetadata() ([]string, error) {
	headers, err := c.fetchObjectHeaders()
	if err != nil {
		panic(err)
	}

	metadata := make([]string, 0)
	for _, h := range headers.GetObjectHeaders() {
		metadata = append(metadata, string(h.GetMetadata().GetContent()))
	}

	return metadata, nil
}

func (c *Client) fetchObjectHeaders() (*pb.ObjectHeaders, error) {
	conn, err := c.muxClient.Dial(c.config.MuxIP)
	if err != nil {
		panic(err)
	}

	defer conn.CloseConn()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second * 20)
	defer cancel()

	return conn.GetObjectHeaders(ctx, &empty.Empty{})
}

// Stops the connection and 
func (c *Client) Stop() {

}

func (c *Client) SetConfiguration(config *Config) {
	c.configLock.Lock()
	defer c.configLock.Unlock()
	c.config = config
}

func (c *Client) Configuration() *Config {
	c.configLock.RLock()
	defer c.configLock.RUnlock()
	return c.config
}

func (c *Client) Name() string {
	return c.name
}

func (c *Client) setPolicyAttributes(p []byte) {
	c.attrLock.RLock()
	defer c.attrLock.RUnlock()
	c.policyAttributes = p
}

// Returns the attributes of the client used to access remote files.
func (c *Client) getPolicyAttributes() []byte {
	c.attrLock.RLock()
	defer c.attrLock.RUnlock()
	return c.policyAttributes
}

func (c *Client) storeFiles(objects *pb.ObjectFiles) error {
	for _, o := range objects.GetObjectFiles() {
		// create file at file mount
		// Track files being stored at Lohpi. Undo/remove files if the event 
		// of a more restrictive policy
	}
}