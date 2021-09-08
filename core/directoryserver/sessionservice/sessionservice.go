package sessionservice

import (
	"strconv"
	"errors"
	"net"
	"net/http"
	"net/url"
	"github.com/joonnna/workerpool"
	pb "github.com/arcsecc/lohpi/protobuf"
	log "github.com/sirupsen/logrus"
)

var (
	ErrNoClient = errors.New("Client is nil")
	ErrNoDNSName = errors.New("DNS name is nil")
)

type SessionService  struct {
	dispatcher *workerpool.Dispatcher

	connections net.Conn
}

// See pb.GossipMessage
type PolicyBatch struct {
	Policies []*Policy `json:"policies"`
}

type Policy struct {
	DatasetIdentifier string `json:"dataset_identifier"`
	Content bool `json:"content"`
	Version string `json:"version"`
}

func NewSessionService() (*SessionService, error) {
	return &SessionService{
		dispatcher: workerpool.NewDispatcher(50),
	}, nil
}

func (c *SessionService) StartClientSession(handler func(w http.ResponseWriter, r *http.Request)) {
	log.Println("Starting client session")
	
}

func (c *SessionService) SubmitPolicy(client *pb.Client, policyBatch *PolicyBatch) error {
	if client == nil {
		return ErrNoClient
	}

	dnsName := client.GetDNSName()
	if dnsName == "" {
		return ErrNoDNSName
	}

	c.dispatcher.Submit(func() {
		c.revokeDataset(policyBatch, dnsName)
	})

	return nil
}

func (c *SessionService) revokeDataset(policyBatch *PolicyBatch, remoteAddr string) {
	
	// Prepare request. Use DNS name for now
	req := &http.Request{
		Method: "POST",
		URL: &url.URL{
			Scheme: "http", //https
			Host:   remoteAddr + ":" + strconv.Itoa(5555),
			Path:   "/policies",
		},
		Header: http.Header{},
	}
	_ = req
}

func (c *SessionService) Stop() {
	c.dispatcher.Stop()
}