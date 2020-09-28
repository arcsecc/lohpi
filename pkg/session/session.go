package session

import (
	"errors"
	"encoding/base64"
	"crypto/rand"
	"io"
	"sync"
	pb "github.com/tomcat-bit/lohpi/protobuf"
)

type Manager struct {
	clients 	map[string]*Entry
	lock        sync.RWMutex // protects session
}

// Entry used to keep track of who accesses what
type Entry struct {
	Client *pb.Client
	sessionId string
}

func NewManager() *Manager {
	return &Manager{
		clients: make(map[string]*Entry),
		lock:	sync.RWMutex{},
	}
}

// TODO implement ttl countdowns
func (m *Manager) CheckoutObjct(objectId string, e *Entry) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if _, ok := m.clients[objectId]; ok {
		return errors.New("Client '" + objectId + " 'already exists.")
	}

	e.sessionId = sessionId()
	m.clients[objectId] = e
	return nil 
}

func (m *Manager) AddClient(clientId string, c *pb.Client) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if _, ok := m.clients[clientId]; ok {
		return errors.New("Client '" + clientId + " 'already exists in the network.")
	}

	m.clients[clientId] = &Entry{c, sessionId()}
	return nil
}

func (m *Manager) ClientExists(clientId string) bool {
	m.lock.RLock()
	defer m.lock.RUnlock()
	_, ok := m.clients[clientId]
	return ok
}

func (m *Manager) IsCheckedOut(key string) bool {
	m.lock.RLock()
	defer m.lock.RUnlock()
	_, ok := m.clients[key]
	return ok
}

// Returns a unique session ID
func sessionId() string {
	b := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		panic(err)
	}
	return base64.URLEncoding.EncodeToString(b)
}