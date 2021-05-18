package lohpi

import (
	"github.com/arcsecc/lohpi/core/policystore"
	"time"
)

type Config struct {
	// Policy store specific
	Name                     string  `default:"Policy store"`
	Host                     string  `default:"127.0.1.1"`
	BatchSize                int     `default:"10"`
	GossipInterval           uint32  `default:"10"`
	Port                     int     `default:"8083"`
	GRPCPort                 int     `default:"8084"`
	MulticastAcceptanceLevel float64 `default:"0.5"`
	NumDirectRecipients      int     `default:"1"`

	// Other parameters
	MuxAddress               string `default:"127.0.1.1:8081"`
	LohpiCaAddr              string `default:"127.0.1.1:8301"`
	RecIP                    string `default:"127.0.1.1:8084"`
	PolicyStoreGitRepository string `required:"true"`
}

type PolicyStoreOption func(*PolicyStore)

// Sets the name of the policy store. Default value is "".
func PolicyStoreWithName(name string) PolicyStoreOption {
	return func(p *PolicyStore) {
		p.config.Name = name
	}
}

func PolicyStoreWithHost(host string) PolicyStoreOption {
	return func(p *PolicyStore) {
		p.config.Host = host
	}
}

func PolicyStoreWithPolicyBatchSize(batchSize int) PolicyStoreOption {
	return func(p *PolicyStore) {
		p.config.PolicyBatchSize = batchSize
	}
}

func PolicyStoreWithGossipInterval(interval time.Duration) PolicyStoreOption {
	return func(p *PolicyStore) {
		p.config.GossipInterval = interval
	}
}

func PolicyStoreWithHTTPPort(port int) PolicyStoreOption {
	return func(p *PolicyStore) {
		p.config.HTTPPort = port
	}
}

func PolicyStoreWithGRPCPort(port int) PolicyStoreOption {
	return func(p *PolicyStore) {
		p.config.GRPCPort = port
	}
}

func PolicyStoreWithMulticastAcceptanceLevel(threshold float64) PolicyStoreOption {
	return func(p *PolicyStore) {
		p.config.MulticastAcceptanceLevel = threshold
	}
}

func PolicyStoreWithDirectRecipients(n int) PolicyStoreOption {
	return func(p *PolicyStore) {
		p.config.DirectRecipients = n
	}
}

func PolicyStoreWithLohpiCaConnectionString(addr string, port int) PolicyStoreOption {
	return func(p *PolicyStore) {
		p.config.LohpiCaAddress = addr
		p.config.LohpiCaPort = port
	}
}

func PolicyStoreWithDirectoryServerConnectionString(addr string, port int) PolicyStoreOption {
	return func(p *PolicyStore) {
		p.config.DirectoryServerAddress = addr
		p.config.DirectoryServerGPRCPort = port
	}
}

func PolicyStoreWithGitRepository(path string) PolicyStoreOption {
	return func(p *PolicyStore) {
		p.config.GitRepositoryPath = path
	}
}

func PolicyStoreWithTLS(enabled bool) PolicyStoreOption {
	return func(p *PolicyStore) {
		p.config.TLSEnabled = enabled
	}
}

type PolicyStore struct {
	psCore *policystore.PolicyStoreCore
	config *policystore.Config
}

func NewPolicyStore(opts ...PolicyStoreOption) (*PolicyStore, error) {
	const (
		defaultName                     = ""
		defaultHost                     = "127.0.1.1"
		defaultPolicyBatchSize          = 10
		defaultGossipInterval           = 5 * time.Second
		defaultHTTPPort                 = 8083
		defaultGRPCPort                 = 8084
		defaultMulticastAcceptanceLevel = 0.5
		defaultDirectRecipients         = 1
		defaultDirectoryServerAddress   = "127.0.1.1"
		defaultDirectoryServerGPRCPort  = 8081
		defaultLohpiCaAddress           = "127.0.1.1"
		defaultLohpiCaPort              = 8301
		defaultGitRepositoryPath        = "policy_store_repository"
	)

	config := &policystore.Config{
		Name:                     defaultName,
		Host:                     defaultHost,
		PolicyBatchSize:          defaultPolicyBatchSize,
		GossipInterval:           defaultGossipInterval,
		HTTPPort:                 defaultHTTPPort,
		GRPCPort:                 defaultGRPCPort,
		MulticastAcceptanceLevel: defaultMulticastAcceptanceLevel,
		DirectRecipients:         defaultDirectRecipients,
		LohpiCaAddress:           defaultLohpiCaAddress,
		DirectoryServerAddress:   defaultDirectoryServerAddress,
		LohpiCaPort:              defaultLohpiCaPort,
		GitRepositoryPath:        defaultGitRepositoryPath,
	}

	p := &PolicyStore{
		config: config,
	}

	for _, opt := range opts {
		opt(p)
	}

	psCore, err := policystore.NewPolicyStoreCore(config)
	if err != nil {
		return nil, err
	}

	p.psCore = psCore

	return p, nil
}

func (ps *PolicyStore) Start() error {
	return ps.psCore.Start()
}

func (ps *PolicyStore) IfritAddress() string {
	return ps.psCore.IfritAddress()
}

func (ps *PolicyStore) Stop() {
	ps.psCore.Stop()
}

func (ps *PolicyStore) RunPolicyBatcher() {
	ps.psCore.RunPolicyBatcher()
}

func (ps *PolicyStore) StopPolicyBatcher() {
	ps.psCore.StopPolicyBatcher()
}
