package directoryserver

import (
	lohpi_ca "github.com/arcsecc/lohpi/cauth"
	ifrit_ca "github.com/joonnna/ifrit/cauth"

	"github.com/arcsecc/lohpi/core/message"

	"crypto/ecdsa"
	"crypto/x509"
	

	"encoding/pem"
	"github.com/joonnna/ifrit"
	"context"
	//"github.com/arcsecc/lohpi/core/datasetmanager"
	//"github.com/arcsecc/lohpi/core/directoryserver"
	"github.com/arcsecc/lohpi/core/node"
	pb "github.com/arcsecc/lohpi/protobuf"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	pbtime "google.golang.org/protobuf/types/known/timestamppb"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"os"
	"testing"
	
	//"github.com/stretchr/testify/assert"
)

var (
	IfritCaPort       = 8300
	IfritCaConfigPath = "ifrit_ca_config"
	LohpiCaPort       = 8301
	LohpiCaConfigPath = "lohpi_ca_config"
	CryptoDummyDir	  = "crypto_dummy_dir" 
	DirectoryServerCoreName = "directory server test"
)

type DirectoryServerSuite struct {
	suite.Suite

	// Actual directory server
	ds *DirectoryServerCore

	nodeA *node.NodeCore
	nodeB *node.NodeCore
}

func init() {
	log.Println("Setting up CA")

	if err := setupCA(); err != nil {
		log.Fatal(err)
	}
}

// Run before each test
func (suite *DirectoryServerSuite) SetupTest() {
	dsConfig := &Config{
		Name:                           DirectoryServerCoreName,
		HTTPPort:                        8080,
		GRPCPort:                        0, //????
		SQLConnectionString:             "user=lohpi_dev_user password=password! host=localhost port=5432 dbname=directory_server_db_test sslmode=disable",
		Hostname:                        "127.0.1.1",
		IfritCryptoUnitWorkingDirectory: ".",
		IfritTCPPort:                    0,
		IfritUDPPort:                    0,
	}

	// Cert manager
	certStub, err := newCertManagerStub()
	if err != nil {
		log.Fatal(err)
	}

	gossipObsStub := &gossipObserverStub{}

	ds, err := NewDirectoryServerCore(certStub, gossipObsStub, &dsLookupServiceStub{}, &membershipManagerStub{}, &datasetCheckoutManagerStub{}, dsConfig)
	require.NoError(suite.T(), err, "Error from creating new directory server must be nil")
	require.NotEmpty(suite.T(), ds)

	// Dummy nodes, in terms of protobuf definitions
/*	nodeA, err := nodeA()
	if err != nil {
		log.Fatal(err)
	}*/

	suite.ds = ds

	// Cleanup
	suite.T().Cleanup(func() {
		if err := os.RemoveAll(IfritCaConfigPath); err != nil {
			log.Errorln(err.Error())
		}

		if err := os.RemoveAll(LohpiCaConfigPath); err != nil {
			log.Errorln(err.Error())
		}

		if err := os.RemoveAll("kake"); err != nil {
			log.Errorln(err.Error())
		}

		if err := os.RemoveAll("crypto/lohpi"); err != nil {
			log.Errorln(err.Error())
		}

		if err := os.RemoveAll(CryptoDummyDir); err != nil {
			log.Errorln(err.Error())
		}
		
		//nodeA.Shutdown()
	})
}

func (suite *DirectoryServerSuite) TestDirectoryServerStart() {
	suite.ds.Start()
}

func (suite *DirectoryServerSuite) TestDirectoryServerMessageHandler() {
	// Valid messages
	messages := []*pb.Message{
		&pb.Message{
			Type:   message.MSG_TYPE_ADD_DATASET_IDENTIFIER,
			StringValue: "test datset",
			Sender: &pb.Node{
				Name: "node a",
				IfritAddress: "127.0.0.1:8500",
				Id: []byte("0909efe23rei23i"),
				HttpsAddress: "127.0.1.1",
				Port: 8000,
				BootTime: pbtime.Now(),
			},
		},
		&pb.Message{
			Type:   message.MSG_SYNCHRONIZE_DATASET_IDENTIFIERS,
			StringSlice: []string{"a", "b", "c"},
			Sender: &pb.Node{
				Name: "node a",
				IfritAddress: "127.0.0.1:8500",
				Id: []byte("0909efe23rei23i"),
				HttpsAddress: "127.0.1.1",
				Port: 8000,
				BootTime: pbtime.Now(),
			},
		},
	}

	for _, m := range messages {
		data, err := proto.Marshal(m)
		require.NoError(suite.T(), err, "Error from marshalling messages must be nil")

		resp, err := suite.ds.messageHandler(data)
		require.NoError(suite.T(), err, "Error should be nil")

		respAsMessage := &pb.Message{}
		err = proto.Unmarshal(resp, respAsMessage)
		require.NoError(suite.T(), err, "Error from unmarshalling messages must be nil")
		require.Equal(suite.T(), respAsMessage.GetType(), message.MSG_TYPE_OK)
	}

	// Invalid messages
	m := &pb.Message{
		Type: "Unknown type",
	}

	data, err := proto.Marshal(m)
	require.NoError(suite.T(), err, "Error from marshalling messages must be nil")

	resp, err := suite.ds.messageHandler(data)
	require.EqualError(suite.T(), err, ErrUnknownMessageType.Error(), "Expected error, got nil instead")
	require.Empty(suite.T(), resp)
	
	// Try a nil node
	m = &pb.Message{
		Type: message.MSG_TYPE_ADD_DATASET_IDENTIFIER,
		Sender: nil,
	}

	data, err = proto.Marshal(m)
	require.NoError(suite.T(), err, "Error from marshalling messages must be nil")

	resp, err = suite.ds.messageHandler(data)
	require.EqualError(suite.T(), err, ErrDatasetLookupInsert.Error(), "Expected error, got nil instead")
	require.Empty(suite.T(), resp)
}

func (suite *DirectoryServerSuite) TestDirectoryServerHandshake() {
	// Create a dummy node to handshake the directory server
	node := &pb.Node{
		Name: "node a",
		IfritAddress: "127.0.0.1:8500",
		Id: []byte("0909efe23rei23i"),
		HttpsAddress: "127.0.1.1",
		Port: 8000,
		BootTime: pbtime.Now(),
	}

	resp, err := suite.ds.Handshake(context.Background(), node)
	require.NoError(suite.T(), err, "Error from creating new directory server must be nil")
	require.Equal(suite.T(), resp.GetIp(), "127.0.1.1:0")
	
	// Nil node
	resp, err = suite.ds.Handshake(context.Background(), nil)
	require.Error(suite.T(), err, "Error should be non-nil")
	require.Empty(suite.T(), resp)

	// Node without name
	node = &pb.Node{
		Name: "",
		IfritAddress: "127.0.0.1:8500",
		Id: []byte("0909efe23rei23i"),
		HttpsAddress: "127.0.1.1",
		Port: 8000,
		BootTime: pbtime.Now(),
	}

	resp, err = suite.ds.Handshake(context.Background(), node)
	require.EqualError(suite.T(), err, ErrAddNetworkNode.Error(), "Expected error, got nil instead")
	require.Empty(suite.T(), resp)
}

func TestDirectoryServerSuite(t *testing.T) {
	suite.Run(t, new(DirectoryServerSuite))
}

func (suite *DirectoryServerSuite) TestStart() {
	suite.ds.Start()
}

func (suite *DirectoryServerSuite) TestPbNode() {
	node := suite.ds.pbNode()
	require.NotEmptyf(suite.T(), node, "Expected node to be non-empty")
	require.Equal(suite.T(), node.GetName(), DirectoryServerCoreName)
}

func (suite *DirectoryServerSuite) TestShutdownDirectoryServer() {
	//suite.ifritCa.Shutdown()
	//suite.lohpiCa.Shutdown()
}

// Interface stubs
type certManagerStub struct {
	caCert     *x509.Certificate
	ownCert    *x509.Certificate
	privateKey *ecdsa.PrivateKey
}

func newCertManagerStub() (*certManagerStub, error) {
	caCertString := `
-----BEGIN CERTIFICATE-----
MIIB4TCCAUqgAwIBAgIRANV53q5j1kn97OBbJpyHdFYwDQYJKoZIhvcNAQELBQAw
ADAeFw0xMTA5MDMxMTIxMjhaFw0zMTA5MDMxMTIxMjhaMAAwgZ8wDQYJKoZIhvcN
AQEBBQADgY0AMIGJAoGBAO6KxS1gx0deBr5xAOz/vle3Dm7Abg8YSoDcgl6ZIydM
UazD9qZWe6wDi38PGcPl3WHPhx5DvUfV3XSKqPOZXhhG9mCf4wMRTjlCsC7bE341
f3dbLqGtMhm3js5uBZ17FoPqoriAsCh9hqzclRtjNJ6j43h8Cu2bF7Mu9aSrJ4Ll
AgMBAAGjWzBZMA4GA1UdDwEB/wQEAwIChDAdBgNVHSUEFjAUBggrBgEFBQcDAgYI
KwYBBQUHAwEwDwYDVR0TAQH/BAUwAwEB/zAOBgNVHQ4EBwQFAQIDBAUwBwYDVQ0l
BAAwDQYJKoZIhvcNAQELBQADgYEAr8G9j/ceLxFdQYevNg3pBRPal4Oy3Pydr+6y
u0Eo7H5pVHy/ugbuL8o4DSOeaPeiL9ZayKm5XLACkWzzX+yIIlTZdDVuNEN751LX
3CPnU7j5eirgPwRjzqsOeUn2Pv5WqJe5rtkkSJf2F1ndxO8WpRFhDnp4krqvOzuh
wjAaPPQ=
-----END CERTIFICATE-----`

	ownCertString := `
-----BEGIN CERTIFICATE-----
MIICPjCCAaegAwIBAgIQbWazu65gl8pX22U2ZIz/GjANBgkqhkiG9w0BAQsFADAA
MB4XDTExMDkwMzEyNTc0MVoXDTMxMDkwMzEyNTc0MVowXjELMAkGA1UEBhMCTk8x
LjAVBgNVBAcTDjEyNy4wLjEuMTo4MDgwMBUGA1UEBxMOMTI3LjAuMS4xOjgwODEx
HzAdBgNVBAMTFkxvaHBpIGRpcmVjdG9yeSBzZXJ2ZXIwgZswEAYHKoZIzj0CAQYF
K4EEACMDgYYABAFpSp3bopjVIEIMsJPUFT/OhkHHip60GJoeC+9N8Qmrxw+dO099
i8MmjMWPrb1BAV2Jwc0OTgYI0C2/Hlxxc/wplwCNH1Zg67bNGBkudamNoZJg+h5f
huKZAuZqHahKH4rALHqlEWbmzk7g3kzm1ncshNlEVLUvy+RkF+EhXBgx274naKNf
MF0wDgYDVR0PAQH/BAQDAgWgMB0GA1UdJQQWMBQGCCsGAQUFBwMCBggrBgEFBQcD
ATAQBgNVHSMECTAHgAUBAgMEBTAaBgNVHREEEzARggkxMjcuMC4xLjGHBH8AAQEw
DQYJKoZIhvcNAQELBQADgYEAyxnBbeEZN82qOY2xbMz0K+W98O4GRHTMyjVn6Ots
tE+tj1FMh2xLIXnXBCOQxLCko87bWjXgJcMvZ3Ayf3hn2NXoECWz4fsgVVMu3OXF
sij+nqkjYV2+v3Ht43hZ1yGLJF4Ok4TcTj280sB2kajRpC9sdFhhSC7A6uVmvwIu
uqs=
-----END CERTIFICATE-----`

	privateKeyString := `
-----BEGIN EC PRIVATE KEY-----
MIHcAgEBBEIBonmGHnQ5oSykEXFuUjqSHHYTMTfDokSthSKMZfnToiiTAsaScYiE
8djT/rewHpIk7KWh+JNi550P3/n5xXJ0Fy6gBwYFK4EEACOhgYkDgYYABAFpSp3b
opjVIEIMsJPUFT/OhkHHip60GJoeC+9N8Qmrxw+dO099i8MmjMWPrb1BAV2Jwc0O
TgYI0C2/Hlxxc/wplwCNH1Zg67bNGBkudamNoZJg+h5fhuKZAuZqHahKH4rALHql
EWbmzk7g3kzm1ncshNlEVLUvy+RkF+EhXBgx274naA==
-----END EC PRIVATE KEY-----

`

	b, _ := pem.Decode([]byte(caCertString))
	caCert, err := x509.ParseCertificate(b.Bytes)
	if err != nil {
		return nil, err
	}

	b, _ = pem.Decode([]byte(ownCertString))
	ownCert, err := x509.ParseCertificate(b.Bytes)
	if err != nil {
		return nil, err
	}

	b, _ = pem.Decode([]byte(privateKeyString))
	privateKey, err := x509.ParseECPrivateKey(b.Bytes)
	if err != nil {
		return nil, err
	}

	return &certManagerStub{
		caCert:     caCert,
		ownCert:    ownCert,
		privateKey: privateKey,
	}, nil
}

func (cm *certManagerStub) Certificate() *x509.Certificate {
	return cm.ownCert
}

func (cm *certManagerStub) CaCertificate() *x509.Certificate {
	return cm.caCert
}

func (cm *certManagerStub) PrivateKey() *ecdsa.PrivateKey {
	return cm.privateKey
}

func (cm *certManagerStub) PublicKey() *ecdsa.PublicKey {
	return &cm.privateKey.PublicKey
}

type dsLookupServiceStub struct {
}

func (ds *dsLookupServiceStub) DatasetNodeExists(datasetId string) bool {
	return false
}

func (ds *dsLookupServiceStub) RemoveDatasetLookupEntry(datasetId string) error {
	return nil
}

func (ds *dsLookupServiceStub) InsertDatasetLookupEntry(datasetId string, nodeName string) error {
	if datasetId == "" {
		return ErrDatasetLookupInsert
	}

	if nodeName == "" {
		return ErrDatasetLookupInsert
	}

	return nil
}

func (ds *dsLookupServiceStub) DatasetLookupNode(datasetId string) *pb.Node {
	return nil
}

func (ds *dsLookupServiceStub) DatasetIdentifiers() []string {
	return nil
}

type membershipManagerStub struct {
}

func (m *membershipManagerStub) NetworkNodes() map[string]*pb.Node {
	return nil
}

func (m *membershipManagerStub) NetworkNode(nodeId string) *pb.Node {
	return nil
}

func (m *membershipManagerStub) AddNetworkNode(nodeId string, node *pb.Node) error {
	if nodeId == "" {
		return ErrAddNetworkNode
	}

	if node == nil {
		return ErrAddNetworkNode
	}

	return nil
}

func (m *membershipManagerStub) NetworkNodeExists(id string) bool {
	return false
}

func (m *membershipManagerStub) RemoveNetworkNode(id string) error {
	return nil
}

type datasetCheckoutManagerStub struct {
}

func (ds *datasetCheckoutManagerStub) CheckoutDataset(datasetId string, checkout *pb.DatasetCheckout) error {
	return nil
}

func (ds *datasetCheckoutManagerStub) DatasetIsCheckedOut(datasetId string) bool {
	return false
}

func (ds *datasetCheckoutManagerStub) DatasetIsCheckedOutByClient(datasetId string, client *pb.Client) bool {
	return false
}

func (ds *datasetCheckoutManagerStub) DatasetCheckouts(datasetId string) ([]*pb.DatasetCheckout, error) {
	return nil, nil
}

type gossipObserverStub struct {

}

func (s *gossipObserverStub) InsertObservedGossip(g *pb.GossipMessage) error {
	return nil
}

func (s *gossipObserverStub) GossipIsObserved(g *pb.GossipMessage) bool {
	return false
}

func (s *gossipObserverStub) InsertAppliedGossipMessage(msg *pb.GossipMessage) error {
	return nil
}

func setupCA() error {
	// Create Ifrit run directory
	err := os.MkdirAll(IfritCaConfigPath, 0777)
	if err != nil {
		return err
	}

	ifritCa, err := ifrit_ca.NewCa(IfritCaConfigPath)
	if err != nil {
		return err
	}

	// Add initial group.
	err = ifritCa.NewGroup(5, 10)
	if err != nil {
		return err
	}

	// Create Lohpi run directory
	err = os.MkdirAll(LohpiCaConfigPath, 0777)
	if err != nil {
		return err
	}

	lohpiCa, err := lohpi_ca.NewCa(LohpiCaConfigPath)
	if err != nil {
		return err
	}

	go ifritCa.Start("127.0.1.1", "8300")
	go lohpiCa.Start(8301)

	return nil
}

// SETUP BELOW THIS LINE

func nodeA() (*node.NodeCore, error) {
	/*
	sqlConnString := "user=lohpi_dev_user password=password! host=localhost port=5432 dbname=node_db_test sslmode=disable"

	nodeAConfig := &node.Config{
		Name:                   "nodeA",
		SQLConnectionString:    sqlConnString,
		Port:                   0,
		PolicySyncInterval:		time.Second * 100,
		DatasetSyncInterval:    time.Second * 100,
		DatasetIdentifiersSyncInterval: time.Second * 100,
		CheckedOutDatasetPolicySyncInterval: time.Second * 100,
		Hostname:				"127.0.1.1",
		IfritTCPPort: 			0,
		IfritUDPPort: 			0,
	}

	// Crypto unit
	cryptoUnitConfig := &comm.CryptoUnitConfig{
		Identity: pkix.Name{
			Country: []string{"NO"},
			Locality: []string{
				fmt.Sprintf("%s:%d", "127.0.1.1", 0), 
			},
		},
		CaAddr: "127.0.1.1:8301",
		Hostnames: []string{"127.0.1.1"},
	}

	cu, err := comm.NewCu(CryptoDummyDir, cryptoUnitConfig)
	if err != nil {
		return nil, err
	}

	// Dataset manager service
/*	datasetIndexerUnitConfig := &datasetmanager.DatasetIndexerUnitConfig{
		SQLConnectionString: "user=lohpi_dev_user password=password! host=localhost port=5432 dbname=node_db_test sslmode=disable",
	}
	dsManager, err := datasetmanager.NewDatasetIndexerUnit("node1", datasetIndexerUnitConfig)
	if err != nil {
		return nil, err
	}

	// Checkout manager
	dsCheckoutManagerConfig := &datasetmanager.DatasetCheckoutServiceUnitConfig{
		SQLConnectionString: "user=lohpi_dev_user password=password! host=localhost port=5432 dbname=node_db_test sslmode=disable",
	}
	dsCheckoutManager, err := datasetmanager.NewDatasetCheckoutServiceUnit("node1", dsCheckoutManagerConfig)
	if err != nil {
		return nil, err
	}

	node, err := node.NewNodeCore(cu, &policyLoggerStub{}, dsManager, &stateSyncerStub{}, dsCheckoutManager, nodeAConfig)
	if err != nil {
		return nil, err
	}

	return node, nil*/
	return nil, nil
}

func setupNode(node *node.NodeCore) error {
/*	opts := &node.DatasetIndexingOptions {
		AllowMultipleCheckouts: true,
	}

	if err := n.nodeCore.IndexDataset(datasetId, opts); err != nil {
		return err
	}*/

	return nil
}

type policyLoggerStub struct {

}

func (p *policyLoggerStub) InsertObservedGossip(g *pb.GossipMessage) error {
	return nil
}

func (p *policyLoggerStub) GossipIsObserved(g *pb.GossipMessage) bool {
	return false
}

func (p *policyLoggerStub) InsertAppliedGossipMessage(msg *pb.GossipMessage) error {
	return nil
}

type stateSyncerStub struct {

}

func (s *stateSyncerStub) RegisterIfritClient(client *ifrit.Client)  {
}

func (s *stateSyncerStub) SynchronizeDatasetIdentifiers(ctx context.Context, identifiers []string, remoteAddr string) error {
	return nil
}

func (s *stateSyncerStub) SynchronizeDatasets(ctx context.Context, datasets map[string]*pb.Dataset, targetAddr string) (map[string]*pb.Dataset, error) {
	return nil, nil
}
