package directoryserver_test

import (
	lohpi_ca "github.com/arcsecc/lohpi/cauth"
	ifrit_ca "github.com/joonnna/ifrit/cauth"

	"crypto/ecdsa"
	"crypto/x509"
	"encoding/pem"
	"github.com/arcsecc/lohpi"
	"github.com/arcsecc/lohpi/core/directoryserver"
	"github.com/arcsecc/lohpi/core/node"
	pb "github.com/arcsecc/lohpi/protobuf"
	log "github.com/sirupsen/logrus"
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
)

type DirectoryServerSuite struct {
	suite.Suite

	// Actual directory server
	ds *directoryserver.DirectoryServerCore

	nodes []*node.NodeCore
}

func init() {
	log.Println("Setting up CA")

	if err := setupCA(); err != nil {
		log.Fatal(err)
	}
}

// Run before each test
func (suite *DirectoryServerSuite) SetupTest() {
	dsConfig := &directoryserver.Config{
		Name:                            "directory server test",
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

	ds, err := directoryserver.NewDirectoryServerCore(certStub, &dsLookupServiceStub{}, &membershipManagerStub{}, &datasetCheckoutManagerStub{}, dsConfig)
	require.NoError(suite.T(), err, "Error must be nil")
	require.NotEmpty(suite.T(), ds)

	// Dummy nodes, in terms of protobuf definitions
	/*nodes, err := nodes()
	if err != nil {
		log.Fatal(err)
	}*/

	suite.ds = ds

	// Cleanup
	suite.T().Cleanup(func() {
		/*errc*log.Printf("stopping stuff\n")
		suite.ds.Stop()*/

		if err := os.RemoveAll(IfritCaConfigPath); err != nil {
			log.Errorln(err.Error())
		}

		if err := os.RemoveAll(LohpiCaConfigPath); err != nil {
			log.Errorln(err.Error())
		}
	})
}

// Returns a set of dummy nodes used to test the directory server. Assume the
// bootstrap parameters to be correct.
/*func nodes() ([]*node.NodeCore, error) {
	nodes := make([]*node.NodeCore, 0)

	nodeConfigs := []*node.Config{
		&node.Config{
			HostName:                       "127.0.1.1",
			HTTPPort:                       8020,
			PolicyStoreAddress:             "127.0.1.1:8081",
			LohpiCaAddress:                 "127.0.1.1:8301",
			Name:                           "First node",
			AllowMultipleCheckouts:         true,
			PolicyObserverWorkingDirectory: ".",
		},
		&node.Config{
			HostName:                       "127.0.1.1",
			HTTPPort:                       8022,
			PolicyStoreAddress:             "127.0.1.1:8081",
			LohpiCaAddress:                 "127.0.1.1:8301",
			Name:                           "Second node",
			AllowMultipleCheckouts:         true,
			PolicyObserverWorkingDirectory: ".",
		},
	}

	for _, c := range nodeConfigs {
		n, err := node.NewNodeCore(c)
		if err != nil {
			return nil, err
		}

		nodes = append(nodes, n)
	}

	return nodes, nil
}*/

func (suite *DirectoryServerSuite) TestDirectoryServerStart() {
	suite.ds.Start()
}

func (suite *DirectoryServerSuite) TestDirectoryServerMessageHandler() {
	// Send message to the directory server as a Lohpi node
	config := &node.Config{
		Hostname: "127.0.1.1",
		Port:     8020,
		Name:     "First node",
	}

	node, err := lohpi.NewNode(config, true)
	require.NoErrorf(suite.T(), err, "Could not create a new dummy node")
	_ = node

}

func (suite *DirectoryServerSuite) TestDirectoryServerHandshake() {
	/*	// Add nil node
		resp, err := suite.ds.Handshake(context.Background(), nil)
		require.Empty(suite.T(), resp)
		require.Error(suite.T(), err)

		// Add random nodes and check the response
		for _, n := range suite.nodes {
			resp, err := suite.ds.Handshake(context.Background(), n.PbNode())
			require.NoError(suite.T(), err)
			require.NotEqual(suite.T(), resp.Ip, nil)
			require.NotEqual(suite.T(), resp.Id, nil)
		}

		// Add same node to trigger duplicate error
		resp, err = suite.ds.Handshake(context.Background(), suite.nodes[0].PbNode())
		require.Empty(suite.T(), resp)
		require.Error(suite.T(), err)*/
}

func TestDirectoryServerSuite(t *testing.T) {
	suite.Run(t, new(DirectoryServerSuite))
}

func (suite *DirectoryServerSuite) TestStart() {
	//suite.ds.Start()
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

func (ds *datasetCheckoutManagerStub) DatasetIsCheckedOut(datasetId string, client *pb.Client) bool {
	return false
}

func (ds *datasetCheckoutManagerStub) DatasetCheckouts(datasetId string) ([]*pb.DatasetCheckout, error) {
	return nil, nil
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
