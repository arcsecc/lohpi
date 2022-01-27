package comm_test

import (
	"crypto/x509/pkix"
	"fmt"
	"github.com/arcsecc/lohpi/cauth"
	"github.com/arcsecc/lohpi/core/comm"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"os"
	"testing"
)

var (
	CaPort    = 8301
	CryptoDir = "test_crypto_dir"
)

type CryptoUnitTestSuite struct {
	suite.Suite
	c      *comm.CryptoUnit
	config *comm.CryptoUnitConfig
}

func TestCryptoUnitTestSuite(t *testing.T) {
	suite.Run(t, new(CryptoUnitTestSuite))
}

// Run before each test
func (suite *CryptoUnitTestSuite) SetupTest() {
	// Create a CA we can request certificates from
	ca, err := cauth.NewCa(".")
	require.NoError(suite.T(), err, "Failed to create ca")
	require.NotEmpty(suite.T(), ca)
	go ca.Start(CaPort)

	config := &comm.CryptoUnitConfig{
		Identity: pkix.Name{
			Country:    []string{"NO"},
			CommonName: "common name",
			Locality: []string{
				fmt.Sprintf("%s:%d", "127.0.1.1", 8008),
				fmt.Sprintf("%s:%d", "127.0.1.1", 8001),
			},
		},
		CaAddr:    "127.0.1.1:8301",
		Hostnames: []string{"127.0.1.1"},
	}

	cu, err := comm.NewCu(CryptoDir, config)
	require.NoError(suite.T(), err, "Expected no error")
	require.NotEmpty(suite.T(), cu)

	// Save state
	err = cu.SaveState()
	require.NoError(suite.T(), err, "Required an error to be raised")

	suite.c = cu

	// Cleanup
	suite.T().Cleanup(func() {
		if err := os.RemoveAll(CryptoDir); err != nil {
			log.Fatal(err)
		}
	})
}

func (suite *CryptoUnitTestSuite) TestLoadCu() {
	cu, err := comm.LoadCu(CryptoDir)
	require.NoError(suite.T(), err, "Failed to load crypto unit")
	require.NotEmpty(suite.T(), cu)
}

func (suite *CryptoUnitTestSuite) TestInvalidCryptoUnitConfigurations() {
	// No configuration but valid directory
	cu, err := comm.NewCu(CryptoDir, nil)
	require.Error(suite.T(), err, "Required an error to be raised")
	require.Empty(suite.T(), cu)

	// No configuration or valid directory
	cu, err = comm.NewCu("", nil)
	require.Error(suite.T(), err, "Required an error to be raised")
	require.Empty(suite.T(), cu)

	// Invalid configuration: without hostname or CA address
	config := &comm.CryptoUnitConfig{
		Identity: pkix.Name{
			Country:    []string{"NO"},
			CommonName: "common name",
			Locality: []string{
				fmt.Sprintf("%s:%d", "127.0.1.1", 8008),
				fmt.Sprintf("%s:%d", "127.0.1.1", 8001),
			},
		},
	}

	cu, err = comm.NewCu(CryptoDir, config)
	require.Error(suite.T(), err, "Required an error to be raised")
	require.Empty(suite.T(), cu)

	// Invalid configuration: without hostname but with CA address
	config = &comm.CryptoUnitConfig{
		Identity: pkix.Name{
			Country:    []string{"NO"},
			CommonName: "common name",
			Locality: []string{
				fmt.Sprintf("%s:%d", "127.0.1.1", 8008),
				fmt.Sprintf("%s:%d", "127.0.1.1", 8001),
			},
		},
		Hostnames: []string{"127.0.1.1"},
	}

	cu, err = comm.NewCu(CryptoDir, config)
	require.Error(suite.T(), err, "Required an error to be raised")
	require.Empty(suite.T(), cu)
}

func (suite *CryptoUnitTestSuite) TestCertificate() {
	cert := suite.c.Certificate()
	require.NotEmpty(suite.T(), cert)
}

func (suite *CryptoUnitTestSuite) TestCaCertificate() {
	cert := suite.c.CaCertificate()
	require.NotEmpty(suite.T(), cert)
}

func (suite *CryptoUnitTestSuite) TestPrivateKey() {
	cert := suite.c.PrivateKey()
	require.NotEmpty(suite.T(), cert)
}
func (suite *CryptoUnitTestSuite) PublicKey() {
	cert := suite.c.PublicKey()
	require.NotEmpty(suite.T(), cert)
}
