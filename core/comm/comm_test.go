package comm_test

import (
	"crypto/ecdsa"
	"crypto/x509"
	"encoding/pem"
	"github.com/arcsecc/lohpi/core/comm"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"testing"
)

type CommTestSuite struct {
	suite.Suite
	caCert     *x509.Certificate
	ownCert    *x509.Certificate
	privateKey *ecdsa.PrivateKey
}

func TestCommTestSuite(t *testing.T) {
	suite.Run(t, new(CommTestSuite))
}

func (suite *CommTestSuite) SetupTest() {
	caCert, ownCert, privateKey, err := newCryptoResources()
	if err != nil {
		log.Fatal(err.Error())
	}

	suite.caCert = caCert
	suite.ownCert = ownCert
	suite.privateKey = privateKey
}

func (suite *CommTestSuite) TestServerConfig() {
	config, err := comm.ServerConfig(suite.ownCert, suite.caCert, suite.privateKey)
	require.NoError(suite.T(), err)
	require.NotEmpty(suite.T(), config)

	config, err = comm.ServerConfig(nil, nil, nil)
	require.Error(suite.T(), err)
	require.Empty(suite.T(), config)
}

func newCryptoResources() (*x509.Certificate, *x509.Certificate, *ecdsa.PrivateKey, error) {
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
		return nil, nil, nil, err
	}

	b, _ = pem.Decode([]byte(ownCertString))
	ownCert, err := x509.ParseCertificate(b.Bytes)
	if err != nil {
		return nil, nil, nil, err
	}

	b, _ = pem.Decode([]byte(privateKeyString))
	key, err := x509.ParseECPrivateKey(b.Bytes)
	if err != nil {
		return nil, nil, nil, err
	}

	return caCert, ownCert, key, nil
}
