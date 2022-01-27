package comm

import (
	"bytes"
	"crypto"
	"crypto/ecdsa"
	"crypto/rsa"
	"crypto/elliptic"
//	"crypto/tls"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io/ioutil"
	"math/big"
	"net/http"
	"path/filepath"
	"time"
)

var (
	errNoIp               = errors.New("No ip present in received identity")
	errNoAddrs            = errors.New("Not enough addresses present in identity")
	errNoCa               = errors.New("No Lohpi ca address supplied")
	errNoCryptoUnitConfig = errors.New("No crypto unit config present")
	errNoPath             = errors.New("Crypto unit path is not set")
	errNoHostnames        = errors.New("Hostnames is not set")
)

type CryptoUnit struct {
	priv crypto.PrivateKey
	pub crypto.PublicKey
	caAddr string

	self *x509.Certificate
	ca   *x509.Certificate
}

type certResponse struct {
	OwnCert []byte
	CaCert  []byte
}

type certSet struct {
	ownCert *x509.Certificate
	caCert  *x509.Certificate
}

type CryptoUnitConfig struct {
	Identity  pkix.Name
	CaAddr    string
	Hostnames []string
}

// Returns a new CryptoUnit. If 'useCa' is true, use signed certificates from a homemade CA.
func NewCu(config *CryptoUnitConfig, useCa bool,) (*CryptoUnit, error) {
	if err := validateConfiguration(config); err != nil {
		return nil, err
	}

	var certs *certSet
	var cu *CryptoUnit

	// Use homemade CA
	if useCa {
		priv, err := genECDSAKeys()
		if err != nil {
			return nil, err
		}
		
		addr := fmt.Sprintf("http://%s/certificateRequest", config.CaAddr)
		certs, err = sendCertRequest(priv, addr, config.Identity, config.Hostnames)
		if err != nil {
			return nil, err
		}
		
		cu = &CryptoUnit{
			ca: certs.caCert,
			self: certs.ownCert,
			priv: priv,
		}
	} else {
		priv, err := genRSAKeys()
		if err != nil {
			return nil, err
		}

		certs, err = selfSignedCert(priv, config.Identity)
		if err != nil {
			return nil, err
		}

		cu = &CryptoUnit{
			ca: certs.caCert,
			self: certs.ownCert,
			priv: priv,
		}
	}

	return cu, nil
}

func validateConfiguration(config *CryptoUnitConfig) error {
	if config == nil {
		return errNoCryptoUnitConfig
	}
	
	if config.Hostnames == nil {
		return errNoHostnames
	}

	if len(config.Hostnames) == 0 {
		return errNoHostnames
	}

	if config.CaAddr == "" {
		return errNoCa
	}

	return nil
}

func LoadCu(path string) (*CryptoUnit, error) {
	// Load own cert
	certPath := filepath.Join(path, "owncert.pem")
	fp, err := ioutil.ReadFile(certPath)
	if err != nil {
		return nil, err
	}

	certBlock, _ := pem.Decode(fp)
	ownCert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		return nil, err
	}

	// Load CA's cert
	certPath = filepath.Join(path, "cacert.pem")
	fp, err = ioutil.ReadFile(certPath)
	if err != nil {
		return nil, err
	}

	certBlock, _ = pem.Decode(fp)
	caCert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		return nil, err
	}

	// Load private key
	keyPath := filepath.Join(path, "key.pem")
	fp, err = ioutil.ReadFile(keyPath)
	if err != nil {
		return nil, err
	}

	keyBlock, _ := pem.Decode(fp)
	key, err := x509.ParseECPrivateKey(keyBlock.Bytes)
	if err != nil {
		return nil, err
	}

	return &CryptoUnit{
		ca:               caCert,
		self:             ownCert,
		priv:             key,
	}, nil
}

func (cu *CryptoUnit) Certificate() *x509.Certificate {
	return cu.self
}

func (cu *CryptoUnit) CaCertificate() *x509.Certificate {
	return cu.ca
}

func (cu *CryptoUnit) PrivateKey() crypto.PrivateKey {
	return cu.priv
}

func (cu *CryptoUnit) PublicKey() crypto.PublicKey {
	return cu.pub
}

func sendCertRequest(privKey *ecdsa.PrivateKey, caAddr string, pk pkix.Name, hostnames []string) (*certSet, error) {
	var certs certResponse
	set := &certSet{}

	template := x509.CertificateRequest{
		SignatureAlgorithm: x509.ECDSAWithSHA256,
		Subject: pk,
		DNSNames: hostnames,
	}

	certReqBytes, err := x509.CreateCertificateRequest(rand.Reader, &template, privKey)
	if err != nil {
		return nil, err
	}

	resp, err := http.Post(caAddr, "text", bytes.NewBuffer(certReqBytes))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	err = json.NewDecoder(resp.Body).Decode(&certs)
	if err != nil {
		return nil, err
	}

	set.ownCert, err = x509.ParseCertificate(certs.OwnCert)
	if err != nil {
		return nil, err
	}

	set.caCert, err = x509.ParseCertificate(certs.CaCert)
	if err != nil {
		return nil, err
	}

	return set, nil
}

func selfSignedCert(priv *rsa.PrivateKey, pk pkix.Name) (*certSet, error) {
	ext := pkix.Extension{
		Id: []int{2, 5, 13, 37},
		Critical: false,
	}

	serial, err := genSerialNumber()
	if err != nil {
		return nil, err
	}

	// TODO generate ids and serial numbers differently
	newCert := &x509.Certificate{
		SerialNumber:          serial,
		SubjectKeyId:          genId(),
		Subject:               pk,
		BasicConstraintsValid: true,
		NotBefore:             time.Now().AddDate(-10, 0, 0),
		NotAfter:              time.Now().AddDate(10, 0, 0),
		ExtraExtensions:       []pkix.Extension{ext},
		//PublicKey:             priv.PublicKey,
		IsCA:                  true,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign,
	}

	signedCert, err := x509.CreateCertificate(rand.Reader, newCert, newCert, priv.Public(), priv)
	if err != nil {
		return nil, err
	}

	parsed, err := x509.ParseCertificate(signedCert)
	if err != nil {
		return nil, err
	}

	return &certSet{ownCert: parsed}, nil
}

func genId() []byte {
	nonce := make([]byte, 32)
	rand.Read(nonce)
	return nonce

}

func genSerialNumber() (*big.Int, error) {
	sLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	s, err := rand.Int(rand.Reader, sLimit)
	if err != nil {
		return nil, err
	}

	return s, nil
}

func genECDSAKeys() (*ecdsa.PrivateKey, error) {
	priv,  err := ecdsa.GenerateKey(elliptic.P521(), rand.Reader)
	if err != nil {
		return nil, err
	}

	return priv, nil
}

func genRSAKeys() (*rsa.PrivateKey, error) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, err
	}

	return priv, nil
}