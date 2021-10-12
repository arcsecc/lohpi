package comm

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
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
	priv   *ecdsa.PrivateKey
	caAddr string

	self *x509.Certificate
	ca   *x509.Certificate

	workingDirectory string
	selfCertFilePath string
	caCertFilePath   string
	keyFilePath      string
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

func NewCu(path string, config *CryptoUnitConfig) (*CryptoUnit, error) {
	if path == "" {
		return nil, errNoPath
	}

	if err := validateConfiguration(config); err != nil {
		return nil, err
	}

	if err := os.MkdirAll(path, 0700); err != nil {
		panic(err)
		return nil, err
	}

	var certs *certSet

	priv, err := genKeys()
	if err != nil {
		return nil, err
	}

	addr := fmt.Sprintf("http://%s/certificateRequest", config.CaAddr)
	certs, err = sendCertRequest(priv, addr, config.Identity, config.Hostnames)
	if err != nil {
		return nil, err
	}

	return &CryptoUnit{
		ca:               certs.caCert,
		self:             certs.ownCert,
		priv:             priv,
		keyFilePath:      "key.pem",
		selfCertFilePath: "owncert.pem",
		caCertFilePath:   "cacert.pem",
		workingDirectory: path,
	}, nil
}

func validateConfiguration(config *CryptoUnitConfig) error {
	if config == nil {
		return errNoCryptoUnitConfig
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
		keyFilePath:      "key.pem",
		selfCertFilePath: "owncert.pem",
		caCertFilePath:   "cacert.pem",
		workingDirectory: path,
		//caAddr?
	}, nil
}

func (cu *CryptoUnit) SaveState() error {
	// Save private key
	p := filepath.Join(cu.workingDirectory, cu.keyFilePath)
	f, err := os.Create(p)
	if err != nil {
		log.Errorln(err.Error())
	}

	b, err := x509.MarshalECPrivateKey(cu.priv)
	if err != nil {
		log.Errorln(err.Error())
	}

	block := &pem.Block{
		Type:  "EC PRIVATE KEY",
		Bytes: b,
	}

	if err := pem.Encode(f, block); err != nil {
		log.Errorln(err.Error())
	}

	// Save CA's X.509 certificate
	p = filepath.Join(cu.workingDirectory, cu.caCertFilePath)
	f, err = os.Create(p)
	if err != nil {
		log.Errorln(err.Error())
	}

	// Save CA cert
	b = cu.ca.Raw
	block = &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: b,
	}

	if err := pem.Encode(f, block); err != nil {
		log.Errorln(err.Error())
	}

	if err := f.Close(); err != nil {
		log.Errorln(err.Error())
	}

	// Save my own X.509 certificate
	p = filepath.Join(cu.workingDirectory, cu.selfCertFilePath)
	f, err = os.Create(p)
	if err != nil {
		log.Errorln(err.Error())
	}

	b = cu.self.Raw
	block = &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: b,
	}

	if err := pem.Encode(f, block); err != nil {
		log.Errorln(err.Error())
	}

	return f.Close()
}

func (cu *CryptoUnit) Certificate() *x509.Certificate {
	return cu.self
}

func (cu *CryptoUnit) CaCertificate() *x509.Certificate {
	return cu.ca
}

func (cu *CryptoUnit) PrivateKey() *ecdsa.PrivateKey {
	return cu.priv
}

func (cu *CryptoUnit) PublicKey() *ecdsa.PublicKey {
	return &cu.priv.PublicKey
}

func sendCertRequest(privKey *ecdsa.PrivateKey, caAddr string, pk pkix.Name, hostnames []string) (*certSet, error) {
	var certs certResponse
	set := &certSet{}

	if hostnames == nil {
		return nil, errNoHostnames
	}

	template := x509.CertificateRequest{
		SignatureAlgorithm: x509.ECDSAWithSHA256,
		Subject:            pk,
		DNSNames:           hostnames,
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

func selfSignedCert(priv *ecdsa.PrivateKey, pk pkix.Name) (*certSet, error) {
	ext := pkix.Extension{
		Id:       []int{2, 5, 13, 37},
		Critical: false,
	}

	serial, err := genSerialNumber()
	if err != nil {
		return nil, err
	}

	serviceAddr := strings.Split(pk.Locality[0], ":")
	if len(serviceAddr) <= 0 {
		return nil, errNoIp
	}

	ip := net.ParseIP(serviceAddr[0])
	if ip == nil {
		return nil, errNoIp
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
		PublicKey:             priv.PublicKey,
		IPAddresses:           []net.IP{ip},
		IsCA:                  true,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth,
			x509.ExtKeyUsageServerAuth},
		KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment |
			x509.KeyUsageCertSign,
	}

	signedCert, err := x509.CreateCertificate(rand.Reader, newCert,
		newCert, priv.Public(), priv)
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

func genKeys() (*ecdsa.PrivateKey, error) {
	privKey, err := ecdsa.GenerateKey(elliptic.P521(), rand.Reader)
	if err != nil {
		return nil, err
	}

	return privKey, nil
}
