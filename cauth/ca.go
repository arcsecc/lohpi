package cauth

import (
	"fmt"
	"bytes"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"github.com/tomcat-bit/lohpi/pkg/netutil"
	"io"
	"math/big"
	"net"
	"net/http"
	"strings"
	"time"
	"os"

	log "github.com/inconshreveable/log15"
)

var (
	errNoPort         = errors.New("No Port number specified in config.")
	errNoCertFilePath = errors.New("Tried to save public group certificates with no filepath set in config")
	errNoKeyFilePath  = errors.New("Tried to save private key with no filepath set in config")
	errNoConfig 	  = errors.New("Configuration is null.")
)

type Ca struct {
	privKey *rsa.PrivateKey
	pubKey  crypto.PublicKey
	caCert  *x509.Certificate

	listener   net.Listener
	httpServer *http.Server

	config *CaConfig
}

type CaConfig struct {
	KeyFilePath  string
	CertFilePath string
	Port int		
}

func NewCa(c *CaConfig) (*Ca, error) {
	if c == nil {
		return nil, errNoConfig
	}

	privKey, err := genKeys()
	if err != nil {
		return nil, err
	}

	listener, err := netutil.ListenOnPort(c.Port)
	if err != nil {
		return nil, err
	}

	caCert, err := generateCaCert(privKey.Public(), privKey)
	if err != nil {
		return nil, err
	}

	ca := &Ca{
		privKey:      privKey,
		pubKey:       privKey.Public(),
		caCert:       caCert,
		listener:     listener,
		config:		  c, 
	}

	return ca, nil
}

// SavePrivateKey writes the CA private key to the given io object.
func (c *Ca) SavePrivateKey() error {
	if c.config.KeyFilePath == "" {
		return errNoKeyFilePath
	}

	f, err := os.Create(c.config.KeyFilePath)
	if err != nil {
		log.Error(err.Error())
		return err
	}

	log.Info("Save Lohpi CA private key.", "file", c.config.KeyFilePath)

	b := x509.MarshalPKCS1PrivateKey(c.privKey)

	block := &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: b,
	}

	return pem.Encode(f, block)
}

// SaveCertificate Public key / certifiace to the given io object.
func (c *Ca) SaveCertificate() error {
	if c.config.CertFilePath == "" {
		return errNoCertFilePath
	}

	f, err := os.Create(c.config.CertFilePath)
	if err != nil {
		log.Error(err.Error())
		return err
	}

	log.Info("Save Lohpi CA certificate.", "file", c.config.CertFilePath)

	b := c.caCert.Raw
	block := &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: b,
	}
	err = pem.Encode(f, block)
	if err != nil {
		return err
	}

	return nil
}

func (c *Ca) Start() error {
	log.Info("Started Lohpi certificate authority", "addr", c.listener.Addr().String())
	return c.httpHandler()
}

func (c *Ca) httpHandler() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/certificateRequest", c.certificateSigning)
	mux.HandleFunc("/clientCertificateRequest", c.clientCertificateSigning)

	c.httpServer = &http.Server{
		Handler: mux,
	}

	err := c.httpServer.Serve(c.listener)
	if err != nil {
		log.Error(err.Error())
		return err
	}

	return nil
}

func (c *Ca) clientCertificateSigning(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	fmt.Fprintf(w, "hello from Lohpi ca")
}

func (c *Ca) certificateSigning(w http.ResponseWriter, r *http.Request) {
	var body bytes.Buffer
	io.Copy(&body, r.Body)
	r.Body.Close()

	reqCert, err := x509.ParseCertificateRequest(body.Bytes())
	if err != nil {
		log.Error(err.Error())
		return
	}

	log.Info("Got a Lohpi certificate request", "addr", reqCert.Subject.Locality)

	ext := pkix.Extension{
		Id:       []int{2, 5, 13, 37},
		Critical: false,
	}

	serialNumber, err := genSerialNumber()
	if err != nil {
		log.Error(err.Error())
		return
	}

	ipAddr, err := net.ResolveIPAddr("ip4", strings.Split(reqCert.Subject.Locality[0], ":")[0])
	if err != nil {
		log.Error(err.Error())
		return
	}

	newCert := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject:         reqCert.Subject,
		NotBefore:       time.Now().AddDate(-10, 0, 0),
		NotAfter:        time.Now().AddDate(10, 0, 0),
		ExtraExtensions: []pkix.Extension{ext},
		PublicKey:       reqCert.PublicKey,
		IPAddresses:     []net.IP{ipAddr.IP},
		ExtKeyUsage:     []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:        x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
	}

	signedCert, err := x509.CreateCertificate(rand.Reader, newCert, c.caCert, reqCert.PublicKey, c.privKey)
	if err != nil {
		log.Error(err.Error())
		return
	}

	respStruct := struct {
		OwnCert []byte
		CaCert  []byte
	}{
		OwnCert: signedCert,
		CaCert:  c.caCert.Raw,
	}

	b := new(bytes.Buffer)
	json.NewEncoder(b).Encode(respStruct)

	_, err = w.Write(b.Bytes())
	if err != nil {
		log.Error(err.Error())
		return
	}
}

func generateCaCert(pubKey crypto.PublicKey, privKey *rsa.PrivateKey) (*x509.Certificate, error) {
	serialNumber, err := genSerialNumber()
	if err != nil {
		log.Error(err.Error())
		return nil, err
	}

	ext := pkix.Extension{
		Id:       []int{2, 5, 13, 37},
		Critical: false,
	}

	caCert := &x509.Certificate{
		SerialNumber:          serialNumber,
		SubjectKeyId:          []byte{1, 2, 3, 4, 5},
		BasicConstraintsValid: true,
		IsCA:                  true,
		NotBefore:             time.Now().AddDate(-10, 0, 0),
		NotAfter:              time.Now().AddDate(10, 0, 0),
		PublicKey:             pubKey,
		ExtraExtensions:       []pkix.Extension{ext},
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
	}

	gCert, err := x509.CreateCertificate(rand.Reader, caCert, caCert, pubKey, privKey)
	if err != nil {
		log.Error(err.Error())
		return nil, err
	}

	cert, err := x509.ParseCertificate(gCert)
	if err != nil {
		log.Error(err.Error())
		return nil, err
	}

	log.Info("Created new Lohpi CA certificate!")
	return cert, nil
}

func genKeys() (*rsa.PrivateKey, error) {
	priv, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		return nil, err
	}

	return priv, nil
}

func genSerialNumber() (*big.Int, error) {
	sLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	s, err := rand.Int(rand.Reader, sLimit)
	if err != nil {
		return nil, err
	}

	return s, nil
}