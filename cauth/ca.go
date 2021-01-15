package cauth

import (
	"bytes"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gorilla/mux"
	log "github.com/inconshreveable/log15"
)

var (
	errNoPort         = errors.New("No Port number specified in config.")
	errNoCertFilePath = errors.New("Tried to save public group certificates with no filepath set in config")
	errNoKeyFilePath  = errors.New("Tried to save private key with no filepath set in config")
	errNoConfig       = errors.New("Configuration is null.")
)

type Ca struct {
	privKey *rsa.PrivateKey
	pubKey  crypto.PublicKey
	caCert  *x509.Certificate

	listener   net.Listener
	httpServer *http.Server

	dirPath      string
	keyFilePath  string
	certFilePath string
}

func NewCa(path string) (*Ca, error) {
	privKey, err := genKeys()
	if err != nil {
		return nil, err
	}

	caCert, err := generateCaCert(privKey.Public(), privKey)
	if err != nil {
		return nil, err
	}

	c := &Ca{
		privKey:      privKey,
		pubKey:       privKey.Public(),
		caCert:       caCert,
		dirPath:      path,
		keyFilePath:  "key.pem",
		certFilePath: "cert.pem",
	}

	return c, nil
}

// LoadCa initializes a CA from a file path
func LoadCa(path string) (*Ca, error) {
	keyPath := filepath.Join(path, "key.pem")

	fp, err := ioutil.ReadFile(keyPath)
	if err != nil {
		return nil, err
	}

	// Load private key
	keyBlock, _ := pem.Decode(fp)
	key, err := x509.ParsePKCS1PrivateKey(keyBlock.Bytes)
	if err != nil {
		return nil, err
	}

	// Load certificate
	certPath := filepath.Join(path, "cert.pem")
	fp, err = ioutil.ReadFile(certPath)
	if err != nil {
		return nil, err
	}

	certBlock, _ := pem.Decode(fp)
	cert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		return nil, err
	}

	c := &Ca{
		privKey:      key,
		pubKey:       key.Public(),
		caCert:       cert,
		dirPath:      path,
		keyFilePath:  "key.pem",
		certFilePath: "cert.pem",
	}

	return c, nil
}

// SavePrivateKey writes the CA private key to the given io object.
func (c *Ca) SavePrivateKey() error {
	if c.keyFilePath == "" {
		return errNoKeyFilePath
	}

	p := filepath.Join(c.dirPath, c.keyFilePath)

	f, err := os.Create(p)
	if err != nil {
		log.Error(err.Error())
		return err
	}

	b := x509.MarshalPKCS1PrivateKey(c.privKey)

	block := &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: b,
	}

	return pem.Encode(f, block)
}

// SaveCertificate Public key / certifiace to the given io object.
func (c *Ca) SaveCertificate() error {
	if c.certFilePath == "" {
		return errNoCertFilePath
	}

	p := filepath.Join(c.dirPath, c.certFilePath)
	f, err := os.Create(p)
	if err != nil {
		log.Error(err.Error())
		return err
	}

	log.Info("Save Lohpi CA certificate.", "file", c.certFilePath)

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

func (c *Ca) Start(host string, port int) error {
	l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		return err
	}
	c.listener = l

	log.Info("Started certificate authority", "addr", c.listener.Addr().String())

	return c.httpHandler()
}

func (c *Ca) Shutdown() {
	log.Info("Shuting down certificate authority")
	c.listener.Close()
}

func (c *Ca) httpHandler() error {
	r := mux.NewRouter()
	r.HandleFunc("/certificateRequest", c.certificateSigning).Methods("POST")
	r.HandleFunc("/clientCertificateRequest", c.clientCertificateSigning).Methods("POST")

	c.httpServer = &http.Server{
		Handler:     r,
		ReadTimeout: time.Second * 10,
	}

	err := c.httpServer.Serve(c.listener)
	if err != nil {
		log.Error(err.Error())
		return err
	}

	return nil
}

func (c *Ca) clientCertificateSigning(w http.ResponseWriter, r *http.Request) {
	var body bytes.Buffer
	io.Copy(&body, r.Body)
	r.Body.Close()

	reqCert, err := x509.ParseCertificateRequest(body.Bytes())
	if err != nil {
		http.Error(w, http.StatusText(http.StatusInternalServerError)+":"+err.Error(), http.StatusInternalServerError)
		log.Error(err.Error())
		return
	}

	serialNumber, err := genSerialNumber()
	if err != nil {
		log.Error(err.Error())
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	newCert := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject:      reqCert.Subject,
		NotBefore:    time.Now().AddDate(-10, 0, 0),
		NotAfter:     time.Now().AddDate(10, 0, 0),
		//ExtraExtensions: []pkix.Extension{ext},
		PublicKey:   reqCert.PublicKey,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
	}

	signedCert, err := x509.CreateCertificate(rand.Reader, newCert, c.caCert, reqCert.PublicKey, c.privKey)
	if err != nil {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
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
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		log.Error(err.Error())
		return
	}
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

	/*	ext := pkix.Extension{
		Id:       []int{2, 5, 13, 37},
		Critical: false,
	}*/

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
		Subject:      reqCert.Subject,
		NotBefore:    time.Now().AddDate(-10, 0, 0),
		NotAfter:     time.Now().AddDate(10, 0, 0),
		//ExtraExtensions: []pkix.Extension{ext},
		PublicKey:   reqCert.PublicKey,
		IPAddresses: []net.IP{ipAddr.IP},
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
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
