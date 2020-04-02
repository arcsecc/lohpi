package cauth

import (
	"crypto"
	"crypto/rsa"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"net"
	"net/http"
	"errors"
	"bytes"
	"io"
	"encoding/json"
	"time"
	"strings"
	"math/big"
	"encoding/pem"	
	"firestore/netutil"
	"os"

	"github.com/spf13/viper"
	
	log "github.com/inconshreveable/log15"
)

var	(
	errNoAddr         = errors.New("No network address provided in cert request.")
	errNoPort         = errors.New("No port number specified in config.")
	errNoCertFilepath = errors.New("Tried to save public group certificates with no filepath set in config")
	errNoKeyFilepath  = errors.New("Tried to save private key with no filepath set in config")
)

type Ca struct {
	privKey *rsa.PrivateKey
	pubKey  crypto.PublicKey
	caCert *x509.Certificate

	keyFilePath  string
	certFilePath string

	listener   net.Listener
	httpServer *http.Server
}

func NewCa() (*Ca, error) {
	err := readConfig()
	if err != nil {
		return nil, err
	}

	privKey, err := genKeys()
	if err != nil {
		return nil, err
	}

	if exists := viper.IsSet("port"); !exists {
		return nil, errNoPort
	}

	listener, err := netutil.ListenOnPort(viper.GetInt("port"))
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
		caCert: 	  caCert,
		listener:     listener,
		keyFilePath:  viper.GetString("key_filepath"),
		certFilePath: viper.GetString("certificate_filepath"),
	}

	return c, nil
}

// SavePrivateKey writes the CA private key to the given io object.
func (c *Ca) SavePrivateKey() error {
	if c.keyFilePath == "" {
		return errNoKeyFilepath
	}

	f, err := os.Create(c.keyFilePath)
	if err != nil {
		log.Error(err.Error())
		return err
	}

	log.Info("Save Lohpi CA private key.", "file", c.keyFilePath)

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
		return errNoCertFilepath
	}

	f, err := os.Create(c.certFilePath)
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

func (c *Ca) Start() error {
	log.Info("Started Lohpi certificate authority", "addr", c.listener.Addr().String())
	return c.httpHandler()
}

func (c *Ca) httpHandler() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/certificateRequest", c.certificateSigning)

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
		SerialNumber:    serialNumber,
//		SubjectKeyId:    id,
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
		OwnCert    []byte
		CaCert     []byte
	}{
		OwnCert:    signedCert,
		CaCert:     c.caCert.Raw,
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
		SerialNumber:          	serialNumber,
		SubjectKeyId:          	[]byte{1, 2, 3, 4, 5},
		BasicConstraintsValid: 	true,
		IsCA:            		true,
		NotBefore:       		time.Now().AddDate(-10, 0, 0),
		NotAfter:        		time.Now().AddDate(10, 0, 0),
		PublicKey:       		pubKey,
		ExtraExtensions: 		[]pkix.Extension{ext},
		ExtKeyUsage:     		[]x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:        		x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
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

func readConfig() error {
	viper.SetConfigName("lohpi_ca_config")
	viper.AddConfigPath("/var/tmp")
	viper.AddConfigPath(".")

	viper.SetConfigType("yaml")

	err := viper.ReadInConfig()
	if err != nil {
		log.Error(err.Error())
		return err
	}
	return nil
}