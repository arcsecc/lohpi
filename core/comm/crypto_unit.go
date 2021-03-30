package comm

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"strings"
	"time"

	log "github.com/inconshreveable/log15"
)

var (
	errNoIp    = errors.New("No ip present in received identity")
	errNoAddrs = errors.New("Not enough addresses present in identity")
	errNoCa    = errors.New("No Lohpi ca address supplied")
)

type CryptoUnit struct {
	priv   *ecdsa.PrivateKey
	pk     pkix.Name
	caAddr string

	self    *x509.Certificate
	ca      *x509.Certificate
	trusted bool
}

type certResponse struct {
	OwnCert []byte
	CaCert  []byte
	Trusted bool
}

type certSet struct {
	ownCert *x509.Certificate
	caCert  *x509.Certificate
	trusted bool
}

func NewCu(identity pkix.Name, caAddr string) (*CryptoUnit, error) {
	var certs *certSet

	serviceAddr := strings.Split(identity.Locality[0], ":")
	if len(serviceAddr) <= 0 {
		return nil, errNoIp
	}

	ip := net.ParseIP(serviceAddr[0])
	if ip == nil {
		return nil, errNoIp
	}

	priv, err := genKeys()
	if err != nil {
		return nil, err
	}

	if caAddr != "" {
		addr := fmt.Sprintf("http://%s/certificateRequest", caAddr)
		certs, err = sendCertRequest(priv, addr, identity)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, errNoCa
	}

	return &CryptoUnit{
		ca:     certs.caCert,
		self:   certs.ownCert,
		caAddr: caAddr,
		pk:     identity,
		priv:   priv,
	}, nil
}

func (cu *CryptoUnit) Trusted() bool {
	return cu.trusted
}

func (cu *CryptoUnit) Certificate() *x509.Certificate {
	return cu.self
}

func (cu *CryptoUnit) CaCertificate() *x509.Certificate {
	return cu.ca
}

func (cu *CryptoUnit) Priv() *ecdsa.PrivateKey {
	return cu.priv
}

func (cu *CryptoUnit) Pub() *ecdsa.PublicKey {
	return &cu.priv.PublicKey
}

func (cu *CryptoUnit) EncodePublicKey() ([]byte, error) {
	pubASN1, err := x509.MarshalPKIXPublicKey(&cu.priv.PublicKey)
	if err != nil {
		return nil, err
	}

	return pem.EncodeToMemory(&pem.Block{
		Type:  "ECDSA PUBLIC KEY",
		Bytes: pubASN1,
	}), nil
}

func (cu *CryptoUnit) DecodePublicKey(key []byte) (*ecdsa.PublicKey, error) {
	block, _ := pem.Decode(key)
	if block == nil {
		return nil, errors.New("failed to parse PEM block containing the key")
	}

	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, err
	}

	switch pub := pub.(type) {
	case *ecdsa.PublicKey:
		return pub, nil
	default:
		break
	}
	return nil, errors.New("Key type is not valid")
}

func (cu *CryptoUnit) Verify(data, r, s []byte, pub *ecdsa.PublicKey) bool {
	if pub == nil {
		log.Error("Peer had no publicKey")
		return false
	}

	var rInt, sInt big.Int

	b := hashContent(data)

	rInt.SetBytes(r)
	sInt.SetBytes(s)

	return ecdsa.Verify(pub, b, &rInt, &sInt)
}

func (cu *CryptoUnit) Sign(data []byte) ([]byte, []byte, error) {
	hash := hashContent(data)

	r, s, err := ecdsa.Sign(rand.Reader, cu.priv, hash)
	if err != nil {
		return nil, nil, err
	}

	return r.Bytes(), s.Bytes(), nil
}

func sendCertRequest(privKey *ecdsa.PrivateKey, caAddr string, pk pkix.Name) (*certSet, error) {
	var certs certResponse
	set := &certSet{}

	template := x509.CertificateRequest{
		SignatureAlgorithm: x509.ECDSAWithSHA256,
		Subject:            pk,
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

	set.trusted = certs.Trusted

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

func hashContent(data []byte) []byte {
	h := sha256.New()
	h.Write(data)
	return h.Sum(nil)
}
