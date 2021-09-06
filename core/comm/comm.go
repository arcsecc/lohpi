package comm

import (
	"crypto/ecdsa"
	"crypto/tls"
	"crypto/x509"
	"errors"
)

var (
	errNilCaCert = errors.New("Given CA certificate was nil")
	errNilCert   = errors.New("Given certificate was nil")
	errNilPriv   = errors.New("Given private key was nil")
)

func ServerConfig(c, caCert *x509.Certificate, key *ecdsa.PrivateKey) (*tls.Config, error) {
	if c == nil {
		return nil, errNilCert
	}

	if caCert == nil {
		return nil, errNilCaCert
	}

	if key == nil {
		return nil, errNilPriv
	}

	tlsCert := tls.Certificate{
		Certificate: [][]byte{c.Raw},
		PrivateKey:  key,
	}

	conf := &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
	}

	if caCert == nil {
		conf.ClientAuth = tls.RequestClientCert
	} else {
		pool := x509.NewCertPool()
		pool.AddCert(caCert)
		conf.ClientCAs = pool
		conf.ClientAuth = tls.RequireAndVerifyClientCert
	}

	return conf, nil
}

func ClientConfig(c, caCert *x509.Certificate, key *ecdsa.PrivateKey) *tls.Config {
	tlsCert := tls.Certificate{
		Certificate: [][]byte{c.Raw},
		PrivateKey:  key,
	}

	conf := &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
	}

	if caCert != nil {
		pool := x509.NewCertPool()
		pool.AddCert(caCert)

		conf.RootCAs = pool
	} else {
		panic(errors.New("caCert is nil"))
	}

	return conf
}
