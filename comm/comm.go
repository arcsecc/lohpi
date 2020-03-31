package comm

import (
	"crypto/ecdsa"
	"crypto/tls"
	"crypto/x509"
	"errors"
	//"net"
)

var (
	errNilCert = errors.New("Given certificate was nil")
	errNilPriv = errors.New("Given private key was nil")
)

type Comm struct {
	config *tls.Config
}

/*
func NewComm(cert, caCert *x509.Certificate, priv *ecdsa.PrivateKey, l net.Listener) (*Comm, error) {
	if cert == nil {
		return nil, errNilCert
	}

	if priv == nil {
		return nil, errNilPriv
	}

	serverConf := serverConfig(cert, caCert, priv)

	server, err := newServer(serverConf, l)
	if err != nil {
		return nil, err
	}

	clientConf := clientConfig(cert, caCert, priv)

	client, err := newClient(clientConf)
	if err != nil {
		return nil, err
	}
}*/

func ServerConfig(c, caCert *x509.Certificate, key *ecdsa.PrivateKey) *tls.Config {
	tlsCert := tls.Certificate{
		Certificate: [][]byte{c.Raw},
		PrivateKey:  key,
	}

	conf := &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		//InsecureSkipVerify: true,				// OOPS
	}

	if caCert == nil {
		conf.ClientAuth = tls.RequestClientCert
	} else {
		pool := x509.NewCertPool()
		pool.AddCert(caCert)
		conf.ClientCAs = pool
		conf.ClientAuth = tls.RequireAndVerifyClientCert
	}

	return conf
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
