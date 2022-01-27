package comm

import (
	"crypto"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/http"
	"golang.org/x/crypto/acme/autocert"

	"log"
)

var (
	errNilCert   = errors.New("Given certificate was nil")
	errNilPriv   = errors.New("Given private key was nil")
)

func ServerConfig(c, caCert *x509.Certificate, key crypto.PrivateKey) (*tls.Config, error) {
	if c == nil {
		return nil, errNilCert
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

func ServerConfigWithACME(hostname string) (*tls.Config, error) {
	log.Println("Hostname:", hostname)
	manager := autocert.Manager{
		Prompt: autocert.AcceptTOS,
		HostPolicy: autocert.HostWhitelist(hostname),
	}

	go http.ListenAndServe(":http", manager.HTTPHandler(nil))

	return manager.TLSConfig(), nil
}

func ClientConfig(c, caCert *x509.Certificate, key crypto.PrivateKey) *tls.Config {
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
		conf.InsecureSkipVerify = true
	}

	return conf
}

