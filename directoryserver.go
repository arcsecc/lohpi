package lohpi

import (
	"github.com/arcsecc/lohpi/core/directoryserver"
)

type DirectoryServerOption func(*DirectoryServer)


// Sets the directory server's HTTP server port to port. Default value is 8080.
func DirectoryServerWithHTTPPort(port int) DirectoryServerOption {
	return func(d *DirectoryServer) {
		d.conf.HTTPPort = port
	}
}

// Sets the CA's address and port number. The default values are "127.0.0.1" and 8301, respectively.
func DirectoryServerWithLohpiCaConnectionString(addr string, port int) DirectoryServerOption {
	return func(d *DirectoryServer) {
		d.conf.LohpiCaAddress = addr
		d.conf.LohpiCaPort = port
	}
}

// Sets the directory server's gRPC port to port. Default value is 8081. 
func DirectoryServerWithGRPCPport(port int) DirectoryServerOption {
	return func(d *DirectoryServer) {
		d.conf.GRPCPort = port
	}
}

// Sets the directory server's X.509 certificate. Default value is "".
func DirectoryServerWithCertificate(certificateFile string) DirectoryServerOption {
	return func(d *DirectoryServer) {
		d.conf.CertificateFile = certificateFile
	}
}

// Sets the directory server's private key file path. Default value is "".
func DirectoryServerWithPrivateKey(privateKeyFile string) DirectoryServerOption {
	return func(d *DirectoryServer) {
		d.conf.PrivateKeyFile = privateKeyFile
	}
}

// TODO: enable ifrit config in the similar way

type DirectoryServer struct {
	dsCore *directoryserver.DirectoryServerCore
	conf *directoryserver.Config
}

// Returns a new DirectoryServer using the given directory server options. Returns a non-nil error, if any. 
func NewDirectoryServer(opts ...DirectoryServerOption) (*DirectoryServer, error) {
	const (
		defaulthttpPort = 8080
		defaultGrpcPort = 8081
		defaultLohpiCaAddr = "127.0.1.1"
		defaultLohpiCaPort = 8301
		defaultUseTLS = false
		defaultCertificateFile = ""
		defaultPrivateKey = ""
	)

	// Default configuration
	conf := &directoryserver.Config{
		HTTPPort: defaulthttpPort,
		GRPCPort: defaultGrpcPort,
		LohpiCaAddress: defaultLohpiCaAddr,
		LohpiCaPort: defaultLohpiCaPort,
		UseTLS: defaultUseTLS,
		CertificateFile: defaultCertificateFile,
		PrivateKeyFile: defaultPrivateKey,
	}

	ds := &DirectoryServer{
		conf: conf,
	}

	for _, opt := range opts {
		opt(ds)
	}

	dsCore, err := directoryserver.NewDirectoryServerCore(conf)
	if err != nil {
		return nil, err
	}

	ds.dsCore = dsCore

	return ds, nil
}
 
// Starts the directory server by running the Ifrit server, gRPC server and HTTP server. The call will return when 
// these services have been started.
func (d *DirectoryServer) Start() {
	d.dsCore.Start()
}

// Performs a graceful shutdown of the directory server.
func (d *DirectoryServer) Stop() {
	d.dsCore.Stop()
}

// Returns the persistent cache of the directory server.
/*func (d *DirectoryServer) Cache() *cache.Cache {
	return d.memCache
}*/