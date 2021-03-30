package comm

import (
	"crypto/ecdsa"
	"crypto/x509"
	pb "github.com/arcsecc/lohpi/protobuf"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"time"
)

// TODO: clean up this mess

type DirectoryGRPCClient struct {
	dialOptions []grpc.DialOption
	pb.DirectoryServerClient
}

type DirectoryServerConn struct {
	pb.DirectoryServerClient
	cc *grpc.ClientConn
}

type PolicyStoreGRPCClient struct {
	dialOptions []grpc.DialOption
	pb.PolicyStoreClient
}

type PolicyStoreConn struct {
	pb.PolicyStoreClient
	cc *grpc.ClientConn
}

func NewMuxGRPCClient(cert, caCert *x509.Certificate, priv *ecdsa.PrivateKey) (*DirectoryGRPCClient, error) {
	var dialOptions []grpc.DialOption
	// check paramters here
	config := ClientConfig(cert, caCert, priv)
	creds := credentials.NewTLS(config)

	dialOptions = append(dialOptions, grpc.WithTransportCredentials(creds))
	dialOptions = append(dialOptions, grpc.WithBackoffMaxDelay(time.Minute*1))

	return &DirectoryGRPCClient{
		dialOptions: dialOptions,
	}, nil
}

func (c *DirectoryGRPCClient) Dial(addr string) (*DirectoryServerConn, error) {
	cc, err := grpc.Dial(addr, c.dialOptions...)
	if err != nil {
		return nil, err
	} else {
		connection := &DirectoryServerConn{
			DirectoryServerClient: pb.NewDirectoryServerClient(cc),
			cc:                    cc,
		}

		return connection, nil
	}
}

func (c *DirectoryServerConn) CloseConn() {
	c.cc.Close()
}

func NewPolicyStoreClient(cert, caCert *x509.Certificate, priv *ecdsa.PrivateKey) (*PolicyStoreGRPCClient, error) {
	var dialOptions []grpc.DialOption
	// check paramters here
	config := ClientConfig(cert, caCert, priv)
	creds := credentials.NewTLS(config)

	dialOptions = append(dialOptions, grpc.WithTransportCredentials(creds))
	dialOptions = append(dialOptions, grpc.WithBackoffMaxDelay(time.Minute*1))

	return &PolicyStoreGRPCClient{
		dialOptions: dialOptions,
	}, nil
}

func (c *PolicyStoreGRPCClient) Dial(addr string) (*PolicyStoreConn, error) {
	cc, err := grpc.Dial(addr, c.dialOptions...)
	if err != nil {
		return nil, err
	} else {
		connection := &PolicyStoreConn{
			PolicyStoreClient: pb.NewPolicyStoreClient(cc),
			cc:                cc,
		}

		return connection, nil
	}
}

func (c *PolicyStoreConn) CloseConn() {
	c.cc.Close()
}
