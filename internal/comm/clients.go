package comm

import (
	"crypto/x509"
	"crypto/ecdsa"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	pb "github.com/tomcat-bit/lohpi/protobuf"
)

// TODO: clean up this mess

type MuxGRPCClient struct {
	dialOptions []grpc.DialOption
	pb.MuxClient
}

type MuxConn struct {
	pb.MuxClient
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

type RecGRPCClient struct {
	dialOptions []grpc.DialOption
	pb.RecClient
}

type RecConn struct {
	pb.RecClient
	cc *grpc.ClientConn
}

func NewMuxGRPCClient(cert, caCert *x509.Certificate, priv *ecdsa.PrivateKey) (*MuxGRPCClient, error) {
	var dialOptions []grpc.DialOption
	// check paramters here
	config := ClientConfig(cert, caCert, priv)
	creds := credentials.NewTLS(config)

	dialOptions = append(dialOptions, grpc.WithTransportCredentials(creds))
	dialOptions = append(dialOptions, grpc.WithBackoffMaxDelay(time.Minute * 1))

	return &MuxGRPCClient{
		dialOptions:    dialOptions,
	}, nil
}

func (c *MuxGRPCClient) Dial(addr string) (*MuxConn, error) {
	cc, err := grpc.Dial(addr, c.dialOptions...)
	if err != nil {
		return nil, err
	} else {
		connection := &MuxConn{
			MuxClient: 		pb.NewMuxClient(cc),
			cc:          	 cc,
		}
		
		return connection, nil
	}
}

func (c *MuxConn) CloseConn() {
	c.cc.Close()
}

func NewPolicyStoreClient(cert, caCert *x509.Certificate, priv *ecdsa.PrivateKey) (*PolicyStoreGRPCClient, error) {
	var dialOptions []grpc.DialOption
	// check paramters here
	config := ClientConfig(cert, caCert, priv)
	creds := credentials.NewTLS(config)

	dialOptions = append(dialOptions, grpc.WithTransportCredentials(creds))
	dialOptions = append(dialOptions, grpc.WithBackoffMaxDelay(time.Minute * 1))

	return &PolicyStoreGRPCClient{
		dialOptions:    dialOptions,
	}, nil
}

func (c *PolicyStoreGRPCClient) Dial(addr string) (*PolicyStoreConn, error) {
	cc, err := grpc.Dial(addr, c.dialOptions...)
	if err != nil {
		return nil, err
	} else {
		connection := &PolicyStoreConn{
			PolicyStoreClient: 	pb.NewPolicyStoreClient(cc),
			cc:          		cc,
		}
		
		return connection, nil
	}
}

func (c *PolicyStoreConn) CloseConn() {
	c.cc.Close()
}

func NewRecClient(cert, caCert *x509.Certificate, priv *ecdsa.PrivateKey) (*RecGRPCClient, error) {
	var dialOptions []grpc.DialOption
	// check paramters here
	config := ClientConfig(cert, caCert, priv)
	creds := credentials.NewTLS(config)

	dialOptions = append(dialOptions, grpc.WithTransportCredentials(creds))
	dialOptions = append(dialOptions, grpc.WithBackoffMaxDelay(time.Minute * 1))

	return &RecGRPCClient{
		dialOptions:    dialOptions,
	}, nil
}

func (c *RecGRPCClient) Dial(addr string) (*RecConn, error) {
	cc, err := grpc.Dial(addr, c.dialOptions...)
	if err != nil {
		return nil, err
	} else {
		connection := &RecConn{
			RecClient:	pb.NewRecClient(cc),
			cc:         cc,
		}
		
		return connection, nil
	}
}

func (c *RecConn) CloseConn() {
	c.cc.Close()
}