package client

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/squareup/pranadb/conf"
	pranadbtls "github.com/squareup/pranadb/conf/tls"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/server"
	"github.com/stretchr/testify/require"
)

func TestSessionTimeout(t *testing.T) {
	cfg := conf.NewTestConfig(1)
	cfg.EnableAPIServer = true
	serverAddress := "localhost:6584"
	cfg.APIServerListenAddresses = []string{serverAddress}
	s, err := server.NewServer(*cfg)
	require.NoError(t, err)
	err = s.Start()
	require.NoError(t, err)
	defer func() {
		err = s.Stop()
		require.NoError(t, err)
	}()

	cli := NewClient(serverAddress, pranadbtls.TLSConfig{})
	err = cli.Start()
	require.NoError(t, err)
	defer func() {
		err = cli.Stop()
		require.NoError(t, err)
	}()
	ch, err := cli.ExecuteStatement("use sys", nil)
	require.NoError(t, err)
	for range ch {
	}
	ch, err = cli.ExecuteStatement("select * from sys.tables", nil)
	require.NoError(t, err)
	for range ch {
	}
}

func TestMutualTLS(t *testing.T) {
	cfg := conf.NewTestConfig(1)
	cfg.EnableAPIServer = true
	serverAddress := "localhost:6584"
	cfg.APIServerListenAddresses = []string{serverAddress}
	caCert, err := getCACert()
	require.NoError(t, err)
	tlsConfig, err := getTLSCert(caCert, "Company, INC.")
	require.NoError(t, err)
	cfg.APITLSConfig = tlsConfig
	s, err := server.NewServer(*cfg)
	require.NoError(t, err)
	err = s.Start()
	require.NoError(t, err)
	defer func() {
		err = s.Stop()
		require.NoError(t, err)
	}()

	clientTLSConfig, err := getTLSCert(caCert, "Client, INC.")
	require.NoError(t, err)
	cli := NewClient(serverAddress, clientTLSConfig)
	err = cli.Start()
	require.NoError(t, err)
	defer func() {
		err = cli.Stop()
		require.NoError(t, err)
	}()
	ch, err := cli.ExecuteStatement("select * from sys.tables", nil)
	require.NoError(t, err)
	for range ch {
	}
}

type caCert struct {
	cert    *x509.Certificate
	pem     *bytes.Buffer
	privKey interface{}
}

func getCACert() (*caCert, error) {
	ca := &x509.Certificate{
		SerialNumber: big.NewInt(2019),
		Subject: pkix.Name{
			Organization:  []string{"Company, INC."},
			Country:       []string{"US"},
			Province:      []string{""},
			Locality:      []string{"San Francisco"},
			StreetAddress: []string{"Golden Gate Bridge"},
			PostalCode:    []string{"94016"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(1, 0, 0),
		IsCA:                  true,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	caPrivKey, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return &caCert{}, errors.WithStack(err)
	}
	caBytes, err := x509.CreateCertificate(rand.Reader, ca, ca, &caPrivKey.PublicKey, caPrivKey)
	if err != nil {
		return &caCert{}, errors.WithStack(err)
	}
	caPEM, err := encodePEM(caBytes, "CERTIFICATE")
	if err != nil {
		return &caCert{}, errors.WithStack(err)
	}
	return &caCert{
		cert:    ca,
		pem:     caPEM,
		privKey: caPrivKey,
	}, nil
}

func getTLSCert(ca *caCert, ou string) (pranadbtls.TLSConfig, error) {
	// Generate cert
	cert := &x509.Certificate{
		SerialNumber: big.NewInt(1658),
		Subject: pkix.Name{
			Organization:  []string{ou},
			Country:       []string{"US"},
			Province:      []string{""},
			Locality:      []string{"San Francisco"},
			StreetAddress: []string{"Golden Gate Bridge"},
			PostalCode:    []string{"94016"},
		},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
		DNSNames:     []string{"localhost"},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().AddDate(1, 0, 0),
		SubjectKeyId: []byte{1, 2, 3, 4, 6},
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	certPrivKey, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return pranadbtls.TLSConfig{}, errors.WithStack(err)
	}
	certBytes, err := x509.CreateCertificate(rand.Reader, cert, ca.cert, &certPrivKey.PublicKey, ca.privKey)
	if err != nil {
		return pranadbtls.TLSConfig{}, errors.WithStack(err)
	}
	certPEM, err := encodePEM(certBytes, "CERTIFICATE")
	if err != nil {
		return pranadbtls.TLSConfig{}, errors.WithStack(err)
	}
	certPrivKeyPEM, err := encodePEM(x509.MarshalPKCS1PrivateKey(certPrivKey), "RSA PRIVATE KEY")
	if err != nil {
		return pranadbtls.TLSConfig{}, errors.WithStack(err)
	}
	tlsConfig := pranadbtls.TLSConfig{
		CACert: pranadbtls.NamedFileContent{
			Contents: ca.pem.Bytes(),
		},
		Cert: pranadbtls.NamedFileContent{
			Contents: certPEM.Bytes(),
		},
		Key: pranadbtls.NamedFileContent{
			Contents: certPrivKeyPEM.Bytes(),
		},
		ClientAuth: "RequireAndVerifyClientCert",
	}
	return tlsConfig, nil
}

func encodePEM(content []byte, certType string) (*bytes.Buffer, error) {
	certPEM := new(bytes.Buffer)
	err := pem.Encode(certPEM, &pem.Block{
		Type:  certType,
		Bytes: content,
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return certPEM, nil
}
