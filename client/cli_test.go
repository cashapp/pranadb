package client

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io/ioutil"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/squareup/pranadb/conf"
	"github.com/squareup/pranadb/conf/tls"
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

	cli := NewClient(serverAddress, tls.CertsConfig{})
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

func TestSelfSignedTLSCertNoClientCert(t *testing.T) {
	cfg := conf.NewTestConfig(1)
	cfg.EnableAPIServer = true
	serverAddress := "localhost:6584"
	cfg.APIServerListenAddresses = []string{serverAddress}

	certsDir, err := ioutil.TempDir("", "certs")
	require.NoError(t, err)
	defer os.RemoveAll(certsDir)

	certsConfig, err := createTLSCert(certsDir, nil, "Company, INC.")
	require.NoError(t, err)
	cfg.APITLSConfig = tls.ServerTLSConfig{
		CertsConfig: certsConfig,
		ClientAuth:  "NoClientCert",
	}
	s, err := server.NewServer(*cfg)
	require.NoError(t, err)
	err = s.Start()
	require.NoError(t, err)
	defer func() {
		err = s.Stop()
		require.NoError(t, err)
	}()

	// Use server's self-signed cert as its own CA.
	cli := NewClient(serverAddress, tls.CertsConfig{
		CACert: certsConfig.Cert,
	})
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

	ch, err = cli.ExecuteStatement("select * from tables", nil)
	require.NoError(t, err)
	for range ch {
	}
}

func TestTLSSelfSignedCANoClientCert(t *testing.T) {
	cfg := conf.NewTestConfig(1)
	cfg.EnableAPIServer = true
	serverAddress := "localhost:6584"
	cfg.APIServerListenAddresses = []string{serverAddress}

	certsDir, err := ioutil.TempDir("", "certs")
	require.NoError(t, err)
	defer os.RemoveAll(certsDir)

	caCert, err := createCACert(certsDir)
	require.NoError(t, err)
	certsConfig, err := createTLSCert(certsDir, caCert, "Company, INC.")
	require.NoError(t, err)
	cfg.APITLSConfig = tls.ServerTLSConfig{
		CertsConfig: certsConfig,
		ClientAuth:  "NoClientCert",
	}
	s, err := server.NewServer(*cfg)
	require.NoError(t, err)
	err = s.Start()
	require.NoError(t, err)
	defer func() {
		err = s.Stop()
		require.NoError(t, err)
	}()

	// Use the self-signed CA cert.
	cli := NewClient(serverAddress, tls.CertsConfig{
		CACert: caCert.path,
	})
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

	ch, err = cli.ExecuteStatement("select * from tables", nil)
	require.NoError(t, err)
	for range ch {
	}
}

func TestMutualTLS(t *testing.T) {
	cfg := conf.NewTestConfig(1)
	cfg.EnableAPIServer = true
	serverAddress := "localhost:6584"
	cfg.APIServerListenAddresses = []string{serverAddress}

	serverCertsDir, err := ioutil.TempDir("", "server-certs")
	require.NoError(t, err)
	defer os.RemoveAll(serverCertsDir)

	caCert, err := createCACert(serverCertsDir)
	require.NoError(t, err)
	certsConfig, err := createTLSCert(serverCertsDir, caCert, "Company, INC.")
	certsConfig.CACert = caCert.path
	require.NoError(t, err)
	cfg.APITLSConfig = tls.ServerTLSConfig{
		CertsConfig: certsConfig,
		ClientAuth:  "RequireAndVerifyClientCert",
	}
	s, err := server.NewServer(*cfg)
	require.NoError(t, err)
	err = s.Start()
	require.NoError(t, err)
	defer func() {
		err = s.Stop()
		require.NoError(t, err)
	}()

	clientCertsDir, err := ioutil.TempDir("", "client-certs")
	require.NoError(t, err)
	defer os.RemoveAll(clientCertsDir)

	clientTLSConfig, err := createTLSCert(clientCertsDir, caCert, "Client, INC.")
	clientTLSConfig.CACert = caCert.path
	require.NoError(t, err)

	cli := NewClient(serverAddress, clientTLSConfig)
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

	ch, err = cli.ExecuteStatement("select * from tables", nil)
	require.NoError(t, err)
	for range ch {
	}
}

type caCert struct {
	cert    *x509.Certificate
	pem     *bytes.Buffer
	privKey interface{}
	path    string
}

func createCACert(certsDir string) (*caCert, error) {
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
	certPath, err := saveCert(certsDir, "ca.pem", caPEM)
	if err != nil {
		return &caCert{}, errors.WithStack(err)
	}
	return &caCert{
		cert:    ca,
		pem:     caPEM,
		privKey: caPrivKey,
		path:    certPath,
	}, nil
}

func createTLSCert(certsDir string, ca *caCert, ou string) (tls.CertsConfig, error) {
	subjectKeyID := make([]byte, 5)
	_, err := rand.Read(subjectKeyID)
	if err != nil {
		return tls.CertsConfig{}, errors.WithStack(err)
	}

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
		SubjectKeyId: subjectKeyID,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	certPrivKey, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return tls.CertsConfig{}, errors.WithStack(err)
	}

	// If no CA is provided make the certificate self-signed: the cert is the root (parent) cert.
	parentCert := cert
	var parentCertPrivKey interface{} = certPrivKey
	if ca != nil {
		parentCert = ca.cert
		parentCertPrivKey = ca.privKey
	}
	certBytes, err := x509.CreateCertificate(rand.Reader, cert, parentCert, &certPrivKey.PublicKey, parentCertPrivKey)
	if err != nil {
		return tls.CertsConfig{}, errors.WithStack(err)
	}
	certPEM, err := encodePEM(certBytes, "CERTIFICATE")
	if err != nil {
		return tls.CertsConfig{}, errors.WithStack(err)
	}
	certPath, err := saveCert(certsDir, "cert.pem", certPEM)
	if err != nil {
		return tls.CertsConfig{}, errors.WithStack(err)
	}
	certPrivKeyPEM, err := encodePEM(x509.MarshalPKCS1PrivateKey(certPrivKey), "RSA PRIVATE KEY")
	if err != nil {
		return tls.CertsConfig{}, errors.WithStack(err)
	}
	certPrivKeyPath, err := saveCert(certsDir, "key.pem", certPrivKeyPEM)
	if err != nil {
		return tls.CertsConfig{}, errors.WithStack(err)
	}
	tlsConfig := tls.CertsConfig{
		Cert: certPath,
		Key:  certPrivKeyPath,
	}
	return tlsConfig, nil
}

func saveCert(certsDir string, fileName string, buffer *bytes.Buffer) (string, error) {
	certPath := filepath.Join(certsDir, fileName)
	err := ioutil.WriteFile(certPath, buffer.Bytes(), 0644)
	return certPath, errors.WithStack(err)
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
