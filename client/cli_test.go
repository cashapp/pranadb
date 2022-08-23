package client

import (
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/conf"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/internal/testcerts"
	"github.com/squareup/pranadb/server"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"strings"
	"testing"
)

var (
	caCertPath string

	caSignedServerKeyPath  string
	caSignedServerCertPath string

	caSignedClientKeyPath  string
	caSignedClientCertPath string

	selfSignedServerKeyPath  string
	selfSignedServerCertPath string

	selfSignedClientKeyPath  string
	selfSignedClientCertPath string

	selfSignedClientKeyPath2  string
	selfSignedClientCertPath2 string
)

func TestMain(m *testing.M) {
	// Set up all the certs we need here. Creating certs is slow, we don't want to do it on each test

	log.Info("creating certificates required for tests")

	tmpDir, err := ioutil.TempDir("", "cli_test")
	if err != nil {
		log.Fatalf("failed to create tmp dir %v", err)
	}

	caCert, err := testcerts.CreateTestCACert("acme CA")
	if err != nil {
		log.Fatalf("failed to create ca cert %v", err)
	}
	caCertPath, err = testcerts.WritePemToTmpFile(tmpDir, caCert.Pem.Bytes())
	if err != nil {
		log.Fatalf("failed to write pem file %v", err)
	}

	caSignedServerCertPath, caSignedServerKeyPath, err = testcerts.CreateCertKeyPairToTmpFile(tmpDir, caCert, "acme badgers ltd.")
	if err != nil {
		log.Fatalf("failed to cert key pair %v", err)
	}
	caSignedClientCertPath, caSignedClientKeyPath, err = testcerts.CreateCertKeyPairToTmpFile(tmpDir, caCert, "acme squirrels ltd.")
	if err != nil {
		log.Fatalf("failed to cert key pair %v", err)
	}

	selfSignedServerCertPath, selfSignedServerKeyPath, err = testcerts.CreateCertKeyPairToTmpFile(tmpDir, nil, "acme antelopes ltd.")
	if err != nil {
		log.Fatalf("failed to cert key pair %v", err)
	}
	selfSignedClientCertPath, selfSignedClientKeyPath, err = testcerts.CreateCertKeyPairToTmpFile(tmpDir, nil, "acme aardvarks ltd.")
	if err != nil {
		log.Fatalf("failed to cert key pair %v", err)
	}

	selfSignedClientCertPath2, selfSignedClientKeyPath2, err = testcerts.CreateCertKeyPairToTmpFile(tmpDir, nil, "acme squirrels ltd.")
	if err != nil {
		log.Fatalf("failed to cert key pair %v", err)
	}

	defer func() {
		defer func() {
			err := os.RemoveAll(tmpDir)
			if err != nil {
				log.Fatalf("failed to remove test tmpdir %v", err)
			}
		}()
	}()
	m.Run()
}

func TestPlaintextGRPC(t *testing.T) {
	err := testClientTransport(t, false, TLSConfig{}, conf.TLSConfig{})
	require.NoError(t, err)
}

func TestGRPCWithTlsNoClientAuth(t *testing.T) {
	testWithTLSNoClientAuth(t, false)
}

func TestHTTPWithTlsNoClientAuth(t *testing.T) {
	testWithTLSNoClientAuth(t, true)
}

func testWithTLSNoClientAuth(t *testing.T, httpAPI bool) {
	t.Helper()
	clientTLSConfig := TLSConfig{
		EnableTLS:        true,
		TrustedCertsPath: caSignedServerCertPath,
	}
	serverTLSConfig := conf.TLSConfig{
		Enabled:  true,
		KeyPath:  caSignedServerKeyPath,
		CertPath: caSignedServerCertPath,
	}
	err := testClientTransport(t, httpAPI, clientTLSConfig, serverTLSConfig)
	require.NoError(t, err)
}

func TestGRPCWithTLSNoClientAuthNoServerTrust(t *testing.T) {
	testWithTLSNoClientAuthNoServerTrust(t, false)
}

func TestHTTPWithTLSNoClientAuthNoServerTrust(t *testing.T) {
	testWithTLSNoClientAuthNoServerTrust(t, true)
}

func testWithTLSNoClientAuthNoServerTrust(t *testing.T, httpAPI bool) {
	t.Helper()
	clientTLSConfig := TLSConfig{
		EnableTLS:                 true,
		DisableServerVerification: true,
	}
	serverTLSConfig := conf.TLSConfig{
		Enabled:  true,
		KeyPath:  caSignedServerKeyPath,
		CertPath: caSignedServerCertPath,
	}
	err := testClientTransport(t, httpAPI, clientTLSConfig, serverTLSConfig)
	require.NoError(t, err)
}

func TestGRPCWithTLSClientAuthSingleCA(t *testing.T) {
	testWithTLSClientAuthSingleCA(t, false)
}

func TestHTTPWithTLSClientAuthSingleCA(t *testing.T) {
	testWithTLSClientAuthSingleCA(t, true)
}

func testWithTLSClientAuthSingleCA(t *testing.T, httpAPI bool) {
	t.Helper()
	// Use the same CA that signs both the server and client certificate
	clientTLSConfig := TLSConfig{
		EnableTLS:        true,
		TrustedCertsPath: caCertPath,
		KeyPath:          caSignedClientKeyPath,
		CertPath:         caSignedClientCertPath,
	}
	serverTLSConfig := conf.TLSConfig{
		Enabled:         true,
		KeyPath:         caSignedServerKeyPath,
		CertPath:        caSignedServerCertPath,
		ClientCertsPath: caCertPath,
		ClientAuth:      "require-and-verify-client-cert",
	}
	err := testClientTransport(t, httpAPI, clientTLSConfig, serverTLSConfig)
	require.NoError(t, err)
}

func TestGRPCWithTLSClientAuthSelfSignedCerts(t *testing.T) {
	testWithTLSClientAuthSelfSignedCerts(t, false)
}

func TestHTTPWithTLSClientAuthSelfSignedCerts(t *testing.T) {
	testWithTLSClientAuthSelfSignedCerts(t, true)
}

func testWithTLSClientAuthSelfSignedCerts(t *testing.T, httpAPI bool) {
	t.Helper()
	// Using self signed certs for server and client - no common CA
	clientTLSConfig := TLSConfig{
		EnableTLS:        true,
		TrustedCertsPath: selfSignedServerCertPath,
		KeyPath:          selfSignedClientKeyPath,
		CertPath:         selfSignedClientCertPath,
	}
	serverTLSConfig := conf.TLSConfig{
		Enabled:         true,
		KeyPath:         selfSignedServerKeyPath,
		CertPath:        selfSignedServerCertPath,
		ClientCertsPath: selfSignedClientCertPath,
		ClientAuth:      "require-and-verify-client-cert",
	}
	err := testClientTransport(t, httpAPI, clientTLSConfig, serverTLSConfig)
	require.NoError(t, err)
}

func TestGRPCWithTLSClientAuthFailNoClientCertProvided(t *testing.T) {
	testWithTLSClientAuthFailNoClientCertProvided(t, false)
}

func TestHTTPWithTLSClientAuthFailNoClientCertProvided(t *testing.T) {
	testWithTLSClientAuthFailNoClientCertProvided(t, true)
}

func testWithTLSClientAuthFailNoClientCertProvided(t *testing.T, httpAPI bool) {
	t.Helper()
	// Clients certs required but not provided
	clientTLSConfig := TLSConfig{
		EnableTLS:        true,
		TrustedCertsPath: selfSignedClientCertPath,
	}
	serverTLSConfig := conf.TLSConfig{
		Enabled:         true,
		KeyPath:         selfSignedServerKeyPath,
		CertPath:        selfSignedServerCertPath,
		ClientCertsPath: selfSignedClientCertPath,
		ClientAuth:      "require-and-verify-client-cert",
	}
	err := testClientTransport(t, httpAPI, clientTLSConfig, serverTLSConfig)
	require.Error(t, err)
}

func TestGRPCWithTLSClientAuthFailUntrustedClientCertProvided(t *testing.T) {
	testWithTLSClientAuthFailUntrustedClientCertProvided(t, false)
}

func TestHTTPWithTLSClientAuthFailUntrustedClientCertProvided(t *testing.T) {
	testWithTLSClientAuthFailUntrustedClientCertProvided(t, true)
}

func testWithTLSClientAuthFailUntrustedClientCertProvided(t *testing.T, httpAPI bool) {
	t.Helper()
	// Clients certs required but untrusted one provided
	clientTLSConfig := TLSConfig{
		EnableTLS:        true,
		TrustedCertsPath: selfSignedServerCertPath,
		CertPath:         selfSignedClientCertPath2,
		KeyPath:          selfSignedClientKeyPath2,
	}
	serverTLSConfig := conf.TLSConfig{
		Enabled:         true,
		KeyPath:         selfSignedServerKeyPath,
		CertPath:        selfSignedServerCertPath,
		ClientCertsPath: selfSignedClientCertPath,
		ClientAuth:      "require-and-verify-client-cert",
	}
	err := testClientTransport(t, httpAPI, clientTLSConfig, serverTLSConfig)
	require.Error(t, err)
}

func testClientTransport(t *testing.T, httpAPI bool, clientTLSConfig TLSConfig, serverTLSConfig conf.TLSConfig) error {
	t.Helper()
	cfg := conf.NewTestConfig(1)
	serverAddress := "localhost:6584"
	if httpAPI {
		cfg.HTTPAPIServerEnabled = true
		cfg.GRPCAPIServerEnabled = false
		cfg.HTTPAPIServerListenAddresses = []string{serverAddress}
		cfg.HTTPAPIServerTLSConfig = serverTLSConfig
	} else {
		cfg.GRPCAPIServerEnabled = true
		cfg.HTTPAPIServerEnabled = false
		cfg.GRPCAPIServerListenAddresses = []string{serverAddress}
		cfg.GRPCAPIServerTLSConfig = serverTLSConfig
	}
	s, err := server.NewServer(*cfg)
	require.NoError(t, err)
	err = s.Start()
	require.NoError(t, err)
	defer func() {
		err = s.Stop()
		require.NoError(t, err)
	}()
	var cli *Client
	if httpAPI {
		cli = NewClientUsingHTTP(serverAddress, clientTLSConfig)
	} else {
		cli = NewClientUsingGRPC(serverAddress, clientTLSConfig)
	}
	cli.SetExitOnError(false)
	err = cli.Start()
	if err != nil {
		return err
	}
	log.Println("started client")
	defer func() {
		err = cli.Stop()
		require.NoError(t, err)
	}()
	ch, err := cli.ExecuteStatement("use sys", nil, nil)
	if err != nil {
		return err
	}
	for range ch {
	}
	log.Println("about to execute select *")
	ch, err = cli.ExecuteStatement("select * from sys.tables", nil, nil)
	if err != nil {
		log.Printf("select * returned err %v", err)
		return err
	}
	var out strings.Builder
	log.Println("about to print results")
	for line := range ch {
		log.Println(line)
		out.WriteString(line)
		out.WriteRune('\n')
	}
	log.Println("ranged through results")
	actual := out.String()
	if actual == expectedOutput {
		return nil
	}
	return errors.New(actual)
}

var expectedOutput = "+-----------------------------------------------------------------------------------------------------------------+\n" +
	"| id                   | kind       | schema_n.. | name       | table_info | topic_info | query      | mv_name    |\n" +
	"+-----------------------------------------------------------------------------------------------------------------+\n" +
	"0 rows returned\n"

func TestClientNotExistentKeyFile(t *testing.T) {
	tlsConfig := TLSConfig{
		EnableTLS: true,
		KeyPath:   "nothing/here/client.key",
		CertPath:  caSignedClientCertPath,
	}
	cl := NewClientUsingGRPC("localhost:6584", tlsConfig)
	err := cl.Start()
	require.Error(t, err)
	require.Equal(t, "open nothing/here/client.key: no such file or directory", err.Error())
}

func TestClientNotExistentCertFile(t *testing.T) {
	tlsConfig := TLSConfig{
		EnableTLS: true,
		KeyPath:   caSignedClientKeyPath,
		CertPath:  "nothing/here/client.crt",
	}
	cl := NewClientUsingGRPC("localhost:6584", tlsConfig)
	err := cl.Start()
	require.Error(t, err)
	require.Equal(t, "open nothing/here/client.crt: no such file or directory", err.Error())
}
