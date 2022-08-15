package tls

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"os"

	"github.com/squareup/pranadb/errors"
)

// CertsConfig is the TLS certificates configuration.
type CertsConfig struct {
	// nolint: golint
	CACert      string `help:"PEM-encoded CA certificate file path."`
	Key         string `help:"Private PEM-encoded key file path." xor:"cert"`
	Cert        string `help:"Public PEM-encoded cert file path." xor:"cert"`
	caContent   []byte
	keyContent  []byte
	certContent []byte
}

// ServerTLSConfig is the PranaDB TLS configuration.
type ServerTLSConfig struct {
	CertsConfig
	ClientAuth string `help:"Client certificate authentication mode, maps to the tls ClientAuthType"`
}

var clientAuthTypeMap = map[string]tls.ClientAuthType{
	"NoClientCert":               tls.NoClientCert,
	"RequestClientCert":          tls.RequestClientCert,
	"RequireAnyClientCert":       tls.RequireAnyClientCert,
	"VerifyClientCertIfGiven":    tls.VerifyClientCertIfGiven,
	"RequireAndVerifyClientCert": tls.RequireAndVerifyClientCert,
	"":                           tls.RequireAndVerifyClientCert, // The default value if nothing is provided by the configuration
}

// Validate validates the existence of the TLS cert, key and CA if provided
func (c *CertsConfig) Validate() error {
	_, err := os.Stat(c.Key)
	if err != nil {
		return errors.Errorf("failed to open %q: %s", c.Key, err)
	}

	_, err = os.Stat(c.Cert)
	if err != nil {
		return errors.Errorf("failed to open %q: %s", c.Cert, err)
	}

	if c.CACert == "" {
		return nil
	}
	_, err = os.Stat(c.CACert)
	return errors.WithStack(err)
}

func (c *CertsConfig) load() error {
	var err error
	if c.Cert != "" {
		c.certContent, err = ioutil.ReadFile(c.Cert)
		if err != nil {
			return errors.WithStack(err)
		}
	}
	if c.Key != "" {
		c.keyContent, err = ioutil.ReadFile(c.Key)
		if err != nil {
			return errors.WithStack(err)
		}
	}
	if c.CACert == "" {
		return nil
	}
	c.caContent, err = ioutil.ReadFile(c.CACert)
	return errors.WithStack(err)
}

// BuildServerTLSConfig loads CA certs and public/private key pair given options and configures Server side TLS.
func BuildServerTLSConfig(config ServerTLSConfig) (*tls.Config, error) {
	err := config.load()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	certs, err := tls.X509KeyPair(config.certContent, config.keyContent)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to parse client key pair")
	}
	tlsConfig.Certificates = []tls.Certificate{certs}

	certPool := x509.NewCertPool()
	if config.CACert != "" {
		if ok := certPool.AppendCertsFromPEM(config.caContent); !ok {
			return nil, errors.Errorf("failed to append CA PEM from (invalid PEM block?)")
		}
	}
	tlsConfig.ClientCAs = certPool

	authType, exists := clientAuthTypeMap[config.ClientAuth]
	if !exists {
		return nil, errors.Errorf("invalid client auth value %s", config.ClientAuth)
	}
	tlsConfig.ClientAuth = authType
	return tlsConfig, nil
}

// BuildClientTLSConfig loads CA certs and public/private key pair given options.
func BuildClientTLSConfig(config CertsConfig) (*tls.Config, error) {
	err := config.load()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	if config.Cert != "" && config.Key != "" {
		certs, err := tls.X509KeyPair(config.certContent, config.keyContent)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse client key pair")
		}
		tlsConfig.Certificates = []tls.Certificate{certs}
	}

	certPool := x509.NewCertPool()
	if config.CACert != "" {
		if ok := certPool.AppendCertsFromPEM(config.caContent); !ok {
			return nil, errors.Errorf("failed to append CA PEM from (invalid PEM block?)")
		}
	}
	tlsConfig.RootCAs = certPool
	return tlsConfig, nil
}
