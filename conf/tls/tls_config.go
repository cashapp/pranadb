package tls

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"

	"github.com/squareup/pranadb/errors"

	"github.com/alecthomas/kong"
)

// TLSConfig can be used to build a TLS configuration.
type TLSConfig struct {
	// nolint: golint
	CACert     NamedFileContent `help:"PEM-encoded CA certificate."`
	Key        NamedFileContent `help:"Private PEM-encoded key."`
	Cert       NamedFileContent `help:"Public PEM-encoded cert."`
	ClientAuth string           `help:"Client certificate authentication mode, maps to the tls ClientAuthType"`
}

var clientAuthTypeMap = map[string]tls.ClientAuthType{
	"NoClientCert":               tls.NoClientCert,
	"RequestClientCert":          tls.RequestClientCert,
	"RequireAnyClientCert":       tls.RequireAnyClientCert,
	"VerifyClientCertIfGiven":    tls.VerifyClientCertIfGiven,
	"RequireAndVerifyClientCert": tls.RequireAndVerifyClientCert,
	"":                           tls.RequireAndVerifyClientCert, // The default value if nothing is provided by the configuration
}

// NamedFileContent is a custom type that is similar to kong.NamedFileContentFlag but includes an error object.
type NamedFileContent struct {
	Filename string
	Contents []byte
	Error    error // The error returned from ioutil.ReadFile when decoding the corresponding flag.
}

// Decode is the custom kong decoder that is similar to kong.NamedFileContentFlag's decode implementation,
// but stores the error from ioutil.ReadFile instead of returning it.
func (f *NamedFileContent) Decode(ctx *kong.DecodeContext) error { // nolint: golint
	var filename string
	err := ctx.Scan.PopValueInto("filename", &filename)
	if err != nil {
		return errors.Errorf("Failed to retrieve filename flag: %v", err)
	}
	// This allows unsetting of file content flags.
	if filename == "" {
		*f = NamedFileContent{}
		return nil
	}
	filename = kong.ExpandPath(filename)
	data, err := ioutil.ReadFile(filename) // nolint: gosec
	if err != nil {
		// Instead of returning an error, set contents to nil and store the error.
		// The error will be checked during validation when building the configuration.
		f.Contents = nil
		f.Error = err
	} else {
		f.Contents = data
	}

	f.Filename = filename
	return nil
}

// ValidateTLSFiles returns the first error it finds from the NamedFileContent fields. The errors are
// populated by kong during the decode step, and are evaluated during validation.
func (t *TLSConfig) ValidateTLSFiles() error {
	if t.CACert.Error != nil {
		return errors.Errorf("failed to open %q: %s", t.CACert.Filename, t.CACert.Error)
	}

	if t.Key.Error != nil {
		return errors.Errorf("failed to open %q: %s", t.Key.Filename, t.Key.Error)
	}

	if t.Cert.Error != nil {
		return errors.Errorf("failed to open %q: %s", t.Cert.Filename, t.Cert.Error)
	}
	return nil
}

// BuildTLSConfig loads CA certs and public/private key pair given options.
//
// Will return (nil, nil) if both cert and keyare not specified.
func BuildTLSConfig(config TLSConfig) (*tls.Config, error) {
	tlsConfig := &tls.Config{ // nolint: gosec
		MinVersion: tls.VersionTLS12,
	}
	rootCertPool := x509.NewCertPool()
	if ok := rootCertPool.AppendCertsFromPEM(config.CACert.Contents); !ok {
		return nil, errors.Errorf("failed to append CA PEM from (invalid PEM block?)")
	}
	tlsConfig.RootCAs = rootCertPool
	tlsConfig.ClientCAs = rootCertPool

	certs, err := tls.X509KeyPair(config.Cert.Contents, config.Key.Contents)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to parse client key pair")
	}
	tlsConfig.Certificates = []tls.Certificate{certs}

	clientAuth, ok := clientAuthTypeMap[config.ClientAuth]
	if !ok {
		return nil, errors.Errorf("invalid tls client auth setting '%s'", config.ClientAuth)
	}
	tlsConfig.ClientAuth = clientAuth

	return tlsConfig, nil
}
