package common

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"

	"github.com/squareup/pranadb/conf"
	"github.com/squareup/pranadb/errors"
)

func CreateServerTLSConfig(config conf.TLSConfig) (*tls.Config, error) {
	if !config.Enabled {
		return nil, nil
	}
	tlsConfig := &tls.Config{ // nolint: gosec
		MinVersion: tls.VersionTLS12,
	}
	keyPair, err := CreateKeyPair(config.CertPath, config.KeyPath)
	if err != nil {
		return nil, err
	}
	tlsConfig.Certificates = []tls.Certificate{keyPair}
	if config.ClientCertsPath != "" {
		clientCerts, err := ioutil.ReadFile(config.ClientCertsPath)
		if err != nil {
			return nil, err
		}
		trustedCertPool := x509.NewCertPool()
		if ok := trustedCertPool.AppendCertsFromPEM(clientCerts); !ok {
			return nil, errors.Errorf("failed to append trusted certs PEM (invalid PEM block?)")
		}
		tlsConfig.ClientCAs = trustedCertPool
	}
	clientAuth, ok := clientAuthTypeMap[config.ClientAuth]
	if !ok {
		return nil, errors.Errorf("invalid tls client auth setting '%s'", config.ClientAuth)
	}
	if config.ClientCertsPath != "" && config.ClientAuth == "" {
		// If client certs provided then default to client auth required
		clientAuth = tls.RequireAndVerifyClientCert
	}
	tlsConfig.ClientAuth = clientAuth
	return tlsConfig, nil
}

var clientAuthTypeMap = map[string]tls.ClientAuthType{
	conf.ClientAuthModeNoClientCert:               tls.NoClientCert,
	conf.ClientAuthModeRequestClientCert:          tls.RequestClientCert,
	conf.ClientAuthModeRequireAnyClientCert:       tls.RequireAnyClientCert,
	conf.ClientAuthModeVerifyClientCertIfGiven:    tls.VerifyClientCertIfGiven,
	conf.ClientAuthModeRequireAndVerifyClientCert: tls.RequireAndVerifyClientCert,
	conf.ClientAuthModeUnspecified:                tls.NoClientCert,
}
