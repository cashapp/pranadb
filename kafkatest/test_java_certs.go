package kafkatest

import (
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"

	"github.com/squareup/pranadb/errors"
	"software.sslmate.com/src/go-pkcs12"
)

func createTrustStorePKCS12(certPEM []byte) ([]byte, error) {
	certBlock, _ := pem.Decode(certPEM)
	if certBlock == nil {
		return nil, errors.Errorf("invalid cert PEM")
	}

	if certBlock.Type != "CERTIFICATE" {
		return nil, errors.Errorf(`expected "CERTIFICATE" block, got %q`, certBlock.Type)
	}

	cert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		return nil, errors.Wrap(err, "could not parse certificate")
	}

	p12, err := pkcs12.EncodeTrustStore(rand.Reader, []*x509.Certificate{cert}, "changeit")
	if err != nil {
		return nil, errors.Wrap(err, "could not encode p12")
	}
	return p12, nil
}

func createKeystorePKCS12(privateKeyPEM []byte, certPEM []byte) ([]byte, error) {
	privateKeyBlock, _ := pem.Decode(privateKeyPEM)
	if privateKeyBlock == nil {
		return nil, errors.Errorf("invalid private key PEM")
	}

	if privateKeyBlock.Type != "RSA PRIVATE KEY" {
		return nil, errors.Errorf(`expected "RSA PRIVATE KEY" block, got %q`, privateKeyBlock.Type)
	}

	privateKey, err := x509.ParsePKCS1PrivateKey(privateKeyBlock.Bytes)
	if err != nil {
		return nil, errors.Wrapf(err, "could not decode private key")
	}

	certBlock, _ := pem.Decode(certPEM)
	if certBlock == nil {
		return nil, errors.Errorf("invalid cert PEM")
	}

	if certBlock.Type != "CERTIFICATE" {
		return nil, errors.Errorf(`expected "CERTIFICATE" block, got %q`, certBlock.Type)
	}

	cert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		return nil, errors.Wrap(err, "could not parse certificate")
	}

	p12, err := pkcs12.Encode(rand.Reader, privateKey, cert, nil, "changeit")
	if err != nil {
		return nil, errors.Wrap(err, "could not encode p12")
	}
	return p12, nil
}
