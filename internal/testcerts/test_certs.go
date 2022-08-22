package testcerts

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"github.com/google/uuid"
	"io/fs"
	"io/ioutil"
	"math/big"
	"net"
	"time"
)

/*
Some test helpers for creating tests
*/

type CACertInfo struct {
	Cert    *x509.Certificate
	Pem     *bytes.Buffer
	PrivKey interface{}
}

func CreateTestCACert(orgName string) (*CACertInfo, error) {
	ca := &x509.Certificate{
		SerialNumber: big.NewInt(2019),
		Subject: pkix.Name{
			Organization:  []string{orgName},
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
		return nil, err
	}
	caBytes, err := x509.CreateCertificate(rand.Reader, ca, ca, &caPrivKey.PublicKey, caPrivKey)
	if err != nil {
		return nil, err
	}
	caPEM, err := encodePEM(caBytes, "CERTIFICATE")
	if err != nil {
		return nil, err
	}
	return &CACertInfo{
		Cert:    ca,
		Pem:     caPEM,
		PrivKey: caPrivKey,
	}, nil
}

func CreateCertKeyPair(ca *CACertInfo, orgName string) ([]byte, []byte, error) {
	cert := &x509.Certificate{
		SerialNumber: big.NewInt(1658),
		Subject: pkix.Name{
			Organization:  []string{orgName},
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
		return nil, nil, err
	}
	var certBytes []byte
	if ca != nil {
		// CA signed certificate
		certBytes, err = x509.CreateCertificate(rand.Reader, cert, ca.Cert, &certPrivKey.PublicKey, ca.PrivKey)
	} else {
		// self signed certificate
		certBytes, err = x509.CreateCertificate(rand.Reader, cert, cert, &certPrivKey.PublicKey, certPrivKey)
	}
	if err != nil {
		return nil, nil, err
	}
	certPEM, err := encodePEM(certBytes, "CERTIFICATE")
	if err != nil {
		return nil, nil, err
	}
	certPrivKeyPEM, err := encodePEM(x509.MarshalPKCS1PrivateKey(certPrivKey), "RSA PRIVATE KEY")
	if err != nil {
		return nil, nil, err
	}
	return certPEM.Bytes(), certPrivKeyPEM.Bytes(), nil
}

func WritePemToTmpFile(dir string, bytes []byte) (string, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}
	fname := fmt.Sprintf("%s/pem-%s.pem", dir, id.String())
	err = ioutil.WriteFile(fname, bytes, fs.ModePerm)
	if err != nil {
		return "", err
	}
	return fname, nil
}

func CreateCertKeyPairToTmpFile(tmpDir string, caCertInfo *CACertInfo, orgName string) (string, string, error) {
	certBytes, keyBytes, err := CreateCertKeyPair(caCertInfo, orgName)
	if err != nil {
		return "", "", err
	}
	serverCertPath, err := WritePemToTmpFile(tmpDir, certBytes)
	if err != nil {
		return "", "", err
	}
	serverKeyPath, err := WritePemToTmpFile(tmpDir, keyBytes)
	if err != nil {
		return "", "", err
	}
	return serverCertPath, serverKeyPath, nil
}

func encodePEM(content []byte, certType string) (*bytes.Buffer, error) {
	certPEM := new(bytes.Buffer)
	err := pem.Encode(certPEM, &pem.Block{
		Type:  certType,
		Bytes: content,
	})
	if err != nil {
		return nil, err
	}
	return certPEM, nil
}
