package selfsignedcert

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"time"
)

// Generate a self-signed certificate; returns the certificate and its private key or an error
func Generate(dnsNames []string) ([]byte, []byte, error) {
	return GenerateWithKeySize(keySize, dnsNames)
}

// GenerateToDisk generates a self-signed certificate and writes it and the private key to disk based
// on the configured environment and returns the paths to the generated private key and certificate,
// or an error.
func GenerateToDisk(dnsNames []string) (*string, *string, error) {
	privateKey, certificate, err := GenerateWithKeySize(keySize, dnsNames)
	if err != nil {
		log.Errorf("Failed to generate self-signed certificate and persist to local disk; %s", err.Error())
		return nil, nil, err
	}

	// Write the private key to disk
	keyOut, err := os.OpenFile(privateKeyPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		log.Errorf("Failed to open %s for writing; %s", privateKeyPath, err.Error())
		return nil, nil, err
	}
	_, err = keyOut.Write(privateKey)
	if err != nil {
		log.Errorf("Failed to write private key data to %s; %s", privateKeyPath, err.Error())
	}
	err = keyOut.Close()
	if err != nil {
		log.Errorf("Failed to generate self-signed certificate and write to disk; %s", err.Error())
		return nil, nil, err
	}

	// Write the certificate to disk
	certOut, err := os.Create(certificatePath)
	if err != nil {
		log.Errorf("Failed to open %s for writing; %s", certificatePath, err.Error())
		return nil, nil, err
	}
	_, err = certOut.Write(certificate)
	if err != nil {
		log.Errorf("Failed to write certificate data to %s; %s", certificatePath, err.Error())
	}
	err = certOut.Close()
	if err != nil {
		log.Errorf("Failed to generate self-signed certificate and write to disk; %s", err.Error())
		return nil, nil, err
	}

	return stringOrNil(privateKeyPath), stringOrNil(certificatePath), nil
}

// GenerateWithKeySize a self-signed certificate; returns the certificate and its private key or an error
func GenerateWithKeySize(bits int, dnsNames []string) ([]byte, []byte, error) {
	priv, err := rsa.GenerateKey(rand.Reader, bits)
	if err != nil {
		log.Errorf("Failed to generate RSA private key; %s", err.Error())
		return nil, nil, err
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{certificateOrganization},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(defaultCertificateValidityPeriod),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              dnsNames,
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, publicKey(priv), priv)
	if err != nil {
		log.Errorf("Failed to create certificate; %s", err)
		return nil, nil, err
	}

	keyBuf := new(bytes.Buffer)
	if err := pem.Encode(keyBuf, pemBlockForKey(priv)); err != nil {
		log.Errorf("Failed to write private key data to buffer; %s", err)
	}

	certBuf := new(bytes.Buffer)
	if err := pem.Encode(certBuf, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes}); err != nil {
		log.Errorf("Failed to write certificate data to buffer; %s", err)
		return nil, nil, err
	}

	return keyBuf.Bytes(), certBuf.Bytes(), nil
}
