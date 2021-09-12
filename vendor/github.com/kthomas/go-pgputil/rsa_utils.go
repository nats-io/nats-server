package pgputil

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
)

var (
	// ErrKeyMustBePEMEncoded is an error indicating the given key was not a PEM-encoded PKCS1 or PKCS8 private key
	ErrKeyMustBePEMEncoded = errors.New("Invalid key: Key must be PEM encoded PKCS1 or PKCS8 private key")

	// ErrNotRSAPrivateKey is an error indicating the given key was not a valid RSA private key
	ErrNotRSAPrivateKey = errors.New("Key is not a valid RSA private key")

	// ErrNotRSAPublicKey is an error indicating the given key was not a valid RSA public key
	ErrNotRSAPublicKey = errors.New("Key is not a valid RSA public key")
)

// DecodeRSAPrivateKeyFromPEM parses a PEM-encoded PKCS1 or PKCS8 private key
func DecodeRSAPrivateKeyFromPEM(key []byte) (*rsa.PrivateKey, error) {
	var err error

	// Parse PEM block
	var block *pem.Block
	if block, _ = pem.Decode(key); block == nil {
		return nil, ErrKeyMustBePEMEncoded
	}

	var parsedKey interface{}
	if parsedKey, err = x509.ParsePKCS1PrivateKey(block.Bytes); err != nil {
		if parsedKey, err = x509.ParsePKCS8PrivateKey(block.Bytes); err != nil {
			return nil, err
		}
	}

	var pkey *rsa.PrivateKey
	var ok bool
	if pkey, ok = parsedKey.(*rsa.PrivateKey); !ok {
		return nil, ErrNotRSAPrivateKey
	}

	return pkey, nil
}

// DecodeRSAPrivateKeyFromPEMWithPassword parses a PEM-encoded PKCS1 or PKCS8 private key protected with password
func DecodeRSAPrivateKeyFromPEMWithPassword(key []byte, password string) (*rsa.PrivateKey, error) {
	var err error

	// Parse PEM block
	var block *pem.Block
	if block, _ = pem.Decode(key); block == nil {
		return nil, ErrKeyMustBePEMEncoded
	}

	var parsedKey interface{}

	var blockDecrypted []byte
	if blockDecrypted, err = x509.DecryptPEMBlock(block, []byte(password)); err != nil {
		return nil, err
	}

	if parsedKey, err = x509.ParsePKCS1PrivateKey(blockDecrypted); err != nil {
		if parsedKey, err = x509.ParsePKCS8PrivateKey(blockDecrypted); err != nil {
			return nil, err
		}
	}

	var pkey *rsa.PrivateKey
	var ok bool
	if pkey, ok = parsedKey.(*rsa.PrivateKey); !ok {
		return nil, ErrNotRSAPrivateKey
	}

	return pkey, nil
}

// DecodeRSAPublicKeyFromPEM parses a PEM-encoded PKCS1 or PKCS8 public key
func DecodeRSAPublicKeyFromPEM(key []byte) (*rsa.PublicKey, error) {
	var err error

	// Parse PEM block
	var block *pem.Block
	if block, _ = pem.Decode(key); block == nil {
		return nil, ErrKeyMustBePEMEncoded
	}

	// Parse the key
	var parsedKey interface{}
	if parsedKey, err = x509.ParsePKIXPublicKey(block.Bytes); err != nil {
		if cert, err := x509.ParseCertificate(block.Bytes); err == nil {
			parsedKey = cert.PublicKey
		} else {
			return nil, err
		}
	}

	var pkey *rsa.PublicKey
	var ok bool
	if pkey, ok = parsedKey.(*rsa.PublicKey); !ok {
		return nil, ErrNotRSAPublicKey
	}

	return pkey, nil
}

// EncodePrivateKeyToPEM encodes the given private key from RSA to PEM format
func EncodePrivateKeyToPEM(privateKey *rsa.PrivateKey) []byte {
	return pem.EncodeToMemory(&pem.Block{
		Type:    "RSA PRIVATE KEY",
		Headers: nil,
		Bytes:   x509.MarshalPKCS1PrivateKey(privateKey),
	})
}
