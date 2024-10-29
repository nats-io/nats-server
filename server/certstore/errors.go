package certstore

import (
	"errors"
)

var (
	// ErrBadCryptoStoreProvider represents inablity to establish link with a certificate store
	ErrBadCryptoStoreProvider = errors.New("unable to open certificate store or store not available")

	// ErrBadRSAHashAlgorithm represents a bad or unsupported RSA hash algorithm
	ErrBadRSAHashAlgorithm = errors.New("unsupported RSA hash algorithm")

	// ErrBadSigningAlgorithm represents a bad or unsupported signing algorithm
	ErrBadSigningAlgorithm = errors.New("unsupported signing algorithm")

	// ErrStoreRSASigningError represents an error returned from store during RSA signature
	ErrStoreRSASigningError = errors.New("unable to obtain RSA signature from store")

	// ErrStoreECDSASigningError represents an error returned from store during ECDSA signature
	ErrStoreECDSASigningError = errors.New("unable to obtain ECDSA signature from store")

	// ErrNoPrivateKeyStoreRef represents an error getting a handle to a private key in store
	ErrNoPrivateKeyStoreRef = errors.New("unable to obtain private key handle from store")

	// ErrExtractingPrivateKeyMetadata represents a family of errors extracting metadata about the private key in store
	ErrExtractingPrivateKeyMetadata = errors.New("unable to extract private key metadata")

	// ErrExtractingECCPublicKey represents an error exporting ECC-type public key from store
	ErrExtractingECCPublicKey = errors.New("unable to extract ECC public key from store")

	// ErrExtractingRSAPublicKey represents an error exporting RSA-type public key from store
	ErrExtractingRSAPublicKey = errors.New("unable to extract RSA public key from store")

	// ErrExtractingPublicKey represents a general error exporting public key from store
	ErrExtractingPublicKey = errors.New("unable to extract public key from store")

	// ErrBadPublicKeyAlgorithm represents a bad or unsupported public key algorithm
	ErrBadPublicKeyAlgorithm = errors.New("unsupported public key algorithm")

	// ErrExtractPropertyFromKey represents a general failure to extract a metadata property field
	ErrExtractPropertyFromKey = errors.New("unable to extract property from key")

	// ErrBadECCCurveName represents an ECC signature curve name that is bad or unsupported
	ErrBadECCCurveName = errors.New("unsupported ECC curve name")

	// ErrFailedCertSearch represents not able to find certificate in store
	ErrFailedCertSearch = errors.New("unable to find certificate in store")

	// ErrFailedX509Extract represents not being able to extract x509 certificate from found cert in store
	ErrFailedX509Extract = errors.New("unable to extract x509 from certificate")

	// ErrBadMatchByType represents unknown CERT_MATCH_BY passed
	ErrBadMatchByType = errors.New("cert match by type not implemented")

	// ErrBadCertStore represents unknown CERT_STORE passed
	ErrBadCertStore = errors.New("cert store type not implemented")

	// ErrConflictCertFileAndStore represents ambiguous configuration of both file and store
	ErrConflictCertFileAndStore = errors.New("'cert_file' and 'cert_store' may not both be configured")

	// ErrBadCertStoreField represents malformed cert_store option
	ErrBadCertStoreField = errors.New("expected 'cert_store' to be a valid non-empty string")

	// ErrBadCertMatchByField represents malformed cert_match_by option
	ErrBadCertMatchByField = errors.New("expected 'cert_match_by' to be a valid non-empty string")

	// ErrBadCertMatchField represents malformed cert_match option
	ErrBadCertMatchField = errors.New("expected 'cert_match' to be a valid non-empty string")

	// ErrBadCaCertMatchField represents malformed cert_match option
	ErrBadCaCertMatchField = errors.New("expected 'ca_certs_match' to be a valid non-empty string array")

	// ErrBadCertMatchSkipInvalidField represents malformed cert_match_skip_invalid option
	ErrBadCertMatchSkipInvalidField = errors.New("expected 'cert_match_skip_invalid' to be a boolean")

	// ErrOSNotCompatCertStore represents cert_store passed that exists but is not valid on current OS
	ErrOSNotCompatCertStore = errors.New("cert_store not compatible with current operating system")
)
