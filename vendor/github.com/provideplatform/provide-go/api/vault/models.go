package vault

import (
	uuid "github.com/kthomas/go.uuid"
	"github.com/provideplatform/provide-go/api"
)

// MaxHDIteration is the maximum HD account index
const MaxHDIteration = 4294967295

// KeyTypeAsymmetric asymmetric key type
const KeyTypeAsymmetric = "asymmetric"

// KeyTypeSymmetric symmetric key type
const KeyTypeSymmetric = "symmetric"

// KeyUsageEncryptDecrypt encrypt/decrypt usage
const KeyUsageEncryptDecrypt = "encrypt/decrypt"

// KeyUsageSignVerify sign/verify usage
const KeyUsageSignVerify = "sign/verify"

// KeySpecAES256GCM AES-256-GCM key spec
const KeySpecAES256GCM = "AES-256-GCM"

// KeySpecChaCha20 ChaCha20 key spec
const KeySpecChaCha20 = "ChaCha20"

// KeySpecECCBabyJubJub babyJubJub key spec
const KeySpecECCBabyJubJub = "babyJubJub"

// KeySpecECCBIP39 BIP39 key spec
const KeySpecECCBIP39 = "BIP39"

// KeySpecECCC25519 C25519 key spec
const KeySpecECCC25519 = "C25519"

// KeySpecECCEd25519 Ed25519 key spec
const KeySpecECCEd25519 = "Ed25519"

// KeySpecECCSecp256k1 secp256k1 key spec
const KeySpecECCSecp256k1 = "secp256k1"

// NonceSizeSymmetric chacha20 & aes256 encrypt/decrypt nonce size
const NonceSizeSymmetric = 12

// KeySpecRSA2048 rsa 2048 key spec
const KeySpecRSA2048 = "RSA-2048"

// KeyBits2048 is the bit length for 2048-bit keys
const KeyBits2048 = 2048

// KeyBits3072 is the bit length for 3072-bit keys
const KeyBits3072 = 3072

// KeyBits4096 is the bit length for 4096-bit keys
const KeyBits4096 = 4096

// KeySpecRSA3072 rsa 3072 key spec
const KeySpecRSA3072 = "RSA-3072"

// KeySpecRSA4096 rsa 4096 key spec
const KeySpecRSA4096 = "RSA-4096"

// Vault provides secure key management
type Vault struct {
	api.Model
	Name        *string `json:"name"`
	Description *string `json:"description"`
}

// Key represents a symmetric or asymmetric signing key
type Key struct {
	api.Model
	VaultID     *uuid.UUID `json:"vault_id"`
	Type        *string    `json:"type"` // symmetric or asymmetric
	Usage       *string    `json:"usage"`
	Spec        *string    `json:"spec"`
	Name        *string    `json:"name"`
	Description *string    `json:"description"`

	// these fields are only populated for ephemeral keys
	Ephemeral  *bool   `json:"ephemeral,omitempty"`
	PrivateKey *string `json:"private_key,omitempty"`
	Seed       *string `json:"seed,omitempty"`

	Address          *string `json:"address,omitempty"`
	HDDerivationPath *string `json:"hd_derivation_path,omitempty"`
	PublicKey        *string `json:"public_key,omitempty"`
}

// Secret represents a string, encrypted by the vault master key
type Secret struct {
	api.Model
	VaultID     *uuid.UUID `json:"vault_id"`
	Type        *string    `json:"type"` // arbitrary secret type
	Name        *string    `json:"name"`
	Description *string    `json:"description"`
	Value       *string    `json:"value,omitempty"`
}

// EncryptDecryptRequestResponse contains the data (i.e., encrypted or decrypted) and an optional nonce
type EncryptDecryptRequestResponse struct {
	Data  string  `json:"data"`
	Nonce *string `json:"nonce,omitempty"`
}

// SignRequest contains a message to be signed
type SignRequest struct {
	Message string `json:"message"`
}

// SignResponse contains the signature for the message
type SignResponse struct {
	Signature      *string `json:"signature,omitempty"`
	Address        *string `json:"address,omitempty"`
	DerivationPath *string `json:"hd_derivation_path,omitempty"`
}

// VerifyRequest contains the message and signature for verification
type VerifyRequest struct {
	Message   string `json:"message"`
	Signature string `json:"signature"`
}

// VerifyResponse contains a flag indicating if the signature was verified
type VerifyResponse struct {
	Verified bool `json:"verified"`
}

// BLSAggregateRequestResponse provides the BLS sig information to aggregate n BLS signatures into one BLS signature
type BLSAggregateRequestResponse struct {
	Signatures         []*string `json:"signatures,omitempty"`
	AggregateSignature *string   `json:"aggregate_signature,omitempty"`
}

// SealUnsealRequestResponse provides the unseal information
type SealUnsealRequestResponse struct {
	UnsealerKey    *string `json:"key,omitempty"`
	ValidationHash *string `json:"validation_hash,omitempty"`
}
