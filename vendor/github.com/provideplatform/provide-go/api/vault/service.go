package vault

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/provideplatform/provide-go/api"
	"github.com/provideplatform/provide-go/common"
)

const defaultVaultHost = "vault.provide.services"
const defaultVaultPath = "api/v1"
const defaultVaultScheme = "https"

// Service for the vault api
type Service struct {
	api.Client
}

// InitVaultService convenience method to initialize an `vault.Service` instance
func InitVaultService(token *string) *Service {
	host := defaultVaultHost
	if os.Getenv("VAULT_API_HOST") != "" {
		host = os.Getenv("VAULT_API_HOST")
	}

	path := defaultVaultPath
	if os.Getenv("VAULT_API_PATH") != "" {
		path = os.Getenv("VAULT_API_PATH")
	}

	scheme := defaultVaultScheme
	if os.Getenv("VAULT_API_SCHEME") != "" {
		scheme = os.Getenv("VAULT_API_SCHEME")
	}

	return &Service{
		api.Client{
			Host:   host,
			Path:   path,
			Scheme: scheme,
			Token:  token,
		},
	}
}

// CreateVault on behalf of the given API token
func CreateVault(token string, params map[string]interface{}) (*Vault, error) {
	status, resp, err := InitVaultService(common.StringOrNil(token)).Post("vaults", params)
	if err != nil {
		return nil, err
	}

	if status != 201 {
		return nil, fmt.Errorf("failed to create vault; status: %v; %s", status, resp)
	}

	vlt := &Vault{}
	vltraw, err := json.Marshal(resp)
	if err != nil {
		return nil, fmt.Errorf("failed to create vault; status: %v; %s", status, err.Error())
	}
	err = json.Unmarshal(vltraw, &vlt)

	if err != nil {
		return nil, fmt.Errorf("failed to create vault; status: %v; %s", status, err.Error())
	}

	return vlt, nil
}

// ListVaults retrieves a paginated list of vaults scoped to the given API token
func ListVaults(token string, params map[string]interface{}) ([]*Vault, error) {
	status, resp, err := InitVaultService(common.StringOrNil(token)).Get("vaults", params)
	if err != nil {
		return nil, err
	}

	if status != 200 {
		return nil, fmt.Errorf("failed to fetch vaults; status: %v; %s", status, resp)
	}

	vaults := make([]*Vault, 0)
	for _, item := range resp.([]interface{}) {
		vlt := &Vault{}
		vltraw, err := json.Marshal(item)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch vaults; status: %v; %s", status, err.Error())
		}
		err = json.Unmarshal(vltraw, &vlt)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch vaults; status: %v; %s", status, err.Error())
		}
		vaults = append(vaults, vlt)
	}

	return vaults, nil
}

// ListKeys retrieves a paginated list of vault keys
func ListKeys(token, vaultID string, params map[string]interface{}) ([]*Key, error) {
	uri := fmt.Sprintf("vaults/%s/keys", vaultID)
	status, resp, err := InitVaultService(common.StringOrNil(token)).Get(uri, params)
	if err != nil {
		return nil, err
	}

	if status != 200 {
		return nil, fmt.Errorf("failed to fetch keys; status: %v; %s", status, resp)
	}

	keys := make([]*Key, 0)
	for _, item := range resp.([]interface{}) {
		key := &Key{}
		keyraw, err := json.Marshal(item)
		if err != nil {
			return nil, fmt.Errorf("failed to list vault keys; status: %v; %s", status, err.Error())
		}
		err = json.Unmarshal(keyraw, &key)
		if err != nil {
			return nil, fmt.Errorf("failed to list vault keys; status: %v; %s", status, err.Error())
		}
		keys = append(keys, key)
	}

	return keys, nil
}

// CreateKey creates a new vault key
func CreateKey(token, vaultID string, params map[string]interface{}) (*Key, error) {
	uri := fmt.Sprintf("vaults/%s/keys", vaultID)
	status, resp, err := InitVaultService(common.StringOrNil(token)).Post(uri, params)
	if err != nil {
		return nil, err
	}

	if status != 201 {
		return nil, fmt.Errorf("failed to create vault key; status: %v; %s", status, resp)
	}

	key := &Key{}
	keyraw, err := json.Marshal(resp)
	if err != nil {
		return nil, fmt.Errorf("failed to create vault key; status: %v; %s", status, err.Error())
	}
	err = json.Unmarshal(keyraw, &key)
	if err != nil {
		return nil, fmt.Errorf("failed to create vault key; status: %v; %s", status, err.Error())
	}

	return key, nil
}

// FetchKey fetches a key from the given vault
func FetchKey(token, vaultID, keyID string) (*Key, error) {
	uri := fmt.Sprintf("vaults/%s/keys/%s", vaultID, keyID)
	status, resp, err := InitVaultService(common.StringOrNil(token)).Get(uri, map[string]interface{}{})
	if err != nil {
		return nil, err
	}

	if status != 200 {
		return nil, fmt.Errorf("failed to fetch key; status: %v; %s", status, resp)
	}

	key := &Key{}
	keyraw, err := json.Marshal(resp)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch key; status: %v; %s", status, err.Error())
	}
	err = json.Unmarshal(keyraw, &key)

	if err != nil {
		return nil, fmt.Errorf("failed to fetch key; status: %v; %s", status, err.Error())
	}

	return key, nil
}

// DeriveKey derives a key
func DeriveKey(token, vaultID, keyID string, params map[string]interface{}) (*Key, error) {
	uri := fmt.Sprintf("vaults/%s/keys/%s/derive", vaultID, keyID)
	status, resp, err := InitVaultService(common.StringOrNil(token)).Post(uri, params)
	if err != nil {
		return nil, err
	}

	if status != 201 {
		return nil, fmt.Errorf("failed to derive vault key; status: %v; %s", status, resp)
	}

	// FIXME...
	key := &Key{}
	keyraw, _ := json.Marshal(resp)
	err = json.Unmarshal(keyraw, &key)

	if err != nil {
		return nil, fmt.Errorf("failed to derive key; status: %v; %s", status, err.Error())
	}

	return key, nil
}

// DeleteKey deletes a key
func DeleteKey(token, vaultID, keyID string) error {
	uri := fmt.Sprintf("vaults/%s/keys/%s", vaultID, keyID)
	status, resp, err := InitVaultService(common.StringOrNil(token)).Delete(uri)
	if err != nil {
		return err
	}

	if status != 204 {
		return fmt.Errorf("failed to delete key; status: %v; %s", status, resp)
	}

	return nil
}

// SignMessage signs a message with the given key
func SignMessage(token, vaultID, keyID, msg string, opts map[string]interface{}) (*SignResponse, error) {
	uri := fmt.Sprintf("vaults/%s/keys/%s/sign", vaultID, keyID)
	status, resp, err := InitVaultService(common.StringOrNil(token)).Post(uri, map[string]interface{}{
		"message": msg,
		"options": opts,
	})
	if err != nil {
		return nil, err
	}

	if status != 201 {
		return nil, fmt.Errorf("failed to sign message with key; status: %v; %s", status, resp)
	}

	r := &SignResponse{}
	raw, err := json.Marshal(resp)
	if err != nil {
		return nil, fmt.Errorf("failed to sign message with key; status: %v; %s", status, err.Error())
	}
	err = json.Unmarshal(raw, &r)

	if err != nil {
		return nil, fmt.Errorf("failed to sign message with key; status: %v; %s", status, err.Error())
	}

	return r, nil
}

// VerifySignature verifies a signature
func VerifySignature(token, vaultID, keyID, msg, sig string, opts map[string]interface{}) (*VerifyResponse, error) {
	uri := fmt.Sprintf("vaults/%s/keys/%s/verify", vaultID, keyID)
	status, resp, err := InitVaultService(common.StringOrNil(token)).Post(uri, map[string]interface{}{
		"message":   msg,
		"signature": sig,
		"options":   opts,
	})
	if err != nil {
		return nil, err
	}

	if status != 200 {
		return nil, fmt.Errorf("failed to verify message signature; status: %v; %s", status, resp)
	}

	r := &VerifyResponse{}
	raw, err := json.Marshal(resp)
	if err != nil {
		return nil, fmt.Errorf("failed to verify message signature; status: %v; %s", status, err.Error())
	}

	err = json.Unmarshal(raw, &r)

	if err != nil {
		return nil, fmt.Errorf("failed to verify message signature; status: %v; %s", status, err.Error())
	}

	return r, nil
}

// ListSecrets retrieves a paginated list of secrets in the vault
func ListSecrets(token, vaultID string, params map[string]interface{}) ([]*Secret, error) {
	uri := fmt.Sprintf("vaults/%s/secrets", vaultID)
	status, resp, err := InitVaultService(common.StringOrNil(token)).Get(uri, params)
	if err != nil {
		return nil, err
	}

	if status != 200 {
		return nil, fmt.Errorf("failed to fetch secrets; status: %v; %s", status, resp)
	}

	secrets := make([]*Secret, 0)
	for _, item := range resp.([]interface{}) {
		secret := &Secret{}
		secretraw, err := json.Marshal(item)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch secrets; status: %v; %s", status, resp)
		}
		err = json.Unmarshal(secretraw, &secret)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch secrets; status: %v; %s", status, resp)
		}
		secrets = append(secrets, secret)
	}

	return secrets, nil
}

// CreateSecret stores a new secret in the vault
func CreateSecret(token, vaultID, value, name, description, secretType string) (*Secret, error) {
	uri := fmt.Sprintf("vaults/%s/secrets", vaultID)
	status, resp, err := InitVaultService(common.StringOrNil(token)).Post(uri, map[string]interface{}{
		"name":        name,
		"description": description,
		"type":        secretType,
		"value":       value,
	})
	if err != nil {
		return nil, err
	}

	if status != 201 {
		return nil, fmt.Errorf("failed to create secret; status: %v; %s", status, resp)
	}

	secret := &Secret{}
	secretraw, err := json.Marshal(resp)
	if err != nil {
		return nil, fmt.Errorf("failed to create secret; status: %v; %s", status, err.Error())
	}
	err = json.Unmarshal(secretraw, &secret)

	if err != nil {
		return nil, fmt.Errorf("failed to create secret; status: %v; %s", status, err.Error())
	}

	return secret, nil
}

// FetchSecret fetches a secret from the given vault
func FetchSecret(token, vaultID, secretID string, params map[string]interface{}) (*Secret, error) {
	uri := fmt.Sprintf("vaults/%s/secrets/%s", vaultID, secretID)
	status, resp, err := InitVaultService(common.StringOrNil(token)).Get(uri, params)
	if err != nil {
		return nil, err
	}

	if status != 200 {
		return nil, fmt.Errorf("failed to fetch secret; status: %v; %s", status, resp)
	}

	secret := &Secret{}
	secretraw, err := json.Marshal(resp)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch secret; status: %v; %s", status, err.Error())
	}
	err = json.Unmarshal(secretraw, &secret)

	if err != nil {
		return nil, fmt.Errorf("failed to fetch secret; status: %v; %s", status, err.Error())
	}

	return secret, nil
}

// DeleteSecret deletes a secret from the vault
func DeleteSecret(token, vaultID, secretID string) error {
	uri := fmt.Sprintf("vaults/%s/secrets/%s", vaultID, secretID)
	status, resp, err := InitVaultService(common.StringOrNil(token)).Delete(uri)
	if err != nil {
		return err
	}

	if status != 204 {
		return fmt.Errorf("failed to delete secret; status: %v; %s", status, resp)
	}

	return nil
}

// Encrypt encrypts provided data with a key from the vault and a randomly generated nonce
func Encrypt(token, vaultID, keyID, data string) (*EncryptDecryptRequestResponse, error) {
	uri := fmt.Sprintf("vaults/%s/keys/%s/encrypt", vaultID, keyID)
	status, resp, err := InitVaultService(common.StringOrNil(token)).Post(uri, map[string]interface{}{
		"data": data,
	})
	if err != nil {
		return nil, err
	}

	if status != 200 {
		return nil, fmt.Errorf("failed to encrypt payload; status: %v; %s", status, resp)
	}

	r := &EncryptDecryptRequestResponse{}
	raw, err := json.Marshal(resp)
	if err != nil {
		return nil, fmt.Errorf("failed to encrypt payload; status: %v; %s", status, err.Error())
	}
	err = json.Unmarshal(raw, &r)
	if err != nil {
		return nil, fmt.Errorf("failed to encrypt payload; status: %v; %s", status, err.Error())
	}

	return r, nil
}

// EncryptWithNonce encrypts provided data with a key from the vault and provided nonce
func EncryptWithNonce(token, vaultID, keyID, data, nonce string) (*EncryptDecryptRequestResponse, error) {
	uri := fmt.Sprintf("vaults/%s/keys/%s/encrypt", vaultID, keyID)
	status, resp, err := InitVaultService(common.StringOrNil(token)).Post(uri, map[string]interface{}{
		"data":  data,
		"nonce": nonce,
	})
	if err != nil {
		return nil, err
	}

	if status != 200 {
		return nil, fmt.Errorf("failed to encrypt payload; status: %v; %s", status, resp)
	}

	r := &EncryptDecryptRequestResponse{}
	raw, err := json.Marshal(resp)
	if err != nil {
		return nil, fmt.Errorf("failed to encrypt payload; status: %v; %s", status, err.Error())
	}
	err = json.Unmarshal(raw, &r)
	if err != nil {
		return nil, fmt.Errorf("failed to encrypt payload; status: %v; %s", status, err.Error())
	}

	return r, nil
}

// Decrypt decrypts provided encrypted data with a key from the vault
func Decrypt(token, vaultID, keyID string, params map[string]interface{}) (*EncryptDecryptRequestResponse, error) {
	uri := fmt.Sprintf("vaults/%s/keys/%s/decrypt", vaultID, keyID)
	status, resp, err := InitVaultService(common.StringOrNil(token)).Post(uri, params)
	if err != nil {
		return nil, err
	}

	if status != 200 {
		return nil, fmt.Errorf("failed to decrypt payload; status: %v; %s", status, resp)
	}

	r := &EncryptDecryptRequestResponse{}
	raw, err := json.Marshal(resp)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt payload; status: %v; %s", status, err.Error())
	}
	err = json.Unmarshal(raw, &r)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt payload; status: %v; %s", status, err.Error())
	}

	return r, nil
}

// Seal seals the vault to disable decryption of vault, key and secret material
func Seal(token string, params map[string]interface{}) (*SealUnsealRequestResponse, error) {
	uri := fmt.Sprintf("seal")
	status, resp, err := InitVaultService(common.StringOrNil(token)).Post(uri, params)
	if err != nil {
		return nil, err
	}

	if status != 204 {
		return nil, fmt.Errorf("failed to seal vault; status %v, %s", status, resp)
	}

	return nil, nil
}

// Unseal unseals the vault to enable decryption of vault, key and secret material
func Unseal(token *string, params map[string]interface{}) (*SealUnsealRequestResponse, error) {
	status, resp, err := InitVaultService(token).Post("unseal", params)
	if err != nil {
		return nil, err
	}

	if status != 204 {
		return nil, fmt.Errorf("failed to unseal vault; status %v, %s", status, resp)
	}

	return nil, nil
}

// GenerateSeal returns a valid unsealing key used to encrypt vault master keys
func GenerateSeal(token string, params map[string]interface{}) (*SealUnsealRequestResponse, error) {
	uri := fmt.Sprintf("unsealerkey")
	status, resp, err := InitVaultService(common.StringOrNil(token)).Post(uri, params)
	if err != nil {
		return nil, err
	}

	if status != 201 {
		return nil, fmt.Errorf("failed to generate vault seal/unseal key; status: %v; %s", status, resp)
	}

	r := &SealUnsealRequestResponse{}
	raw, err := json.Marshal(resp)
	if err != nil {
		return nil, fmt.Errorf("failed to generate vault seal/unseal key; status: %v; %s", status, err.Error())
	}
	err = json.Unmarshal(raw, &r)
	if err != nil {
		return nil, fmt.Errorf("failed to generate vault seal/unseal key; status: %v; %s", status, err.Error())
	}
	return r, nil
}

// AggregateSignatures aggregates BLS signatures into a single BLS signature
func AggregateSignatures(token *string, params map[string]interface{}) (*BLSAggregateRequestResponse, error) {
	uri := fmt.Sprintf("bls/aggregate")
	status, resp, err := InitVaultService(token).Post(uri, params)

	if err != nil {
		return nil, err
	}

	if status != 201 {
		return nil, fmt.Errorf("failed to aggregate bls signatures. status: %v", status)
	}

	response := &BLSAggregateRequestResponse{}
	responseRaw, _ := json.Marshal(resp)
	err = json.Unmarshal(responseRaw, &response)
	if err != nil {
		return nil, fmt.Errorf("failed to get aggregate response. status: %v; %s", status, err.Error())
	}

	return response, nil
}

// VerifyAggregateSignatures verifies a bls signature
func VerifyAggregateSignatures(token *string, params map[string]interface{}) (*VerifyResponse, error) {
	uri := fmt.Sprintf("bls/verify")
	status, resp, err := InitVaultService(token).Post(uri, params)

	if err != nil {
		return nil, err
	}

	if status != 200 {
		return nil, fmt.Errorf("failed to aggregate bls signatures. status: %v", status)
	}

	response := &VerifyResponse{}
	responseRaw, _ := json.Marshal(resp)
	err = json.Unmarshal(responseRaw, &response)
	if err != nil {
		return nil, fmt.Errorf("failed to get aggregate signature verification response. status: %v; %s", status, err.Error())
	}

	return response, nil
}

// VerifyDetachedSignature verifies a signature generated by a key external to vault
func VerifyDetachedSignature(token, spec, msg, sig, publicKey string, opts map[string]interface{}) (*VerifyResponse, error) {
	uri := fmt.Sprintf("verify")
	status, resp, err := InitVaultService(common.StringOrNil(token)).Post(uri, map[string]interface{}{
		"spec":       spec,
		"public_key": publicKey,
		"message":    msg,
		"signature":  sig,
		"options":    opts,
	})
	if err != nil {
		return nil, err
	}

	if status != 200 {
		return nil, fmt.Errorf("failed to verify message signature; status: %v; %s", status, resp)
	}

	r := &VerifyResponse{}
	raw, err := json.Marshal(resp)
	if err != nil {
		return nil, fmt.Errorf("failed to verify message signature; status: %v; %s", status, err.Error())
	}

	err = json.Unmarshal(raw, &r)

	if err != nil {
		return nil, fmt.Errorf("failed to verify message signature; status: %v; %s", status, err.Error())
	}

	return r, nil
}
