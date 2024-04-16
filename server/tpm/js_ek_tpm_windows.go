// Copyright 2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build windows

package tpm

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/google/go-tpm/legacy/tpm2"
	"github.com/google/go-tpm/tpmutil"
	"github.com/nats-io/nkeys"
)

var (
	// Version of the NATS TPM JS implmentation
	JsKeyTPMVersion = 1
)

// How this works:
// Create a Storage Root Key (SRK) in the TPM.
// If existing JS Encryption keys do not exist on disk.
// 	  - Create a JetStream encryption key (js key) and seal it to the SRK
//      using a provided js encryption key password.
// 	  - Save the public and private blobs to a file on disk.
//    - Return the new js encryption key (the private portion of the nkey)
// Otherwise (keys exist on disk)
//    - Read the public and private blobs from disk
//    - Load them into the TPM
//    - Unseal the js key using the TPM, and the provided js encryption keys password.
//
// Note: a SRK password for the SRK is supported but not tested here.

// Gets/Regenerates the Storage Root Key (SRK) from the TPM. Caller MUST flush this handle when done.
func regenerateSRK(rwc io.ReadWriteCloser, srkPassword string) (tpmutil.Handle, error) {
	// Default EK template defined in:
	// https://trustedcomputinggroup.org/wp-content/uploads/Credential_Profile_EK_V2.0_R14_published.pdf
	// Shared SRK template based off of EK template and specified in:
	// https://trustedcomputinggroup.org/wp-content/uploads/TCG-TPM-v2.0-Provisioning-Guidance-Published-v1r1.pdf
	srkTemplate := tpm2.Public{
		Type:       tpm2.AlgRSA,
		NameAlg:    tpm2.AlgSHA256,
		Attributes: tpm2.FlagFixedTPM | tpm2.FlagFixedParent | tpm2.FlagSensitiveDataOrigin | tpm2.FlagUserWithAuth | tpm2.FlagRestricted | tpm2.FlagDecrypt | tpm2.FlagNoDA,
		AuthPolicy: nil,
		// We must use RSA 2048 for the intel TSS2 stack
		RSAParameters: &tpm2.RSAParams{
			Symmetric: &tpm2.SymScheme{
				Alg:     tpm2.AlgAES,
				KeyBits: 128,
				Mode:    tpm2.AlgCFB,
			},
			KeyBits:    2048,
			ModulusRaw: make([]byte, 256),
		},
	}
	// Create the parent key against which to seal the data
	srkHandle, _, err := tpm2.CreatePrimary(rwc, tpm2.HandleOwner, tpm2.PCRSelection{}, "", srkPassword, srkTemplate)
	return srkHandle, err
}

type natsTPMPersistedKeys struct {
	Version    int    `json:"version"`
	PrivateKey []byte `json:"private_key"`
	PublicKey  []byte `json:"public_key"`
}

// Writes the private and public blobs to disk in a single file. If the directory does
// not exist, it will be created. If the file already exists it will be overwritten.
func writeTPMKeysToFile(filename string, privateBlob []byte, publicBlob []byte) error {
	keyDir := filepath.Dir(filename)
	if err := os.MkdirAll(keyDir, 0750); err != nil {
		return fmt.Errorf("unable to create/access directory %q: %v", keyDir, err)
	}

	// Create a new set of persisted keys. Note that the private key doesn't necessarily
	// need to be protected as the TPM password is required to use unseal, although it's
	// a good idea to put this in a secure location accessible to the server.
	tpmKeys := natsTPMPersistedKeys{
		Version:    JsKeyTPMVersion,
		PrivateKey: make([]byte, base64.StdEncoding.EncodedLen(len(privateBlob))),
		PublicKey:  make([]byte, base64.StdEncoding.EncodedLen(len(publicBlob))),
	}
	base64.StdEncoding.Encode(tpmKeys.PrivateKey, privateBlob)
	base64.StdEncoding.Encode(tpmKeys.PublicKey, publicBlob)
	// Convert to JSON
	keysJSON, err := json.Marshal(tpmKeys)
	if err != nil {
		return fmt.Errorf("unable to marshal keys to JSON: %v", err)
	}
	// Write the JSON to a file
	if err := os.WriteFile(filename, keysJSON, 0640); err != nil {
		return fmt.Errorf("unable to write keys file to %q: %v", filename, err)
	}
	return nil
}

// Reads the private and public blobs from a single file. If the file does not exist,
// or the file cannot be read and the keys decoded, an error is returned.
func readTPMKeysFromFile(filename string) ([]byte, []byte, error) {
	keysJSON, err := os.ReadFile(filename)
	if err != nil {
		return nil, nil, err
	}

	var tpmKeys natsTPMPersistedKeys
	if err := json.Unmarshal(keysJSON, &tpmKeys); err != nil {
		return nil, nil, fmt.Errorf("unable to unmarshal TPM file keys JSON from %s: %v", filename, err)
	}

	// Placeholder for future-proofing. Here is where we would
	// check the current version against tpmKeys.Version and
	// handle any changes.

	// Base64 decode the private and public blobs.
	privateBlob := make([]byte, base64.StdEncoding.DecodedLen(len(tpmKeys.PrivateKey)))
	publicBlob := make([]byte, base64.StdEncoding.DecodedLen(len(tpmKeys.PublicKey)))
	prn, err := base64.StdEncoding.Decode(privateBlob, tpmKeys.PrivateKey)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to decode privateBlob from base64: %v", err)
	}
	pun, err := base64.StdEncoding.Decode(publicBlob, tpmKeys.PublicKey)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to decode publicBlob from base64: %v", err)
	}
	return publicBlob[:pun], privateBlob[:prn], nil
}

// Creates a new JetStream encryption key, seals it to the TPM, and saves the public and
// private blobs to disk in a JSON encoded file. The key is returned as a string.
func createAndSealJsEncryptionKey(rwc io.ReadWriteCloser, srkHandle tpmutil.Handle, srkPassword, jsKeyFile, jsKeyPassword string, pcr int) (string, error) {
	// Get the authorization policy that will protect the data to be sealed
	sessHandle, policy, err := policyPCRPasswordSession(rwc, pcr)
	if err != nil {
		return "", fmt.Errorf("unable to get policy: %v", err)
	}
	if err := tpm2.FlushContext(rwc, sessHandle); err != nil {
		return "", fmt.Errorf("unable to flush session: %v", err)
	}
	// Seal the data to the parent key and the policy
	user, err := nkeys.CreateUser()
	if err != nil {
		return "", fmt.Errorf("unable to create seed: %v", err)
	}
	// We'll use the seed to represent the encryption key.
	jsStoreKey, err := user.Seed()
	if err != nil {
		return "", fmt.Errorf("unable to get seed: %v", err)
	}
	privateArea, publicArea, err := tpm2.Seal(rwc, srkHandle, srkPassword, jsKeyPassword, policy, jsStoreKey)
	if err != nil {
		return "", fmt.Errorf("unable to seal data: %v", err)
	}
	err = writeTPMKeysToFile(jsKeyFile, privateArea, publicArea)
	if err != nil {
		return "", fmt.Errorf("unable to write key file: %v", err)
	}
	return string(jsStoreKey), nil
}

// Unseals the JetStream encryption key from the TPM with the provided keys.
// The key is returned as a string.
func unsealJsEncrpytionKey(rwc io.ReadWriteCloser, pcr int, srkHandle tpmutil.Handle, srkPassword, objectPassword string, publicBlob, privateBlob []byte) (string, error) {
	// Load the public/private blobs into the TPM for decryption.
	objectHandle, _, err := tpm2.Load(rwc, srkHandle, srkPassword, publicBlob, privateBlob)
	if err != nil {
		return "", fmt.Errorf("unable to load data: %v", err)
	}
	defer tpm2.FlushContext(rwc, objectHandle)

	// Create the authorization session with TPM.
	sessHandle, _, err := policyPCRPasswordSession(rwc, pcr)
	if err != nil {
		return "", fmt.Errorf("unable to get auth session: %v", err)
	}
	defer func() {
		tpm2.FlushContext(rwc, sessHandle)
	}()
	// Unseal the data we've loaded into the TPM with the object (js key) password.
	unsealedData, err := tpm2.UnsealWithSession(rwc, sessHandle, objectHandle, objectPassword)
	if err != nil {
		return "", fmt.Errorf("unable to unseal data: %v", err)
	}
	return string(unsealedData), nil
}

// Returns session handle and policy digest.
func policyPCRPasswordSession(rwc io.ReadWriteCloser, pcr int) (sessHandle tpmutil.Handle, policy []byte, retErr error) {
	sessHandle, _, err := tpm2.StartAuthSession(
		rwc,
		tpm2.HandleNull,  /*tpmKey*/
		tpm2.HandleNull,  /*bindKey*/
		make([]byte, 16), /*nonceCaller*/
		nil,              /*secret*/
		tpm2.SessionPolicy,
		tpm2.AlgNull,
		tpm2.AlgSHA256)
	if err != nil {
		return tpm2.HandleNull, nil, fmt.Errorf("unable to start session: %v", err)
	}
	defer func() {
		if sessHandle != tpm2.HandleNull && err != nil {
			if err := tpm2.FlushContext(rwc, sessHandle); err != nil {
				retErr = fmt.Errorf("%v\nunable to flush session: %v", retErr, err)
			}
		}
	}()

	pcrSelection := tpm2.PCRSelection{
		Hash: tpm2.AlgSHA256,
		PCRs: []int{pcr},
	}
	if err := tpm2.PolicyPCR(rwc, sessHandle, nil, pcrSelection); err != nil {
		return sessHandle, nil, fmt.Errorf("unable to bind PCRs to auth policy: %v", err)
	}
	if err := tpm2.PolicyPassword(rwc, sessHandle); err != nil {
		return sessHandle, nil, fmt.Errorf("unable to require password for auth policy: %v", err)
	}
	policy, err = tpm2.PolicyGetDigest(rwc, sessHandle)
	if err != nil {
		return sessHandle, nil, fmt.Errorf("unable to get policy digest: %v", err)
	}
	return sessHandle, policy, nil
}

// LoadJetStreamEncryptionKeyFromTPM loads the JetStream encryption key from the TPM.
// If the keyfile does not exist, a key will be created and sealed. Public and private blobs
// used to decrypt the key in future sessions will be saved to disk in the file provided.
// The key will be unsealed and returned only with the correct password and PCR value.
func LoadJetStreamEncryptionKeyFromTPM(srkPassword, jsKeyFile, jsKeyPassword string, pcr int) (string, error) {
	rwc, err := tpm2.OpenTPM()
	if err != nil {
		return "", fmt.Errorf("could not open the TPM: %v", err)
	}
	defer rwc.Close()

	// Load the key from the TPM
	srkHandle, err := regenerateSRK(rwc, srkPassword)
	defer func() {
		tpm2.FlushContext(rwc, srkHandle)
	}()
	if err != nil {
		return "", fmt.Errorf("unable to regenerate SRK from the TPM: %v", err)
	}
	// Read the keys from the key file. If the filed doesn't exist it means we need to create
	// a new js encrytpion key.
	publicBlob, privateBlob, err := readTPMKeysFromFile(jsKeyFile)
	if err != nil {
		if os.IsNotExist(err) {
			jsek, err := createAndSealJsEncryptionKey(rwc, srkHandle, srkPassword, jsKeyFile, jsKeyPassword, pcr)
			if err != nil {
				return "", fmt.Errorf("unable to generate new key from the TPM: %v", err)
			}
			// we've created and sealed the JS Encryption key, now we just return it.
			return jsek, nil
		}
		return "", fmt.Errorf("unable to load key from TPM: %v", err)
	}

	// Unseal the JetStream encryption key using the TPM.
	jsek, err := unsealJsEncrpytionKey(rwc, pcr, srkHandle, srkPassword, jsKeyPassword, publicBlob, privateBlob)
	if err != nil {
		return "", fmt.Errorf("unable to unseal key from the TPM: %v", err)
	}
	return jsek, nil
}
