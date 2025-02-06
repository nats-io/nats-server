// Copyright 2022-2024 The NATS Authors
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

package certstore

import (
	"crypto"
	"crypto/x509"
	"io"
	"runtime"
	"strings"
)

type StoreType int

const MATCHBYEMPTY = 0
const STOREEMPTY = 0

const (
	windowsCurrentUser StoreType = iota + 1
	windowsLocalMachine
)

var StoreMap = map[string]StoreType{
	"windowscurrentuser":  windowsCurrentUser,
	"windowslocalmachine": windowsLocalMachine,
}

var StoreOSMap = map[StoreType]string{
	windowsCurrentUser:  "windows",
	windowsLocalMachine: "windows",
}

type MatchByType int

const (
	matchByIssuer MatchByType = iota + 1
	matchBySubject
	matchByThumbprint
)

var MatchByMap = map[string]MatchByType{
	"issuer":     matchByIssuer,
	"subject":    matchBySubject,
	"thumbprint": matchByThumbprint,
}

var Usage = `
In place of cert_file and key_file you may use the windows certificate store:

    tls {
        cert_store:     "WindowsCurrentUser"
        cert_match_by:  "Subject"
        cert_match:     "MyServer123"
    }
`

func ParseCertStore(certStore string) (StoreType, error) {
	certStoreType, exists := StoreMap[strings.ToLower(certStore)]
	if !exists {
		return 0, ErrBadCertStore
	}
	validOS, exists := StoreOSMap[certStoreType]
	if !exists || validOS != runtime.GOOS {
		return 0, ErrOSNotCompatCertStore
	}
	return certStoreType, nil
}

func ParseCertMatchBy(certMatchBy string) (MatchByType, error) {
	certMatchByType, exists := MatchByMap[strings.ToLower(certMatchBy)]
	if !exists {
		return 0, ErrBadMatchByType
	}
	return certMatchByType, nil
}

func GetLeafIssuer(leaf *x509.Certificate, vOpts x509.VerifyOptions) (issuer *x509.Certificate) {
	chains, err := leaf.Verify(vOpts)
	if err != nil || len(chains) == 0 {
		issuer = nil
	} else {
		issuer = chains[0][1]
	}
	return
}

// credential provides access to a public key and is a crypto.Signer.
type credential interface {
	// Public returns the public key corresponding to the leaf certificate.
	Public() crypto.PublicKey
	// Sign signs digest with the private key.
	Sign(rand io.Reader, digest []byte, opts crypto.SignerOpts) (signature []byte, err error)
}
