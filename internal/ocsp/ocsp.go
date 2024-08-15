// Copyright 2019-2024 The NATS Authors
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

package testhelper

import (
	"crypto"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/crypto/ocsp"
)

const (
	defaultResponseTTL = 4 * time.Second
	defaultAddress     = "127.0.0.1:8888"
)

func NewOCSPResponderCustomAddress(t *testing.T, issuerCertPEM, issuerKeyPEM string, addr string) *http.Server {
	t.Helper()
	return NewOCSPResponderBase(t, issuerCertPEM, issuerCertPEM, issuerKeyPEM, false, addr, defaultResponseTTL, "")
}

func NewOCSPResponder(t *testing.T, issuerCertPEM, issuerKeyPEM string) *http.Server {
	t.Helper()
	return NewOCSPResponderBase(t, issuerCertPEM, issuerCertPEM, issuerKeyPEM, false, defaultAddress, defaultResponseTTL, "")
}

func NewOCSPResponderDesignatedCustomAddress(t *testing.T, issuerCertPEM, respCertPEM, respKeyPEM string, addr string) *http.Server {
	t.Helper()
	return NewOCSPResponderBase(t, issuerCertPEM, respCertPEM, respKeyPEM, true, addr, defaultResponseTTL, "")
}

func NewOCSPResponderPreferringHTTPMethod(t *testing.T, issuerCertPEM, issuerKeyPEM, method string) *http.Server {
	t.Helper()
	return NewOCSPResponderBase(t, issuerCertPEM, issuerCertPEM, issuerKeyPEM, false, defaultAddress, defaultResponseTTL, method)
}

func NewOCSPResponderCustomTimeout(t *testing.T, issuerCertPEM, issuerKeyPEM string, responseTTL time.Duration) *http.Server {
	t.Helper()
	return NewOCSPResponderBase(t, issuerCertPEM, issuerCertPEM, issuerKeyPEM, false, defaultAddress, responseTTL, "")
}

func NewOCSPResponderBase(t *testing.T, issuerCertPEM, respCertPEM, respKeyPEM string, embed bool, addr string, responseTTL time.Duration, method string) *http.Server {
	t.Helper()
	var mu sync.Mutex
	status := make(map[string]int)

	issuerCert := parseCertPEM(t, issuerCertPEM)
	respCert := parseCertPEM(t, respCertPEM)
	respKey := parseKeyPEM(t, respKeyPEM)

	mux := http.NewServeMux()
	// The "/statuses/" endpoint is for directly setting a key-value pair in
	// the CA's status database.
	mux.HandleFunc("/statuses/", func(rw http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		key := r.URL.Path[len("/statuses/"):]
		switch r.Method {
		case "GET":
			mu.Lock()
			n, ok := status[key]
			if !ok {
				n = ocsp.Unknown
			}
			mu.Unlock()

			fmt.Fprintf(rw, "%s %d", key, n)
		case "POST":
			data, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(rw, err.Error(), http.StatusBadRequest)
				return
			}

			n, err := strconv.Atoi(string(data))
			if err != nil {
				http.Error(rw, err.Error(), http.StatusBadRequest)
				return
			}

			mu.Lock()
			status[key] = n
			mu.Unlock()

			fmt.Fprintf(rw, "%s %d", key, n)
		default:
			http.Error(rw, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}
	})
	// The "/" endpoint is for normal OCSP requests. This actually parses an
	// OCSP status request and signs a response with a CA. Lightly based off:
	// https://www.ietf.org/rfc/rfc2560.txt
	mux.HandleFunc("/", func(rw http.ResponseWriter, r *http.Request) {
		var reqData []byte
		var err error

		switch {
		case r.Method == "GET":
			if method != "" && r.Method != method {
				http.Error(rw, "", http.StatusBadRequest)
				return
			}
			reqData, err = base64.StdEncoding.DecodeString(r.URL.Path[1:])
		case r.Method == "POST":
			if method != "" && r.Method != method {
				http.Error(rw, "", http.StatusBadRequest)
				return
			}
			reqData, err = io.ReadAll(r.Body)
			r.Body.Close()
		default:
			http.Error(rw, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}
		if err != nil {
			http.Error(rw, err.Error(), http.StatusBadRequest)
			return
		}

		ocspReq, err := ocsp.ParseRequest(reqData)
		if err != nil {
			http.Error(rw, err.Error(), http.StatusBadRequest)
			return
		}

		mu.Lock()
		n, ok := status[ocspReq.SerialNumber.String()]
		if !ok {
			n = ocsp.Unknown
		}
		mu.Unlock()

		tmpl := ocsp.Response{
			Status:       n,
			SerialNumber: ocspReq.SerialNumber,
			ThisUpdate:   time.Now(),
		}
		if responseTTL != 0 {
			tmpl.NextUpdate = tmpl.ThisUpdate.Add(responseTTL)
		}
		if embed {
			tmpl.Certificate = respCert
		}
		respData, err := ocsp.CreateResponse(issuerCert, respCert, tmpl, respKey)
		if err != nil {
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}

		rw.Header().Set("Content-Type", "application/ocsp-response")
		rw.Header().Set("Content-Length", fmt.Sprint(len(respData)))

		fmt.Fprint(rw, string(respData))
	})

	srv := &http.Server{
		Addr:        addr,
		Handler:     mux,
		ReadTimeout: time.Second * 5,
	}
	go srv.ListenAndServe()
	time.Sleep(1 * time.Second)
	return srv
}

func parseCertPEM(t *testing.T, certPEM string) *x509.Certificate {
	t.Helper()
	block := parsePEM(t, certPEM)

	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("failed to parse cert '%s': %s", certPEM, err)
	}
	return cert
}

func parseKeyPEM(t *testing.T, keyPEM string) crypto.Signer {
	t.Helper()
	block := parsePEM(t, keyPEM)

	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		key, err = x509.ParsePKCS1PrivateKey(block.Bytes)
		if err != nil {
			t.Fatalf("failed to parse ikey %s: %s", keyPEM, err)
		}
	}
	keyc := key.(crypto.Signer)
	return keyc
}

func parsePEM(t *testing.T, pemPath string) *pem.Block {
	t.Helper()
	data, err := os.ReadFile(pemPath)
	if err != nil {
		t.Fatal(err)
	}

	block, _ := pem.Decode(data)
	if block == nil {
		t.Fatalf("failed to decode PEM %s", pemPath)
	}
	return block
}

func GetOCSPStatus(s tls.ConnectionState) (*ocsp.Response, error) {
	if len(s.VerifiedChains) == 0 {
		return nil, fmt.Errorf("missing TLS verified chains")
	}
	chain := s.VerifiedChains[0]

	if got, want := len(chain), 2; got < want {
		return nil, fmt.Errorf("incomplete cert chain, got %d, want at least %d", got, want)
	}
	leaf, issuer := chain[0], chain[1]

	resp, err := ocsp.ParseResponseForCert(s.OCSPResponse, leaf, issuer)
	if err != nil {
		return nil, fmt.Errorf("failed to parse OCSP response: %w", err)
	}
	if err := resp.CheckSignatureFrom(issuer); err != nil {
		return resp, err
	}
	return resp, nil
}

func SetOCSPStatus(t *testing.T, ocspURL, certPEM string, status int) {
	t.Helper()

	cert := parseCertPEM(t, certPEM)

	hc := &http.Client{Timeout: 10 * time.Second}
	resp, err := hc.Post(
		fmt.Sprintf("%s/statuses/%s", ocspURL, cert.SerialNumber),
		"",
		strings.NewReader(fmt.Sprint(status)),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read OCSP HTTP response body: %s", err)
	}

	if got, want := resp.Status, "200 OK"; got != want {
		t.Error(strings.TrimSpace(string(data)))
		t.Fatalf("unexpected OCSP HTTP set status, got %q, want %q", got, want)
	}
}
