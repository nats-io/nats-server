// Copyright 2021 The NATS Authors
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

package server

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/asn1"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/ocsp"
)

const (
	defaultOCSPStoreDir      = "ocsp"
	defaultOCSPCheckInterval = 24 * time.Hour
	minOCSPCheckInterval     = 2 * time.Minute
)

type OCSPMode uint8

const (
	// OCSPModeAuto staples a status, only if "status_request" is set in cert.
	OCSPModeAuto OCSPMode = iota

	// OCSPModeAlways enforces OCSP stapling for certs and shuts down the server in
	// case a server is revoked or cannot get OCSP staples.
	OCSPModeAlways

	// OCSPModeNever disables OCSP stapling even if cert has Must-Staple flag.
	OCSPModeNever

	// OCSPModeMust honors the Must-Staple flag from a certificate but also causing shutdown
	// in case the certificate has been revoked.
	OCSPModeMust
)

// OCSPMonitor monitors the state of a staple per certificate.
type OCSPMonitor struct {
	mu       sync.Mutex
	raw      []byte
	srv      *Server
	certFile string
	resp     *ocsp.Response
	hc       *http.Client
	stopCh   chan struct{}
	Leaf     *x509.Certificate
	Issuer   *x509.Certificate

	shutdownOnRevoke bool
}

func (oc *OCSPMonitor) getNextRun() time.Duration {
	oc.mu.Lock()
	nextUpdate := oc.resp.NextUpdate
	oc.mu.Unlock()

	now := time.Now()
	if nextUpdate.IsZero() {
		// If response is missing NextUpdate, we check the day after.
		// Technically, if NextUpdate is missing, we can try whenever.
		// https://tools.ietf.org/html/rfc6960#section-4.2.2.1
		return defaultOCSPCheckInterval
	}
	dur := nextUpdate.Sub(now) / 2

	// If negative, then wait a couple of minutes before getting another staple.
	if dur < 0 {
		return minOCSPCheckInterval
	}

	return dur
}

func (oc *OCSPMonitor) getStatus() ([]byte, *ocsp.Response, error) {
	raw, resp := oc.getCacheStatus()
	if len(raw) > 0 && resp != nil {
		// Check if the OCSP is still valid.
		if err := validOCSPResponse(resp); err == nil {
			return raw, resp, nil
		}
	}
	var err error
	raw, resp, err = oc.getLocalStatus()
	if err == nil {
		return raw, resp, nil
	}

	return oc.getRemoteStatus()
}

func (oc *OCSPMonitor) getCacheStatus() ([]byte, *ocsp.Response) {
	oc.mu.Lock()
	defer oc.mu.Unlock()
	return oc.raw, oc.resp
}

func (oc *OCSPMonitor) getLocalStatus() ([]byte, *ocsp.Response, error) {
	opts := oc.srv.getOpts()
	storeDir := opts.StoreDir
	if storeDir == _EMPTY_ {
		return nil, nil, fmt.Errorf("store_dir not set")
	}

	// This key must be based upon the current full certificate, not the public key,
	// so MUST be on the full raw certificate and not an SPKI or other reduced form.
	key := fmt.Sprintf("%x", sha256.Sum256(oc.Leaf.Raw))

	oc.mu.Lock()
	raw, err := ioutil.ReadFile(filepath.Join(storeDir, defaultOCSPStoreDir, key))
	oc.mu.Unlock()
	if err != nil {
		return nil, nil, err
	}

	resp, err := ocsp.ParseResponse(raw, oc.Issuer)
	if err != nil {
		return nil, nil, err
	}
	if err := validOCSPResponse(resp); err != nil {
		return nil, nil, err
	}

	// Cache the response.
	oc.mu.Lock()
	oc.raw = raw
	oc.resp = resp
	oc.mu.Unlock()

	return raw, resp, nil
}

func (oc *OCSPMonitor) getRemoteStatus() ([]byte, *ocsp.Response, error) {
	opts := oc.srv.getOpts()
	var overrideURLs []string
	if config := opts.OCSPConfig; config != nil {
		overrideURLs = config.OverrideURLs
	}
	getRequestBytes := func(u string, hc *http.Client) ([]byte, error) {
		resp, err := hc.Get(u)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("non-ok http status: %d", resp.StatusCode)
		}

		return ioutil.ReadAll(resp.Body)
	}

	// Request documentation:
	// https://tools.ietf.org/html/rfc6960#appendix-A.1

	reqDER, err := ocsp.CreateRequest(oc.Leaf, oc.Issuer, nil)
	if err != nil {
		return nil, nil, err
	}

	reqEnc := base64.StdEncoding.EncodeToString(reqDER)

	responders := oc.Leaf.OCSPServer
	if len(overrideURLs) > 0 {
		responders = overrideURLs
	}
	if len(responders) == 0 {
		return nil, nil, fmt.Errorf("no available ocsp servers")
	}

	oc.mu.Lock()
	hc := oc.hc
	oc.mu.Unlock()
	var raw []byte
	for _, u := range responders {
		u = strings.TrimSuffix(u, "/")
		raw, err = getRequestBytes(fmt.Sprintf("%s/%s", u, reqEnc), hc)
		if err == nil {
			break
		}
	}
	if err != nil {
		return nil, nil, fmt.Errorf("exhausted ocsp servers: %w", err)
	}

	resp, err := ocsp.ParseResponse(raw, oc.Issuer)
	if err != nil {
		return nil, nil, err
	}
	if err := validOCSPResponse(resp); err != nil {
		return nil, nil, err
	}

	if storeDir := opts.StoreDir; storeDir != _EMPTY_ {
		key := fmt.Sprintf("%x", sha256.Sum256(oc.Leaf.Raw))
		if err := oc.writeOCSPStatus(storeDir, key, raw); err != nil {
			return nil, nil, fmt.Errorf("failed to write ocsp status: %w", err)
		}
	}

	oc.mu.Lock()
	oc.raw = raw
	oc.resp = resp
	oc.mu.Unlock()

	return raw, resp, nil
}

func (oc *OCSPMonitor) run() {
	s := oc.srv
	s.mu.Lock()
	quitCh := s.quitCh
	s.mu.Unlock()

	defer s.grWG.Done()

	oc.mu.Lock()
	shutdownOnRevoke := oc.shutdownOnRevoke
	certFile := oc.certFile
	stopCh := oc.stopCh
	oc.mu.Unlock()

	var nextRun time.Duration
	_, resp, err := oc.getStatus()
	if err == nil && resp.Status == ocsp.Good {
		nextRun = oc.getNextRun()
		t := resp.NextUpdate.Format(time.RFC3339Nano)
		s.Noticef(
			"Found OCSP status for certificate at '%s': good, next update %s, checking again in %s",
			certFile, t, nextRun,
		)
	} else if err == nil && shutdownOnRevoke {
		// If resp.Status is ocsp.Revoked, ocsp.Unknown, or any other value.
		s.Errorf("Found OCSP status for certificate at '%s': %s", certFile, ocspStatusString(resp.Status))
		s.Shutdown()
		return
	}

	for {
		// On reload, if the certificate changes then need to stop this monitor.
		select {
		case <-time.After(nextRun):
		case <-stopCh:
			// In case of reload and have to restart the OCSP stapling monitoring.
			return
		case <-quitCh:
			// Server quit channel.
			return
		}
		_, resp, err := oc.getRemoteStatus()
		if err != nil {
			nextRun = oc.getNextRun()
			s.Errorf("Bad OCSP status update for certificate '%s': %s, trying again in %v", certFile, err, nextRun)
			continue
		}

		switch n := resp.Status; n {
		case ocsp.Good:
			nextRun = oc.getNextRun()
			t := resp.NextUpdate.Format(time.RFC3339Nano)
			s.Noticef(
				"Received OCSP status for certificate '%s': good, next update %s, checking again in %s",
				certFile, t, nextRun,
			)
			continue
		default:
			s.Errorf("Received OCSP status for certificate '%s': %s", certFile, ocspStatusString(n))
			if shutdownOnRevoke {
				s.Shutdown()
			}
			return
		}
	}
}

func (oc *OCSPMonitor) stop() {
	oc.mu.Lock()
	stopCh := oc.stopCh
	oc.mu.Unlock()
	select {
	case stopCh <- struct{}{}:
	default:
		// OCSP Monitor already stopped, likely due to certificate being revoked.
	}
}

// NewOCSPMonitor takes a TLS configuration then wraps it with the callbacks set for OCSP verification
// along with a monitor that will periodically fetch OCSP staples.
func (srv *Server) NewOCSPMonitor(kind string, tc *tls.Config) (*tls.Config, *OCSPMonitor, error) {
	opts := srv.getOpts()
	oc := opts.OCSPConfig

	// We need to track the CA certificate in case the CA is not present
	// in the chain to be able to verify the signature of the OCSP staple.
	var (
		certFile string
		caFile   string
		tcOpts   *TLSConfigOpts
	)
	switch kind {
	case typeStringMap[CLIENT]:
		tcOpts = opts.tlsConfigOpts
		if opts.TLSCert != _EMPTY_ {
			certFile = opts.TLSCert
		}
		if opts.TLSCaCert != _EMPTY_ {
			caFile = opts.TLSCaCert
		}
	case typeStringMap[ROUTER]:
		tcOpts = opts.Cluster.tlsConfigOpts
	case typeStringMap[LEAF]:
		tcOpts = opts.LeafNode.tlsConfigOpts
	case typeStringMap[GATEWAY]:
		tcOpts = opts.Gateway.tlsConfigOpts
	}
	if tcOpts != nil {
		certFile = tcOpts.CertFile
		caFile = tcOpts.CaFile
	}

	// NOTE: Currently OCSP Stapling is enabled only for the first certificate found.
	var mon *OCSPMonitor
	for _, cert := range tc.Certificates {
		var shutdownOnRevoke bool
		mustStaple := hasOCSPStatusRequest(cert.Leaf)
		if oc != nil {
			switch {
			case oc.Mode == OCSPModeNever:
				if mustStaple {
					srv.Warnf("Certificate at '%s' has MustStaple but OCSP is disabled", certFile)
				}
				return tc, nil, nil
			case oc.Mode == OCSPModeAlways:
				// Start the monitor for this cert even if it does not have
				// the MustStaple flag and shutdown the server in case the
				// staple ever gets revoked.
				mustStaple = true
				shutdownOnRevoke = true
			case oc.Mode == OCSPModeMust && mustStaple:
				shutdownOnRevoke = true
			case oc.Mode == OCSPModeAuto && !mustStaple:
				// "status_request" MustStaple flag not set in certificate. No need to do anything.
				return tc, nil, nil
			}
		}
		if !mustStaple {
			// No explicit OCSP config and cert does not have MustStaple flag either.
			return tc, nil, nil
		}

		if err := srv.setupOCSPStapleStoreDir(); err != nil {
			return nil, nil, err
		}

		// TODO: Add OCSP 'responder_cert' option in case CA cert not available.
		issuer, err := getOCSPIssuer(caFile, cert.Certificate)
		if err != nil {
			return nil, nil, err
		}

		mon = &OCSPMonitor{
			srv:              srv,
			hc:               &http.Client{Timeout: 30 * time.Second},
			shutdownOnRevoke: shutdownOnRevoke,
			certFile:         certFile,
			stopCh:           make(chan struct{}, 1),
			Leaf:             cert.Leaf,
			Issuer:           issuer,
		}

		// Get the certificate status from the memory, then remote OCSP responder.
		if _, resp, err := mon.getStatus(); err != nil {
			return nil, nil, fmt.Errorf("bad OCSP status update for certificate at '%s': %s", certFile, err)
		} else if err == nil && resp != nil && resp.Status != ocsp.Good && shutdownOnRevoke {
			return nil, nil, fmt.Errorf("found existing OCSP status for certificate at '%s': %s", certFile, ocspStatusString(resp.Status))
		}

		// Callbacks below will be in charge of returning the certificate instead,
		// so this has to be nil.
		tc.Certificates = nil

		// GetCertificate returns a certificate that's presented to a
		// client.
		tc.GetCertificate = func(info *tls.ClientHelloInfo) (*tls.Certificate, error) {
			raw, _, err := mon.getStatus()
			if err != nil {
				return nil, err
			}

			return &tls.Certificate{
				OCSPStaple:                   raw,
				Certificate:                  cert.Certificate,
				PrivateKey:                   cert.PrivateKey,
				SupportedSignatureAlgorithms: cert.SupportedSignatureAlgorithms,
				SignedCertificateTimestamps:  cert.SignedCertificateTimestamps,
				Leaf:                         cert.Leaf,
			}, nil
		}

		// Check whether need to verify staples from a client connection depending on the type.
		switch kind {
		case typeStringMap[ROUTER], typeStringMap[GATEWAY], typeStringMap[LEAF]:
			tc.VerifyConnection = func(s tls.ConnectionState) error {
				oresp := s.OCSPResponse
				if oresp == nil {
					return fmt.Errorf("%s client missing OCSP Staple", kind)
				}

				// Client route connections will verify the response of
				// the staple.
				if len(s.VerifiedChains) == 0 {
					return fmt.Errorf("%s client missing TLS verified chains", kind)
				}
				chain := s.VerifiedChains[0]
				resp, err := ocsp.ParseResponseForCert(oresp, chain[0], issuer)
				if err != nil {
					return fmt.Errorf("failed to parse OCSP response from %s client: %w", kind, err)
				}
				if err := resp.CheckSignatureFrom(issuer); err != nil {
					return err
				}
				if resp.Status != ocsp.Good {
					return fmt.Errorf("bad status for OCSP Staple from %s client: %s", kind, ocspStatusString(resp.Status))
				}

				return nil
			}

			// When server makes a client connection, need to also present an OCSP Staple.
			tc.GetClientCertificate = func(info *tls.CertificateRequestInfo) (*tls.Certificate, error) {
				raw, _, err := mon.getStatus()
				if err != nil {
					return nil, err
				}
				cert.OCSPStaple = raw

				return &cert, nil
			}
		default:
			// GetClientCertificate returns a certificate that's presented to a server.
			tc.GetClientCertificate = func(info *tls.CertificateRequestInfo) (*tls.Certificate, error) {
				return &cert, nil
			}
		}

	}
	return tc, mon, nil
}

func hasOCSPStatusRequest(cert *x509.Certificate) bool {
	// OID for id-pe-tlsfeature defined in RFC here:
	// https://datatracker.ietf.org/doc/html/rfc7633
	tlsFeatures := asn1.ObjectIdentifier{1, 3, 6, 1, 5, 5, 7, 1, 24}
	const statusRequestExt = 5

	// Example values:
	// * [48 3 2 1 5] - seen when creating own certs locally
	// * [30 3 2 1 5] - seen in the wild
	// Documentation:
	// https://tools.ietf.org/html/rfc6066

	for _, ext := range cert.Extensions {
		if !ext.Id.Equal(tlsFeatures) {
			continue
		}

		var val []int
		rest, err := asn1.Unmarshal(ext.Value, &val)
		if err != nil || len(rest) > 0 {
			return false
		}

		for _, n := range val {
			if n == statusRequestExt {
				return true
			}
		}
		break
	}

	return false
}

// writeOCSPStatus writes an OCSP status to a temporary file then moves it to a
// new path, in an attempt to avoid corrupting existing data.
func (oc *OCSPMonitor) writeOCSPStatus(storeDir, file string, data []byte) error {
	storeDir = filepath.Join(storeDir, defaultOCSPStoreDir)
	tmp, err := ioutil.TempFile(storeDir, "tmp-cert-status")
	if err != nil {
		return err
	}

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}

	oc.mu.Lock()
	err = os.Rename(tmp.Name(), filepath.Join(storeDir, file))
	oc.mu.Unlock()
	if err != nil {
		os.Remove(tmp.Name())
		return err
	}

	return nil
}

func parseCertPEM(name string) (*x509.Certificate, error) {
	data, err := ioutil.ReadFile(name)
	if err != nil {
		return nil, err
	}

	// Ignoring left over byte slice.
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, fmt.Errorf("failed to parse PEM cert %s", name)
	}
	if block.Type != "CERTIFICATE" {
		return nil, fmt.Errorf("unexpected PEM certificate type: %s", block.Type)
	}

	return x509.ParseCertificate(block.Bytes)
}

// getOCSPIssuer returns a CA cert from the given path. If the path is empty,
// then this checks a given cert chain. If both are empty, then it returns an
// error.
func getOCSPIssuer(issuerCert string, chain [][]byte) (*x509.Certificate, error) {
	var issuer *x509.Certificate
	var err error
	switch {
	case len(chain) == 1 && issuerCert == _EMPTY_:
		err = fmt.Errorf("require ocsp ca in chain or configuration")
	case issuerCert != _EMPTY_:
		issuer, err = parseCertPEM(issuerCert)
	case len(chain) > 1 && issuerCert == _EMPTY_:
		issuer, err = x509.ParseCertificate(chain[1])
	default:
		err = fmt.Errorf("invalid ocsp ca configuration")
	}
	if err != nil {
		return nil, err
	} else if !issuer.IsCA {
		return nil, fmt.Errorf("%s invalid ca basic constraints: is not ca", issuerCert)
	}

	return issuer, nil
}

func ocspStatusString(n int) string {
	switch n {
	case ocsp.Good:
		return "good"
	case ocsp.Revoked:
		return "revoked"
	default:
		return "unknown"
	}
}

func validOCSPResponse(r *ocsp.Response) error {
	// Time validation not handled by ParseResponse.
	// https://tools.ietf.org/html/rfc6960#section-4.2.2.1
	if !r.NextUpdate.IsZero() && r.NextUpdate.Before(time.Now()) {
		t := r.NextUpdate.Format(time.RFC3339Nano)
		return fmt.Errorf("invalid ocsp NextUpdate, is past time: %s", t)
	}
	if r.ThisUpdate.After(time.Now()) {
		t := r.ThisUpdate.Format(time.RFC3339Nano)
		return fmt.Errorf("invalid ocsp ThisUpdate, is future time: %s", t)
	}

	return nil
}
