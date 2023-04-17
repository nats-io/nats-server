// Copyright 2023 The NATS Authors
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

package certidp

import (
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"golang.org/x/crypto/ocsp"
)

const (
	DefaultAllowedClockSkew     = 30 * time.Second
	DefaultOCSPResponderTimeout = 2 * time.Second
	DefaultTTLUnsetNextUpdate   = 1 * time.Hour
)

type StatusAssertion int

var (
	StatusAssertionStrToVal = map[string]StatusAssertion{
		"good":    ocsp.Good,
		"revoked": ocsp.Revoked,
		"unknown": ocsp.Unknown,
	}
	StatusAssertionValToStr = map[StatusAssertion]string{
		ocsp.Good:    "good",
		ocsp.Revoked: "revoked",
		ocsp.Unknown: "unknown",
	}
	StatusAssertionIntToVal = map[int]StatusAssertion{
		0: ocsp.Good,
		1: ocsp.Revoked,
		2: ocsp.Unknown,
	}
)

func GetStatusAssertionStr(sa int) string {
	return StatusAssertionValToStr[StatusAssertionIntToVal[sa]]
}

func (sa StatusAssertion) MarshalJSON() ([]byte, error) {
	str, ok := StatusAssertionValToStr[sa]
	if !ok {
		// set unknown as fallback
		str = StatusAssertionValToStr[ocsp.Unknown]
	}
	return json.Marshal(str)
}

func (sa *StatusAssertion) UnmarshalJSON(in []byte) error {
	v, ok := StatusAssertionStrToVal[strings.ReplaceAll(string(in), "\"", "")]
	if !ok {
		// set unknown as fallback
		v = StatusAssertionStrToVal["unknown"]
	}
	*sa = v
	return nil
}

type ChainLink struct {
	Leaf             *x509.Certificate
	Issuer           *x509.Certificate
	OCSPWebEndpoints *[]*url.URL
}

// OCSPPeerConfig holds the parsed OCSP peer configuration section of TLS configuration
type OCSPPeerConfig struct {
	Verify                 bool
	Timeout                float64
	ClockSkew              float64
	WarnOnly               bool
	UnknownIsGood          bool
	AllowWhenCAUnreachable bool
	TTLUnsetNextUpdate     float64
}

func NewOCSPPeerConfig() *OCSPPeerConfig {
	return &OCSPPeerConfig{
		Verify:                 false,
		Timeout:                DefaultOCSPResponderTimeout.Seconds(),
		ClockSkew:              DefaultAllowedClockSkew.Seconds(),
		WarnOnly:               false,
		UnknownIsGood:          false,
		AllowWhenCAUnreachable: false,
		TTLUnsetNextUpdate:     DefaultTTLUnsetNextUpdate.Seconds(),
	}
}

// Log is a neutral method of passing server loggers to plugins
type Log struct {
	Debugf  func(format string, v ...interface{})
	Noticef func(format string, v ...interface{})
	Warnf   func(format string, v ...interface{})
	Errorf  func(format string, v ...interface{})
	Tracef  func(format string, v ...interface{})
}

type CertInfo struct {
	Subject     string `json:"subject,omitempty"`
	Issuer      string `json:"issuer,omitempty"`
	Fingerprint string `json:"fingerprint,omitempty"`
	Raw         []byte `json:"raw,omitempty"`
}

var OCSPPeerUsage = `
For client, leaf spoke (remotes), and leaf hub connections, you may enable OCSP peer validation:

    tls {
        ...
        # mTLS must be enabled (with exception of Leaf remotes)
        verify: true
        ...
        # short form enables peer verify and takes option defaults
        ocsp_peer: true
        
        # long form includes settable options
        ocsp_peer {
           # Enable OCSP peer validation (default false)
           verify: true

           # OCSP responder timeout in seconds (may be fractional, default 2 seconds)
           ca_timeout: 2

           # Allowed skew between server and OCSP responder time in seconds (may be fractional, default 30 seconds)
           allowed_clockskew: 30

           # Warn-only and never reject connections (default false)
           warn_only: false

           # Treat response Unknown status as valid certificate (default false)
           unknown_is_good: false

           # Warn-only if no CA response can be obtained and no cached revocation exists (default false)
           allow_when_ca_unreachable: false

           # If response NextUpdate unset by CA, set a default cache TTL in seconds from ThisUpdate (default 1 hour)
           cache_ttl_when_next_update_unset: 3600
        }
        ...
    }

Note: OCSP validation for route and gateway connections is enabled using the 'ocsp' configuration option.
`

// GenerateFingerprint returns a base64-encoded SHA256 hash of the raw certificate
func GenerateFingerprint(cert *x509.Certificate) string {
	data := sha256.Sum256(cert.Raw)
	return base64.StdEncoding.EncodeToString(data[:])
}

func getWebEndpoints(uris []string) []*url.URL {
	var urls []*url.URL
	for _, uri := range uris {
		endpoint, err := url.ParseRequestURI(uri)
		if err != nil {
			// skip invalid URLs
			continue
		}
		if endpoint.Scheme != "http" && endpoint.Scheme != "https" {
			// skip non-web URLs
			continue
		}
		urls = append(urls, endpoint)
	}
	return urls
}

// GetSubjectDNForm returns RDN sequence concatenation of the certificate's subject to be
// used in logs, events, etc. Should never be used for reliable cache matching or other crypto purposes.
func GetSubjectDNForm(cert *x509.Certificate) string {
	if cert == nil {
		return ""
	}
	return strings.TrimSuffix(fmt.Sprintf("%s+", cert.Subject.ToRDNSequence()), "+")
}

// GetIssuerDNForm returns RDN sequence concatenation of the certificate's issuer to be
// used in logs, events, etc. Should never be used for reliable cache matching or other crypto purposes.
func GetIssuerDNForm(cert *x509.Certificate) string {
	if cert == nil {
		return ""
	}
	return strings.TrimSuffix(fmt.Sprintf("%s+", cert.Issuer.ToRDNSequence()), "+")
}

// CertOCSPEligible checks if the certificate's issuer has populated AIA with OCSP responder endpoint(s)
// and is thus eligible for OCSP validation
func CertOCSPEligible(link *ChainLink) bool {
	if link == nil || link.Leaf.Raw == nil || len(link.Leaf.Raw) == 0 {
		return false
	}
	if link.Leaf.OCSPServer == nil || len(link.Leaf.OCSPServer) == 0 {
		return false
	}
	urls := getWebEndpoints(link.Leaf.OCSPServer)
	if len(urls) == 0 {
		return false
	}
	link.OCSPWebEndpoints = &urls
	return true
}

// GetLeafIssuerCert returns the issuer certificate of the leaf (positional) certificate in the chain
func GetLeafIssuerCert(chain []*x509.Certificate, leafPos int) *x509.Certificate {
	if len(chain) == 0 || leafPos < 0 {
		return nil
	}
	// self-signed certificate or too-big leafPos
	if leafPos >= len(chain)-1 {
		return nil
	}
	// returns pointer to issuer cert or nil
	return (chain)[leafPos+1]
}

// OCSPResponseCurrent checks if the OCSP response is current (i.e. not expired and not future effective)
func OCSPResponseCurrent(ocspr *ocsp.Response, opts *OCSPPeerConfig, log *Log) bool {
	skew := time.Duration(opts.ClockSkew * float64(time.Second))
	if skew < 0*time.Second {
		skew = DefaultAllowedClockSkew
	}
	now := time.Now().UTC()
	// Typical effectivity check based on CA response ThisUpdate and NextUpdate semantics
	if !ocspr.NextUpdate.IsZero() && ocspr.NextUpdate.Before(now.Add(-1*skew)) {
		t := ocspr.NextUpdate.Format(time.RFC3339Nano)
		nt := now.Format(time.RFC3339Nano)
		log.Debugf(DbgResponseExpired, t, nt, skew)
		return false
	}
	// CA responder can assert NextUpdate unset, in which case use config option to set a default cache TTL
	if ocspr.NextUpdate.IsZero() {
		ttl := time.Duration(opts.TTLUnsetNextUpdate * float64(time.Second))
		if ttl < 0*time.Second {
			ttl = DefaultTTLUnsetNextUpdate
		}
		expiryTime := ocspr.ThisUpdate.Add(ttl)
		if expiryTime.Before(now.Add(-1 * skew)) {
			t := expiryTime.Format(time.RFC3339Nano)
			nt := now.Format(time.RFC3339Nano)
			log.Debugf(DbgResponseTTLExpired, t, nt, skew)
			return false
		}
	}
	if ocspr.ThisUpdate.After(now.Add(skew)) {
		t := ocspr.ThisUpdate.Format(time.RFC3339Nano)
		nt := now.Format(time.RFC3339Nano)
		log.Debugf(DbgResponseFutureDated, t, nt, skew)
		return false
	}
	return true
}

// ValidDelegationCheck checks if the CA OCSP Response was signed by a valid CA Issuer delegate as per (RFC 6960, section 4.2.2.2)
// If a valid delegate or direct-signed by CA Issuer, true returned.
func ValidDelegationCheck(iss *x509.Certificate, ocspr *ocsp.Response) bool {
	// This call assumes prior successful parse and signature validation of the OCSP response
	// The Go OCSP library (as of x/crypto/ocsp v0.9) will detect and perform a 1-level delegate signature check but does not
	// implement the additional criteria for delegation specified in RFC 6960, section 4.2.2.2.
	if iss == nil || ocspr == nil {
		return false
	}
	// not a delegation, no-op
	if ocspr.Certificate == nil {
		return true
	}
	// delegate is self-same with CA Issuer, not a delegation although response issued in that form
	if ocspr.Certificate.Equal(iss) {
		return true
	}
	// we need to verify CA Issuer stamped id-kp-OCSPSigning on delegate
	delegatedSigner := false
	for _, keyUseExt := range ocspr.Certificate.ExtKeyUsage {
		if keyUseExt == x509.ExtKeyUsageOCSPSigning {
			delegatedSigner = true
			break
		}
	}
	return delegatedSigner
}
