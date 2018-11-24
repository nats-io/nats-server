package jwt

import (
	"crypto/sha256"
	"encoding/base32"
	"errors"
	"fmt"
	"strings"

	"github.com/nats-io/nkeys"
)

// Activation defines the custom parts of an activation claim
type Activation struct {
	ImportSubject Subject    `json:"subject,omitempty"`
	ImportType    ExportType `json:"type,omitempty"`
	Limits
}

// IsService returns true if an Activation is for a service
func (a *Activation) IsService() bool {
	return a.ImportType == Service
}

// IsStream returns true if an Activation is for a stream
func (a *Activation) IsStream() bool {
	return a.ImportType == Stream
}

// Validate checks the exports and limits in an activation JWT
func (a *Activation) Validate(vr *ValidationResults) {
	if !a.IsService() && !a.IsStream() {
		vr.AddError("invalid export type: %q", a.ImportType)
	}

	if a.IsService() {
		if a.ImportSubject.HasWildCards() {
			vr.AddError("services cannot have wildcard subject: %q", a.ImportSubject)
		}
	}

	a.ImportSubject.Validate(vr)
	a.Limits.Validate(vr)
}

// ActivationClaims holds the data specific to an activation JWT
type ActivationClaims struct {
	ClaimsData
	Activation `json:"nats,omitempty"`
}

// NewActivationClaims creates a new activation claim with the provided sub
func NewActivationClaims(subject string) *ActivationClaims {
	if subject == "" {
		return nil
	}
	ac := &ActivationClaims{}
	ac.Subject = subject
	return ac
}

// Encode turns an activation claim into a JWT strimg
func (a *ActivationClaims) Encode(pair nkeys.KeyPair) (string, error) {
	if !nkeys.IsValidPublicAccountKey(a.ClaimsData.Subject) {
		return "", errors.New("expected subject to be an account")
	}
	a.ClaimsData.Type = ActivationClaim
	return a.ClaimsData.encode(pair, a)
}

// DecodeActivationClaims tries to create an activation claim from a JWT string
func DecodeActivationClaims(token string) (*ActivationClaims, error) {
	v := ActivationClaims{}
	if err := Decode(token, &v); err != nil {
		return nil, err
	}
	return &v, nil
}

// Payload returns the activation specific part of the JWT
func (a *ActivationClaims) Payload() interface{} {
	return a.Activation
}

// Validate checks the claims
func (a *ActivationClaims) Validate(vr *ValidationResults) {
	a.ClaimsData.Validate(vr)
	a.Activation.Validate(vr)
}

// ExpectedPrefixes defines the types that can sign an activation jwt, account and oeprator
func (a *ActivationClaims) ExpectedPrefixes() []nkeys.PrefixByte {
	return []nkeys.PrefixByte{nkeys.PrefixByteAccount, nkeys.PrefixByteOperator}
}

// Claims returns the generic part of the JWT
func (a *ActivationClaims) Claims() *ClaimsData {
	return &a.ClaimsData
}

func (a *ActivationClaims) String() string {
	return a.ClaimsData.String(a)
}

// HashID returns a hash of the claims that can be used to identify it.
// The hash is calculated by creating a string with
// issuerPubKey.subjectPubKey.<subject> and constructing the sha-256 hash and base32 encoding that.
// <subject> is the exported subject, minus any wildcards, so foo.* becomes foo.
// the one special case is that if the export start with "*" or is ">" the <subject> "_"
func (a *ActivationClaims) HashID() (string, error) {

	if a.Issuer == "" || a.Subject == "" || a.ImportSubject == "" {
		return "", fmt.Errorf("not enough data in the activaion claims to create a hash")
	}

	subject := cleanSubject(string(a.ImportSubject))
	base := fmt.Sprintf("%s.%s.%s", a.Issuer, a.Subject, subject)
	h := sha256.New()
	h.Write([]byte(base))
	sha := h.Sum(nil)
	hash := base32.StdEncoding.EncodeToString(sha)

	return hash, nil
}

func cleanSubject(subject string) string {
	split := strings.Split(subject, ".")
	cleaned := ""

	for i, tok := range split {
		if tok == "*" || tok == ">" {
			if i == 0 {
				cleaned = "_"
				break
			}

			cleaned = strings.Join(split[:i], ".")
			break
		}
	}
	if cleaned == "" {
		cleaned = subject
	}
	return cleaned
}
