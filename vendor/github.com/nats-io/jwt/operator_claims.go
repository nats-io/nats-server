package jwt

import (
	"errors"

	"github.com/nats-io/nkeys"
)

// Operator specific claims
type Operator struct {
	Identities  []Identity `json:"identity,omitempty"`
	SigningKeys []string   `json:"signing_keys,omitempty"`
}

// Validate checks the validity of the operators contents
func (o *Operator) Validate(vr *ValidationResults) {
	for _, i := range o.Identities {
		i.Validate(vr)
	}

	if o.SigningKeys == nil {
		return
	}

	for _, k := range o.SigningKeys {
		if !nkeys.IsValidPublicOperatorKey(k) {
			vr.AddError("%s is not an operator public key", k)
		}
	}
}

// OperatorClaims define the data for an operator JWT
type OperatorClaims struct {
	ClaimsData
	Operator `json:"nats,omitempty"`
}

// NewOperatorClaims creates a new operator claim with the specified subject, which should be an operator public key
func NewOperatorClaims(subject string) *OperatorClaims {
	if subject == "" {
		return nil
	}
	c := &OperatorClaims{}
	c.Subject = subject
	return c
}

// DidSign checks the claims against the operator's public key and its signing keys
func (s *OperatorClaims) DidSign(op Claims) bool {
	if op == nil {
		return false
	}

	issuer := op.Claims().Issuer

	if issuer == s.Subject {
		return true
	}

	for _, k := range s.SigningKeys {
		if k == issuer {
			return true
		}
	}

	return false
}

// AddSigningKey creates the signing keys array if necessary
// appends the new key, NO Validation is performed
func (s *OperatorClaims) AddSigningKey(pk string) {

	if s.SigningKeys == nil {
		s.SigningKeys = []string{pk}
		return
	}

	s.SigningKeys = append(s.SigningKeys, pk)
}

// Encode the claims into a JWT string
func (s *OperatorClaims) Encode(pair nkeys.KeyPair) (string, error) {
	if !nkeys.IsValidPublicOperatorKey(s.Subject) {
		return "", errors.New("expected subject to be an operator public key")
	}
	s.ClaimsData.Type = OperatorClaim
	return s.ClaimsData.encode(pair, s)
}

// DecodeOperatorClaims tries to create an operator claims from a JWt string
func DecodeOperatorClaims(token string) (*OperatorClaims, error) {
	v := OperatorClaims{}
	if err := Decode(token, &v); err != nil {
		return nil, err
	}
	return &v, nil
}

func (s *OperatorClaims) String() string {
	return s.ClaimsData.String(s)
}

// Payload returns the operator specific data for an operator JWT
func (s *OperatorClaims) Payload() interface{} {
	return &s.Operator
}

// Validate the contents of the claims
func (s *OperatorClaims) Validate(vr *ValidationResults) {
	s.ClaimsData.Validate(vr)
	s.Operator.Validate(vr)
}

// ExpectedPrefixes defines the nkey types that can sign operator claims, operator
func (s *OperatorClaims) ExpectedPrefixes() []nkeys.PrefixByte {
	return []nkeys.PrefixByte{nkeys.PrefixByteOperator}
}

// Claims returns the generic claims data
func (s *OperatorClaims) Claims() *ClaimsData {
	return &s.ClaimsData
}
