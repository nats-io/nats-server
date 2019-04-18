/*
 * Copyright 2018 The NATS Authors
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package jwt

import (
	"errors"

	"github.com/nats-io/nkeys"
)

// Operator specific claims
type Operator struct {
	Identities  []Identity `json:"identity,omitempty"`
	SigningKeys StringList `json:"signing_keys,omitempty"`
}

// Validate checks the validity of the operators contents
func (o *Operator) Validate(vr *ValidationResults) {
	for _, i := range o.Identities {
		i.Validate(vr)
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
func (oc *OperatorClaims) DidSign(op Claims) bool {
	if op == nil {
		return false
	}
	issuer := op.Claims().Issuer
	if issuer == oc.Subject {
		return true
	}
	return oc.SigningKeys.Contains(issuer)
}

// deprecated AddSigningKey, use claim.SigningKeys.Add()
func (oc *OperatorClaims) AddSigningKey(pk string) {
	oc.SigningKeys.Add(pk)
}

// Encode the claims into a JWT string
func (oc *OperatorClaims) Encode(pair nkeys.KeyPair) (string, error) {
	if !nkeys.IsValidPublicOperatorKey(oc.Subject) {
		return "", errors.New("expected subject to be an operator public key")
	}
	oc.ClaimsData.Type = OperatorClaim
	return oc.ClaimsData.encode(pair, oc)
}

// DecodeOperatorClaims tries to create an operator claims from a JWt string
func DecodeOperatorClaims(token string) (*OperatorClaims, error) {
	v := OperatorClaims{}
	if err := Decode(token, &v); err != nil {
		return nil, err
	}
	return &v, nil
}

func (oc *OperatorClaims) String() string {
	return oc.ClaimsData.String(oc)
}

// Payload returns the operator specific data for an operator JWT
func (oc *OperatorClaims) Payload() interface{} {
	return &oc.Operator
}

// Validate the contents of the claims
func (oc *OperatorClaims) Validate(vr *ValidationResults) {
	oc.ClaimsData.Validate(vr)
	oc.Operator.Validate(vr)
}

// ExpectedPrefixes defines the nkey types that can sign operator claims, operator
func (oc *OperatorClaims) ExpectedPrefixes() []nkeys.PrefixByte {
	return []nkeys.PrefixByte{nkeys.PrefixByteOperator}
}

// Claims returns the generic claims data
func (oc *OperatorClaims) Claims() *ClaimsData {
	return &oc.ClaimsData
}
