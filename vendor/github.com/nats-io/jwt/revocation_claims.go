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
	"github.com/nats-io/nkeys"
)

// Revocation defines the custom parts of a revocation JWt
type Revocation struct {
	JWT    string `json:"jwt,omitempty"`
	Reason string `json:"reason,omitempty"`
}

// Validate checks the JWT and reason for a revocation
func (u *Revocation) Validate(vr *ValidationResults) {
	if u.JWT == "" {
		vr.AddError("revocation token has no JWT to revoke")
	}

	_, err := DecodeGeneric(u.JWT)

	if err != nil {
		vr.AddError("revocation token has an invalid JWT")
	}
}

// RevocationClaims defines a revocation tokens data
type RevocationClaims struct {
	ClaimsData
	Revocation `json:"nats,omitempty"`
}

// NewRevocationClaims creates a new revocation JWT for the specified subject/public key
func NewRevocationClaims(subject string) *RevocationClaims {
	if subject == "" {
		return nil
	}
	c := &RevocationClaims{}
	c.Subject = subject
	return c
}

// Encode translates the claims to a JWT string
func (rc *RevocationClaims) Encode(pair nkeys.KeyPair) (string, error) {
	rc.ClaimsData.Type = RevocationClaim
	return rc.ClaimsData.encode(pair, rc)
}

// DecodeRevocationClaims tries to parse a JWT string as a RevocationClaims
func DecodeRevocationClaims(token string) (*RevocationClaims, error) {
	v := RevocationClaims{}
	if err := Decode(token, &v); err != nil {
		return nil, err
	}
	return &v, nil
}

func (rc *RevocationClaims) String() string {
	return rc.ClaimsData.String(rc)
}

// Payload returns the revocation specific part of the claims
func (rc *RevocationClaims) Payload() interface{} {
	return &rc.Revocation
}

// Validate checks the generic and revocation parts of the claims
func (rc *RevocationClaims) Validate(vr *ValidationResults) {
	rc.ClaimsData.Validate(vr)
	rc.Revocation.Validate(vr)

	theJWT, err := DecodeGeneric(rc.Revocation.JWT)
	if err != nil {
		vr.AddError("revocation contains an invalid JWT")
		return // can't do the remaining checks
	}

	if theJWT.Issuer != rc.Issuer {
		vr.AddError("Revocation issuer doesn't match JWT to revoke")
	}
}

// ExpectedPrefixes defines who can sign a revocation token, account or operator
func (rc *RevocationClaims) ExpectedPrefixes() []nkeys.PrefixByte {
	return []nkeys.PrefixByte{nkeys.PrefixByteOperator, nkeys.PrefixByteAccount}
}

// Claims returns the generic part of the claims
func (rc *RevocationClaims) Claims() *ClaimsData {
	return &rc.ClaimsData
}
