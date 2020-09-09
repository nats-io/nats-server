/*
 * Copyright 2018-2019 The NATS Authors
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
	"sort"
	"time"

	"github.com/nats-io/nkeys"
)

// NoLimit is used to indicate a limit field is unlimited in value.
const NoLimit = -1

type AccountLimits struct {
	Imports         int64 `json:"imports,omitempty"`   // Max number of imports
	Exports         int64 `json:"exports,omitempty"`   // Max number of exports
	WildcardExports bool  `json:"wildcards,omitempty"` // Are wildcards allowed in exports
	Conn            int64 `json:"conn,omitempty"`      // Max number of active connections
	LeafNodeConn    int64 `json:"leaf,omitempty"`      // Max number of active leaf node connections
}

// IsUnlimited returns true if all limits are unlimited
func (a *AccountLimits) IsUnlimited() bool {
	return *a == AccountLimits{NoLimit, NoLimit, true, NoLimit, NoLimit}
}

type NatsLimits struct {
	Subs    int64 `json:"subs,omitempty"`    // Max number of subscriptions
	Data    int64 `json:"data,omitempty"`    // Max number of bytes
	Payload int64 `json:"payload,omitempty"` // Max message payload
}

// IsUnlimited returns true if all limits are unlimited
func (n *NatsLimits) IsUnlimited() bool {
	return *n == NatsLimits{NoLimit, NoLimit, NoLimit}
}

type JetStreamLimits struct {
	MemoryStorage int64 `json:"mem_storage,omitempty"`  // Max number of bytes stored in memory across all streams. (0 means disabled)
	DiskStorage   int64 `json:"disk_storage,omitempty"` // Max number of bytes stored on disk across all streams. (0 means disabled)
	Streams       int64 `json:"streams,omitempty"`      // Max number of streams
	Consumer      int64 `json:"consumer,omitempty"`     // Max number of consumer
}

// IsUnlimited returns true if all limits are unlimited
func (j *JetStreamLimits) IsUnlimited() bool {
	return *j == JetStreamLimits{NoLimit, NoLimit, NoLimit, NoLimit}
}

// OperatorLimits are used to limit access by an account
type OperatorLimits struct {
	NatsLimits
	AccountLimits
	JetStreamLimits
}

// IsEmpty returns true if all of the limits are 0/false.
func (o *OperatorLimits) IsEmpty() bool {
	return *o == OperatorLimits{}
}

// IsUnlimited returns true if all limits are unlimited
func (o *OperatorLimits) IsUnlimited() bool {
	return o.AccountLimits.IsUnlimited() && o.NatsLimits.IsUnlimited() && o.JetStreamLimits.IsUnlimited()
}

// Validate checks that the operator limits contain valid values
func (o *OperatorLimits) Validate(_ *ValidationResults) {
	// negative values mean unlimited, so all numbers are valid
}

// Account holds account specific claims data
type Account struct {
	Imports            Imports        `json:"imports,omitempty"`
	Exports            Exports        `json:"exports,omitempty"`
	Limits             OperatorLimits `json:"limits,omitempty"`
	SigningKeys        StringList     `json:"signing_keys,omitempty"`
	Revocations        RevocationList `json:"revocations,omitempty"`
	DefaultPermissions Permissions    `json:"default_permissions,omitempty"`
	GenericFields
}

// Validate checks if the account is valid, based on the wrapper
func (a *Account) Validate(acct *AccountClaims, vr *ValidationResults) {
	a.Imports.Validate(acct.Subject, vr)
	a.Exports.Validate(vr)
	a.Limits.Validate(vr)
	a.DefaultPermissions.Validate(vr)

	if !a.Limits.IsEmpty() && a.Limits.Imports >= 0 && int64(len(a.Imports)) > a.Limits.Imports {
		vr.AddError("the account contains more imports than allowed by the operator")
	}

	// Check Imports and Exports for limit violations.
	if a.Limits.Imports != NoLimit {
		if int64(len(a.Imports)) > a.Limits.Imports {
			vr.AddError("the account contains more imports than allowed by the operator")
		}
	}
	if a.Limits.Exports != NoLimit {
		if int64(len(a.Exports)) > a.Limits.Exports {
			vr.AddError("the account contains more exports than allowed by the operator")
		}
		// Check for wildcard restrictions
		if !a.Limits.WildcardExports {
			for _, ex := range a.Exports {
				if ex.Subject.HasWildCards() {
					vr.AddError("the account contains wildcard exports that are not allowed by the operator")
				}
			}
		}
	}

	for _, k := range a.SigningKeys {
		if !nkeys.IsValidPublicAccountKey(k) {
			vr.AddError("%s is not an account public key", k)
		}
	}
}

// AccountClaims defines the body of an account JWT
type AccountClaims struct {
	ClaimsData
	Account `json:"nats,omitempty"`
}

// NewAccountClaims creates a new account JWT
func NewAccountClaims(subject string) *AccountClaims {
	if subject == "" {
		return nil
	}
	c := &AccountClaims{}
	// Set to unlimited to start. We do it this way so we get compiler
	// errors if we add to the OperatorLimits.
	c.Limits = OperatorLimits{
		NatsLimits{NoLimit, NoLimit, NoLimit},
		AccountLimits{NoLimit, NoLimit, true, NoLimit, NoLimit},
		JetStreamLimits{NoLimit, NoLimit, NoLimit, NoLimit}}
	c.Subject = subject
	return c
}

// Encode converts account claims into a JWT string
func (a *AccountClaims) Encode(pair nkeys.KeyPair) (string, error) {
	if !nkeys.IsValidPublicAccountKey(a.Subject) {
		return "", errors.New("expected subject to be account public key")
	}
	sort.Sort(a.Exports)
	sort.Sort(a.Imports)
	a.Type = AccountClaim
	return a.ClaimsData.encode(pair, a)
}

// DecodeAccountClaims decodes account claims from a JWT string
func DecodeAccountClaims(token string) (*AccountClaims, error) {
	claims, err := Decode(token)
	if err != nil {
		return nil, err
	}
	ac, ok := claims.(*AccountClaims)
	if !ok {
		return nil, errors.New("not account claim")
	}
	return ac, nil
}

func (a *AccountClaims) String() string {
	return a.ClaimsData.String(a)
}

// Payload pulls the accounts specific payload out of the claims
func (a *AccountClaims) Payload() interface{} {
	return &a.Account
}

// Validate checks the accounts contents
func (a *AccountClaims) Validate(vr *ValidationResults) {
	a.ClaimsData.Validate(vr)
	a.Account.Validate(a, vr)

	if nkeys.IsValidPublicAccountKey(a.ClaimsData.Issuer) {
		if !a.Limits.IsEmpty() {
			vr.AddWarning("self-signed account JWTs shouldn't contain operator limits")
		}
	}
}

func (a *AccountClaims) ClaimType() ClaimType {
	return a.Type
}

func (a *AccountClaims) updateVersion() {
	a.GenericFields.Version = libVersion
}

// ExpectedPrefixes defines the types that can encode an account jwt, account and operator
func (a *AccountClaims) ExpectedPrefixes() []nkeys.PrefixByte {
	return []nkeys.PrefixByte{nkeys.PrefixByteAccount, nkeys.PrefixByteOperator}
}

// Claims returns the accounts claims data
func (a *AccountClaims) Claims() *ClaimsData {
	return &a.ClaimsData
}

// DidSign checks the claims against the account's public key and its signing keys
func (a *AccountClaims) DidSign(op Claims) bool {
	if op != nil {
		issuer := op.Claims().Issuer
		if issuer == a.Subject {
			return true
		}
		return a.SigningKeys.Contains(issuer)
	}
	return false
}

// Revoke enters a revocation by public key using time.Now().
func (a *AccountClaims) Revoke(pubKey string) {
	a.RevokeAt(pubKey, time.Now())
}

// RevokeAt enters a revocation by public key and timestamp into this export
// If there is already a revocation for this public key that is newer, it is kept.
func (a *AccountClaims) RevokeAt(pubKey string, timestamp time.Time) {
	if a.Revocations == nil {
		a.Revocations = RevocationList{}
	}

	a.Revocations.Revoke(pubKey, timestamp)
}

// ClearRevocation removes any revocation for the public key
func (a *AccountClaims) ClearRevocation(pubKey string) {
	a.Revocations.ClearRevocation(pubKey)
}

// IsRevokedAt checks if the public key is in the revoked list with a timestamp later than
// the one passed in. Generally this method is called with time.Now() but other time's can
// be used for testing.
func (a *AccountClaims) IsRevokedAt(pubKey string, timestamp time.Time) bool {
	return a.Revocations.IsRevoked(pubKey, timestamp)
}

// IsRevoked checks if the public key is in the revoked list with time.Now()
func (a *AccountClaims) IsRevoked(pubKey string) bool {
	return a.Revocations.IsRevoked(pubKey, time.Now())
}
