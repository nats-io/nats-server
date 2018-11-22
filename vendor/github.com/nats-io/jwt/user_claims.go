package jwt

import (
	"errors"

	"github.com/nats-io/nkeys"
)

// User defines the user specific data in a user JWT
type User struct {
	Permissions
	Limits
}

// Validate checks the permissions and limits in a User jwt
func (u *User) Validate(vr *ValidationResults) {
	u.Permissions.Validate(vr)
	u.Limits.Validate(vr)
}

// UserClaims defines a user JWT
type UserClaims struct {
	ClaimsData
	User `json:"nats,omitempty"`
}

// NewUserClaims creates a user JWT with the specific subject/public key
func NewUserClaims(subject string) *UserClaims {
	if subject == "" {
		return nil
	}
	c := &UserClaims{}
	c.Subject = subject
	return c
}

// Encode tries to turn the user claims into a JWT string
func (u *UserClaims) Encode(pair nkeys.KeyPair) (string, error) {
	if !nkeys.IsValidPublicUserKey(u.Subject) {
		return "", errors.New("expected subject to be user public key")
	}
	u.ClaimsData.Type = UserClaim
	return u.ClaimsData.encode(pair, u)
}

// DecodeUserClaims tries to parse a user claims from a JWT string
func DecodeUserClaims(token string) (*UserClaims, error) {
	v := UserClaims{}
	if err := Decode(token, &v); err != nil {
		return nil, err
	}
	return &v, nil
}

// Validate checks the generic and specific parts of the user jwt
func (u *UserClaims) Validate(vr *ValidationResults) {
	u.ClaimsData.Validate(vr)
	u.User.Validate(vr)
}

// ExpectedPrefixes defines the types that can encode a user JWT, account
func (u *UserClaims) ExpectedPrefixes() []nkeys.PrefixByte {
	return []nkeys.PrefixByte{nkeys.PrefixByteAccount}
}

// Claims returns the generic data from a user jwt
func (u *UserClaims) Claims() *ClaimsData {
	return &u.ClaimsData
}

// Payload returns the user specific data from a user JWT
func (u *UserClaims) Payload() interface{} {
	return &u.User
}

func (u *UserClaims) String() string {
	return u.ClaimsData.String(u)
}
