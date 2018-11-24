package jwt

import "github.com/nats-io/nkeys"

// GenericClaims can be used to read a JWT as a map for any non-generic fields
type GenericClaims struct {
	ClaimsData
	Data map[string]interface{} `json:"nats,omitempty"`
}

// NewGenericClaims creates a map-based Claims
func NewGenericClaims(subject string) *GenericClaims {
	if subject == "" {
		return nil
	}
	c := GenericClaims{}
	c.Subject = subject
	c.Data = make(map[string]interface{})
	return &c
}

// DecodeGeneric takes a JWT string and decodes it into a ClaimsData and map
func DecodeGeneric(token string) (*GenericClaims, error) {
	v := GenericClaims{}
	if err := Decode(token, &v); err != nil {
		return nil, err
	}
	return &v, nil
}

// Claims returns the standard part of the generic claim
func (gc *GenericClaims) Claims() *ClaimsData {
	return &gc.ClaimsData
}

// Payload returns the custom part of the claiims data
func (gc *GenericClaims) Payload() interface{} {
	return &gc.Data
}

// Encode takes a generic claims and creates a JWT string
func (gc *GenericClaims) Encode(pair nkeys.KeyPair) (string, error) {
	return gc.ClaimsData.encode(pair, gc)
}

// Validate checks the generic part of the claims data
func (gc *GenericClaims) Validate(vr *ValidationResults) {
	gc.ClaimsData.Validate(vr)
}

func (gc *GenericClaims) String() string {
	return gc.ClaimsData.String(gc)
}

// ExpectedPrefixes returns the types allowed to encode a generic JWT, which is nil for all
func (gc *GenericClaims) ExpectedPrefixes() []nkeys.PrefixByte {
	return nil
}
