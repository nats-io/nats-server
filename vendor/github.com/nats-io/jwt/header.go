package jwt

import (
	"encoding/json"
	"fmt"
	"strings"
)

const (
	// Version
	Version = "0.0.5"

	// TokenTypeJwt is the JWT token type supported JWT tokens
	// encoded and decoded by this library
	TokenTypeJwt = "jwt"

	// AlgorithmNkey is the algorithm supported by JWT tokens
	// encoded and decoded by this library
	AlgorithmNkey = "ed25519"
)

// Header is a JWT Jose Header
type Header struct {
	Type      string `json:"typ"`
	Algorithm string `json:"alg"`
}

// Parses a header JWT token
func parseHeaders(s string) (*Header, error) {
	h, err := decodeString(s)
	if err != nil {
		return nil, err
	}
	header := Header{}
	if err := json.Unmarshal(h, &header); err != nil {
		return nil, err
	}

	if err := header.Valid(); err != nil {
		return nil, err
	}
	return &header, nil
}

// Valid validates the Header. It returns nil if the Header is
// a JWT header, and the algorithm used is the NKEY algorithm.
func (h *Header) Valid() error {
	if TokenTypeJwt != strings.ToLower(h.Type) {
		return fmt.Errorf("not supported type %q", h.Type)
	}

	if AlgorithmNkey != strings.ToLower(h.Algorithm) {
		return fmt.Errorf("unexpected %q algorithm", h.Algorithm)
	}
	return nil
}
