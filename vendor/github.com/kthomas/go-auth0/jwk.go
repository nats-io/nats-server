package auth0

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"

	"github.com/form3tech-oss/jwt-go"
)

// JWTKeys lists the JWT keys
type JWTKeys struct {
	Keys []*JSONWebKey `json:"keys"`
}

// JSONWebKey represents an auth0 JWT
type JSONWebKey struct {
	Kty string   `json:"kty"`
	Kid string   `json:"kid"`
	Use string   `json:"use"`
	N   string   `json:"n"`
	E   string   `json:"e"`
	X5c []string `json:"x5c"`
}

// GetJWKs
func GetJWKs() (map[string]interface{}, error) {
	domain := auth0Domain
	if domain == "" {
		domain = os.Getenv("AUTH0_DOMAIN")
		if domain == "" {
			return nil, errors.New("failed to parse auth0 domain")
		}
	}

	url := fmt.Sprintf("https://%s/.well-known/jwks.json", domain)
	resp, err := http.Get(url)
	if err != nil {
		log.Warningf("failed to fetch auth0 JWT keys; %s", err.Error())
		return nil, err
	}
	defer resp.Body.Close()

	keys := &JWTKeys{}
	err = json.NewDecoder(resp.Body).Decode(&keys)
	if err != nil {
		log.Warningf("failed to decode auth0 JWT keys; %s", err.Error())
		return nil, err
	}

	keyMap := map[string]interface{}{}

	for i, _ := range keys.Keys {
		cert := "-----BEGIN CERTIFICATE-----\n" + keys.Keys[i].X5c[0] + "\n-----END CERTIFICATE-----"
		result, err := jwt.ParseRSAPublicKeyFromPEM([]byte(cert))
		if err != nil {
			log.Warningf("failed to parse auth0 public key; %s", err.Error())
			continue
		}
		keyMap[keys.Keys[i].Kid] = *result
		log.Debugf("resolved auth0 JWK: %s", keys.Keys[i].Kid)
	}

	return keyMap, nil
}
