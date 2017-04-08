package auth

import (
	"fmt"

	jwt "github.com/dgrijalva/jwt-go"
	"github.com/nats-io/gnatsd/server"
)

// JWTAuth holds the JWT secret used to validate a token
type JWTAuth struct {
	Secret string
}

// Check authenticates a client from a token
func (p *JWTAuth) Check(c server.ClientAuth) bool {
	opts := c.GetOpts()

	if p.Secret != "" {

		// If token == JWT secret, let it authenticate with default permissions.
		if p.Secret == opts.Authorization {
			return true
		}

		token, err := jwt.ParseWithClaims(opts.Authorization, &JwtUserClaims{}, func(token *jwt.Token) (interface{}, error) {
			// Validate the alg:
			if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("Unexpected signing method: %v", token.Header["alg"])
			}

			hmacSecret := []byte(p.Secret)
			return hmacSecret, nil
		})
		if err != nil {
			return false
		}
		if claims, ok := token.Claims.(*JwtUserClaims); ok && token.Valid {
			c.RegisterUser(claims.toUser())
			return true
		}

		return false
	}

	return false
}

// JwtUserClaims containing the NATS authorization claims within a JWT token.
type JwtUserClaims struct {
	server.User
	jwt.StandardClaims
}

// Returns JwtUserClaims as server.User
func (juc *JwtUserClaims) toUser() *server.User {
	return &server.User{
		Username:    juc.Username,
		Permissions: juc.Permissions,
	}
}
