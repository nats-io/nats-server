package auth0

import "time"

type Auth0OAuthTokenResponse struct {
	AccessToken *string       `json:"access_token"`
	TokenType   *string       `json:"token_type"`
	ExpiresIn   time.Duration `json:"expires_in"`
}
