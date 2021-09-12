package auth0

import (
	"os"

	logger "github.com/kthomas/go-logger"
)

const defaultauth0APINamespace = "v2"

var (
	// auth0APINamespace is the configured Auth0 API namespace (i.e., 'v2')
	auth0APINamespace string

	// auth0Domain is the configured Auth0 domain
	auth0Domain string

	// auth0ClientID is the configured Auth0 client id
	auth0ClientID string

	// auth0ClientSecret is the configured Auth0 client secret
	auth0ClientSecret string

	// auth0Audience is the configured Auth0 audience
	auth0Audience string

	// log is the configured logger
	log *logger.Logger
)

func init() {
	lvl := os.Getenv("AUTH0_LOG_LEVEL")
	if lvl == "" {
		lvl = "INFO"
	}
	var endpoint *string
	if os.Getenv("SYSLOG_ENDPOINT") != "" {
		endpt := os.Getenv("SYSLOG_ENDPOINT")
		endpoint = &endpt
	}
	log = logger.NewLogger("auth0", lvl, endpoint)
}

// RequireAuth0 reads the Auth0 configuration from the environment
func RequireAuth0() {
	log.Debugf("Attempting to read required Auth0 configuration from environment")

	auth0APINamespace = os.Getenv("AUTH0_API_NAMESPACE")
	if auth0APINamespace == "" {
		auth0APINamespace = defaultauth0APINamespace
	}

	auth0Domain = os.Getenv("AUTH0_DOMAIN")
	if auth0Domain == "" {
		log.Panicf("Failed to parse AUTH0_DOMAIN environment variable")
	}

	auth0ClientID = os.Getenv("AUTH0_CLIENT_ID")
	if auth0ClientID == "" {
		log.Panicf("Failed to parse AUTH0_CLIENT_ID environment variable")
	}

	auth0ClientSecret = os.Getenv("AUTH0_CLIENT_SECRET")
	if auth0ClientSecret == "" {
		log.Panicf("Failed to parse AUTH0_CLIENT_SECRET environment variable")
	}

	auth0Audience = os.Getenv("AUTH0_AUDIENCE")
	if auth0Audience == "" {
		log.Panicf("Failed to parse AUTH0_AUDIENCE environment variable")
	}
}
