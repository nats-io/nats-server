package websocket

import (
	"crypto/tls"
	"time"
)

// WebsocketOpts are options for websocket
type WebsocketOpts struct {
	// The server will accept websocket client connections on this hostname/IP.
	Host string
	// The server will accept websocket client connections on this port.
	Port int
	// The host:port to advertise to websocket clients in the cluster.
	Advertise string

	// If no user name is provided when a client connects, will default to the
	// matching user from the global list of users in `Options.Users`.
	NoAuthUser string

	// Name of the cookie, which if present in WebSocket upgrade headers,
	// will be treated as JWT during CONNECT phase as long as
	// "jwt" specified in the CONNECT options is missing or empty.
	JWTCookie string

	// Authentication section. If anything is configured in this section,
	// it will override the authorization configuration of regular clients.
	Username string
	Password string
	Token    string

	// Timeout for the authentication process.
	AuthTimeout float64

	// By default the server will enforce the use of TLS. If no TLS configuration
	// is provided, you need to explicitly set NoTLS to true to allow the server
	// to start without TLS configuration. Note that if a TLS configuration is
	// present, this boolean is ignored and the server will run the Websocket
	// server with that TLS configuration.
	// Running without TLS is less secure since Websocket clients that use bearer
	// tokens will send them in clear. So this should not be used in production.
	NoTLS bool

	// TLS configuration is required.
	TLSConfig *tls.Config
	// If true, map certificate values for authentication purposes.
	TLSMap bool

	// When present, accepted client certificates (verify/verify_and_map) must be in this list
	TLSPinnedCerts map[string]struct{}

	// If true, the Origin header must match the request's host.
	SameOrigin bool

	// Only origins in this list will be accepted. If empty and
	// SameOrigin is false, any origin is accepted.
	AllowedOrigins []string

	// If set to true, the server will negotiate with clients
	// if compression can be used. If this is false, no compression
	// will be used (both in server and clients) since it has to
	// be negotiated between both endpoints
	Compression bool

	// Total time allowed for the server to read the client request
	// and write the response back to the client. This include the
	// time needed for the TLS Handshake.
	HandshakeTimeout time.Duration
}
