package server

import (
	"errors"
	"fmt"
	"github.com/nats-io/nats-server/v2/server/internal/network/websocket"
	"net"
	"net/url"
	"time"
)

// processWSClient is similar to createClient() but has some modifications
// specific to handle websocket clients.
// The comments have been kept to minimum to reduce code size.
// Check createClient() for more details.
func (s *Server) processWSClient(conn net.Conn, ws *websocket.Websocket) error {
	opts := s.getOpts()
	preparedClient := prepareWsClient(conn, ws, opts)

	err := preparedClient.finalizeWsClient(s)
	if err != nil {
		return err
	}
	c := preparedClient

	s.mu.Lock()
	if !s.running || s.ldm {
		if s.shutdown {
			conn.Close()
		}
		s.mu.Unlock()
		return fmt.Errorf("server is not running or not in lameDuck mode during runing websocket")
	}
	s.startWebsocketClient(c)
	return nil
}

// prepareWsClient creates a client without assigning srv.
func prepareWsClient(conn net.Conn, ws *websocket.Websocket, opts *Options) *client {
	maxPay := int32(opts.MaxPayload)
	maxSubs := int32(opts.MaxSubs)
	if maxSubs == 0 {
		maxSubs = -1
	}
	now := time.Now().UTC()

	c := &client{nc: conn, opts: defaultOpts, mpay: maxPay, msubs: maxSubs, start: now, last: now,
		websocketClient: &websocket.Client{Ws: ws}}

	return c
}

func (c *client) finalizeWsClient(s *Server) error {
	c.mu.Lock()
	wsClient := c.websocketClient
	wsClient.Mu = &c.mu
	wsClient.Nc = c.nc
	wsClient.Out = &c.out

	c.srv = s
	err := c.registerWithAccount(s.globalAccount())
	if err != nil {
		return err
	}
	opts := s.getOpts()
	var info Info
	var authRequired bool

	s.mu.Lock()
	info = s.copyInfo()
	// Check auth, override if applicable.
	if !info.AuthRequired {
		// Set info.AuthRequired since this is what is sent to the client.
		info.AuthRequired = s.websocket.AuthOverride
	}
	if s.nonceRequired() {
		var raw [nonceLen]byte
		nonce := raw[:]
		s.generateNonce(nonce)
		info.Nonce = string(nonce)
	}
	c.nonce = []byte(info.Nonce)
	authRequired = info.AuthRequired

	s.totalClients++
	s.mu.Unlock()

	c.mu.Lock()
	if authRequired {
		c.flags.set(expectConnect)
	}
	c.initClient()
	c.Debugf("Client connection created")
	c.sendProtoNow(c.generateClientInfoJSON(info))
	c.mu.Unlock()

	if opts.MaxConn > 0 && len(s.clients) >= opts.MaxConn {
		s.mu.Unlock()
		c.maxConnExceeded()
		return nil
	}
	s.clients[c.cid] = c

	// Websocket clients do TLS in the websocket http Server.
	// So no TLS here...
	s.mu.Unlock()

	c.mu.Lock()

	if authRequired {
		timeout := opts.AuthTimeout
		// Possibly override with Websocket specific value.
		if opts.Websocket.AuthTimeout != 0 {
			timeout = opts.Websocket.AuthTimeout
		}
		c.setAuthTimer(secondsToDuration(timeout))
	}
	c.mu.Unlock()

	return nil
}

func (s *Server) startWebsocketClient(c *client) {
	c.mu.Lock()
	if c.isClosed() {
		c.mu.Unlock()
		c.closeConnection(WriteError)
		return
	}

	c.setPingTimer()

	s.startGoRoutine(func() { c.readLoop(nil) })
	s.startGoRoutine(func() { c.writeLoop() })
	c.mu.Unlock()
}

// ValidateWebsocketOptions validates the websocket related options.
func ValidateWebsocketOptions(o *Options) error {
	wo := &o.Websocket
	// If no port is defined, we don't care about other options
	if wo.Port == 0 {
		return nil
	}
	// Enforce TLS... unless NoTLS is set to true.
	if wo.TLSConfig == nil && !wo.NoTLS {
		return errors.New("websocket requires TLS configuration")
	}
	// Make sure that allowed origins, if specified, can be parsed.
	for _, ao := range wo.AllowedOrigins {
		if _, err := url.Parse(ao); err != nil {
			return fmt.Errorf("unable to parse allowed origin: %v", err)
		}
	}
	// If there is a NoAuthUser, we need to have Users defined and
	// the user to be present.
	if wo.NoAuthUser != _EMPTY_ {
		if err := ValidateNoAuthUser(o, wo.NoAuthUser); err != nil {
			return err
		}
	}
	// Token/Username not possible if there are users/nkeys
	if len(o.Users) > 0 || len(o.Nkeys) > 0 {
		if wo.Username != _EMPTY_ {
			return fmt.Errorf("websocket authentication username not compatible with presence of users/nkeys")
		}
		if wo.Token != _EMPTY_ {
			return fmt.Errorf("websocket authentication token not compatible with presence of users/nkeys")
		}
	}
	// Using JWT requires Trusted Keys
	if wo.JWTCookie != _EMPTY_ {
		if len(o.TrustedOperators) == 0 && len(o.TrustedKeys) == 0 {
			return fmt.Errorf("trusted operators or trusted keys configuration is required for JWT authentication via cookie %q", wo.JWTCookie)
		}
	}
	if err := ValidatePinnedCerts(wo.TLSPinnedCerts); err != nil {
		return fmt.Errorf("websocket: %v", err)
	}
	return nil
}
