package server

import (
	"fmt"
	"github.com/nats-io/nats-server/v2/server/internal/network/websocket"
	"net"
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
	c.websocketClient.Mu = &c.mu
	c.websocketClient.Nc = c.nc
	c.websocketClient.Out = &c.out

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
