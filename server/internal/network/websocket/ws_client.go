package websocket

import (
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/nats-io/nats-server/v2/server"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// handleControlFrame Handles the PING, PONG and CLOSE websocket control frames.
//
// Client lock MUST NOT be held on entry.
func (c *server.client) handleControlFrame(r *readInfo, frameType opCode, nc io.Reader, buf []byte, pos int) (int, error) {
	var payload []byte
	var err error

	if r.rem > 0 {
		payload, pos, err = get(nc, buf, pos, r.rem)
		if err != nil {
			return pos, err
		}
		if r.mask {
			r.unmask(payload)
		}
		r.rem = 0
	}
	switch frameType {
	case closeMessage:
		status := closeStatusNoStatusReceived
		var body string
		lp := len(payload)
		// If there is a payload, the status is represented as a 2-byte
		// unsigned integer (in network byte order). Then, there may be an
		// optional body.
		hasStatus, hasBody := lp >= closeSatusSize, lp > closeSatusSize
		if hasStatus {
			// Decode the status
			status = int(binary.BigEndian.Uint16(payload[:closeSatusSize]))
			// Now if there is a body, capture it and make sure this is a valid UTF-8.
			if hasBody {
				body = string(payload[closeSatusSize:])
				if !utf8.ValidString(body) {
					// https://tools.ietf.org/html/rfc6455#section-5.5.1
					// If body is present, it must be a valid utf8
					status = closeStatusInvalidPayloadData
					body = "invalid utf8 body in close frame"
				}
			}
		}
		c.wsEnqueueControlMessage(closeMessage, createCloseMessage(status, body))
		// Return io.EOF so that readLoop will close the connection as ClientClosed
		// after processing pending buffers.
		return pos, io.EOF
	case pingMessage:
		c.wsEnqueueControlMessage(wsPongMessage, payload)
	case wsPongMessage:
		// Nothing to do..
	}
	return pos, nil
}

// Returns a slice of byte slices corresponding to payload of websocket frames.
// The byte slice `buf` is filled with bytes from the connection's read loop.
// This function will decode the frame headers and unmask the payload(s).
// It is possible that the returned slices point to the given `buf` slice, so
// `buf` should not be overwritten until the returned slices have been parsed.
//
// Client lock MUST NOT be held on entry.
func (c *server.client) wsRead(r *readInfo, ior io.Reader, buf []byte) ([][]byte, error) {
	var (
		bufs   [][]byte
		tmpBuf []byte
		err    error
		pos    int
		max    = len(buf)
	)
	for pos != max {
		if r.fs {
			b0 := buf[pos]
			frameType := opCode(b0 & 0xF)
			final := b0&finalBit != 0
			compressed := b0&rsv1Bit != 0
			pos++

			tmpBuf, pos, err = get(ior, buf, pos, 1)
			if err != nil {
				return bufs, err
			}
			b1 := tmpBuf[0]

			// Clients MUST set the mask bit. If not set, reject.
			// However, LEAF by default will not have masking, unless they are forced to, by configuration.
			if r.mask && b1&maskBit == 0 {
				return bufs, c.wsHandleProtocolError("mask bit missing")
			}

			// Store size in case it is < 125
			r.rem = int(b1 & 0x7F)

			switch frameType {
			case pingMessage, wsPongMessage, closeMessage:
				if r.rem > maxControlPayloadSize {
					return bufs, c.wsHandleProtocolError(
						fmt.Sprintf("control frame length bigger than maximum allowed of %v bytes",
							maxControlPayloadSize))
				}
				if !final {
					return bufs, c.wsHandleProtocolError("control frame does not have final bit set")
				}
			case textMessage, binaryMessage:
				if !r.ff {
					return bufs, c.wsHandleProtocolError("new message started before final frame for previous message was received")
				}
				r.ff = final
				r.fc = compressed
			case continuationFrame:
				// Compressed bit must be only set in the first frame
				if r.ff || compressed {
					return bufs, c.wsHandleProtocolError("invalid continuation frame")
				}
				r.ff = final
			default:
				return bufs, c.wsHandleProtocolError(fmt.Sprintf("unknown opcode %v", frameType))
			}

			switch r.rem {
			case 126:
				tmpBuf, pos, err = get(ior, buf, pos, 2)
				if err != nil {
					return bufs, err
				}
				r.rem = int(binary.BigEndian.Uint16(tmpBuf))
			case 127:
				tmpBuf, pos, err = get(ior, buf, pos, 8)
				if err != nil {
					return bufs, err
				}
				r.rem = int(binary.BigEndian.Uint64(tmpBuf))
			}

			if r.mask {
				// Read masking key
				tmpBuf, pos, err = get(ior, buf, pos, 4)
				if err != nil {
					return bufs, err
				}
				copy(r.mkey[:], tmpBuf)
				r.mkpos = 0
			}

			// Handle control messages in place...
			if isControlFrame(frameType) {
				pos, err = c.wsHandleControlFrame(r, frameType, ior, buf, pos)
				if err != nil {
					return bufs, err
				}
				continue
			}

			// Done with the frame header
			r.fs = false
		}
		if pos < max {
			var b []byte
			var n int

			n = r.rem
			if pos+n > max {
				n = max - pos
			}
			b = buf[pos : pos+n]
			pos += n
			r.rem -= n
			// If needed, unmask the buffer
			if r.mask {
				r.unmask(b)
			}
			addToBufs := true
			// Handle compressed message
			if r.fc {
				// Assume that we may have continuation frames or not the full payload.
				addToBufs = false
				// Make a copy of the buffer before adding it to the list
				// of compressed fragments.
				r.cbufs = append(r.cbufs, append([]byte(nil), b...))
				// When we have the final frame and we have read the full payload,
				// we can decompress it.
				if r.ff && r.rem == 0 {
					b, err = r.decompress()
					if err != nil {
						return bufs, err
					}
					r.fc = false
					// Now we can add to `bufs`
					addToBufs = true
				}
			}
			// For non compressed frames, or when we have decompressed the
			// whole message.
			if addToBufs {
				bufs = append(bufs, b)
			}
			// If payload has been fully read, then indicate that next
			// is the start of a frame.
			if r.rem == 0 {
				r.fs = true
			}
		}
	}
	return bufs, nil
}

// Enqueues a websocket control message.
// If the control message is a closeMessage, then marks this client
// has having sent the close message (since only one should be sent).
// This will prevent the generic closeConnection() to enqueue one.
//
// Client lock held on entry.
func (c *server.client) enqueueControlMessageLocked(controlMsg opCode, payload []byte) {
	// Control messages are never compressed and their size will be
	// less than maxControlPayloadSize, which means the frame header
	// will be only 2 or 6 bytes.
	useMasking := c.ws.maskwrite
	sz := 2
	if useMasking {
		sz += 4
	}
	cm := make([]byte, sz+len(payload))
	n, key := fillFrameHeader(cm, useMasking, firstFrame, finalFrame, uncompressedFrame, controlMsg, len(payload))
	// Note that payload is optional.
	if len(payload) > 0 {
		copy(cm[n:], payload)
		if useMasking {
			maskBuf(key, cm[n:])
		}
	}
	c.out.pb += int64(len(cm))
	if controlMsg == closeMessage {
		// We can't add the close message to the frames buffers
		// now. It will be done on a flushOutbound() when there
		// are no more pending buffers to send.
		c.ws.closeSent = true
		c.ws.closeMsg = cm
	} else {
		c.ws.frames = append(c.ws.frames, cm)
		c.ws.fs += int64(len(cm))
	}
	c.flushSignal()
}

// Invokes enqueueControlMessageLocked under client lock.
//
// Client lock MUST NOT be held on entry
func (c *server.client) wsEnqueueControlMessage(controlMsg opCode, payload []byte) {
	c.mu.Lock()
	c.wsEnqueueControlMessageLocked(controlMsg, payload)
	c.mu.Unlock()
}

// Enqueues a websocket close message with a status mapped from the given `reason`.
//
// Client lock held on entry
func (c *server.client) enqueueCloseMessage(reason server.ClosedState) {
	var status int
	switch reason {
	case server.ClientClosed:
		status = closeStatusNormalClosure
	case server.AuthenticationTimeout, server.AuthenticationViolation, server.SlowConsumerPendingBytes, server.SlowConsumerWriteDeadline,
		server.MaxAccountConnectionsExceeded, server.MaxConnectionsExceeded, server.MaxControlLineExceeded, server.MaxSubscriptionsExceeded,
		server.MissingAccount, server.AuthenticationExpired, server.Revocation:
		status = closeStatusPolicyViolation
	case server.TLSHandshakeError:
		status = closeStatusTLSHandshake
	case server.ParseError, server.ProtocolViolation, server.BadClientProtocolVersion:
		status = closeStatusProtocolError
	case server.MaxPayloadExceeded:
		status = closeStatusMessageTooBig
	case server.ServerShutdown:
		status = closeStatusGoingAway
	case server.WriteError, server.ReadError, server.StaleConnection:
		status = closeStatusAbnormalClosure
	default:
		status = closeStatusInternalSrvError
	}
	body := createCloseMessage(status, reason.String())
	c.wsEnqueueControlMessageLocked(closeMessage, body)
}

// Create and then enqueue a close message with a protocol error and the
// given message. This is invoked when parsing websocket frames.
//
// Lock MUST NOT be held on entry.
func (c *server.client) handleProtocolError(message string) error {
	buf := createCloseMessage(closeStatusProtocolError, message)
	c.wsEnqueueControlMessage(closeMessage, buf)
	return fmt.Errorf(message)
}

func (c *server.client) collapsePtoNB() (net.Buffers, int64) {
	var nb net.Buffers
	var mfs int
	var usz int
	if c.ws.browser {
		mfs = frameSizeForBrowsers
	}
	if len(c.out.p) > 0 {
		p := c.out.p
		c.out.p = nil
		nb = append(c.out.nb, p)
	} else if len(c.out.nb) > 0 {
		nb = c.out.nb
	}
	mask := c.ws.maskwrite
	// Start with possible already framed buffers (that we could have
	// got from partials or control messages such as ws pings or pongs).
	bufs := c.ws.frames
	compress := c.ws.compress
	if compress && len(nb) > 0 {
		// First, make sure we don't compress for very small cumulative buffers.
		for _, b := range nb {
			usz += len(b)
		}
		if usz <= compressThreshold {
			compress = false
		}
	}
	if compress && len(nb) > 0 {
		// Overwrite mfs if this connection does not support fragmented compressed frames.
		if mfs > 0 && c.ws.nocompfrag {
			mfs = 0
		}
		buf := &bytes.Buffer{}

		cp := c.ws.compressor
		if cp == nil {
			c.ws.compressor, _ = flate.NewWriter(buf, flate.BestSpeed)
			cp = c.ws.compressor
		} else {
			cp.Reset(buf)
		}
		var csz int
		for _, b := range nb {
			cp.Write(b)
		}
		if err := cp.Flush(); err != nil {
			c.Errorf("Error during compression: %v", err)
			c.markConnAsClosed(server.WriteError)
			return nil, 0
		}
		b := buf.Bytes()
		p := b[:len(b)-4]
		if mfs > 0 && len(p) > mfs {
			for first, final := true, false; len(p) > 0; first = false {
				lp := len(p)
				if lp > mfs {
					lp = mfs
				} else {
					final = true
				}
				fh := make([]byte, maxFrameHeaderSize)
				// Only the first frame should be marked as compressed, so pass
				// `first` for the compressed boolean.
				n, key := fillFrameHeader(fh, mask, first, final, first, binaryMessage, lp)
				if mask {
					maskBuf(key, p[:lp])
				}
				bufs = append(bufs, fh[:n], p[:lp])
				csz += n + lp
				p = p[lp:]
			}
		} else {
			h, key := createFrameHeader(mask, true, binaryMessage, len(p))
			if mask {
				maskBuf(key, p)
			}
			bufs = append(bufs, h, p)
			csz = len(h) + len(p)
		}
		// Add to pb the compressed data size (including headers), but
		// remove the original uncompressed data size that was added
		// during the queueing.
		c.out.pb += int64(csz) - int64(usz)
		c.ws.fs += int64(csz)
	} else if len(nb) > 0 {
		var total int
		if mfs > 0 {
			// We are limiting the frame size.
			startFrame := func() int {
				bufs = append(bufs, make([]byte, maxFrameHeaderSize))
				return len(bufs) - 1
			}
			endFrame := func(idx, size int) {
				n, key := fillFrameHeader(bufs[idx], mask, firstFrame, finalFrame, uncompressedFrame, binaryMessage, size)
				c.out.pb += int64(n)
				c.ws.fs += int64(n + size)
				bufs[idx] = bufs[idx][:n]
				if mask {
					maskBufs(key, bufs[idx+1:])
				}
			}

			fhIdx := startFrame()
			for i := 0; i < len(nb); i++ {
				b := nb[i]
				if total+len(b) <= mfs {
					bufs = append(bufs, b)
					total += len(b)
					continue
				}
				for len(b) > 0 {
					endStart := total != 0
					if endStart {
						endFrame(fhIdx, total)
					}
					total = len(b)
					if total >= mfs {
						total = mfs
					}
					if endStart {
						fhIdx = startFrame()
					}
					bufs = append(bufs, b[:total])
					b = b[total:]
				}
			}
			if total > 0 {
				endFrame(fhIdx, total)
			}
		} else {
			// If there is no limit on the frame size, create a single frame for
			// all pending buffers.
			for _, b := range nb {
				total += len(b)
			}
			wsfh, key := createFrameHeader(mask, false, binaryMessage, total)
			c.out.pb += int64(len(wsfh))
			bufs = append(bufs, wsfh)
			idx := len(bufs)
			bufs = append(bufs, nb...)
			if mask {
				maskBufs(key, bufs[idx:])
			}
			c.ws.fs += int64(len(wsfh) + total)
		}
	}
	if len(c.ws.closeMsg) > 0 {
		bufs = append(bufs, c.ws.closeMsg)
		c.ws.fs += int64(len(c.ws.closeMsg))
		c.ws.closeMsg = nil
	}
	c.ws.frames = nil
	return bufs, c.ws.fs
}

// setOriginOptions creates or updates the existing map
func (s *server.Server) setOriginOptions(o *server.WebsocketOpts) {
	ws := &s.websocket
	ws.mu.Lock()
	defer ws.mu.Unlock()
	// Copy over the option's same origin boolean
	ws.sameOrigin = o.SameOrigin
	// Reset the map. Will help for config reload if/when we support it.
	ws.allowedOrigins = nil
	if o.AllowedOrigins == nil {
		return
	}
	for _, ao := range o.AllowedOrigins {
		// We have previously checked (during options validation) that the urls
		// are parseable, but if we get an error, report and skip.
		u, err := url.ParseRequestURI(ao)
		if err != nil {
			s.Errorf("error parsing allowed origin: %v", err)
			continue
		}
		h, p, _ := getHostAndPort(u.Scheme == "https", u.Host)
		if ws.allowedOrigins == nil {
			ws.allowedOrigins = make(map[string]*allowedOrigin, len(o.AllowedOrigins))
		}
		ws.allowedOrigins[h] = &allowedOrigin{scheme: u.Scheme, port: p}
	}
}

// configAuth Given the websocket options, we check if any auth configuration
// has been provided. If so, possibly create users/nkey users and
// store them in s.websocket.users/nkeys.
// Also update a boolean that indicates if auth is required for
// websocket clients.
// Server lock is held on entry.
func (s *server.Server) configAuth(opts *server.WebsocketOpts) {
	ws := &s.websocket
	// If any of those is specified, we consider that there is an override.
	ws.authOverride = opts.Username != server._EMPTY_ || opts.Token != server._EMPTY_ || opts.NoAuthUser != server._EMPTY_
}

func (s *server.Server) StartWebsocketServer() {
	sopts := s.getOpts()
	o := &sopts.Websocket

	s.wsSetOriginOptions(o)

	var hl net.Listener
	var proto string
	var err error

	port := o.Port
	if port == -1 {
		port = 0
	}
	hp := net.JoinHostPort(o.Host, strconv.Itoa(port))

	// We are enforcing (when validating the options) the use of TLS, but the
	// code was originally supporting both modes. The reason for TLS only is
	// that we expect users to send JWTs with bearer tokens and we want to
	// avoid the possibility of it being "intercepted".

	s.mu.Lock()
	if s.shutdown {
		s.mu.Unlock()
		return
	}
	// Do not check o.NoTLS here. If a TLS configuration is available, use it,
	// regardless of NoTLS. If we don't have a TLS config, it means that the
	// user has configured NoTLS because otherwise the Server would have failed
	// to start due to options validation.
	if o.TLSConfig != nil {
		proto = SchemePrefixTLS
		config := o.TLSConfig.Clone()
		config.GetConfigForClient = s.wsGetTLSConfig
		hl, err = tls.Listen("tcp", hp, config)
	} else {
		proto = SchemePrefix
		hl, err = net.Listen("tcp", hp)
	}
	s.websocket.listenerErr = err
	if err != nil {
		s.mu.Unlock()
		s.Fatalf("Unable to listen for websocket connections: %v", err)
		return
	}
	if port == 0 {
		o.Port = hl.Addr().(*net.TCPAddr).Port
	}
	s.Noticef("Listening for websocket clients on %s://%s:%d", proto, o.Host, o.Port)
	if proto == SchemePrefix {
		s.Warnf("Websocket not configured with TLS. DO NOT USE IN PRODUCTION!")
	}

	s.websocket.tls = proto == "wss"
	s.websocket.connectURLs, err = s.getConnectURLs(o.Advertise, o.Host, o.Port)
	if err != nil {
		s.Fatalf("Unable to get websocket connect URLs: %v", err)
		hl.Close()
		s.mu.Unlock()
		return
	}
	hasLeaf := sopts.LeafNode.Port != 0
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		res, err := s.wsUpgrade(w, r)
		if err != nil {
			s.Errorf(err.Error())
			return
		}
		switch res.kind {
		case server.CLIENT:
			s.createWSClient(res.conn, res.ws)
		case server.MQTT:
			s.createMQTTClient(res.conn, res.ws)
		case server.LEAF:
			if !hasLeaf {
				s.Errorf("Not configured to accept leaf node connections")
				// Silently close for now. If we want to send an error back, we would
				// need to create the leafnode client anyway, so that is is handling websocket
				// frames, then send the error to the remote.
				res.conn.Close()
				return
			}
			s.createLeafNode(res.conn, nil, nil, res.ws)
		}
	})
	hs := &http.Server{
		Addr:        hp,
		Handler:     mux,
		ReadTimeout: o.HandshakeTimeout,
		ErrorLog:    log.New(&server.captureHTTPServerLog{s, "websocket: "}, server._EMPTY_, 0),
	}
	s.websocket.Server = hs
	s.websocket.listener = hl
	go func() {
		if err := hs.Serve(hl); err != http.ErrServerClosed {
			s.Fatalf("websocket Listener error: %v", err)
		}
		if s.isLameDuckMode() {
			// Signal that we are not accepting new clients
			s.ldmCh <- true
			// Now wait for the Shutdown...
			<-s.quitCh
			return
		}
		s.done <- true
	}()
	s.mu.Unlock()
}

// The TLS configuration is passed to the Listener when the websocket
// "Server" is setup. That prevents TLS configuration updates on reload
// from being used. By setting this function in Tls.Config.GetConfigForClient
// we instruct the TLS handshake to ask for the Tls configuration to be
// used for a specific client. We don't care which client, we always use
// the same TLS configuration.
func (s *server.Server) getTLSConfig(_ *tls.ClientHelloInfo) (*tls.Config, error) {
	opts := s.getOpts()
	return opts.Websocket.TLSConfig, nil
}

// This is similar to createClient() but has some modifications
// specific to handle websocket clients.
// The comments have been kept to minimum to reduce code size.
// Check createClient() for more details.
func (s *server.Server) createWSClient(conn net.Conn, ws *Websocket) *server.client {
	opts := s.getOpts()

	maxPay := int32(opts.MaxPayload)
	maxSubs := int32(opts.MaxSubs)
	if maxSubs == 0 {
		maxSubs = -1
	}
	now := time.Now().UTC()

	c := &server.client{srv: s, nc: conn, opts: server.defaultOpts, mpay: maxPay, msubs: maxSubs, start: now, last: now, ws: ws}

	c.registerWithAccount(s.globalAccount())

	var info server.Info
	var authRequired bool

	s.mu.Lock()
	info = s.copyInfo()
	// Check auth, override if applicable.
	if !info.AuthRequired {
		// Set info.AuthRequired since this is what is sent to the client.
		info.AuthRequired = s.websocket.authOverride
	}
	if s.nonceRequired() {
		var raw [server.nonceLen]byte
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
		c.flags.set(server.expectConnect)
	}
	c.initClient()
	c.Debugf("Client connection created")
	c.sendProtoNow(c.generateClientInfoJSON(info))
	c.mu.Unlock()

	s.mu.Lock()
	if !s.running || s.ldm {
		if s.shutdown {
			conn.Close()
		}
		s.mu.Unlock()
		return c
	}

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

	if c.isClosed() {
		c.mu.Unlock()
		c.closeConnection(server.WriteError)
		return nil
	}

	if authRequired {
		timeout := opts.AuthTimeout
		// Possibly override with Websocket specific value.
		if opts.Websocket.AuthTimeout != 0 {
			timeout = opts.Websocket.AuthTimeout
		}
		c.setAuthTimer(server.secondsToDuration(timeout))
	}

	c.setPingTimer()

	s.startGoRoutine(func() { c.readLoop(nil) })
	s.startGoRoutine(func() { c.writeLoop() })

	c.mu.Unlock()

	return c
}

// Process websocket client handshake. On success, returns the raw net.Conn that
// will be used to create a *client object.
// Invoked from the HTTP Server listening on websocket port.
func (s *server.Server) upgrade(w http.ResponseWriter, r *http.Request) (*upgradeResult, error) {
	kind := server.CLIENT
	if r.URL != nil {
		ep := r.URL.EscapedPath()
		if strings.HasPrefix(ep, server.leafNodeWSPath) {
			kind = server.LEAF
		} else if strings.HasPrefix(ep, server.mqttWSPath) {
			kind = server.MQTT
		}
	}

	opts := s.getOpts()

	// From https://tools.ietf.org/html/rfc6455#section-4.2.1
	// Point 1.
	if r.Method != "GET" {
		return nil, returnHTTPError(w, r, http.StatusMethodNotAllowed, "request method must be GET")
	}
	// Point 2.
	if r.Host == server._EMPTY_ {
		return nil, returnHTTPError(w, r, http.StatusBadRequest, "'Host' missing in request")
	}
	// Point 3.
	if !headerContains(r.Header, "Upgrade", "websocket") {
		return nil, returnHTTPError(w, r, http.StatusBadRequest, "invalid value for header 'Upgrade'")
	}
	// Point 4.
	if !headerContains(r.Header, "Connection", "Upgrade") {
		return nil, returnHTTPError(w, r, http.StatusBadRequest, "invalid value for header 'Connection'")
	}
	// Point 5.
	key := r.Header.Get("Sec-Websocket-Key")
	if key == server._EMPTY_ {
		return nil, returnHTTPError(w, r, http.StatusBadRequest, "key missing")
	}
	// Point 6.
	if !headerContains(r.Header, "Sec-Websocket-Version", "13") {
		return nil, returnHTTPError(w, r, http.StatusBadRequest, "invalid version")
	}
	// Others are optional
	// Point 7.
	if err := s.websocket.checkOrigin(r); err != nil {
		return nil, returnHTTPError(w, r, http.StatusForbidden, fmt.Sprintf("origin not allowed: %v", err))
	}
	// Point 8.
	// We don't have protocols, so ignore.
	// Point 9.
	// Extensions, only support for compression at the moment
	compress := opts.Websocket.Compression
	if compress {
		// Simply check if permessage-deflate extension is present.
		compress, _ = pMCExtensionSupport(r.Header, true)
	}
	// We will do masking if asked (unless we reject for tests)
	noMasking := r.Header.Get(noMaskingHeader) == noMaskingValue && !testRejectNoMasking

	h := w.(http.Hijacker)
	conn, brw, err := h.Hijack()
	if err != nil {
		if conn != nil {
			conn.Close()
		}
		return nil, returnHTTPError(w, r, http.StatusInternalServerError, err.Error())
	}
	if brw.Reader.Buffered() > 0 {
		conn.Close()
		return nil, returnHTTPError(w, r, http.StatusBadRequest, "client sent data before handshake is complete")
	}

	var buf [1024]byte
	p := buf[:0]

	// From https://tools.ietf.org/html/rfc6455#section-4.2.2
	p = append(p, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: "...)
	p = append(p, acceptKey(key)...)
	p = append(p, server._CRLF_...)
	if compress {
		p = append(p, pMCFullResponse...)
	}
	if noMasking {
		p = append(p, noMaskingFullResponse...)
	}
	if kind == server.MQTT {
		p = append(p, wsMQTTSecProto...)
	}
	p = append(p, server._CRLF_...)

	if _, err = conn.Write(p); err != nil {
		conn.Close()
		return nil, err
	}
	// If there was a deadline set for the handshake, clear it now.
	if opts.Websocket.HandshakeTimeout > 0 {
		conn.SetDeadline(time.Time{})
	}
	// Server always expect "clients" to send masked payload, unless the option
	// "no-masking" has been enabled.
	ws := &Websocket{compress: compress, maskread: !noMasking}

	// Check for X-Forwarded-For header
	if cips, ok := r.Header[xForwardedForHeader]; ok {
		cip := cips[0]
		if net.ParseIP(cip) != nil {
			ws.clientIP = cip
		}
	}

	if kind == server.CLIENT || kind == server.MQTT {
		// Indicate if this is likely coming from a browser.
		if ua := r.Header.Get("User-Agent"); ua != server._EMPTY_ && strings.HasPrefix(ua, "Mozilla/") {
			ws.browser = true
			// Disable fragmentation of compressed frames for Safari browsers.
			// Unfortunately, you could be running Chrome on macOS and this
			// string will contain "Safari/" (along "Chrome/"). However, what
			// I have found is that actual Safari browser also have "Version/".
			// So make the combination of the two.
			ws.nocompfrag = ws.compress && strings.Contains(ua, "Version/") && strings.Contains(ua, "Safari/")
		}
		if opts.Websocket.JWTCookie != server._EMPTY_ {
			if c, err := r.Cookie(opts.Websocket.JWTCookie); err == nil && c != nil {
				ws.cookieJwt = c.Value
			}
		}
	}
	return &upgradeResult{conn: conn, ws: ws, kind: kind}, nil
}

// ValidateWebsocketOptions validates the websocket related options.
func ValidateWebsocketOptions(o *server.Options) error {
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
		if err := server.ValidateNoAuthUser(o, wo.NoAuthUser); err != nil {
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
	if err := server.ValidatePinnedCerts(wo.TLSPinnedCerts); err != nil {
		return fmt.Errorf("websocket: %v", err)
	}
	return nil
}
