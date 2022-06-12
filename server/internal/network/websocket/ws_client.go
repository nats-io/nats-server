package websocket

import (
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats-server/v2/server/internal/util"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

type client struct {
	ws Websocket
	mu sync.Mutex
}

// handleControlFrame Handles the PING, PONG and CLOSE websocket control frames.
//
// Client lock MUST NOT be held on entry.
func (c *client) handleControlFrame(r *readInfo, frameType opCode, nc io.Reader, buf []byte, pos int) (int, error) {
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
		c.enqueueControlMessage(closeMessage, createCloseMessage(status, body))
		// Return io.EOF so that readLoop will close the connection as ClientClosed
		// after processing pending buffers.
		return pos, io.EOF
	case pingMessage:
		c.enqueueControlMessage(wsPongMessage, payload)
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
func (c *client) wsRead(r *readInfo, ior io.Reader, buf []byte) ([][]byte, error) {
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
				return bufs, c.handleProtocolError("mask bit missing")
			}

			// Store size in case it is < 125
			r.rem = int(b1 & 0x7F)

			switch frameType {
			case pingMessage, wsPongMessage, closeMessage:
				if r.rem > maxControlPayloadSize {
					return bufs, c.handleProtocolError(
						fmt.Sprintf("control frame length bigger than maximum allowed of %v bytes",
							maxControlPayloadSize))
				}
				if !final {
					return bufs, c.handleProtocolError("control frame does not have final bit set")
				}
			case textMessage, binaryMessage:
				if !r.ff {
					return bufs, c.handleProtocolError("new message started before final frame for previous message was received")
				}
				r.ff = final
				r.fc = compressed
			case continuationFrame:
				// Compressed bit must be only set in the first frame
				if r.ff || compressed {
					return bufs, c.handleProtocolError("invalid continuation frame")
				}
				r.ff = final
			default:
				return bufs, c.handleProtocolError(fmt.Sprintf("unknown opcode %v", frameType))
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
				pos, err = c.handleControlFrame(r, frameType, ior, buf, pos)
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
func (c *client) enqueueControlMessageLocked(controlMsg opCode, payload []byte) {
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
		c.ws.CloseSent = true
		c.ws.CloseMsg = cm
	} else {
		c.ws.frames = append(c.ws.frames, cm)
		c.ws.fs += int64(len(cm))
	}
	c.flushSignal()
}

// Invokes enqueueControlMessageLocked under client lock.
//
// Client lock MUST NOT be held on entry
func (c *client) enqueueControlMessage(controlMsg opCode, payload []byte) {
	c.mu.Lock()
	c.enqueueControlMessageLocked(controlMsg, payload)
	c.mu.Unlock()
}

// Enqueues a websocket close message with a status mapped from the given `reason`.
//
// Client lock held on entry
func (c *client) enqueueCloseMessage(reason server.ClosedState) {
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
	c.enqueueControlMessageLocked(closeMessage, body)
}

// Create and then enqueue a close message with a protocol error and the
// given message. This is invoked when parsing websocket frames.
//
// Lock MUST NOT be held on entry.
func (c *client) handleProtocolError(message string) error {
	buf := createCloseMessage(closeStatusProtocolError, message)
	c.enqueueControlMessage(closeMessage, buf)
	return fmt.Errorf(message)
}

func (c *client) collapsePtoNB() (net.Buffers, int64) {
	var nb net.Buffers
	var mfs int
	var usz int
	if c.ws.Browser {
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
	compress := c.ws.Compress
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
		if mfs > 0 && c.ws.Nocompfrag {
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
	if len(c.ws.CloseMsg) > 0 {
		bufs = append(bufs, c.ws.CloseMsg)
		c.ws.fs += int64(len(c.ws.CloseMsg))
		c.ws.CloseMsg = nil
	}
	c.ws.frames = nil
	return bufs, c.ws.fs
}

// setOriginOptions creates or updates the existing map
func (ws *SrvWebsocket) setOriginOptions(o *WebsocketOpts) {
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
			// todo: wait to use global log in another pr
			//s.Errorf("error parsing allowed origin: %v", err)
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
func (ws *SrvWebsocket) configAuth(opts *WebsocketOpts) {
	// If any of those is specified, we consider that there is an override.
	ws.AuthOverride = opts.Username != _EMPTY_ || opts.Token != _EMPTY_ || opts.NoAuthUser != _EMPTY_
}

// ProcessSrvWebsocket modify the ws as the config passed, the ws could run except the error is not nil
// should check s.shutdown(unfinished)
func (ws *SrvWebsocket) ProcessSrvWebsocket(preparedServer *http.Server, o *WebsocketOpts) error {

	ws.setOriginOptions(o)

	var hl net.Listener
	var proto string
	var err error

	port := o.Port
	if port == -1 {
		port = 0
	}
	hp := net.JoinHostPort(o.Host, strconv.Itoa(port))
	preparedServer.Addr = hp

	//ws.mu.Lock()
	//if ws.shutdown {
	//	ws.mu.Unlock()
	//	return
	//}

	// Do not check o.NoTLS here. If a TLS configuration is available, use it,
	// regardless of NoTLS. If we don't have a TLS config, it means that the
	// user has configured NoTLS because otherwise the Server would have failed
	// to start due to options validation.
	if o.TLSConfig != nil {
		proto = SchemePrefixTLS
		config := o.TLSConfig.Clone()
		config.GetConfigForClient = o.getTLSConfig
		hl, err = tls.Listen("tcp", hp, config)
	} else {
		proto = SchemePrefix
		hl, err = net.Listen("tcp", hp)
	}
	ws.listenerErr = err
	if err != nil {
		ws.mu.Unlock()
		//ws.Fatalf("Unable to listen for websocket connections: %v", err)
		return err
	}
	if port == 0 {
		o.Port = hl.Addr().(*net.TCPAddr).Port
	}
	//ws.Noticef("Listening for websocket clients on %ws://%ws:%d", proto, o.Host, o.Port)
	if proto == SchemePrefix {
		//ws.Warnf("Websocket not configured with TLS. DO NOT USE IN PRODUCTION!")
	}

	ws.Tls = proto == "wss"
	ws.ConnectURLs, err = util.GetConnectURLs(o.Advertise, o.Host, o.Port)
	if err != nil {
		//ws.Fatalf("Unable to get websocket connect URLs: %v", err)
		hl.Close()
		ws.mu.Unlock()
		return err
	}
	ws.Server = preparedServer
	ws.Listener = hl
	ws.mu.Unlock()
	return nil
}

// The TLS configuration is passed to the Listener when the websocket
// "Server" is setup. That prevents TLS configuration updates on reload
// from being used. By setting this function in Tls.Config.GetConfigForClient
// we instruct the TLS handshake to ask for the Tls configuration to be
// used for a specific client. We don't care which client, we always use
// the same TLS configuration.
func (opts *WebsocketOpts) getTLSConfig(_ *tls.ClientHelloInfo) (*tls.Config, error) {
	return opts.TLSConfig, nil
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

// ReadLoop is the websocket read functionality.
// Runs in its own Go routine.
func (c *client) ReadLoop(pre []byte, done chan struct{}) {
	// Grab the connection off the client, it will be cleared on a close.
	// We check for that after the loop, but want to avoid a nil dereference
	c.mu.Lock()
	defer func() {
		done <- struct{}{}
		close(done)
	}()
	if c.isClosed() {
		c.mu.Unlock()
		return
	}
	nc := c.nc
	//ws := c.isWebsocket()
	//if c.isMqtt() {
	//	c.mqtt.r = &mqttReader{reader: nc}
	//}
	c.in.rsz = startBufSize

	// Check the per-account-cache for closed subscriptions
	cpacc := c.kind == ROUTER || c.kind == GATEWAY
	// Last per-account-cache check for closed subscriptions
	lpacc := time.Now()
	acc := c.acc
	var masking bool
	masking = c.ws.Maskread
	c.mu.Unlock()

	defer func() {
		// These are used only in the readloop, so we can set them to nil
		// on exit of the readLoop.
		c.in.results, c.in.pacache = nil, nil
	}()

	// Start read buffer.
	b := make([]byte, c.in.rsz)

	// Websocket clients will return several slices if there are multiple
	// websocket frames in the blind read. For non WS clients though, we
	// will always have 1 slice per loop iteration. So we define this here
	// so non WS clients will use bufs[0] = b[:n].
	var _bufs [1][]byte
	bufs := _bufs[:1]

	var wsr *wsReadInfo
	if ws {
		wsr = &wsReadInfo{mask: masking}
		wsr.init()
	}

	for {
		var n int
		var err error

		// If we have a pre buffer parse that first.
		if len(pre) > 0 {
			b = pre
			n = len(pre)
			pre = nil
		} else {
			n, err = nc.Read(b)
			// If we have any data we will try to parse and exit at the end.
			if n == 0 && err != nil {
				c.closeConnection(closedStateForErr(err))
				return
			}
		}
		if ws {
			bufs, err = c.wsRead(wsr, nc, b[:n])
			if bufs == nil && err != nil {
				if err != io.EOF {
					c.Errorf("read error: %v", err)
				}
				c.closeConnection(closedStateForErr(err))
			} else if bufs == nil {
				continue
			}
		} else {
			bufs[0] = b[:n]
		}
		start := time.Now()

		// Check if the account has mappings and if so set the local readcache flag.
		// We check here to make sure any changes such as config reload are reflected here.
		if c.kind == CLIENT || c.kind == LEAF {
			if acc.hasMappings() {
				c.in.flags.set(hasMappings)
			} else {
				c.in.flags.clear(hasMappings)
			}
		}

		// Clear inbound stats cache
		c.in.msgs = 0
		c.in.bytes = 0
		c.in.subs = 0

		// Main call into parser for inbound data. This will generate callouts
		// to process messages, etc.
		for i := 0; i < len(bufs); i++ {
			if err := c.parse(bufs[i]); err != nil {
				if err == ErrMinimumVersionRequired {
					// Special case here, currently only for leaf node connections.
					// When process the CONNECT protocol, if the minimum version
					// required was not met, an error was printed and sent back to
					// the remote, and connection was closed after a certain delay
					// (to avoid "rapid" reconnection from the remote).
					// We don't need to do any of the things below, simply return.
					return
				}
				if dur := time.Since(start); dur >= readLoopReportThreshold {
					c.Warnf("Readloop processing time: %v", dur)
				}
				// Need to call flushClients because some of the clients have been
				// assigned messages and their "fsp" incremented, and need now to be
				// decremented and their writeLoop signaled.
				c.flushClients(0)
				// handled inline
				if err != ErrMaxPayload && err != ErrAuthentication {
					c.Error(err)
					c.closeConnection(ProtocolViolation)
				}
				return
			}
		}

		// Updates stats for client and server that were collected
		// from parsing through the buffer.
		if c.in.msgs > 0 {
			atomic.AddInt64(&c.inMsgs, int64(c.in.msgs))
			atomic.AddInt64(&c.inBytes, int64(c.in.bytes))
			atomic.AddInt64(&s.inMsgs, int64(c.in.msgs))
			atomic.AddInt64(&s.inBytes, int64(c.in.bytes))
		}

		// Signal to writeLoop to flush to socket.
		last := c.flushClients(0)

		// Update activity, check read buffer size.
		c.mu.Lock()

		// Activity based on interest changes or data/msgs.
		if c.in.msgs > 0 || c.in.subs > 0 {
			c.last = last
		}

		if n >= cap(b) {
			c.in.srs = 0
		} else if n < cap(b)/2 { // divide by 2 b/c we want less than what we would shrink to.
			c.in.srs++
		}

		// Update read buffer size as/if needed.
		if n >= cap(b) && cap(b) < maxBufSize {
			// Grow
			c.in.rsz = int32(cap(b) * 2)
			b = make([]byte, c.in.rsz)
		} else if n < cap(b) && cap(b) > minBufSize && c.in.srs > shortsToShrink {
			// Shrink, for now don't accelerate, ping/pong will eventually sort it out.
			c.in.rsz = int32(cap(b) / 2)
			b = make([]byte, c.in.rsz)
		}
		// re-snapshot the account since it can change during reload, etc.
		acc = c.acc
		// Refresh nc because in some cases, we have upgraded c.nc to TLS.
		nc = c.nc
		c.mu.Unlock()

		// Connection was closed
		if nc == nil {
			return
		}

		if dur := time.Since(start); dur >= readLoopReportThreshold {
			c.Warnf("Readloop processing time: %v", dur)
		}

		// We could have had a read error from above but still read some data.
		// If so do the close here unconditionally.
		if err != nil {
			c.closeConnection(closedStateForErr(err))
			return
		}

		if cpacc && (start.Sub(lpacc)) >= closedSubsCheckInterval {
			c.pruneClosedSubFromPerAccountCache()
			lpacc = time.Now()
		}
	}
}
