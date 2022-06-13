package websocket

import (
	"bytes"
	"compress/flate"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"github.com/nats-io/nats-server/v2/server/internal/network"
	"github.com/nats-io/nats-server/v2/server/internal/util"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"unicode/utf8"
)

type Client struct {
	Ws  *Websocket
	Mu  *sync.Mutex
	Out *network.Outbound
	Nc  net.Conn
}

// Returns a slice of byte slices corresponding to payload of websocket Frames.
// The byte slice `buf` is filled with bytes from the connection's read loop.
// This function will decode the frame headers and unmask the payload(s).
// It is possible that the returned slices point to the given `buf` slice, so
// `buf` should not be overwritten until the returned slices have been parsed.
//
// Client lock MUST NOT be held on entry.
func (c *Client) Read(r *ReadInfo, ior io.Reader, buf []byte) ([][]byte, error) {
	var (
		bufs   [][]byte
		tmpBuf []byte
		err    error
		pos    int
		max    = len(buf)
	)
	for pos != max {
		if r.Fs {
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
			if r.Mask && b1&maskBit == 0 {
				return bufs, c.handleProtocolError("mask bit missing")
			}

			// Store size in case it is < 125
			r.Rem = int(b1 & 0x7F)

			switch frameType {
			case pingMessage, wsPongMessage, closeMessage:
				if r.Rem > maxControlPayloadSize {
					return bufs, c.handleProtocolError(
						fmt.Sprintf("control frame length bigger than maximum allowed of %v bytes",
							maxControlPayloadSize))
				}
				if !final {
					return bufs, c.handleProtocolError("control frame does not have final bit set")
				}
			case textMessage, binaryMessage:
				if !r.Ff {
					return bufs, c.handleProtocolError("new message started before final frame for previous message was received")
				}
				r.Ff = final
				r.Fc = compressed
			case continuationFrame:
				// Compressed bit must be only set in the first frame
				if r.Ff || compressed {
					return bufs, c.handleProtocolError("invalid continuation frame")
				}
				r.Ff = final
			default:
				return bufs, c.handleProtocolError(fmt.Sprintf("unknown opcode %v", frameType))
			}

			switch r.Rem {
			case 126:
				tmpBuf, pos, err = get(ior, buf, pos, 2)
				if err != nil {
					return bufs, err
				}
				r.Rem = int(binary.BigEndian.Uint16(tmpBuf))
			case 127:
				tmpBuf, pos, err = get(ior, buf, pos, 8)
				if err != nil {
					return bufs, err
				}
				r.Rem = int(binary.BigEndian.Uint64(tmpBuf))
			}

			if r.Mask {
				// Read masking key
				tmpBuf, pos, err = get(ior, buf, pos, 4)
				if err != nil {
					return bufs, err
				}
				copy(r.Mkey[:], tmpBuf)
				r.Mkpos = 0
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
			r.Fs = false
		}
		if pos < max {
			var b []byte
			var n int

			n = r.Rem
			if pos+n > max {
				n = max - pos
			}
			b = buf[pos : pos+n]
			pos += n
			r.Rem -= n
			// If needed, unmask the buffer
			if r.Mask {
				r.unmask(b)
			}
			addToBufs := true
			// Handle compressed message
			if r.Fc {
				// Assume that we may have continuation Frames or not the full payload.
				addToBufs = false
				// Make a copy of the buffer before adding it to the list
				// of compressed fragments.
				r.Cbufs = append(r.Cbufs, append([]byte(nil), b...))
				// When we have the final frame and we have read the full payload,
				// we can decompress it.
				if r.Ff && r.Rem == 0 {
					b, err = r.decompress()
					if err != nil {
						return bufs, err
					}
					r.Fc = false
					// Now we can add to `bufs`
					addToBufs = true
				}
			}
			// For non compressed Frames, or when we have decompressed the
			// whole message.
			if addToBufs {
				bufs = append(bufs, b)
			}
			// If payload has been fully read, then indicate that next
			// is the start of a frame.
			if r.Rem == 0 {
				r.Fs = true
			}
		}
	}
	return bufs, nil
}

// CollapsePtoNB returns error when it fails to compress the pb.
func (c *Client) CollapsePtoNB() (net.Buffers, int64, error) {
	var nb net.Buffers
	var mfs int
	var usz int
	if c.Ws.Browser {
		mfs = frameSizeForBrowsers
	}
	if len(c.Out.P) > 0 {
		p := c.Out.P
		c.Out.P = nil
		nb = append(c.Out.Nb, p)
	} else if len(c.Out.Nb) > 0 {
		nb = c.Out.Nb
	}
	mask := c.Ws.Maskwrite
	// Start with possible already framed buffers (that we could have
	// got from partials or control messages such as Ws pings or pongs).
	bufs := c.Ws.Frames
	compress := c.Ws.Compress
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
		// Overwrite mfs if this connection does not support fragmented compressed Frames.
		if mfs > 0 && c.Ws.Nocompfrag {
			mfs = 0
		}
		buf := &bytes.Buffer{}

		cp := c.Ws.compressor
		if cp == nil {
			c.Ws.compressor, _ = flate.NewWriter(buf, flate.BestSpeed)
			cp = c.Ws.compressor
		} else {
			cp.Reset(buf)
		}
		var csz int
		for _, b := range nb {
			cp.Write(b)
		}
		if err := cp.Flush(); err != nil {
			return nil, 0, fmt.Errorf("error during compression: %w", err)
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
		c.Out.Pb += int64(csz) - int64(usz)
		c.Ws.Fs += int64(csz)
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
				c.Out.Pb += int64(n)
				c.Ws.Fs += int64(n + size)
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
			c.Out.Pb += int64(len(wsfh))
			bufs = append(bufs, wsfh)
			idx := len(bufs)
			bufs = append(bufs, nb...)
			if mask {
				maskBufs(key, bufs[idx:])
			}
			c.Ws.Fs += int64(len(wsfh) + total)
		}
	}
	if len(c.Ws.CloseMsg) > 0 {
		bufs = append(bufs, c.Ws.CloseMsg)
		c.Ws.Fs += int64(len(c.Ws.CloseMsg))
		c.Ws.CloseMsg = nil
	}
	c.Ws.Frames = nil
	return bufs, c.Ws.Fs, nil
}

// Enqueues a websocket control message.
// If the control message is a closeMessage, then marks this client
// has having sent the close message (since only one should be sent).
// This will prevent the generic closeConnection() to enqueue one.
//
// Client lock held on entry.
func (c *Client) enqueueControlMessageLocked(controlMsg opCode, payload []byte) {
	// Control messages are never compressed and their size will be
	// less than maxControlPayloadSize, which means the frame header
	// will be only 2 or 6 bytes.
	useMasking := c.Ws.Maskwrite
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
	c.Out.Pb += int64(len(cm))
	if controlMsg == closeMessage {
		// We can't add the close message to the Frames buffers
		// now. It will be done on a flushOutbound() when there
		// are no more pending buffers to send.
		c.Ws.CloseSent = true
		c.Ws.CloseMsg = cm
	} else {
		c.Ws.Frames = append(c.Ws.Frames, cm)
		c.Ws.Fs += int64(len(cm))
	}
	c.flushSignal()
}

// flushSignal will use server to queue the flush IO operation to a pool of flushers.
// Lock must be held.
func (c *Client) flushSignal() {
	c.Out.Sg.Signal()
}

// handleControlFrame Handles the PING, PONG and CLOSE websocket control Frames.
//
// Client lock MUST NOT be held on entry.
func (c *Client) handleControlFrame(r *ReadInfo, frameType opCode, nc io.Reader, buf []byte, pos int) (int, error) {
	var payload []byte
	var err error

	if r.Rem > 0 {
		payload, pos, err = get(nc, buf, pos, r.Rem)
		if err != nil {
			return pos, err
		}
		if r.Mask {
			r.unmask(payload)
		}
		r.Rem = 0
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

// Invokes enqueueControlMessageLocked under client lock.
//
// Client lock MUST NOT be held on entry
func (c *Client) enqueueControlMessage(controlMsg opCode, payload []byte) {
	c.Mu.Lock()
	c.enqueueControlMessageLocked(controlMsg, payload)
	c.Mu.Unlock()
}

// EnqueueCloseMessage enqueues a websocket close message with a status mapped from the given `reason`.
//
// Client lock held on entry
func (c *Client) EnqueueCloseMessage(reason ClosedState) {
	var status int
	switch reason {
	case ClientClosed:
		status = closeStatusNormalClosure
	case AuthenticationTimeout, AuthenticationViolation, SlowConsumerPendingBytes, SlowConsumerWriteDeadline,
		MaxAccountConnectionsExceeded, MaxConnectionsExceeded, MaxControlLineExceeded, MaxSubscriptionsExceeded,
		MissingAccount, AuthenticationExpired, Revocation:
		status = closeStatusPolicyViolation
	case TLSHandshakeError:
		status = closeStatusTLSHandshake
	case ParseError, ProtocolViolation, BadClientProtocolVersion:
		status = closeStatusProtocolError
	case MaxPayloadExceeded:
		status = closeStatusMessageTooBig
	case ServerShutdown:
		status = closeStatusGoingAway
	case WriteError, ReadError, StaleConnection:
		status = closeStatusAbnormalClosure
	default:
		status = closeStatusInternalSrvError
	}
	body := createCloseMessage(status, reason.String())
	c.enqueueControlMessageLocked(closeMessage, body)
}

// Create and then enqueue a close message with a protocol error and the
// given message. This is invoked when parsing websocket Frames.
//
// Lock MUST NOT be held on entry.
func (c *Client) handleProtocolError(message string) error {
	buf := createCloseMessage(closeStatusProtocolError, message)
	c.enqueueControlMessage(closeMessage, buf)
	return fmt.Errorf(message)
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

// ConfigAuth Given the websocket options, we check if any auth configuration
// has been provided. If so, possibly create users/nkey users and
// store them in s.websocket.users/nkeys.
// Also update a boolean that indicates if auth is required for
// websocket clients.
// Server lock is held on entry.
func (ws *SrvWebsocket) ConfigAuth(opts *WebsocketOpts) {
	// If any of those is specified, we consider that there is an override.
	ws.AuthOverride = opts.Username != _EMPTY_ || opts.Token != _EMPTY_ || opts.NoAuthUser != _EMPTY_
}

// ProcessSrvWebsocket modify the Ws as the config passed, the Ws could run except the error is not nil
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

	//Ws.Mu.Lock()
	//if Ws.shutdown {
	//	Ws.Mu.Unlock()
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
	ws.ListenerErr = err
	if err != nil {
		ws.mu.Unlock()
		//Ws.Fatalf("Unable to listen for websocket connections: %v", err)
		return err
	}
	if port == 0 {
		o.Port = hl.Addr().(*net.TCPAddr).Port
	}
	//Ws.Noticef("Listening for websocket clients on %Ws://%Ws:%d", proto, o.Host, o.Port)
	if proto == SchemePrefix {
		//Ws.Warnf("Websocket not configured with TLS. DO NOT USE IN PRODUCTION!")
	}

	ws.Tls = proto == "wss"
	ws.ConnectURLs, err = util.GetConnectURLs(o.Advertise, o.Host, o.Port)
	if err != nil {
		//Ws.Fatalf("Unable to get websocket connect URLs: %v", err)
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

//// ReadLoop is the websocket read functionality.
//// Runs in its own Go routine.
//func (c *Client) ReadLoop(pre []byte, done chan struct{}) {
//	// Grab the connection off the client, it will be cleared on a close.
//	// We check for that after the loop, but want to avoid a nil dereference
//	c.Mu.Lock()
//	defer func() {
//		done <- struct{}{}
//		close(done)
//	}()
//	if c.isClosed() {
//		c.Mu.Unlock()
//		return
//	}
//	Nc := c.Nc
//	//Ws := c.isWebsocket()
//	//if c.isMqtt() {
//	//	c.mqtt.r = &mqttReader{reader: Nc}
//	//}
//	c.in.rsz = startBufSize
//
//	// Check the per-account-cache for closed subscriptions
//	cpacc := c.kind == ROUTER || c.kind == GATEWAY
//	// Last per-account-cache check for closed subscriptions
//	lpacc := time.Now()
//	acc := c.acc
//	var masking bool
//	masking = c.Ws.Maskread
//	c.Mu.Unlock()
//
//	defer func() {
//		// These are used only in the readloop, so we can set them to nil
//		// on exit of the readLoop.
//		c.in.results, c.in.pacache = nil, nil
//	}()
//
//	// Start read buffer.
//	b := make([]byte, c.in.rsz)
//
//	// Websocket clients will return several slices if there are multiple
//	// websocket Frames in the blind read. For non WS clients though, we
//	// will always have 1 slice per loop iteration. So we define this here
//	// so non WS clients will use bufs[0] = b[:n].
//	var _bufs [1][]byte
//	bufs := _bufs[:1]
//
//	var wsr *wsReadInfo
//	if Ws {
//		wsr = &wsReadInfo{mask: masking}
//		wsr.init()
//	}
//
//	for {
//		var n int
//		var err error
//
//		// If we have a pre buffer parse that first.
//		if len(pre) > 0 {
//			b = pre
//			n = len(pre)
//			pre = nil
//		} else {
//			n, err = Nc.Read(b)
//			// If we have any data we will try to parse and exit at the end.
//			if n == 0 && err != nil {
//				c.closeConnection(closedStateForErr(err))
//				return
//			}
//		}
//		if Ws {
//			bufs, err = c.Read(wsr, Nc, b[:n])
//			if bufs == nil && err != nil {
//				if err != io.EOF {
//					c.Errorf("read error: %v", err)
//				}
//				c.closeConnection(closedStateForErr(err))
//			} else if bufs == nil {
//				continue
//			}
//		} else {
//			bufs[0] = b[:n]
//		}
//		start := time.Now()
//
//		// Check if the account has mappings and if so set the local readcache flag.
//		// We check here to make sure any changes such as config reload are reflected here.
//		if c.kind == CLIENT || c.kind == LEAF {
//			if acc.hasMappings() {
//				c.in.flags.set(hasMappings)
//			} else {
//				c.in.flags.clear(hasMappings)
//			}
//		}
//
//		// Clear inbound stats cache
//		c.in.msgs = 0
//		c.in.bytes = 0
//		c.in.subs = 0
//
//		// Main call into parser for inbound data. This will generate callouts
//		// to process messages, etc.
//		for i := 0; i < len(bufs); i++ {
//			if err := c.parse(bufs[i]); err != nil {
//				if err == ErrMinimumVersionRequired {
//					// Special case here, currently only for leaf node connections.
//					// When process the CONNECT protocol, if the minimum version
//					// required was not met, an error was printed and sent back to
//					// the remote, and connection was closed after a certain delay
//					// (to avoid "rapid" reconnection from the remote).
//					// We don't need to do any of the things below, simply return.
//					return
//				}
//				if dur := time.Since(start); dur >= readLoopReportThreshold {
//					c.Warnf("Readloop processing time: %v", dur)
//				}
//				// Need to call flushClients because some of the clients have been
//				// assigned messages and their "fsp" incremented, and need now to be
//				// decremented and their writeLoop signaled.
//				c.flushClients(0)
//				// handled inline
//				if err != ErrMaxPayload && err != ErrAuthentication {
//					c.Error(err)
//					c.closeConnection(ProtocolViolation)
//				}
//				return
//			}
//		}
//
//		// Updates stats for client and server that were collected
//		// from parsing through the buffer.
//		if c.in.msgs > 0 {
//			atomic.AddInt64(&c.inMsgs, int64(c.in.msgs))
//			atomic.AddInt64(&c.inBytes, int64(c.in.bytes))
//			atomic.AddInt64(&s.inMsgs, int64(c.in.msgs))
//			atomic.AddInt64(&s.inBytes, int64(c.in.bytes))
//		}
//
//		// Signal to writeLoop to flush to socket.
//		last := c.flushClients(0)
//
//		// Update activity, check read buffer size.
//		c.Mu.Lock()
//
//		// Activity based on interest changes or data/msgs.
//		if c.in.msgs > 0 || c.in.subs > 0 {
//			c.last = last
//		}
//
//		if n >= cap(b) {
//			c.in.srs = 0
//		} else if n < cap(b)/2 { // divide by 2 b/c we want less than what we would shrink to.
//			c.in.srs++
//		}
//
//		// Update read buffer size as/if needed.
//		if n >= cap(b) && cap(b) < maxBufSize {
//			// Grow
//			c.in.rsz = int32(cap(b) * 2)
//			b = make([]byte, c.in.rsz)
//		} else if n < cap(b) && cap(b) > minBufSize && c.in.srs > shortsToShrink {
//			// Shrink, for now don't accelerate, ping/pong will eventually sort it Out.
//			c.in.rsz = int32(cap(b) / 2)
//			b = make([]byte, c.in.rsz)
//		}
//		// re-snapshot the account since it can change during reload, etc.
//		acc = c.acc
//		// Refresh Nc because in some cases, we have upgraded c.Nc to TLS.
//		Nc = c.Nc
//		c.Mu.Unlock()
//
//		// Connection was closed
//		if Nc == nil {
//			return
//		}
//
//		if dur := time.Since(start); dur >= readLoopReportThreshold {
//			c.Warnf("Readloop processing time: %v", dur)
//		}
//
//		// We could have had a read error from above but still read some data.
//		// If so do the close here unconditionally.
//		if err != nil {
//			c.closeConnection(closedStateForErr(err))
//			return
//		}
//
//		if cpacc && (start.Sub(lpacc)) >= closedSubsCheckInterval {
//			c.pruneClosedSubFromPerAccountCache()
//			lpacc = time.Now()
//		}
//	}
//}
