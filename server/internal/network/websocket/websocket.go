// Copyright 2020 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package websocket

import (
	"compress/flate"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"io"
	"io/ioutil"
	mrand "math/rand"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
)

type opCode int

const (
	// cR_LF string
	cR_LF   = "\r\n"
	_EMPTY_ = ""

	// From https://tools.ietf.org/html/rfc6455#section-5.2
	textMessage   = opCode(1)
	binaryMessage = opCode(2)
	closeMessage  = opCode(8)
	pingMessage   = opCode(9)
	wsPongMessage = opCode(10)

	finalBit = 1 << 7
	rsv1Bit  = 1 << 6 // Used for compression, from https://tools.ietf.org/html/rfc7692#section-6
	rsv2Bit  = 1 << 5
	rsv3Bit  = 1 << 4

	maskBit = 1 << 7

	continuationFrame     = 0
	maxFrameHeaderSize    = 14 // Since LeafNode may need to behave as a client
	maxControlPayloadSize = 125
	frameSizeForBrowsers  = 4096 // From experiment, webrowsers behave better with limited frame size
	compressThreshold     = 64   // Don't compress for small buffer(s)
	closeSatusSize        = 2

	// From https://tools.ietf.org/html/rfc6455#section-11.7
	closeStatusNormalClosure      = 1000
	closeStatusGoingAway          = 1001
	closeStatusProtocolError      = 1002
	closeStatusUnsupportedData    = 1003
	closeStatusNoStatusReceived   = 1005
	closeStatusAbnormalClosure    = 1006
	closeStatusInvalidPayloadData = 1007
	closeStatusPolicyViolation    = 1008
	closeStatusMessageTooBig      = 1009
	closeStatusInternalSrvError   = 1011
	closeStatusTLSHandshake       = 1015

	firstFrame        = true
	contFrame         = false
	finalFrame        = true
	uncompressedFrame = false

	SchemePrefix    = "ws"
	SchemePrefixTLS = "wss"

	NoMaskingHeader       = "Nats-No-Masking"
	NoMaskingValue        = "true"
	XForwardedForHeader   = "X-Forwarded-For"
	NoMaskingFullResponse = NoMaskingHeader + ": " + NoMaskingValue + cR_LF
	pMCExtension          = "permessage-deflate" // per-message compression
	pMCSrvNoCtx           = "server_no_context_takeover"
	pMCCliNoCtx           = "client_no_context_takeover"
	pMCReqHeaderValue     = pMCExtension + "; " + pMCSrvNoCtx + "; " + pMCCliNoCtx
	PMCFullResponse       = "Sec-WebSocket-Extensions: " + pMCExtension + "; " + pMCSrvNoCtx + "; " +
		pMCCliNoCtx + cR_LF
	secProto = "Sec-Websocket-Protocol"

	//todo: try to separate them
	wsMQTTSecProtoVal = "mqtt"
	MQTTSecProto      = secProto + ": " + wsMQTTSecProtoVal + cR_LF
)

var decompressorPool sync.Pool
var compressLastBlock = []byte{0x00, 0x00, 0xff, 0xff, 0x01, 0x00, 0x00, 0xff, 0xff}

// From https://tools.ietf.org/html/rfc6455#section-1.3
var GUID = []byte("258EAFA5-E914-47DA-95CA-C5AB0DC85B11")

// Test can enable this so that Server does not support "no-masking" requests.
var TestRejectNoMasking = false

type Websocket struct {
	frames     net.Buffers
	Fs         int64
	CloseMsg   []byte
	Compress   bool
	CloseSent  bool
	Browser    bool
	Nocompfrag bool // No fragment for compressed frames
	Maskread   bool
	maskwrite  bool
	compressor *flate.Writer
	CookieJwt  string
	ClientIP   string
}

type ConnURLsMap interface {
	AddUrl(urlStr string) bool
	RemoveUrl(urlStr string) bool
	GetAsStringSlice() []string
}

type SrvWebsocket struct {
	mu             sync.RWMutex
	Server         *http.Server
	Listener       net.Listener
	listenerErr    error
	Tls            bool
	allowedOrigins map[string]*allowedOrigin // host will be the key
	sameOrigin     bool
	ConnectURLs    []string

	ConnectURLsMap ConnURLsMap
	AuthOverride   bool // indicate if there is auth override in websocket config
}

func (ws *SrvWebsocket) Serve() error {
	ws.mu.Lock()
	return ws.Server.Serve(ws.Listener)
}

type allowedOrigin struct {
	scheme string
	port   string
}

type readInfo struct {
	rem   int
	fs    bool
	ff    bool
	fc    bool
	mask  bool // Incoming leafnode connections may not have masking.
	mkpos byte
	mkey  [4]byte
	cbufs [][]byte
	coff  int
}

func (r *readInfo) init() {
	r.fs, r.ff = true, true
}

// Returns a slice containing `needed` bytes from the given buffer `buf`
// starting at position `pos`, and possibly read from the given reader `r`.
// When bytes are present in `buf`, the `pos` is incremented by the number
// of bytes found up to `needed` and the new position is returned. If not
// enough bytes are found, the bytes found in `buf` are copied to the returned
// slice and the remaning bytes are read from `r`.
func get(r io.Reader, buf []byte, pos, needed int) ([]byte, int, error) {
	avail := len(buf) - pos
	if avail >= needed {
		return buf[pos : pos+needed], pos + needed, nil
	}
	b := make([]byte, needed)
	start := copy(b, buf[pos:])
	for start != needed {
		n, err := r.Read(b[start:cap(b)])
		if err != nil {
			return nil, 0, err
		}
		start += n
	}
	return b, pos + avail, nil
}

func (r *readInfo) Read(dst []byte) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	if len(r.cbufs) == 0 {
		return 0, io.EOF
	}
	copied := 0
	rem := len(dst)
	for buf := r.cbufs[0]; buf != nil && rem > 0; {
		n := len(buf[r.coff:])
		if n > rem {
			n = rem
		}
		copy(dst[copied:], buf[r.coff:r.coff+n])
		copied += n
		rem -= n
		r.coff += n
		buf = r.nextCBuf()
	}
	return copied, nil
}

func (r *readInfo) nextCBuf() []byte {
	// We still have remaining data in the first buffer
	if r.coff != len(r.cbufs[0]) {
		return r.cbufs[0]
	}
	// We read the full first buffer. Reset offset.
	r.coff = 0
	// We were at the last buffer, so we are done.
	if len(r.cbufs) == 1 {
		r.cbufs = nil
		return nil
	}
	// Here we move to the next buffer.
	r.cbufs = r.cbufs[1:]
	return r.cbufs[0]
}

func (r *readInfo) ReadByte() (byte, error) {
	if len(r.cbufs) == 0 {
		return 0, io.EOF
	}
	b := r.cbufs[0][r.coff]
	r.coff++
	r.nextCBuf()
	return b, nil
}

func (r *readInfo) decompress() ([]byte, error) {
	r.coff = 0
	// As per https://tools.ietf.org/html/rfc7692#section-7.2.2
	// add 0x00, 0x00, 0xff, 0xff and then a final block so that flate reader
	// does not report unexpected EOF.
	r.cbufs = append(r.cbufs, compressLastBlock)
	// get a decompressor from the pool and bind it to this object (readInfo)
	// that provides Read() and ReadByte() APIs that will consume the compressed
	// buffers (r.cbufs).
	d, _ := decompressorPool.Get().(io.ReadCloser)
	if d == nil {
		d = flate.NewReader(r)
	} else {
		d.(flate.Resetter).Reset(r, nil)
	}
	// This will do the decompression.
	b, err := ioutil.ReadAll(d)
	decompressorPool.Put(d)
	// Now reset the compressed buffers list.
	r.cbufs = nil
	return b, err
}

// Unmask the given slice.
func (r *readInfo) unmask(buf []byte) {
	p := int(r.mkpos)
	if len(buf) < 16 {
		for i := 0; i < len(buf); i++ {
			buf[i] ^= r.mkey[p&3]
			p++
		}
		r.mkpos = byte(p & 3)
		return
	}
	var k [8]byte
	for i := 0; i < 8; i++ {
		k[i] = r.mkey[(p+i)&3]
	}
	km := binary.BigEndian.Uint64(k[:])
	n := (len(buf) / 8) * 8
	for i := 0; i < n; i += 8 {
		tmp := binary.BigEndian.Uint64(buf[i : i+8])
		tmp ^= km
		binary.BigEndian.PutUint64(buf[i:], tmp)
	}
	buf = buf[n:]
	for i := 0; i < len(buf); i++ {
		buf[i] ^= r.mkey[p&3]
		p++
	}
	r.mkpos = byte(p & 3)
}

// Returns true if the op code corresponds to a control frame.
func isControlFrame(frameType opCode) bool {
	return frameType >= closeMessage
}

// Create the frame header.
// Encodes the frame type and optional compression flag, and the size of the payload.
func createFrameHeader(useMasking, compressed bool, frameType opCode, l int) ([]byte, []byte) {
	fh := make([]byte, maxFrameHeaderSize)
	n, key := fillFrameHeader(fh, useMasking, firstFrame, finalFrame, compressed, frameType, l)
	return fh[:n], key
}

func fillFrameHeader(fh []byte, useMasking, first, final, compressed bool, frameType opCode, l int) (int, []byte) {
	var n int
	var b byte
	if first {
		b = byte(frameType)
	}
	if final {
		b |= finalBit
	}
	if compressed {
		b |= rsv1Bit
	}
	b1 := byte(0)
	if useMasking {
		b1 |= maskBit
	}
	switch {
	case l <= 125:
		n = 2
		fh[0] = b
		fh[1] = b1 | byte(l)
	case l < 65536:
		n = 4
		fh[0] = b
		fh[1] = b1 | 126
		binary.BigEndian.PutUint16(fh[2:], uint16(l))
	default:
		n = 10
		fh[0] = b
		fh[1] = b1 | 127
		binary.BigEndian.PutUint64(fh[2:], uint64(l))
	}
	var key []byte
	if useMasking {
		var keyBuf [4]byte
		if _, err := io.ReadFull(rand.Reader, keyBuf[:4]); err != nil {
			kv := mrand.Int31()
			binary.LittleEndian.PutUint32(keyBuf[:4], uint32(kv))
		}
		copy(fh[n:], keyBuf[:4])
		key = fh[n : n+4]
		n += 4
	}
	return n, key
}

// Mask the buffer with the given key
func maskBuf(key, buf []byte) {
	for i := 0; i < len(buf); i++ {
		buf[i] ^= key[i&3]
	}
}

// Mask the buffers, as if they were contiguous, with the given key
func maskBufs(key []byte, bufs [][]byte) {
	pos := 0
	for i := 0; i < len(bufs); i++ {
		buf := bufs[i]
		for j := 0; j < len(buf); j++ {
			buf[j] ^= key[pos&3]
			pos++
		}
	}
}

// Create a close message with the given `status` and `body`.
// If the `body` is more than the maximum allows control frame payload size,
// it is truncated and "..." is added at the end (as a hint that message
// is not complete).
func createCloseMessage(status int, body string) []byte {
	// Since a control message payload is limited in size, we
	// will limit the text and add trailing "..." if truncated.
	// The body of a Close Message must be preceded with 2 bytes,
	// so take that into account for limiting the body length.
	if len(body) > maxControlPayloadSize-2 {
		body = body[:maxControlPayloadSize-5]
		body += "..."
	}
	buf := make([]byte, 2+len(body))
	// We need to have a 2 byte unsigned int that represents the error status code
	// https://tools.ietf.org/html/rfc6455#section-5.5.1
	binary.BigEndian.PutUint16(buf[:2], uint16(status))
	copy(buf[2:], []byte(body))
	return buf
}

func PMCExtensionSupport(header http.Header, checkPMCOnly bool) (bool, bool) {
	for _, extensionList := range header["Sec-Websocket-Extensions"] {
		extensions := strings.Split(extensionList, ",")
		for _, extension := range extensions {
			extension = strings.Trim(extension, " \t")
			params := strings.Split(extension, ";")
			for i, p := range params {
				p = strings.Trim(p, " \t")
				if strings.EqualFold(p, pMCExtension) {
					if checkPMCOnly {
						return true, false
					}
					var snc bool
					var cnc bool
					for j := i + 1; j < len(params); j++ {
						p = params[j]
						p = strings.Trim(p, " \t")
						if strings.EqualFold(p, pMCSrvNoCtx) {
							snc = true
						} else if strings.EqualFold(p, pMCCliNoCtx) {
							cnc = true
						}
						if snc && cnc {
							return true, true
						}
					}
					return true, false
				}
			}
		}
	}
	return false, false
}

// If the Server is configured to accept any origin, then this function returns
// `nil` without checking if the Origin is present and valid. This is also
// the case if the request does not have the Origin header.
// Otherwise, this will check that the Origin matches the same origin or
// any origin in the allowed list.
func (ws *SrvWebsocket) CheckOrigin(r *http.Request) error {
	ws.mu.RLock()
	checkSame := ws.sameOrigin
	listEmpty := len(ws.allowedOrigins) == 0
	ws.mu.RUnlock()
	if !checkSame && listEmpty {
		return nil
	}
	origin := r.Header.Get("Origin")
	if origin == _EMPTY_ {
		origin = r.Header.Get("Sec-Websocket-Origin")
	}
	// If the header is not present, we will accept.
	// From https://datatracker.ietf.org/doc/html/rfc6455#section-1.6
	// "Naturally, when the WebSocket Protocol is used by a dedicated client
	// directly (i.e., not from a web page through a web browser), the origin
	// model is not useful, as the client can provide any arbitrary origin string."
	if origin == _EMPTY_ {
		return nil
	}
	u, err := url.ParseRequestURI(origin)
	if err != nil {
		return err
	}
	oh, op, err := getHostAndPort(u.Scheme == "https", u.Host)
	if err != nil {
		return err
	}
	// If checking same origin, compare with the http's request's Host.
	if checkSame {
		rh, rp, err := getHostAndPort(r.TLS != nil, r.Host)
		if err != nil {
			return err
		}
		if oh != rh || op != rp {
			return errors.New("not same origin")
		}
		// I guess it is possible to have cases where one wants to check
		// same origin, but also that the origin is in the allowed list.
		// So continue with the next check.
	}
	if !listEmpty {
		ws.mu.RLock()
		ao := ws.allowedOrigins[oh]
		ws.mu.RUnlock()
		if ao == nil || u.Scheme != ao.scheme || op != ao.port {
			return errors.New("not in the allowed list")
		}
	}
	return nil
}

func getHostAndPort(tls bool, hostport string) (string, string, error) {
	host, port, err := net.SplitHostPort(hostport)
	if err != nil {
		// If error is missing port, then use defaults based on the scheme
		if ae, ok := err.(*net.AddrError); ok && strings.Contains(ae.Err, "missing port") {
			err = nil
			host = hostport
			if tls {
				port = "443"
			} else {
				port = "80"
			}
		}
	}
	return strings.ToLower(host), port, err
}

func wsMakeChallengeKey() (string, error) {
	p := make([]byte, 16)
	if _, err := io.ReadFull(rand.Reader, p); err != nil {
		return _EMPTY_, err
	}
	return base64.StdEncoding.EncodeToString(p), nil
}

func IsWebsocketURL(u *url.URL) bool {
	return strings.HasPrefix(strings.ToLower(u.Scheme), SchemePrefix)
}

func IsWebsocketSecurityURL(u *url.URL) bool {
	return strings.HasPrefix(strings.ToLower(u.Scheme), SchemePrefixTLS)
}
