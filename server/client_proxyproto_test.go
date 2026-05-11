// Copyright 2025 The NATS Authors
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

package server

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"
)

// mockConn is a mock net.Conn for testing
type mockConn struct {
	net.Conn
	readBuf  *bytes.Buffer
	writeBuf *bytes.Buffer
	closed   bool
	deadline time.Time
}

func newMockConn(data []byte) *mockConn {
	return &mockConn{
		readBuf:  bytes.NewBuffer(data),
		writeBuf: &bytes.Buffer{},
	}
}

func (m *mockConn) Read(b []byte) (int, error) {
	if m.closed {
		return 0, fmt.Errorf("connection closed")
	}
	if !m.deadline.IsZero() && time.Now().After(m.deadline) {
		return 0, &net.OpError{Op: "read", Err: fmt.Errorf("timeout")}
	}
	return m.readBuf.Read(b)
}

func (m *mockConn) Write(b []byte) (int, error) {
	if m.closed {
		return 0, fmt.Errorf("connection closed")
	}
	return m.writeBuf.Write(b)
}

func (m *mockConn) Close() error {
	m.closed = true
	return nil
}

func (m *mockConn) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 4222}
}

func (m *mockConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("192.168.1.100"), Port: 54321}
}

func (m *mockConn) SetDeadline(t time.Time) error {
	m.deadline = t
	return nil
}

func (m *mockConn) SetReadDeadline(t time.Time) error {
	m.deadline = t
	return nil
}

func (m *mockConn) SetWriteDeadline(t time.Time) error {
	return nil
}

// buildProxyV2Header builds a valid PROXY protocol v2 header
func buildProxyV2Header(t *testing.T, srcIP, dstIP string, srcPort, dstPort uint16, family byte) []byte {
	t.Helper()

	buf := &bytes.Buffer{}
	buf.WriteString(proxyProtoV2Sig)                    // Write signature
	buf.WriteByte(proxyProtoV2Ver | proxyProtoCmdProxy) // Write version and command (version 2, PROXY command)
	buf.WriteByte(family | proxyProtoProtoStream)       // Write family and protocol

	src := net.ParseIP(srcIP)
	dst := net.ParseIP(dstIP)
	var addrData []byte
	switch family {
	case proxyProtoFamilyInet:
		// IPv4: 12 bytes
		addrData = make([]byte, proxyProtoAddrSizeIPv4)
		copy(addrData[0:4], src.To4())
		copy(addrData[4:8], dst.To4())
		binary.BigEndian.PutUint16(addrData[8:10], srcPort)
		binary.BigEndian.PutUint16(addrData[10:12], dstPort)
	case proxyProtoFamilyInet6:
		// IPv6: 36 bytes
		addrData = make([]byte, proxyProtoAddrSizeIPv6)
		copy(addrData[0:16], src.To16())
		copy(addrData[16:32], dst.To16())
		binary.BigEndian.PutUint16(addrData[32:34], srcPort)
		binary.BigEndian.PutUint16(addrData[34:36], dstPort)
	default:
		t.Fatalf("unsupported address family: %d", family)
	}

	addrLen := make([]byte, 2)
	binary.BigEndian.PutUint16(addrLen, uint16(len(addrData)))
	buf.Write(addrLen)
	buf.Write(addrData)

	return buf.Bytes()
}

// buildProxyV2LocalHeader builds a PROXY protocol v2 LOCAL command header
func buildProxyV2LocalHeader() []byte {
	buf := &bytes.Buffer{}
	buf.WriteString(proxyProtoV2Sig)                              // // Write signature
	buf.WriteByte(proxyProtoV2Ver | proxyProtoCmdLocal)           // // Write version and command (version 2, LOCAL command)
	buf.WriteByte(proxyProtoFamilyUnspec | proxyProtoProtoUnspec) // // Write family and protocol (UNSPEC)
	buf.WriteByte(0)
	buf.WriteByte(0)
	return buf.Bytes()
}

// buildProxyV1Header builds a valid PROXY protocol v1 header (text format)
func buildProxyV1Header(t *testing.T, protocol, srcIP, dstIP string, srcPort, dstPort uint16) []byte {
	t.Helper()

	if protocol != "TCP4" && protocol != "TCP6" && protocol != "UNKNOWN" {
		t.Fatalf("invalid protocol: %s", protocol)
	}

	var line string
	if protocol == "UNKNOWN" {
		line = "PROXY UNKNOWN\r\n"
	} else {
		line = fmt.Sprintf("PROXY %s %s %s %d %d\r\n", protocol, srcIP, dstIP, srcPort, dstPort)
	}

	return []byte(line)
}

func TestClientProxyProtoV2ParseIPv4(t *testing.T) {
	header := buildProxyV2Header(t, "192.168.1.50", "10.0.0.1", 12345, 4222, proxyProtoFamilyInet)
	conn := newMockConn(header)

	addr, err := readProxyProtoV2Header(conn)
	require_NoError(t, err)
	require_NotNil(t, addr)

	require_Equal(t, addr.srcIP.String(), "192.168.1.50")
	require_Equal(t, addr.srcPort, 12345)

	require_Equal(t, addr.dstIP.String(), "10.0.0.1")
	require_Equal(t, addr.dstPort, 4222)

	// Test String() and Network() methods
	require_Equal(t, addr.String(), "192.168.1.50:12345")
	require_Equal(t, addr.Network(), "tcp4")
}

func TestClientProxyProtoV2ParseIPv6(t *testing.T) {
	header := buildProxyV2Header(t, "2001:db8::1", "2001:db8::2", 54321, 4222, proxyProtoFamilyInet6)
	conn := newMockConn(header)

	addr, err := readProxyProtoV2Header(conn)
	require_NoError(t, err)
	require_NotNil(t, addr)

	require_Equal(t, addr.srcIP.String(), "2001:db8::1")
	require_Equal(t, addr.srcPort, 54321)

	require_Equal(t, addr.dstIP.String(), "2001:db8::2")
	require_Equal(t, addr.dstPort, 4222)

	// Test Network() method for IPv6
	require_Equal(t, addr.String(), "[2001:db8::1]:54321")
	require_Equal(t, addr.Network(), "tcp6")
}

func TestClientProxyProtoV2ParseLocalCommand(t *testing.T) {
	header := buildProxyV2LocalHeader()
	conn := newMockConn(header)

	addr, err := readProxyProtoV2Header(conn)
	require_NoError(t, err)
	require_True(t, addr == nil)
}

func TestClientProxyProtoV2InvalidSignature(t *testing.T) {
	// Create invalid signature
	header := []byte("INVALID_SIG_")
	header = append(header, []byte{0x20, 0x11, 0x00, 0x0C}...)
	conn := newMockConn(header)

	_, err := readProxyProtoV2Header(conn)
	require_Error(t, err, errProxyProtoInvalid)
}

func TestClientProxyProtoV2InvalidVersion(t *testing.T) {
	buf := &bytes.Buffer{}
	buf.WriteString(proxyProtoV2Sig)
	buf.WriteByte(0x10 | proxyProtoCmdProxy) // Version 1 instead of 2
	buf.WriteByte(proxyProtoFamilyInet | proxyProtoProtoStream)
	buf.WriteByte(0)
	buf.WriteByte(0)

	conn := newMockConn(buf.Bytes())

	_, err := readProxyProtoV2Header(conn)
	require_Error(t, err, errProxyProtoInvalid)
}

func TestClientProxyProtoV2UnsupportedFamily(t *testing.T) {
	buf := &bytes.Buffer{}
	buf.WriteString(proxyProtoV2Sig)
	buf.WriteByte(proxyProtoV2Ver | proxyProtoCmdProxy)
	buf.WriteByte(proxyProtoFamilyUnix | proxyProtoProtoStream) // Unix socket family
	buf.WriteByte(0)
	buf.WriteByte(0)

	conn := newMockConn(buf.Bytes())

	_, err := readProxyProtoV2Header(conn)
	require_Error(t, err, errProxyProtoUnsupported)
}

func TestClientProxyProtoV2UnsupportedProtocol(t *testing.T) {
	buf := &bytes.Buffer{}
	buf.WriteString(proxyProtoV2Sig)
	buf.WriteByte(proxyProtoV2Ver | proxyProtoCmdProxy)
	buf.WriteByte(proxyProtoFamilyInet | proxyProtoProtoDatagram) // UDP instead of TCP
	buf.WriteByte(0)
	buf.WriteByte(12)

	conn := newMockConn(buf.Bytes())

	_, err := readProxyProtoV2Header(conn)
	require_Error(t, err, errProxyProtoUnsupported)
}

func TestClientProxyProtoV2TruncatedHeader(t *testing.T) {
	header := buildProxyV2Header(t, "192.168.1.50", "10.0.0.1", 12345, 4222, proxyProtoFamilyInet)
	// Only send first 10 bytes (incomplete header)
	conn := newMockConn(header[:10])

	_, err := readProxyProtoV2Header(conn)
	require_Error(t, err, io.ErrUnexpectedEOF)
}

func TestClientProxyProtoV2ShortAddressData(t *testing.T) {
	buf := &bytes.Buffer{}
	buf.WriteString(proxyProtoV2Sig)
	buf.WriteByte(proxyProtoV2Ver | proxyProtoCmdProxy)
	buf.WriteByte(proxyProtoFamilyInet | proxyProtoProtoStream)
	// Set address length to 12 but don't provide data
	buf.WriteByte(0)
	buf.WriteByte(12)
	// Only provide 5 bytes instead of 12
	buf.Write([]byte{1, 2, 3, 4, 5})

	conn := newMockConn(buf.Bytes())

	_, err := readProxyProtoV2Header(conn)
	require_Error(t, err, io.ErrUnexpectedEOF)
}

func TestProxyConnRemoteAddr(t *testing.T) {
	// Create a real TCP connection for testing
	originalAddr := &net.TCPAddr{IP: net.ParseIP("192.168.1.100"), Port: 54321}

	// Create proxy address
	proxyAddr := &proxyProtoAddr{
		srcIP:   net.ParseIP("10.0.0.50"),
		srcPort: 12345,
		dstIP:   net.ParseIP("10.0.0.1"),
		dstPort: 4222,
	}

	mockConn := newMockConn(nil)
	wrapped := &proxyConn{
		Conn:       mockConn,
		remoteAddr: proxyAddr,
	}

	// Verify RemoteAddr returns the proxied address
	addr := wrapped.RemoteAddr()
	require_Equal(t, addr.String(), "10.0.0.50:12345")
	require_Equal(t, mockConn.RemoteAddr().String(), originalAddr.String())
}

func TestClientProxyProtoV2EndToEnd(t *testing.T) {
	// Start a test server with PROXY protocol enabled
	opts := DefaultOptions()
	opts.Port = -1 // Random port
	opts.ProxyProtocol = true

	s := RunServer(opts)
	defer s.Shutdown()

	// Get the server's listening port
	addr := s.Addr().String()

	// Connect to the server
	conn, err := net.Dial("tcp", addr)
	require_NoError(t, err)
	defer conn.Close()

	// Send PROXY protocol header
	clientIP := "203.0.113.50"
	clientPort := uint16(54321)
	header := buildProxyV2Header(t, clientIP, "127.0.0.1", clientPort, 4222, proxyProtoFamilyInet)

	_, err = conn.Write(header)
	require_NoError(t, err)

	// Send CONNECT message
	connectMsg := "CONNECT {\"verbose\":false,\"pedantic\":false,\"protocol\":1}\r\n"
	_, err = conn.Write([]byte(connectMsg))
	require_NoError(t, err)

	// Read INFO and +OK
	buf := make([]byte, 4096)
	n, err := conn.Read(buf)
	require_NoError(t, err)

	response := string(buf[:n])
	require_True(t, strings.Contains(response, "INFO"))

	// Give server time to process
	time.Sleep(100 * time.Millisecond)

	// Check server's client list to verify the IP was extracted correctly
	s.mu.Lock()
	clients := s.clients
	s.mu.Unlock()
	require_True(t, len(clients) != 0)

	// Find our client
	var foundClient *client
	for _, c := range clients {
		c.mu.Lock()
		if c.host == clientIP && c.port == clientPort {
			foundClient = c
		}
		c.mu.Unlock()
		if foundClient != nil {
			break
		}
	}
	require_NotNil(t, foundClient)
}

func TestClientProxyProtoV2LocalCommandEndToEnd(t *testing.T) {
	// Start a test server with PROXY protocol enabled
	opts := DefaultOptions()
	opts.Port = -1 // Random port
	opts.ProxyProtocol = true

	s := RunServer(opts)
	defer s.Shutdown()

	// Get the server's listening port
	addr := s.Addr().String()

	// Connect to the server
	conn, err := net.Dial("tcp", addr)
	require_NoError(t, err)
	defer conn.Close()

	// Send PROXY protocol LOCAL header (health check)
	header := buildProxyV2LocalHeader()

	_, err = conn.Write(header)
	require_NoError(t, err)

	// Send CONNECT message
	connectMsg := "CONNECT {\"verbose\":false,\"pedantic\":false,\"protocol\":1}\r\n"
	_, err = conn.Write([]byte(connectMsg))
	require_NoError(t, err)

	// Read INFO and +OK
	buf := make([]byte, 4096)
	n, err := conn.Read(buf)
	require_NoError(t, err)

	response := string(buf[:n])
	require_True(t, strings.Contains(response, "INFO"))

	// Connection should work normally with LOCAL command
	time.Sleep(100 * time.Millisecond)

	// Verify at least one client is connected
	s.mu.Lock()
	numClients := len(s.clients)
	s.mu.Unlock()
	require_NotEqual(t, numClients, 0)
}

// ============================================================================
// PROXY Protocol v1 Tests
// ============================================================================

func TestClientProxyProtoV1ParseTCP4(t *testing.T) {
	header := buildProxyV1Header(t, "TCP4", "192.168.1.50", "10.0.0.1", 12345, 4222)
	conn := newMockConn(header)

	addr, _, err := readProxyProtoHeader(conn)
	require_NoError(t, err)
	require_NotNil(t, addr)

	require_Equal(t, addr.srcIP.String(), "192.168.1.50")
	require_Equal(t, addr.srcPort, 12345)

	require_Equal(t, addr.dstIP.String(), "10.0.0.1")
	require_Equal(t, addr.dstPort, 4222)
}

func TestClientProxyProtoV1ParseTCP6(t *testing.T) {
	header := buildProxyV1Header(t, "TCP6", "2001:db8::1", "2001:db8::2", 54321, 4222)
	conn := newMockConn(header)

	addr, _, err := readProxyProtoHeader(conn)
	require_NoError(t, err)
	require_NotNil(t, addr)

	require_Equal(t, addr.srcIP.String(), "2001:db8::1")
	require_Equal(t, addr.srcPort, 54321)

	require_Equal(t, addr.dstIP.String(), "2001:db8::2")
	require_Equal(t, addr.dstPort, 4222)
}

func TestClientProxyProtoV1ParseUnknown(t *testing.T) {
	header := buildProxyV1Header(t, "UNKNOWN", "", "", 0, 0)
	conn := newMockConn(header)

	addr, _, err := readProxyProtoHeader(conn)
	require_NoError(t, err)
	require_True(t, addr == nil)
}

func TestClientProxyProtoV1PreservesCoalescedClientBytes(t *testing.T) {
	header := buildProxyV1Header(t, "TCP4", "192.168.1.50", "10.0.0.1", 12345, 4222)
	connect := []byte("CONNECT {\"verbose\":false,\"pedantic\":false,\"protocol\":1}\r\n")
	conn := newMockConn(append(header, connect...))

	addr, remaining, err := readProxyProtoHeader(conn)
	require_NoError(t, err)
	require_NotNil(t, addr)
	require_Equal(t, string(remaining), string(connect))

	rest, err := io.ReadAll(conn)
	require_NoError(t, err)
	require_Equal(t, len(rest), 0)
}

func TestClientProxyProtoV1InvalidFormat(t *testing.T) {
	// Missing fields
	header := []byte("PROXY TCP4 192.168.1.1\r\n")
	conn := newMockConn(header)

	_, _, err := readProxyProtoHeader(conn)
	require_Error(t, err, errProxyProtoInvalid)
}

func TestClientProxyProtoV1LineTooLong(t *testing.T) {
	// Create a line longer than 107 bytes
	longIP := strings.Repeat("1234567890", 12) // 120 chars
	header := fmt.Appendf(nil, "PROXY TCP4 %s 10.0.0.1 12345 443\r\n", longIP)
	conn := newMockConn(header)

	_, _, err := readProxyProtoHeader(conn)
	require_Error(t, err, errProxyProtoInvalid)
}

func TestClientProxyProtoV1InvalidIP(t *testing.T) {
	header := []byte("PROXY TCP4 not.an.ip.addr 10.0.0.1 12345 443\r\n")
	conn := newMockConn(header)

	_, _, err := readProxyProtoHeader(conn)
	require_Error(t, err, errProxyProtoInvalid)
}

func TestClientProxyProtoV1MismatchedProtocol(t *testing.T) {
	// TCP4 with IPv6 address
	header := buildProxyV1Header(t, "TCP4", "2001:db8::1", "2001:db8::2", 12345, 443)
	conn := newMockConn(header)

	_, _, err := readProxyProtoHeader(conn)
	require_Error(t, err, errProxyProtoInvalid)

	// TCP6 with IPv4 address
	header2 := buildProxyV1Header(t, "TCP6", "192.168.1.1", "10.0.0.1", 12345, 443)
	conn2 := newMockConn(header2)

	_, _, err = readProxyProtoHeader(conn2)
	require_Error(t, err, errProxyProtoInvalid)
}

func TestClientProxyProtoV1InvalidPort(t *testing.T) {
	header := []byte("PROXY TCP4 192.168.1.1 10.0.0.1 99999 443\r\n")
	conn := newMockConn(header)

	_, _, err := readProxyProtoHeader(conn)
	require_True(t, err != nil)
}

func TestClientProxyProtoV1EndToEnd(t *testing.T) {
	// Start a test server with PROXY protocol enabled
	opts := DefaultOptions()
	opts.Port = -1 // Random port
	opts.ProxyProtocol = true

	s := RunServer(opts)
	defer s.Shutdown()

	// Get the server's listening port
	addr := s.Addr().String()

	// Connect to the server
	conn, err := net.Dial("tcp", addr)
	require_NoError(t, err)
	defer conn.Close()

	// Send the PROXY protocol header and first client commands in one write so
	// the parser must preserve bytes read past the end of the v1 header.
	clientIP, clientPort := "203.0.113.50", uint16(54321)
	header := buildProxyV1Header(t, "TCP4", clientIP, "127.0.0.1", clientPort, 4222)
	payload := append(header, []byte("CONNECT {\"verbose\":true,\"pedantic\":false,\"protocol\":1}\r\nPING\r\n")...)

	_, err = conn.Write(payload)
	require_NoError(t, err)

	require_NoError(t, conn.SetReadDeadline(time.Now().Add(2*time.Second)))
	cr := bufio.NewReader(conn)

	line, err := cr.ReadString('\n')
	require_NoError(t, err)
	require_True(t, strings.HasPrefix(line, "INFO "))

	line, err = cr.ReadString('\n')
	require_NoError(t, err)
	require_True(t, strings.HasPrefix(line, "+OK"))

	expectPong(t, cr)

	// Give server time to process
	time.Sleep(100 * time.Millisecond)

	// Check server's client list to verify the IP was extracted correctly
	s.mu.Lock()
	clients := s.clients
	var foundClient *client
	for _, c := range clients {
		c.mu.Lock()
		if c.host == clientIP && c.port == clientPort {
			foundClient = c
		}
		c.mu.Unlock()
		if foundClient != nil {
			break
		}
	}
	s.mu.Unlock()
	require_NotNil(t, foundClient)
}

// ============================================================================
// Mixed Protocol Version Tests
// ============================================================================

func TestClientProxyProtoVersionDetection(t *testing.T) {
	// Test v1 detection
	v1Header := buildProxyV1Header(t, "TCP4", "192.168.1.1", "10.0.0.1", 12345, 443)
	conn1 := newMockConn(v1Header)

	addr1, _, err := readProxyProtoHeader(conn1)
	require_NoError(t, err)
	require_NotNil(t, addr1)
	require_Equal(t, addr1.srcIP.String(), "192.168.1.1")

	// Test v2 detection
	v2Header := buildProxyV2Header(t, "192.168.1.2", "10.0.0.1", 54321, 443, proxyProtoFamilyInet)
	conn2 := newMockConn(v2Header)

	addr2, _, err := readProxyProtoHeader(conn2)
	require_NoError(t, err)
	require_NotNil(t, addr2)
	require_Equal(t, addr2.srcIP.String(), "192.168.1.2")
}

func TestClientProxyProtoUnrecognizedVersion(t *testing.T) {
	// Invalid header that doesn't match v1 or v2
	header := []byte("HELLO WORLD\r\n")
	conn := newMockConn(header)

	_, _, err := readProxyProtoHeader(conn)
	require_Error(t, err, errProxyProtoUnrecognized)
}

// ============================================================================
// PROXY Protocol + TLS Tests (regression for issue #7571)
// ============================================================================

// runProxyProtoTLSServer starts a server with proxy_protocol and a required
// TLS listener (no allow_non_tls, no tls_first).
func runProxyProtoTLSServer(t *testing.T) *Server {
	t.Helper()
	tc := &TLSConfigOpts{
		CertFile: "../test/configs/certs/server-cert.pem",
		KeyFile:  "../test/configs/certs/server-key.pem",
		CaFile:   "../test/configs/certs/ca.pem",
	}
	tlsConfig, err := GenTLSConfig(tc)
	require_NoError(t, err)

	opts := DefaultOptions()
	opts.Port = -1
	opts.ProxyProtocol = true
	opts.TLSConfig = tlsConfig
	opts.TLSTimeout = 2
	return RunServer(opts)
}

// proxyProtoTLSClientConfig builds a tls.Config trusting the test CA. The
// server cert is issued for "localhost" / 127.0.0.1, so ServerName must match.
func proxyProtoTLSClientConfig(t *testing.T) *tls.Config {
	t.Helper()
	caPEM, err := os.ReadFile("../test/configs/certs/ca.pem")
	require_NoError(t, err)
	pool := x509.NewCertPool()
	require_True(t, pool.AppendCertsFromPEM(caPEM))
	return &tls.Config{
		RootCAs:    pool,
		ServerName: "localhost",
	}
}

// findProxyProtoClient finds a connected client whose host/port matches what
// was advertised in the PROXY header.
func findProxyProtoClient(t *testing.T, s *Server, clientIP string, clientPort uint16) *client {
	t.Helper()
	s.mu.Lock()
	clients := make([]*client, 0, len(s.clients))
	for _, c := range s.clients {
		clients = append(clients, c)
	}
	s.mu.Unlock()

	for _, c := range clients {
		c.mu.Lock()
		host, port := c.host, c.port
		c.mu.Unlock()
		if host == clientIP && port == clientPort {
			return c
		}
	}
	return nil
}

// connectProxyProtoTLS mirrors what an L4 proxy and a TLS-enabled NATS client
// do over a single TCP connection: prepend the PROXY header, read the initial
// plaintext INFO, then upgrade to TLS. Returns the buffered reader and the
// TLS-wrapped connection for subsequent protocol writes.
func connectProxyProtoTLS(t *testing.T, addr string, header []byte) (*bufio.Reader, *tls.Conn) {
	t.Helper()
	rawConn, err := net.Dial("tcp", addr)
	require_NoError(t, err)
	require_NoError(t, rawConn.SetDeadline(time.Now().Add(5*time.Second)))

	_, err = rawConn.Write(header)
	require_NoError(t, err)

	// Server emits INFO over plaintext before the TLS upgrade. Read it now
	// so the bytes don't end up as input to the TLS handshake.
	plainReader := bufio.NewReader(rawConn)
	infoLine, err := plainReader.ReadString('\n')
	require_NoError(t, err)
	require_True(t, strings.HasPrefix(infoLine, "INFO "))
	require_Equal(t, plainReader.Buffered(), 0)

	tlsConn := tls.Client(rawConn, proxyProtoTLSClientConfig(t))
	require_NoError(t, tlsConn.Handshake())
	return bufio.NewReader(tlsConn), tlsConn
}

// TestClientProxyProtoV1WithRequiredTLS verifies that with proxy_protocol and
// required TLS, a client may send the PROXY v1 header followed by the TLS
// upgrade on a single TCP connection. Regression for issue #7571.
func TestClientProxyProtoV1WithRequiredTLS(t *testing.T) {
	s := runProxyProtoTLSServer(t)
	defer s.Shutdown()

	clientIP, clientPort := "203.0.113.50", uint16(54321)
	header := buildProxyV1Header(t, "TCP4", clientIP, "127.0.0.1", clientPort, 4222)
	cr, tlsConn := connectProxyProtoTLS(t, s.Addr().String(), header)
	defer tlsConn.Close()

	_, err := tlsConn.Write([]byte("CONNECT {\"verbose\":true,\"pedantic\":false,\"protocol\":1}\r\nPING\r\n"))
	require_NoError(t, err)

	line, err := cr.ReadString('\n')
	require_NoError(t, err)
	require_True(t, strings.HasPrefix(line, "+OK"))
	expectPong(t, cr)

	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		if findProxyProtoClient(t, s, clientIP, clientPort) == nil {
			return fmt.Errorf("no client with PROXY-supplied host/port yet")
		}
		return nil
	})
}

// TestClientProxyProtoV2WithRequiredTLS is the v2 counterpart to
// TestClientProxyProtoV1WithRequiredTLS.
func TestClientProxyProtoV2WithRequiredTLS(t *testing.T) {
	s := runProxyProtoTLSServer(t)
	defer s.Shutdown()

	clientIP, clientPort := "203.0.113.51", uint16(54322)
	header := buildProxyV2Header(t, clientIP, "127.0.0.1", clientPort, 4222, proxyProtoFamilyInet)
	cr, tlsConn := connectProxyProtoTLS(t, s.Addr().String(), header)
	defer tlsConn.Close()

	_, err := tlsConn.Write([]byte("CONNECT {\"verbose\":true,\"pedantic\":false,\"protocol\":1}\r\nPING\r\n"))
	require_NoError(t, err)

	line, err := cr.ReadString('\n')
	require_NoError(t, err)
	require_True(t, strings.HasPrefix(line, "+OK"))
	expectPong(t, cr)

	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		if findProxyProtoClient(t, s, clientIP, clientPort) == nil {
			return fmt.Errorf("no client with PROXY-supplied host/port yet")
		}
		return nil
	})
}
