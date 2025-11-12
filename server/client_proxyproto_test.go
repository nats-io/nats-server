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
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
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

	addr, err := readProxyProtoHeader(conn)
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

	addr, err := readProxyProtoHeader(conn)
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

	addr, err := readProxyProtoHeader(conn)
	require_NoError(t, err)
	require_True(t, addr == nil)
}

func TestClientProxyProtoV1InvalidFormat(t *testing.T) {
	// Missing fields
	header := []byte("PROXY TCP4 192.168.1.1\r\n")
	conn := newMockConn(header)

	_, err := readProxyProtoHeader(conn)
	require_Error(t, err, errProxyProtoInvalid)
}

func TestClientProxyProtoV1LineTooLong(t *testing.T) {
	// Create a line longer than 107 bytes
	longIP := strings.Repeat("1234567890", 12) // 120 chars
	header := fmt.Appendf(nil, "PROXY TCP4 %s 10.0.0.1 12345 443\r\n", longIP)
	conn := newMockConn(header)

	_, err := readProxyProtoHeader(conn)
	require_Error(t, err, errProxyProtoInvalid)
}

func TestClientProxyProtoV1InvalidIP(t *testing.T) {
	header := []byte("PROXY TCP4 not.an.ip.addr 10.0.0.1 12345 443\r\n")
	conn := newMockConn(header)

	_, err := readProxyProtoHeader(conn)
	require_Error(t, err, errProxyProtoInvalid)
}

func TestClientProxyProtoV1MismatchedProtocol(t *testing.T) {
	// TCP4 with IPv6 address
	header := buildProxyV1Header(t, "TCP4", "2001:db8::1", "2001:db8::2", 12345, 443)
	conn := newMockConn(header)

	_, err := readProxyProtoHeader(conn)
	require_Error(t, err, errProxyProtoInvalid)

	// TCP6 with IPv4 address
	header2 := buildProxyV1Header(t, "TCP6", "192.168.1.1", "10.0.0.1", 12345, 443)
	conn2 := newMockConn(header2)

	_, err = readProxyProtoHeader(conn2)
	require_Error(t, err, errProxyProtoInvalid)
}

func TestClientProxyProtoV1InvalidPort(t *testing.T) {
	header := []byte("PROXY TCP4 192.168.1.1 10.0.0.1 99999 443\r\n")
	conn := newMockConn(header)

	_, err := readProxyProtoHeader(conn)
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

	// Send PROXY protocol v1 header
	clientIP, clientPort := "203.0.113.50", uint16(54321)
	header := buildProxyV1Header(t, "TCP4", clientIP, "127.0.0.1", clientPort, 4222)

	_, err = conn.Write(header)
	require_NoError(t, err)

	// Send CONNECT message
	_, err = conn.Write([]byte("CONNECT {\"verbose\":false,\"pedantic\":false,\"protocol\":1}\r\n"))
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

	addr1, err := readProxyProtoHeader(conn1)
	require_NoError(t, err)
	require_NotNil(t, addr1)
	require_Equal(t, addr1.srcIP.String(), "192.168.1.1")

	// Test v2 detection
	v2Header := buildProxyV2Header(t, "192.168.1.2", "10.0.0.1", 54321, 443, proxyProtoFamilyInet)
	conn2 := newMockConn(v2Header)

	addr2, err := readProxyProtoHeader(conn2)
	require_NoError(t, err)
	require_NotNil(t, addr2)
	require_Equal(t, addr2.srcIP.String(), "192.168.1.2")
}

func TestClientProxyProtoUnrecognizedVersion(t *testing.T) {
	// Invalid header that doesn't match v1 or v2
	header := []byte("HELLO WORLD\r\n")
	conn := newMockConn(header)

	_, err := readProxyProtoHeader(conn)
	require_Error(t, err, errProxyProtoUnrecognized)
}
