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
	"errors"
	"fmt"
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

	// Write signature
	buf.WriteString(proxyProtoV2Sig)

	// Write version and command (version 2, PROXY command)
	buf.WriteByte(proxyProtoV2Ver | proxyProtoCmdProxy)

	// Write family and protocol
	buf.WriteByte(family | proxyProtoProtoStream)

	// Parse IPs
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

	// Write address length
	addrLen := make([]byte, 2)
	binary.BigEndian.PutUint16(addrLen, uint16(len(addrData)))
	buf.Write(addrLen)

	// Write address data
	buf.Write(addrData)

	return buf.Bytes()
}

// buildProxyV2LocalHeader builds a PROXY protocol v2 LOCAL command header
func buildProxyV2LocalHeader() []byte {
	buf := &bytes.Buffer{}

	// Write signature
	buf.WriteString(proxyProtoV2Sig)

	// Write version and command (version 2, LOCAL command)
	buf.WriteByte(proxyProtoV2Ver | proxyProtoCmdLocal)

	// Write family and protocol (UNSPEC)
	buf.WriteByte(proxyProtoFamilyUnspec | proxyProtoProtoUnspec)

	// Write address length (0 for LOCAL)
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

func TestProxyProtoV2ParseIPv4(t *testing.T) {
	header := buildProxyV2Header(t, "192.168.1.50", "10.0.0.1", 12345, 4222, proxyProtoFamilyInet)
	conn := newMockConn(header)

	addr, err := readProxyProtoV2Header(conn)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if addr == nil {
		t.Fatal("Expected address, got nil")
	}

	expectedSrcIP := "192.168.1.50"
	if addr.srcIP.String() != expectedSrcIP {
		t.Errorf("Expected src IP %s, got %s", expectedSrcIP, addr.srcIP.String())
	}

	expectedDstIP := "10.0.0.1"
	if addr.dstIP.String() != expectedDstIP {
		t.Errorf("Expected dst IP %s, got %s", expectedDstIP, addr.dstIP.String())
	}

	if addr.srcPort != 12345 {
		t.Errorf("Expected src port 12345, got %d", addr.srcPort)
	}

	if addr.dstPort != 4222 {
		t.Errorf("Expected dst port 4222, got %d", addr.dstPort)
	}

	// Test String() and Network() methods
	addrStr := addr.String()
	if addrStr != "192.168.1.50:12345" {
		t.Errorf("Expected address string '192.168.1.50:12345', got '%s'", addrStr)
	}

	network := addr.Network()
	if network != "tcp4" {
		t.Errorf("Expected network 'tcp4', got '%s'", network)
	}
}

func TestProxyProtoV2ParseIPv6(t *testing.T) {
	header := buildProxyV2Header(t, "2001:db8::1", "2001:db8::2", 54321, 4222, proxyProtoFamilyInet6)
	conn := newMockConn(header)

	addr, err := readProxyProtoV2Header(conn)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if addr == nil {
		t.Fatal("Expected address, got nil")
	}

	expectedSrcIP := "2001:db8::1"
	if addr.srcIP.String() != expectedSrcIP {
		t.Errorf("Expected src IP %s, got %s", expectedSrcIP, addr.srcIP.String())
	}

	expectedDstIP := "2001:db8::2"
	if addr.dstIP.String() != expectedDstIP {
		t.Errorf("Expected dst IP %s, got %s", expectedDstIP, addr.dstIP.String())
	}

	if addr.srcPort != 54321 {
		t.Errorf("Expected src port 54321, got %d", addr.srcPort)
	}

	if addr.dstPort != 4222 {
		t.Errorf("Expected dst port 4222, got %d", addr.dstPort)
	}

	// Test Network() method for IPv6
	network := addr.Network()
	if network != "tcp6" {
		t.Errorf("Expected network 'tcp6', got '%s'", network)
	}
}

func TestProxyProtoV2ParseLocalCommand(t *testing.T) {
	header := buildProxyV2LocalHeader()
	conn := newMockConn(header)

	addr, err := readProxyProtoV2Header(conn)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// LOCAL command should return nil address
	if addr != nil {
		t.Errorf("Expected nil address for LOCAL command, got: %v", addr)
	}
}

func TestProxyProtoV2InvalidSignature(t *testing.T) {
	// Create invalid signature
	header := []byte("INVALID_SIG_")
	header = append(header, []byte{0x20, 0x11, 0x00, 0x0C}...)
	conn := newMockConn(header)

	_, err := readProxyProtoV2Header(conn)
	if !errors.Is(err, errProxyProtoInvalid) {
		t.Errorf("Expected errProxyProtoInvalid, got: %v", err)
	}
}

func TestProxyProtoV2InvalidVersion(t *testing.T) {
	buf := &bytes.Buffer{}
	buf.WriteString(proxyProtoV2Sig)
	buf.WriteByte(0x10 | proxyProtoCmdProxy) // Version 1 instead of 2
	buf.WriteByte(proxyProtoFamilyInet | proxyProtoProtoStream)
	buf.WriteByte(0)
	buf.WriteByte(0)

	conn := newMockConn(buf.Bytes())

	_, err := readProxyProtoV2Header(conn)
	if !errors.Is(err, errProxyProtoInvalid) {
		t.Errorf("Expected errProxyProtoInvalid, got: %v", err)
	}
}

func TestProxyProtoV2UnsupportedFamily(t *testing.T) {
	buf := &bytes.Buffer{}
	buf.WriteString(proxyProtoV2Sig)
	buf.WriteByte(proxyProtoV2Ver | proxyProtoCmdProxy)
	buf.WriteByte(proxyProtoFamilyUnix | proxyProtoProtoStream) // Unix socket family
	buf.WriteByte(0)
	buf.WriteByte(0)

	conn := newMockConn(buf.Bytes())

	_, err := readProxyProtoV2Header(conn)
	if !errors.Is(err, errProxyProtoUnsupported) {
		t.Errorf("Expected errProxyProtoUnsupported, got: %v", err)
	}
}

func TestProxyProtoV2UnsupportedProtocol(t *testing.T) {
	buf := &bytes.Buffer{}
	buf.WriteString(proxyProtoV2Sig)
	buf.WriteByte(proxyProtoV2Ver | proxyProtoCmdProxy)
	buf.WriteByte(proxyProtoFamilyInet | proxyProtoProtoDatagram) // UDP instead of TCP
	buf.WriteByte(0)
	buf.WriteByte(12)

	conn := newMockConn(buf.Bytes())

	_, err := readProxyProtoV2Header(conn)
	if !errors.Is(err, errProxyProtoUnsupported) {
		t.Errorf("Expected errProxyProtoUnsupported, got: %v", err)
	}
}

func TestProxyProtoV2TruncatedHeader(t *testing.T) {
	header := buildProxyV2Header(t, "192.168.1.50", "10.0.0.1", 12345, 4222, proxyProtoFamilyInet)
	// Only send first 10 bytes (incomplete header)
	conn := newMockConn(header[:10])

	_, err := readProxyProtoV2Header(conn)
	if err == nil {
		t.Error("Expected error for truncated header, got nil")
	}
}

func TestProxyProtoV2ShortAddressData(t *testing.T) {
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
	if err == nil {
		t.Error("Expected error for short address data, got nil")
	}
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

	// Create mock conn
	mockConn := newMockConn(nil)

	// Wrap it
	wrapped := &proxyConn{
		Conn:       mockConn,
		remoteAddr: proxyAddr,
	}

	// Verify RemoteAddr returns the proxied address
	addr := wrapped.RemoteAddr()
	if addr.String() != "10.0.0.50:12345" {
		t.Errorf("Expected wrapped address '10.0.0.50:12345', got '%s'", addr.String())
	}

	// Verify the underlying connection still has original address
	if mockConn.RemoteAddr().String() != originalAddr.String() {
		t.Errorf("Expected original address '%s', got '%s'", originalAddr.String(), mockConn.RemoteAddr().String())
	}
}

func TestProxyProtoV2EndToEnd(t *testing.T) {
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
	if err != nil {
		t.Fatalf("Error connecting to server: %v", err)
	}
	defer conn.Close()

	// Send PROXY protocol header
	clientIP := "203.0.113.50"
	clientPort := uint16(54321)
	header := buildProxyV2Header(t, clientIP, "127.0.0.1", clientPort, 4222, proxyProtoFamilyInet)

	if _, err := conn.Write(header); err != nil {
		t.Fatalf("Error writing PROXY header: %v", err)
	}

	// Send CONNECT message
	connectMsg := "CONNECT {\"verbose\":false,\"pedantic\":false,\"protocol\":1}\r\n"
	if _, err := conn.Write([]byte(connectMsg)); err != nil {
		t.Fatalf("Error writing CONNECT: %v", err)
	}

	// Read INFO and +OK
	buf := make([]byte, 4096)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("Error reading response: %v", err)
	}

	response := string(buf[:n])
	if !strings.Contains(response, "INFO") {
		t.Errorf("Expected INFO in response, got: %s", response)
	}

	// Give server time to process
	time.Sleep(100 * time.Millisecond)

	// Check server's client list to verify the IP was extracted correctly
	s.mu.Lock()
	clients := s.clients
	s.mu.Unlock()
	if len(clients) == 0 {
		t.Fatal("Expected at least one client connection")
	}

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

	if foundClient == nil {
		t.Errorf("Expected to find client with IP %s:%d", clientIP, clientPort)
	}
}

func TestProxyProtoV2LocalCommandEndToEnd(t *testing.T) {
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
	if err != nil {
		t.Fatalf("Error connecting to server: %v", err)
	}
	defer conn.Close()

	// Send PROXY protocol LOCAL header (health check)
	header := buildProxyV2LocalHeader()

	if _, err := conn.Write(header); err != nil {
		t.Fatalf("Error writing PROXY LOCAL header: %v", err)
	}

	// Send CONNECT message
	connectMsg := "CONNECT {\"verbose\":false,\"pedantic\":false,\"protocol\":1}\r\n"
	if _, err := conn.Write([]byte(connectMsg)); err != nil {
		t.Fatalf("Error writing CONNECT: %v", err)
	}

	// Read INFO and +OK
	buf := make([]byte, 4096)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("Error reading response: %v", err)
	}

	response := string(buf[:n])
	if !strings.Contains(response, "INFO") {
		t.Errorf("Expected INFO in response, got: %s", response)
	}

	// Connection should work normally with LOCAL command
	time.Sleep(100 * time.Millisecond)

	// Verify at least one client is connected
	s.mu.Lock()
	numClients := len(s.clients)
	s.mu.Unlock()

	if numClients == 0 {
		t.Error("Expected at least one client connection")
	}
}

// ============================================================================
// PROXY Protocol v1 Tests
// ============================================================================

func TestProxyProtoV1ParseTCP4(t *testing.T) {
	header := buildProxyV1Header(t, "TCP4", "192.168.1.50", "10.0.0.1", 12345, 4222)
	conn := newMockConn(header)

	addr, err := readProxyProtoHeader(conn)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if addr == nil {
		t.Fatal("Expected address, got nil")
	}

	expectedSrcIP := "192.168.1.50"
	if addr.srcIP.String() != expectedSrcIP {
		t.Errorf("Expected src IP %s, got %s", expectedSrcIP, addr.srcIP.String())
	}

	if addr.srcPort != 12345 {
		t.Errorf("Expected src port 12345, got %d", addr.srcPort)
	}

	if addr.dstPort != 4222 {
		t.Errorf("Expected dst port 4222, got %d", addr.dstPort)
	}
}

func TestProxyProtoV1ParseTCP6(t *testing.T) {
	header := buildProxyV1Header(t, "TCP6", "2001:db8::1", "2001:db8::2", 54321, 4222)
	conn := newMockConn(header)

	addr, err := readProxyProtoHeader(conn)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if addr == nil {
		t.Fatal("Expected address, got nil")
	}

	expectedSrcIP := "2001:db8::1"
	if addr.srcIP.String() != expectedSrcIP {
		t.Errorf("Expected src IP %s, got %s", expectedSrcIP, addr.srcIP.String())
	}

	if addr.srcPort != 54321 {
		t.Errorf("Expected src port 54321, got %d", addr.srcPort)
	}
}

func TestProxyProtoV1ParseUnknown(t *testing.T) {
	header := buildProxyV1Header(t, "UNKNOWN", "", "", 0, 0)
	conn := newMockConn(header)

	addr, err := readProxyProtoHeader(conn)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// UNKNOWN should return nil address (like v2 LOCAL)
	if addr != nil {
		t.Errorf("Expected nil address for UNKNOWN, got: %v", addr)
	}
}

func TestProxyProtoV1InvalidFormat(t *testing.T) {
	// Missing fields
	header := []byte("PROXY TCP4 192.168.1.1\r\n")
	conn := newMockConn(header)

	_, err := readProxyProtoHeader(conn)
	if !errors.Is(err, errProxyProtoInvalid) {
		t.Errorf("Expected errProxyProtoInvalid, got: %v", err)
	}
}

func TestProxyProtoV1LineTooLong(t *testing.T) {
	// Create a line longer than 107 bytes
	longIP := strings.Repeat("1234567890", 12) // 120 chars
	header := []byte(fmt.Sprintf("PROXY TCP4 %s 10.0.0.1 12345 443\r\n", longIP))
	conn := newMockConn(header)

	_, err := readProxyProtoHeader(conn)
	if !errors.Is(err, errProxyProtoInvalid) {
		t.Errorf("Expected errProxyProtoInvalid, got: %v", err)
	}
}

func TestProxyProtoV1InvalidIP(t *testing.T) {
	header := []byte("PROXY TCP4 not.an.ip.addr 10.0.0.1 12345 443\r\n")
	conn := newMockConn(header)

	_, err := readProxyProtoHeader(conn)
	if !errors.Is(err, errProxyProtoInvalid) {
		t.Errorf("Expected errProxyProtoInvalid, got: %v", err)
	}
}

func TestProxyProtoV1MismatchedProtocol(t *testing.T) {
	// TCP4 with IPv6 address
	header := buildProxyV1Header(t, "TCP4", "2001:db8::1", "2001:db8::2", 12345, 443)
	conn := newMockConn(header)

	_, err := readProxyProtoHeader(conn)
	if !errors.Is(err, errProxyProtoInvalid) {
		t.Errorf("Expected errProxyProtoInvalid for mismatched protocol, got: %v", err)
	}

	// TCP6 with IPv4 address
	header2 := buildProxyV1Header(t, "TCP6", "192.168.1.1", "10.0.0.1", 12345, 443)
	conn2 := newMockConn(header2)

	_, err = readProxyProtoHeader(conn2)
	if !errors.Is(err, errProxyProtoInvalid) {
		t.Errorf("Expected errProxyProtoInvalid for mismatched protocol, got: %v", err)
	}
}

func TestProxyProtoV1InvalidPort(t *testing.T) {
	header := []byte("PROXY TCP4 192.168.1.1 10.0.0.1 99999 443\r\n")
	conn := newMockConn(header)

	_, err := readProxyProtoHeader(conn)
	if err == nil {
		t.Error("Expected error for invalid port, got nil")
	}
}

func TestProxyProtoV1EndToEnd(t *testing.T) {
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
	if err != nil {
		t.Fatalf("Error connecting to server: %v", err)
	}
	defer conn.Close()

	// Send PROXY protocol v1 header
	clientIP := "203.0.113.50"
	clientPort := uint16(54321)
	header := buildProxyV1Header(t, "TCP4", clientIP, "127.0.0.1", clientPort, 4222)

	if _, err := conn.Write(header); err != nil {
		t.Fatalf("Error writing PROXY header: %v", err)
	}

	// Send CONNECT message
	connectMsg := "CONNECT {\"verbose\":false,\"pedantic\":false,\"protocol\":1}\r\n"
	if _, err := conn.Write([]byte(connectMsg)); err != nil {
		t.Fatalf("Error writing CONNECT: %v", err)
	}

	// Read INFO and +OK
	buf := make([]byte, 4096)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("Error reading response: %v", err)
	}

	response := string(buf[:n])
	if !strings.Contains(response, "INFO") {
		t.Errorf("Expected INFO in response, got: %s", response)
	}

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

	if foundClient == nil {
		t.Errorf("Expected to find client with IP %s:%d", clientIP, clientPort)
	}
}

// ============================================================================
// Mixed Protocol Version Tests
// ============================================================================

func TestProxyProtoVersionDetection(t *testing.T) {
	// Test v1 detection
	v1Header := buildProxyV1Header(t, "TCP4", "192.168.1.1", "10.0.0.1", 12345, 443)
	conn1 := newMockConn(v1Header)

	addr1, err := readProxyProtoHeader(conn1)
	if err != nil {
		t.Fatalf("v1 detection failed: %v", err)
	}
	if addr1 == nil {
		t.Fatal("Expected v1 address, got nil")
	}
	if addr1.srcIP.String() != "192.168.1.1" {
		t.Errorf("v1: Expected 192.168.1.1, got %s", addr1.srcIP.String())
	}

	// Test v2 detection
	v2Header := buildProxyV2Header(t, "192.168.1.2", "10.0.0.1", 54321, 443, proxyProtoFamilyInet)
	conn2 := newMockConn(v2Header)

	addr2, err := readProxyProtoHeader(conn2)
	if err != nil {
		t.Fatalf("v2 detection failed: %v", err)
	}
	if addr2 == nil {
		t.Fatal("Expected v2 address, got nil")
	}
	if addr2.srcIP.String() != "192.168.1.2" {
		t.Errorf("v2: Expected 192.168.1.2, got %s", addr2.srcIP.String())
	}
}

func TestProxyProtoUnrecognizedVersion(t *testing.T) {
	// Invalid header that doesn't match v1 or v2
	header := []byte("HELLO WORLD\r\n")
	conn := newMockConn(header)

	_, err := readProxyProtoHeader(conn)
	if err == nil {
		t.Error("Expected error for unrecognized protocol, got nil")
	}
	if !strings.Contains(err.Error(), "unrecognized") {
		t.Errorf("Expected 'unrecognized' in error message, got: %v", err)
	}
}
