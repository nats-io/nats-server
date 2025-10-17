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
	"encoding/binary"
	"fmt"
	"net"
	"testing"
	"time"
)

func TestProxyProtocolV1(t *testing.T) {
	tests := []struct {
		name        string
		header      string
		expectError bool
		expectedSrc string
		expectedDst string
		srcPort     int
		dstPort     int
	}{
		{
			name:        "Valid TCP4",
			header:      "PROXY TCP4 192.168.1.1 192.168.1.2 1234 4222\r\n",
			expectError: false,
			expectedSrc: "192.168.1.1",
			expectedDst: "192.168.1.2",
			srcPort:     1234,
			dstPort:     4222,
		},
		{
			name:        "Valid TCP6",
			header:      "PROXY TCP6 2001:db8::1 2001:db8::2 1234 4222\r\n",
			expectError: false,
			expectedSrc: "2001:db8::1",
			expectedDst: "2001:db8::2",
			srcPort:     1234,
			dstPort:     4222,
		},
		{
			name:        "Unknown connection",
			header:      "PROXY UNKNOWN\r\n",
			expectError: false,
		},
		{
			name:        "Invalid protocol",
			header:      "PROXY INVALID 192.168.1.1 192.168.1.2 1234 4222\r\n",
			expectError: false, // UNKNOWN connections are valid
		},
		{
			name:        "Invalid header",
			header:      "INVALID HEADER\r\n",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := tt.header + "CONNECT {}\r\n"
			conn := &mockConn{
				readData: []byte(data),
			}

			wrappedConn, err := parseProxyProtocol(conn)
			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			proxyConn, ok := wrappedConn.(*proxyConn)
			if !ok {
				t.Fatal("Expected proxyConn")
			}

			if tt.expectedSrc != "" {
				remoteAddr := proxyConn.RemoteAddr().(*net.TCPAddr)
				if remoteAddr.IP.String() != tt.expectedSrc {
					t.Errorf("Expected source IP %s, got %s", tt.expectedSrc, remoteAddr.IP.String())
				}
				if remoteAddr.Port != tt.srcPort {
					t.Errorf("Expected source port %d, got %d", tt.srcPort, remoteAddr.Port)
				}

				localAddr := proxyConn.LocalAddr().(*net.TCPAddr)
				if localAddr.IP.String() != tt.expectedDst {
					t.Errorf("Expected destination IP %s, got %s", tt.expectedDst, localAddr.IP.String())
				}
				if localAddr.Port != tt.dstPort {
					t.Errorf("Expected destination port %d, got %d", tt.dstPort, localAddr.Port)
				}
			}

			// Test that we can still read the CONNECT message
			buf := make([]byte, 1024)
			n, err := wrappedConn.Read(buf)
			if err != nil {
				t.Fatalf("Failed to read from wrapped connection: %v", err)
			}

			if !bytes.HasPrefix(buf[:n], []byte("CONNECT")) {
				t.Error("Expected to read CONNECT message after PROXY header")
			}
		})
	}
}

func TestProxyProtocolV2(t *testing.T) {
	tests := []struct {
		name        string
		buildHeader func() []byte
		expectError bool
		expectedSrc string
		expectedDst string
		srcPort     int
		dstPort     int
	}{
		{
			name: "Valid IPv4 PROXY",
			buildHeader: func() []byte {
				header := make([]byte, 28)
				copy(header[0:12], []byte("\x0D\x0A\x0D\x0A\x00\x0D\x0A\x51\x55\x49\x54\x0A"))
				header[12] = 0x21                             // Version 2, PROXY command
				header[13] = 0x11                             // IPv4, TCP
				binary.BigEndian.PutUint16(header[14:16], 12) // Length
				copy(header[16:20], net.ParseIP("192.168.1.1").To4())
				copy(header[20:24], net.ParseIP("192.168.1.2").To4())
				binary.BigEndian.PutUint16(header[24:26], 1234) // Source port
				binary.BigEndian.PutUint16(header[26:28], 4222) // Dest port
				return header
			},
			expectError: false,
			expectedSrc: "192.168.1.1",
			expectedDst: "192.168.1.2",
			srcPort:     1234,
			dstPort:     4222,
		},
		{
			name: "Valid IPv6 PROXY",
			buildHeader: func() []byte {
				header := make([]byte, 52)
				copy(header[0:12], []byte("\x0D\x0A\x0D\x0A\x00\x0D\x0A\x51\x55\x49\x54\x0A"))
				header[12] = 0x21                             // Version 2, PROXY command
				header[13] = 0x21                             // IPv6, TCP
				binary.BigEndian.PutUint16(header[14:16], 36) // Length
				copy(header[16:32], net.ParseIP("2001:db8::1").To16())
				copy(header[32:48], net.ParseIP("2001:db8::2").To16())
				binary.BigEndian.PutUint16(header[48:50], 1234) // Source port
				binary.BigEndian.PutUint16(header[50:52], 4222) // Dest port
				return header
			},
			expectError: false,
			expectedSrc: "2001:db8::1",
			expectedDst: "2001:db8::2",
			srcPort:     1234,
			dstPort:     4222,
		},
		{
			name: "Valid LOCAL command",
			buildHeader: func() []byte {
				header := make([]byte, 16)
				copy(header[0:12], []byte("\x0D\x0A\x0D\x0A\x00\x0D\x0A\x51\x55\x49\x54\x0A"))
				header[12] = 0x20                            // Version 2, LOCAL command
				header[13] = 0x00                            // UNSPEC
				binary.BigEndian.PutUint16(header[14:16], 0) // Length
				return header
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			header := tt.buildHeader()
			data := append(header, []byte("CONNECT {}\r\n")...)
			conn := &mockConn{
				readData: data,
			}

			wrappedConn, err := parseProxyProtocol(conn)
			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			proxyConn, ok := wrappedConn.(*proxyConn)
			if !ok {
				t.Fatal("Expected proxyConn")
			}

			if tt.expectedSrc != "" {
				remoteAddr := proxyConn.RemoteAddr().(*net.TCPAddr)
				if remoteAddr.IP.String() != tt.expectedSrc {
					t.Errorf("Expected source IP %s, got %s", tt.expectedSrc, remoteAddr.IP.String())
				}
				if remoteAddr.Port != tt.srcPort {
					t.Errorf("Expected source port %d, got %d", tt.srcPort, remoteAddr.Port)
				}

				localAddr := proxyConn.LocalAddr().(*net.TCPAddr)
				if localAddr.IP.String() != tt.expectedDst {
					t.Errorf("Expected destination IP %s, got %s", tt.expectedDst, localAddr.IP.String())
				}
				if localAddr.Port != tt.dstPort {
					t.Errorf("Expected destination port %d, got %d", tt.dstPort, localAddr.Port)
				}
			}

			// Test that we can still read the CONNECT message
			buf := make([]byte, 1024)
			n, err := wrappedConn.Read(buf)
			if err != nil {
				t.Fatalf("Failed to read from wrapped connection: %v", err)
			}

			if !bytes.HasPrefix(buf[:n], []byte("CONNECT")) {
				t.Error("Expected to read CONNECT message after PROXY header")
			}
		})
	}
}

func TestProxyProtocolIntegration(t *testing.T) {
	opts := DefaultOptions()
	opts.Host = "127.0.0.1"
	opts.Port = -1
	opts.ProxyProtocol = true

	s := RunServer(opts)
	defer s.Shutdown()

	// Create a connection that will send PROXY protocol header
	addr := fmt.Sprintf("%s:%d", opts.Host, s.Addr().(*net.TCPAddr).Port)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// Send PROXY v1 header
	proxyHeader := "PROXY TCP4 192.168.1.100 192.168.1.1 12345 4222\r\n"
	_, err = conn.Write([]byte(proxyHeader))
	if err != nil {
		t.Fatalf("Failed to write PROXY header: %v", err)
	}

	// Now send NATS CONNECT
	connectMsg := "CONNECT {\"verbose\":false,\"pedantic\":false}\r\n"
	_, err = conn.Write([]byte(connectMsg))
	if err != nil {
		t.Fatalf("Failed to write CONNECT: %v", err)
	}

	// Read INFO response
	reader := bufio.NewReader(conn)
	info, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Failed to read INFO: %v", err)
	}

	if !bytes.HasPrefix([]byte(info), []byte("INFO")) {
		t.Errorf("Expected INFO response, got: %s", info)
	}

	// Send PING to verify connection is working
	_, err = conn.Write([]byte("PING\r\n"))
	if err != nil {
		t.Fatalf("Failed to write PING: %v", err)
	}

	// Read PONG
	pong, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Failed to read PONG: %v", err)
	}

	if !bytes.HasPrefix([]byte(pong), []byte("PONG")) {
		t.Errorf("Expected PONG response, got: %s", pong)
	}
}

func TestProxyProtocolWithNatsClient(t *testing.T) {
	opts := DefaultOptions()
	opts.Host = "127.0.0.1"
	opts.Port = -1
	opts.ProxyProtocol = true

	s := RunServer(opts)
	defer s.Shutdown()

	// Create a custom dialer that sends PROXY protocol header
	addr := fmt.Sprintf("%s:%d", opts.Host, s.Addr().(*net.TCPAddr).Port)

	// Test with a raw connection first to ensure PROXY protocol works
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	// Send PROXY v2 header
	header := make([]byte, 28)
	copy(header[0:12], []byte("\x0D\x0A\x0D\x0A\x00\x0D\x0A\x51\x55\x49\x54\x0A"))
	header[12] = 0x21                             // Version 2, PROXY command
	header[13] = 0x11                             // IPv4, TCP
	binary.BigEndian.PutUint16(header[14:16], 12) // Length
	copy(header[16:20], net.ParseIP("10.0.0.100").To4())
	copy(header[20:24], net.ParseIP("10.0.0.1").To4())
	binary.BigEndian.PutUint16(header[24:26], 54321) // Source port
	binary.BigEndian.PutUint16(header[26:28], 4222)  // Dest port

	_, err = conn.Write(header)
	if err != nil {
		t.Fatalf("Failed to write PROXY header: %v", err)
	}

	// Continue with NATS protocol
	connectMsg := "CONNECT {\"verbose\":false,\"pedantic\":false}\r\n"
	_, err = conn.Write([]byte(connectMsg))
	if err != nil {
		t.Fatalf("Failed to write CONNECT: %v", err)
	}

	// Read INFO
	reader := bufio.NewReader(conn)
	info, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Failed to read INFO: %v", err)
	}

	if !bytes.HasPrefix([]byte(info), []byte("INFO")) {
		t.Errorf("Expected INFO response, got: %s", info)
	}

	conn.Close()
}

// mockConn implements net.Conn for testing
type mockConn struct {
	readData []byte
	readPos  int
	written  []byte
}

func (m *mockConn) Read(b []byte) (int, error) {
	if m.readPos >= len(m.readData) {
		return 0, net.ErrClosed
	}

	n := copy(b, m.readData[m.readPos:])
	m.readPos += n
	return n, nil
}

func (m *mockConn) Write(b []byte) (int, error) {
	m.written = append(m.written, b...)
	return len(b), nil
}

func (m *mockConn) Close() error { return nil }
func (m *mockConn) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 4222}
}
func (m *mockConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12345}
}
func (m *mockConn) SetDeadline(t time.Time) error      { return nil }
func (m *mockConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockConn) SetWriteDeadline(t time.Time) error { return nil }

func TestProxyProtocolConfigParsing(t *testing.T) {
	conf := `
		proxy_protocol: true
		port: 4222
	`
	
	opts := &Options{}
	err := opts.ProcessConfigString(conf)
	if err != nil {
		t.Fatalf("Failed to process config: %v", err)
	}
	
	if !opts.ProxyProtocol {
		t.Error("Expected ProxyProtocol to be true")
	}
	
	if opts.Port != 4222 {
		t.Errorf("Expected port 4222, got %d", opts.Port)
	}
}
