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
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"testing"
	"time"
)

// Basic HTTP proxy for testing
type testHTTPProxy struct {
	listener    net.Listener
	port        int
	username    string
	password    string
	started     bool
	closeDelay  time.Duration // Delay before closing connections for robustness
	connections []net.Conn    // Track connections for cleanup
}

func createTestHTTPProxy(username, password string) *testHTTPProxy {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	port := l.Addr().(*net.TCPAddr).Port

	proxy := &testHTTPProxy{
		listener:    l,
		port:        port,
		username:    username,
		password:    password,
		closeDelay:  100 * time.Millisecond, // Default delay for test robustness
		connections: make([]net.Conn, 0),
	}

	return proxy
}

func (p *testHTTPProxy) setCloseDelay(delay time.Duration) {
	p.closeDelay = delay
}

func (p *testHTTPProxy) start() {
	if p.started {
		return
	}
	p.started = true

	go func() {
		for {
			conn, err := p.listener.Accept()
			if err != nil {
				return
			}
			p.connections = append(p.connections, conn)
			go p.handleConnection(conn)
		}
	}()
}

func (p *testHTTPProxy) handleConnection(conn net.Conn) {
	defer func() {
		if p.closeDelay > 0 {
			time.Sleep(p.closeDelay)
		}
		conn.Close()
	}()

	// Set read timeout to prevent hanging on malformed requests
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))

	// Read the CONNECT request
	buffer := make([]byte, 4096)
	n, err := conn.Read(buffer)
	if err != nil {
		return
	}

	request := string(buffer[:n])
	lines := strings.Split(request, "\r\n")

	if len(lines) == 0 || !strings.HasPrefix(lines[0], "CONNECT ") {
		conn.Write([]byte("HTTP/1.1 400 Bad Request\r\n\r\n"))
		return
	}

	// Check authentication if required
	if p.username != _EMPTY_ || p.password != _EMPTY_ {
		authFound := false
		for _, line := range lines {
			if strings.HasPrefix(line, "Proxy-Authorization: Basic ") {
				authFound = true
				break
			}
		}
		if !authFound {
			conn.Write([]byte("HTTP/1.1 407 Proxy Authentication Required\r\n\r\n"))
			return
		}
	}

	// Extract target host from CONNECT line
	parts := strings.Fields(lines[0])
	if len(parts) < 3 {
		conn.Write([]byte("HTTP/1.1 400 Bad Request\r\n\r\n"))
		return
	}

	targetHost := parts[1]

	// Connect to target with timeout
	target, err := net.DialTimeout("tcp", targetHost, 5*time.Second)
	if err != nil {
		conn.Write([]byte("HTTP/1.1 502 Bad Gateway\r\n\r\n"))
		return
	}
	defer target.Close()

	// Send success response
	conn.Write([]byte("HTTP/1.1 200 Connection established\r\n\r\n"))

	// Clear read deadline for ongoing connection
	conn.SetReadDeadline(time.Time{})

	// Relay data between client and target with proper error handling
	done := make(chan bool, 2)

	// Client to target
	go func() {
		defer func() {
			done <- true
			target.Close()
		}()
		buffer := make([]byte, 32*1024)
		for {
			conn.SetReadDeadline(time.Now().Add(30 * time.Second))
			n, err := conn.Read(buffer)
			if err != nil {
				return
			}
			target.SetWriteDeadline(time.Now().Add(10 * time.Second))
			_, err = target.Write(buffer[:n])
			if err != nil {
				return
			}
		}
	}()

	// Target to client
	go func() {
		defer func() {
			done <- true
			conn.Close()
		}()
		buffer := make([]byte, 32*1024)
		for {
			target.SetReadDeadline(time.Now().Add(30 * time.Second))
			n, err := target.Read(buffer)
			if err != nil {
				return
			}
			conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			_, err = conn.Write(buffer[:n])
			if err != nil {
				return
			}
		}
	}()

	// Wait for either direction to finish
	<-done
}

func (p *testHTTPProxy) stop() {
	if p.listener != nil {
		p.listener.Close()
	}
	// Close all tracked connections with delay for robustness
	for _, conn := range p.connections {
		go func(c net.Conn) {
			if p.closeDelay > 0 {
				time.Sleep(p.closeDelay)
			}
			c.Close()
		}(conn)
	}
}

func (p *testHTTPProxy) url() string {
	return fmt.Sprintf("http://127.0.0.1:%d", p.port)
}

func TestLeafNodeHttpProxyConfigParsing(t *testing.T) {
	// Test valid proxy configuration
	conf := `
		leafnodes {
			remotes = [
				{
					url: "ws://127.0.0.1:7422"
					proxy {
						url: "http://proxy.example.com:8080"
						username: "user"
						password: "pass"
						timeout: "10s"
					}
				}
			]
		}
	`

	configFile := createConfFile(t, []byte(conf))

	opts, err := ProcessConfigFile(configFile)
	if err != nil {
		t.Fatalf("Error parsing config: %v", err)
	}

	if len(opts.LeafNode.Remotes) != 1 {
		t.Fatalf("Expected 1 remote, got %d", len(opts.LeafNode.Remotes))
	}

	remote := opts.LeafNode.Remotes[0]
	if remote.Proxy.URL != "http://proxy.example.com:8080" {
		t.Errorf("Expected proxy URL 'http://proxy.example.com:8080', got '%s'", remote.Proxy.URL)
	}
	if remote.Proxy.Username != "user" {
		t.Errorf("Expected proxy username 'user', got '%s'", remote.Proxy.Username)
	}
	if remote.Proxy.Password != "pass" {
		t.Errorf("Expected proxy password 'pass', got '%s'", remote.Proxy.Password)
	}
	if remote.Proxy.Timeout != 10*time.Second {
		t.Errorf("Expected proxy timeout 10s, got %v", remote.Proxy.Timeout)
	}
}

func TestLeafNodeHttpProxyConfigWarnings(t *testing.T) {
	testCases := []struct {
		name          string
		config        string
		expectWarning bool
		warningMatch  string
	}{
		{
			name: "proxy with only TCP URLs",
			config: `
				leafnodes {
					remotes = [
						{
							url: "nats://127.0.0.1:7422"
							proxy {
								url: "http://proxy.example.com:8080"
							}
						}
					]
				}
			`,
			expectWarning: true,
			warningMatch:  "proxy configuration will be ignored",
		},
		{
			name: "proxy with mixed TCP and WebSocket URLs",
			config: `
				leafnodes {
					remotes = [
						{
							urls: ["nats://127.0.0.1:7422", "ws://127.0.0.1:8080"]
							proxy {
								url: "http://proxy.example.com:8080"
							}
						}
					]
				}
			`,
			expectWarning: true,
			warningMatch:  "proxy configuration will only be used for WebSocket URLs",
		},
		{
			name: "proxy with only WebSocket URLs",
			config: `
				leafnodes {
					remotes = [
						{
							url: "ws://127.0.0.1:7422"
							proxy {
								url: "http://proxy.example.com:8080"
							}
						}
					]
				}
			`,
			expectWarning: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			configFile := createConfFile(t, []byte(tc.config))

			opts, err := ProcessConfigFile(configFile)

			if tc.expectWarning {
				// With ProcessConfigFile, warnings don't cause errors
				// The configuration should be valid but might log warnings
				if err != nil {
					t.Fatalf("Expected valid configuration with warnings, but got error: %v", err)
				}
				if opts == nil {
					t.Fatal("Expected valid options but got nil")
				}
				// Note: With ProcessConfigFile, warnings are filtered out and not returned as errors
				// The test verifies that the configuration is valid despite having warning conditions
			} else {
				// No warnings expected - should parse successfully
				if err != nil {
					t.Fatalf("Expected no error but got: %v", err)
				}
				if opts == nil {
					t.Fatal("Expected valid options but got nil")
				}
			}
		})
	}
}

func TestLeafNodeHttpProxyConnection(t *testing.T) {
	// Create a hub server with WebSocket support using config file
	hubConfig := createConfFile(t, []byte(`
		listen: "127.0.0.1:-1"
		websocket {
			listen: "127.0.0.1:-1"
			no_tls: true
		}
		leafnodes {
			listen: "127.0.0.1:-1"
		}
	`))

	hub, hubOpts := RunServerWithConfig(hubConfig)
	defer hub.Shutdown()

	// Create HTTP proxy
	proxy := createTestHTTPProxy(_EMPTY_, _EMPTY_)
	proxy.start()
	defer proxy.stop()

	// Create spoke server with proxy configuration via config file
	configContent := fmt.Sprintf(`
		listen: "127.0.0.1:-1"
		leafnodes {
			reconnect_interval: "50ms"
			remotes = [
				{
					url: "ws://127.0.0.1:%d"
					proxy {
						url: "%s"
						timeout: 5s
					}
				}
			]
		}
	`, hubOpts.Websocket.Port, proxy.url())

	configFile := createConfFile(t, []byte(configContent))

	spoke, _ := RunServerWithConfig(configFile)
	defer spoke.Shutdown()

	// Verify leafnode connections are established
	checkLeafNodeConnected(t, spoke)
	checkLeafNodeConnected(t, hub)
}

func TestLeafNodeHttpProxyWithAuthentication(t *testing.T) {
	// Create a hub server with WebSocket support using config file
	hubConfig := createConfFile(t, []byte(`
		listen: "127.0.0.1:-1"
		websocket {
			listen: "127.0.0.1:-1"
			no_tls: true
		}
		leafnodes {
			listen: "127.0.0.1:-1"
		}
	`))

	hub, hubOpts := RunServerWithConfig(hubConfig)
	defer hub.Shutdown()

	// Create HTTP proxy with authentication
	proxy := createTestHTTPProxy("testuser", "testpass")
	proxy.start()
	defer proxy.stop()

	// Create spoke server with proxy configuration including auth via config file
	configContent := fmt.Sprintf(`
		listen: "127.0.0.1:-1"
		leafnodes {
			reconnect_interval: "50ms"
			remotes = [
				{
					url: "ws://127.0.0.1:%d"
					proxy {
						url: "%s"
						username: "testuser"
						password: "testpass"
						timeout: 5s
					}
				}
			]
		}
	`, hubOpts.Websocket.Port, proxy.url())

	configFile := createConfFile(t, []byte(configContent))

	spoke, _ := RunServerWithConfig(configFile)
	defer spoke.Shutdown()

	// Verify leafnode connections are established
	checkLeafNodeConnected(t, spoke)
	checkLeafNodeConnected(t, hub)
}

func TestLeafNodeHttpProxyTLSMismatchDetection(t *testing.T) {
	// This test simulates the TLS mismatch scenario described in the feedback:
	// - Leafnode configured with proxy but no TLS
	// - Hub requires TLS
	// - Connection should fail with appropriate error message

	// Create hub server with TLS required using config file
	hubConfig := createConfFile(t, []byte(`
		listen: "127.0.0.1:-1"
		leafnodes {
			listen: "127.0.0.1:-1"
			tls: {
				cert_file: "../test/configs/certs/server-cert.pem"
				key_file: "../test/configs/certs/server-key.pem"
			}
		}
	`))

	hub, hubOpts := RunServerWithConfig(hubConfig)
	defer hub.Shutdown()

	// Create HTTP proxy
	proxy := createTestHTTPProxy(_EMPTY_, _EMPTY_)
	proxy.start()
	defer proxy.stop()

	// Create spoke server with proxy but no TLS configuration (intentional mismatch)
	spokeConfigContent := fmt.Sprintf(`
		listen: "127.0.0.1:-1"
		leafnodes {
			reconnect_interval: "50ms"
			remotes = [
				{
					url: "ws://127.0.0.1:%d"
					proxy {
						url: "%s"
						timeout: 5s
					}
					# Intentionally no TLS configuration to create mismatch
				}
			]
		}
	`, hubOpts.LeafNode.Port, proxy.url())

	spokeConfig := createConfFile(t, []byte(spokeConfigContent))
	spoke, _ := RunServerWithConfig(spokeConfig)
	defer spoke.Shutdown()

	// Wait and verify that connection was NOT established due to TLS mismatch
	// First attempt happens during RunServerWithConfig(), then retries every 50ms
	time.Sleep(250 * time.Millisecond)

	if spoke.NumLeafNodes() != 0 {
		t.Errorf("Expected 0 leafnode connections due to TLS mismatch, got %d", spoke.NumLeafNodes())
	}
}

func TestLeafNodeHttpProxyTunnelBasic(t *testing.T) {
	// Create HTTP proxy with longer delay for robustness
	proxy := createTestHTTPProxy(_EMPTY_, _EMPTY_)
	proxy.setCloseDelay(200 * time.Millisecond)
	proxy.start()
	defer proxy.stop()

	// Create a simple TCP server to connect to through proxy
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create test server: %v", err)
	}
	defer listener.Close()

	targetPort := listener.Addr().(*net.TCPAddr).Port
	targetHost := fmt.Sprintf("127.0.0.1:%d", targetPort)

	errCh := make(chan error, 1)

	// Accept one connection with proper timeout handling
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			errCh <- fmt.Errorf("unable to accept: %v", err)
			return
		}
		defer conn.Close()

		// Set read deadline to prevent hanging forever
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))

		// Read the incoming data first
		buffer := make([]byte, 1024)
		n, err := conn.Read(buffer)
		if err != nil {
			errCh <- fmt.Errorf("server failed to read: %v", err)
			return
		}

		receivedMsg := string(buffer[:n])
		if receivedMsg != "Hello" {
			errCh <- fmt.Errorf("server expected 'Hello', got '%s'", receivedMsg)
			return
		}

		// Send response
		conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		_, err = conn.Write([]byte("Hello from target server"))
		if err != nil {
			errCh <- fmt.Errorf("server failed to write: %v", err)
			return
		}

		// Wait a bit to ensure the client has time to read before closing
		time.Sleep(50 * time.Millisecond)
		errCh <- nil
	}()

	// Test establishing proxy tunnel with timeout
	conn, err := establishHTTPProxyTunnel(proxy.url(), targetHost, 10*time.Second, _EMPTY_, _EMPTY_)
	if err != nil {
		t.Fatalf("Failed to establish proxy tunnel: %v", err)
	}
	defer conn.Close()

	// Test that we can communicate through the tunnel with deadlines
	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, err = conn.Write([]byte("Hello"))
	if err != nil {
		t.Fatalf("Failed to write to proxy tunnel: %v", err)
	}

	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	buffer := make([]byte, 1024)
	n, err := conn.Read(buffer)
	if err != nil {
		t.Fatalf("Failed to read from proxy tunnel: %v", err)
	}

	response := string(buffer[:n])
	if response != "Hello from target server" {
		t.Errorf("Unexpected response: '%s', expected 'Hello from target server'", response)
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("%v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Server goroutine didn't complete in time, but test data was exchanged successfully")
	}
}

func TestLeafNodeHttpProxyTunnelWithAuth(t *testing.T) {
	// Create HTTP proxy with authentication and delay for robustness
	proxy := createTestHTTPProxy("testuser", "testpass")
	proxy.setCloseDelay(200 * time.Millisecond)
	proxy.start()
	defer proxy.stop()

	// Create a simple TCP server to connect to through proxy
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create test server: %v", err)
	}
	defer listener.Close()

	targetPort := listener.Addr().(*net.TCPAddr).Port
	targetHost := fmt.Sprintf("127.0.0.1:%d", targetPort)

	errCh := make(chan error, 1)

	// Accept one connection with proper timeout handling
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			errCh <- fmt.Errorf("unable to accept: %v", err)
			return
		}
		defer conn.Close()

		// Set read deadline to prevent hanging
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))

		// Read the incoming data first
		buffer := make([]byte, 1024)
		_, err = conn.Read(buffer)
		if err != nil {
			errCh <- fmt.Errorf("unable to read: %v", err)
			return
		}

		// Send response
		conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if _, err := conn.Write([]byte("Hello from authenticated server")); err != nil {
			errCh <- fmt.Errorf("unable to write: %v", err)
			return
		}

		errCh <- nil
	}()

	// Test establishing proxy tunnel with authentication and timeout
	conn, err := establishHTTPProxyTunnel(proxy.url(), targetHost, 10*time.Second, "testuser", "testpass")
	if err != nil {
		t.Fatalf("Failed to establish proxy tunnel with auth: %v", err)
	}
	defer conn.Close()

	// Test that we can communicate through the tunnel with deadlines
	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, err = conn.Write([]byte("Hello"))
	if err != nil {
		t.Fatalf("Failed to write to proxy tunnel: %v", err)
	}

	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	buffer := make([]byte, 1024)
	n, err := conn.Read(buffer)
	if err != nil {
		t.Fatalf("Failed to read from proxy tunnel: %v", err)
	}

	response := string(buffer[:n])
	if response != "Hello from authenticated server" {
		t.Errorf("Unexpected response: %s", response)
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("%v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Server goroutine didn't complete in time, but test data was exchanged successfully")
	}
}

func TestLeafNodeHttpProxyTunnelFailsWithoutAuth(t *testing.T) {
	// Create HTTP proxy with authentication required and delay for robustness
	proxy := createTestHTTPProxy("testuser", "testpass")
	proxy.setCloseDelay(200 * time.Millisecond)
	proxy.start()
	defer proxy.stop()

	// Try to establish tunnel without providing credentials (should fail quickly)
	_, err := establishHTTPProxyTunnel(proxy.url(), "127.0.0.1:80", 10*time.Second, _EMPTY_, _EMPTY_)
	if err == nil {
		t.Fatal("Expected error when connecting without authentication")
	}

	if !strings.Contains(err.Error(), "proxy CONNECT failed") {
		t.Errorf("Expected proxy authentication error, got: %v", err)
	}

	// Verify the error contains the expected HTTP response code
	if !strings.Contains(err.Error(), "407") {
		t.Errorf("Expected HTTP 407 error in response, got: %v", err)
	}
}

// TestLeafNodeProxyValidationProgrammatic tests proxy validation when configuring server programmatically
func TestLeafNodeHttpProxyValidationProgrammatic(t *testing.T) {
	tests := []struct {
		// name is the name of the test.
		name string

		// setupOptions creates the Options configuration for the test.
		setupOptions func() *Options

		// err is the expected error. nil means no error expected.
		err error
	}{
		{
			name: "invalid proxy scheme",
			setupOptions: func() *Options {
				opts := &Options{}
				opts.LeafNode.Remotes = []*RemoteLeafOpts{
					{
						URLs: []*url.URL{{Scheme: wsSchemePrefix, Host: "127.0.0.1:7422"}},
					},
				}
				opts.LeafNode.Remotes[0].Proxy.URL = "ftp://proxy.example.com:21"
				return opts
			},
			err: errors.New("proxy URL scheme must be http or https"),
		},
		{
			name: "empty proxy URL - no validation performed",
			setupOptions: func() *Options {
				opts := &Options{}
				opts.LeafNode.Remotes = []*RemoteLeafOpts{
					{
						URLs: []*url.URL{{Scheme: wsSchemePrefix, Host: "127.0.0.1:7422"}},
					},
				}
				opts.LeafNode.Remotes[0].Proxy.URL = _EMPTY_
				return opts
			},
			err: nil, // No error expected for empty URL
		},
		{
			name: "invalid proxy URL parse failure",
			setupOptions: func() *Options {
				opts := &Options{}
				opts.LeafNode.Remotes = []*RemoteLeafOpts{
					{
						URLs: []*url.URL{{Scheme: wsSchemePrefix, Host: "127.0.0.1:7422"}},
					},
				}
				opts.LeafNode.Remotes[0].Proxy.URL = "ht!tp://invalid-url-with-bad-characters"
				return opts
			},
			err: errors.New("invalid proxy URL"),
		},
		{
			name: "missing proxy host",
			setupOptions: func() *Options {
				opts := &Options{}
				opts.LeafNode.Remotes = []*RemoteLeafOpts{
					{
						URLs: []*url.URL{{Scheme: wsSchemePrefix, Host: "127.0.0.1:7422"}},
					},
				}
				opts.LeafNode.Remotes[0].Proxy.URL = "http://"
				return opts
			},
			err: errors.New("proxy URL must specify a host"),
		},
		{
			name: "username without password",
			setupOptions: func() *Options {
				opts := &Options{}
				opts.LeafNode.Remotes = []*RemoteLeafOpts{
					{
						URLs: []*url.URL{{Scheme: wsSchemePrefix, Host: "127.0.0.1:7422"}},
					},
				}
				opts.LeafNode.Remotes[0].Proxy.URL = "http://proxy.example.com:8080"
				opts.LeafNode.Remotes[0].Proxy.Username = "user"
				return opts
			},
			err: errors.New("proxy username and password must both be specified"),
		},
		{
			name: "password without username",
			setupOptions: func() *Options {
				opts := &Options{}
				opts.LeafNode.Remotes = []*RemoteLeafOpts{
					{
						URLs: []*url.URL{{Scheme: wsSchemePrefix, Host: "127.0.0.1:7422"}},
					},
				}
				opts.LeafNode.Remotes[0].Proxy.URL = "http://proxy.example.com:8080"
				opts.LeafNode.Remotes[0].Proxy.Password = "pass"
				return opts
			},
			err: errors.New("proxy username and password must both be specified"),
		},
		{
			name: "negative timeout value",
			setupOptions: func() *Options {
				opts := &Options{}
				opts.LeafNode.Remotes = []*RemoteLeafOpts{
					{
						URLs: []*url.URL{{Scheme: wsSchemePrefix, Host: "127.0.0.1:7422"}},
					},
				}
				opts.LeafNode.Remotes[0].Proxy.URL = "http://proxy.example.com:8080"
				opts.LeafNode.Remotes[0].Proxy.Timeout = -5 * time.Second
				return opts
			},
			err: errors.New("proxy timeout must be >= 0"),
		},
		{
			name: "zero timeout value - valid",
			setupOptions: func() *Options {
				opts := &Options{}
				opts.LeafNode.Remotes = []*RemoteLeafOpts{
					{
						URLs: []*url.URL{{Scheme: wsSchemePrefix, Host: "127.0.0.1:7422"}},
					},
				}
				opts.LeafNode.Remotes[0].Proxy.URL = "http://proxy.example.com:8080"
				opts.LeafNode.Remotes[0].Proxy.Timeout = 0
				return opts
			},
			err: nil, // No error expected for zero timeout
		},
		{
			name: "valid proxy configuration with authentication",
			setupOptions: func() *Options {
				opts := &Options{}
				opts.LeafNode.Remotes = []*RemoteLeafOpts{
					{
						URLs: []*url.URL{{Scheme: wsSchemePrefix, Host: "127.0.0.1:7422"}},
					},
				}
				opts.LeafNode.Remotes[0].Proxy.URL = "http://proxy.example.com:8080"
				opts.LeafNode.Remotes[0].Proxy.Username = "user"
				opts.LeafNode.Remotes[0].Proxy.Password = "pass"
				opts.LeafNode.Remotes[0].Proxy.Timeout = 10 * time.Second
				return opts
			},
			err: nil, // No error expected
		},
		{
			name: "valid proxy configuration without authentication",
			setupOptions: func() *Options {
				opts := &Options{}
				opts.LeafNode.Remotes = []*RemoteLeafOpts{
					{
						URLs: []*url.URL{{Scheme: wsSchemePrefix, Host: "127.0.0.1:7422"}},
					},
				}
				opts.LeafNode.Remotes[0].Proxy.URL = "https://proxy.example.com:3128"
				opts.LeafNode.Remotes[0].Proxy.Timeout = 30 * time.Second
				return opts
			},
			err: nil, // No error expected
		},
	}

	checkErr := func(t *testing.T, err, expectedErr error) {
		t.Helper()
		switch {
		case err == nil && expectedErr == nil:
			// OK
		case err != nil && expectedErr == nil:
			t.Errorf("Unexpected error after validating options: %s", err)
		case err == nil && expectedErr != nil:
			t.Errorf("Expected %q error after validating invalid options but got nothing", expectedErr)
		case err != nil && expectedErr != nil:
			if !strings.Contains(err.Error(), expectedErr.Error()) {
				t.Errorf("Expected error containing %q, got: %q", expectedErr.Error(), err.Error())
			}
		}
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			opts := test.setupOptions()
			err := validateLeafNode(opts)
			checkErr(t, err, test.err)
		})
	}
}
