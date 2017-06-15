// Copyright 2015-2016 Apcera Inc. All rights reserved.

package server

import (
	"flag"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/go-nats"
)

func DefaultOptions() *Options {
	return &Options{
		Host:     "localhost",
		Port:     -1,
		HTTPPort: -1,
		Cluster:  ClusterOpts{Port: -1},
		ProfPort: -1,
		NoLog:    true,
		NoSigs:   true,
		Debug:    true,
		Trace:    true,
	}
}

// New Go Routine based server
func RunServer(opts *Options) *Server {
	if opts == nil {
		opts = DefaultOptions()
	}
	s := New(opts)

	if s == nil {
		panic("No NATS Server object returned.")
	}

	if !opts.NoLog {
		s.ConfigureLogger()
	}

	// Run server in Go routine.
	go s.Start()

	// Wait for accept loop(s) to be started
	if !s.ReadyForConnections(10 * time.Second) {
		panic("Unable to start NATS Server in Go Routine")
	}
	return s
}

func TestStartProfiler(t *testing.T) {
	s := New(DefaultOptions())
	s.StartProfiler()
	s.mu.Lock()
	s.profiler.Close()
	s.mu.Unlock()
}

func TestStartupAndShutdown(t *testing.T) {

	opts := DefaultOptions()

	s := RunServer(opts)
	defer s.Shutdown()

	if !s.isRunning() {
		t.Fatal("Could not run server")
	}

	// Debug stuff.
	numRoutes := s.NumRoutes()
	if numRoutes != 0 {
		t.Fatalf("Expected numRoutes to be 0 vs %d\n", numRoutes)
	}

	numRemotes := s.NumRemotes()
	if numRemotes != 0 {
		t.Fatalf("Expected numRemotes to be 0 vs %d\n", numRemotes)
	}

	numClients := s.NumClients()
	if numClients != 0 && numClients != 1 {
		t.Fatalf("Expected numClients to be 1 or 0 vs %d\n", numClients)
	}

	numSubscriptions := s.NumSubscriptions()
	if numSubscriptions != 0 {
		t.Fatalf("Expected numSubscriptions to be 0 vs %d\n", numSubscriptions)
	}
}

func TestTlsCipher(t *testing.T) {
	if strings.Compare(tlsCipher(0x0005), "TLS_RSA_WITH_RC4_128_SHA") != 0 {
		t.Fatalf("Invalid tls cipher")
	}
	if strings.Compare(tlsCipher(0x000a), "TLS_RSA_WITH_3DES_EDE_CBC_SHA") != 0 {
		t.Fatalf("Invalid tls cipher")
	}
	if strings.Compare(tlsCipher(0x002f), "TLS_RSA_WITH_AES_128_CBC_SHA") != 0 {
		t.Fatalf("Invalid tls cipher")
	}
	if strings.Compare(tlsCipher(0x0035), "TLS_RSA_WITH_AES_256_CBC_SHA") != 0 {
		t.Fatalf("Invalid tls cipher")
	}
	if strings.Compare(tlsCipher(0xc007), "TLS_ECDHE_ECDSA_WITH_RC4_128_SHA") != 0 {
		t.Fatalf("Invalid tls cipher")
	}
	if strings.Compare(tlsCipher(0xc009), "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA") != 0 {
		t.Fatalf("Invalid tls cipher")
	}
	if strings.Compare(tlsCipher(0xc00a), "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA") != 0 {
		t.Fatalf("Invalid tls cipher")
	}
	if strings.Compare(tlsCipher(0xc011), "TLS_ECDHE_RSA_WITH_RC4_128_SHA") != 0 {
		t.Fatalf("Invalid tls cipher")
	}
	if strings.Compare(tlsCipher(0xc012), "TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA") != 0 {
		t.Fatalf("Invalid tls cipher")
	}
	if strings.Compare(tlsCipher(0xc013), "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA") != 0 {
		t.Fatalf("Invalid tls cipher")
	}
	if strings.Compare(tlsCipher(0xc014), "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA") != 0 {
		t.Fatalf("IUnknownnvalid tls cipher")
	}
	if strings.Compare(tlsCipher(0xc02f), "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256") != 0 {
		t.Fatalf("Invalid tls cipher")
	}
	if strings.Compare(tlsCipher(0xc02b), "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256") != 0 {
		t.Fatalf("Invalid tls cipher")
	}
	if strings.Compare(tlsCipher(0xc030), "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384") != 0 {
		t.Fatalf("Invalid tls cipher")
	}
	if strings.Compare(tlsCipher(0xc02c), "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384") != 0 {
		t.Fatalf("Invalid tls cipher")
	}
	if !strings.Contains(tlsCipher(0x9999), "Unknown") {
		t.Fatalf("Expected an unknown cipher.")
	}
}

func TestGetConnectURLs(t *testing.T) {
	opts := DefaultOptions()
	opts.Port = 4222

	var globalIP net.IP

	checkGlobalConnectURLs := func() {
		s := New(opts)
		defer s.Shutdown()

		urls := s.getClientConnectURLs()
		if len(urls) == 0 {
			t.Fatalf("Expected to get a list of urls, got none for listen addr: %v", opts.Host)
		}
		for _, u := range urls {
			tcpaddr, err := net.ResolveTCPAddr("tcp", u)
			if err != nil {
				t.Fatalf("Error resolving: %v", err)
			}
			ip := tcpaddr.IP
			if !ip.IsGlobalUnicast() {
				t.Fatalf("IP %v is not global", ip.String())
			}
			if ip.IsUnspecified() {
				t.Fatalf("IP %v is unspecified", ip.String())
			}
			addr := strings.TrimSuffix(u, ":4222")
			if addr == opts.Host {
				t.Fatalf("Returned url is not right: %v", u)
			}
			if globalIP == nil {
				globalIP = ip
			}
		}
	}

	listenAddrs := []string{"0.0.0.0", "::"}
	for _, listenAddr := range listenAddrs {
		opts.Host = listenAddr
		checkGlobalConnectURLs()
	}

	checkConnectURLsHasOnlyOne := func() {
		s := New(opts)
		defer s.Shutdown()

		urls := s.getClientConnectURLs()
		if len(urls) != 1 {
			t.Fatalf("Expected one URL, got %v", urls)
		}
		tcpaddr, err := net.ResolveTCPAddr("tcp", urls[0])
		if err != nil {
			t.Fatalf("Error resolving: %v", err)
		}
		ip := tcpaddr.IP
		if ip.String() != opts.Host {
			t.Fatalf("Expected connect URL to be %v, got %v", opts.Host, ip.String())
		}
	}

	singleConnectReturned := []string{"127.0.0.1", "::1"}
	if globalIP != nil {
		singleConnectReturned = append(singleConnectReturned, globalIP.String())
	}
	for _, listenAddr := range singleConnectReturned {
		opts.Host = listenAddr
		checkConnectURLsHasOnlyOne()
	}
}

func TestNoDeadlockOnStartFailure(t *testing.T) {
	opts := DefaultOptions()
	opts.Host = "x.x.x.x" // bad host
	opts.Port = 4222
	opts.HTTPHost = opts.Host
	opts.Cluster.Host = "localhost"
	opts.Cluster.Port = -1
	opts.ProfPort = -1
	s := New(opts)

	// This should return since it should fail to start a listener
	// on x.x.x.x:4222
	s.Start()

	// We should be able to shutdown
	s.Shutdown()
}

func TestMaxConnections(t *testing.T) {
	opts := DefaultOptions()
	opts.MaxConn = 1
	s := RunServer(opts)
	defer s.Shutdown()

	addr := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	nc, err := nats.Connect(addr)
	if err != nil {
		t.Fatalf("Error creating client: %v\n", err)
	}
	defer nc.Close()

	nc2, err := nats.Connect(addr)
	if err == nil {
		nc2.Close()
		t.Fatal("Expected connection to fail")
	}
}

func TestProcessCommandLineArgs(t *testing.T) {
	var host string
	var port int
	cmd := flag.NewFlagSet("gnatsd", flag.ExitOnError)
	cmd.StringVar(&host, "a", "0.0.0.0", "Host.")
	cmd.IntVar(&port, "p", 4222, "Port.")

	cmd.Parse([]string{"-a", "127.0.0.1", "-p", "9090"})
	showVersion, showHelp, err := ProcessCommandLineArgs(cmd)
	if err != nil {
		t.Errorf("Expected no errors, got: %s", err)
	}
	if showVersion || showHelp {
		t.Errorf("Expected not having to handle subcommands")
	}

	cmd.Parse([]string{"version"})
	showVersion, showHelp, err = ProcessCommandLineArgs(cmd)
	if err != nil {
		t.Errorf("Expected no errors, got: %s", err)
	}
	if !showVersion {
		t.Errorf("Expected having to handle version command")
	}
	if showHelp {
		t.Errorf("Expected not having to handle help command")
	}

	cmd.Parse([]string{"help"})
	showVersion, showHelp, err = ProcessCommandLineArgs(cmd)
	if err != nil {
		t.Errorf("Expected no errors, got: %s", err)
	}
	if showVersion {
		t.Errorf("Expected not having to handle version command")
	}
	if !showHelp {
		t.Errorf("Expected having to handle help command")
	}

	cmd.Parse([]string{"foo", "-p", "9090"})
	_, _, err = ProcessCommandLineArgs(cmd)
	if err == nil {
		t.Errorf("Expected an error handling the command arguments")
	}
}

func TestWriteDeadline(t *testing.T) {
	opts := DefaultOptions()
	opts.WriteDeadline = 20 * time.Millisecond
	s := RunServer(opts)
	defer s.Shutdown()

	c, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", opts.Host, opts.Port), 3*time.Second)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer c.Close()
	if _, err := c.Write([]byte("CONNECT {}\r\nPING\r\nSUB foo 1\r\n")); err != nil {
		t.Fatalf("Error sending protocols to server: %v", err)
	}
	// Reduce socket buffer to increase reliability of getting
	// write deadline errors.
	c.(*net.TCPConn).SetReadBuffer(10)

	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	sender, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer sender.Close()

	payload := make([]byte, 1000000)
	start := time.Now()
	for i := 0; i < 10; i++ {
		if err := sender.Publish("foo", payload); err != nil {
			t.Fatalf("Error on publish: %v", err)
		}
	}
	dur := time.Since(start)
	// user more than the write deadline to account for calls
	// overhead, running with -race, etc...
	if dur > 110*time.Millisecond {
		t.Fatalf("Flush should have returned sooner, took: %v", dur)
	}
	// Flush sender connection to ensure that all data has been sent.
	sender.Flush()
	// At this point server should have closed connection c.

	// On certain platforms, it may take more than one call before
	// getting the error.
	for i := 0; i < 100; i++ {
		if _, err := c.Write([]byte("PUB bar 5\r\nhello\r\n")); err != nil {
			// ok
			return
		}
	}
	t.Fatal("Connection should have been closed")
}

func TestRandomPorts(t *testing.T) {
	opts := DefaultOptions()
	opts.HTTPPort = -1
	opts.Port = -1
	s := RunServer(opts)

	defer s.Shutdown()

	if s.Addr() == nil || s.Addr().(*net.TCPAddr).Port <= 0 {
		t.Fatal("Should have dynamically assigned server port.")
	}

	if s.Addr() == nil || s.Addr().(*net.TCPAddr).Port == 4222 {
		t.Fatal("Should not have dynamically assigned default port: 4222.")
	}

	if s.MonitorAddr() == nil || s.MonitorAddr().Port <= 0 {
		t.Fatal("Should have dynamically assigned monitoring port.")
	}

}

func TestNilMonitoringPort(t *testing.T) {
	opts := DefaultOptions()
	opts.HTTPPort = 0
	opts.HTTPSPort = 0
	s := RunServer(opts)

	defer s.Shutdown()

	if s.MonitorAddr() != nil {
		t.Fatal("HttpAddr should be nil.")
	}
}
