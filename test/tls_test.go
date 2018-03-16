// Copyright 2015-2018 The NATS Authors
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

package test

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/gnatsd/server"
	"github.com/nats-io/go-nats"
)

func TestTLSConnection(t *testing.T) {
	srv, opts := RunServerWithConfig("./configs/tls.conf")
	defer srv.Shutdown()

	endpoint := fmt.Sprintf("%s:%d", opts.Host, opts.Port)
	nurl := fmt.Sprintf("tls://%s:%s@%s/", opts.Username, opts.Password, endpoint)
	nc, err := nats.Connect(nurl)
	if err == nil {
		nc.Close()
		t.Fatalf("Expected error trying to connect to secure server")
	}

	// Do simple SecureConnect
	nc, err = nats.Connect(fmt.Sprintf("tls://%s/", endpoint))
	if err == nil {
		nc.Close()
		t.Fatalf("Expected error trying to connect to secure server with no auth")
	}

	// Now do more advanced checking, verifying servername and using rootCA.

	nc, err = nats.Connect(nurl, nats.RootCAs("./configs/certs/ca.pem"))
	if err != nil {
		t.Fatalf("Got an error on Connect with Secure Options: %+v\n", err)
	}
	defer nc.Close()

	subj := "foo-tls"
	sub, _ := nc.SubscribeSync(subj)

	nc.Publish(subj, []byte("We are Secure!"))
	nc.Flush()
	nmsgs, _ := sub.QueuedMsgs()
	if nmsgs != 1 {
		t.Fatalf("Expected to receive a message over the TLS connection")
	}
}

func TestTLSClientCertificate(t *testing.T) {
	srv, opts := RunServerWithConfig("./configs/tlsverify.conf")
	defer srv.Shutdown()

	nurl := fmt.Sprintf("tls://%s:%d", opts.Host, opts.Port)

	_, err := nats.Connect(nurl)
	if err == nil {
		t.Fatalf("Expected error trying to connect to secure server without a certificate")
	}

	// Load client certificate to successfully connect.
	certFile := "./configs/certs/client-cert.pem"
	keyFile := "./configs/certs/client-key.pem"
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatalf("error parsing X509 certificate/key pair: %v", err)
	}

	// Load in root CA for server verification
	rootPEM, err := ioutil.ReadFile("./configs/certs/ca.pem")
	if err != nil || rootPEM == nil {
		t.Fatalf("failed to read root certificate")
	}
	pool := x509.NewCertPool()
	ok := pool.AppendCertsFromPEM([]byte(rootPEM))
	if !ok {
		t.Fatalf("failed to parse root certificate")
	}

	config := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ServerName:   opts.Host,
		RootCAs:      pool,
		MinVersion:   tls.VersionTLS12,
	}

	copts := nats.GetDefaultOptions()
	copts.Url = nurl
	copts.Secure = true
	copts.TLSConfig = config

	nc, err := copts.Connect()
	if err != nil {
		t.Fatalf("Got an error on Connect with Secure Options: %+v\n", err)
	}
	nc.Flush()
	defer nc.Close()
}

func TestTLSVerifyClientCertificate(t *testing.T) {
	srv, opts := RunServerWithConfig("./configs/tlsverify_noca.conf")
	defer srv.Shutdown()

	nurl := fmt.Sprintf("tls://%s:%d", opts.Host, opts.Port)

	// The client is configured properly, but the server has no CA
	// to verify the client certificate. Connection should fail.
	nc, err := nats.Connect(nurl,
		nats.ClientCert("./configs/certs/client-cert.pem", "./configs/certs/client-key.pem"),
		nats.RootCAs("./configs/certs/ca.pem"))
	if err == nil {
		nc.Close()
		t.Fatal("Expected failure to connect, did not")
	}
}

func TestTLSConnectionTimeout(t *testing.T) {
	opts := LoadConfig("./configs/tls.conf")
	opts.TLSTimeout = 0.25

	srv := RunServer(opts)
	defer srv.Shutdown()

	// Dial with normal TCP
	endpoint := fmt.Sprintf("%s:%d", opts.Host, opts.Port)
	conn, err := net.Dial("tcp", endpoint)
	if err != nil {
		t.Fatalf("Could not connect to %q", endpoint)
	}
	defer conn.Close()

	// Read deadlines
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))

	// Read the INFO string.
	br := bufio.NewReader(conn)
	info, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("Failed to read INFO - %v", err)
	}
	if !strings.HasPrefix(info, "INFO ") {
		t.Fatalf("INFO response incorrect: %s\n", info)
	}
	wait := time.Duration(opts.TLSTimeout * float64(time.Second))
	time.Sleep(wait)
	// Read deadlines
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	tlsErr, err := br.ReadString('\n')
	if err == nil && !strings.Contains(tlsErr, "-ERR 'Secure Connection - TLS Required") {
		t.Fatalf("TLS Timeout response incorrect: %q\n", tlsErr)
	}
}

// Ensure there is no race between authorization timeout and TLS handshake.
func TestTLSAuthorizationShortTimeout(t *testing.T) {
	opts := LoadConfig("./configs/tls.conf")
	opts.AuthTimeout = 0.001

	srv := RunServer(opts)
	defer srv.Shutdown()

	endpoint := fmt.Sprintf("%s:%d", opts.Host, opts.Port)
	nurl := fmt.Sprintf("tls://%s:%s@%s/", opts.Username, opts.Password, endpoint)

	// Expect an error here (no CA) but not a TLS oversized record error which
	// indicates the authorization timeout fired too soon.
	_, err := nats.Connect(nurl)
	if err == nil {
		t.Fatal("Expected error trying to connect to secure server")
	}
	if strings.Contains(err.Error(), "oversized record") {
		t.Fatal("Corrupted TLS handshake:", err)
	}
}

func stressConnect(t *testing.T, wg *sync.WaitGroup, errCh chan error, url string, index int) {
	defer wg.Done()

	subName := fmt.Sprintf("foo.%d", index)

	for i := 0; i < 33; i++ {
		nc, err := nats.Connect(url, nats.RootCAs("./configs/certs/ca.pem"))
		if err != nil {
			errCh <- fmt.Errorf("Unable to create TLS connection: %v\n", err)
			return
		}
		defer nc.Close()

		sub, err := nc.SubscribeSync(subName)
		if err != nil {
			errCh <- fmt.Errorf("Unable to subscribe on '%s': %v\n", subName, err)
			return
		}

		if err := nc.Publish(subName, []byte("secure data")); err != nil {
			errCh <- fmt.Errorf("Unable to send on '%s': %v\n", subName, err)
		}

		if _, err := sub.NextMsg(2 * time.Second); err != nil {
			errCh <- fmt.Errorf("Unable to get next message: %v\n", err)
		}

		nc.Close()
	}

	errCh <- nil
}

func TestTLSStressConnect(t *testing.T) {
	opts, err := server.ProcessConfigFile("./configs/tls.conf")
	if err != nil {
		panic(fmt.Sprintf("Error processing configuration file: %v", err))
	}
	opts.NoSigs, opts.NoLog = true, true

	// For this test, remove the authorization
	opts.Username = ""
	opts.Password = ""

	// Increase ssl timeout
	opts.TLSTimeout = 2.0

	srv := RunServer(opts)
	defer srv.Shutdown()

	nurl := fmt.Sprintf("tls://%s:%d", opts.Host, opts.Port)

	threadCount := 3

	errCh := make(chan error, threadCount)

	var wg sync.WaitGroup
	wg.Add(threadCount)

	for i := 0; i < threadCount; i++ {
		go stressConnect(t, &wg, errCh, nurl, i)
	}

	wg.Wait()

	var lastError error
	for i := 0; i < threadCount; i++ {
		err := <-errCh
		if err != nil {
			lastError = err
		}
	}

	if lastError != nil {
		t.Fatalf("%v\n", lastError)
	}
}

func TestTLSBadAuthError(t *testing.T) {
	srv, opts := RunServerWithConfig("./configs/tls.conf")
	defer srv.Shutdown()

	endpoint := fmt.Sprintf("%s:%d", opts.Host, opts.Port)
	nurl := fmt.Sprintf("tls://%s:%s@%s/", opts.Username, "NOT_THE_PASSWORD", endpoint)

	_, err := nats.Connect(nurl, nats.RootCAs("./configs/certs/ca.pem"))
	if err == nil {
		t.Fatalf("Expected error trying to connect to secure server")
	}
	if err.Error() != nats.ErrAuthorization.Error() {
		t.Fatalf("Excpected and auth violation, got %v\n", err)
	}
}

func TestTLSConnectionCurvePref(t *testing.T) {
	srv, opts := RunServerWithConfig("./configs/tls_curve_pref.conf")
	defer srv.Shutdown()

	if len(opts.TLSConfig.CurvePreferences) != 1 {
		t.Fatal("Invalid curve preference loaded.")
	}

	if opts.TLSConfig.CurvePreferences[0] != tls.CurveP256 {
		t.Fatalf("Invalid curve preference loaded [%v].", opts.TLSConfig.CurvePreferences[0])
	}

	endpoint := fmt.Sprintf("%s:%d", opts.Host, opts.Port)
	nurl := fmt.Sprintf("tls://%s:%s@%s/", opts.Username, opts.Password, endpoint)
	nc, err := nats.Connect(nurl)
	if err == nil {
		nc.Close()
		t.Fatalf("Expected error trying to connect to secure server")
	}

	// Do simple SecureConnect
	nc, err = nats.Connect(fmt.Sprintf("tls://%s/", endpoint))
	if err == nil {
		nc.Close()
		t.Fatalf("Expected error trying to connect to secure server with no auth")
	}

	// Now do more advanced checking, verifying servername and using rootCA.

	nc, err = nats.Connect(nurl, nats.RootCAs("./configs/certs/ca.pem"))
	if err != nil {
		t.Fatalf("Got an error on Connect with Secure Options: %+v\n", err)
	}
	defer nc.Close()

	subj := "foo-tls"
	sub, _ := nc.SubscribeSync(subj)

	nc.Publish(subj, []byte("We are Secure!"))
	nc.Flush()
	nmsgs, _ := sub.QueuedMsgs()
	if nmsgs != 1 {
		t.Fatalf("Expected to receive a message over the TLS connection")
	}
}
