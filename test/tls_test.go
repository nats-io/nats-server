// Copyright 2015 Apcera Inc. All rights reserved.

package test

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/nats-io/nats"
)

func TestTLSConnection(t *testing.T) {
	srv, opts := RunServerWithConfig("./configs/tls.conf")
	defer srv.Shutdown()

	endpoint := fmt.Sprintf("%s:%d", opts.Host, opts.Port)
	nurl := fmt.Sprintf("nats://%s:%s@%s/", opts.Username, opts.Password, endpoint)
	nc, err := nats.Connect(nurl)
	if err == nil {
		t.Fatalf("Expected error trying to connect to secure server")
	}

	// Do simple SecureConnect
	nc, err = nats.SecureConnect(fmt.Sprintf("nats://%s/", endpoint))
	if err == nil {
		t.Fatalf("Expected error trying to connect to secure server with no auth")
	}

	nc, err = nats.SecureConnect(nurl)
	if err != nil {
		t.Fatalf("Got an error on SecureConnect: %+v\n", err)
	}
	subj := "foo-tls"
	sub, _ := nc.SubscribeSync(subj)

	nc.Publish(subj, []byte("We are Secure!"))
	nc.Flush()
	nmsgs, _ := sub.QueuedMsgs()
	if nmsgs != 1 {
		t.Fatalf("Expected to receive a message over the TLS connection")
	}
	defer nc.Close()

	// Now do more advanced checking, verifying servername and using rootCA.
	// Setup our own TLSConfig using RootCA from our self signed cert.
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
		ServerName: opts.Host,
		RootCAs:    pool,
		MinVersion: tls.VersionTLS12,
	}

	copts := nats.DefaultOptions
	copts.Url = nurl
	copts.Secure = true
	copts.TLSConfig = config

	nc, err = copts.Connect()
	if err != nil {
		t.Fatalf("Got an error on Connect with Secure Options: %+v\n", err)
	}
	nc.Flush()
	defer nc.Close()
}
