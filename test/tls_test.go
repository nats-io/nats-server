// Copyright 2015-2019 The NATS Authors
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
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
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

func TestTLSClientCertificateHasUserID(t *testing.T) {
	srv, opts := RunServerWithConfig("./configs/tls_cert_id.conf")
	defer srv.Shutdown()
	nurl := fmt.Sprintf("tls://%s:%d", opts.Host, opts.Port)
	nc, err := nats.Connect(nurl,
		nats.ClientCert("./configs/certs/client-id-auth-cert.pem", "./configs/certs/client-id-auth-key.pem"),
		nats.RootCAs("./configs/certs/ca.pem"))
	if err != nil {
		t.Fatalf("Expected to connect, got %v", err)
	}
	defer nc.Close()
}

func TestTLSClientCertificateCNBasedAuth(t *testing.T) {
	srv, opts := RunServerWithConfig("./configs/tls_cert_cn.conf")
	defer srv.Shutdown()
	nurl := fmt.Sprintf("tls://%s:%d", opts.Host, opts.Port)
	errCh1 := make(chan error)
	errCh2 := make(chan error)

	// Using the default permissions
	nc1, err := nats.Connect(nurl,
		nats.ClientCert("./configs/certs/tlsauth/client.pem", "./configs/certs/tlsauth/client-key.pem"),
		nats.RootCAs("./configs/certs/tlsauth/ca.pem"),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			errCh1 <- err
		}),
	)
	if err != nil {
		t.Fatalf("Expected to connect, got %v", err)
	}
	defer nc1.Close()

	// Admin permissions can publish to '>'
	nc2, err := nats.Connect(nurl,
		nats.ClientCert("./configs/certs/tlsauth/client2.pem", "./configs/certs/tlsauth/client2-key.pem"),
		nats.RootCAs("./configs/certs/tlsauth/ca.pem"),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			errCh2 <- err
		}),
	)
	if err != nil {
		t.Fatalf("Expected to connect, got %v", err)
	}
	defer nc2.Close()

	err = nc1.Publish("foo.bar", []byte("hi"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = nc1.SubscribeSync("foo.>")
	if err != nil {
		t.Fatal(err)
	}
	nc1.Flush()

	sub, err := nc2.SubscribeSync(">")
	if err != nil {
		t.Fatal(err)
	}
	nc2.Flush()
	err = nc2.Publish("hello", []byte("hi"))
	if err != nil {
		t.Fatal(err)
	}
	nc2.Flush()

	_, err = sub.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatalf("Error during wait for next message: %s", err)
	}

	// Wait for a couple of errors
	var count int
Loop:
	for {
		select {
		case err := <-errCh1:
			if err != nil {
				count++
			}
			if count == 2 {
				break Loop
			}
		case err := <-errCh2:
			if err != nil {
				t.Fatalf("Received unexpected auth error from client: %s", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("Timed out expecting auth errors")
		}
	}
}

func TestTLSClientCertificateSANsBasedAuth(t *testing.T) {
	// In this test we have 3 clients, one with permissions defined
	// for SAN 'app.nats.dev', other for SAN 'app.nats.prod' and another
	// one without the default permissions for the CN.
	srv, opts := RunServerWithConfig("./configs/tls_cert_san_auth.conf")
	defer srv.Shutdown()
	nurl := fmt.Sprintf("tls://%s:%d", opts.Host, opts.Port)
	defaultErrCh := make(chan error)
	devErrCh := make(chan error)
	prodErrCh := make(chan error)

	// default: Using the default permissions (no SANs)
	//
	//    Subject: CN = www.nats.io
	//
	defaultc, err := nats.Connect(nurl,
		nats.ClientCert("./configs/certs/sans/client.pem", "./configs/certs/sans/client-key.pem"),
		nats.RootCAs("./configs/certs/sans/ca.pem"),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			defaultErrCh <- err
		}),
	)
	if err != nil {
		t.Fatalf("Expected to connect, got %v", err)
	}
	defer defaultc.Close()

	// dev: Using SAN 'app.nats.dev' with permissions to its own sandbox.
	//
	//    Subject: CN = www.nats.io
	//
	//    X509v3 Subject Alternative Name:
	//        DNS:app.nats.dev, DNS:*.app.nats.dev
	//
	devc, err := nats.Connect(nurl,
		nats.ClientCert("./configs/certs/sans/dev.pem", "./configs/certs/sans/dev-key.pem"),
		nats.RootCAs("./configs/certs/sans/ca.pem"),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			devErrCh <- err
		}),
	)
	if err != nil {
		t.Fatalf("Expected to connect, got %v", err)
	}
	defer devc.Close()

	// prod: Using SAN 'app.nats.prod' with all permissions.
	//
	//    Subject: CN = www.nats.io
	//
	//    X509v3 Subject Alternative Name:
	//        DNS:app.nats.prod, DNS:*.app.nats.prod
	//
	prodc, err := nats.Connect(nurl,
		nats.ClientCert("./configs/certs/sans/prod.pem", "./configs/certs/sans/prod-key.pem"),
		nats.RootCAs("./configs/certs/sans/ca.pem"),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			prodErrCh <- err
		}),
	)
	if err != nil {
		t.Fatalf("Expected to connect, got %v", err)
	}
	defer prodc.Close()

	// No permissions to publish or subscribe on foo.>
	err = devc.Publish("foo.bar", []byte("hi"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = devc.SubscribeSync("foo.>")
	if err != nil {
		t.Fatal(err)
	}
	devc.Flush()

	prodSub, err := prodc.SubscribeSync(">")
	if err != nil {
		t.Fatal(err)
	}
	prodc.Flush()
	err = prodc.Publish("hello", []byte("hi"))
	if err != nil {
		t.Fatal(err)
	}
	prodc.Flush()

	// prod: can receive message on wildcard subscription.
	_, err = prodSub.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatalf("Error during wait for next message: %s", err)
	}

	// dev: enough permissions to publish to sandbox subject.
	devSub, err := devc.SubscribeSync("sandbox.>")
	if err != nil {
		t.Fatal(err)
	}
	devc.Flush()
	err = devc.Publish("sandbox.foo.bar", []byte("hi!"))
	if err != nil {
		t.Fatal(err)
	}
	// both dev and prod clients can receive message
	_, err = devSub.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatalf("Error during wait for next message: %s", err)
	}
	_, err = prodSub.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatalf("Error during wait for next message: %s", err)
	}

	// default: no enough permissions
	_, err = defaultc.SubscribeSync("sandbox.>")
	if err != nil {
		t.Fatal(err)
	}
	defaultSub, err := defaultc.SubscribeSync("public.>")
	if err != nil {
		t.Fatal(err)
	}
	defaultc.Flush()
	err = devc.Publish("public.foo.bar", []byte("hi!"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = defaultSub.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatalf("Error during wait for next message: %s", err)
	}

	// Wait for a couple of errors
	var count int
Loop:
	for {
		select {
		case err := <-defaultErrCh:
			if err != nil {
				count++
			}
			if count == 3 {
				break Loop
			}
		case err := <-devErrCh:
			if err != nil {
				count++
			}
			if count == 3 {
				break Loop
			}
		case err := <-prodErrCh:
			if err != nil {
				t.Fatalf("Received unexpected auth error from client: %s", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("Timed out expecting auth errors")
		}
	}
}

func TestTLSClientCertificateTLSAuthMultipleOptions(t *testing.T) {
	srv, opts := RunServerWithConfig("./configs/tls_cert_san_emails.conf")
	defer srv.Shutdown()
	nurl := fmt.Sprintf("tls://%s:%d", opts.Host, opts.Port)
	defaultErrCh := make(chan error)
	devErrCh := make(chan error)
	prodErrCh := make(chan error)

	// default: Using the default permissions, there are SANs
	// present in the cert but they are not users in the NATS config
	// so the subject is used instead.
	//
	//    Subject: CN = www.nats.io
	//
	//    X509v3 Subject Alternative Name:
	//        DNS:app.nats.dev, DNS:*.app.nats.dev
	//
	defaultc, err := nats.Connect(nurl,
		nats.ClientCert("./configs/certs/sans/dev.pem", "./configs/certs/sans/dev-key.pem"),
		nats.RootCAs("./configs/certs/sans/ca.pem"),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			defaultErrCh <- err
		}),
	)
	if err != nil {
		t.Fatalf("Expected to connect, got %v", err)
	}
	defer defaultc.Close()

	// dev: Using SAN to validate user, even if emails are present in the config.
	//
	//    Subject: CN = www.nats.io
	//
	//    X509v3 Subject Alternative Name:
	//        DNS:app.nats.dev, email:admin@app.nats.dev, email:root@app.nats.dev
	//
	devc, err := nats.Connect(nurl,
		nats.ClientCert("./configs/certs/sans/dev-email.pem", "./configs/certs/sans/dev-email-key.pem"),
		nats.RootCAs("./configs/certs/sans/ca.pem"),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			devErrCh <- err
		}),
	)
	if err != nil {
		t.Fatalf("Expected to connect, got %v", err)
	}
	defer devc.Close()

	// prod: Using SAN '*.app.nats.prod' with all permissions, which is not the first SAN.
	//
	//    Subject: CN = www.nats.io
	//
	//    X509v3 Subject Alternative Name:
	//        DNS:app.nats.prod, DNS:*.app.nats.prod
	//
	prodc, err := nats.Connect(nurl,
		nats.ClientCert("./configs/certs/sans/prod.pem", "./configs/certs/sans/prod-key.pem"),
		nats.RootCAs("./configs/certs/sans/ca.pem"),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			prodErrCh <- err
		}),
	)
	if err != nil {
		t.Fatalf("Expected to connect, got %v", err)
	}
	defer prodc.Close()

	// No permissions to publish or subscribe on foo.>
	err = devc.Publish("foo.bar", []byte("hi"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = devc.SubscribeSync("foo.>")
	if err != nil {
		t.Fatal(err)
	}
	devc.Flush()

	prodSub, err := prodc.SubscribeSync(">")
	if err != nil {
		t.Fatal(err)
	}
	prodc.Flush()
	err = prodc.Publish("hello", []byte("hi"))
	if err != nil {
		t.Fatal(err)
	}
	prodc.Flush()

	// prod: can receive message on wildcard subscription.
	_, err = prodSub.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatalf("Error during wait for next message: %s", err)
	}

	// dev: enough permissions to publish to sandbox subject.
	devSub, err := devc.SubscribeSync("sandbox.>")
	if err != nil {
		t.Fatal(err)
	}
	devc.Flush()
	err = devc.Publish("sandbox.foo.bar", []byte("hi!"))
	if err != nil {
		t.Fatal(err)
	}
	// both dev and prod clients can receive message
	_, err = devSub.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatalf("Error during wait for next message: %s", err)
	}
	_, err = prodSub.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatalf("Error during wait for next message: %s", err)
	}

	// default: no enough permissions
	_, err = defaultc.SubscribeSync("sandbox.>")
	if err != nil {
		t.Fatal(err)
	}
	defaultSub, err := defaultc.SubscribeSync("public.>")
	if err != nil {
		t.Fatal(err)
	}
	defaultc.Flush()
	err = devc.Publish("public.foo.bar", []byte("hi!"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = defaultSub.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatalf("Error during wait for next message: %s", err)
	}

	// Wait for a couple of errors
	var count int
Loop:
	for {
		select {
		case err := <-defaultErrCh:
			if err != nil {
				count++
			}
			if count == 3 {
				break Loop
			}
		case err := <-devErrCh:
			if err != nil {
				count++
			}
			if count == 3 {
				break Loop
			}
		case err := <-prodErrCh:
			if err != nil {
				t.Fatalf("Received unexpected auth error from client: %s", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("Timed out expecting auth errors")
		}
	}
}

func TestTLSRoutesCertificateCNBasedAuth(t *testing.T) {
	// Base config for the servers
	optsA := LoadConfig("./configs/tls_cert_cn_routes.conf")
	optsB := LoadConfig("./configs/tls_cert_cn_routes.conf")
	optsC := LoadConfig("./configs/tls_cert_cn_routes_invalid_auth.conf")

	// TLS map should have been enabled
	if !optsA.Cluster.TLSMap || !optsB.Cluster.TLSMap || !optsC.Cluster.TLSMap {
		t.Error("Expected Cluster TLS verify and map feature to be activated")
	}

	routeURLs := "nats://localhost:9935, nats://localhost:9936, nats://localhost:9937"

	optsA.Host = "127.0.0.1"
	optsA.Port = 9335
	optsA.Cluster.Host = optsA.Host
	optsA.Cluster.Port = 9935
	optsA.Routes = server.RoutesFromStr(routeURLs)
	srvA := RunServer(optsA)
	defer srvA.Shutdown()

	optsB.Host = "127.0.0.1"
	optsB.Port = 9336
	optsB.Cluster.Host = optsB.Host
	optsB.Cluster.Port = 9936
	optsB.Routes = server.RoutesFromStr(routeURLs)
	srvB := RunServer(optsB)
	defer srvB.Shutdown()

	optsC.Host = "127.0.0.1"
	optsC.Port = 9337
	optsC.Cluster.Host = optsC.Host
	optsC.Cluster.Port = 9937
	optsC.Routes = server.RoutesFromStr(routeURLs)
	srvC := RunServer(optsC)
	defer srvC.Shutdown()

	// srvC is not connected to srvA and srvB due to wrong cert
	checkClusterFormed(t, srvA, srvB)

	nc1, err := nats.Connect(fmt.Sprintf("%s:%d", optsA.Host, optsA.Port), nats.Name("A"))
	if err != nil {
		t.Fatalf("Expected to connect, got %v", err)
	}
	defer nc1.Close()

	nc2, err := nats.Connect(fmt.Sprintf("%s:%d", optsB.Host, optsB.Port), nats.Name("B"))
	if err != nil {
		t.Fatalf("Expected to connect, got %v", err)
	}
	defer nc2.Close()

	// This client is partitioned from rest of cluster due to using wrong cert.
	nc3, err := nats.Connect(fmt.Sprintf("%s:%d", optsC.Host, optsC.Port), nats.Name("C"))
	if err != nil {
		t.Fatalf("Expected to connect, got %v", err)
	}
	defer nc3.Close()

	// Allowed via permissions to broadcast to other members of cluster.
	publicSub, err := nc1.SubscribeSync("public.>")
	if err != nil {
		t.Fatal(err)
	}

	// Not part of cluster permissions so message is not forwarded.
	privateSub, err := nc1.SubscribeSync("private.>")
	if err != nil {
		t.Fatal(err)
	}
	// This ensures that srvA has received the sub, not srvB
	nc1.Flush()

	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		if n := srvB.NumSubscriptions(); n != 1 {
			return fmt.Errorf("Expected 1 sub, got %v", n)
		}
		return nil
	})

	// Not forwarded by cluster so won't be received.
	err = nc2.Publish("private.foo", []byte("private message on server B"))
	if err != nil {
		t.Fatal(err)
	}
	err = nc2.Publish("public.foo", []byte("public message from server B to server A"))
	if err != nil {
		t.Fatal(err)
	}
	nc2.Flush()
	err = nc3.Publish("public.foo", []byte("dropped message since unauthorized server"))
	if err != nil {
		t.Fatal(err)
	}
	nc3.Flush()

	// Message will be received since allowed via permissions
	_, err = publicSub.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatalf("Error during wait for next message: %s", err)
	}
	msg, err := privateSub.NextMsg(500 * time.Millisecond)
	if err == nil {
		t.Errorf("Received unexpected message from private sub: %+v", msg)
	}
	msg, err = publicSub.NextMsg(500 * time.Millisecond)
	if err == nil {
		t.Errorf("Received unexpected message from private sub: %+v", msg)
	}
}

func TestTLSGatewaysCertificateCNBasedAuth(t *testing.T) {
	// Base config for the servers
	optsA := LoadConfig("./configs/tls_cert_cn_gateways.conf")
	optsB := LoadConfig("./configs/tls_cert_cn_gateways.conf")
	optsC := LoadConfig("./configs/tls_cert_cn_gateways_invalid_auth.conf")

	// TLS map should have been enabled
	if !optsA.Gateway.TLSMap || !optsB.Gateway.TLSMap || !optsC.Gateway.TLSMap {
		t.Error("Expected Cluster TLS verify and map feature to be activated")
	}

	gwA, err := url.Parse("nats://localhost:9995")
	if err != nil {
		t.Fatal(err)
	}
	gwB, err := url.Parse("nats://localhost:9996")
	if err != nil {
		t.Fatal(err)
	}
	gwC, err := url.Parse("nats://localhost:9997")
	if err != nil {
		t.Fatal(err)
	}

	optsA.Host = "127.0.0.1"
	optsA.Port = -1
	optsA.Gateway.Name = "A"
	optsA.Gateway.Port = 9995

	optsB.Host = "127.0.0.1"
	optsB.Port = -1
	optsB.Gateway.Name = "B"
	optsB.Gateway.Port = 9996

	optsC.Host = "127.0.0.1"
	optsC.Port = -1
	optsC.Gateway.Name = "C"
	optsC.Gateway.Port = 9997

	gateways := make([]*server.RemoteGatewayOpts, 3)
	gateways[0] = &server.RemoteGatewayOpts{
		Name: optsA.Gateway.Name,
		URLs: []*url.URL{gwA},
	}
	gateways[1] = &server.RemoteGatewayOpts{
		Name: optsB.Gateway.Name,
		URLs: []*url.URL{gwB},
	}
	gateways[2] = &server.RemoteGatewayOpts{
		Name: optsC.Gateway.Name,
		URLs: []*url.URL{gwC},
	}
	optsA.Gateway.Gateways = gateways
	optsB.Gateway.Gateways = gateways
	optsC.Gateway.Gateways = gateways

	server.SetGatewaysSolicitDelay(100 * time.Millisecond)
	defer server.ResetGatewaysSolicitDelay()

	srvA := RunServer(optsA)
	defer srvA.Shutdown()

	srvB := RunServer(optsB)
	defer srvB.Shutdown()

	srvC := RunServer(optsC)
	defer srvC.Shutdown()

	waitForOutboundGateways(t, srvA, 1, 2*time.Second)
	waitForOutboundGateways(t, srvB, 1, 2*time.Second)

	nc1, err := nats.Connect(srvA.Addr().String(), nats.Name("A"))
	if err != nil {
		t.Fatalf("Expected to connect, got %v", err)
	}
	defer nc1.Close()

	nc2, err := nats.Connect(srvB.Addr().String(), nats.Name("B"))
	if err != nil {
		t.Fatalf("Expected to connect, got %v", err)
	}
	defer nc2.Close()

	nc3, err := nats.Connect(srvC.Addr().String(), nats.Name("C"))
	if err != nil {
		t.Fatalf("Expected to connect, got %v", err)
	}
	defer nc3.Close()

	received := make(chan *nats.Msg)
	_, err = nc1.Subscribe("foo", func(msg *nats.Msg) {
		received <- msg
	})
	if err != nil {
		t.Error(err)
	}
	_, err = nc3.Subscribe("help", func(msg *nats.Msg) {
		nc3.Publish(msg.Reply, []byte("response"))
	})
	if err != nil {
		t.Error(err)
	}

	go func() {
		for range time.NewTicker(10 * time.Millisecond).C {
			if nc2.IsClosed() {
				return
			}
			nc2.Publish("foo", []byte("bar"))
		}
	}()

	select {
	case <-received:
	case <-time.After(2 * time.Second):
		t.Error("Timed out waiting for gateway messages")
	}

	msg, err := nc2.Request("help", []byte("should fail"), 100*time.Millisecond)
	if err == nil {
		t.Errorf("Expected to not receive any messages, got: %+v", msg)
	}
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
	if strings.ToLower(err.Error()) != nats.ErrAuthorization.Error() {
		t.Fatalf("Expected and auth violation, got %v\n", err)
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

type captureSlowConsumerLogger struct {
	dummyLogger
	ch    chan string
	gotIt bool
}

func (l *captureSlowConsumerLogger) Noticef(format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	if strings.Contains(msg, "Slow Consumer") {
		l.Lock()
		if !l.gotIt {
			l.gotIt = true
			l.ch <- msg
		}
		l.Unlock()
	}
}

func TestTLSTimeoutNotReportSlowConsumer(t *testing.T) {
	oa, err := server.ProcessConfigFile("./configs/srv_a_tls.conf")
	if err != nil {
		t.Fatalf("Unable to load config file: %v", err)
	}

	// Override TLSTimeout to very small value so that handshake fails
	oa.Cluster.TLSTimeout = 0.0000001
	sa := RunServer(oa)
	defer sa.Shutdown()

	ch := make(chan string, 1)
	cscl := &captureSlowConsumerLogger{ch: ch}
	sa.SetLogger(cscl, false, false)

	ob, err := server.ProcessConfigFile("./configs/srv_b_tls.conf")
	if err != nil {
		t.Fatalf("Unable to load config file: %v", err)
	}

	sb := RunServer(ob)
	defer sb.Shutdown()

	// Watch the logger for a bit and make sure we don't get any
	// Slow Consumer error.
	select {
	case e := <-ch:
		t.Fatalf("Unexpected slow consumer error: %s", e)
	case <-time.After(500 * time.Millisecond):
		// ok
	}
}

func TestNotReportSlowConsumerUnlessConnected(t *testing.T) {
	oa, err := server.ProcessConfigFile("./configs/srv_a_tls.conf")
	if err != nil {
		t.Fatalf("Unable to load config file: %v", err)
	}

	// Override WriteDeadline to very small value so that handshake
	// fails with a slow consumer error.
	oa.WriteDeadline = 1 * time.Nanosecond
	sa := RunServer(oa)
	defer sa.Shutdown()

	ch := make(chan string, 1)
	cscl := &captureSlowConsumerLogger{ch: ch}
	sa.SetLogger(cscl, false, false)

	nc := createClientConn(t, oa.Host, oa.Port)
	defer nc.Close()

	// Make sure we don't get any Slow Consumer error.
	select {
	case e := <-ch:
		t.Fatalf("Unexpected slow consumer error: %s", e)
	case <-time.After(500 * time.Millisecond):
		// ok
	}
}

func TestTLSHandshakeFailureMemUsage(t *testing.T) {
	for i, test := range []struct {
		name   string
		config string
	}{
		{
			"connect to TLS client port",
			`
				port: -1
				%s
			`,
		},
		{
			"connect to TLS route port",
			`
				port: -1
				cluster {
					port: -1
					%s
				}
			`,
		},
		{
			"connect to TLS gateway port",
			`
				port: -1
				gateway {
					name: "A"
					port: -1
					%s
				}
			`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			content := fmt.Sprintf(test.config, `
				tls {
					cert_file: "configs/certs/server-cert.pem"
					key_file: "configs/certs/server-key.pem"
					ca_file: "configs/certs/ca.pem"
					timeout: 5
				}
			`)
			conf := createConfFile(t, []byte(content))
			defer os.Remove(conf)
			s, opts := RunServerWithConfig(conf)
			defer s.Shutdown()

			var port int
			switch i {
			case 0:
				port = opts.Port
			case 1:
				port = s.ClusterAddr().Port
			case 2:
				port = s.GatewayAddr().Port
			}

			varz, _ := s.Varz(nil)
			base := varz.Mem
			buf := make([]byte, 1024*1024)
			for i := 0; i < 100; i++ {
				conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", port))
				if err != nil {
					t.Fatalf("Error on dial: %v", err)
				}
				conn.Write(buf)
				conn.Close()
			}

			varz, _ = s.Varz(nil)
			newval := (varz.Mem - base) / (1024 * 1024)
			// May need to adjust that, but right now, with 100 clients
			// we are at about 20MB for client port, 11MB for route
			// and 6MB for gateways, so pick 50MB as the threshold
			if newval >= 50 {
				t.Fatalf("Consumed too much memory: %v MB", newval)
			}
		})
	}
}
