// Copyright 2015-2020 The NATS Authors
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
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

var noOpErrHandler = func(_ *nats.Conn, _ *nats.Subscription, _ error) {}

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
	optsA.Cluster.Name = "xyz"
	optsA.Cluster.Host = optsA.Host
	optsA.Cluster.Port = 9935
	optsA.Routes = server.RoutesFromStr(routeURLs)
	optsA.NoSystemAccount = true
	srvA := RunServer(optsA)
	defer srvA.Shutdown()

	optsB.Host = "127.0.0.1"
	optsB.Port = 9336
	optsB.Cluster.Name = "xyz"
	optsB.Cluster.Host = optsB.Host
	optsB.Cluster.Port = 9936
	optsB.Routes = server.RoutesFromStr(routeURLs)
	optsB.NoSystemAccount = true
	srvB := RunServer(optsB)
	defer srvB.Shutdown()

	optsC.Host = "127.0.0.1"
	optsC.Port = 9337
	optsC.Cluster.Name = "xyz"
	optsC.Cluster.Host = optsC.Host
	optsC.Cluster.Port = 9937
	optsC.Routes = server.RoutesFromStr(routeURLs)
	optsC.NoSystemAccount = true
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

	// Because we need to use "localhost" in the gw URLs (to match
	// hostname in the user/CN), the server may try to connect to
	// a [::1], etc.. that may or may not work, so give a lot of
	// time for that to complete ok.
	waitForOutboundGateways(t, srvA, 1, 5*time.Second)
	waitForOutboundGateways(t, srvB, 1, 5*time.Second)

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
		select {
		case received <- msg:
		default:
		}
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

func TestClientTLSAndNonTLSConnections(t *testing.T) {
	s, opts := RunServerWithConfig("./configs/tls_mixed.conf")
	defer s.Shutdown()

	surl := fmt.Sprintf("tls://%s:%d", opts.Host, opts.Port)
	nc, err := nats.Connect(surl, nats.RootCAs("./configs/certs/ca.pem"))
	if err != nil {
		t.Fatalf("Failed to connect with TLS: %v", err)
	}
	defer nc.Close()

	// Now also make sure we can connect through plain text.
	nurl := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	nc2, err := nats.Connect(nurl)
	if err != nil {
		t.Fatalf("Failed to connect without TLS: %v", err)
	}
	defer nc2.Close()

	// Make sure they can go back and forth.
	sub, err := nc.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Error subscribing: %v", err)
	}
	sub2, err := nc2.SubscribeSync("bar")
	if err != nil {
		t.Fatalf("Error subscribing: %v", err)
	}
	nc.Flush()
	nc2.Flush()

	nmsgs := 100
	for i := 0; i < nmsgs; i++ {
		nc2.Publish("foo", []byte("HELLO FROM PLAINTEXT"))
		nc.Publish("bar", []byte("HELLO FROM TLS"))
	}
	// Now wait for the messages.
	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		if msgs, _, err := sub.Pending(); err != nil || msgs != nmsgs {
			return fmt.Errorf("Did not receive the correct number of messages: %d", msgs)
		}
		if msgs, _, err := sub2.Pending(); err != nil || msgs != nmsgs {
			return fmt.Errorf("Did not receive the correct number of messages: %d", msgs)
		}
		return nil
	})

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
			defer removeFile(t, conf)
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
				conn.SetWriteDeadline(time.Now().Add(10 * time.Millisecond))
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

func TestTLSClientAuthWithRDNSequence(t *testing.T) {
	for _, test := range []struct {
		name   string
		config string
		certs  nats.Option
		err    error
		rerr   error
	}{
		// To generate certs for these tests:
		//
		// ```
		// openssl req -newkey rsa:2048  -nodes -keyout client-$CLIENT_ID.key -subj "/C=US/ST=CA/L=Los Angeles/OU=NATS/O=NATS/CN=*.example.com/DC=example/DC=com" -addext extendedKeyUsage=clientAuth -out client-$CLIENT_ID.csr
		// openssl x509 -req -extfile <(printf "subjectAltName=DNS:localhost,DNS:example.com,DNS:www.example.com") -days 1825 -in client-$CLIENT_ID.csr -CA ca.pem -CAkey ca.key -CAcreateserial -out client-$CLIENT_ID.pem
		// ```
		//
		// To confirm subject from cert:
		//
		// ```
		// openssl x509 -in client-$CLIENT_ID.pem -text | grep Subject:
		// ```
		//
		{
			"connect with tls using full RDN sequence",
			`
                                port: -1
                                %s

                                authorization {
                                  users = [
                                    { user = "CN=localhost,OU=NATS,O=NATS,L=Los Angeles,ST=CA,C=US,DC=foo1,DC=foo2" }
                                  ]
                                }
                        `,
			// C = US, ST = CA, L = Los Angeles, O = NATS, OU = NATS, CN = localhost, DC = foo1, DC = foo2
			nats.ClientCert("./configs/certs/rdns/client-a.pem", "./configs/certs/rdns/client-a.key"),
			nil,
			nil,
		},
		{
			"connect with tls using full RDN sequence in original order",
			`
				port: -1
				%s

				authorization {
				  users = [
				    { user = "DC=foo2,DC=foo1,CN=localhost,OU=NATS,O=NATS,L=Los Angeles,ST=CA,C=US" }
				  ]
				}
			`,
			// C = US, ST = CA, L = Los Angeles, O = NATS, OU = NATS, CN = localhost, DC = foo1, DC = foo2
			nats.ClientCert("./configs/certs/rdns/client-a.pem", "./configs/certs/rdns/client-a.key"),
			nil,
			nil,
		},
		{
			"connect with tls using partial RDN sequence has different permissions",
			`
				port: -1
				%s

				authorization {
				  users = [
				    { user = "DC=foo2,DC=foo1,CN=localhost,OU=NATS,O=NATS,L=Los Angeles,ST=CA,C=US" },
				    { user = "CN=localhost,OU=NATS,O=NATS,L=Los Angeles,ST=CA,C=US,DC=foo1,DC=foo2" },
				    { user = "CN=localhost,OU=NATS,O=NATS,L=Los Angeles,ST=CA,C=US",
                                      permissions = { subscribe = { deny = ">" }} }
				  ]
				}
			`,
			// C = US, ST = California, L = Los Angeles, O = NATS, OU = NATS, CN = localhost
			nats.ClientCert("./configs/certs/rdns/client-b.pem", "./configs/certs/rdns/client-b.key"),
			nil,
			errors.New("nats: timeout"),
		},
		{
			"connect with tls and RDN sequence partially matches",
			`
				port: -1
				%s

				authorization {
				  users = [
				    { user = "DC=foo2,DC=foo1,CN=localhost,OU=NATS,O=NATS,L=Los Angeles,ST=CA,C=US" }
				    { user = "CN=localhost,OU=NATS,O=NATS,L=Los Angeles,ST=CA,C=US,DC=foo1,DC=foo2" }
				    { user = "CN=localhost,OU=NATS,O=NATS,L=Los Angeles,ST=CA,C=US"},
				  ]
				}
			`,
			// Cert is:
			//
			// C = US, ST = CA, L = Los Angeles, O = NATS, OU = NATS, CN = localhost, DC = foo3, DC = foo4
			//
			// but it will actually match the user without DCs so will not get an error:
			//
			// C = US, ST = CA, L = Los Angeles, O = NATS, OU = NATS, CN = localhost
			//
			nats.ClientCert("./configs/certs/rdns/client-c.pem", "./configs/certs/rdns/client-c.key"),
			nil,
			nil,
		},
		{
			"connect with tls and RDN sequence does not match",
			`
				port: -1
				%s

				authorization {
				  users = [
				    { user = "CN=localhost,OU=NATS,O=NATS,L=Los Angeles,ST=CA,C=US,DC=foo1,DC=foo2" },
				    { user = "DC=foo2,DC=foo1,CN=localhost,OU=NATS,O=NATS,L=Los Angeles,ST=CA,C=US" }
				  ]
				}
			`,
			// C = US, ST = California, L = Los Angeles, O = NATS, OU = NATS, CN = localhost, DC = foo3, DC = foo4
			//
			nats.ClientCert("./configs/certs/rdns/client-c.pem", "./configs/certs/rdns/client-c.key"),
			errors.New("nats: Authorization Violation"),
			nil,
		},
		{
			"connect with tls and RDN sequence with space after comma not should matter",
			`
				port: -1
				%s

				authorization {
				  users = [
				    { user = "DC=foo2, DC=foo1, CN=localhost, OU=NATS, O=NATS, L=Los Angeles, ST=CA, C=US" }
				  ]
				}
			`,
			// C=US/ST=California/L=Los Angeles/O=NATS/OU=NATS/CN=localhost/DC=foo1/DC=foo2
			//
			nats.ClientCert("./configs/certs/rdns/client-a.pem", "./configs/certs/rdns/client-a.key"),
			nil,
			nil,
		},
		{
			"connect with tls and full RDN sequence respects order",
			`
				port: -1
				%s

				authorization {
				  users = [
				    { user = "DC=com, DC=example, CN=*.example.com, O=NATS, OU=NATS, L=Los Angeles, ST=CA, C=US" }
				  ]
				}
			`,
			//
			// C = US, ST = CA, L = Los Angeles, OU = NATS, O = NATS, CN = *.example.com, DC = example, DC = com
			//
			nats.ClientCert("./configs/certs/rdns/client-d.pem", "./configs/certs/rdns/client-d.key"),
			nil,
			nil,
		},
		{
			"connect with tls and full RDN sequence with added domainComponents and spaces also matches",
			`
				port: -1
				%s

				authorization {
				  users = [
				    { user = "CN=*.example.com,OU=NATS,O=NATS,L=Los Angeles,ST=CA,C=US,DC=example,DC=com" }
				  ]
				}
			`,
			//
			// C = US, ST = CA, L = Los Angeles, OU = NATS, O = NATS, CN = *.example.com, DC = example, DC = com
			//
			nats.ClientCert("./configs/certs/rdns/client-d.pem", "./configs/certs/rdns/client-d.key"),
			nil,
			nil,
		},
		{
			"connect with tls and full RDN sequence with correct order takes precedence over others matches",
			`
				port: -1
				%s

				authorization {
				  users = [
				    { user = "CN=*.example.com,OU=NATS,O=NATS,L=Los Angeles,ST=CA,C=US,DC=example,DC=com",
                                        permissions = { subscribe = { deny = ">" }} }

				    # This should take precedence since it is in the RFC2253 order.
				    { user = "DC=com,DC=example,CN=*.example.com,O=NATS,OU=NATS,L=Los Angeles,ST=CA,C=US" }

				    { user = "CN=*.example.com,OU=NATS,O=NATS,L=Los Angeles,ST=CA,C=US",
                                        permissions = { subscribe = { deny = ">" }} }
				  ]
				}
			`,
			//
			// C = US, ST = CA, L = Los Angeles, OU = NATS, O = NATS, CN = *.example.com, DC = example, DC = com
			//
			nats.ClientCert("./configs/certs/rdns/client-d.pem", "./configs/certs/rdns/client-d.key"),
			nil,
			nil,
		},
		{
			"connect with tls and RDN includes multiple CN elements",
			`
				port: -1
				%s

				authorization {
				  users = [
				    { user = "DC=com,DC=acme,OU=Organic Units,OU=Users,CN=jdoe,CN=123456,CN=John Doe" }
                                  ]
				}
			`,
			//
			// OpenSSL: -subj "/CN=John Doe/CN=123456/CN=jdoe/OU=Users/OU=Organic Units/DC=acme/DC=com"
			// Go:       CN=jdoe,OU=Users+OU=Organic Units
			// RFC2253:  DC=com,DC=acme,OU=Organic Units,OU=Users,CN=jdoe,CN=123456,CN=John Doe
			//
			nats.ClientCert("./configs/certs/rdns/client-e.pem", "./configs/certs/rdns/client-e.key"),
			nil,
			nil,
		},
		{
			"connect with tls and DN includes a multi value RDN",
			`
				port: -1
				%s

				authorization {
				  users = [
				    { user = "CN=John Doe,DC=DEV+O=users,DC=OpenSSL,DC=org" }
                                  ]
				}
			`,
			//
			// OpenSSL: -subj "/DC=org/DC=OpenSSL/DC=DEV+O=users/CN=John Doe"
			// Go:       CN=John Doe,O=users
			// RFC2253:  CN=John Doe,DC=DEV+O=users,DC=OpenSSL,DC=org
			//
			nats.ClientCert("./configs/certs/rdns/client-f.pem", "./configs/certs/rdns/client-f.key"),
			nil,
			nil,
		},
		{
			"connect with tls and DN includes a multi value RDN but there is no match",
			`
				port: -1
				%s

				authorization {
				  users = [
				    { user = "CN=John Doe,DC=DEV,DC=OpenSSL,DC=org" }
                                  ]
				}
			`,
			//
			// OpenSSL: -subj "/DC=org/DC=OpenSSL/DC=DEV+O=users/CN=John Doe" -multivalue-rdn
			// Go:       CN=John Doe,O=users
			// RFC2253:  CN=John Doe,DC=DEV+O=users,DC=OpenSSL,DC=org
			//
			nats.ClientCert("./configs/certs/rdns/client-f.pem", "./configs/certs/rdns/client-f.key"),
			errors.New("nats: Authorization Violation"),
			nil,
		},
		{
			"connect with tls and DN includes a multi value RDN that are reordered",
			`
				port: -1
				%s

				authorization {
				  users = [
				    { user = "CN=John Doe,O=users+DC=DEV,DC=OpenSSL,DC=org" }
                                  ]
				}
			`,
			//
			// OpenSSL: -subj "/DC=org/DC=OpenSSL/DC=DEV+O=users/CN=John Doe" -multivalue-rdn
			// Go:       CN=John Doe,O=users
			// RFC2253:  CN=John Doe,DC=DEV+O=users,DC=OpenSSL,DC=org
			//
			nats.ClientCert("./configs/certs/rdns/client-f.pem", "./configs/certs/rdns/client-f.key"),
			nil,
			nil,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			content := fmt.Sprintf(test.config, `
				tls {
					cert_file: "configs/certs/rdns/server.pem"
					key_file: "configs/certs/rdns/server.key"
					ca_file: "configs/certs/rdns/ca.pem"
					timeout: 5
					verify_and_map: true
				}
			`)
			conf := createConfFile(t, []byte(content))
			defer removeFile(t, conf)
			s, opts := RunServerWithConfig(conf)
			defer s.Shutdown()

			nc, err := nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port),
				test.certs,
				nats.RootCAs("./configs/certs/rdns/ca.pem"),
				nats.ErrorHandler(noOpErrHandler),
			)
			if test.err == nil && err != nil {
				t.Errorf("Expected to connect, got %v", err)
			} else if test.err != nil && err == nil {
				t.Errorf("Expected error on connect")
			} else if test.err != nil && err != nil {
				// Error on connect was expected
				if test.err.Error() != err.Error() {
					t.Errorf("Expected error %s, got: %s", test.err, err)
				}
				return
			}
			defer nc.Close()

			nc.Subscribe("ping", func(m *nats.Msg) {
				m.Respond([]byte("pong"))
			})
			nc.Flush()

			_, err = nc.Request("ping", []byte("ping"), 250*time.Millisecond)
			if test.rerr != nil && err == nil {
				t.Errorf("Expected error getting response")
			} else if test.rerr == nil && err != nil {
				t.Errorf("Expected response")
			}
		})
	}
}

func TestTLSClientAuthWithRDNSequenceReordered(t *testing.T) {
	for _, test := range []struct {
		name   string
		config string
		certs  nats.Option
		err    error
		rerr   error
	}{
		{
			"connect with tls and DN includes a multi value RDN that are reordered",
			`
				port: -1
				%s

				authorization {
				  users = [
				    { user = "DC=org,DC=OpenSSL,O=users+DC=DEV,CN=John Doe" }
                                  ]
				}
			`,
			//
			// OpenSSL: -subj "/DC=org/DC=OpenSSL/DC=DEV+O=users/CN=John Doe" -multivalue-rdn
			// Go:       CN=John Doe,O=users
			// RFC2253:  CN=John Doe,DC=DEV+O=users,DC=OpenSSL,DC=org
			//
			nats.ClientCert("./configs/certs/rdns/client-f.pem", "./configs/certs/rdns/client-f.key"),
			nil,
			nil,
		},
		{
			"connect with tls and DN includes a multi value RDN that are reordered but not equal RDNs",
			`
				port: -1
				%s

				authorization {
				  users = [
				    { user = "DC=org,DC=OpenSSL,O=users,CN=John Doe" }
                                  ]
				}
			`,
			//
			// OpenSSL: -subj "/DC=org/DC=OpenSSL/DC=DEV+O=users/CN=John Doe" -multivalue-rdn
			// Go:       CN=John Doe,O=users
			// RFC2253:  CN=John Doe,DC=DEV+O=users,DC=OpenSSL,DC=org
			//
			nats.ClientCert("./configs/certs/rdns/client-f.pem", "./configs/certs/rdns/client-f.key"),
			errors.New("nats: Authorization Violation"),
			nil,
		},
		{
			"connect with tls and DN includes a multi value RDN that are reordered but not equal RDNs",
			`
				port: -1
				%s

				authorization {
				  users = [
				    { user = "DC=OpenSSL, DC=org, O=users, CN=John Doe" }
                                  ]
				}
			`,
			//
			// OpenSSL: -subj "/DC=org/DC=OpenSSL/DC=DEV+O=users/CN=John Doe" -multivalue-rdn
			// Go:       CN=John Doe,O=users
			// RFC2253:  CN=John Doe,DC=DEV+O=users,DC=OpenSSL,DC=org
			//
			nats.ClientCert("./configs/certs/rdns/client-f.pem", "./configs/certs/rdns/client-f.key"),
			errors.New("nats: Authorization Violation"),
			nil,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			content := fmt.Sprintf(test.config, `
				tls {
					cert_file: "configs/certs/rdns/server.pem"
					key_file: "configs/certs/rdns/server.key"
					ca_file: "configs/certs/rdns/ca.pem"
					timeout: 5
					verify_and_map: true
				}
			`)
			conf := createConfFile(t, []byte(content))
			defer removeFile(t, conf)
			s, opts := RunServerWithConfig(conf)
			defer s.Shutdown()

			nc, err := nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port),
				test.certs,
				nats.RootCAs("./configs/certs/rdns/ca.pem"),
			)
			if test.err == nil && err != nil {
				t.Errorf("Expected to connect, got %v", err)
			} else if test.err != nil && err == nil {
				t.Errorf("Expected error on connect")
			} else if test.err != nil && err != nil {
				// Error on connect was expected
				if test.err.Error() != err.Error() {
					t.Errorf("Expected error %s, got: %s", test.err, err)
				}
				return
			}
			defer nc.Close()

			nc.Subscribe("ping", func(m *nats.Msg) {
				m.Respond([]byte("pong"))
			})
			nc.Flush()

			_, err = nc.Request("ping", []byte("ping"), 250*time.Millisecond)
			if test.rerr != nil && err == nil {
				t.Errorf("Expected error getting response")
			} else if test.rerr == nil && err != nil {
				t.Errorf("Expected response")
			}
		})
	}
}

func TestTLSClientSVIDAuth(t *testing.T) {
	for _, test := range []struct {
		name   string
		config string
		certs  nats.Option
		err    error
		rerr   error
	}{
		{
			"connect with tls using certificate with URIs",
			`
				port: -1
				%s

				authorization {
				  users = [
				    {
                                      user = "spiffe://localhost/my-nats-service/user-a"
                                    }
				  ]
				}
			`,
			nats.ClientCert("./configs/certs/svid/client-a.pem", "./configs/certs/svid/client-a.key"),
			nil,
			nil,
		},
		{
			"connect with tls using certificate with limited different permissions",
			`
				port: -1
				%s

				authorization {
				  users = [
				    {
                                      user = "spiffe://localhost/my-nats-service/user-a"
                                    },
				    {
                                      user = "spiffe://localhost/my-nats-service/user-b"
                                      permissions = { subscribe = { deny = ">" }}
                                    }
				  ]
				}
			`,
			nats.ClientCert("./configs/certs/svid/client-b.pem", "./configs/certs/svid/client-b.key"),
			nil,
			errors.New("nats: timeout"),
		},
		{
			"connect with tls without URIs in permissions will still match SAN",
			`
				port: -1
				%s

				authorization {
				  users = [
				    {
                                      user = "O=SPIRE,C=US"
                                    }
				  ]
				}
			`,
			nats.ClientCert("./configs/certs/svid/client-b.pem", "./configs/certs/svid/client-b.key"),
			nil,
			nil,
		},
		{
			"connect with tls but no permissions",
			`
				port: -1
				%s

				authorization {
				  users = [
				    {
                                      user = "spiffe://localhost/my-nats-service/user-c"
                                    }
				  ]
				}
			`,
			nats.ClientCert("./configs/certs/svid/client-a.pem", "./configs/certs/svid/client-a.key"),
			errors.New("nats: Authorization Violation"),
			nil,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			content := fmt.Sprintf(test.config, `
				tls {
					cert_file: "configs/certs/svid/server.pem"
					key_file: "configs/certs/svid/server.key"
					ca_file: "configs/certs/svid/ca.pem"
					timeout: 5
                                        insecure: true
					verify_and_map: true
				}
			`)
			conf := createConfFile(t, []byte(content))
			defer removeFile(t, conf)
			s, opts := RunServerWithConfig(conf)
			defer s.Shutdown()

			nc, err := nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port),
				test.certs,
				nats.RootCAs("./configs/certs/svid/ca.pem"),
				nats.ErrorHandler(noOpErrHandler),
			)
			if test.err == nil && err != nil {
				t.Errorf("Expected to connect, got %v", err)
			} else if test.err != nil && err == nil {
				t.Errorf("Expected error on connect")
			} else if test.err != nil && err != nil {
				// Error on connect was expected
				if test.err.Error() != err.Error() {
					t.Errorf("Expected error %s, got: %s", test.err, err)
				}
				return
			}
			defer nc.Close()

			nc.Subscribe("ping", func(m *nats.Msg) {
				m.Respond([]byte("pong"))
			})
			nc.Flush()

			_, err = nc.Request("ping", []byte("ping"), 250*time.Millisecond)
			if test.rerr != nil && err == nil {
				t.Errorf("Expected error getting response")
			} else if test.rerr == nil && err != nil {
				t.Errorf("Expected response")
			}
		})
	}
}

func TestTLSPinnedCertsClient(t *testing.T) {
	tmpl := `
	host: localhost
	port: -1
	tls {
		ca_file: "configs/certs/ca.pem"
		cert_file: "configs/certs/server-cert.pem"
		key_file: "configs/certs/server-key.pem"
		# Require a client certificate and map user id from certificate
		verify: true
		pinned_certs: ["%s"]
	}`

	confFileName := createConfFile(t, []byte(fmt.Sprintf(tmpl, "aaaaaaaa09fde09451411ba3b42c0f74727d61a974c69fd3cf5257f39c75f0e9")))
	defer removeFile(t, confFileName)
	srv, o := RunServerWithConfig(confFileName)
	defer srv.Shutdown()

	if len(o.TLSPinnedCerts) != 1 {
		t.Fatal("expected one pinned cert")
	}

	opts := []nats.Option{
		nats.RootCAs("configs/certs/ca.pem"),
		nats.ClientCert("./configs/certs/client-cert.pem", "./configs/certs/client-key.pem"),
	}

	nc, err := nats.Connect(srv.ClientURL(), opts...)
	if err == nil {
		nc.Close()
		t.Fatalf("Expected error trying to connect without a certificate in pinned_certs")
	}

	ioutil.WriteFile(confFileName, []byte(fmt.Sprintf(tmpl, "bf6f821f09fde09451411ba3b42c0f74727d61a974c69fd3cf5257f39c75f0e9")), 0660)
	if err := srv.Reload(); err != nil {
		t.Fatalf("on Reload got %v", err)
	}
	// reload pinned to the certs used
	nc, err = nats.Connect(srv.ClientURL(), opts...)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	nc.Close()
}

func TestTLSPinnedCertsRoute(t *testing.T) {
	tmplSeed := `
	host: localhost
	port: -1
	cluster {
		port: -1
		tls {
			ca_file: "configs/certs/ca.pem"
			cert_file: "configs/certs/server-cert.pem"
			key_file: "configs/certs/server-key.pem"
		}
	}`
	// this server connects to seed, but is set up to not trust seeds cert
	tmplSrv := `
	host: localhost
	port: -1
	cluster {
		port: -1
		routes = [nats-route://localhost:%d]
		tls {
			ca_file: "configs/certs/ca.pem"
			cert_file: "configs/certs/server-cert.pem"
			key_file: "configs/certs/server-key.pem"
			# Require a client certificate and map user id from certificate
			verify: true
			# expected to fail the seed server
			pinned_certs: ["%s"]
		}
	}`

	confSeed := createConfFile(t, []byte(tmplSeed))
	defer removeFile(t, confSeed)
	srvSeed, o := RunServerWithConfig(confSeed)
	defer srvSeed.Shutdown()

	confSrv := createConfFile(t, []byte(fmt.Sprintf(tmplSrv, o.Cluster.Port, "89386860ea1222698ea676fc97310bdf2bff6f7e2b0420fac3b3f8f5a08fede5")))
	defer removeFile(t, confSrv)
	srv, _ := RunServerWithConfig(confSrv)
	defer srv.Shutdown()

	checkClusterFormed(t, srvSeed, srv)

	// this change will result in the server being and remaining disconnected
	ioutil.WriteFile(confSrv, []byte(fmt.Sprintf(tmplSrv, o.Cluster.Port, "aaaaaaaa09fde09451411ba3b42c0f74727d61a974c69fd3cf5257f39c75f0e9")), 0660)
	if err := srv.Reload(); err != nil {
		t.Fatalf("on Reload got %v", err)
	}

	checkNumRoutes(t, srvSeed, 0)
	checkNumRoutes(t, srv, 0)
}
