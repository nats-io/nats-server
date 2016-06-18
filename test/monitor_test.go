// Copyright 2012-2015 Apcera Inc. All rights reserved.

package test

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/nats-io/gnatsd/server"
	"github.com/nats-io/nats"
)

const CLIENT_PORT = 11422
const MONITOR_PORT = 11522

func runMonitorServer() *server.Server {
	resetPreviousHTTPConnections()
	opts := DefaultTestOptions
	opts.Port = CLIENT_PORT
	opts.HTTPPort = MONITOR_PORT
	opts.HTTPHost = "localhost"

	return RunServer(&opts)
}

func runMonitorServerNoHTTPPort() *server.Server {
	resetPreviousHTTPConnections()
	opts := DefaultTestOptions
	opts.Port = CLIENT_PORT
	opts.HTTPPort = 0

	return RunServer(&opts)
}

func resetPreviousHTTPConnections() {
	http.DefaultTransport = &http.Transport{}
}

// Make sure that we do not run the http server for monitoring unless asked.
func TestNoMonitorPort(t *testing.T) {
	s := runMonitorServerNoHTTPPort()
	defer s.Shutdown()

	url := fmt.Sprintf("http://localhost:%d/", MONITOR_PORT)
	if resp, err := http.Get(url + "varz"); err == nil {
		t.Fatalf("Expected error: Got %+v\n", resp)
	}
	if resp, err := http.Get(url + "healthz"); err == nil {
		t.Fatalf("Expected error: Got %+v\n", resp)
	}
	if resp, err := http.Get(url + "connz"); err == nil {
		t.Fatalf("Expected error: Got %+v\n", resp)
	}
}

func TestVarz(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	url := fmt.Sprintf("http://localhost:%d/", MONITOR_PORT)
	resp, err := http.Get(url + "varz")
	if err != nil {
		t.Fatalf("Expected no error: Got %v\n", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("Expected a 200 response, got %d\n", resp.StatusCode)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Got an error reading the body: %v\n", err)
	}

	v := server.Varz{}
	if err := json.Unmarshal(body, &v); err != nil {
		t.Fatalf("Got an error unmarshalling the body: %v\n", err)
	}

	// Do some sanity checks on values
	if time.Since(v.Start) > 10*time.Second {
		t.Fatal("Expected start time to be within 10 seconds.")
	}

	cl := createClientConnSubscribeAndPublish(t)
	defer cl.Close()

	resp, err = http.Get(url + "varz")
	if err != nil {
		t.Fatalf("Expected no error: Got %v\n", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("Expected a 200 response, got %d\n", resp.StatusCode)
	}
	defer resp.Body.Close()
	body, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Got an error reading the body: %v\n", err)
	}

	v = server.Varz{}
	if err := json.Unmarshal(body, &v); err != nil {
		t.Fatalf("Got an error unmarshalling the body: %v\n", err)
	}

	if v.Connections != 1 {
		t.Fatalf("Expected Connections of 1, got %v\n", v.Connections)
	}
	if v.InMsgs != 1 {
		t.Fatalf("Expected InMsgs of 1, got %v\n", v.InMsgs)
	}
	if v.OutMsgs != 1 {
		t.Fatalf("Expected OutMsgs of 1, got %v\n", v.OutMsgs)
	}
	if v.InBytes != 5 {
		t.Fatalf("Expected InBytes of 5, got %v\n", v.InBytes)
	}
	if v.OutBytes != 5 {
		t.Fatalf("Expected OutBytes of 5, got %v\n", v.OutBytes)
	}
}

func TestConnz(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	url := fmt.Sprintf("http://localhost:%d/", MONITOR_PORT)
	resp, err := http.Get(url + "connz")
	if err != nil {
		t.Fatalf("Expected no error: Got %v\n", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("Expected a 200 response, got %d\n", resp.StatusCode)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Got an error reading the body: %v\n", err)
	}

	c := server.Connz{}
	if err := json.Unmarshal(body, &c); err != nil {
		t.Fatalf("Got an error unmarshalling the body: %v\n", err)
	}

	// Test contents..
	if c.NumConns != 0 {
		t.Fatalf("Expected 0 connections, got %d\n", c.NumConns)
	}
	if c.Total != 0 {
		t.Fatalf("Expected 0 live connections, got %d\n", c.Total)
	}
	if c.Conns == nil || len(c.Conns) != 0 {
		t.Fatalf("Expected 0 connections in array, got %p\n", c.Conns)
	}

	cl := createClientConnSubscribeAndPublish(t)
	defer cl.Close()

	resp, err = http.Get(url + "connz")
	if err != nil {
		t.Fatalf("Expected no error: Got %v\n", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("Expected a 200 response, got %d\n", resp.StatusCode)
	}
	defer resp.Body.Close()
	body, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Got an error reading the body: %v\n", err)
	}
	if err := json.Unmarshal(body, &c); err != nil {
		t.Fatalf("Got an error unmarshalling the body: %v\n", err)
	}

	if c.NumConns != 1 {
		t.Fatalf("Expected 1 connection, got %d\n", c.NumConns)
	}
	if c.Total != 1 {
		t.Fatalf("Expected 1 live connection, got %d\n", c.Total)
	}
	if c.Conns == nil || len(c.Conns) != 1 {
		t.Fatalf("Expected 1 connection in array, got %p\n", c.Conns)
	}

	if c.Limit != server.DefaultConnListSize {
		t.Fatalf("Expected limit of %d, got %v\n", server.DefaultConnListSize, c.Limit)
	}

	if c.Offset != 0 {
		t.Fatalf("Expected offset of 0, got %v\n", c.Offset)
	}

	// Test inside details of each connection
	ci := c.Conns[0]

	if ci.Cid == 0 {
		t.Fatalf("Expected non-zero cid, got %v\n", ci.Cid)
	}
	if ci.IP != "127.0.0.1" {
		t.Fatalf("Expected \"127.0.0.1\" for IP, got %v\n", ci.IP)
	}
	if ci.Port == 0 {
		t.Fatalf("Expected non-zero port, got %v\n", ci.Port)
	}
	if ci.NumSubs != 1 {
		t.Fatalf("Expected num_subs of 1, got %v\n", ci.NumSubs)
	}
	if len(ci.Subs) != 0 {
		t.Fatalf("Expected subs of 0, got %v\n", ci.Subs)
	}
	if ci.InMsgs != 1 {
		t.Fatalf("Expected InMsgs of 1, got %v\n", ci.InMsgs)
	}
	if ci.OutMsgs != 1 {
		t.Fatalf("Expected OutMsgs of 1, got %v\n", ci.OutMsgs)
	}
	if ci.InBytes != 5 {
		t.Fatalf("Expected InBytes of 1, got %v\n", ci.InBytes)
	}
	if ci.OutBytes != 5 {
		t.Fatalf("Expected OutBytes of 1, got %v\n", ci.OutBytes)
	}
}

func TestTLSConnz(t *testing.T) {
	srv, opts := RunServerWithConfig("./configs/tls.conf")
	defer srv.Shutdown()
	rootCAFile := "./configs/certs/ca.pem"
	clientCertFile := "./configs/certs/client-cert.pem"
	clientKeyFile := "./configs/certs/client-key.pem"

	// Test with secure connection
	endpoint := fmt.Sprintf("%s:%d", opts.Host, opts.Port)
	nurl := fmt.Sprintf("tls://%s:%s@%s/", opts.Username, opts.Password, endpoint)
	nc, err := nats.Connect(nurl, nats.RootCAs(rootCAFile))
	if err != nil {
		t.Fatalf("Got an error on Connect with Secure Options: %+v\n", err)
	}
	defer nc.Close()
	ch := make(chan struct{})
	nc.Subscribe("foo", func(m *nats.Msg) { ch <- struct{}{} })
	nc.Publish("foo", []byte("Hello"))

	// Wait for message
	<-ch

	url := fmt.Sprintf("https://localhost:%d/", opts.HTTPSPort)
	tlsConfig := &tls.Config{}
	caCert, err := ioutil.ReadFile(rootCAFile)
	if err != nil {
		t.Fatalf("Got error reading RootCA file: %s", err)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	tlsConfig.RootCAs = caCertPool

	cert, err := tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
	if err != nil {
		t.Fatalf("Got error reading client certificates: %s", err)
	}
	tlsConfig.Certificates = []tls.Certificate{cert}
	transport := &http.Transport{TLSClientConfig: tlsConfig}
	httpClient := &http.Client{Transport: transport}

	resp, err := httpClient.Get(url + "connz")
	if err != nil {
		t.Fatalf("Expected no error: Got %v\n", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("Expected a 200 response, got %d\n", resp.StatusCode)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		t.Fatalf("Got an error reading the body: %v\n", err)
	}
	c := server.Connz{}
	if err := json.Unmarshal(body, &c); err != nil {
		t.Fatalf("Got an error unmarshalling the body: %v\n", err)
	}

	if c.NumConns != 1 {
		t.Fatalf("Expected 1 connection, got %d\n", c.NumConns)
	}
	if c.Total != 1 {
		t.Fatalf("Expected 1 live connection, got %d\n", c.Total)
	}
	if c.Conns == nil || len(c.Conns) != 1 {
		t.Fatalf("Expected 1 connection in array, got %d\n", len(c.Conns))
	}

	// Test inside details of each connection
	ci := c.Conns[0]

	if ci.Cid == 0 {
		t.Fatalf("Expected non-zero cid, got %v\n", ci.Cid)
	}
	if ci.IP != "127.0.0.1" {
		t.Fatalf("Expected \"127.0.0.1\" for IP, got %v\n", ci.IP)
	}
	if ci.Port == 0 {
		t.Fatalf("Expected non-zero port, got %v\n", ci.Port)
	}
	if ci.NumSubs != 1 {
		t.Fatalf("Expected num_subs of 1, got %v\n", ci.NumSubs)
	}
	if len(ci.Subs) != 0 {
		t.Fatalf("Expected subs of 0, got %v\n", ci.Subs)
	}
	if ci.InMsgs != 1 {
		t.Fatalf("Expected InMsgs of 1, got %v\n", ci.InMsgs)
	}
	if ci.OutMsgs != 1 {
		t.Fatalf("Expected OutMsgs of 1, got %v\n", ci.OutMsgs)
	}
	if ci.InBytes != 5 {
		t.Fatalf("Expected InBytes of 1, got %v\n", ci.InBytes)
	}
	if ci.OutBytes != 5 {
		t.Fatalf("Expected OutBytes of 1, got %v\n", ci.OutBytes)
	}
	if ci.Start.IsZero() {
		t.Fatalf("Expected Start to be valid\n")
	}
	if ci.Uptime == "" {
		t.Fatalf("Expected Uptime to be valid\n")
	}
	if ci.LastActivity.IsZero() {
		t.Fatalf("Expected LastActivity to be valid\n")
	}
	if ci.LastActivity.UnixNano() < ci.Start.UnixNano() {
		t.Fatalf("Expected LastActivity [%v] to be > Start [%v]\n", ci.LastActivity, ci.Start)
	}
	if ci.Idle == "" {
		t.Fatalf("Expected Idle to be valid\n")
	}
}

func TestConnzWithSubs(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	cl := createClientConnSubscribeAndPublish(t)
	defer cl.Close()

	url := fmt.Sprintf("http://localhost:%d/", MONITOR_PORT)
	resp, err := http.Get(url + "connz?subs=1")
	if err != nil {
		t.Fatalf("Expected no error: Got %v\n", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("Expected a 200 response, got %d\n", resp.StatusCode)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Got an error reading the body: %v\n", err)
	}

	c := server.Connz{}
	if err := json.Unmarshal(body, &c); err != nil {
		t.Fatalf("Got an error unmarshalling the body: %v\n", err)
	}

	// Test inside details of each connection
	ci := c.Conns[0]
	if len(ci.Subs) != 1 || ci.Subs[0] != "foo" {
		t.Fatalf("Expected subs of 1, got %v\n", ci.Subs)
	}
}

func TestConnzWithAuth(t *testing.T) {
	srv, opts := RunServerWithConfig("./configs/multi_user.conf")
	defer srv.Shutdown()

	endpoint := fmt.Sprintf("%s:%d", opts.Host, opts.Port)
	curl := fmt.Sprintf("nats://%s:%s@%s/", opts.Users[0].Username, opts.Users[0].Password, endpoint)
	nc, err := nats.Connect(curl)
	if err != nil {
		t.Fatalf("Got an error on Connect: %+v\n", err)
	}
	defer nc.Close()

	ch := make(chan struct{})
	nc.Subscribe("foo", func(m *nats.Msg) { ch <- struct{}{} })
	nc.Publish("foo", []byte("Hello"))

	// Wait for message
	<-ch

	url := fmt.Sprintf("http://localhost:%d/", opts.HTTPPort)

	resp, err := http.Get(url + "connz?auth=1")
	if err != nil {
		t.Fatalf("Expected no error: Got %v\n", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("Expected a 200 response, got %d\n", resp.StatusCode)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Got an error reading the body: %v\n", err)
	}

	c := server.Connz{}
	if err := json.Unmarshal(body, &c); err != nil {
		t.Fatalf("Got an error unmarshalling the body: %v\n", err)
	}

	// Test that we have authorized_user and its Alice.
	ci := c.Conns[0]
	if ci.AuthorizedUser != opts.Users[0].Username {
		t.Fatalf("Expected authorized_user to be %q, got %q\n",
			opts.Users[0].Username, ci.AuthorizedUser)
	}

}

func TestConnzWithOffsetAndLimit(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	cl1 := createClientConnSubscribeAndPublish(t)
	defer cl1.Close()

	cl2 := createClientConnSubscribeAndPublish(t)
	defer cl2.Close()

	url := fmt.Sprintf("http://localhost:%d/", MONITOR_PORT)
	resp, err := http.Get(url + "connz?offset=1&limit=1")
	if err != nil {
		t.Fatalf("Expected no error: Got %v\n", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("Expected a 200 response, got %d\n", resp.StatusCode)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Got an error reading the body: %v\n", err)
	}

	c := server.Connz{}
	if err := json.Unmarshal(body, &c); err != nil {
		t.Fatalf("Got an error unmarshalling the body: %v\n", err)
	}

	if c.Limit != 1 {
		t.Fatalf("Expected limit of 1, got %v\n", c.Limit)
	}

	if c.Offset != 1 {
		t.Fatalf("Expected offset of 1, got %v\n", c.Offset)
	}

	if len(c.Conns) != 1 {
		t.Fatalf("Expected conns of 1, got %v\n", len(c.Conns))
	}
}

func TestSubsz(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	cl := createClientConnSubscribeAndPublish(t)
	defer cl.Close()

	url := fmt.Sprintf("http://localhost:%d/", MONITOR_PORT)
	resp, err := http.Get(url + "subscriptionsz")
	if err != nil {
		t.Fatalf("Expected no error: Got %v\n", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("Expected a 200 response, got %d\n", resp.StatusCode)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Got an error reading the body: %v\n", err)
	}

	su := server.Subsz{}
	if err := json.Unmarshal(body, &su); err != nil {
		t.Fatalf("Got an error unmarshalling the body: %v\n", err)
	}

	// Do some sanity checks on values
	if su.NumSubs != 1 {
		t.Fatalf("Expected num_subs of 1, got %v\n", su.NumSubs)
	}
}

func TestHTTPHost(t *testing.T) {
	s := runMonitorServer()
	defer s.Shutdown()

	// Grab non-localhost address and try to use that to connect.
	// Should fail.
	var ip net.IP
	ifaces, _ := net.Interfaces()
	for _, i := range ifaces {
		addrs, _ := i.Addrs()
		for _, addr := range addrs {
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			// Skip loopback/localhost or any ipv6 for now.
			if ip.IsLoopback() || ip.To4() == nil {
				ip = nil
				continue
			}
			break
		}
		if ip != nil {
			break
		}
	}
	if ip == nil {
		t.Fatalf("Could not find non-loopback IPV4 address")
	}
	url := fmt.Sprintf("http://%v:%d/", ip, MONITOR_PORT)
	if resp, err := http.Get(url + "varz"); err == nil {
		t.Fatalf("Expected error: Got %+v\n", resp)
	}
}

// Create a connection to test ConnInfo
func createClientConnSubscribeAndPublish(t *testing.T) net.Conn {
	cl := createClientConn(t, "localhost", CLIENT_PORT)

	send := sendCommand(t, cl)
	send, expect := setupConn(t, cl)
	expectMsgs := expectMsgsCommand(t, expect)

	send("SUB foo 1\r\nPUB foo 5\r\nhello\r\n")
	expectMsgs(1)

	return cl
}
