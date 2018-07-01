// Copyright 2012-2018 The NATS Authors
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
	"crypto/tls"
	"flag"
	"io/ioutil"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestDefaultOptions(t *testing.T) {
	golden := &Options{
		Host:             DEFAULT_HOST,
		Port:             DEFAULT_PORT,
		MaxConn:          DEFAULT_MAX_CONNECTIONS,
		HTTPHost:         DEFAULT_HOST,
		PingInterval:     DEFAULT_PING_INTERVAL,
		MaxPingsOut:      DEFAULT_PING_MAX_OUT,
		TLSTimeout:       float64(TLS_TIMEOUT) / float64(time.Second),
		AuthTimeout:      float64(AUTH_TIMEOUT) / float64(time.Second),
		MaxControlLine:   MAX_CONTROL_LINE_SIZE,
		MaxPayload:       MAX_PAYLOAD_SIZE,
		MaxPending:       MAX_PENDING_SIZE,
		WriteDeadline:    DEFAULT_FLUSH_DEADLINE,
		RQSubsSweep:      DEFAULT_REMOTE_QSUBS_SWEEPER,
		MaxClosedClients: DEFAULT_MAX_CLOSED_CLIENTS,
	}

	opts := &Options{}
	processOptions(opts)

	if !reflect.DeepEqual(golden, opts) {
		t.Fatalf("Default Options are incorrect.\nexpected: %+v\ngot: %+v",
			golden, opts)
	}
}

func TestOptions_RandomPort(t *testing.T) {
	opts := &Options{Port: RANDOM_PORT}
	processOptions(opts)

	if opts.Port != 0 {
		t.Fatalf("Process of options should have resolved random port to "+
			"zero.\nexpected: %d\ngot: %d\n", 0, opts.Port)
	}
}

func TestConfigFile(t *testing.T) {
	golden := &Options{
		ConfigFile:     "./configs/test.conf",
		Host:           "127.0.0.1",
		Port:           4242,
		Username:       "derek",
		Password:       "porkchop",
		AuthTimeout:    1.0,
		Debug:          false,
		Trace:          true,
		Logtime:        false,
		HTTPPort:       8222,
		PidFile:        "/tmp/gnatsd.pid",
		ProfPort:       6543,
		Syslog:         true,
		RemoteSyslog:   "udp://foo.com:33",
		MaxControlLine: 2048,
		MaxPayload:     65536,
		MaxConn:        100,
		MaxSubs:        1000,
		MaxPending:     10000000,
		PingInterval:   60 * time.Second,
		MaxPingsOut:    3,
		WriteDeadline:  3 * time.Second,
	}

	opts, err := ProcessConfigFile("./configs/test.conf")
	if err != nil {
		t.Fatalf("Received an error reading config file: %v\n", err)
	}

	if !reflect.DeepEqual(golden, opts) {
		t.Fatalf("Options are incorrect.\nexpected: %+v\ngot: %+v",
			golden, opts)
	}
}

func TestTLSConfigFile(t *testing.T) {
	golden := &Options{
		ConfigFile:  "./configs/tls.conf",
		Host:        "127.0.0.1",
		Port:        4443,
		Username:    "derek",
		Password:    "foo",
		AuthTimeout: 1.0,
		TLSTimeout:  2.0,
	}
	opts, err := ProcessConfigFile("./configs/tls.conf")
	if err != nil {
		t.Fatalf("Received an error reading config file: %v\n", err)
	}
	tlsConfig := opts.TLSConfig
	if tlsConfig == nil {
		t.Fatal("Expected opts.TLSConfig to be non-nil")
	}
	opts.TLSConfig = nil
	if !reflect.DeepEqual(golden, opts) {
		t.Fatalf("Options are incorrect.\nexpected: %+v\ngot: %+v",
			golden, opts)
	}
	// Now check TLSConfig a bit more closely
	// CipherSuites
	ciphers := defaultCipherSuites()
	if !reflect.DeepEqual(tlsConfig.CipherSuites, ciphers) {
		t.Fatalf("Got incorrect cipher suite list: [%+v]", tlsConfig.CipherSuites)
	}
	if tlsConfig.MinVersion != tls.VersionTLS12 {
		t.Fatalf("Expected MinVersion of 1.2 [%v], got [%v]", tls.VersionTLS12, tlsConfig.MinVersion)
	}
	if !tlsConfig.PreferServerCipherSuites {
		t.Fatal("Expected PreferServerCipherSuites to be true")
	}
	// Verify hostname is correct in certificate
	if len(tlsConfig.Certificates) != 1 {
		t.Fatal("Expected 1 certificate")
	}
	cert := tlsConfig.Certificates[0].Leaf
	if err := cert.VerifyHostname("127.0.0.1"); err != nil {
		t.Fatalf("Could not verify hostname in certificate: %v\n", err)
	}

	// Now test adding cipher suites.
	opts, err = ProcessConfigFile("./configs/tls_ciphers.conf")
	if err != nil {
		t.Fatalf("Received an error reading config file: %v\n", err)
	}
	tlsConfig = opts.TLSConfig
	if tlsConfig == nil {
		t.Fatal("Expected opts.TLSConfig to be non-nil")
	}

	// CipherSuites listed in the config - test all of them.
	ciphers = []uint16{
		tls.TLS_RSA_WITH_RC4_128_SHA,
		tls.TLS_RSA_WITH_3DES_EDE_CBC_SHA,
		tls.TLS_RSA_WITH_AES_128_CBC_SHA,
		tls.TLS_RSA_WITH_AES_256_CBC_SHA,
		tls.TLS_ECDHE_ECDSA_WITH_RC4_128_SHA,
		tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA,
		tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,
		tls.TLS_ECDHE_RSA_WITH_RC4_128_SHA,
		tls.TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA,
		tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,
		tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
		tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
	}

	if !reflect.DeepEqual(tlsConfig.CipherSuites, ciphers) {
		t.Fatalf("Got incorrect cipher suite list: [%+v]", tlsConfig.CipherSuites)
	}

	// Test an unrecognized/bad cipher
	if _, err := ProcessConfigFile("./configs/tls_bad_cipher.conf"); err == nil {
		t.Fatal("Did not receive an error from a unrecognized cipher")
	}

	// Test an empty cipher entry in a config file.
	if _, err := ProcessConfigFile("./configs/tls_empty_cipher.conf"); err == nil {
		t.Fatal("Did not receive an error from empty cipher_suites")
	}

	// Test a curve preference from the config.
	curves := []tls.CurveID{
		tls.CurveP256,
	}

	// test on a file that  will load the curve preference defaults
	opts, err = ProcessConfigFile("./configs/tls_ciphers.conf")
	if err != nil {
		t.Fatalf("Received an error reading config file: %v\n", err)
	}

	if !reflect.DeepEqual(opts.TLSConfig.CurvePreferences, defaultCurvePreferences()) {
		t.Fatalf("Got incorrect curve preference list: [%+v]", tlsConfig.CurvePreferences)
	}

	// Test specifying a single curve preference
	opts, err = ProcessConfigFile("./configs/tls_curve_prefs.conf")
	if err != nil {
		t.Fatal("Did not receive an error from a unrecognized cipher.")
	}

	if !reflect.DeepEqual(opts.TLSConfig.CurvePreferences, curves) {
		t.Fatalf("Got incorrect cipher suite list: [%+v]", tlsConfig.CurvePreferences)
	}

	// Test an unrecognized/bad curve preference
	if _, err := ProcessConfigFile("./configs/tls_bad_curve_prefs.conf"); err == nil {
		t.Fatal("Did not receive an error from a unrecognized curve preference")
	}
	// Test an empty curve preference
	if _, err := ProcessConfigFile("./configs/tls_empty_curve_prefs.conf"); err == nil {
		t.Fatal("Did not receive an error from empty curve preferences")
	}
}

func TestMergeOverrides(t *testing.T) {
	golden := &Options{
		ConfigFile:     "./configs/test.conf",
		Host:           "127.0.0.1",
		Port:           2222,
		Username:       "derek",
		Password:       "porkchop",
		AuthTimeout:    1.0,
		Debug:          true,
		Trace:          true,
		Logtime:        false,
		HTTPPort:       DEFAULT_HTTP_PORT,
		PidFile:        "/tmp/gnatsd.pid",
		ProfPort:       6789,
		Syslog:         true,
		RemoteSyslog:   "udp://foo.com:33",
		MaxControlLine: 2048,
		MaxPayload:     65536,
		MaxConn:        100,
		MaxSubs:        1000,
		MaxPending:     10000000,
		PingInterval:   60 * time.Second,
		MaxPingsOut:    3,
		Cluster: ClusterOpts{
			NoAdvertise:    true,
			ConnectRetries: 2,
		},
		WriteDeadline: 3 * time.Second,
	}
	fopts, err := ProcessConfigFile("./configs/test.conf")
	if err != nil {
		t.Fatalf("Received an error reading config file: %v\n", err)
	}

	// Overrides via flags
	opts := &Options{
		Port:     2222,
		Password: "porkchop",
		Debug:    true,
		HTTPPort: DEFAULT_HTTP_PORT,
		ProfPort: 6789,
		Cluster: ClusterOpts{
			NoAdvertise:    true,
			ConnectRetries: 2,
		},
	}
	merged := MergeOptions(fopts, opts)

	if !reflect.DeepEqual(golden, merged) {
		t.Fatalf("Options are incorrect.\nexpected: %+v\ngot: %+v",
			golden, merged)
	}
}

func TestRemoveSelfReference(t *testing.T) {
	url1, _ := url.Parse("nats-route://user:password@10.4.5.6:4223")
	url2, _ := url.Parse("nats-route://user:password@127.0.0.1:4223")
	url3, _ := url.Parse("nats-route://user:password@127.0.0.1:4223")

	routes := []*url.URL{url1, url2, url3}

	newroutes, err := RemoveSelfReference(4223, routes)
	if err != nil {
		t.Fatalf("Error during RemoveSelfReference: %v", err)
	}

	if len(newroutes) != 1 {
		t.Fatalf("Wrong number of routes: %d", len(newroutes))
	}

	if newroutes[0] != routes[0] {
		t.Fatalf("Self reference IP address %s in Routes", routes[0])
	}
}

func TestAllowRouteWithDifferentPort(t *testing.T) {
	url1, _ := url.Parse("nats-route://user:password@127.0.0.1:4224")
	routes := []*url.URL{url1}

	newroutes, err := RemoveSelfReference(4223, routes)
	if err != nil {
		t.Fatalf("Error during RemoveSelfReference: %v", err)
	}

	if len(newroutes) != 1 {
		t.Fatalf("Wrong number of routes: %d", len(newroutes))
	}
}

func TestRouteFlagOverride(t *testing.T) {
	routeFlag := "nats-route://ruser:top_secret@127.0.0.1:8246"
	rurl, _ := url.Parse(routeFlag)

	golden := &Options{
		ConfigFile: "./configs/srv_a.conf",
		Host:       "127.0.0.1",
		Port:       7222,
		Cluster: ClusterOpts{
			Host:        "127.0.0.1",
			Port:        7244,
			Username:    "ruser",
			Password:    "top_secret",
			AuthTimeout: 0.5,
		},
		Routes:    []*url.URL{rurl},
		RoutesStr: routeFlag,
	}

	fopts, err := ProcessConfigFile("./configs/srv_a.conf")
	if err != nil {
		t.Fatalf("Received an error reading config file: %v\n", err)
	}

	// Overrides via flags
	opts := &Options{
		RoutesStr: routeFlag,
	}
	merged := MergeOptions(fopts, opts)

	if !reflect.DeepEqual(golden, merged) {
		t.Fatalf("Options are incorrect.\nexpected: %+v\ngot: %+v",
			golden, merged)
	}
}

func TestClusterFlagsOverride(t *testing.T) {
	routeFlag := "nats-route://ruser:top_secret@127.0.0.1:7246"
	rurl, _ := url.Parse(routeFlag)

	// In this test, we override the cluster listen string. Note that in
	// the golden options, the cluster other infos correspond to what
	// is recovered from the configuration file, this explains the
	// discrepency between ClusterListenStr and the rest.
	// The server would then process the ClusterListenStr override and
	// correctly override ClusterHost/ClustherPort/etc..
	golden := &Options{
		ConfigFile: "./configs/srv_a.conf",
		Host:       "127.0.0.1",
		Port:       7222,
		Cluster: ClusterOpts{
			Host:        "127.0.0.1",
			Port:        7244,
			ListenStr:   "nats://127.0.0.1:8224",
			Username:    "ruser",
			Password:    "top_secret",
			AuthTimeout: 0.5,
		},
		Routes: []*url.URL{rurl},
	}

	fopts, err := ProcessConfigFile("./configs/srv_a.conf")
	if err != nil {
		t.Fatalf("Received an error reading config file: %v\n", err)
	}

	// Overrides via flags
	opts := &Options{
		Cluster: ClusterOpts{
			ListenStr: "nats://127.0.0.1:8224",
		},
	}
	merged := MergeOptions(fopts, opts)

	if !reflect.DeepEqual(golden, merged) {
		t.Fatalf("Options are incorrect.\nexpected: %+v\ngot: %+v",
			golden, merged)
	}
}

func TestRouteFlagOverrideWithMultiple(t *testing.T) {
	routeFlag := "nats-route://ruser:top_secret@127.0.0.1:8246, nats-route://ruser:top_secret@127.0.0.1:8266"
	rurls := RoutesFromStr(routeFlag)

	golden := &Options{
		ConfigFile: "./configs/srv_a.conf",
		Host:       "127.0.0.1",
		Port:       7222,
		Cluster: ClusterOpts{
			Host:        "127.0.0.1",
			Port:        7244,
			Username:    "ruser",
			Password:    "top_secret",
			AuthTimeout: 0.5,
		},
		Routes:    rurls,
		RoutesStr: routeFlag,
	}

	fopts, err := ProcessConfigFile("./configs/srv_a.conf")
	if err != nil {
		t.Fatalf("Received an error reading config file: %v\n", err)
	}

	// Overrides via flags
	opts := &Options{
		RoutesStr: routeFlag,
	}
	merged := MergeOptions(fopts, opts)

	if !reflect.DeepEqual(golden, merged) {
		t.Fatalf("Options are incorrect.\nexpected: %+v\ngot: %+v",
			golden, merged)
	}
}

func TestDynamicPortOnListen(t *testing.T) {
	opts, err := ProcessConfigFile("./configs/listen-1.conf")
	if err != nil {
		t.Fatalf("Received an error reading config file: %v\n", err)
	}
	if opts.Port != -1 {
		t.Fatalf("Received incorrect port %v, expected -1\n", opts.Port)
	}
	if opts.HTTPPort != -1 {
		t.Fatalf("Received incorrect monitoring port %v, expected -1\n", opts.HTTPPort)
	}
	if opts.HTTPSPort != -1 {
		t.Fatalf("Received incorrect secure monitoring port %v, expected -1\n", opts.HTTPSPort)
	}
}

func TestListenConfig(t *testing.T) {
	opts, err := ProcessConfigFile("./configs/listen.conf")
	if err != nil {
		t.Fatalf("Received an error reading config file: %v\n", err)
	}
	processOptions(opts)

	// Normal clients
	host := "10.0.1.22"
	port := 4422
	monHost := "127.0.0.1"
	if opts.Host != host {
		t.Fatalf("Received incorrect host %q, expected %q\n", opts.Host, host)
	}
	if opts.HTTPHost != monHost {
		t.Fatalf("Received incorrect host %q, expected %q\n", opts.HTTPHost, monHost)
	}
	if opts.Port != port {
		t.Fatalf("Received incorrect port %v, expected %v\n", opts.Port, port)
	}

	// Clustering
	clusterHost := "127.0.0.1"
	clusterPort := 4244

	if opts.Cluster.Host != clusterHost {
		t.Fatalf("Received incorrect cluster host %q, expected %q\n", opts.Cluster.Host, clusterHost)
	}
	if opts.Cluster.Port != clusterPort {
		t.Fatalf("Received incorrect cluster port %v, expected %v\n", opts.Cluster.Port, clusterPort)
	}

	// HTTP
	httpHost := "127.0.0.1"
	httpPort := 8422

	if opts.HTTPHost != httpHost {
		t.Fatalf("Received incorrect http host %q, expected %q\n", opts.HTTPHost, httpHost)
	}
	if opts.HTTPPort != httpPort {
		t.Fatalf("Received incorrect http port %v, expected %v\n", opts.HTTPPort, httpPort)
	}

	// HTTPS
	httpsPort := 9443
	if opts.HTTPSPort != httpsPort {
		t.Fatalf("Received incorrect https port %v, expected %v\n", opts.HTTPSPort, httpsPort)
	}
}

func TestListenPortOnlyConfig(t *testing.T) {
	opts, err := ProcessConfigFile("./configs/listen_port.conf")
	if err != nil {
		t.Fatalf("Received an error reading config file: %v\n", err)
	}
	processOptions(opts)

	port := 8922

	if opts.Host != DEFAULT_HOST {
		t.Fatalf("Received incorrect host %q, expected %q\n", opts.Host, DEFAULT_HOST)
	}
	if opts.HTTPHost != DEFAULT_HOST {
		t.Fatalf("Received incorrect host %q, expected %q\n", opts.Host, DEFAULT_HOST)
	}
	if opts.Port != port {
		t.Fatalf("Received incorrect port %v, expected %v\n", opts.Port, port)
	}
}

func TestListenPortWithColonConfig(t *testing.T) {
	opts, err := ProcessConfigFile("./configs/listen_port_with_colon.conf")
	if err != nil {
		t.Fatalf("Received an error reading config file: %v\n", err)
	}
	processOptions(opts)

	port := 8922

	if opts.Host != DEFAULT_HOST {
		t.Fatalf("Received incorrect host %q, expected %q\n", opts.Host, DEFAULT_HOST)
	}
	if opts.HTTPHost != DEFAULT_HOST {
		t.Fatalf("Received incorrect host %q, expected %q\n", opts.Host, DEFAULT_HOST)
	}
	if opts.Port != port {
		t.Fatalf("Received incorrect port %v, expected %v\n", opts.Port, port)
	}
}

func TestListenMonitoringDefault(t *testing.T) {
	opts := &Options{
		Host: "10.0.1.22",
	}
	processOptions(opts)

	host := "10.0.1.22"
	if opts.Host != host {
		t.Fatalf("Received incorrect host %q, expected %q\n", opts.Host, host)
	}
	if opts.HTTPHost != host {
		t.Fatalf("Received incorrect host %q, expected %q\n", opts.Host, host)
	}
	if opts.Port != DEFAULT_PORT {
		t.Fatalf("Received incorrect port %v, expected %v\n", opts.Port, DEFAULT_PORT)
	}
}

func TestMultipleUsersConfig(t *testing.T) {
	opts, err := ProcessConfigFile("./configs/multiple_users.conf")
	if err != nil {
		t.Fatalf("Received an error reading config file: %v\n", err)
	}
	processOptions(opts)
}

// Test highly depends on contents of the config file listed below. Any changes to that file
// may very well break this test.
func TestAuthorizationConfig(t *testing.T) {
	opts, err := ProcessConfigFile("./configs/authorization.conf")
	if err != nil {
		t.Fatalf("Received an error reading config file: %v\n", err)
	}
	processOptions(opts)
	lu := len(opts.Users)
	if lu != 3 {
		t.Fatalf("Expected 3 users, got %d\n", lu)
	}
	// Build a map
	mu := make(map[string]*User)
	for _, u := range opts.Users {
		mu[u.Username] = u
	}

	// Alice
	alice, ok := mu["alice"]
	if !ok {
		t.Fatalf("Expected to see user Alice\n")
	}
	// Check for permissions details
	if alice.Permissions == nil {
		t.Fatalf("Expected Alice's permissions to be non-nil\n")
	}
	if alice.Permissions.Publish == nil {
		t.Fatalf("Expected Alice's publish permissions to be non-nil\n")
	}
	if len(alice.Permissions.Publish) != 1 {
		t.Fatalf("Expected Alice's publish permissions to have 1 element, got %d\n",
			len(alice.Permissions.Publish))
	}
	pubPerm := alice.Permissions.Publish[0]
	if pubPerm != "*" {
		t.Fatalf("Expected Alice's publish permissions to be '*', got %q\n", pubPerm)
	}
	if alice.Permissions.Subscribe == nil {
		t.Fatalf("Expected Alice's subscribe permissions to be non-nil\n")
	}
	if len(alice.Permissions.Subscribe) != 1 {
		t.Fatalf("Expected Alice's subscribe permissions to have 1 element, got %d\n",
			len(alice.Permissions.Subscribe))
	}
	subPerm := alice.Permissions.Subscribe[0]
	if subPerm != ">" {
		t.Fatalf("Expected Alice's subscribe permissions to be '>', got %q\n", subPerm)
	}

	// Bob
	bob, ok := mu["bob"]
	if !ok {
		t.Fatalf("Expected to see user Bob\n")
	}
	if bob.Permissions == nil {
		t.Fatalf("Expected Bob's permissions to be non-nil\n")
	}

	// Susan
	susan, ok := mu["susan"]
	if !ok {
		t.Fatalf("Expected to see user Susan\n")
	}
	if susan.Permissions == nil {
		t.Fatalf("Expected Susan's permissions to be non-nil\n")
	}
	// Check susan closely since she inherited the default permissions.
	if susan.Permissions == nil {
		t.Fatalf("Expected Susan's permissions to be non-nil\n")
	}
	if susan.Permissions.Publish != nil {
		t.Fatalf("Expected Susan's publish permissions to be nil\n")
	}
	if susan.Permissions.Subscribe == nil {
		t.Fatalf("Expected Susan's subscribe permissions to be non-nil\n")
	}
	if len(susan.Permissions.Subscribe) != 1 {
		t.Fatalf("Expected Susan's subscribe permissions to have 1 element, got %d\n",
			len(susan.Permissions.Subscribe))
	}
	subPerm = susan.Permissions.Subscribe[0]
	if subPerm != "PUBLIC.>" {
		t.Fatalf("Expected Susan's subscribe permissions to be 'PUBLIC.>', got %q\n", subPerm)
	}
}

func TestTokenWithUserPass(t *testing.T) {
	confFileName := "test.conf"
	defer os.Remove(confFileName)
	content := `
	authorization={
		user: user
		pass: password
		token: $2a$11$whatever
	}`
	if err := ioutil.WriteFile(confFileName, []byte(content), 0666); err != nil {
		t.Fatalf("Error writing config file: %v", err)
	}
	_, err := ProcessConfigFile(confFileName)
	if err == nil {
		t.Fatal("Expected error, got none")
	}
	if !strings.Contains(err.Error(), "token") {
		t.Fatalf("Expected error related to token, got %v", err)
	}
}

func TestTokenWithUsers(t *testing.T) {
	confFileName := "test.conf"
	defer os.Remove(confFileName)
	content := `
	authorization={
		token: $2a$11$whatever
		users: [
			{user: test, password: test}
		]
	}`
	if err := ioutil.WriteFile(confFileName, []byte(content), 0666); err != nil {
		t.Fatalf("Error writing config file: %v", err)
	}
	_, err := ProcessConfigFile(confFileName)
	if err == nil {
		t.Fatal("Expected error, got none")
	}
	if !strings.Contains(err.Error(), "token") {
		t.Fatalf("Expected error related to token, got %v", err)
	}
}

func TestParseWriteDeadline(t *testing.T) {
	confFile := "test.conf"
	defer os.Remove(confFile)
	if err := ioutil.WriteFile(confFile, []byte("write_deadline: \"1x\"\n"), 0666); err != nil {
		t.Fatalf("Error writing config file: %v", err)
	}
	_, err := ProcessConfigFile(confFile)
	if err == nil {
		t.Fatal("Expected error, got none")
	}
	if !strings.Contains(err.Error(), "parsing") {
		t.Fatalf("Expected error related to parsing, got %v", err)
	}
	os.Remove(confFile)
	if err := ioutil.WriteFile(confFile, []byte("write_deadline: \"1s\"\n"), 0666); err != nil {
		t.Fatalf("Error writing config file: %v", err)
	}
	opts, err := ProcessConfigFile(confFile)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if opts.WriteDeadline != time.Second {
		t.Fatalf("Expected write_deadline to be 1s, got %v", opts.WriteDeadline)
	}
	os.Remove(confFile)
	oldStdout := os.Stdout
	_, w, _ := os.Pipe()
	defer func() {
		w.Close()
		os.Stdout = oldStdout
	}()
	os.Stdout = w
	if err := ioutil.WriteFile(confFile, []byte("write_deadline: 2\n"), 0666); err != nil {
		t.Fatalf("Error writing config file: %v", err)
	}
	opts, err = ProcessConfigFile(confFile)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if opts.WriteDeadline != 2*time.Second {
		t.Fatalf("Expected write_deadline to be 2s, got %v", opts.WriteDeadline)
	}
}

func TestOptionsClone(t *testing.T) {
	opts := &Options{
		ConfigFile:     "./configs/test.conf",
		Host:           "127.0.0.1",
		Port:           2222,
		Username:       "derek",
		Password:       "porkchop",
		AuthTimeout:    1.0,
		Debug:          true,
		Trace:          true,
		Logtime:        false,
		HTTPPort:       DEFAULT_HTTP_PORT,
		PidFile:        "/tmp/gnatsd.pid",
		ProfPort:       6789,
		Syslog:         true,
		RemoteSyslog:   "udp://foo.com:33",
		MaxControlLine: 2048,
		MaxPayload:     65536,
		MaxConn:        100,
		PingInterval:   60 * time.Second,
		MaxPingsOut:    3,
		Cluster: ClusterOpts{
			NoAdvertise:    true,
			ConnectRetries: 2,
		},
		WriteDeadline: 3 * time.Second,
		Routes:        []*url.URL{&url.URL{}},
		Users:         []*User{&User{Username: "foo", Password: "bar"}},
	}

	clone := opts.Clone()

	if !reflect.DeepEqual(opts, clone) {
		t.Fatalf("Cloned Options are incorrect.\nexpected: %+v\ngot: %+v",
			clone, opts)
	}

	clone.Users[0].Password = "baz"
	if reflect.DeepEqual(opts, clone) {
		t.Fatal("Expected Options to be different")
	}
}

func TestOptionsCloneNilLists(t *testing.T) {
	opts := &Options{}

	clone := opts.Clone()

	if clone.Routes != nil {
		t.Fatalf("Expected Routes to be nil, got: %v", clone.Routes)
	}
	if clone.Users != nil {
		t.Fatalf("Expected Users to be nil, got: %v", clone.Users)
	}
}

func TestOptionsCloneNil(t *testing.T) {
	opts := (*Options)(nil)
	clone := opts.Clone()
	if clone != nil {
		t.Fatalf("Expected nil, got: %+v", clone)
	}
}

func TestEmptyConfig(t *testing.T) {
	opts, err := ProcessConfigFile("")

	if err != nil {
		t.Fatalf("Expected no error from empty config, got: %+v", err)
	}

	if opts.ConfigFile != "" {
		t.Fatalf("Expected empty config, got: %+v", opts)
	}
}

func TestMalformedListenAddress(t *testing.T) {
	opts, err := ProcessConfigFile("./configs/malformed_listen_address.conf")
	if err == nil {
		t.Fatalf("Expected an error reading config file: got %+v\n", opts)
	}
}

func TestMalformedClusterAddress(t *testing.T) {
	opts, err := ProcessConfigFile("./configs/malformed_cluster_address.conf")
	if err == nil {
		t.Fatalf("Expected an error reading config file: got %+v\n", opts)
	}
}

func TestOptionsProcessConfigFile(t *testing.T) {
	// Create options with default values of Debug and Trace
	// that are the opposite of what is in the config file.
	// Set another option that is not present in the config file.
	logFileName := "test.log"
	opts := &Options{
		Debug:   true,
		Trace:   false,
		LogFile: logFileName,
	}
	configFileName := "./configs/test.conf"
	if err := opts.ProcessConfigFile(configFileName); err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}
	// Verify that values are as expected
	if opts.ConfigFile != configFileName {
		t.Fatalf("Expected ConfigFile to be set to %q, got %v", configFileName, opts.ConfigFile)
	}
	if opts.Debug {
		t.Fatal("Debug option should have been set to false from config file")
	}
	if !opts.Trace {
		t.Fatal("Trace option should have been set to true from config file")
	}
	if opts.LogFile != logFileName {
		t.Fatalf("Expected LogFile to be %q, got %q", logFileName, opts.LogFile)
	}
}

func TestConfigureOptions(t *testing.T) {
	// Options.Configure() will snapshot the flags. This is used by the reload code.
	// We need to set it back to nil otherwise it will impact reload tests.
	defer func() { FlagSnapshot = nil }()

	ch := make(chan bool, 1)
	checkPrintInvoked := func() {
		ch <- true
	}
	usage := func() { panic("should not get there") }
	var fs *flag.FlagSet
	type testPrint struct {
		args                   []string
		version, help, tlsHelp func()
	}
	testFuncs := []testPrint{
		testPrint{[]string{"-v"}, checkPrintInvoked, usage, PrintTLSHelpAndDie},
		testPrint{[]string{"version"}, checkPrintInvoked, usage, PrintTLSHelpAndDie},
		testPrint{[]string{"-h"}, PrintServerAndExit, checkPrintInvoked, PrintTLSHelpAndDie},
		testPrint{[]string{"help"}, PrintServerAndExit, checkPrintInvoked, PrintTLSHelpAndDie},
		testPrint{[]string{"-help_tls"}, PrintServerAndExit, usage, checkPrintInvoked},
	}
	for _, tf := range testFuncs {
		fs = flag.NewFlagSet("test", flag.ContinueOnError)
		opts, err := ConfigureOptions(fs, tf.args, tf.version, tf.help, tf.tlsHelp)
		if err != nil {
			t.Fatalf("Error on configure: %v", err)
		}
		if opts != nil {
			t.Fatalf("Expected options to be nil, got %v", opts)
		}
		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Fatalf("Should have invoked print function for args=%v", tf.args)
		}
	}

	// Helper function that expect parsing with given args to not produce an error.
	mustNotFail := func(args []string) *Options {
		fs := flag.NewFlagSet("test", flag.ContinueOnError)
		opts, err := ConfigureOptions(fs, args, PrintServerAndExit, fs.Usage, PrintTLSHelpAndDie)
		if err != nil {
			stackFatalf(t, "Error on configure: %v", err)
		}
		return opts
	}

	// Helper function that expect configuration to fail.
	expectToFail := func(args []string, errContent ...string) {
		fs := flag.NewFlagSet("test", flag.ContinueOnError)
		// Silence the flagSet so that on failure nothing is printed.
		// (flagSet would print error message about unknown flags, etc..)
		silenceOuput := &bytes.Buffer{}
		fs.SetOutput(silenceOuput)
		opts, err := ConfigureOptions(fs, args, PrintServerAndExit, fs.Usage, PrintTLSHelpAndDie)
		if opts != nil || err == nil {
			stackFatalf(t, "Expected no option and an error, got opts=%v and err=%v", opts, err)
		}
		for _, testErr := range errContent {
			if strings.Contains(err.Error(), testErr) {
				// We got the error we wanted.
				return
			}
		}
		stackFatalf(t, "Expected errors containing any of those %v, got %v", errContent, err)
	}

	// Basic test with port number
	opts := mustNotFail([]string{"-p", "1234"})
	if opts.Port != 1234 {
		t.Fatalf("Expected port to be 1234, got %v", opts.Port)
	}

	// Should fail because of unknown parameter
	expectToFail([]string{"foo"}, "command")

	// Should fail because unknown flag
	expectToFail([]string{"-xxx", "foo"}, "flag")

	// Should fail because of config file missing
	expectToFail([]string{"-c", "xxx.cfg"}, "file")

	// Should fail because of too many args for signal command
	expectToFail([]string{"-sl", "quit=pid=foo"}, "signal")

	// Should fail because of invalid pid
	// On windows, if not running with admin privileges, you would get access denied.
	expectToFail([]string{"-sl", "quit=pid"}, "pid", "denied")

	// The config file set Trace to true.
	opts = mustNotFail([]string{"-c", "./configs/test.conf"})
	if !opts.Trace {
		t.Fatal("Trace should have been set to true")
	}

	// The config file set Trace to true, but was overridden by param -V=false
	opts = mustNotFail([]string{"-c", "./configs/test.conf", "-V=false"})
	if opts.Trace {
		t.Fatal("Trace should have been set to false")
	}

	// The config file set Trace to true, but was overridden by param -DV=false
	opts = mustNotFail([]string{"-c", "./configs/test.conf", "-DV=false"})
	if opts.Debug || opts.Trace {
		t.Fatal("Debug and Trace should have been set to false")
	}

	// The config file set Trace to true, but was overridden by param -DV
	opts = mustNotFail([]string{"-c", "./configs/test.conf", "-DV"})
	if !opts.Debug || !opts.Trace {
		t.Fatal("Debug and Trace should have been set to true")
	}

	// This should fail since -cluster is missing
	expectedURL, _ := url.Parse("nats://127.0.0.1:6223")
	expectToFail([]string{"-routes", expectedURL.String()}, "solicited routes")

	// Ensure that we can set cluster and routes from command line
	opts = mustNotFail([]string{"-cluster", "nats://127.0.0.1:6222", "-routes", expectedURL.String()})
	if opts.Cluster.ListenStr != "nats://127.0.0.1:6222" {
		t.Fatalf("Unexpected Cluster.ListenStr=%q", opts.Cluster.ListenStr)
	}
	if opts.RoutesStr != "nats://127.0.0.1:6223" || len(opts.Routes) != 1 || opts.Routes[0].String() != expectedURL.String() {
		t.Fatalf("Unexpected RoutesStr: %q and Routes: %v", opts.RoutesStr, opts.Routes)
	}

	// Use a config with cluster configuration and explicit route defined.
	// Override with empty routes string.
	opts = mustNotFail([]string{"-c", "./configs/srv_a.conf", "-routes", ""})
	if opts.RoutesStr != "" || len(opts.Routes) != 0 {
		t.Fatalf("Unexpected RoutesStr: %q and Routes: %v", opts.RoutesStr, opts.Routes)
	}

	// Use a config with cluster configuration and override cluster listen string
	expectedURL, _ = url.Parse("nats-route://ruser:top_secret@127.0.0.1:7246")
	opts = mustNotFail([]string{"-c", "./configs/srv_a.conf", "-cluster", "nats://ivan:pwd@127.0.0.1:6222"})
	if opts.Cluster.Username != "ivan" || opts.Cluster.Password != "pwd" || opts.Cluster.Port != 6222 ||
		len(opts.Routes) != 1 || opts.Routes[0].String() != expectedURL.String() {
		t.Fatalf("Unexpected Cluster and/or Routes: %#v - %v", opts.Cluster, opts.Routes)
	}

	// Disable clustering from command line
	opts = mustNotFail([]string{"-c", "./configs/srv_a.conf", "-cluster", ""})
	if opts.Cluster.Port != 0 {
		t.Fatalf("Unexpected Cluster: %v", opts.Cluster)
	}

	// Various erros due to malformed cluster listen string.
	// (adding -routes to have more than 1 set flag to check
	// that Visit() stops when an error is found).
	expectToFail([]string{"-cluster", ":", "-routes", ""}, "protocol")
	expectToFail([]string{"-cluster", "nats://127.0.0.1", "-routes", ""}, "port")
	expectToFail([]string{"-cluster", "nats://127.0.0.1:xxx", "-routes", ""}, "integer")
	expectToFail([]string{"-cluster", "nats://ivan:127.0.0.1:6222", "-routes", ""}, "colons")
	expectToFail([]string{"-cluster", "nats://ivan@127.0.0.1:6222", "-routes", ""}, "password")

	// Override config file's TLS configuration from command line, and completely disable TLS
	opts = mustNotFail([]string{"-c", "./configs/tls.conf", "-tls=false"})
	if opts.TLSConfig != nil || opts.TLS {
		t.Fatal("Expected TLS to be disabled")
	}
	// Override config file's TLS configuration from command line, and force TLS verification.
	// However, since TLS config has to be regenerated, user need to provide -tlscert and -tlskey too.
	// So this should fail.
	expectToFail([]string{"-c", "./configs/tls.conf", "-tlsverify"}, "valid")

	// Now same than above, but with all valid params.
	opts = mustNotFail([]string{"-c", "./configs/tls.conf", "-tlsverify", "-tlscert", "./configs/certs/server.pem", "-tlskey", "./configs/certs/key.pem"})
	if opts.TLSConfig == nil || !opts.TLSVerify {
		t.Fatal("Expected TLS to be configured and force verification")
	}

	// Configure TLS, but some TLS params missing
	expectToFail([]string{"-tls"}, "valid")
	expectToFail([]string{"-tls", "-tlscert", "./configs/certs/server.pem"}, "valid")
	// One of the file does not exist
	expectToFail([]string{"-tls", "-tlscert", "./configs/certs/server.pem", "-tlskey", "./configs/certs/notfound.pem"}, "file")

	// Configure TLS and check that this results in a TLSConfig option.
	opts = mustNotFail([]string{"-tls", "-tlscert", "./configs/certs/server.pem", "-tlskey", "./configs/certs/key.pem"})
	if opts.TLSConfig == nil || !opts.TLS {
		t.Fatal("Expected TLSConfig to be set")
	}
}
