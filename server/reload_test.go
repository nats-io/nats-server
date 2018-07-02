// Copyright 2017-2018 The NATS Authors
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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/go-nats"
)

// Ensure Reload returns an error when attempting to reload a server that did
// not start with a config file.
func TestConfigReloadNoConfigFile(t *testing.T) {
	server := New(&Options{NoSigs: true})
	loaded := server.ConfigTime()
	if server.Reload() == nil {
		t.Fatal("Expected Reload to return an error")
	}
	if reloaded := server.ConfigTime(); reloaded != loaded {
		t.Fatalf("ConfigTime is incorrect.\nexpected: %s\ngot: %s", loaded, reloaded)
	}
}

// Ensure Reload returns an error when attempting to change an option which
// does not support reloading.
func TestConfigReloadUnsupported(t *testing.T) {
	server, opts, config := newServerWithSymlinkConfig(t, "tmp.conf", "./configs/reload/test.conf")
	defer os.Remove(config)
	defer server.Shutdown()

	loaded := server.ConfigTime()

	golden := &Options{
		ConfigFile:     config,
		Host:           "0.0.0.0",
		Port:           2233,
		AuthTimeout:    1.0,
		Debug:          false,
		Trace:          false,
		Logtime:        false,
		MaxControlLine: 1024,
		MaxPayload:     1048576,
		MaxConn:        65536,
		PingInterval:   2 * time.Minute,
		MaxPingsOut:    2,
		WriteDeadline:  2 * time.Second,
		Cluster: ClusterOpts{
			Host: "127.0.0.1",
			Port: -1,
		},
		NoSigs: true,
	}
	processOptions(golden)

	if !reflect.DeepEqual(golden, server.getOpts()) {
		t.Fatalf("Options are incorrect.\nexpected: %+v\ngot: %+v",
			golden, opts)
	}

	// Change config file to bad config by replacing symlink.
	createSymlink(t, config, "./configs/reload/reload_unsupported.conf")

	// This should fail because `cluster` host cannot be changed.
	if err := server.Reload(); err == nil {
		t.Fatal("Expected Reload to return an error")
	}

	// Ensure config didn't change.
	if !reflect.DeepEqual(golden, server.getOpts()) {
		t.Fatalf("Options are incorrect.\nexpected: %+v\ngot: %+v",
			golden, opts)
	}

	if reloaded := server.ConfigTime(); reloaded != loaded {
		t.Fatalf("ConfigTime is incorrect.\nexpected: %s\ngot: %s", loaded, reloaded)
	}
}

// This checks that if we change an option that does not support hot-swapping
// we get an error. Using `listen` for now (test may need to be updated if
// server is changed to support change of listen spec).
func TestConfigReloadUnsupportedHotSwapping(t *testing.T) {
	orgConfig := "tmp_a.conf"
	newConfig := "tmp_b.conf"
	defer os.Remove(orgConfig)
	defer os.Remove(newConfig)
	if err := ioutil.WriteFile(orgConfig, []byte("listen: 127.0.0.1:-1"), 0666); err != nil {
		t.Fatalf("Error creating config file: %v", err)
	}
	if err := ioutil.WriteFile(newConfig, []byte("listen: 127.0.0.1:9999"), 0666); err != nil {
		t.Fatalf("Error creating config file: %v", err)
	}

	server, _, config := newServerWithSymlinkConfig(t, "tmp.conf", orgConfig)
	defer os.Remove(config)
	defer server.Shutdown()

	loaded := server.ConfigTime()

	time.Sleep(time.Millisecond)

	// Change config file with unsupported option hot-swap
	createSymlink(t, config, newConfig)

	// This should fail because `listen` host cannot be changed.
	if err := server.Reload(); err == nil || !strings.Contains(err.Error(), "not supported") {
		t.Fatalf("Expected Reload to return a not supported error, got %v", err)
	}

	if reloaded := server.ConfigTime(); reloaded != loaded {
		t.Fatalf("ConfigTime is incorrect.\nexpected: %s\ngot: %s", loaded, reloaded)
	}
}

// Ensure Reload returns an error when reloading from a bad config file.
func TestConfigReloadInvalidConfig(t *testing.T) {
	server, opts, config := newServerWithSymlinkConfig(t, "tmp.conf", "./configs/reload/test.conf")
	defer os.Remove(config)
	defer server.Shutdown()

	loaded := server.ConfigTime()

	golden := &Options{
		ConfigFile:     config,
		Host:           "0.0.0.0",
		Port:           2233,
		AuthTimeout:    1.0,
		Debug:          false,
		Trace:          false,
		Logtime:        false,
		MaxControlLine: 1024,
		MaxPayload:     1048576,
		MaxConn:        65536,
		PingInterval:   2 * time.Minute,
		MaxPingsOut:    2,
		WriteDeadline:  2 * time.Second,
		Cluster: ClusterOpts{
			Host: "127.0.0.1",
			Port: -1,
		},
		NoSigs: true,
	}
	processOptions(golden)

	if !reflect.DeepEqual(golden, server.getOpts()) {
		t.Fatalf("Options are incorrect.\nexpected: %+v\ngot: %+v",
			golden, opts)
	}

	// Change config file to bad config by replacing symlink.
	createSymlink(t, config, "./configs/reload/invalid.conf")

	// This should fail because the new config should not parse.
	if err := server.Reload(); err == nil {
		t.Fatal("Expected Reload to return an error")
	}

	// Ensure config didn't change.
	if !reflect.DeepEqual(golden, server.getOpts()) {
		t.Fatalf("Options are incorrect.\nexpected: %+v\ngot: %+v",
			golden, opts)
	}

	if reloaded := server.ConfigTime(); reloaded != loaded {
		t.Fatalf("ConfigTime is incorrect.\nexpected: %s\ngot: %s", loaded, reloaded)
	}
}

// Ensure Reload returns nil and the config is changed on success.
func TestConfigReload(t *testing.T) {
	var content []byte
	if runtime.GOOS != "windows" {
		content = []byte(`
			remote_syslog: "udp://127.0.0.1:514" # change on reload
			log_file:      "/tmp/gnatsd-2.log" # change on reload
		`)
	}
	platformConf := "platform.conf"
	defer os.Remove(platformConf)
	if err := ioutil.WriteFile(platformConf, content, 0666); err != nil {
		t.Fatalf("Unable to write config file: %v", err)
	}
	server, opts, config := runServerWithSymlinkConfig(t, "tmp.conf", "./configs/reload/test.conf")
	defer os.Remove(config)
	defer server.Shutdown()

	loaded := server.ConfigTime()

	golden := &Options{
		ConfigFile:     config,
		Host:           "0.0.0.0",
		Port:           2233,
		AuthTimeout:    1.0,
		Debug:          false,
		Trace:          false,
		NoLog:          true,
		Logtime:        false,
		MaxControlLine: 1024,
		MaxPayload:     1048576,
		MaxConn:        65536,
		PingInterval:   2 * time.Minute,
		MaxPingsOut:    2,
		WriteDeadline:  2 * time.Second,
		Cluster: ClusterOpts{
			Host: "127.0.0.1",
			Port: server.ClusterAddr().Port,
		},
		NoSigs: true,
	}
	processOptions(golden)

	if !reflect.DeepEqual(golden, opts) {
		t.Fatalf("Options are incorrect.\nexpected: %+v\ngot: %+v",
			golden, opts)
	}

	// Change config file to new config by replacing symlink.
	createSymlink(t, config, "./configs/reload/reload.conf")

	if err := server.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}

	// Ensure config changed.
	updated := server.getOpts()
	if !updated.Trace {
		t.Fatal("Expected Trace to be true")
	}
	if !updated.Debug {
		t.Fatal("Expected Debug to be true")
	}
	if !updated.Logtime {
		t.Fatal("Expected Logtime to be true")
	}
	if !updated.Syslog {
		t.Fatal("Expected Syslog to be true")
	}
	if runtime.GOOS != "windows" {
		if updated.RemoteSyslog != "udp://127.0.0.1:514" {
			t.Fatalf("RemoteSyslog is incorrect.\nexpected: udp://127.0.0.1:514\ngot: %s", updated.RemoteSyslog)
		}
		if updated.LogFile != "/tmp/gnatsd-2.log" {
			t.Fatalf("LogFile is incorrect.\nexpected: /tmp/gnatsd-2.log\ngot: %s", updated.LogFile)
		}
	}
	if updated.TLSConfig == nil {
		t.Fatal("Expected TLSConfig to be non-nil")
	}
	if !server.info.TLSRequired {
		t.Fatal("Expected TLSRequired to be true")
	}
	if !server.info.TLSVerify {
		t.Fatal("Expected TLSVerify to be true")
	}
	if updated.Username != "tyler" {
		t.Fatalf("Username is incorrect.\nexpected: tyler\ngot: %s", updated.Username)
	}
	if updated.Password != "T0pS3cr3t" {
		t.Fatalf("Password is incorrect.\nexpected: T0pS3cr3t\ngot: %s", updated.Password)
	}
	if updated.AuthTimeout != 2 {
		t.Fatalf("AuthTimeout is incorrect.\nexpected: 2\ngot: %f", updated.AuthTimeout)
	}
	if !server.info.AuthRequired {
		t.Fatal("Expected AuthRequired to be true")
	}
	if !updated.Cluster.NoAdvertise {
		t.Fatal("Expected NoAdvertise to be true")
	}
	if updated.PidFile != "/tmp/gnatsd.pid" {
		t.Fatalf("PidFile is incorrect.\nexpected: /tmp/gnatsd.pid\ngot: %s", updated.PidFile)
	}
	if updated.MaxControlLine != 512 {
		t.Fatalf("MaxControlLine is incorrect.\nexpected: 512\ngot: %d", updated.MaxControlLine)
	}
	if updated.PingInterval != 5*time.Second {
		t.Fatalf("PingInterval is incorrect.\nexpected 5s\ngot: %s", updated.PingInterval)
	}
	if updated.MaxPingsOut != 1 {
		t.Fatalf("MaxPingsOut is incorrect.\nexpected 1\ngot: %d", updated.MaxPingsOut)
	}
	if updated.WriteDeadline != 3*time.Second {
		t.Fatalf("WriteDeadline is incorrect.\nexpected 3s\ngot: %s", updated.WriteDeadline)
	}
	if updated.MaxPayload != 1024 {
		t.Fatalf("MaxPayload is incorrect.\nexpected 1024\ngot: %d", updated.MaxPayload)
	}

	if reloaded := server.ConfigTime(); !reloaded.After(loaded) {
		t.Fatalf("ConfigTime is incorrect.\nexpected greater than: %s\ngot: %s", loaded, reloaded)
	}
}

// Ensure Reload supports TLS config changes. Test this by starting a server
// with TLS enabled, connect to it to verify, reload config using a different
// key pair and client verification enabled, ensure reconnect fails, then
// ensure reconnect succeeds when the client provides a cert.
func TestConfigReloadRotateTLS(t *testing.T) {
	server, opts, config := runServerWithSymlinkConfig(t, "tmp.conf", "./configs/reload/tls_test.conf")
	defer os.Remove(config)
	defer server.Shutdown()

	// Ensure we can connect as a sanity check.
	addr := fmt.Sprintf("nats://%s:%d", opts.Host, server.Addr().(*net.TCPAddr).Port)

	nc, err := nats.Connect(addr, nats.Secure())
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer nc.Close()
	sub, err := nc.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Error subscribing: %v", err)
	}
	defer sub.Unsubscribe()

	// Rotate cert and enable client verification.
	createSymlink(t, config, "./configs/reload/tls_verify_test.conf")
	if err := server.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}

	// Ensure connecting fails.
	if _, err := nats.Connect(addr, nats.Secure()); err == nil {
		t.Fatal("Expected connect to fail")
	}

	// Ensure connecting succeeds when client presents cert.
	cert := nats.ClientCert("./configs/certs/cert.new.pem", "./configs/certs/key.new.pem")
	conn, err := nats.Connect(addr, cert, nats.RootCAs("./configs/certs/cert.new.pem"))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	conn.Close()

	// Ensure the original connection can still publish/receive.
	if err := nc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Error publishing: %v", err)
	}
	nc.Flush()
	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("Error receiving msg: %v", err)
	}
	if string(msg.Data) != "hello" {
		t.Fatalf("Msg is incorrect.\nexpected: %+v\ngot: %+v", []byte("hello"), msg.Data)
	}
}

// Ensure Reload supports enabling TLS. Test this by starting a server without
// TLS enabled, connect to it to verify, reload config with TLS enabled, ensure
// reconnect fails, then ensure reconnect succeeds when using secure.
func TestConfigReloadEnableTLS(t *testing.T) {
	server, opts, config := runServerWithSymlinkConfig(t, "tmp.conf", "./configs/reload/basic.conf")
	defer os.Remove(config)
	defer server.Shutdown()

	// Ensure we can connect as a sanity check.
	addr := fmt.Sprintf("nats://%s:%d", opts.Host, server.Addr().(*net.TCPAddr).Port)
	nc, err := nats.Connect(addr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	nc.Close()

	// Enable TLS.
	createSymlink(t, config, "./configs/reload/tls_test.conf")
	if err := server.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}

	// Ensure connecting fails.
	if _, err := nats.Connect(addr); err == nil {
		t.Fatal("Expected connect to fail")
	}

	// Ensure connecting succeeds when using secure.
	nc, err = nats.Connect(addr, nats.Secure())
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	nc.Close()
}

// Ensure Reload supports disabling TLS. Test this by starting a server with
// TLS enabled, connect to it to verify, reload config with TLS disabled,
// ensure reconnect fails, then ensure reconnect succeeds when connecting
// without secure.
func TestConfigReloadDisableTLS(t *testing.T) {
	server, opts, config := runServerWithSymlinkConfig(t, "tmp.conf", "./configs/reload/tls_test.conf")
	defer os.Remove(config)
	defer server.Shutdown()

	// Ensure we can connect as a sanity check.
	addr := fmt.Sprintf("nats://%s:%d", opts.Host, server.Addr().(*net.TCPAddr).Port)
	nc, err := nats.Connect(addr, nats.Secure())
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	nc.Close()

	// Disable TLS.
	createSymlink(t, config, "./configs/reload/basic.conf")
	if err := server.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}

	// Ensure connecting fails.
	if _, err := nats.Connect(addr, nats.Secure()); err == nil {
		t.Fatal("Expected connect to fail")
	}

	// Ensure connecting succeeds when not using secure.
	nc, err = nats.Connect(addr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	nc.Close()
}

// Ensure Reload supports single user authentication config changes. Test this
// by starting a server with authentication enabled, connect to it to verify,
// reload config using a different username/password, ensure reconnect fails,
// then ensure reconnect succeeds when using the correct credentials.
func TestConfigReloadRotateUserAuthentication(t *testing.T) {
	server, opts, config := runServerWithSymlinkConfig(t, "tmp.conf",
		"./configs/reload/single_user_authentication_1.conf")
	defer os.Remove(config)
	defer server.Shutdown()

	// Ensure we can connect as a sanity check.
	addr := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	nc, err := nats.Connect(addr, nats.UserInfo("tyler", "T0pS3cr3t"))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer nc.Close()
	disconnected := make(chan struct{})
	asyncErr := make(chan error)
	nc.SetErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
		asyncErr <- err
	})
	nc.SetDisconnectHandler(func(*nats.Conn) {
		disconnected <- struct{}{}
	})

	// Change user credentials.
	createSymlink(t, config, "./configs/reload/single_user_authentication_2.conf")
	if err := server.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}

	// Ensure connecting fails.
	if _, err := nats.Connect(addr, nats.UserInfo("tyler", "T0pS3cr3t")); err == nil {
		t.Fatal("Expected connect to fail")
	}

	// Ensure connecting succeeds when using new credentials.
	conn, err := nats.Connect(addr, nats.UserInfo("derek", "passw0rd"))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	conn.Close()

	// Ensure the previous connection received an authorization error.
	select {
	case err := <-asyncErr:
		if err != nats.ErrAuthorization {
			t.Fatalf("Expected ErrAuthorization, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Expected authorization error")
	}

	// Ensure the previous connection was disconnected.
	select {
	case <-disconnected:
	case <-time.After(2 * time.Second):
		t.Fatal("Expected connection to be disconnected")
	}
}

// Ensure Reload supports enabling single user authentication. Test this by
// starting a server with authentication disabled, connect to it to verify,
// reload config using with a username/password, ensure reconnect fails, then
// ensure reconnect succeeds when using the correct credentials.
func TestConfigReloadEnableUserAuthentication(t *testing.T) {
	server, opts, config := runServerWithSymlinkConfig(t, "tmp.conf", "./configs/reload/basic.conf")
	defer os.Remove(config)
	defer server.Shutdown()

	// Ensure we can connect as a sanity check.
	addr := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	nc, err := nats.Connect(addr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer nc.Close()
	disconnected := make(chan struct{})
	asyncErr := make(chan error)
	nc.SetErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
		asyncErr <- err
	})
	nc.SetDisconnectHandler(func(*nats.Conn) {
		disconnected <- struct{}{}
	})

	// Enable authentication.
	createSymlink(t, config, "./configs/reload/single_user_authentication_1.conf")
	if err := server.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}

	// Ensure connecting fails.
	if _, err := nats.Connect(addr); err == nil {
		t.Fatal("Expected connect to fail")
	}

	// Ensure connecting succeeds when using new credentials.
	conn, err := nats.Connect(addr, nats.UserInfo("tyler", "T0pS3cr3t"))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	conn.Close()

	// Ensure the previous connection received an authorization error.
	select {
	case err := <-asyncErr:
		if err != nats.ErrAuthorization {
			t.Fatalf("Expected ErrAuthorization, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Expected authorization error")
	}

	// Ensure the previous connection was disconnected.
	select {
	case <-disconnected:
	case <-time.After(2 * time.Second):
		t.Fatal("Expected connection to be disconnected")
	}
}

// Ensure Reload supports disabling single user authentication. Test this by
// starting a server with authentication enabled, connect to it to verify,
// reload config using with authentication disabled, then ensure connecting
// with no credentials succeeds.
func TestConfigReloadDisableUserAuthentication(t *testing.T) {
	server, opts, config := runServerWithSymlinkConfig(t, "tmp.conf",
		"./configs/reload/single_user_authentication_1.conf")
	defer os.Remove(config)
	defer server.Shutdown()

	// Ensure we can connect as a sanity check.
	addr := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	nc, err := nats.Connect(addr, nats.UserInfo("tyler", "T0pS3cr3t"))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer nc.Close()
	nc.SetErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
		t.Fatalf("Client received an unexpected error: %v", err)
	})

	// Disable authentication.
	createSymlink(t, config, "./configs/reload/basic.conf")
	if err := server.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}

	// Ensure connecting succeeds with no credentials.
	conn, err := nats.Connect(addr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	conn.Close()
}

// Ensure Reload supports token authentication config changes. Test this by
// starting a server with token authentication enabled, connect to it to
// verify, reload config using a different token, ensure reconnect fails, then
// ensure reconnect succeeds when using the correct token.
func TestConfigReloadRotateTokenAuthentication(t *testing.T) {
	server, opts, config := runServerWithSymlinkConfig(t, "tmp.conf", "./configs/reload/token_authentication_1.conf")
	defer os.Remove(config)
	defer server.Shutdown()

	disconnected := make(chan struct{})
	asyncErr := make(chan error)
	eh := func(nc *nats.Conn, sub *nats.Subscription, err error) { asyncErr <- err }
	dh := func(*nats.Conn) { disconnected <- struct{}{} }

	// Ensure we can connect as a sanity check.
	addr := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	nc, err := nats.Connect(addr, nats.Token("T0pS3cr3t"), nats.ErrorHandler(eh), nats.DisconnectHandler(dh))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer nc.Close()

	// Change authentication token.
	createSymlink(t, config, "./configs/reload/token_authentication_2.conf")
	if err := server.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}

	// Ensure connecting fails.
	if _, err := nats.Connect(addr, nats.Token("T0pS3cr3t")); err == nil {
		t.Fatal("Expected connect to fail")
	}

	// Ensure connecting succeeds when using new credentials.
	conn, err := nats.Connect(addr, nats.Token("passw0rd"))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	conn.Close()

	// Ensure the previous connection received an authorization error.
	select {
	case err := <-asyncErr:
		if err != nats.ErrAuthorization {
			t.Fatalf("Expected ErrAuthorization, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Expected authorization error")
	}

	// Ensure the previous connection was disconnected.
	select {
	case <-disconnected:
	case <-time.After(2 * time.Second):
		t.Fatal("Expected connection to be disconnected")
	}
}

// Ensure Reload supports enabling token authentication. Test this by starting
// a server with authentication disabled, connect to it to verify, reload
// config using with a token, ensure reconnect fails, then ensure reconnect
// succeeds when using the correct token.
func TestConfigReloadEnableTokenAuthentication(t *testing.T) {
	server, opts, config := runServerWithSymlinkConfig(t, "tmp.conf", "./configs/reload/basic.conf")
	defer os.Remove(config)
	defer server.Shutdown()

	// Ensure we can connect as a sanity check.
	addr := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	nc, err := nats.Connect(addr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer nc.Close()
	disconnected := make(chan struct{})
	asyncErr := make(chan error)
	nc.SetErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
		asyncErr <- err
	})
	nc.SetDisconnectHandler(func(*nats.Conn) {
		disconnected <- struct{}{}
	})

	// Enable authentication.
	createSymlink(t, config, "./configs/reload/token_authentication_1.conf")
	if err := server.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}

	// Ensure connecting fails.
	if _, err := nats.Connect(addr); err == nil {
		t.Fatal("Expected connect to fail")
	}

	// Ensure connecting succeeds when using new credentials.
	conn, err := nats.Connect(addr, nats.Token("T0pS3cr3t"))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	conn.Close()

	// Ensure the previous connection received an authorization error.
	select {
	case err := <-asyncErr:
		if err != nats.ErrAuthorization {
			t.Fatalf("Expected ErrAuthorization, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Expected authorization error")
	}

	// Ensure the previous connection was disconnected.
	select {
	case <-disconnected:
	case <-time.After(2 * time.Second):
		t.Fatal("Expected connection to be disconnected")
	}
}

// Ensure Reload supports disabling single token authentication. Test this by
// starting a server with authentication enabled, connect to it to verify,
// reload config using with authentication disabled, then ensure connecting
// with no token succeeds.
func TestConfigReloadDisableTokenAuthentication(t *testing.T) {
	server, opts, config := runServerWithSymlinkConfig(t, "tmp.conf", "./configs/reload/token_authentication_1.conf")
	defer os.Remove(config)
	defer server.Shutdown()

	// Ensure we can connect as a sanity check.
	addr := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	nc, err := nats.Connect(addr, nats.Token("T0pS3cr3t"))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer nc.Close()
	nc.SetErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
		t.Fatalf("Client received an unexpected error: %v", err)
	})

	// Disable authentication.
	createSymlink(t, config, "./configs/reload/basic.conf")
	if err := server.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}

	// Ensure connecting succeeds with no credentials.
	conn, err := nats.Connect(addr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	conn.Close()
}

// Ensure Reload supports users authentication config changes. Test this by
// starting a server with users authentication enabled, connect to it to
// verify, reload config using a different user, ensure reconnect fails, then
// ensure reconnect succeeds when using the correct credentials.
func TestConfigReloadRotateUsersAuthentication(t *testing.T) {
	server, opts, config := runServerWithSymlinkConfig(t, "tmp.conf", "./configs/reload/multiple_users_1.conf")
	defer os.Remove(config)
	defer server.Shutdown()

	// Ensure we can connect as a sanity check.
	addr := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	nc, err := nats.Connect(addr, nats.UserInfo("alice", "foo"))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer nc.Close()
	disconnected := make(chan struct{})
	asyncErr := make(chan error)
	nc.SetErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
		asyncErr <- err
	})
	nc.SetDisconnectHandler(func(*nats.Conn) {
		disconnected <- struct{}{}
	})

	// These credentials won't change.
	nc2, err := nats.Connect(addr, nats.UserInfo("bob", "bar"))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer nc2.Close()
	sub, err := nc2.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Error subscribing: %v", err)
	}
	defer sub.Unsubscribe()

	// Change users credentials.
	createSymlink(t, config, "./configs/reload/multiple_users_2.conf")
	if err := server.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}

	// Ensure connecting fails.
	if _, err := nats.Connect(addr, nats.UserInfo("alice", "foo")); err == nil {
		t.Fatal("Expected connect to fail")
	}

	// Ensure connecting succeeds when using new credentials.
	conn, err := nats.Connect(addr, nats.UserInfo("alice", "baz"))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	conn.Close()

	// Ensure the previous connection received an authorization error.
	select {
	case err := <-asyncErr:
		if err != nats.ErrAuthorization {
			t.Fatalf("Expected ErrAuthorization, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Expected authorization error")
	}

	// Ensure the previous connection was disconnected.
	select {
	case <-disconnected:
	case <-time.After(2 * time.Second):
		t.Fatal("Expected connection to be disconnected")
	}

	// Ensure the connection using unchanged credentials can still
	// publish/receive.
	if err := nc2.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Error publishing: %v", err)
	}
	nc2.Flush()
	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("Error receiving msg: %v", err)
	}
	if string(msg.Data) != "hello" {
		t.Fatalf("Msg is incorrect.\nexpected: %+v\ngot: %+v", []byte("hello"), msg.Data)
	}
}

// Ensure Reload supports enabling users authentication. Test this by starting
// a server with authentication disabled, connect to it to verify, reload
// config using with users, ensure reconnect fails, then ensure reconnect
// succeeds when using the correct credentials.
func TestConfigReloadEnableUsersAuthentication(t *testing.T) {
	server, opts, config := runServerWithSymlinkConfig(t, "tmp.conf", "./configs/reload/basic.conf")
	defer os.Remove(config)
	defer server.Shutdown()

	// Ensure we can connect as a sanity check.
	addr := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	nc, err := nats.Connect(addr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer nc.Close()
	disconnected := make(chan struct{})
	asyncErr := make(chan error)
	nc.SetErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
		asyncErr <- err
	})
	nc.SetDisconnectHandler(func(*nats.Conn) {
		disconnected <- struct{}{}
	})

	// Enable authentication.
	createSymlink(t, config, "./configs/reload/multiple_users_1.conf")
	if err := server.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}

	// Ensure connecting fails.
	if _, err := nats.Connect(addr); err == nil {
		t.Fatal("Expected connect to fail")
	}

	// Ensure connecting succeeds when using new credentials.
	conn, err := nats.Connect(addr, nats.UserInfo("alice", "foo"))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	conn.Close()

	// Ensure the previous connection received an authorization error.
	select {
	case err := <-asyncErr:
		if err != nats.ErrAuthorization {
			t.Fatalf("Expected ErrAuthorization, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Expected authorization error")
	}

	// Ensure the previous connection was disconnected.
	select {
	case <-disconnected:
	case <-time.After(5 * time.Second):
		t.Fatal("Expected connection to be disconnected")
	}
}

// Ensure Reload supports disabling users authentication. Test this by starting
// a server with authentication enabled, connect to it to verify,
// reload config using with authentication disabled, then ensure connecting
// with no credentials succeeds.
func TestConfigReloadDisableUsersAuthentication(t *testing.T) {
	server, opts, config := runServerWithSymlinkConfig(t, "tmp.conf", "./configs/reload/multiple_users_1.conf")
	defer os.Remove(config)
	defer server.Shutdown()

	// Ensure we can connect as a sanity check.
	addr := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	nc, err := nats.Connect(addr, nats.UserInfo("alice", "foo"))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer nc.Close()
	nc.SetErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
		t.Fatalf("Client received an unexpected error: %v", err)
	})

	// Disable authentication.
	createSymlink(t, config, "./configs/reload/basic.conf")
	if err := server.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}

	// Ensure connecting succeeds with no credentials.
	conn, err := nats.Connect(addr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	conn.Close()
}

// Ensure Reload supports changing permissions. Test this by starting a server
// with a user configured with certain permissions, test publish and subscribe,
// reload config with new permissions, ensure the previous subscription was
// closed and publishes fail, then ensure the new permissions succeed.
func TestConfigReloadChangePermissions(t *testing.T) {
	server, opts, config := runServerWithSymlinkConfig(t, "tmp.conf", "./configs/reload/authorization_1.conf")
	defer os.Remove(config)
	defer server.Shutdown()

	addr := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	nc, err := nats.Connect(addr, nats.UserInfo("bob", "bar"))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer nc.Close()
	asyncErr := make(chan error)
	nc.SetErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
		asyncErr <- err
	})
	// Ensure we can publish and receive messages as a sanity check.
	sub, err := nc.SubscribeSync("_INBOX.>")
	if err != nil {
		t.Fatalf("Error subscribing: %v", err)
	}
	nc.Flush()

	conn, err := nats.Connect(addr, nats.UserInfo("alice", "foo"))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer conn.Close()

	sub2, err := conn.SubscribeSync("req.foo")
	if err != nil {
		t.Fatalf("Error subscribing: %v", err)
	}
	if err := conn.Publish("_INBOX.foo", []byte("hello")); err != nil {
		t.Fatalf("Error publishing message: %v", err)
	}
	conn.Flush()

	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("Error receiving msg: %v", err)
	}
	if string(msg.Data) != "hello" {
		t.Fatalf("Msg is incorrect.\nexpected: %+v\ngot: %+v", []byte("hello"), msg.Data)
	}

	if err := nc.Publish("req.foo", []byte("world")); err != nil {
		t.Fatalf("Error publishing message: %v", err)
	}
	nc.Flush()

	msg, err = sub2.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("Error receiving msg: %v", err)
	}
	if string(msg.Data) != "world" {
		t.Fatalf("Msg is incorrect.\nexpected: %+v\ngot: %+v", []byte("world"), msg.Data)
	}

	// Change permissions.
	createSymlink(t, config, "./configs/reload/authorization_2.conf")
	if err := server.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}

	// Ensure we receive an error for the subscription that is no longer
	// authorized.
	select {
	case err := <-asyncErr:
		if !strings.Contains(err.Error(), "permissions violation for subscription to \"_inbox.>\"") {
			t.Fatalf("Expected permissions violation error, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Expected permissions violation error")
	}

	// Ensure we receive an error when publishing to req.foo and we no longer
	// receive messages on _INBOX.>.
	if err := nc.Publish("req.foo", []byte("hola")); err != nil {
		t.Fatalf("Error publishing message: %v", err)
	}
	nc.Flush()
	if err := conn.Publish("_INBOX.foo", []byte("mundo")); err != nil {
		t.Fatalf("Error publishing message: %v", err)
	}
	conn.Flush()

	select {
	case err := <-asyncErr:
		if !strings.Contains(err.Error(), "permissions violation for publish to \"req.foo\"") {
			t.Fatalf("Expected permissions violation error, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Expected permissions violation error")
	}

	queued, _, err := sub2.Pending()
	if err != nil {
		t.Fatalf("Failed to get pending messaged: %v", err)
	}
	if queued != 0 {
		t.Fatalf("Pending is incorrect.\nexpected: 0\ngot: %d", queued)
	}

	queued, _, err = sub.Pending()
	if err != nil {
		t.Fatalf("Failed to get pending messaged: %v", err)
	}
	if queued != 0 {
		t.Fatalf("Pending is incorrect.\nexpected: 0\ngot: %d", queued)
	}

	// Ensure we can publish to _INBOX.foo.bar and subscribe to _INBOX.foo.>.
	sub, err = nc.SubscribeSync("_INBOX.foo.>")
	if err != nil {
		t.Fatalf("Error subscribing: %v", err)
	}
	nc.Flush()
	if err := nc.Publish("_INBOX.foo.bar", []byte("testing")); err != nil {
		t.Fatalf("Error publishing message: %v", err)
	}
	nc.Flush()
	msg, err = sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("Error receiving msg: %v", err)
	}
	if string(msg.Data) != "testing" {
		t.Fatalf("Msg is incorrect.\nexpected: %+v\ngot: %+v", []byte("testing"), msg.Data)
	}

	select {
	case err := <-asyncErr:
		t.Fatalf("Received unexpected error: %v", err)
	default:
	}
}

// Ensure Reload returns an error when attempting to change cluster address
// host.
func TestConfigReloadClusterHostUnsupported(t *testing.T) {
	server, _, config := newServerWithSymlinkConfig(t, "tmp.conf", "./configs/reload/srv_a_1.conf")
	defer os.Remove(config)
	defer server.Shutdown()

	// Attempt to change cluster listen host.
	createSymlink(t, config, "./configs/reload/srv_c_1.conf")

	// This should fail because cluster address cannot be changed.
	if err := server.Reload(); err == nil {
		t.Fatal("Expected Reload to return an error")
	}
}

// Ensure Reload returns an error when attempting to change cluster address
// port.
func TestConfigReloadClusterPortUnsupported(t *testing.T) {
	server, _, config := newServerWithSymlinkConfig(t, "tmp.conf", "./configs/reload/srv_a_1.conf")
	defer os.Remove(config)
	defer server.Shutdown()

	// Attempt to change cluster listen port.
	createSymlink(t, config, "./configs/reload/srv_b_1.conf")

	// This should fail because cluster address cannot be changed.
	if err := server.Reload(); err == nil {
		t.Fatal("Expected Reload to return an error")
	}
}

// Ensure Reload supports enabling route authorization. Test this by starting
// two servers in a cluster without authorization, ensuring messages flow
// between them, then reloading with authorization and ensuring messages no
// longer flow until reloading with the correct credentials.
func TestConfigReloadEnableClusterAuthorization(t *testing.T) {
	srvb, srvbOpts, srvbConfig := runServerWithSymlinkConfig(t, "tmp_b.conf", "./configs/reload/srv_b_1.conf")
	defer os.Remove(srvbConfig)
	defer srvb.Shutdown()

	srva, srvaOpts, srvaConfig := runServerWithSymlinkConfig(t, "tmp_a.conf", "./configs/reload/srv_a_1.conf")
	defer os.Remove(srvaConfig)
	defer srva.Shutdown()

	checkClusterFormed(t, srva, srvb)

	srvaAddr := fmt.Sprintf("nats://%s:%d", srvaOpts.Host, srvaOpts.Port)
	srvaConn, err := nats.Connect(srvaAddr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer srvaConn.Close()
	sub, err := srvaConn.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Error subscribing: %v", err)
	}
	defer sub.Unsubscribe()
	if err := srvaConn.Flush(); err != nil {
		t.Fatalf("Error flushing: %v", err)
	}

	srvbAddr := fmt.Sprintf("nats://%s:%d", srvbOpts.Host, srvbOpts.Port)
	srvbConn, err := nats.Connect(srvbAddr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer srvbConn.Close()

	if numRoutes := srvb.NumRoutes(); numRoutes != 1 {
		t.Fatalf("Expected 1 route, got %d", numRoutes)
	}

	// Ensure messages flow through the cluster as a sanity check.
	if err := srvbConn.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Error publishing: %v", err)
	}
	srvbConn.Flush()
	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("Error receiving message: %v", err)
	}
	if string(msg.Data) != "hello" {
		t.Fatalf("Msg is incorrect.\nexpected: %+v\ngot: %+v", []byte("hello"), msg.Data)
	}

	// Enable route authorization.
	createSymlink(t, srvbConfig, "./configs/reload/srv_b_2.conf")
	if err := srvb.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}

	if numRoutes := srvb.NumRoutes(); numRoutes != 0 {
		t.Fatalf("Expected 0 routes, got %d", numRoutes)
	}

	// Ensure messages no longer flow through the cluster.
	for i := 0; i < 5; i++ {
		if err := srvbConn.Publish("foo", []byte("world")); err != nil {
			t.Fatalf("Error publishing: %v", err)
		}
		srvbConn.Flush()
	}
	if _, err := sub.NextMsg(50 * time.Millisecond); err != nats.ErrTimeout {
		t.Fatalf("Expected ErrTimeout, got %v", err)
	}

	// Reload Server A with correct route credentials.
	createSymlink(t, srvaConfig, "./configs/reload/srv_a_2.conf")
	defer os.Remove(srvaConfig)
	if err := srva.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}
	checkClusterFormed(t, srva, srvb)

	if numRoutes := srvb.NumRoutes(); numRoutes != 1 {
		t.Fatalf("Expected 1 route, got %d", numRoutes)
	}

	// Ensure messages flow through the cluster now.
	if err := srvbConn.Publish("foo", []byte("hola")); err != nil {
		t.Fatalf("Error publishing: %v", err)
	}
	srvbConn.Flush()
	msg, err = sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("Error receiving message: %v", err)
	}
	if string(msg.Data) != "hola" {
		t.Fatalf("Msg is incorrect.\nexpected: %+v\ngot: %+v", []byte("hola"), msg.Data)
	}
}

// Ensure Reload supports disabling route authorization. Test this by starting
// two servers in a cluster with authorization, ensuring messages flow
// between them, then reloading without authorization and ensuring messages
// still flow.
func TestConfigReloadDisableClusterAuthorization(t *testing.T) {
	srvb, srvbOpts, srvbConfig := runServerWithSymlinkConfig(t, "tmp_b.conf", "./configs/reload/srv_b_2.conf")
	defer os.Remove(srvbConfig)
	defer srvb.Shutdown()

	srva, srvaOpts, srvaConfig := runServerWithSymlinkConfig(t, "tmp_a.conf", "./configs/reload/srv_a_2.conf")
	defer os.Remove(srvaConfig)
	defer srva.Shutdown()

	checkClusterFormed(t, srva, srvb)

	srvaAddr := fmt.Sprintf("nats://%s:%d", srvaOpts.Host, srvaOpts.Port)
	srvaConn, err := nats.Connect(srvaAddr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer srvaConn.Close()

	sub, err := srvaConn.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Error subscribing: %v", err)
	}
	defer sub.Unsubscribe()
	if err := srvaConn.Flush(); err != nil {
		t.Fatalf("Error flushing: %v", err)
	}

	srvbAddr := fmt.Sprintf("nats://%s:%d", srvbOpts.Host, srvbOpts.Port)
	srvbConn, err := nats.Connect(srvbAddr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer srvbConn.Close()

	if numRoutes := srvb.NumRoutes(); numRoutes != 1 {
		t.Fatalf("Expected 1 route, got %d", numRoutes)
	}

	// Ensure messages flow through the cluster as a sanity check.
	if err := srvbConn.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Error publishing: %v", err)
	}
	srvbConn.Flush()
	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("Error receiving message: %v", err)
	}
	if string(msg.Data) != "hello" {
		t.Fatalf("Msg is incorrect.\nexpected: %+v\ngot: %+v", []byte("hello"), msg.Data)
	}

	// Disable route authorization.
	createSymlink(t, srvbConfig, "./configs/reload/srv_b_1.conf")
	if err := srvb.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}

	checkClusterFormed(t, srva, srvb)

	if numRoutes := srvb.NumRoutes(); numRoutes != 1 {
		t.Fatalf("Expected 1 route, got %d", numRoutes)
	}

	// Ensure messages still flow through the cluster.
	if err := srvbConn.Publish("foo", []byte("hola")); err != nil {
		t.Fatalf("Error publishing: %v", err)
	}
	srvbConn.Flush()
	msg, err = sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("Error receiving message: %v", err)
	}
	if string(msg.Data) != "hola" {
		t.Fatalf("Msg is incorrect.\nexpected: %+v\ngot: %+v", []byte("hola"), msg.Data)
	}
}

// Ensure Reload supports changing cluster routes. Test this by starting
// two servers in a cluster, ensuring messages flow between them, then
// reloading with a different route and ensuring messages flow through the new
// cluster.
func TestConfigReloadClusterRoutes(t *testing.T) {
	srvb, srvbOpts, srvbConfig := runServerWithSymlinkConfig(t, "tmp_b.conf", "./configs/reload/srv_b_1.conf")
	defer os.Remove(srvbConfig)
	defer srvb.Shutdown()

	srva, srvaOpts, srvaConfig := runServerWithSymlinkConfig(t, "tmp_a.conf", "./configs/reload/srv_a_1.conf")
	defer os.Remove(srvaConfig)
	defer srva.Shutdown()

	checkClusterFormed(t, srva, srvb)

	srvcOpts, err := ProcessConfigFile("./configs/reload/srv_c_1.conf")
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}
	srvcOpts.NoLog = true
	srvcOpts.NoSigs = true

	srvc := RunServer(srvcOpts)
	defer srvc.Shutdown()

	srvaAddr := fmt.Sprintf("nats://%s:%d", srvaOpts.Host, srvaOpts.Port)
	srvaConn, err := nats.Connect(srvaAddr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer srvaConn.Close()

	sub, err := srvaConn.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Error subscribing: %v", err)
	}
	defer sub.Unsubscribe()
	if err := srvaConn.Flush(); err != nil {
		t.Fatalf("Error flushing: %v", err)
	}

	srvbAddr := fmt.Sprintf("nats://%s:%d", srvbOpts.Host, srvbOpts.Port)
	srvbConn, err := nats.Connect(srvbAddr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer srvbConn.Close()

	if numRoutes := srvb.NumRoutes(); numRoutes != 1 {
		t.Fatalf("Expected 1 route, got %d", numRoutes)
	}

	// Ensure messages flow through the cluster as a sanity check.
	if err := srvbConn.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Error publishing: %v", err)
	}
	srvbConn.Flush()
	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("Error receiving message: %v", err)
	}
	if string(msg.Data) != "hello" {
		t.Fatalf("Msg is incorrect.\nexpected: %+v\ngot: %+v", []byte("hello"), msg.Data)
	}

	// Reload cluster routes.
	createSymlink(t, srvaConfig, "./configs/reload/srv_a_3.conf")
	if err := srva.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}

	// Kill old route server.
	srvbConn.Close()
	srvb.Shutdown()

	checkClusterFormed(t, srva, srvc)

	srvcAddr := fmt.Sprintf("nats://%s:%d", srvcOpts.Host, srvcOpts.Port)
	srvcConn, err := nats.Connect(srvcAddr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer srvcConn.Close()

	// Ensure messages flow through the new cluster.
	for i := 0; i < 5; i++ {
		if err := srvcConn.Publish("foo", []byte("hola")); err != nil {
			t.Fatalf("Error publishing: %v", err)
		}
		srvcConn.Flush()
	}
	msg, err = sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("Error receiving message: %v", err)
	}
	if string(msg.Data) != "hola" {
		t.Fatalf("Msg is incorrect.\nexpected: %+v\ngot: %+v", []byte("hola"), msg.Data)
	}
}

// Ensure Reload supports removing a solicited route. In this case from A->B
// Test this by starting two servers in a cluster, ensuring messages flow between them.
// Then stop server B, and have server A continue to try to connect. Reload A with a config
// that removes the route and make sure it does not connect to server B when its restarted.
func TestConfigReloadClusterRemoveSolicitedRoutes(t *testing.T) {
	srvb, srvbOpts := RunServerWithConfig("./configs/reload/srv_b_1.conf")
	defer srvb.Shutdown()

	srva, srvaOpts, srvaConfig := runServerWithSymlinkConfig(t, "tmp_a.conf", "./configs/reload/srv_a_1.conf")
	defer os.Remove(srvaConfig)
	defer srva.Shutdown()

	checkClusterFormed(t, srva, srvb)

	srvaAddr := fmt.Sprintf("nats://%s:%d", srvaOpts.Host, srvaOpts.Port)
	srvaConn, err := nats.Connect(srvaAddr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer srvaConn.Close()
	sub, err := srvaConn.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Error subscribing: %v", err)
	}
	defer sub.Unsubscribe()
	if err := srvaConn.Flush(); err != nil {
		t.Fatalf("Error flushing: %v", err)
	}

	srvbAddr := fmt.Sprintf("nats://%s:%d", srvbOpts.Host, srvbOpts.Port)
	srvbConn, err := nats.Connect(srvbAddr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer srvbConn.Close()

	if err := srvbConn.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Error publishing: %v", err)
	}
	srvbConn.Flush()
	msg, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("Error receiving message: %v", err)
	}
	if string(msg.Data) != "hello" {
		t.Fatalf("Msg is incorrect.\nexpected: %+v\ngot: %+v", []byte("hello"), msg.Data)
	}

	// Now stop server B.
	srvb.Shutdown()

	// Wait til route is dropped.
	checkNumRoutes(t, srva, 0)

	// Now change config for server A to not solicit a route to server B.
	createSymlink(t, srvaConfig, "./configs/reload/srv_a_4.conf")
	defer os.Remove(srvaConfig)
	if err := srva.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}

	// Restart server B.
	srvb, _ = RunServerWithConfig("./configs/reload/srv_b_1.conf")
	defer srvb.Shutdown()

	// We should not have a cluster formed here.
	numRoutes := 0
	deadline := time.Now().Add(2 * DEFAULT_ROUTE_RECONNECT)
	for time.Now().Before(deadline) {
		if numRoutes = srva.NumRoutes(); numRoutes != 0 {
			break
		} else {
			time.Sleep(100 * time.Millisecond)
		}
	}
	if numRoutes != 0 {
		t.Fatalf("Expected 0 routes for server A, got %d", numRoutes)
	}
}

func reloadUpdateConfig(t *testing.T, s *Server, conf, content string) {
	if err := ioutil.WriteFile(conf, []byte(content), 0666); err != nil {
		stackFatalf(t, "Error creating config file: %v", err)
	}
	if err := s.Reload(); err != nil {
		stackFatalf(t, "Error on reload: %v", err)
	}
}

func TestConfigReloadClusterAdvertise(t *testing.T) {
	conf := "routeadv.conf"
	if err := ioutil.WriteFile(conf, []byte(`
	listen: "0.0.0.0:-1"
	cluster: {
		listen: "0.0.0.0:-1"
	}
	`), 0666); err != nil {
		t.Fatalf("Error creating config file: %v", err)
	}
	defer os.Remove(conf)
	opts, err := ProcessConfigFile(conf)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}
	opts.NoLog = true
	opts.NoSigs = true
	s := RunServer(opts)
	defer s.Shutdown()

	orgClusterPort := s.ClusterAddr().Port

	verify := func(expectedHost string, expectedPort int, expectedIP string) {
		s.mu.Lock()
		routeInfo := s.routeInfo
		routeInfoJSON := Info{}
		err = json.Unmarshal(s.routeInfoJSON[5:], &routeInfoJSON) // Skip "INFO "
		s.mu.Unlock()
		if err != nil {
			t.Fatalf("Error on Unmarshal: %v", err)
		}
		if routeInfo.Host != expectedHost || routeInfo.Port != expectedPort || routeInfo.IP != expectedIP {
			t.Fatalf("Expected host/port/IP to be %s:%v, %q, got %s:%d, %q",
				expectedHost, expectedPort, expectedIP, routeInfo.Host, routeInfo.Port, routeInfo.IP)
		}
		// Check that server routeInfoJSON was updated too
		if !reflect.DeepEqual(routeInfo, routeInfoJSON) {
			t.Fatalf("Expected routeInfoJSON to be %+v, got %+v", routeInfo, routeInfoJSON)
		}
	}

	// Update config with cluster_advertise
	reloadUpdateConfig(t, s, conf, `
	listen: "0.0.0.0:-1"
	cluster: {
		listen: "0.0.0.0:-1"
		cluster_advertise: "me:1"
	}
	`)
	verify("me", 1, "nats-route://me:1/")

	// Update config with cluster_advertise (no port specified)
	reloadUpdateConfig(t, s, conf, `
	listen: "0.0.0.0:-1"
	cluster: {
		listen: "0.0.0.0:-1"
		cluster_advertise: "me"
	}
	`)
	verify("me", orgClusterPort, fmt.Sprintf("nats-route://me:%d/", orgClusterPort))

	// Update config with cluster_advertise (-1 port specified)
	reloadUpdateConfig(t, s, conf, `
	listen: "0.0.0.0:-1"
	cluster: {
		listen: "0.0.0.0:-1"
		cluster_advertise: "me:-1"
	}
	`)
	verify("me", orgClusterPort, fmt.Sprintf("nats-route://me:%d/", orgClusterPort))

	// Update to remove cluster_advertise
	reloadUpdateConfig(t, s, conf, `
	listen: "0.0.0.0:-1"
	cluster: {
		listen: "0.0.0.0:-1"
	}
	`)
	verify("0.0.0.0", orgClusterPort, "")
}

func TestConfigReloadClusterNoAdvertise(t *testing.T) {
	conf := "routeadv.conf"
	if err := ioutil.WriteFile(conf, []byte(`
	listen: "0.0.0.0:-1"
	client_advertise: "me:1"
	cluster: {
		listen: "0.0.0.0:-1"
	}
	`), 0666); err != nil {
		t.Fatalf("Error creating config file: %v", err)
	}
	defer os.Remove(conf)
	opts, err := ProcessConfigFile(conf)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}
	opts.NoLog = true
	opts.NoSigs = true
	s := RunServer(opts)
	defer s.Shutdown()

	s.mu.Lock()
	ccurls := s.routeInfo.ClientConnectURLs
	s.mu.Unlock()
	if len(ccurls) != 1 && ccurls[0] != "me:1" {
		t.Fatalf("Unexpected routeInfo.ClientConnectURLS: %v", ccurls)
	}

	// Update config with no_advertise
	reloadUpdateConfig(t, s, conf, `
	listen: "0.0.0.0:-1"
	client_advertise: "me:1"
	cluster: {
		listen: "0.0.0.0:-1"
		no_advertise: true
	}
	`)

	s.mu.Lock()
	ccurls = s.routeInfo.ClientConnectURLs
	s.mu.Unlock()
	if len(ccurls) != 0 {
		t.Fatalf("Unexpected routeInfo.ClientConnectURLS: %v", ccurls)
	}

	// Update config with cluster_advertise (no port specified)
	reloadUpdateConfig(t, s, conf, `
	listen: "0.0.0.0:-1"
	client_advertise: "me:1"
	cluster: {
		listen: "0.0.0.0:-1"
	}
	`)
	s.mu.Lock()
	ccurls = s.routeInfo.ClientConnectURLs
	s.mu.Unlock()
	if len(ccurls) != 1 && ccurls[0] != "me:1" {
		t.Fatalf("Unexpected routeInfo.ClientConnectURLS: %v", ccurls)
	}
}

func TestConfigReloadMaxSubsUnsupported(t *testing.T) {
	conf := "maxsubs.conf"
	if err := ioutil.WriteFile(conf, []byte(`max_subs: 1`), 0666); err != nil {
		t.Fatalf("Error creating config file: %v", err)
	}
	defer os.Remove(conf)
	opts, err := ProcessConfigFile(conf)
	if err != nil {
		stackFatalf(t, "Error processing config file: %v", err)
	}
	opts.NoLog = true
	opts.NoSigs = true
	s := RunServer(opts)
	defer s.Shutdown()

	if err := ioutil.WriteFile(conf, []byte(`max_subs: 10`), 0666); err != nil {
		t.Fatalf("Error writing config file: %v", err)
	}
	if err := s.Reload(); err == nil {
		t.Fatal("Expected Reload to return an error")
	}
}

func TestConfigReloadClientAdvertise(t *testing.T) {
	conf := "clientadv.conf"
	if err := ioutil.WriteFile(conf, []byte(`listen: "0.0.0.0:-1"`), 0666); err != nil {
		t.Fatalf("Error creating config file: %v", err)
	}
	defer os.Remove(conf)
	opts, err := ProcessConfigFile(conf)
	if err != nil {
		stackFatalf(t, "Error processing config file: %v", err)
	}
	opts.NoLog = true
	opts.NoSigs = true
	s := RunServer(opts)
	defer s.Shutdown()

	orgPort := s.Addr().(*net.TCPAddr).Port

	verify := func(expectedHost string, expectedPort int) {
		s.mu.Lock()
		info := s.info
		s.mu.Unlock()
		if info.Host != expectedHost || info.Port != expectedPort {
			stackFatalf(t, "Expected host/port to be %s:%d, got %s:%d",
				expectedHost, expectedPort, info.Host, info.Port)
		}
	}

	// Update config with ClientAdvertise (port specified)
	reloadUpdateConfig(t, s, conf, `
	listen: "0.0.0.0:-1"
	client_advertise: "me:1"
	`)
	verify("me", 1)

	// Update config with ClientAdvertise (no port specified)
	reloadUpdateConfig(t, s, conf, `
	listen: "0.0.0.0:-1"
	client_advertise: "me"
	`)
	verify("me", orgPort)

	// Update config with ClientAdvertise (-1 port specified)
	reloadUpdateConfig(t, s, conf, `
	listen: "0.0.0.0:-1"
	client_advertise: "me:-1"
	`)
	verify("me", orgPort)

	// Now remove ClientAdvertise to check that original values
	// are restored.
	reloadUpdateConfig(t, s, conf, `listen: "0.0.0.0:-1"`)
	verify("0.0.0.0", orgPort)
}

// Ensure Reload supports changing the max connections. Test this by starting a
// server with no max connections, connecting two clients, reloading with a
// max connections of one, and ensuring one client is disconnected.
func TestConfigReloadMaxConnections(t *testing.T) {
	server, opts, config := runServerWithSymlinkConfig(t, "tmp.conf", "./configs/reload/basic.conf")
	defer os.Remove(config)
	defer server.Shutdown()

	// Make two connections.
	addr := fmt.Sprintf("nats://%s:%d", opts.Host, server.Addr().(*net.TCPAddr).Port)
	nc1, err := nats.Connect(addr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer nc1.Close()
	closed := make(chan struct{}, 1)
	nc1.SetDisconnectHandler(func(*nats.Conn) {
		closed <- struct{}{}
	})
	nc2, err := nats.Connect(addr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer nc2.Close()
	nc2.SetDisconnectHandler(func(*nats.Conn) {
		closed <- struct{}{}
	})

	if numClients := server.NumClients(); numClients != 2 {
		t.Fatalf("Expected 2 clients, got %d", numClients)
	}

	// Set max connections to one.
	createSymlink(t, config, "./configs/reload/max_connections.conf")
	if err := server.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}

	// Ensure one connection was closed.
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Expected to be disconnected")
	}

	if numClients := server.NumClients(); numClients != 1 {
		t.Fatalf("Expected 1 client, got %d", numClients)
	}

	// Ensure new connections fail.
	_, err = nats.Connect(addr)
	if err == nil {
		t.Fatal("Expected error on connect")
	}
}

// Ensure reload supports changing the max payload size. Test this by starting
// a server with the default size limit, ensuring publishes work, reloading
// with a restrictive limit, and ensuring publishing an oversized message fails
// and disconnects the client.
func TestConfigReloadMaxPayload(t *testing.T) {
	server, opts, config := runServerWithSymlinkConfig(t, "tmp.conf", "./configs/reload/basic.conf")
	defer os.Remove(config)
	defer server.Shutdown()

	addr := fmt.Sprintf("nats://%s:%d", opts.Host, server.Addr().(*net.TCPAddr).Port)
	nc, err := nats.Connect(addr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer nc.Close()
	closed := make(chan struct{})
	nc.SetDisconnectHandler(func(*nats.Conn) {
		closed <- struct{}{}
	})

	conn, err := nats.Connect(addr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer conn.Close()
	sub, err := conn.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Error subscribing: %v", err)
	}
	conn.Flush()

	// Ensure we can publish as a sanity check.
	if err := nc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Error publishing: %v", err)
	}
	nc.Flush()
	_, err = sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("Error receiving message: %v", err)
	}

	// Set max payload to one.
	createSymlink(t, config, "./configs/reload/max_payload.conf")
	if err := server.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}

	// Ensure oversized messages don't get delivered and the client is
	// disconnected.
	if err := nc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Error publishing: %v", err)
	}
	nc.Flush()
	_, err = sub.NextMsg(20 * time.Millisecond)
	if err != nats.ErrTimeout {
		t.Fatalf("Expected ErrTimeout, got: %v", err)
	}

	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Expected to be disconnected")
	}
}

// Ensure reload supports rotating out files. Test this by starting
// a server with log and pid files, reloading new ones, then check that
// we can rename and delete the old log/pid files.
func TestConfigReloadRotateFiles(t *testing.T) {
	opts, config := newOptionsWithSymlinkConfig(t, "tmp.conf", "./configs/reload/file_rotate.conf")
	server := RunServer(opts)
	defer func() {
		os.Remove(config)
		os.Remove("log1.txt")
		os.Remove("gnatsd1.pid")
	}()
	defer server.Shutdown()

	// Configure the logger to enable actual logging
	server.ConfigureLogger()

	// Load a config that renames the files.
	createSymlink(t, config, "./configs/reload/file_rotate1.conf")
	if err := server.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}

	// Make sure the new files exist.
	if _, err := os.Stat("log1.txt"); os.IsNotExist(err) {
		t.Fatalf("Error reloading config, no new file: %v", err)
	}
	if _, err := os.Stat("gnatsd1.pid"); os.IsNotExist(err) {
		t.Fatalf("Error reloading config, no new file: %v", err)
	}

	// Check that old file can be renamed.
	if err := os.Rename("log.txt", "log_old.txt"); err != nil {
		t.Fatalf("Error reloading config, cannot rename file: %v", err)
	}
	if err := os.Rename("gnatsd.pid", "gnatsd_old.pid"); err != nil {
		t.Fatalf("Error reloading config, cannot rename file: %v", err)
	}

	// Check that the old files can be removed after rename.
	if err := os.Remove("log_old.txt"); err != nil {
		t.Fatalf("Error reloading config, cannot delete file: %v", err)
	}
	if err := os.Remove("gnatsd_old.pid"); err != nil {
		t.Fatalf("Error reloading config, cannot delete file: %v", err)
	}
}

func runServerWithSymlinkConfig(t *testing.T, symlinkName, configName string) (*Server, *Options, string) {
	t.Helper()
	opts, config := newOptionsWithSymlinkConfig(t, symlinkName, configName)
	opts.NoLog = true
	opts.NoSigs = true
	return RunServer(opts), opts, config
}

func newServerWithSymlinkConfig(t *testing.T, symlinkName, configName string) (*Server, *Options, string) {
	t.Helper()
	opts, config := newOptionsWithSymlinkConfig(t, symlinkName, configName)
	return New(opts), opts, config
}

func newOptionsWithSymlinkConfig(t *testing.T, symlinkName, configName string) (*Options, string) {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Error getting working directory: %v", err)
	}
	config := filepath.Join(dir, symlinkName)
	createSymlink(t, config, configName)
	opts, err := ProcessConfigFile(config)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}
	opts.NoSigs = true
	return opts, config
}

func createSymlink(t *testing.T, symlinkName, fileName string) {
	t.Helper()
	os.Remove(symlinkName)
	if err := os.Symlink(fileName, symlinkName); err != nil {
		t.Fatalf("Error creating symlink: %v (ensure you have privileges)", err)
	}
}
