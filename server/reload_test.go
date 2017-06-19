// Copyright 2017 Apcera Inc. All rights reserved.

package server

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/go-nats"
)

// Ensure Reload returns an error when attempting to reload a server that did
// not start with a config file.
func TestConfigReloadNoConfigFile(t *testing.T) {
	server := New(&Options{})
	if server.Reload() == nil {
		t.Fatal("Expected Reload to return an error")
	}
}

// Ensure Reload returns an error when attempting to change an option which
// does not support reloading.
func TestConfigReloadUnsupported(t *testing.T) {
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Error getting working directory: %v", err)
	}
	config := filepath.Join(dir, "tmp.conf")

	golden := &Options{
		ConfigFile:     config,
		Host:           "0.0.0.0",
		Port:           4222,
		AuthTimeout:    1.0,
		Debug:          false,
		Trace:          false,
		Logtime:        false,
		LogFile:        "/tmp/gnatsd.log",
		MaxControlLine: 1024,
		MaxPayload:     1048576,
		MaxConn:        65536,
		PingInterval:   2 * time.Minute,
		MaxPingsOut:    2,
		WriteDeadline:  2 * time.Second,
		Cluster: ClusterOpts{
			Host: "localhost",
			Port: -1,
		},
	}
	processOptions(golden)

	if err := os.Symlink("./configs/reload/test.conf", config); err != nil {
		t.Fatalf("Error creating symlink: %v (ensure you have privileges)", err)
	}
	defer os.Remove(config)
	opts, err := ProcessConfigFile(config)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}
	server := New(opts)

	if !reflect.DeepEqual(golden, server.getOpts()) {
		t.Fatalf("Options are incorrect.\nexpected: %+v\ngot: %+v",
			golden, opts)
	}

	// Change config file to bad config by replacing symlink.
	if err := os.Remove(config); err != nil {
		t.Fatalf("Error deleting symlink: %v", err)
	}
	if err := os.Symlink("./configs/reload/reload_unsupported.conf", config); err != nil {
		t.Fatalf("Error creating symlink: %v (ensure you have privileges)", err)
	}

	// This should fail because `debug` cannot be changed.
	if err := server.Reload(); err == nil {
		t.Fatal("Expected Reload to return an error")
	}

	// Ensure config didn't change.
	if !reflect.DeepEqual(golden, server.getOpts()) {
		t.Fatalf("Options are incorrect.\nexpected: %+v\ngot: %+v",
			golden, opts)
	}
}

// Ensure Reload returns an error when reloading from a bad config file.
func TestConfigReloadInvalidConfig(t *testing.T) {
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Error getting working directory: %v", err)
	}
	config := filepath.Join(dir, "tmp.conf")

	golden := &Options{
		ConfigFile:     config,
		Host:           "0.0.0.0",
		Port:           4222,
		AuthTimeout:    1.0,
		Debug:          false,
		Trace:          false,
		Logtime:        false,
		LogFile:        "/tmp/gnatsd.log",
		MaxControlLine: 1024,
		MaxPayload:     1048576,
		MaxConn:        65536,
		PingInterval:   2 * time.Minute,
		MaxPingsOut:    2,
		WriteDeadline:  2 * time.Second,
		Cluster: ClusterOpts{
			Host: "localhost",
			Port: -1,
		},
	}
	processOptions(golden)

	if err := os.Symlink("./configs/reload/test.conf", config); err != nil {
		t.Fatalf("Error creating symlink: %v (ensure you have privileges)", err)
	}
	defer os.Remove(config)
	opts, err := ProcessConfigFile(config)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}
	server := New(opts)

	if !reflect.DeepEqual(golden, server.getOpts()) {
		t.Fatalf("Options are incorrect.\nexpected: %+v\ngot: %+v",
			golden, opts)
	}

	// Change config file to bad config by replacing symlink.
	if err := os.Remove(config); err != nil {
		t.Fatalf("Error deleting symlink: %v", err)
	}
	if err := os.Symlink("./configs/reload/invalid.conf", config); err != nil {
		t.Fatalf("Error creating symlink: %v (ensure you have privileges)", err)
	}

	// This should fail because the new config should not parse.
	if err := server.Reload(); err == nil {
		t.Fatal("Expected Reload to return an error")
	}

	// Ensure config didn't change.
	if !reflect.DeepEqual(golden, server.getOpts()) {
		t.Fatalf("Options are incorrect.\nexpected: %+v\ngot: %+v",
			golden, opts)
	}
}

// Ensure Reload returns nil and the config is changed on success.
func TestConfigReload(t *testing.T) {
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Error getting working directory: %v", err)
	}
	config := filepath.Join(dir, "tmp.conf")

	golden := &Options{
		ConfigFile:     config,
		Host:           "0.0.0.0",
		Port:           4222,
		AuthTimeout:    1.0,
		Debug:          false,
		Trace:          false,
		Logtime:        false,
		LogFile:        "/tmp/gnatsd.log",
		MaxControlLine: 1024,
		MaxPayload:     1048576,
		MaxConn:        65536,
		PingInterval:   2 * time.Minute,
		MaxPingsOut:    2,
		WriteDeadline:  2 * time.Second,
		Cluster: ClusterOpts{
			Host: "localhost",
			Port: -1,
		},
	}
	processOptions(golden)

	if err := os.Symlink("./configs/reload/test.conf", config); err != nil {
		t.Fatalf("Error creating symlink: %v (ensure you have privileges)", err)
	}
	defer os.Remove(config)
	opts, err := ProcessConfigFile(config)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}
	server := New(opts)

	if !reflect.DeepEqual(golden, server.getOpts()) {
		t.Fatalf("Options are incorrect.\nexpected: %+v\ngot: %+v",
			golden, opts)
	}

	// Change config file to new config by replacing symlink.
	if err := os.Remove(config); err != nil {
		t.Fatalf("Error deleting symlink: %v", err)
	}
	if err := os.Symlink("./configs/reload/reload.conf", config); err != nil {
		t.Fatalf("Error creating symlink: %v (ensure you have privileges)", err)
	}

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
}

// Ensure Reload supports TLS config changes. Test this by starting a server
// with TLS enabled, connect to it to verify, reload config using a different
// key pair and client verification enabled, ensure reconnect fails, then
// ensure reconnect succeeds when the client provides a cert.
func TestConfigReloadRotateTLS(t *testing.T) {
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Error getting working directory: %v", err)
	}
	config := filepath.Join(dir, "tmp.conf")

	if err := os.Symlink("./configs/reload/tls_test.conf", config); err != nil {
		t.Fatalf("Error creating symlink: %v (ensure you have privileges)", err)
	}
	defer os.Remove(config)

	opts, err := ProcessConfigFile(config)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}

	server := RunServer(opts)
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
	if err := os.Remove(config); err != nil {
		t.Fatalf("Error deleting symlink: %v", err)
	}
	if err := os.Symlink("./configs/reload/tls_verify_test.conf", config); err != nil {
		t.Fatalf("Error creating symlink: %v (ensure you have privileges)", err)
	}
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
	msg, err := sub.NextMsg(time.Second)
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
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Error getting working directory: %v", err)
	}
	config := filepath.Join(dir, "tmp.conf")

	if err := os.Symlink("./configs/reload/basic.conf", config); err != nil {
		t.Fatalf("Error creating symlink: %v (ensure you have privileges)", err)
	}
	defer os.Remove(config)

	opts, err := ProcessConfigFile(config)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}

	server := RunServer(opts)
	defer server.Shutdown()

	// Ensure we can connect as a sanity check.
	addr := fmt.Sprintf("nats://%s:%d", opts.Host, server.Addr().(*net.TCPAddr).Port)
	nc, err := nats.Connect(addr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	nc.Close()

	// Enable TLS.
	if err := os.Remove(config); err != nil {
		t.Fatalf("Error deleting symlink: %v", err)
	}
	if err := os.Symlink("./configs/reload/tls_test.conf", config); err != nil {
		t.Fatalf("Error creating symlink: %v (ensure you have privileges)", err)
	}
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
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Error getting working directory: %v", err)
	}
	config := filepath.Join(dir, "tmp.conf")

	if err := os.Symlink("./configs/reload/tls_test.conf", config); err != nil {
		t.Fatalf("Error creating symlink: %v (ensure you have privileges)", err)
	}
	defer os.Remove(config)

	opts, err := ProcessConfigFile(config)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}

	server := RunServer(opts)
	defer server.Shutdown()

	// Ensure we can connect as a sanity check.
	addr := fmt.Sprintf("nats://%s:%d", opts.Host, server.Addr().(*net.TCPAddr).Port)
	nc, err := nats.Connect(addr, nats.Secure())
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	nc.Close()

	// Disable TLS.
	if err := os.Remove(config); err != nil {
		t.Fatalf("Error deleting symlink: %v", err)
	}
	if err := os.Symlink("./configs/reload/basic.conf", config); err != nil {
		t.Fatalf("Error creating symlink: %v (ensure you have privileges)", err)
	}
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
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Error getting working directory: %v", err)
	}
	config := filepath.Join(dir, "tmp.conf")

	if err := os.Symlink("./configs/reload/single_user_authentication_1.conf", config); err != nil {
		t.Fatalf("Error creating symlink: %v (ensure you have privileges)", err)
	}
	defer os.Remove(config)

	opts, err := ProcessConfigFile(config)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}

	server := RunServer(opts)
	defer server.Shutdown()

	// Ensure we can connect as a sanity check.
	addr := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	nc, err := nats.Connect(addr, nats.UserInfo("tyler", "T0pS3cr3t"))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	disconnected := make(chan struct{})
	asyncErr := make(chan error)
	nc.SetErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
		asyncErr <- err
	})
	nc.SetDisconnectHandler(func(*nats.Conn) {
		disconnected <- struct{}{}
	})

	// Change user credentials.
	if err := os.Remove(config); err != nil {
		t.Fatalf("Error deleting symlink: %v", err)
	}
	if err := os.Symlink("./configs/reload/single_user_authentication_2.conf", config); err != nil {
		t.Fatalf("Error creating symlink: %v (ensure you have privileges)", err)
	}
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

// Ensure Reload supports enabling single user authentication. Test this by
// starting a server with authentication disabled, connect to it to verify,
// reload config using with a username/password, ensure reconnect fails, then
// ensure reconnect succeeds when using the correct credentials.
func TestConfigReloadEnableUserAuthentication(t *testing.T) {
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Error getting working directory: %v", err)
	}
	config := filepath.Join(dir, "tmp.conf")

	if err := os.Symlink("./configs/reload/basic.conf", config); err != nil {
		t.Fatalf("Error creating symlink: %v (ensure you have privileges)", err)
	}
	defer os.Remove(config)

	opts, err := ProcessConfigFile(config)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}

	server := RunServer(opts)
	defer server.Shutdown()

	// Ensure we can connect as a sanity check.
	addr := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	nc, err := nats.Connect(addr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	disconnected := make(chan struct{})
	asyncErr := make(chan error)
	nc.SetErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
		asyncErr <- err
	})
	nc.SetDisconnectHandler(func(*nats.Conn) {
		disconnected <- struct{}{}
	})

	// Enable authentication.
	if err := os.Remove(config); err != nil {
		t.Fatalf("Error deleting symlink: %v", err)
	}
	if err := os.Symlink("./configs/reload/single_user_authentication_1.conf", config); err != nil {
		t.Fatalf("Error creating symlink: %v (ensure you have privileges)", err)
	}
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
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Error getting working directory: %v", err)
	}
	config := filepath.Join(dir, "tmp.conf")

	if err := os.Symlink("./configs/reload/single_user_authentication_1.conf", config); err != nil {
		t.Fatalf("Error creating symlink: %v (ensure you have privileges)", err)
	}
	defer os.Remove(config)

	opts, err := ProcessConfigFile(config)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}

	server := RunServer(opts)
	defer server.Shutdown()

	// Ensure we can connect as a sanity check.
	addr := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	nc, err := nats.Connect(addr, nats.UserInfo("tyler", "T0pS3cr3t"))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	nc.SetErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
		t.Fatalf("Client received an unexpected error: %v", err)
	})

	// Disable authentication.
	if err := os.Remove(config); err != nil {
		t.Fatalf("Error deleting symlink: %v", err)
	}
	if err := os.Symlink("./configs/reload/basic.conf", config); err != nil {
		t.Fatalf("Error creating symlink: %v (ensure you have privileges)", err)
	}
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
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Error getting working directory: %v", err)
	}
	config := filepath.Join(dir, "tmp.conf")

	if err := os.Symlink("./configs/reload/token_authentication_1.conf", config); err != nil {
		t.Fatalf("Error creating symlink: %v (ensure you have privileges)", err)
	}
	defer os.Remove(config)

	opts, err := ProcessConfigFile(config)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}

	server := RunServer(opts)
	defer server.Shutdown()

	// Ensure we can connect as a sanity check.
	addr := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	nc, err := nats.Connect(addr, nats.Token("T0pS3cr3t"))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	disconnected := make(chan struct{})
	asyncErr := make(chan error)
	nc.SetErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
		asyncErr <- err
	})
	nc.SetDisconnectHandler(func(*nats.Conn) {
		disconnected <- struct{}{}
	})

	// Change authentication token.
	if err := os.Remove(config); err != nil {
		t.Fatalf("Error deleting symlink: %v", err)
	}
	if err := os.Symlink("./configs/reload/token_authentication_2.conf", config); err != nil {
		t.Fatalf("Error creating symlink: %v (ensure you have privileges)", err)
	}
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
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Error getting working directory: %v", err)
	}
	config := filepath.Join(dir, "tmp.conf")

	if err := os.Symlink("./configs/reload/basic.conf", config); err != nil {
		t.Fatalf("Error creating symlink: %v (ensure you have privileges)", err)
	}
	defer os.Remove(config)

	opts, err := ProcessConfigFile(config)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}

	server := RunServer(opts)
	defer server.Shutdown()

	// Ensure we can connect as a sanity check.
	addr := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	nc, err := nats.Connect(addr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	disconnected := make(chan struct{})
	asyncErr := make(chan error)
	nc.SetErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
		asyncErr <- err
	})
	nc.SetDisconnectHandler(func(*nats.Conn) {
		disconnected <- struct{}{}
	})

	// Enable authentication.
	if err := os.Remove(config); err != nil {
		t.Fatalf("Error deleting symlink: %v", err)
	}
	if err := os.Symlink("./configs/reload/token_authentication_1.conf", config); err != nil {
		t.Fatalf("Error creating symlink: %v (ensure you have privileges)", err)
	}
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
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Error getting working directory: %v", err)
	}
	config := filepath.Join(dir, "tmp.conf")

	if err := os.Symlink("./configs/reload/token_authentication_1.conf", config); err != nil {
		t.Fatalf("Error creating symlink: %v (ensure you have privileges)", err)
	}
	defer os.Remove(config)

	opts, err := ProcessConfigFile(config)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}

	server := RunServer(opts)
	defer server.Shutdown()

	// Ensure we can connect as a sanity check.
	addr := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	nc, err := nats.Connect(addr, nats.Token("T0pS3cr3t"))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	nc.SetErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
		t.Fatalf("Client received an unexpected error: %v", err)
	})

	// Disable authentication.
	if err := os.Remove(config); err != nil {
		t.Fatalf("Error deleting symlink: %v", err)
	}
	if err := os.Symlink("./configs/reload/basic.conf", config); err != nil {
		t.Fatalf("Error creating symlink: %v (ensure you have privileges)", err)
	}
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
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Error getting working directory: %v", err)
	}
	config := filepath.Join(dir, "tmp.conf")

	if err := os.Symlink("./configs/reload/multiple_users_1.conf", config); err != nil {
		t.Fatalf("Error creating symlink: %v (ensure you have privileges)", err)
	}
	defer os.Remove(config)

	opts, err := ProcessConfigFile(config)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}

	server := RunServer(opts)
	defer server.Shutdown()

	// Ensure we can connect as a sanity check.
	addr := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	nc, err := nats.Connect(addr, nats.UserInfo("alice", "foo"))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	disconnected := make(chan struct{})
	asyncErr := make(chan error)
	nc.SetErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
		asyncErr <- err
	})
	nc.SetDisconnectHandler(func(*nats.Conn) {
		disconnected <- struct{}{}
	})

	// These credentials won't change.
	nc, err = nats.Connect(addr, nats.UserInfo("bob", "bar"))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer nc.Close()
	sub, err := nc.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Error subscribing: %v", err)
	}
	defer sub.Unsubscribe()

	// Change users credentials.
	if err := os.Remove(config); err != nil {
		t.Fatalf("Error deleting symlink: %v", err)
	}
	if err := os.Symlink("./configs/reload/multiple_users_2.conf", config); err != nil {
		t.Fatalf("Error creating symlink: %v (ensure you have privileges)", err)
	}
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
	if err := nc.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Error publishing: %v", err)
	}
	nc.Flush()
	msg, err := sub.NextMsg(time.Second)
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
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Error getting working directory: %v", err)
	}
	config := filepath.Join(dir, "tmp.conf")

	if err := os.Symlink("./configs/reload/basic.conf", config); err != nil {
		t.Fatalf("Error creating symlink: %v (ensure you have privileges)", err)
	}
	defer os.Remove(config)

	opts, err := ProcessConfigFile(config)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}

	server := RunServer(opts)
	defer server.Shutdown()

	// Ensure we can connect as a sanity check.
	addr := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	nc, err := nats.Connect(addr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	disconnected := make(chan struct{})
	asyncErr := make(chan error)
	nc.SetErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
		asyncErr <- err
	})
	nc.SetDisconnectHandler(func(*nats.Conn) {
		disconnected <- struct{}{}
	})

	// Enable authentication.
	if err := os.Remove(config); err != nil {
		t.Fatalf("Error deleting symlink: %v", err)
	}
	if err := os.Symlink("./configs/reload/multiple_users_1.conf", config); err != nil {
		t.Fatalf("Error creating symlink: %v (ensure you have privileges)", err)
	}
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

// Ensure Reload supports disabling users authentication. Test this by starting
// a server with authentication enabled, connect to it to verify,
// reload config using with authentication disabled, then ensure connecting
// with no credentials succeeds.
func TestConfigReloadDisableUsersAuthentication(t *testing.T) {
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Error getting working directory: %v", err)
	}
	config := filepath.Join(dir, "tmp.conf")

	if err := os.Symlink("./configs/reload/multiple_users_1.conf", config); err != nil {
		t.Fatalf("Error creating symlink: %v (ensure you have privileges)", err)
	}
	defer os.Remove(config)

	opts, err := ProcessConfigFile(config)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}

	server := RunServer(opts)
	defer server.Shutdown()

	// Ensure we can connect as a sanity check.
	addr := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	nc, err := nats.Connect(addr, nats.UserInfo("alice", "foo"))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	nc.SetErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
		t.Fatalf("Client received an unexpected error: %v", err)
	})

	// Disable authentication.
	if err := os.Remove(config); err != nil {
		t.Fatalf("Error deleting symlink: %v", err)
	}
	if err := os.Symlink("./configs/reload/basic.conf", config); err != nil {
		t.Fatalf("Error creating symlink: %v (ensure you have privileges)", err)
	}
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
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Error getting working directory: %v", err)
	}
	config := filepath.Join(dir, "tmp.conf")

	if err := os.Symlink("./configs/reload/authorization_1.conf", config); err != nil {
		t.Fatalf("Error creating symlink: %v (ensure you have privileges)", err)
	}
	defer os.Remove(config)

	opts, err := ProcessConfigFile(config)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}

	server := RunServer(opts)
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

	msg, err := sub.NextMsg(time.Second)
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

	msg, err = sub2.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Error receiving msg: %v", err)
	}
	if string(msg.Data) != "world" {
		t.Fatalf("Msg is incorrect.\nexpected: %+v\ngot: %+v", []byte("world"), msg.Data)
	}

	// Change permissions.
	if err := os.Remove(config); err != nil {
		t.Fatalf("Error deleting symlink: %v", err)
	}
	if err := os.Symlink("./configs/reload/authorization_2.conf", config); err != nil {
		t.Fatalf("Error creating symlink: %v (ensure you have privileges)", err)
	}
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
	case <-time.After(2 * time.Second):
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
	case <-time.After(2 * time.Second):
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
	msg, err = sub.NextMsg(time.Second)
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
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Error getting working directory: %v", err)
	}
	config := filepath.Join(dir, "tmp.conf")

	if err := os.Symlink("./configs/reload/srv_a_1.conf", config); err != nil {
		t.Fatalf("Error creating symlink: %v (ensure you have privileges)", err)
	}
	defer os.Remove(config)
	opts, err := ProcessConfigFile(config)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}
	server := New(opts)

	// Attempt to change cluster listen host.
	if err := os.Remove(config); err != nil {
		t.Fatalf("Error deleting symlink: %v", err)
	}
	if err := os.Symlink("./configs/reload/srv_c_1.conf", config); err != nil {
		t.Fatalf("Error creating symlink: %v (ensure you have privileges)", err)
	}

	// This should fail because cluster address cannot be changed.
	if err := server.Reload(); err == nil {
		t.Fatal("Expected Reload to return an error")
	}
}

// Ensure Reload returns an error when attempting to change cluster address
// port.
func TestConfigReloadClusterPortUnsupported(t *testing.T) {
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Error getting working directory: %v", err)
	}
	config := filepath.Join(dir, "tmp.conf")

	if err := os.Symlink("./configs/reload/srv_a_1.conf", config); err != nil {
		t.Fatalf("Error creating symlink: %v (ensure you have privileges)", err)
	}
	defer os.Remove(config)
	opts, err := ProcessConfigFile(config)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}
	server := New(opts)

	// Attempt to change cluster listen port.
	if err := os.Remove(config); err != nil {
		t.Fatalf("Error deleting symlink: %v", err)
	}
	if err := os.Symlink("./configs/reload/srv_b_1.conf", config); err != nil {
		t.Fatalf("Error creating symlink: %v (ensure you have privileges)", err)
	}

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
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Error getting working directory: %v", err)
	}
	srvbConfig := filepath.Join(dir, "tmp_b.conf")

	if err := os.Symlink("./configs/reload/srv_b_1.conf", srvbConfig); err != nil {
		t.Fatalf("Error creating symlink: %v (ensure you have privileges)", err)
	}
	defer os.Remove(srvbConfig)

	srvbOpts, err := ProcessConfigFile(srvbConfig)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}
	srvbOpts.NoLog = true

	srvb := RunServer(srvbOpts)
	defer srvb.Shutdown()

	srvaConfig := filepath.Join(dir, "tmp_a.conf")
	if err := os.Symlink("./configs/reload/srv_a_1.conf", srvaConfig); err != nil {
		t.Fatalf("Error creating symlink: %v (ensure you have privileges)", err)
	}
	defer os.Remove(srvaConfig)

	srvaOpts, err := ProcessConfigFile(srvaConfig)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}
	srvaOpts.NoLog = true

	srva := RunServer(srvaOpts)
	defer srva.Shutdown()

	srvaAddr := fmt.Sprintf("nats://%s:%d", srvaOpts.Host, srvaOpts.Port)
	srvaConn, err := nats.Connect(srvaAddr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}

	sub, err := srvaConn.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Error subscribing: %v", err)
	}
	defer sub.Unsubscribe()

	srvbAddr := fmt.Sprintf("nats://%s:%d", srvbOpts.Host, srvbOpts.Port)
	srvbConn, err := nats.Connect(srvbAddr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}

	if numRoutes := srvb.NumRoutes(); numRoutes != 1 {
		t.Fatalf("Expected 1 route, got %d", numRoutes)
	}

	// Ensure messages flow through the cluster as a sanity check.
	if err := srvbConn.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Error publishing: %v", err)
	}
	srvbConn.Flush()
	msg, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Error receiving message: %v", err)
	}
	if string(msg.Data) != "hello" {
		t.Fatalf("Msg is incorrect.\nexpected: %+v\ngot: %+v", []byte("hello"), msg.Data)
	}

	// Enable route authorization.
	if err := os.Remove(srvbConfig); err != nil {
		t.Fatalf("Error deleting symlink: %v", err)
	}
	if err := os.Symlink("./configs/reload/srv_b_2.conf", srvbConfig); err != nil {
		t.Fatalf("Error creating symlink: %v (ensure you have privileges)", err)
	}
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
	if err := os.Remove(srvaConfig); err != nil {
		t.Fatalf("Error deleting symlink: %v", err)
	}
	if err := os.Symlink("./configs/reload/srv_a_2.conf", srvaConfig); err != nil {
		t.Fatalf("Error creating symlink: %v (ensure you have privileges)", err)
	}
	defer os.Remove(srvaConfig)
	if err := srva.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}
	time.Sleep(10 * time.Millisecond)

	if numRoutes := srvb.NumRoutes(); numRoutes != 1 {
		t.Fatalf("Expected 1 route, got %d", numRoutes)
	}

	// Ensure messages flow through the cluster now.
	if err := srvbConn.Publish("foo", []byte("hola")); err != nil {
		t.Fatalf("Error publishing: %v", err)
	}
	srvbConn.Flush()
	msg, err = sub.NextMsg(time.Second)
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
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Error getting working directory: %v", err)
	}
	srvbConfig := filepath.Join(dir, "tmp_b.conf")

	if err := os.Symlink("./configs/reload/srv_b_2.conf", srvbConfig); err != nil {
		t.Fatalf("Error creating symlink: %v (ensure you have privileges)", err)
	}
	defer os.Remove(srvbConfig)

	srvbOpts, err := ProcessConfigFile(srvbConfig)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}
	srvbOpts.NoLog = true

	srvb := RunServer(srvbOpts)
	defer srvb.Shutdown()

	srvaConfig := filepath.Join(dir, "tmp_a.conf")
	if err := os.Symlink("./configs/reload/srv_a_2.conf", srvaConfig); err != nil {
		t.Fatalf("Error creating symlink: %v (ensure you have privileges)", err)
	}
	defer os.Remove(srvaConfig)

	srvaOpts, err := ProcessConfigFile(srvaConfig)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}
	srvaOpts.NoLog = true

	srva := RunServer(srvaOpts)
	defer srva.Shutdown()

	srvaAddr := fmt.Sprintf("nats://%s:%d", srvaOpts.Host, srvaOpts.Port)
	srvaConn, err := nats.Connect(srvaAddr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}

	sub, err := srvaConn.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Error subscribing: %v", err)
	}
	defer sub.Unsubscribe()

	srvbAddr := fmt.Sprintf("nats://%s:%d", srvbOpts.Host, srvbOpts.Port)
	srvbConn, err := nats.Connect(srvbAddr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}

	if numRoutes := srvb.NumRoutes(); numRoutes != 1 {
		t.Fatalf("Expected 1 route, got %d", numRoutes)
	}

	// Ensure messages flow through the cluster as a sanity check.
	if err := srvbConn.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Error publishing: %v", err)
	}
	srvbConn.Flush()
	msg, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Error receiving message: %v", err)
	}
	if string(msg.Data) != "hello" {
		t.Fatalf("Msg is incorrect.\nexpected: %+v\ngot: %+v", []byte("hello"), msg.Data)
	}

	// Disable route authorization.
	if err := os.Remove(srvbConfig); err != nil {
		t.Fatalf("Error deleting symlink: %v", err)
	}
	if err := os.Symlink("./configs/reload/srv_b_1.conf", srvbConfig); err != nil {
		t.Fatalf("Error creating symlink: %v (ensure you have privileges)", err)
	}
	if err := srvb.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}

	if numRoutes := srvb.NumRoutes(); numRoutes != 1 {
		t.Fatalf("Expected 1 route, got %d", numRoutes)
	}

	// Ensure messages still flow through the cluster.
	if err := srvbConn.Publish("foo", []byte("hola")); err != nil {
		t.Fatalf("Error publishing: %v", err)
	}
	srvbConn.Flush()
	msg, err = sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Error receiving message: %v", err)
	}
	if string(msg.Data) != "hola" {
		t.Fatalf("Msg is incorrect.\nexpected: %+v\ngot: %+v", []byte("hola"), msg.Data)
	}
}

// Ensure Reload supports changing cluster routes. Test this by starting
// two servers in a cluster, ensuring messages flow between them, then
// reloading with a different route and ensuring messages only flow through the
// new cluster.
func TestConfigReloadClusterRoutes(t *testing.T) {
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Error getting working directory: %v", err)
	}
	srvbConfig := filepath.Join(dir, "tmp_b.conf")

	if err := os.Symlink("./configs/reload/srv_b_1.conf", srvbConfig); err != nil {
		t.Fatalf("Error creating symlink: %v (ensure you have privileges)", err)
	}
	defer os.Remove(srvbConfig)

	srvbOpts, err := ProcessConfigFile(srvbConfig)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}
	srvbOpts.NoLog = true

	srvb := RunServer(srvbOpts)
	defer srvb.Shutdown()

	srvaConfig := filepath.Join(dir, "tmp_a.conf")
	if err := os.Symlink("./configs/reload/srv_a_1.conf", srvaConfig); err != nil {
		t.Fatalf("Error creating symlink: %v (ensure you have privileges)", err)
	}
	defer os.Remove(srvaConfig)

	srvaOpts, err := ProcessConfigFile(srvaConfig)
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}
	srvaOpts.NoLog = true

	srva := RunServer(srvaOpts)
	defer srva.Shutdown()

	srvcOpts, err := ProcessConfigFile("./configs/reload/srv_c_1.conf")
	if err != nil {
		t.Fatalf("Error processing config file: %v", err)
	}
	srvcOpts.NoLog = true

	srvc := RunServer(srvcOpts)
	defer srvc.Shutdown()

	srvaAddr := fmt.Sprintf("nats://%s:%d", srvaOpts.Host, srvaOpts.Port)
	srvaConn, err := nats.Connect(srvaAddr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}

	sub, err := srvaConn.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Error subscribing: %v", err)
	}
	defer sub.Unsubscribe()

	srvbAddr := fmt.Sprintf("nats://%s:%d", srvbOpts.Host, srvbOpts.Port)
	srvbConn, err := nats.Connect(srvbAddr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}

	if numRoutes := srvb.NumRoutes(); numRoutes != 1 {
		t.Fatalf("Expected 1 route, got %d", numRoutes)
	}

	// Ensure messages flow through the cluster as a sanity check.
	if err := srvbConn.Publish("foo", []byte("hello")); err != nil {
		t.Fatalf("Error publishing: %v", err)
	}
	srvbConn.Flush()
	msg, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Error receiving message: %v", err)
	}
	if string(msg.Data) != "hello" {
		t.Fatalf("Msg is incorrect.\nexpected: %+v\ngot: %+v", []byte("hello"), msg.Data)
	}

	// Reload cluster routes.
	if err := os.Remove(srvaConfig); err != nil {
		t.Fatalf("Error deleting symlink: %v", err)
	}
	if err := os.Symlink("./configs/reload/srv_a_3.conf", srvaConfig); err != nil {
		t.Fatalf("Error creating symlink: %v (ensure you have privileges)", err)
	}
	if err := srva.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}

	srvcAddr := fmt.Sprintf("nats://%s:%d", srvcOpts.Host, srvcOpts.Port)
	srvcConn, err := nats.Connect(srvcAddr)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer srvcConn.Close()

	// Ensure messages flow through the new cluster.
	if err := srvcConn.Publish("foo", []byte("hola")); err != nil {
		t.Fatalf("Error publishing: %v", err)
	}
	srvbConn.Flush()
	msg, err = sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Error receiving message: %v", err)
	}
	if string(msg.Data) != "hola" {
		t.Fatalf("Msg is incorrect.\nexpected: %+v\ngot: %+v", []byte("hola"), msg.Data)
	}

	// Ensure messages no longer flow through the old cluster.
	for i := 0; i < 5; i++ {
		if err := srvbConn.Publish("foo", []byte("world")); err != nil {
			t.Fatalf("Error publishing: %v", err)
		}
		srvbConn.Flush()
	}
	if msg, err := sub.NextMsg(50 * time.Millisecond); err != nats.ErrTimeout {
		t.Fatalf("Expected ErrTimeout, got %v %v", err, string(msg.Data))
	}
}
