// Copyright 2017 Apcera Inc. All rights reserved.

package server

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
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
		MaxControlLine: 1024,
		MaxPayload:     1048576,
		MaxConn:        65536,
		PingInterval:   2 * time.Minute,
		MaxPingsOut:    2,
		WriteDeadline:  2 * time.Second,
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
		MaxControlLine: 1024,
		MaxPayload:     1048576,
		MaxConn:        65536,
		PingInterval:   2 * time.Minute,
		MaxPingsOut:    2,
		WriteDeadline:  2 * time.Second,
	}
	processOptions(golden)

	if err := os.Symlink("./configs/reload/test.conf", config); err != nil {
		t.Fatalf("Error creating symlink: %v", err)
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
		t.Fatalf("Error creating symlink: %v", err)
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
		MaxControlLine: 1024,
		MaxPayload:     1048576,
		MaxConn:        65536,
		PingInterval:   2 * time.Minute,
		MaxPingsOut:    2,
		WriteDeadline:  2 * time.Second,
	}
	processOptions(golden)

	if err := os.Symlink("./configs/reload/test.conf", config); err != nil {
		t.Fatalf("Error creating symlink: %v", err)
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
		t.Fatalf("Error creating symlink: %v", err)
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

	if err := os.Symlink("./configs/tls_test.conf", config); err != nil {
		t.Fatalf("Error creating symlink: %v", err)
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
	nc, err := nats.Connect(addr, nats.Secure())
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	nc.Close()

	// Rotate cert and enable client verification.
	if err := os.Remove(config); err != nil {
		t.Fatalf("Error deleting symlink: %v", err)
	}
	if err := os.Symlink("./configs/tls_verify_test.conf", config); err != nil {
		t.Fatalf("Error creating symlink: %v", err)
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
	nc, err = nats.Connect(addr, cert, nats.RootCAs("./configs/certs/cert.new.pem"))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	nc.Close()
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

	if err := os.Symlink("./configs/basic.conf", config); err != nil {
		t.Fatalf("Error creating symlink: %v", err)
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
	nc.Close()

	// Enable TLS.
	if err := os.Remove(config); err != nil {
		t.Fatalf("Error deleting symlink: %v", err)
	}
	if err := os.Symlink("./configs/tls_test.conf", config); err != nil {
		t.Fatalf("Error creating symlink: %v", err)
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

	if err := os.Symlink("./configs/tls_test.conf", config); err != nil {
		t.Fatalf("Error creating symlink: %v", err)
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
	nc, err := nats.Connect(addr, nats.Secure())
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	nc.Close()

	// Disable TLS.
	if err := os.Remove(config); err != nil {
		t.Fatalf("Error deleting symlink: %v", err)
	}
	if err := os.Symlink("./configs/basic.conf", config); err != nil {
		t.Fatalf("Error creating symlink: %v", err)
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
