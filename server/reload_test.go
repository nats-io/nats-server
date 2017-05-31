package server

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
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
		Host:           "localhost",
		Port:           4242,
		Username:       "derek",
		Password:       "bella",
		AuthTimeout:    1.0,
		Debug:          false,
		Trace:          true,
		Logtime:        false,
		HTTPPort:       8222,
		LogFile:        "/tmp/gnatsd.log",
		PidFile:        "/tmp/gnatsd.pid",
		ProfPort:       6543,
		Syslog:         true,
		RemoteSyslog:   "udp://foo.com:33",
		MaxControlLine: 2048,
		MaxPayload:     65536,
		MaxConn:        100,
		PingInterval:   60 * time.Second,
		MaxPingsOut:    3,
		WriteDeadline:  3 * time.Second,
	}
	processOptions(golden)

	if err := os.Symlink("./configs/reload/test.conf", config); err != nil {
		t.Fatalf("Error creating symlink: %v", err)
	}
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
		t.Fatalf("Error creating symlink: %v", err)
	}
	defer func() {
		if err := os.Remove(config); err != nil {
			t.Fatalf("Error deleting symlink: %v", err)
		}
	}()

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
		Host:           "localhost",
		Port:           4242,
		Username:       "derek",
		Password:       "bella",
		AuthTimeout:    1.0,
		Debug:          false,
		Trace:          true,
		Logtime:        false,
		HTTPPort:       8222,
		LogFile:        "/tmp/gnatsd.log",
		PidFile:        "/tmp/gnatsd.pid",
		ProfPort:       6543,
		Syslog:         true,
		RemoteSyslog:   "udp://foo.com:33",
		MaxControlLine: 2048,
		MaxPayload:     65536,
		MaxConn:        100,
		PingInterval:   60 * time.Second,
		MaxPingsOut:    3,
		WriteDeadline:  3 * time.Second,
	}
	processOptions(golden)

	if err := os.Symlink("./configs/reload/test.conf", config); err != nil {
		t.Fatalf("Error creating symlink: %v", err)
	}
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
	defer func() {
		if err := os.Remove(config); err != nil {
			t.Fatalf("Error deleting symlink: %v", err)
		}
	}()

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
		Host:           "localhost",
		Port:           4242,
		Username:       "derek",
		Password:       "bella",
		AuthTimeout:    1.0,
		Debug:          false,
		Trace:          true,
		Logtime:        false,
		HTTPPort:       8222,
		LogFile:        "/tmp/gnatsd.log",
		PidFile:        "/tmp/gnatsd.pid",
		ProfPort:       6543,
		Syslog:         true,
		RemoteSyslog:   "udp://foo.com:33",
		MaxControlLine: 2048,
		MaxPayload:     65536,
		MaxConn:        100,
		PingInterval:   60 * time.Second,
		MaxPingsOut:    3,
		WriteDeadline:  3 * time.Second,
	}
	processOptions(golden)

	if err := os.Symlink("./configs/reload/test.conf", config); err != nil {
		t.Fatalf("Error creating symlink: %v", err)
	}
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
	defer func() {
		if err := os.Remove(config); err != nil {
			t.Fatalf("Error deleting symlink: %v", err)
		}
	}()

	// Should change `trace` to false.
	if err := server.Reload(); err != nil {
		t.Fatalf("Error reloading config: %v", err)
	}

	// Ensure config changed.
	var updatedGolden *Options = &Options{}
	*updatedGolden = *golden
	updatedGolden.Trace = false
	if !reflect.DeepEqual(updatedGolden, server.getOpts()) {
		t.Fatalf("Options are incorrect.\nexpected: %+v\ngot: %+v",
			updatedGolden, opts)
	}
}
