// Copyright 2013-2014 Apcera Inc. All rights reserved.

package server

import (
	"reflect"
	"testing"
	"time"
)

func TestDefaultOptions(t *testing.T) {
	golden := &Options{
		Host:               DEFAULT_HOST,
		Port:               DEFAULT_PORT,
		MaxConn:            DEFAULT_MAX_CONNECTIONS,
		PingInterval:       DEFAULT_PING_INTERVAL,
		MaxPingsOut:        DEFAULT_PING_MAX_OUT,
		SslTimeout:         float64(SSL_TIMEOUT) / float64(time.Second),
		AuthTimeout:        float64(AUTH_TIMEOUT) / float64(time.Second),
		MaxControlLine:     MAX_CONTROL_LINE_SIZE,
		MaxPayload:         MAX_PAYLOAD_SIZE,
		ClusterAuthTimeout: float64(AUTH_TIMEOUT) / float64(time.Second),
	}

	opts := &Options{}
	processOptions(opts)

	if !reflect.DeepEqual(golden, opts) {
		t.Fatalf("Default Options are incorrect.\nexpected: %+v\ngot: %+v",
			golden, opts)
	}
}

func TestConfigFile(t *testing.T) {
	golden := &Options{
		Host:        "apcera.me",
		Port:        4242,
		Username:    "derek",
		Password:    "bella",
		AuthTimeout: 1.0,
		Debug:       false,
		Trace:       true,
		Logtime:     false,
		HTTPPort:    8222,
		LogFile:     "/tmp/gnatsd.log",
		PidFile:     "/tmp/gnatsd.pid",
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

func TestMergeOverrides(t *testing.T) {
	golden := &Options{
		Host:        "apcera.me",
		Port:        2222,
		Username:    "derek",
		Password:    "spooky",
		AuthTimeout: 1.0,
		Debug:       true,
		Trace:       true,
		Logtime:     false,
		HTTPPort:    DEFAULT_HTTP_PORT,
		LogFile:     "/tmp/gnatsd.log",
		PidFile:     "/tmp/gnatsd.pid",
	}
	fopts, err := ProcessConfigFile("./configs/test.conf")
	if err != nil {
		t.Fatalf("Received an error reading config file: %v\n", err)
	}

	// Overrides via flags
	opts := &Options{
		Port:     2222,
		Password: "spooky",
		Debug:    true,
		HTTPPort: DEFAULT_HTTP_PORT,
	}
	merged := MergeOptions(fopts, opts)

	if !reflect.DeepEqual(golden, merged) {
		t.Fatalf("Options are incorrect.\nexpected: %+v\ngot: %+v",
			golden, merged)
	}
}
