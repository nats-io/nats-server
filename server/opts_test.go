// Copyright 2012-2013 Apcera Inc. All rights reserved.

package server

import (
	"reflect"
	"testing"
	"time"
)

func TestDefaultOptions(t *testing.T) {
	golden := &Options{
		Host:           DEFAULT_HOST,
		Port:           DEFAULT_PORT,
		MaxConn:        DEFAULT_MAX_CONNECTIONS,
		PingInterval:   DEFAULT_PING_INTERVAL,
		MaxPingsOut:    DEFAULT_PING_MAX_OUT,
		SslTimeout:     float64(SSL_TIMEOUT) / float64(time.Second),
		AuthTimeout:    float64(AUTH_TIMEOUT) / float64(time.Second),
		MaxControlLine: MAX_CONTROL_LINE_SIZE,
		MaxPayload:     MAX_PAYLOAD_SIZE,
	}

	opts := &Options{}
	processOptions(opts)

	if !reflect.DeepEqual(golden, opts) {
		t.Fatal("Default options are incorrect")
	}
}

func TestConfigFile(t *testing.T) {
	golden := &Options{
		Host:        "apcera.me",
		Port:        4242,
		Username:    "derek",
		Password:    "bella",
		AuthTimeout: 1.0 / float64(time.Second),
		Debug:       false,
		Trace:       true,
		Logtime:     false,
	}

	opts, err := ProcessConfigFile("./configs/test.conf")
	if err != nil {
		t.Fatalf("Received an error reading config file: %v\n", err)
	}

	if !reflect.DeepEqual(golden, opts) {
		t.Fatal("Options are incorrect from config file")
	}
}

func TestMergeOverrides(t *testing.T) {
	golden := &Options{
		Host:        "apcera.me",
		Port:        2222,
		Username:    "derek",
		Password:    "spooky",
		AuthTimeout: 1.0 / float64(time.Second),
		Debug:       true,
		Trace:       true,
		Logtime:     false,
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
	}
	merged := MergeOptions(fopts, opts)
	if !reflect.DeepEqual(golden, merged) {
		t.Fatal("Options are incorrect from config file")
	}
}
