// Copyright 2012 Apcera Inc. All rights reserved.

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
