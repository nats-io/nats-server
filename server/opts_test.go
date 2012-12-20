// Copyright 2012 Apcera Inc. All rights reserved.

package server

import (
	"testing"
	"reflect"
)

func TestDefaultOptions(t *testing.T) {
	golden := &Options{
		Host:         DEFAULT_HOST,
		Port:         DEFAULT_PORT,
		MaxConn:      DEFAULT_MAX_CONNECTIONS,
		PingInterval: DEFAULT_PING_INTERVAL,
		MaxPingsOut:  DEFAULT_PING_MAX_OUT,
	}

	opts := &Options{}
	processOptions(opts)

	if !reflect.DeepEqual(golden, opts) {
		t.Fatal("Default options are incorrect")
	}
}
