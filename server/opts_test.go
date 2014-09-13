// Copyright 2013-2014 Apcera Inc. All rights reserved.

package server

import (
	"net/url"
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
		ProfPort:    6543,
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
		ProfPort:    6789,
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
		ProfPort: 6789,
	}
	merged := MergeOptions(fopts, opts)

	if !reflect.DeepEqual(golden, merged) {
		t.Fatalf("Options are incorrect.\nexpected: %+v\ngot: %+v",
			golden, merged)
	}
}

func TestRemoveSelfReference(t *testing.T) {
	url1, _ := url.Parse("nats-route://user:password@10.4.5.6:4223")
	url2, _ := url.Parse("nats-route://user:password@localhost:4223")
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
