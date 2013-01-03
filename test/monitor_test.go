// Copyright 2012 Apcera Inc. All rights reserved.

package test

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	"github.com/apcera/gnatsd/server"
)

const MONITOR_PORT=11422


// Make sure that we do not run the http server for monitoring unless asked.
func TestNoMonitorPort(t *testing.T) {
	s := startServer(t, MONITOR_PORT, "")
	defer s.stopServer()

	url := fmt.Sprintf("http://localhost:%d/", server.DEFAULT_HTTP_PORT)
	if resp, err := http.Get(url + "varz"); err == nil {
		t.Fatalf("Expected error: Got %+v\n", resp)
	}
	if resp, err := http.Get(url + "healthz"); err == nil {
		t.Fatalf("Expected error: Got %+v\n", resp)
	}
	if resp, err := http.Get(url + "connz"); err == nil {
		t.Fatalf("Expected error: Got %+v\n", resp)
	}
}

func TestVarz(t *testing.T) {
	args := fmt.Sprintf("-m %d", server.DEFAULT_HTTP_PORT)
	s := startServer(t, MONITOR_PORT, args)
	defer s.stopServer()

	url := fmt.Sprintf("http://localhost:%d/", server.DEFAULT_HTTP_PORT)
	resp, err := http.Get(url + "varz")
	if err != nil {
		t.Fatalf("Expected no error: Got %v\n", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("Expected a 200 response, got %d\n", resp.StatusCode)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Got an error reading the body: %v\n", err)
	}

	v := server.Varz{}
	if err := json.Unmarshal(body, &v); err != nil {
		t.Fatalf("Got an error unmarshalling the body: %v\n", err)
	}
	// Do some sanity checks on values
	if time.Since(v.Start) > 10*time.Second {
		t.Fatal("Expected start time to be within 10 seconds.")
	}
	if v.Mem > 8192 {
		t.Fatalf("Did not expect memory to be so high: %d\n", v.Mem)
	}
	// TODO(dlc): Add checks for connections, etc..
}
