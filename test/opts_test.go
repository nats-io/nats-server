// Copyright 2015 Apcera Inc. All rights reserved.

package test

import (
	"testing"
)

func TestServerConfig(t *testing.T) {
	srv, opts := RunServerWithConfig("./ProcessConfigFile("./etcoverride.conf")
	defer srv.Shutdown()

	c := createClientConn(t, opts.Host, opts.Port)
	defer c.Close()

	sinfo := checkInfoMsg(t, c)
	if sinfo.MaxPayload != opts.MaxPayload {
		t.Fatalf("Expected max_payload from server, got %d vs %d",
			opts.MaxPayload, sinfo.MaxPayload)
	}
}
