// Copyright 2013 Apcera Inc. All rights reserved.

package server

import (
	"fmt"
	"os"
)

var usageStr = `
Server options:
    -a, --addr HOST                  Bind to HOST address (default: 0.0.0.0)
    -p, --port PORT                  Use PORT (default: 4222)
    -P, --pid FILE                   File to store PID
    -m, --http_port PORT             Use HTTP PORT
    -c, --config FILE                Configuration File

Logging options:
    -l, --log FILE                   File to redirect log output
    -T, --logtime                    Timestamp log entries (default: true)
    -D, --debug                      Enable debugging output
    -V, --trace                      Trace the raw protocol

Authorization options:
        --user user                  User required for connections
        --pass password              Password required for connections

Common options:
    -h, --help                       Show this message
    -v, --version                    Show version
`

// Usage will print out the flag options for the server.
func Usage() {
	fmt.Printf("%s\n", usageStr)
	os.Exit(0)
}
