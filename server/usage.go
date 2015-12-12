// Copyright 2013-2015 Apcera Inc. All rights reserved.

package server

import (
	"fmt"
	"os"
)

var usageStr = `
Server Options:
    -a, --addr HOST                  Bind to HOST address (default: 0.0.0.0)
    -p, --port PORT                  Use PORT for clients (default: 4222)
    -P, --pid FILE                   File to store PID
    -m, --http_port PORT             Use HTTP PORT for monitoring
    -ms,--https_port PORT            Use HTTPS PORT for monitoring
    -c, --config FILE                Configuration File

Logging Options:
    -l, --log FILE                   File to redirect log output
    -T, --logtime                    Timestamp log entries (default: true)
    -s, --syslog                     Enable syslog as log method
    -r, --remote_syslog              Syslog server addr (udp://localhost:514)
    -D, --debug                      Enable debugging output
    -V, --trace                      Trace the raw protocol
    -DV                              Debug and Trace

Authorization Options:
        --user user                  User required for connections
        --pass password              Password required for connections

TLS Options:
        --tls                        Enable TLS, do not verify clients (default: false)
        --tlscert FILE               Server certificate file
        --tlskey FILE                Private key for server certificate
        --tlsverify                  Enable TLS, very client certificates
        --tlscacert FILE             Client certificate CA for verification

Cluster Options:
        --routes [rurl-1, rurl-2]    Routes to solicit and connect
        --cluster [cluster url]      Cluster URL for solicited routes

Common Options:
    -h, --help                       Show this message
    -v, --version                    Show version
        --help_tls                   TLS help.
`

// Usage will print out the flag options for the server.
func Usage() {
	fmt.Printf("%s\n", usageStr)
	os.Exit(0)
}
