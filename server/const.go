// Copyright 2012-2013 Apcera Inc. All rights reserved.

package server

import (
	"time"
)

const (
	VERSION = "go-0.3.2"

	DEFAULT_PORT = 4222
	DEFAULT_HOST = "0.0.0.0"

	// 1k should be plenty since payloads sans connect string are separate
	MAX_CONTROL_LINE_SIZE = 1024

	// Should be using something different if > 1MB payload
	MAX_PAYLOAD_SIZE = (1024 * 1024)

	// Maximum outbound size per client
	MAX_PENDING_SIZE = (10 * 1024 * 1024)

	// Maximum connections default
	DEFAULT_MAX_CONNECTIONS = (64 * 1024)

	// TLS/SSL wait time
	SSL_TIMEOUT = 500 * time.Millisecond

	// Authorization wait time
	AUTH_TIMEOUT = 2 * SSL_TIMEOUT

	// Ping intervals
	DEFAULT_PING_INTERVAL = 2 * time.Minute
	DEFAULT_PING_MAX_OUT  = 2

	// CRLF string
	CR_LF = "\r\n"

	// Write/Flush Deadlines
	DEFAULT_FLUSH_DEADLINE = 500 * time.Millisecond

	DEFAULT_HTTP_PORT = 8333

	// Accept sleep times on temporary errors
	ACCEPT_MIN_SLEEP = 10 * time.Millisecond
	ACCEPT_MAX_SLEEP = 1 * time.Second

	// Route solicitation intervals.
	DEFAULT_ROUTE_CONNECT = 1 * time.Second

	// Route dial timeout
	DEFAULT_ROUTE_DIAL = 1 * time.Second

	// Default size of proto to print on parse errors
	PROTO_SNIPPET_SIZE = 32

	// Maximum number of arguments from MSG proto
	MAX_MSG_ARGS = 4

	// Maximum number of arguments from PUB proto
	MAX_PUB_ARGS = 3
)
