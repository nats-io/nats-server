// Copyright 2012 Apcera Inc. All rights reserved.

package server

import (
	"time"
)

const (
	VERSION = "go 0.2.1.alpha.2"

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
	SSL_TIMEOUT = 250 * time.Millisecond

	// Authorization wait time
	AUTH_TIMEOUT = 2 * SSL_TIMEOUT

	// Ping intervals
	DEFAULT_PING_INTERVAL = 2 * time.Minute
	DEFAULT_PING_MAX      = 2

	CR_LF = "\r\n"
)
