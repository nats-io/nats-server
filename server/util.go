// Copyright 2012-2017 Apcera Inc. All rights reserved.

package server

import (
	"errors"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nuid"
)

// Use nuid.
func genID() string {
	return nuid.Next()
}

// Ascii numbers 0-9
const (
	asciiZero = 48
	asciiNine = 57
)

// parseSize expects decimal positive numbers. We
// return -1 to signal error.
func parseSize(d []byte) (n int) {
	l := len(d)
	if l == 0 {
		return -1
	}
	var (
		i   int
		dec byte
	)

	// Note: Use `goto` here to avoid for loop in order
	// to have the function be inlined.
	// See: https://github.com/golang/go/issues/14768
loop:
	dec = d[i]
	if dec < asciiZero || dec > asciiNine {
		return -1
	}
	n = n*10 + (int(dec) - asciiZero)

	i++
	if i < l {
		goto loop
	}
	return n
}

// parseInt64 expects decimal positive numbers. We
// return -1 to signal error
func parseInt64(d []byte) (n int64) {
	if len(d) == 0 {
		return -1
	}
	for _, dec := range d {
		if dec < asciiZero || dec > asciiNine {
			return -1
		}
		n = n*10 + (int64(dec) - asciiZero)
	}
	return n
}

// Helper to move from float seconds to time.Duration
func secondsToDuration(seconds float64) time.Duration {
	ttl := seconds * float64(time.Second)
	return time.Duration(ttl)
}

// Parse a host/port string with an optional default port
func parseHostPort(hostPort string, defaultPort string) (host string, port int, err error) {
	if hostPort != "" {
		host, sPort, err := net.SplitHostPort(hostPort)
		switch err.(type) {
		case *net.AddrError:
			// try appending the current port
			host, sPort, err = net.SplitHostPort(hostPort + ":" + defaultPort)
		}
		if err != nil {
			return "", -1, err
		}
		port, err = strconv.Atoi(strings.TrimSpace(sPort))
		if err != nil {
			return "", -1, err
		}
		return strings.TrimSpace(host), port, nil
	}
	return "", -1, errors.New("No hostport specified")
}
