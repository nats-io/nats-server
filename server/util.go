// Copyright 2012-2018 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"errors"
	"fmt"
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

// Parse a host/port string with a default port to use
// if none (or 0 or -1) is specified in `hostPort` string.
func parseHostPort(hostPort string, defaultPort int) (host string, port int, err error) {
	if hostPort != "" {
		host, sPort, err := net.SplitHostPort(hostPort)
		switch err.(type) {
		case *net.AddrError:
			// try appending the current port
			host, sPort, err = net.SplitHostPort(fmt.Sprintf("%s:%d", hostPort, defaultPort))
		}
		if err != nil {
			return "", -1, err
		}
		port, err = strconv.Atoi(strings.TrimSpace(sPort))
		if err != nil {
			return "", -1, err
		}
		if port == 0 || port == -1 {
			port = defaultPort
		}
		return strings.TrimSpace(host), port, nil
	}
	return "", -1, errors.New("No hostport specified")
}
