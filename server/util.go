// Copyright 2012-2024 The NATS Authors
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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net"
	"net/url"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

// This map is used to store URLs string as the key with a reference count as
// the value. This is used to handle gossiped URLs such as connect_urls, etc..
type refCountedUrlSet map[string]int

// Ascii numbers 0-9
const (
	asciiZero = 48
	asciiNine = 57
)

func versionComponents(version string) (major, minor, patch int, err error) {
	m := semVerRe.FindStringSubmatch(version)
	if len(m) == 0 {
		return 0, 0, 0, errors.New("invalid semver")
	}
	major, err = strconv.Atoi(m[1])
	if err != nil {
		return -1, -1, -1, err
	}
	minor, err = strconv.Atoi(m[2])
	if err != nil {
		return -1, -1, -1, err
	}
	patch, err = strconv.Atoi(m[3])
	if err != nil {
		return -1, -1, -1, err
	}
	return major, minor, patch, err
}

func versionAtLeastCheckError(version string, emajor, eminor, epatch int) (bool, error) {
	major, minor, patch, err := versionComponents(version)
	if err != nil {
		return false, err
	}
	if major > emajor ||
		(major == emajor && minor > eminor) ||
		(major == emajor && minor == eminor && patch >= epatch) {
		return true, nil
	}
	return false, err
}

func versionAtLeast(version string, emajor, eminor, epatch int) bool {
	res, _ := versionAtLeastCheckError(version, emajor, eminor, epatch)
	return res
}

// parseSize expects decimal positive numbers. We
// return -1 to signal error.
func parseSize(d []byte) (n int) {
	const maxParseSizeLen = 9 //999M

	l := len(d)
	if l == 0 || l > maxParseSizeLen {
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
		if ae, ok := err.(*net.AddrError); ok && strings.Contains(ae.Err, "missing port") {
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
	return "", -1, errors.New("no hostport specified")
}

// Returns true if URL u1 represents the same URL than u2,
// false otherwise.
func urlsAreEqual(u1, u2 *url.URL) bool {
	return reflect.DeepEqual(u1, u2)
}

// comma produces a string form of the given number in base 10 with
// commas after every three orders of magnitude.
//
// e.g. comma(834142) -> 834,142
//
// This function was copied from the github.com/dustin/go-humanize
// package and is Copyright Dustin Sallings <dustin@spy.net>
func comma(v int64) string {
	sign := ""

	// Min int64 can't be negated to a usable value, so it has to be special cased.
	if v == math.MinInt64 {
		return "-9,223,372,036,854,775,808"
	}

	if v < 0 {
		sign = "-"
		v = 0 - v
	}

	parts := []string{"", "", "", "", "", "", ""}
	j := len(parts) - 1

	for v > 999 {
		parts[j] = strconv.FormatInt(v%1000, 10)
		switch len(parts[j]) {
		case 2:
			parts[j] = "0" + parts[j]
		case 1:
			parts[j] = "00" + parts[j]
		}
		v = v / 1000
		j--
	}
	parts[j] = strconv.Itoa(int(v))
	return sign + strings.Join(parts[j:], ",")
}

// Adds urlStr to the given map. If the string was already present, simply
// bumps the reference count.
// Returns true only if it was added for the first time.
func (m refCountedUrlSet) addUrl(urlStr string) bool {
	m[urlStr]++
	return m[urlStr] == 1
}

// Removes urlStr from the given map. If the string is not present, nothing
// is done and false is returned.
// If the string was present, its reference count is decreased. Returns true
// if this was the last reference, false otherwise.
func (m refCountedUrlSet) removeUrl(urlStr string) bool {
	removed := false
	if ref, ok := m[urlStr]; ok {
		if ref == 1 {
			removed = true
			delete(m, urlStr)
		} else {
			m[urlStr]--
		}
	}
	return removed
}

// Returns the unique URLs in this map as a slice
func (m refCountedUrlSet) getAsStringSlice() []string {
	a := make([]string, 0, len(m))
	for u := range m {
		a = append(a, u)
	}
	return a
}

// natsListenConfig provides a common configuration to match the one used by
// net.Listen() but with our own defaults.
// Go 1.13 introduced default-on TCP keepalives with aggressive timings and
// there's no sane portable way in Go with stdlib to split the initial timer
// from the retry timer.  Linux/BSD defaults are 2hrs/75s and Go sets both
// to 15s; the issue re making them indepedently tunable has been open since
// 2014 and this code here is being written in 2020.
// The NATS protocol has its own L7 PING/PONG keepalive system and the Go
// defaults are inappropriate for IoT deployment scenarios.
// Replace any NATS-protocol calls to net.Listen(...) with
// natsListenConfig.Listen(ctx,...) or use natsListen(); leave calls for HTTP
// monitoring, etc, on the default.
var natsListenConfig = &net.ListenConfig{
	KeepAlive: -1,
}

// natsListen() is the same as net.Listen() except that TCP keepalives are
// disabled (to match Go's behavior before Go 1.13).
func natsListen(network, address string) (net.Listener, error) {
	return natsListenConfig.Listen(context.Background(), network, address)
}

// natsDialTimeout is the same as net.DialTimeout() except the TCP keepalives
// are disabled (to match Go's behavior before Go 1.13).
func natsDialTimeout(network, address string, timeout time.Duration) (net.Conn, error) {
	d := net.Dialer{
		Timeout:   timeout,
		KeepAlive: -1,
	}
	return d.Dial(network, address)
}

// redactURLList() returns a copy of a list of URL pointers where each item
// in the list will either be the same pointer if the URL does not contain a
// password, or to a new object if there is a password.
// The intended use-case is for logging lists of URLs safely.
func redactURLList(unredacted []*url.URL) []*url.URL {
	r := make([]*url.URL, len(unredacted))
	// In the common case of no passwords, if we don't let the new object leave
	// this function then GC should be easier.
	needCopy := false
	for i := range unredacted {
		if unredacted[i] == nil {
			r[i] = nil
			continue
		}
		if _, has := unredacted[i].User.Password(); !has {
			r[i] = unredacted[i]
			continue
		}
		needCopy = true
		ru := *unredacted[i]
		ru.User = url.UserPassword(ru.User.Username(), "xxxxx")
		r[i] = &ru
	}
	if needCopy {
		return r
	}
	return unredacted
}

// redactURLString() attempts to redact a URL string.
func redactURLString(raw string) string {
	if !strings.ContainsRune(raw, '@') {
		return raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	return u.Redacted()
}

// getURLsAsString returns a slice of u.Host from the given slice of url.URL's
func getURLsAsString(urls []*url.URL) []string {
	a := make([]string, 0, len(urls))
	for _, u := range urls {
		a = append(a, u.Host)
	}
	return a
}

// copyBytes make a new slice of the same size than `src` and copy its content.
// If `src` is nil or its length is 0, then this returns `nil`
func copyBytes(src []byte) []byte {
	if len(src) == 0 {
		return nil
	}
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

// copyStrings make a new slice of the same size than `src` and copy its content.
// If `src` is nil, then this returns `nil`
func copyStrings(src []string) []string {
	if src == nil {
		return nil
	}
	dst := make([]string, len(src))
	copy(dst, src)
	return dst
}

// Returns a byte slice for the INFO protocol.
func generateInfoJSON(info *Info) []byte {
	b, _ := json.Marshal(info)
	pcs := [][]byte{[]byte("INFO"), b, []byte(CR_LF)}
	return bytes.Join(pcs, []byte(" "))
}

// hasLock tests if a lock is held on the supplied RWMutex, so that functions
// which require a lock to be held by their caller can check if that lock is
// indeed held or not.
//
// hasLock is non-blocking and so if a lock is already held it will immediately
// return true or false depending on if the lock is held or not
//
// hasLock does not log anything even when MUTEX_CHECK_DEBUG_FILE is set as it
// is intended to assist with logic in the rest of the code.  To capture
// debugging for where a lock is expected, please use preEmptLock
func hasLock(lock *sync.RWMutex) bool {
	// May want to switch this to being part of the main configuration instead
	// of an ENV VAR if this is something that sticks around
	logfile, ok := os.LookupEnv("MUTEX_CHECK_DEBUG_FILE")
	if ok {
		return hasLockthing(lock, false, &logfile)
	}
	return hasLockthing(lock, false, nil)
}

// preEmptLock is the same as hasLock, except that if it receives a lock it does
// not unlock it, allowing the calling function to effectively obtain a mutex
// lock if it was incorrectly called without one being held.  The calling
// function will need to unlock the mutex when complete
//
// preEmptLock is non-blocking and so if a lock is already held it will immediately
// return true or false depending on if the lock is held or not
//
// Setting the environment variable MUTEX_CHECK_DEBUG_FILE causes hasLock to
// append a line to the filename set in the variable which contains a JSON'ified
// stacktrace of what called it, so that it is simple to debug where errant
// locks may lie
func preEmptLock(lock *sync.RWMutex) bool {
	// May want to switch this to being part of the main configuration instead
	// of an ENV VAR if this is something that sticks around
	logfile, ok := os.LookupEnv("MUTEX_CHECK_DEBUG_FILE")
	if ok {
		return hasLockthing(lock, true, &logfile)
	}
	return hasLockthing(lock, true, nil)
}

// hasLockthing is the function which unlies hasLock and preEmptLock, which
// should be called in preference to calling this directly.
func hasLockthing(lock *sync.RWMutex, keepLock bool, logfile *string) bool {
	// TryLock is non-blocking and so this can be used to test if a lock is held
	// by something already. If we can get a lock, then none was already held,
	// if we can't then something already has one
	if !lock.TryLock() {
		// We didn't get a lock, therefore something else has it.  Therefore we
		// don't need to release it and can immediately return true
		return true
	}

	// We obtained a lock, which means nothing else had a lock.  Therefore we
	// can (and should) safely unlock it again (as we are now the one with the
	// lock).  We can do this immediately as we were only locking to test, not
	// to actually use it
	if !keepLock {
		lock.Unlock()
	}

	// Probably only want to use this for debugging, as using `runtime` for
	// normal logging is probably not a great idea.  However this allows us to
	// trace back calls to `hasLock` to determine why a function expecting a
	// lock to be held has been called without one being held
	if logfile != nil {
		var (
			output trace
		)
		pc := make([]uintptr, 32)
		callers := runtime.Callers(1, pc)
		// Can't use range here as we risk a nil pointer deref if the slice isn't full
		for i := 0; i < callers; i++ {
			// Work our way back through the stack trace
			runtimeFunc := runtime.FuncForPC(pc[i])
			file, line := runtimeFunc.FileLine(pc[i])

			// Will use this to create JSON output to make it easier to read/parse
			output.Trace = append(output.Trace, traceEntry{
				Depth:    i,
				File:     file,
				Line:     line,
				Function: runtimeFunc.Name(),
			})
		}

		outStr, err := json.Marshal(output)
		if err != nil {
			outStr = []byte(fmt.Sprintf("could not marshal output, err=[%s], output=[%+v]", err, output))
		}
		outStr = append(outStr, '\n')

		logHandle, err := os.OpenFile(*logfile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
		if err != nil {
			log.Printf("hasLock could not write to mutex log, err=[%s]", err)
		}
		defer logHandle.Close()
		_, err = logHandle.WriteString(string(outStr))
		if err != nil {
			log.Printf("hasLock could not write to mutex log, err=[%s]", err)
		}
	}

	// We can state that nothing had a lock, because we got one, and so can return false
	return false
}

// logUnLock simply unlocks a held lock, but with additional logging to pair
// with preEmptLock and hasLock.  e.g. you can check that the log contains both
// locks and unlocks to ensure that all paths are covered
func logUnLock(lock *sync.RWMutex) {
	doubleUnlock := false

	// Unlock the lock *if* it was held
	if lock.TryLock() {
		// Hmmmm, got a lock when we were trying to unlock
		doubleUnlock = true
	}

	// Either the lock was held already and we want to unlock it, or it was not
	// and TryLock obtained it, so either way we want to unlock :)
	lock.Unlock()

	logfile, ok := os.LookupEnv("MUTEX_CHECK_DEBUG_FILE")
	if ok {
		var (
			output trace
		)
		if doubleUnlock {
			output.Comments = "attempted to double unlock"
		}
		pc := make([]uintptr, 32)
		callers := runtime.Callers(1, pc)
		// Can't use range here as we risk a nil pointer deref if the slice isn't full
		for i := 0; i < callers; i++ {
			// Work our way back through the stack trace
			runtimeFunc := runtime.FuncForPC(pc[i])
			file, line := runtimeFunc.FileLine(pc[i])

			// Will use this to create JSON output to make it easier to read/parse
			output.Trace = append(output.Trace, traceEntry{
				Depth:    i,
				File:     file,
				Line:     line,
				Function: runtimeFunc.Name(),
			})
		}

		outStr, err := json.Marshal(output)
		if err != nil {
			outStr = []byte(fmt.Sprintf("could not marshal output, err=[%s], output=[%+v]", err, output))
		}
		outStr = append(outStr, '\n')

		logHandle, err := os.OpenFile(logfile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
		if err != nil {
			log.Printf("logUnLock could not write to mutex log, err=[%s]", err)
		}
		defer logHandle.Close()
		_, err = logHandle.WriteString(string(outStr))
		if err != nil {
			log.Printf("logUnLock could not write to mutex log, err=[%s]", err)
		}
	}

}

// trace and traceEntry are types used to construct the json'ified stack trace
type trace struct {
	Trace    []traceEntry `json:"trace"`
	Comments string       `json:"comments,omitempty"`
}

type traceEntry struct {
	Depth    int    `json:"depth"`
	File     string `json:"file"`
	Line     int    `json:"line"`
	Function string `json:"function"`
}
