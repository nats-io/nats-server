// Copyright 2026 Michael Utech
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

//go:build linux

package server

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func (c *client) getUDSPeerCreds() (UDSPeerCreds, error) {
	conn, ok := c.nc.(*net.UnixConn)
	if !ok {
		return UDSPeerCreds{UID: -1, GID: -1, PID: -1}, fmt.Errorf("connection is not a UNIX domain socket")
	}

	return getUDSPeerCreds(conn)
}

func getUDSPeerCreds(conn *net.UnixConn) (UDSPeerCreds, error) {
	result := UDSPeerCreds{UID: -1, GID: -1, PID: -1}

	raw, err := conn.SyscallConn()
	if err != nil {
		return result, err
	}

	// Avoid setting the connection to blocking mode via conn.File, use raw socket instead:
	err = raw.Control(func(fd uintptr) {
		ucred, callerr := syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
		if callerr != nil {
			err = callerr
			return
		}
		result = UDSPeerCreds{
			UID: int(ucred.Uid),
			GID: int(ucred.Gid),
			PID: int(ucred.Pid),
		}
	})

	return result, err
}

func (s *Server) UDSAcceptLoop(clr chan struct{}) {
	// If we were to exit before the listener is set up properly,
	// make sure we close the channel.
	defer func() {
		if clr != nil {
			close(clr)
		}
	}()

	if s.isShuttingDown() {
		return
	}

	// Snapshot server options.
	opts := s.getOpts()

	if opts.UDS.Path == _EMPTY_ {
		return
	}

	// Setup state that can enable shutdown
	s.mu.Lock()
	if s.uds == nil {
		s.mu.Unlock()
		return
	}
	path := s.uds.socket.path
	if s.uds.listener == nil {
		s.uds.listener, s.uds.listenerErr = natsListen("unix", path)
		if s.uds.listenerErr != nil {
			conn, dialErr := net.Dial("unix", path)
			if dialErr == nil {
				defer func(conn net.Conn) {
					_ = conn.Close()
				}(conn)

				// Socket is active - someone else is using it
				unixConn := conn.(*net.UnixConn)
				creds, credErr := getUDSPeerCreds(unixConn)
				s.mu.Unlock()
				if credErr != nil {
					s.Fatalf("UNIX domain socket %s in use, PID not available: %v", path, credErr)
					return
				}
				s.Fatalf("UNIX domain socket %s already in use by PID %d (UID %d)", path, creds.PID, creds.UID)
				return
			}
			// Dial failed - socket is stale, remove and retry
			s.Noticef("UNIX domain socket %s appears stale, removing...", path)
			_ = os.Remove(path)
			s.uds.listener, s.uds.listenerErr = natsListen("unix", path)
		}
	}
	if s.uds.listenerErr != nil {
		s.mu.Unlock()
		s.Fatalf("Error listening on UNIX domain socket: %s, %q", path, s.uds.listenerErr)
		return
	}

	// Apply socket permissions if configured
	if err := applyUdsPermissions(s.uds.listener, path, s.uds.socket.gid, s.uds.socket.mode); err != nil {
		s.mu.Unlock()
		s.Fatalf("Error setting UNIX domain socket permissions: %v", err)
		return
	}

	s.Noticef("Listening for client connections on UNIX domain socket %s", path)

	// Alert if PROXY protocol is enabled
	if opts.ProxyProtocol {
		s.Noticef("PROXY protocol enabled for client connections")
	}

	// Alert of TLS enabled.
	if opts.TLSConfig != nil {
		s.Noticef("TLS required for client connections")
		if opts.TLSHandshakeFirst && opts.TLSHandshakeFirstFallback == 0 {
			s.Warnf("Clients that are not using \"TLS Handshake First\" option will fail to connect")
		}
	}

	s.info.UDS = []UDSInfo{{Path: path}}

	// We do not advertize UDS sockets since they are not remotely available and clients connected
	// via UDS already know the path. Advertizing local UDS to remote is useless and might break clients.

	go s.acceptConnections(s.uds.listener, "Client",
		func(conn net.Conn) {
			s.createClient(conn)
			// TODO: perform UDS specific initialization, either here or in createClient()
		},
		func(_ error) bool {
			if s.isLameDuckMode() {
				// Signal that we are not accepting new clients
				s.ldmCh <- true
				// Now wait for the Shutdown...
				<-s.quitCh
				return true
			}
			return false
		})
	s.mu.Unlock()

	// Let the caller know that we are ready
	if clr != nil {
		close(clr)
	}
	clr = nil
}

// peerCredQueryUIDName looks up peer's UID and compares username with value.
// Returns true if username equals value. If UID has no passwd entry, username is empty.
func peerCredQueryUIDName(name string, c UDSPeerCreds, v any, _ map[string]any) (bool, error) {
	username, ok := v.(string)
	if !ok {
		return false, fmt.Errorf("%s: expected string, got %T", name, v)
	}
	u, err := user.LookupId(strconv.Itoa(c.UID))
	if err != nil {
		var unknownUID user.UnknownUserIdError
		if errors.As(err, &unknownUID) {
			// No passwd entry for this UID - treat as empty username
			return username == "", nil
		}
		return false, fmt.Errorf("user lookup for uid %d: %w", c.UID, err)
	}
	return u.Username == username, nil
}

// peerCredQueryGIDName looks up peer's GID and compares group name with value.
// Returns true if group name equals value. If GID has no group entry, name is empty.
func peerCredQueryGIDName(name string, c UDSPeerCreds, v any, _ map[string]any) (bool, error) {
	groupname, ok := v.(string)
	if !ok {
		return false, fmt.Errorf("%s: expected string, got %T", name, v)
	}
	g, err := user.LookupGroupId(strconv.Itoa(c.GID))
	if err != nil {
		var unknownGID user.UnknownGroupIdError
		if errors.As(err, &unknownGID) {
			// No group entry for this GID - treat as empty name
			return groupname == "", nil
		}
		return false, fmt.Errorf("group lookup for gid %d: %w", c.GID, err)
	}
	return g.Name == groupname, nil
}

// getProcessSupplementalGroups reads supplemental groups from /proc/<pid>/status.
func getProcessSupplementalGroups(pid int) ([]int, error) {
	if pid <= 0 {
		return nil, fmt.Errorf("invalid pid %d", pid)
	}
	f, err := os.Open(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return nil, fmt.Errorf("open /proc/%d/status: %w", pid, err)
	}
	defer func(f *os.File) {
		_ = f.Close()
	}(f)

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "Groups:") {
			fields := strings.Fields(line[7:])
			groups := make([]int, 0, len(fields))
			for _, field := range fields {
				gid, err := strconv.Atoi(field)
				if err != nil {
					return nil, fmt.Errorf("invalid gid %q in /proc/%d/status: %w", field, pid, err)
				}
				groups = append(groups, gid)
			}
			return groups, nil
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read /proc/%d/status: %w", pid, err)
	}
	return nil, fmt.Errorf("/proc/%d/status: Groups line not found", pid)
}

// peerCredQueryGroups matches GID against process supplemental groups.
func peerCredQueryGroups(name string, c UDSPeerCreds, v any, _ map[string]any) (bool, error) {
	gid, ok := v.(int64)
	if !ok {
		return false, fmt.Errorf("%s: expected int, got %T", name, v)
	}
	groups, err := getProcessSupplementalGroups(c.PID)
	if err != nil {
		return false, err
	}
	for _, g := range groups {
		if g == int(gid) {
			return true, nil
		}
	}
	return false, nil
}

// peerCredQueryGroupsName matches group name against process supplemental groups.
func peerCredQueryGroupsName(queryName string, c UDSPeerCreds, v any, _ map[string]any) (bool, error) {
	name, ok := v.(string)
	if !ok {
		return false, fmt.Errorf("%s: expected string, got %T", queryName, v)
	}
	g, err := user.LookupGroup(name)
	if err != nil {
		return false, fmt.Errorf("group lookup %q: %w", name, err)
	}
	gid, err := strconv.Atoi(g.Gid)
	if err != nil {
		return false, fmt.Errorf("invalid gid from lookup %q: %w", g.Gid, err)
	}
	groups, err := getProcessSupplementalGroups(c.PID)
	if err != nil {
		return false, err
	}
	for _, grp := range groups {
		if grp == gid {
			return true, nil
		}
	}
	return false, nil
}

// applyUdsPermissions sets group ownership and mode on the socket file.
// Also drains and closes any connections that may have been accepted during
// the race window between listen() and permission application.
func applyUdsPermissions(listener net.Listener, path string, gid, mode int) error {
	// Apply permissions first
	if gid >= 0 {
		if err := os.Chown(path, -1, gid); err != nil {
			return fmt.Errorf("chown %s: %w", path, err)
		}
	}
	if mode >= 0 {
		if err := os.Chmod(path, os.FileMode(mode)); err != nil {
			return fmt.Errorf("chmod %s: %w", path, err)
		}
	}

	// Drain any connections that snuck in during the race window
	if gid >= 0 || mode >= 0 {
		unixListener := listener.(*net.UnixListener)
		_ = unixListener.SetDeadline(time.Now())
		for {
			conn, err := unixListener.Accept()
			if err != nil {
				break // No more pending connections (or deadline hit)
			}
			_ = conn.Close() // Reject connection from race window
		}
		_ = unixListener.SetDeadline(time.Time{}) // Clear deadline
	}

	return nil
}

// newDefaultPeerCredQueries returns the default peer credential query handlers.
func newDefaultPeerCredQueries() map[string]udsPeerCredQuery {
	queries := make(map[string]udsPeerCredQuery, len(builtinPeerCredQueries))
	for name, query := range builtinPeerCredQueries {
		queries[name] = query
	}
	return queries
}

// newUDSServerState creates UDS server state from options.
// Returns nil, nil if UDS is not configured.
func newUDSServerState(opts *Options) (*udsServerState, error) {
	if opts.UDS.Path == _EMPTY_ {
		return nil, nil
	}
	spec, err := newUdsSocketSpec(opts.UDS.Path, opts.UDS.Group, opts.UDS.Mode)
	if err != nil {
		return nil, fmt.Errorf("UDS configuration error: %w", err)
	}
	return &udsServerState{
		socket:                spec,
		rules:                 opts.UDSRules,
		peerCredentialQueries: newDefaultPeerCredQueries(),
	}, nil
}

// newUdsSocketSpec creates a validated udsSocketSpec from raw config values.
// path required to be absolute and canonical (no .., //, etc.), group and mode are optional (empty string = no change after listen).
func newUdsSocketSpec(path, group, mode string) (udsSocketSpec, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return udsSocketSpec{}, fmt.Errorf("invalid path '%s', expect canonical absolute path", path)
	}

	spec := udsSocketSpec{
		path: path,
		gid:  -1,
		mode: -1,
	}

	if mode != "" {
		m, err := parseFileMode(mode)
		if err != nil {
			return udsSocketSpec{}, err
		}
		spec.mode = int(m)
	}

	if group != "" {
		// Try numeric GID first
		if gid, err := strconv.Atoi(group); err == nil {
			if gid < 0 {
				return udsSocketSpec{}, fmt.Errorf("invalid group '%s': negative ID", group)
			}
			spec.gid = gid
		} else {
			// Fall back to group name lookup
			g, err := user.LookupGroup(group)
			if err != nil {
				return udsSocketSpec{}, fmt.Errorf("invalid group '%s': %w", group, err)
			}
			gid, err := strconv.Atoi(g.Gid)
			if err != nil {
				return udsSocketSpec{}, fmt.Errorf("invalid group '%s'.Gid: '%s': %w", group, g.Gid, err)
			}
			spec.gid = gid
		}
	}

	return spec, nil
}

// parseFileMode parses a 4-digit octal mode string (e.g. "0660") to os.FileMode.
func parseFileMode(s string) (os.FileMode, error) {
	if len(s) != 4 || s[0] != '0' {
		return 0, fmt.Errorf("invalid file mode '%s', expected 4 digit octal (e.g. 0660)", s)
	}
	var m os.FileMode
	for i := 1; i < 4; i++ {
		d := s[i] - '0'
		if d > 7 {
			return 0, fmt.Errorf("invalid file mode '%s', expected 4 digit octal (e.g. 0660)", s)
		}
		m = m<<3 | os.FileMode(d)
	}
	if m > 0o777 {
		panic("Contract violation: investigate parseFileMode")
	}
	return m, nil
}
