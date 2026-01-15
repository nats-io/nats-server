// Copyright 2020-2025 Michael Utech
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

//go:build !linux

// TODO: check build/test on platforms other than Linux

package server

import (
	"fmt"
	"os"
)

func (c *client) getUDSPeerCreds() (UDSPeerCreds, error) {
	result := UDSPeerCreds{UID: -1, GID: -1, PID: -1}

	return result, fmt.Errorf("UNIX domain peer credentials not supported")
}

func (s *Server) UDSAcceptLoop(clr chan struct{}) {
	// If we were to exit before the listener is setup properly,
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

	if opts.UDS.Path != _EMPTY_ {
		s.Warnf("UNIX domain sockets are only supported on Linux")
	}
	return
}

var errUnsupportedPlatform = fmt.Errorf("not supported on this platform")

func peerCredQueryUIDName(c UDSPeerCreds, v string) (bool, error) {
	return false, errUnsupportedPlatform
}

func peerCredQueryGIDName(c UDSPeerCreds, v string) (bool, error) {
	return false, errUnsupportedPlatform
}

func peerCredQueryPIDGID(c UDSPeerCreds, v string) (bool, error) {
	return false, errUnsupportedPlatform
}

func peerCredQueryPIDGIDName(c UDSPeerCreds, v string) (bool, error) {
	return false, errUnsupportedPlatform
}

func parseFileMode(s string) (os.FileMode, error) {
	return 0, errUnsupportedPlatform
}

func newUdsSocketSpec(path, group, mode string) (udsSocketSpec, error) {
	return udsSocketSpec{}, errUnsupportedPlatform
}

func newDefaultPeerCredQueries() map[string]PeerCredQueryFunc {
	return nil
}

func newUDSServerState(opts *Options) (*udsServerState, error) {
	if opts.UDS.Path != _EMPTY_ {
		return nil, fmt.Errorf("UDS not supported on this platform")
	}
	return nil, nil
}
