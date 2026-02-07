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

package server

import (
	"fmt"
	"net"
	"strings"
)

// UDSOptions holds UNIX domain socket configuration options.
type UDSOptions struct {
	Path  string
	Group string
	Mode  string
}

// ParseUDSOption parses a UDS option string in the format
// "<abs-path>;group=<grp>;mode=<octal (0660)>". The path is required, group and
// mode are optional.
func ParseUDSOption(s string) (UDSOptions, error) {
	var opts UDSOptions
	parts := strings.Split(s, ";")
	opts.Path = parts[0]

	if opts.Path == _EMPTY_ {
		return UDSOptions{}, fmt.Errorf("invalid path: '' (expected absolute path to UNIX domain socket)")
	}

	for _, part := range parts[1:] {
		if part == _EMPTY_ {
			continue
		}
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			return UDSOptions{}, fmt.Errorf("invalid UDS option format: %q (expected <option>=<value>)", part)
		}
		key, value := kv[0], kv[1]
		switch key {
		case "group":
			opts.Group = value
		case "mode":
			opts.Mode = value
		default:
			return UDSOptions{}, fmt.Errorf("unknown UDS option: %q (expected 'group' or 'mode')", key)
		}
	}

	return opts, nil
}

// parseUDSConfig parses the uds { ... } config block.
func parseUDSConfig(v any, opts *Options, errors *[]error) error {
	var lt token
	defer convertPanicToErrorList(&lt, errors)

	tk, v := unwrapValue(v, &lt)
	m, ok := v.(map[string]any)
	if !ok {
		return &configErr{tk, fmt.Sprintf("Expected map for 'uds' config, got %T", v)}
	}

	for mk, mv := range m {
		tk, mv = unwrapValue(mv, &lt)
		switch strings.ToLower(mk) {
		case "path":
			opts.UDS.Path = mv.(string)
		case "group":
			opts.UDS.Group = mv.(string)
		case "mode":
			opts.UDS.Mode = mv.(string)
		default:
			return &configErr{tk, fmt.Sprintf("Unknown 'uds' config option: %q", mk)}
		}
	}
	return nil
}

// UDSInfo is the UDS information exposed to clients in the INFO message.
type UDSInfo struct {
	Path string `json:"path"`
}

type udsSocketSpec struct {
	path string
	gid  int // -1 = no change
	mode int // -1 = no change, 0 = 0000
}

type udsServerState struct {
	socket                udsSocketSpec
	listener              net.Listener
	listenerErr           error
	rules                 []*UDSRule
	peerCredentialQueries map[string]udsPeerCredQuery
}
