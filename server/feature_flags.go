// Copyright 2026 The NATS Authors
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
	"maps"
	"slices"
	"strings"
)

var featureFlags = map[string]bool{}

// getFeatureFlag is used to retrieve either the default or overwritten value for a feature flag.
// The user's value takes precedence over the system's default. However, if the flag doesn't exist, it's disabled.
// Options read lock should be held.
func (o *Options) getFeatureFlag(k string) bool {
	defaultValue, ok := featureFlags[k]
	if !ok {
		return false // Not supported.
	}
	if userValue, ok := o.FeatureFlags[k]; ok {
		return userValue
	}
	return defaultValue
}

// getMergedFeatureFlags returns a merged map of feature flags, with the user's values taking precedence.
func (o *Options) getMergedFeatureFlags() map[string]bool {
	merged := make(map[string]bool)
	for k, v := range featureFlags {
		merged[k] = v
	}
	for k, v := range o.FeatureFlags {
		if _, ok := featureFlags[k]; !ok {
			continue
		}
		merged[k] = v
	}
	return merged
}

// printFeatureFlags logs the currently used feature flags on server startup.
func (s *Server) printFeatureFlags(o *Options) {
	if len(o.FeatureFlags) == 0 {
		return
	}
	keys := slices.Sorted(maps.Keys(o.FeatureFlags))

	var (
		configured  strings.Builder
		unsupported strings.Builder
	)

	for _, k := range keys {
		// Unsupported
		defaultValue, ok := featureFlags[k]
		if !ok {
			if unsupported.Len() > 0 {
				unsupported.WriteString(", ")
			}
			unsupported.WriteString(k)
			continue
		}

		v := o.FeatureFlags[k]
		if configured.Len() > 0 {
			configured.WriteString(", ")
		}
		configured.WriteString(k)
		configured.WriteString(" (")
		if defaultValue {
			if v {
				configured.WriteString("enabled")
			} else {
				configured.WriteString("opt-out")
			}
		} else if v {
			configured.WriteString("opt-in")
		} else {
			configured.WriteString("disabled")
		}
		configured.WriteString(")")
	}
	if configured.Len() == 0 {
		configured.WriteString("none")
	}

	s.Noticef("  Feature flags:")
	s.Noticef("    Configured:    %s", configured.String())
	if unsupported.Len() > 0 {
		s.Noticef("    Unsupported:   %s", unsupported.String())
	}
}
