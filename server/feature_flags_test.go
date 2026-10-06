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
	"reflect"
	"testing"
)

func TestGetFeatureFlag(t *testing.T) {
	featureFlags["test_flag"] = true
	t.Cleanup(func() { delete(featureFlags, "test_flag") })

	// Unset feature flags gets defaults.
	o := &Options{}
	require_Equal(t, o.getFeatureFlag(""), false)
	require_Equal(t, o.getFeatureFlag("test_flag"), true)

	// User disables the feature flag, it takes precedence.
	o.FeatureFlags = map[string]bool{"test_flag": false}
	require_Equal(t, o.getFeatureFlag("test_flag"), false)

	// User enables the feature flag, but we don't support it anymore.
	// It's allowed for the user to provide stale flags, but we shouldn't return they're enabled.
	delete(featureFlags, "test_flag")
	o.FeatureFlags = map[string]bool{"test_flag": true}
	require_Equal(t, o.getFeatureFlag("test_flag"), false)
}

func TestFeatureFlagsConfigParseNonBool(t *testing.T) {
	conf := createConfFile(t, []byte(`
		feature_flags {
		    opt_in_flag: "true"
		}
	`))

	_, err := ProcessConfigFile(conf)
	require_Error(t, err)
	require_Contains(t, err.Error(), `error parsing feature flag "opt_in_flag": expected bool, got string`)
}

func TestGetMergedFeatureFlags(t *testing.T) {
	previous := featureFlags
	featureFlags = make(map[string]bool)
	featureFlags["test_flag"] = true
	featureFlags["test_flag_default"] = true
	t.Cleanup(func() { featureFlags = previous })

	o := &Options{}
	o.FeatureFlags = map[string]bool{"test_flag": false, "unknown_flag": false}

	// The merged flags should only contain known flags, with the user's values taking precedence.
	merged := o.getMergedFeatureFlags()
	expected := map[string]bool{"test_flag": false, "test_flag_default": true}
	if !reflect.DeepEqual(expected, merged) {
		t.Fatalf("Feature flags are incorrect.\nexpected: %+v\ngot: %+v", expected, merged)
	}
}
