// Copyright 2022-2024 The NATS Authors
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

// This module is NOOP unless the build tag is present.
//go:build !antithesis_assert

package antithesis

import (
	"testing"
)

// SetupCompleted This is a NOOP, refer to the actual implementation documentation
func SetupCompleted() {}

// AssertUnreachable This is a NOOP, refer to the actual implementation documentation
func AssertUnreachable(_ testing.TB, _ string, _ map[string]any) {}

// Assert This is a NOOP, refer to the actual implementation documentation
func Assert(_ testing.TB, _ bool, _ string, _ map[string]any) {}
