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

// This test file is skipped by default (unless the include_js_cluster_long_running_tests tag is set).
// This is a measure to avoid accidentally running tests here (e.g. `go test ./server`)
//go:build !skip_js_tests && include_js_cluster_long_running_tests
// +build !skip_js_tests,include_js_cluster_long_running_tests

package server

import (
	"testing"
)

func TestLongRunningPlaceholder(t *testing.T) {
	t.Logf("Hello World!")
}
