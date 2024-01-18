// Copyright 2023 The NATS Authors
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

package certidp

import "testing"

// Checks the return values of the function GetStatusAssertionStr
func TestGetStatusAssertionStr(t *testing.T) {
	tests := []struct {
		name     string
		input    int
		expected string
	}{
		{
			name:     "GoodStatus",
			input:    0,
			expected: "good",
		},
		{
			name:     "RevokedStatus",
			input:    1,
			expected: "revoked",
		},
		{
			name:     "UnknownStatus",
			input:    2,
			expected: "unknown",
		},
		// Invalid status assertion value.
		{
			name:     "InvalidStatus",
			input:    42,
			expected: "unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetStatusAssertionStr(tt.input)
			if got != tt.expected {
				t.Errorf("Expected GetStatusAssertionStr: %v, got %v", tt.expected, got)
			}
		})
	}
}
