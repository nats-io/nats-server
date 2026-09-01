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
	"testing"
	"time"
)

func TestParseCronSkippedMidnightSaoPaulo(t *testing.T) {
	loc, err := time.LoadLocation("America/Sao_Paulo")
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		pattern string
		want    time.Time
	}{
		{"restricted DOM at 01:00", "0 0 1 4 11 *", time.Date(2018, time.November, 4, 1, 0, 0, 0, loc)},
		{"restricted DOW at 01:00", "0 0 1 * * 0", time.Date(2018, time.November, 4, 1, 0, 0, 0, loc)},
		{"restricted DOM at 02:00", "0 0 2 4 11 *", time.Date(2018, time.November, 4, 2, 0, 0, 0, loc)},
	}
	base := time.Date(2018, time.November, 3, 23, 0, 0, 0, loc)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := parseCron(test.pattern, loc, base.UnixNano())
			if err != nil {
				t.Fatal(err)
			}
			if !got.Equal(test.want) {
				t.Fatalf("pattern %q from %s: got %s, want %s", test.pattern, base, got, test.want)
			}
		})
	}
}
