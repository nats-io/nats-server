// Copyright 2025 The NATS Authors
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
//
//go:build goexperiment.jsonv2

package server

import (
	"bytes"
	jsonv2 "encoding/json/v2"
	"weak"
)

func (js *jetStream) metaSnapshotJSON(streams []writeableStreamAssignment) ([]byte, error) {
	b := js.cluster.lastsnap.Value()
	if b == nil {
		b = bytes.NewBuffer(nil)
		js.cluster.lastsnap = weak.Make(b)
	}
	b.Reset()
	if err := jsonv2.MarshalWrite(b, streams); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}
