// Copyright 2019 The NATS Authors
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
	"errors"
	"time"
)

// StorageType determines how messages are stored for retention.
type StorageType int

const (
	// Memory specifies in memory only.
	MemoryStorage StorageType = iota
	// File specifies on disk, designated by the JetStream config StoreDir.
	FileStorage
)

type MsgSetStore interface {
	StoreMsg(subj string, msg []byte) (uint64, error)
	Lookup(seq uint64) (subj string, msg []byte, ts int64, err error)
	RemoveMsg(seq uint64) bool
	EraseMsg(seq uint64) bool
	Purge() uint64
	GetSeqFromTime(t time.Time) uint64
	StorageBytesUpdate(func(int64))
	Stats() MsgSetStats
	Stop()
}

// MsgSetStats are stats about this given message set.
type MsgSetStats struct {
	Msgs     uint64
	Bytes    uint64
	FirstSeq uint64
	LastSeq  uint64
}

var (
	ErrStoreMsgNotFound = errors.New("no message found")
)
