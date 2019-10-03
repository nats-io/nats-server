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
	"bytes"
	"fmt"
	"testing"
	"time"
)

func TestMemStoreBasics(t *testing.T) {
	ms, err := newMemStore(&MsgSetConfig{Storage: MemoryStorage})
	if err != nil {
		t.Fatalf("Unexpected error creating store: %v", err)
	}
	subj, msg := "foo", []byte("Hello World")
	if seq, err := ms.StoreMsg(subj, msg); err != nil {
		t.Fatalf("Error storing msg: %v", err)
	} else if seq != 1 {
		t.Fatalf("Expected sequence to be 1, got %d", seq)
	}
	stats := ms.Stats()
	if stats.Msgs != 1 {
		t.Fatalf("Expected 1 msg, got %d", stats.Msgs)
	}
	expectedSize := memStoreMsgSize(subj, msg)
	if stats.Bytes != expectedSize {
		t.Fatalf("Expected %d bytes, got %d", expectedSize, stats.Bytes)
	}
	nsubj, nmsg, _, err := ms.Lookup(1)
	if err != nil {
		t.Fatalf("Unexpected error looking up msg: %v", err)
	}
	if nsubj != subj {
		t.Fatalf("Subjects don't match, original %q vs %q", subj, nsubj)
	}
	if !bytes.Equal(nmsg, msg) {
		t.Fatalf("Msgs don't match, original %q vs %q", msg, nmsg)
	}
}

func TestMemStoreMsgLimit(t *testing.T) {
	maxMsgs := uint64(10)
	ms, err := newMemStore(&MsgSetConfig{Storage: MemoryStorage, MaxMsgs: maxMsgs})
	if err != nil {
		t.Fatalf("Unexpected error creating store: %v", err)
	}
	subj, msg := "foo", []byte("Hello World")
	for i := uint64(0); i < maxMsgs; i++ {
		ms.StoreMsg(subj, msg)
	}
	stats := ms.Stats()
	if stats.Msgs != maxMsgs {
		t.Fatalf("Expected %d msgs, got %d", maxMsgs, stats.Msgs)
	}
	if _, err := ms.StoreMsg(subj, msg); err != nil {
		t.Fatalf("Error storing msg: %v", err)
	}
	stats = ms.Stats()
	if stats.Msgs != maxMsgs {
		t.Fatalf("Expected %d msgs, got %d", maxMsgs, stats.Msgs)
	}
	if stats.LastSeq != 11 {
		t.Fatalf("Expected the last sequence to be 11 now, but got %d", stats.LastSeq)
	}
	if stats.FirstSeq != 2 {
		t.Fatalf("Expected the first sequence to be 2 now, but got %d", stats.FirstSeq)
	}
	// Make sure we can not lookup seq 1.
	if _, _, _, err := ms.Lookup(1); err == nil {
		t.Fatalf("Expected error looking up seq 1 but got none")
	}
}

func TestMemStoreBytesLimit(t *testing.T) {
	subj, msg := "foo", make([]byte, 512)
	storedMsgSize := memStoreMsgSize(subj, msg)

	toStore := uint64(1024)
	maxBytes := storedMsgSize * toStore

	ms, err := newMemStore(&MsgSetConfig{Storage: MemoryStorage, MaxBytes: maxBytes})
	if err != nil {
		t.Fatalf("Unexpected error creating store: %v", err)
	}

	for i := uint64(0); i < toStore; i++ {
		ms.StoreMsg(subj, msg)
	}
	stats := ms.Stats()
	if stats.Msgs != toStore {
		t.Fatalf("Expected %d msgs, got %d", toStore, stats.Msgs)
	}
	if stats.Bytes != storedMsgSize*toStore {
		t.Fatalf("Expected bytes to be %d, got %d", storedMsgSize*toStore, stats.Bytes)
	}

	// Now send 10 more and check that bytes limit enforced.
	for i := 0; i < 10; i++ {
		if _, err := ms.StoreMsg(subj, msg); err != nil {
			t.Fatalf("Error storing msg: %v", err)
		}
	}
	stats = ms.Stats()
	if stats.Msgs != toStore {
		t.Fatalf("Expected %d msgs, got %d", toStore, stats.Msgs)
	}
	if stats.Bytes != storedMsgSize*toStore {
		t.Fatalf("Expected bytes to be %d, got %d", storedMsgSize*toStore, stats.Bytes)
	}
	if stats.FirstSeq != 11 {
		t.Fatalf("Expected first sequence to be 11, got %d", stats.FirstSeq)
	}
	if stats.LastSeq != toStore+10 {
		t.Fatalf("Expected last sequence to be %d, got %d", toStore+10, stats.LastSeq)
	}
}

func TestMemStoreAgeLimit(t *testing.T) {
	maxAge := 10 * time.Millisecond
	ms, err := newMemStore(&MsgSetConfig{Storage: MemoryStorage, MaxAge: maxAge})
	if err != nil {
		t.Fatalf("Unexpected error creating store: %v", err)
	}
	// Store some messages. Does not really matter how many.
	subj, msg := "foo", []byte("Hello World")
	toStore := 100
	for i := 0; i < toStore; i++ {
		ms.StoreMsg(subj, msg)
	}
	stats := ms.Stats()
	if stats.Msgs != uint64(toStore) {
		t.Fatalf("Expected %d msgs, got %d", toStore, stats.Msgs)
	}
	// Let them expire
	checkFor(t, time.Second, maxAge, func() error {
		stats = ms.Stats()
		if stats.Msgs != 0 {
			return fmt.Errorf("Expected no msgs, got %d", stats.Msgs)
		}
		if stats.Bytes != 0 {
			return fmt.Errorf("Expected no bytes, got %d", stats.Bytes)
		}
		return nil
	})
}

func TestMemStoreTimeStamps(t *testing.T) {
	ms, err := newMemStore(&MsgSetConfig{Storage: MemoryStorage})
	if err != nil {
		t.Fatalf("Unexpected error creating store: %v", err)
	}
	last := time.Now().UnixNano()
	subj, msg := "foo", []byte("Hello World")
	for i := 0; i < 10; i++ {
		ms.StoreMsg(subj, msg)
		time.Sleep(5 * time.Microsecond)
	}
	for seq := uint64(1); seq <= 10; seq++ {
		_, _, ts, err := ms.Lookup(seq)
		if err != nil {
			t.Fatalf("Unexpected error looking up msg: %v", err)
		}
		// These should be different
		if ts <= last {
			t.Fatalf("Expected different timestamps, got %v", ts)
		}
		last = ts
	}
}
