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
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFileStoreBasics(t *testing.T) {
	storeDir, _ := ioutil.TempDir("", JetStreamStoreDir)
	ms, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, MsgSetConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer ms.Stop()

	subj, msg := "foo", []byte("Hello World")
	for i := 1; i <= 5; i++ {
		if seq, err := ms.StoreMsg(subj, msg); err != nil {
			t.Fatalf("Error storing msg: %v", err)
		} else if seq != uint64(i) {
			t.Fatalf("Expected sequence to be %d, got %d", i, seq)
		}
	}
	stats := ms.Stats()
	if stats.Msgs != 5 {
		t.Fatalf("Expected 5 msgs, got %d", stats.Msgs)
	}
	expectedSize := 5 * fileStoreMsgSize(subj, msg)
	if stats.Bytes != expectedSize {
		t.Fatalf("Expected %d bytes, got %d", expectedSize, stats.Bytes)
	}
	nsubj, nmsg, _, err := ms.Lookup(2)
	if err != nil {
		t.Fatalf("Unexpected error looking up msg: %v", err)
	}
	if nsubj != subj {
		t.Fatalf("Subjects don't match, original %q vs %q", subj, nsubj)
	}
	if !bytes.Equal(nmsg, msg) {
		t.Fatalf("Msgs don't match, original %q vs %q", msg, nmsg)
	}
	_, _, _, err = ms.Lookup(3)
	if err != nil {
		t.Fatalf("Unexpected error looking up msg: %v", err)
	}
}

func TestFileStoreBasicWriteMsgsAndRestore(t *testing.T) {
	storeDir := filepath.Join("", JetStreamStoreDir)
	fcfg := FileStoreConfig{StoreDir: storeDir}

	if _, err := newFileStore(fcfg, MsgSetConfig{Storage: MemoryStorage}); err == nil {
		t.Fatalf("Expected an error with wrong type")
	}
	if _, err := newFileStore(fcfg, MsgSetConfig{Storage: FileStorage}); err == nil {
		t.Fatalf("Expected an error with no name")
	}
	if _, err := newFileStore(fcfg, MsgSetConfig{Name: "dlc", Storage: FileStorage}); err == nil {
		t.Fatalf("Expected an error with non-existent directory")
	}

	// Make the directories to succeed in setup.
	os.MkdirAll(storeDir, 0755)
	defer os.RemoveAll(storeDir)

	ms, err := newFileStore(fcfg, MsgSetConfig{Name: "dlc", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer ms.Stop()

	subj := "foo"

	// Write 100 msgs
	toStore := uint64(100)

	for i := uint64(1); i <= toStore; i++ {
		msg := []byte(fmt.Sprintf("[%08d] Hello World!", i))
		if seq, err := ms.StoreMsg(subj, msg); err != nil {
			t.Fatalf("Error storing msg: %v", err)
		} else if seq != uint64(i) {
			t.Fatalf("Expected sequence to be %d, got %d", i, seq)
		}
	}
	stats := ms.Stats()
	if stats.Msgs != toStore {
		t.Fatalf("Expected %d msgs, got %d", toStore, stats.Msgs)
	}
	msg22 := []byte(fmt.Sprintf("[%08d] Hello World!", 22))
	expectedSize := toStore * fileStoreMsgSize(subj, msg22)

	if stats.Bytes != expectedSize {
		t.Fatalf("Expected %d bytes, got %d", expectedSize, stats.Bytes)
	}
	// Stop will flush to disk.
	ms.Stop()

	ms, err = newFileStore(fcfg, MsgSetConfig{Name: "dlc", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer ms.Stop()

	stats = ms.Stats()
	if stats.Msgs != toStore {
		t.Fatalf("Expected %d msgs, got %d", toStore, stats.Msgs)
	}
	if stats.Bytes != expectedSize {
		t.Fatalf("Expected %d bytes, got %d", expectedSize, stats.Bytes)
	}
}

func TestFileStoreMsgLimit(t *testing.T) {
	storeDir, _ := ioutil.TempDir("", JetStreamStoreDir)
	os.MkdirAll(storeDir, 0755)
	defer os.RemoveAll(storeDir)

	ms, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, MsgSetConfig{Name: "zzz", Storage: FileStorage, MaxMsgs: 10})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer ms.Stop()

	subj, msg := "foo", []byte("Hello World")
	for i := 0; i < 10; i++ {
		ms.StoreMsg(subj, msg)
	}
	stats := ms.Stats()
	if stats.Msgs != 10 {
		t.Fatalf("Expected %d msgs, got %d", 10, stats.Msgs)
	}
	if _, err := ms.StoreMsg(subj, msg); err != nil {
		t.Fatalf("Error storing msg: %v", err)
	}
	stats = ms.Stats()
	if stats.Msgs != 10 {
		t.Fatalf("Expected %d msgs, got %d", 10, stats.Msgs)
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

func TestFileStoreBytesLimit(t *testing.T) {
	subj, msg := "foo", make([]byte, 512)
	storedMsgSize := fileStoreMsgSize(subj, msg)

	toStore := uint64(1024)
	maxBytes := storedMsgSize * toStore

	storeDir, _ := ioutil.TempDir("", JetStreamStoreDir)
	os.MkdirAll(storeDir, 0755)
	defer os.RemoveAll(storeDir)

	ms, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, MsgSetConfig{Name: "zzz", Storage: FileStorage, MaxBytes: int64(maxBytes)})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer ms.Stop()

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

func TestFileStoreAgeLimit(t *testing.T) {
	maxAge := 10 * time.Millisecond

	storeDir, _ := ioutil.TempDir("", JetStreamStoreDir)
	os.MkdirAll(storeDir, 0755)
	defer os.RemoveAll(storeDir)

	ms, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, MsgSetConfig{Name: "zzz", Storage: FileStorage, MaxAge: maxAge})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer ms.Stop()

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
	checkExpired := func(t *testing.T) {
		t.Helper()
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
	// Let them expire
	checkExpired(t)
	// Now add some more and make sure that timer will fire again.
	for i := 0; i < toStore; i++ {
		ms.StoreMsg(subj, msg)
	}
	stats = ms.Stats()
	if stats.Msgs != uint64(toStore) {
		t.Fatalf("Expected %d msgs, got %d", toStore, stats.Msgs)
	}
	checkExpired(t)
}

func TestFileStoreTimeStamps(t *testing.T) {
	storeDir, _ := ioutil.TempDir("", JetStreamStoreDir)
	os.MkdirAll(storeDir, 0755)
	defer os.RemoveAll(storeDir)

	ms, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, MsgSetConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer ms.Stop()

	last := time.Now().UnixNano()
	subj, msg := "foo", []byte("Hello World")
	for i := 0; i < 10; i++ {
		time.Sleep(5 * time.Millisecond)
		ms.StoreMsg(subj, msg)
	}
	for seq := uint64(1); seq <= 10; seq++ {
		_, _, ts, err := ms.Lookup(seq)
		if err != nil {
			t.Fatalf("Unexpected error looking up msg: %v", err)
		}
		// These should be different
		if ts <= last {
			t.Fatalf("Expected different timestamps, got last %v vs %v", last, ts)
		}
		last = ts
	}
}

func TestFileStorePurge(t *testing.T) {
	storeDir, _ := ioutil.TempDir("", JetStreamStoreDir)
	os.MkdirAll(storeDir, 0755)
	defer os.RemoveAll(storeDir)

	ms, err := newFileStore(FileStoreConfig{StoreDir: storeDir, BlockSize: 64 * 1024}, MsgSetConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer ms.Stop()

	subj, msg := "foo", make([]byte, 8*1024)
	storedMsgSize := fileStoreMsgSize(subj, msg)

	toStore := uint64(1024)
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

	if numBlocks := ms.numMsgBlocks(); numBlocks <= 1 {
		t.Fatalf("Expected to have more then 1 msg block, got %d", numBlocks)
	}

	ms.Purge()

	if numBlocks := ms.numMsgBlocks(); numBlocks != 1 {
		t.Fatalf("Expected to have exactly 1 empty msg block, got %d", numBlocks)
	}

	checkPurgeStats := func() {
		t.Helper()
		stats = ms.Stats()
		if stats.Msgs != 0 {
			t.Fatalf("Expected 0 msgs after purge, got %d", stats.Msgs)
		}
		if stats.Bytes != 0 {
			t.Fatalf("Expected 0 bytes after purge, got %d", stats.Bytes)
		}
		if stats.LastSeq != toStore {
			t.Fatalf("Expected LastSeq to be %d., got %d", toStore, stats.LastSeq)
		}
		if stats.FirstSeq != toStore+1 {
			t.Fatalf("Expected FirstSeq to be %d., got %d", toStore+1, stats.FirstSeq)
		}
	}
	checkPurgeStats()

	// Make sure we recover same state.
	ms.Stop()

	ms, err = newFileStore(FileStoreConfig{StoreDir: storeDir, BlockSize: 64 * 1024}, MsgSetConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer ms.Stop()

	if numBlocks := ms.numMsgBlocks(); numBlocks != 1 {
		t.Fatalf("Expected to have exactly 1 empty msg block, got %d", numBlocks)
	}

	checkPurgeStats()
}

func TestFileStoreRemovePartialRecovery(t *testing.T) {
	storeDir, _ := ioutil.TempDir("", JetStreamStoreDir)
	os.MkdirAll(storeDir, 0755)
	defer os.RemoveAll(storeDir)

	ms, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, MsgSetConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer ms.Stop()

	subj, msg := "foo", []byte("Hello World")
	toStore := 100
	for i := 0; i < toStore; i++ {
		ms.StoreMsg(subj, msg)
	}
	stats := ms.Stats()
	if stats.Msgs != uint64(toStore) {
		t.Fatalf("Expected %d msgs, got %d", toStore, stats.Msgs)
	}

	// Remove half
	for i := 1; i <= toStore/2; i++ {
		ms.RemoveMsg(uint64(i))
	}

	stats = ms.Stats()
	if stats.Msgs != uint64(toStore/2) {
		t.Fatalf("Expected %d msgs, got %d", toStore/2, stats.Msgs)
	}

	// Make sure we recover same state.
	ms.Stop()

	ms, err = newFileStore(FileStoreConfig{StoreDir: storeDir}, MsgSetConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer ms.Stop()

	stats2 := ms.Stats()
	if stats != stats2 {
		t.Fatalf("Expected receovered stats to be the same, got %+v vs %+v\n", stats, stats2)
	}
}

func TestFileStoreRemoveOutOfOrderRecovery(t *testing.T) {
	storeDir, _ := ioutil.TempDir("", JetStreamStoreDir)
	os.MkdirAll(storeDir, 0755)
	defer os.RemoveAll(storeDir)

	ms, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, MsgSetConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer ms.Stop()

	subj, msg := "foo", []byte("Hello World")
	toStore := 100
	for i := 0; i < toStore; i++ {
		ms.StoreMsg(subj, msg)
	}
	stats := ms.Stats()
	if stats.Msgs != uint64(toStore) {
		t.Fatalf("Expected %d msgs, got %d", toStore, stats.Msgs)
	}

	// Remove evens
	for i := 2; i <= toStore; i += 2 {
		if !ms.RemoveMsg(uint64(i)) {
			t.Fatalf("Expected remove to return true")
		}
	}

	stats = ms.Stats()
	if stats.Msgs != uint64(toStore/2) {
		t.Fatalf("Expected %d msgs, got %d", toStore/2, stats.Msgs)
	}

	if _, _, _, err := ms.Lookup(1); err != nil {
		t.Fatalf("Expected to retrieve seq 1")
	}
	for i := 2; i <= toStore; i += 2 {
		if _, _, _, err := ms.Lookup(uint64(i)); err == nil {
			t.Fatalf("Expected error looking up seq %d that should be deleted", i)
		}
	}

	// Make sure we recover same state.
	ms.Stop()

	ms, err = newFileStore(FileStoreConfig{StoreDir: storeDir}, MsgSetConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer ms.Stop()

	stats2 := ms.Stats()
	if stats != stats2 {
		t.Fatalf("Expected receovered stats to be the same, got %+v vs %+v\n", stats, stats2)
	}

	if _, _, _, err := ms.Lookup(1); err != nil {
		t.Fatalf("Expected to retrieve seq 1")
	}
	for i := 2; i <= toStore; i += 2 {
		if _, _, _, err := ms.Lookup(uint64(i)); err == nil {
			t.Fatalf("Expected error looking up seq %d that should be deleted", i)
		}
	}
}

func TestFileStoreAgeLimitRecovery(t *testing.T) {
	maxAge := 10 * time.Millisecond

	storeDir, _ := ioutil.TempDir("", JetStreamStoreDir)
	os.MkdirAll(storeDir, 0755)
	defer os.RemoveAll(storeDir)

	ms, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, MsgSetConfig{Name: "zzz", Storage: FileStorage, MaxAge: maxAge})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer ms.Stop()

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
	ms.Stop()
	time.Sleep(2 * maxAge)

	ms, err = newFileStore(FileStoreConfig{StoreDir: storeDir}, MsgSetConfig{Name: "zzz", Storage: FileStorage, MaxAge: maxAge})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer ms.Stop()

	stats = ms.Stats()
	if stats.Msgs != 0 {
		t.Fatalf("Expected no msgs, got %d", stats.Msgs)
	}
	if stats.Bytes != 0 {
		t.Fatalf("Expected no bytes, got %d", stats.Bytes)
	}
}
