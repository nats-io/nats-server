// Copyright 2019-2020 The NATS Authors
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
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/bits"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestFileStoreBasics(t *testing.T) {
	storeDir, _ := ioutil.TempDir("", JetStreamStoreDir)
	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	subj, msg := "foo", []byte("Hello World")
	now := time.Now().UnixNano()
	for i := 1; i <= 5; i++ {
		if seq, ts, err := fs.StoreMsg(subj, msg); err != nil {
			t.Fatalf("Error storing msg: %v", err)
		} else if seq != uint64(i) {
			t.Fatalf("Expected sequence to be %d, got %d", i, seq)
		} else if ts < now || ts > now+int64(time.Millisecond) {
			t.Fatalf("Expected timestamp to be current, got %v", ts-now)
		}
	}

	state := fs.State()
	if state.Msgs != 5 {
		t.Fatalf("Expected 5 msgs, got %d", state.Msgs)
	}
	expectedSize := 5 * fileStoreMsgSize(subj, msg)
	if state.Bytes != expectedSize {
		t.Fatalf("Expected %d bytes, got %d", expectedSize, state.Bytes)
	}
	nsubj, nmsg, _, err := fs.LoadMsg(2)
	if err != nil {
		t.Fatalf("Unexpected error looking up msg: %v", err)
	}
	if nsubj != subj {
		t.Fatalf("Subjects don't match, original %q vs %q", subj, nsubj)
	}
	if !bytes.Equal(nmsg, msg) {
		t.Fatalf("Msgs don't match, original %q vs %q", msg, nmsg)
	}
	_, _, _, err = fs.LoadMsg(3)
	if err != nil {
		t.Fatalf("Unexpected error looking up msg: %v", err)
	}
}

func TestFileStoreBasicWriteMsgsAndRestore(t *testing.T) {
	storeDir, _ := ioutil.TempDir("", JetStreamStoreDir)
	os.MkdirAll(storeDir, 0755)
	defer os.RemoveAll(storeDir)

	fcfg := FileStoreConfig{StoreDir: storeDir}

	if _, err := newFileStore(fcfg, StreamConfig{Storage: MemoryStorage}); err == nil {
		t.Fatalf("Expected an error with wrong type")
	}
	if _, err := newFileStore(fcfg, StreamConfig{Storage: FileStorage}); err == nil {
		t.Fatalf("Expected an error with no name")
	}

	fs, err := newFileStore(fcfg, StreamConfig{Name: "dlc", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	subj := "foo"

	// Write 100 msgs
	toStore := uint64(100)
	for i := uint64(1); i <= toStore; i++ {
		msg := []byte(fmt.Sprintf("[%08d] Hello World!", i))
		if seq, _, err := fs.StoreMsg(subj, msg); err != nil {
			t.Fatalf("Error storing msg: %v", err)
		} else if seq != uint64(i) {
			t.Fatalf("Expected sequence to be %d, got %d", i, seq)
		}
	}
	state := fs.State()
	if state.Msgs != toStore {
		t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
	}
	msg22 := []byte(fmt.Sprintf("[%08d] Hello World!", 22))
	expectedSize := toStore * fileStoreMsgSize(subj, msg22)

	if state.Bytes != expectedSize {
		t.Fatalf("Expected %d bytes, got %d", expectedSize, state.Bytes)
	}
	// Stop will flush to disk.
	fs.Stop()

	// Make sure Store call after does not work.
	if _, _, err := fs.StoreMsg(subj, []byte("no work")); err == nil {
		t.Fatalf("Expected an error for StoreMsg call after Stop, got none")
	}

	// Restart
	fs, err = newFileStore(fcfg, StreamConfig{Name: "dlc", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	state = fs.State()
	if state.Msgs != toStore {
		t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
	}
	if state.Bytes != expectedSize {
		t.Fatalf("Expected %d bytes, got %d", expectedSize, state.Bytes)
	}

	// Now write 100 more msgs
	for i := uint64(101); i <= toStore*2; i++ {
		msg := []byte(fmt.Sprintf("[%08d] Hello World!", i))
		if seq, _, err := fs.StoreMsg(subj, msg); err != nil {
			t.Fatalf("Error storing msg: %v", err)
		} else if seq != uint64(i) {
			t.Fatalf("Expected sequence to be %d, got %d", i, seq)
		}
	}
	state = fs.State()
	if state.Msgs != toStore*2 {
		t.Fatalf("Expected %d msgs, got %d", toStore*2, state.Msgs)
	}

	// Now cycle again and make sure that last batch was stored.
	// Stop will flush to disk.
	fs.Stop()

	// Restart
	fs, err = newFileStore(fcfg, StreamConfig{Name: "dlc", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	state = fs.State()
	if state.Msgs != toStore*2 {
		t.Fatalf("Expected %d msgs, got %d", toStore*2, state.Msgs)
	}
	if state.Bytes != expectedSize*2 {
		t.Fatalf("Expected %d bytes, got %d", expectedSize*2, state.Bytes)
	}
}

func TestFileStoreMsgLimit(t *testing.T) {
	storeDir, _ := ioutil.TempDir("", JetStreamStoreDir)
	os.MkdirAll(storeDir, 0755)
	defer os.RemoveAll(storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, StreamConfig{Name: "zzz", Storage: FileStorage, MaxMsgs: 10})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	subj, msg := "foo", []byte("Hello World")
	for i := 0; i < 10; i++ {
		fs.StoreMsg(subj, msg)
	}
	state := fs.State()
	if state.Msgs != 10 {
		t.Fatalf("Expected %d msgs, got %d", 10, state.Msgs)
	}
	if _, _, err := fs.StoreMsg(subj, msg); err != nil {
		t.Fatalf("Error storing msg: %v", err)
	}
	state = fs.State()
	if state.Msgs != 10 {
		t.Fatalf("Expected %d msgs, got %d", 10, state.Msgs)
	}
	if state.LastSeq != 11 {
		t.Fatalf("Expected the last sequence to be 11 now, but got %d", state.LastSeq)
	}
	if state.FirstSeq != 2 {
		t.Fatalf("Expected the first sequence to be 2 now, but got %d", state.FirstSeq)
	}
	// Make sure we can not lookup seq 1.
	if _, _, _, err := fs.LoadMsg(1); err == nil {
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

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, StreamConfig{Name: "zzz", Storage: FileStorage, MaxBytes: int64(maxBytes)})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	for i := uint64(0); i < toStore; i++ {
		fs.StoreMsg(subj, msg)
	}
	state := fs.State()
	if state.Msgs != toStore {
		t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
	}
	if state.Bytes != storedMsgSize*toStore {
		t.Fatalf("Expected bytes to be %d, got %d", storedMsgSize*toStore, state.Bytes)
	}

	// Now send 10 more and check that bytes limit enforced.
	for i := 0; i < 10; i++ {
		if _, _, err := fs.StoreMsg(subj, msg); err != nil {
			t.Fatalf("Error storing msg: %v", err)
		}
	}
	state = fs.State()
	if state.Msgs != toStore {
		t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
	}
	if state.Bytes != storedMsgSize*toStore {
		t.Fatalf("Expected bytes to be %d, got %d", storedMsgSize*toStore, state.Bytes)
	}
	if state.FirstSeq != 11 {
		t.Fatalf("Expected first sequence to be 11, got %d", state.FirstSeq)
	}
	if state.LastSeq != toStore+10 {
		t.Fatalf("Expected last sequence to be %d, got %d", toStore+10, state.LastSeq)
	}
}

func TestFileStoreAgeLimit(t *testing.T) {
	maxAge := 10 * time.Millisecond

	storeDir, _ := ioutil.TempDir("", JetStreamStoreDir)
	os.MkdirAll(storeDir, 0755)
	defer os.RemoveAll(storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, StreamConfig{Name: "zzz", Storage: FileStorage, MaxAge: maxAge})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	// Store some messages. Does not really matter how many.
	subj, msg := "foo", []byte("Hello World")
	toStore := 100
	for i := 0; i < toStore; i++ {
		fs.StoreMsg(subj, msg)
	}
	state := fs.State()
	if state.Msgs != uint64(toStore) {
		t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
	}
	checkExpired := func(t *testing.T) {
		t.Helper()
		checkFor(t, time.Second, maxAge, func() error {
			state = fs.State()
			if state.Msgs != 0 {
				return fmt.Errorf("Expected no msgs, got %d", state.Msgs)
			}
			if state.Bytes != 0 {
				return fmt.Errorf("Expected no bytes, got %d", state.Bytes)
			}
			return nil
		})
	}
	// Let them expire
	checkExpired(t)
	// Now add some more and make sure that timer will fire again.
	for i := 0; i < toStore; i++ {
		fs.StoreMsg(subj, msg)
	}
	state = fs.State()
	if state.Msgs != uint64(toStore) {
		t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
	}
	checkExpired(t)
}

func TestFileStoreTimeStamps(t *testing.T) {
	storeDir, _ := ioutil.TempDir("", JetStreamStoreDir)
	os.MkdirAll(storeDir, 0755)
	defer os.RemoveAll(storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	last := time.Now().UnixNano()
	subj, msg := "foo", []byte("Hello World")
	for i := 0; i < 10; i++ {
		time.Sleep(5 * time.Millisecond)
		fs.StoreMsg(subj, msg)
	}
	for seq := uint64(1); seq <= 10; seq++ {
		_, _, ts, err := fs.LoadMsg(seq)
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

	blkSize := uint64(64 * 1024)
	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir, BlockSize: blkSize}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	subj, msg := "foo", make([]byte, 8*1024)
	storedMsgSize := fileStoreMsgSize(subj, msg)

	toStore := uint64(1024)
	for i := uint64(0); i < toStore; i++ {
		fs.StoreMsg(subj, msg)
	}
	state := fs.State()
	if state.Msgs != toStore {
		t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
	}
	if state.Bytes != storedMsgSize*toStore {
		t.Fatalf("Expected bytes to be %d, got %d", storedMsgSize*toStore, state.Bytes)
	}

	expectedBlocks := int(storedMsgSize * toStore / blkSize)
	if numBlocks := fs.numMsgBlocks(); numBlocks <= expectedBlocks {
		t.Fatalf("Expected to have more then %d msg blocks, got %d", blkSize, numBlocks)
	}

	fs.Purge()

	if numBlocks := fs.numMsgBlocks(); numBlocks != 1 {
		t.Fatalf("Expected to have exactly 1 empty msg block, got %d", numBlocks)
	}

	checkPurgeState := func(stored uint64) {
		t.Helper()
		state = fs.State()
		if state.Msgs != 0 {
			t.Fatalf("Expected 0 msgs after purge, got %d", state.Msgs)
		}
		if state.Bytes != 0 {
			t.Fatalf("Expected 0 bytes after purge, got %d", state.Bytes)
		}
		if state.LastSeq != stored {
			t.Fatalf("Expected LastSeq to be %d., got %d", toStore, state.LastSeq)
		}
		if state.FirstSeq != stored+1 {
			t.Fatalf("Expected FirstSeq to be %d., got %d", toStore+1, state.FirstSeq)
		}
	}
	checkPurgeState(toStore)

	// Make sure we recover same state.
	fs.Stop()

	fs, err = newFileStore(FileStoreConfig{StoreDir: storeDir, BlockSize: blkSize}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	if numBlocks := fs.numMsgBlocks(); numBlocks != 1 {
		t.Fatalf("Expected to have exactly 1 empty msg block, got %d", numBlocks)
	}

	checkPurgeState(toStore)

	// Now make sure we clean up any dangling purged messages.
	for i := uint64(0); i < toStore; i++ {
		fs.StoreMsg(subj, msg)
	}
	state = fs.State()
	if state.Msgs != toStore {
		t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
	}
	if state.Bytes != storedMsgSize*toStore {
		t.Fatalf("Expected bytes to be %d, got %d", storedMsgSize*toStore, state.Bytes)
	}

	// We will simulate crashing before the purge directory is cleared.
	mdir := path.Join(storeDir, msgDir)
	pdir := path.Join(fs.fcfg.StoreDir, "ptest")
	os.Rename(mdir, pdir)
	os.MkdirAll(mdir, 0755)

	fs.Purge()
	checkPurgeState(toStore * 2)

	// Make sure we recover same state.
	fs.Stop()

	purgeDir := path.Join(fs.fcfg.StoreDir, purgeDir)
	os.Rename(pdir, purgeDir)

	fs, err = newFileStore(FileStoreConfig{StoreDir: storeDir, BlockSize: blkSize}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	if numBlocks := fs.numMsgBlocks(); numBlocks != 1 {
		t.Fatalf("Expected to have exactly 1 empty msg block, got %d", numBlocks)
	}

	checkPurgeState(toStore * 2)

	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		if _, err := os.Stat(purgeDir); err == nil {
			return fmt.Errorf("purge directory still present")
		}
		return nil
	})
}

func TestFileStoreRemovePartialRecovery(t *testing.T) {
	storeDir, _ := ioutil.TempDir("", JetStreamStoreDir)
	os.MkdirAll(storeDir, 0755)
	defer os.RemoveAll(storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	subj, msg := "foo", []byte("Hello World")
	toStore := 100
	for i := 0; i < toStore; i++ {
		fs.StoreMsg(subj, msg)
	}
	state := fs.State()
	if state.Msgs != uint64(toStore) {
		t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
	}

	// Remove half
	for i := 1; i <= toStore/2; i++ {
		fs.RemoveMsg(uint64(i))
	}

	state = fs.State()
	if state.Msgs != uint64(toStore/2) {
		t.Fatalf("Expected %d msgs, got %d", toStore/2, state.Msgs)
	}

	// Make sure we recover same state.
	fs.Stop()

	fs, err = newFileStore(FileStoreConfig{StoreDir: storeDir}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	state2 := fs.State()
	if state != state2 {
		t.Fatalf("Expected recovered state to be the same, got %+v vs %+v\n", state2, state)
	}
}

func TestFileStoreRemoveOutOfOrderRecovery(t *testing.T) {
	storeDir, _ := ioutil.TempDir("", JetStreamStoreDir)
	os.MkdirAll(storeDir, 0755)
	defer os.RemoveAll(storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	subj, msg := "foo", []byte("Hello World")
	toStore := 100
	for i := 0; i < toStore; i++ {
		fs.StoreMsg(subj, msg)
	}
	state := fs.State()
	if state.Msgs != uint64(toStore) {
		t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
	}

	// Remove evens
	for i := 2; i <= toStore; i += 2 {
		if removed, _ := fs.RemoveMsg(uint64(i)); !removed {
			t.Fatalf("Expected remove to return true")
		}
	}

	state = fs.State()
	if state.Msgs != uint64(toStore/2) {
		t.Fatalf("Expected %d msgs, got %d", toStore/2, state.Msgs)
	}

	if _, _, _, err := fs.LoadMsg(1); err != nil {
		t.Fatalf("Expected to retrieve seq 1")
	}
	for i := 2; i <= toStore; i += 2 {
		if _, _, _, err := fs.LoadMsg(uint64(i)); err == nil {
			t.Fatalf("Expected error looking up seq %d that should be deleted", i)
		}
	}

	// Make sure we recover same state.
	fs.Stop()

	fs, err = newFileStore(FileStoreConfig{StoreDir: storeDir}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	state2 := fs.State()
	if state != state2 {
		t.Fatalf("Expected receovered states to be the same, got %+v vs %+v\n", state, state2)
	}

	if _, _, _, err := fs.LoadMsg(1); err != nil {
		t.Fatalf("Expected to retrieve seq 1")
	}
	for i := 2; i <= toStore; i += 2 {
		if _, _, _, err := fs.LoadMsg(uint64(i)); err == nil {
			t.Fatalf("Expected error looking up seq %d that should be deleted", i)
		}
	}
}

func TestFileStoreAgeLimitRecovery(t *testing.T) {
	maxAge := 10 * time.Millisecond

	storeDir, _ := ioutil.TempDir("", JetStreamStoreDir)
	os.MkdirAll(storeDir, 0755)
	defer os.RemoveAll(storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, StreamConfig{Name: "zzz", Storage: FileStorage, MaxAge: maxAge})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	// Store some messages. Does not really matter how many.
	subj, msg := "foo", []byte("Hello World")
	toStore := 100
	for i := 0; i < toStore; i++ {
		fs.StoreMsg(subj, msg)
	}
	state := fs.State()
	if state.Msgs != uint64(toStore) {
		t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
	}
	fs.Stop()

	fs, err = newFileStore(FileStoreConfig{StoreDir: storeDir}, StreamConfig{Name: "zzz", Storage: FileStorage, MaxAge: maxAge})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	// Make sure they expire.
	checkFor(t, time.Second, 2*maxAge, func() error {
		state = fs.State()
		if state.Msgs != 0 {
			return fmt.Errorf("Expected no msgs, got %d", state.Msgs)
		}
		if state.Bytes != 0 {
			return fmt.Errorf("Expected no bytes, got %d", state.Bytes)
		}
		return nil
	})
}

func TestFileStoreBitRot(t *testing.T) {
	storeDir, _ := ioutil.TempDir("", JetStreamStoreDir)
	os.MkdirAll(storeDir, 0755)
	defer os.RemoveAll(storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	// Store some messages. Does not really matter how many.
	subj, msg := "foo", []byte("Hello World")
	toStore := 100
	for i := 0; i < toStore; i++ {
		fs.StoreMsg(subj, msg)
	}
	state := fs.State()
	if state.Msgs != uint64(toStore) {
		t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
	}

	if badSeqs := len(fs.checkMsgs()); badSeqs > 0 {
		t.Fatalf("Expected to have no corrupt msgs, got %d", badSeqs)
	}

	// Now twiddle some bits.
	fs.mu.Lock()
	lmb := fs.lmb
	contents, _ := ioutil.ReadFile(lmb.mfn)
	var index int
	for {
		index = rand.Intn(len(contents))
		// Reverse one byte anywhere.
		b := contents[index]
		contents[index] = bits.Reverse8(b)
		if b != contents[index] {
			break
		}
	}
	ioutil.WriteFile(lmb.mfn, contents, 0644)
	fs.mu.Unlock()

	bseqs := fs.checkMsgs()
	if badSeqs := len(bseqs); badSeqs == 0 {
		t.Fatalf("Expected to have corrupt msgs got none: changed [%d]", index)
	}

	// Make sure we can restore.
	fs.Stop()

	fs, err = newFileStore(FileStoreConfig{StoreDir: storeDir}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	if !reflect.DeepEqual(bseqs, fs.checkMsgs()) {
		t.Fatalf("Different reporting on bad msgs: %+v vs %+v", bseqs, fs.checkMsgs())
	}
}

func TestFileStoreEraseMsg(t *testing.T) {
	storeDir, _ := ioutil.TempDir("", JetStreamStoreDir)
	os.MkdirAll(storeDir, 0755)
	defer os.RemoveAll(storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	subj, msg := "foo", []byte("Hello World")
	fs.StoreMsg(subj, msg)
	_, smsg, _, err := fs.LoadMsg(1)
	if err != nil {
		t.Fatalf("Unexpected error looking up msg: %v", err)
	}
	if !bytes.Equal(msg, smsg) {
		t.Fatalf("Expected same msg, got %q vs %q", smsg, msg)
	}
	// Hold for offset check later.
	sm, _ := fs.msgForSeq(1)

	if removed, _ := fs.EraseMsg(1); !removed {
		t.Fatalf("Expected erase msg to return success")
	}
	if sm2, _ := fs.msgForSeq(1); sm2 != nil {
		t.Fatalf("Expected msg to be erased")
	}

	// Now look on disk as well.
	rl := fileStoreMsgSize(subj, msg)
	buf := make([]byte, rl)
	fp, err := os.Open(path.Join(storeDir, msgDir, fmt.Sprintf(blkScan, 1)))
	if err != nil {
		t.Fatalf("Error opening msgs file: %v", err)
	}
	defer fp.Close()
	fp.ReadAt(buf, sm.off)
	nsubj, nmsg, seq, ts, err := msgFromBuf(buf, nil)
	if err != nil {
		t.Fatalf("error reading message from block: %v", err)
	}
	if nsubj == subj {
		t.Fatalf("Expected the subjects to be different")
	}
	if seq != 0 {
		t.Fatalf("Expected seq to be 0, marking as deleted, got %d", seq)
	}
	if ts != 0 {
		t.Fatalf("Expected timestamp to be 0, got %d", ts)
	}
	if bytes.Equal(nmsg, msg) {
		t.Fatalf("Expected message body to be randomized")
	}
}

func TestFileStoreEraseAndNoIndexRecovery(t *testing.T) {
	storeDir, _ := ioutil.TempDir("", JetStreamStoreDir)
	os.MkdirAll(storeDir, 0755)
	defer os.RemoveAll(storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	subj, msg := "foo", []byte("Hello World")
	toStore := 100
	for i := 0; i < toStore; i++ {
		fs.StoreMsg(subj, msg)
	}
	state := fs.State()
	if state.Msgs != uint64(toStore) {
		t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
	}

	// Erase the even messages.
	for i := 2; i <= toStore; i += 2 {
		if removed, _ := fs.EraseMsg(uint64(i)); !removed {
			t.Fatalf("Expected erase msg to return true")
		}
	}
	state = fs.State()
	if state.Msgs != uint64(toStore/2) {
		t.Fatalf("Expected %d msgs, got %d", toStore/2, state.Msgs)
	}

	// Stop and remove the index file.
	fs.Stop()
	ifn := path.Join(storeDir, msgDir, fmt.Sprintf(indexScan, 1))
	os.Remove(ifn)

	fs, err = newFileStore(FileStoreConfig{StoreDir: storeDir}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	state = fs.State()
	if state.Msgs != uint64(toStore/2) {
		t.Fatalf("Expected %d msgs, got %d", toStore/2, state.Msgs)
	}

	for i := 2; i <= toStore; i += 2 {
		if _, _, _, err := fs.LoadMsg(uint64(i)); err == nil {
			t.Fatalf("Expected error looking up seq %d that should be erased", i)
		}
	}
}

func TestFileStoreMeta(t *testing.T) {
	storeDir, _ := ioutil.TempDir("", JetStreamStoreDir)
	os.MkdirAll(storeDir, 0755)
	defer os.RemoveAll(storeDir)

	mconfig := StreamConfig{Name: "ZZ-22-33", Storage: FileStorage, Subjects: []string{"foo.*"}, Replicas: 22}

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, mconfig)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	metafile := path.Join(storeDir, JetStreamMetaFile)
	metasum := path.Join(storeDir, JetStreamMetaFileSum)

	// Test to make sure meta file and checksum are present.
	if _, err := os.Stat(metafile); os.IsNotExist(err) {
		t.Fatalf("Expected metafile %q to exist", metafile)
	}
	if _, err := os.Stat(metasum); os.IsNotExist(err) {
		t.Fatalf("Expected metafile's checksum %q to exist", metasum)
	}

	buf, err := ioutil.ReadFile(metafile)
	if err != nil {
		t.Fatalf("Error reading metafile: %v", err)
	}
	var mconfig2 StreamConfig
	if err := json.Unmarshal(buf, &mconfig2); err != nil {
		t.Fatalf("Error unmarshalling: %v", err)
	}
	if !reflect.DeepEqual(mconfig, mconfig2) {
		t.Fatalf("Stream configs not equal, got %+v vs %+v", mconfig2, mconfig)
	}
	checksum, err := ioutil.ReadFile(metasum)
	if err != nil {
		t.Fatalf("Error reading metafile checksum: %v", err)
	}
	fs.hh.Reset()
	fs.hh.Write(buf)
	mychecksum := hex.EncodeToString(fs.hh.Sum(nil))
	if mychecksum != string(checksum) {
		t.Fatalf("Checksums do not match, got %q vs %q", mychecksum, checksum)
	}

	// Now create an observable. Same deal for them.
	oconfig := ConsumerConfig{
		DeliverSubject: "d",
		FilterSubject:  "foo",
		AckPolicy:      AckAll,
	}
	oname := "obs22"
	obs, err := fs.ConsumerStore(oname, &oconfig)
	if err != nil {
		t.Fatalf("Unexepected error: %v", err)
	}

	ometafile := path.Join(storeDir, consumerDir, oname, JetStreamMetaFile)
	ometasum := path.Join(storeDir, consumerDir, oname, JetStreamMetaFileSum)

	// Test to make sure meta file and checksum are present.
	if _, err := os.Stat(ometafile); os.IsNotExist(err) {
		t.Fatalf("Expected consumer metafile %q to exist", ometafile)
	}
	if _, err := os.Stat(ometasum); os.IsNotExist(err) {
		t.Fatalf("Expected consumer metafile's checksum %q to exist", ometasum)
	}

	buf, err = ioutil.ReadFile(ometafile)
	if err != nil {
		t.Fatalf("Error reading consumer metafile: %v", err)
	}

	var oconfig2 ConsumerConfig
	if err := json.Unmarshal(buf, &oconfig2); err != nil {
		t.Fatalf("Error unmarshalling: %v", err)
	}
	if oconfig2 != oconfig {
		t.Fatalf("Consumer configs not equal, got %+v vs %+v", oconfig2, oconfig)
	}
	checksum, err = ioutil.ReadFile(ometasum)

	if err != nil {
		t.Fatalf("Error reading consumer metafile checksum: %v", err)
	}

	hh := obs.(*consumerFileStore).hh
	hh.Reset()
	hh.Write(buf)
	mychecksum = hex.EncodeToString(hh.Sum(nil))
	if mychecksum != string(checksum) {
		t.Fatalf("Checksums do not match, got %q vs %q", mychecksum, checksum)
	}
}

func TestFileStoreWriteAndReadSameBlock(t *testing.T) {
	storeDir, _ := ioutil.TempDir("", JetStreamStoreDir)
	os.MkdirAll(storeDir, 0755)
	defer os.RemoveAll(storeDir)

	fs, err := newFileStore(
		FileStoreConfig{StoreDir: storeDir},
		StreamConfig{Name: "zzz", Storage: FileStorage},
	)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	subj, msg := "foo", []byte("Hello World!")

	for i := uint64(1); i <= 10; i++ {
		fs.StoreMsg(subj, msg)
		if _, _, _, err := fs.LoadMsg(i); err != nil {
			t.Fatalf("Error loading %d: %v", i, err)
		}
	}
}

func TestFileStoreAndRetrieveMultiBlock(t *testing.T) {
	storeDir, _ := ioutil.TempDir("", JetStreamStoreDir)
	os.MkdirAll(storeDir, 0755)
	defer os.RemoveAll(storeDir)

	subj, msg := "foo", []byte("Hello World!")
	storedMsgSize := fileStoreMsgSize(subj, msg)

	fs, err := newFileStore(
		FileStoreConfig{StoreDir: storeDir, BlockSize: 4 * storedMsgSize},
		StreamConfig{Name: "zzz", Storage: FileStorage},
	)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for i := 0; i < 20; i++ {
		fs.StoreMsg(subj, msg)
	}
	state := fs.State()
	if state.Msgs != 20 {
		t.Fatalf("Expected 20 msgs, got %d", state.Msgs)
	}
	fs.Stop()

	fs, err = newFileStore(
		FileStoreConfig{StoreDir: storeDir, BlockSize: 4 * storedMsgSize},
		StreamConfig{Name: "zzz", Storage: FileStorage},
	)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for i := uint64(1); i <= 20; i++ {
		if _, _, _, err := fs.LoadMsg(i); err != nil {
			t.Fatalf("Error loading %d: %v", i, err)
		}
	}
}

func TestFileStoreCollapseDmap(t *testing.T) {
	storeDir, _ := ioutil.TempDir("", JetStreamStoreDir)
	os.MkdirAll(storeDir, 0755)
	defer os.RemoveAll(storeDir)

	subj, msg := "foo", []byte("Hello World!")
	storedMsgSize := fileStoreMsgSize(subj, msg)

	fs, err := newFileStore(
		FileStoreConfig{StoreDir: storeDir, BlockSize: 4 * storedMsgSize},
		StreamConfig{Name: "zzz", Storage: FileStorage},
	)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for i := 0; i < 10; i++ {
		fs.StoreMsg(subj, msg)
	}
	state := fs.State()
	if state.Msgs != 10 {
		t.Fatalf("Expected 10 msgs, got %d", state.Msgs)
	}

	checkDmapTotal := func(total int) {
		t.Helper()
		if nde := fs.dmapEntries(); nde != total {
			t.Fatalf("Expecting only %d entries, got %d", total, nde)
		}
	}

	checkFirstSeq := func(seq uint64) {
		t.Helper()
		state := fs.State()
		if state.FirstSeq != seq {
			t.Fatalf("Expected first seq to be %d, got %d", seq, state.FirstSeq)
		}
	}

	// Now remove some out of order, forming gaps and entries in dmaps.
	fs.RemoveMsg(2)
	checkFirstSeq(1)
	fs.RemoveMsg(4)
	checkFirstSeq(1)
	fs.RemoveMsg(8)
	checkFirstSeq(1)

	state = fs.State()
	if state.Msgs != 7 {
		t.Fatalf("Expected 7 msgs, got %d", state.Msgs)
	}

	checkDmapTotal(3)

	// Close gaps..
	fs.RemoveMsg(1)
	checkDmapTotal(2)
	checkFirstSeq(3)

	fs.RemoveMsg(3)
	checkDmapTotal(1)
	checkFirstSeq(5)

	fs.RemoveMsg(5)
	checkDmapTotal(1)
	checkFirstSeq(6)

	fs.RemoveMsg(7)
	checkDmapTotal(2)

	fs.RemoveMsg(6)
	checkDmapTotal(0)
}

func TestFileStoreReadCache(t *testing.T) {
	subj, msg := "foo.bar", make([]byte, 1024)
	storedMsgSize := fileStoreMsgSize(subj, msg)

	storeDir, _ := ioutil.TempDir("", JetStreamStoreDir)
	os.MkdirAll(storeDir, 0755)
	defer os.RemoveAll(storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir, ReadCacheExpire: 50 * time.Millisecond}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	toStore := 500
	totalBytes := uint64(toStore) * storedMsgSize

	for i := 0; i < toStore; i++ {
		fs.StoreMsg(subj, msg)
	}

	fs.LoadMsg(1)
	if csz := fs.cacheSize(); csz != totalBytes {
		t.Fatalf("Expected all messages to be cached, got %d vs %d", csz, totalBytes)
	}
	// Should expire and be removed.
	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		if csz := fs.cacheSize(); csz != 0 {
			return fmt.Errorf("cache size not 0, got %s", FriendlyBytes(int64(csz)))
		}
		return nil
	})
	if cls := fs.cacheLoads(); cls != 1 {
		t.Fatalf("Expected only 1 cache load, got %d", cls)
	}
	// Now make sure we do not reload cache if there is activity.
	fs.LoadMsg(1)
	timeout := time.Now().Add(250 * time.Millisecond)
	for time.Now().Before(timeout) {
		if cls := fs.cacheLoads(); cls != 2 {
			t.Fatalf("cache loads not 2, got %d", cls)
		}
		time.Sleep(5 * time.Millisecond)
		fs.LoadMsg(1) // register activity.
	}
}

func TestFileStoreSnapshot(t *testing.T) {
	storeDir, _ := ioutil.TempDir("", JetStreamStoreDir)
	os.MkdirAll(storeDir, 0755)
	defer os.RemoveAll(storeDir)

	subj, msg := "foo", []byte("Hello Snappy!")

	fs, err := newFileStore(
		FileStoreConfig{StoreDir: storeDir},
		StreamConfig{Name: "zzz", Storage: FileStorage},
	)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	toSend := 2233
	for i := 0; i < toSend; i++ {
		fs.StoreMsg(subj, msg)
	}

	// Create a few consumers.
	o1, err := fs.ConsumerStore("o22", &ConsumerConfig{})
	if err != nil {
		t.Fatalf("Unexepected error: %v", err)
	}
	o2, err := fs.ConsumerStore("o33", &ConsumerConfig{})
	if err != nil {
		t.Fatalf("Unexepected error: %v", err)
	}
	state := &ConsumerState{}
	state.Delivered.ConsumerSeq = 100
	state.Delivered.StreamSeq = 100
	state.AckFloor.ConsumerSeq = 22
	state.AckFloor.StreamSeq = 22

	if err := o1.Update(state); err != nil {
		t.Fatalf("Unexepected error updating state: %v", err)
	}
	state.AckFloor.ConsumerSeq = 33
	state.AckFloor.StreamSeq = 33

	if err := o2.Update(state); err != nil {
		t.Fatalf("Unexepected error updating state: %v", err)
	}

	snapshot := func() []byte {
		t.Helper()
		r, err := fs.Snapshot(5*time.Second, true, true)
		if err != nil {
			t.Fatalf("Error creating snapshot")
		}
		snapshot, err := ioutil.ReadAll(r.Reader)
		if err != nil {
			t.Fatalf("Error reading snapshot")
		}
		return snapshot
	}

	// This will unzip the snapshot and create a new filestore that will recover the state.
	// We will compare the states for this vs the original one.
	verifySnapshot := func(snap []byte) {
		r := bytes.NewReader(snap)
		gzr, err := gzip.NewReader(r)
		if err != nil {
			t.Fatalf("Error creating gzip reader: %v", err)
		}
		defer gzr.Close()
		tr := tar.NewReader(gzr)

		rstoreDir, _ := ioutil.TempDir("", JetStreamStoreDir)
		defer os.RemoveAll(rstoreDir)

		for {
			hdr, err := tr.Next()
			if err == io.EOF {
				break // End of archive
			}
			if err != nil {
				t.Fatalf("Error getting next entry from snapshot: %v", err)
			}
			fpath := path.Join(rstoreDir, filepath.Clean(hdr.Name))
			pdir := filepath.Dir(fpath)
			os.MkdirAll(pdir, 0755)
			fd, err := os.OpenFile(fpath, os.O_CREATE|os.O_RDWR, 0600)
			if err != nil {
				t.Fatalf("Error opening file[%s]: %v", fpath, err)
			}
			if _, err := io.Copy(fd, tr); err != nil {
				t.Fatalf("Error writing file[%s]: %v", fpath, err)
			}
			fd.Close()
		}

		fsr, err := newFileStore(
			FileStoreConfig{StoreDir: rstoreDir},
			StreamConfig{Name: "zzz", Storage: FileStorage},
		)
		if err != nil {
			t.Fatalf("Error restoring from snapshot: %v", err)
		}
		state := fs.State()
		rstate := fsr.State()

		// FIXME(dlc)
		// Right now the upper layers in JetStream recover the consumers and do not expect
		// the lower layers to do that. So for now blank that out of our original state.
		// Will have more exhaustive tests in jetstream_test.go.
		state.Consumers = 0

		// FIXME(dlc) - Also the hashes will not match if directory is not the same, so need to
		// work through that problem too. The test below will pass but if you try to extract a
		// message that will most likely fail.
		if rstate != state {
			t.Fatalf("Restored state does not match, %+v vs %+v", rstate, state)
		}
	}

	// Simple case first.
	snap := snapshot()
	verifySnapshot(snap)

	// Remove first 100 messages.
	for i := 1; i <= 100; i++ {
		fs.RemoveMsg(uint64(i))
	}

	snap = snapshot()
	verifySnapshot(snap)

	// Now sporadic messages inside the stream.
	total := int64(toSend - 100)
	// Delete 50 random messages.
	for i := 0; i < 50; i++ {
		seq := uint64(rand.Int63n(total) + 101)
		fs.RemoveMsg(seq)
	}

	snap = snapshot()
	verifySnapshot(snap)

	// Now check to make sure that we get the correct error when trying to delete or erase
	// a message when a snapshot is in progress and that closing the reader releases that condition.
	sr, err := fs.Snapshot(5*time.Second, true, false)
	if err != nil {
		t.Fatalf("Error creating snapshot")
	}
	if _, err := fs.RemoveMsg(122); err != ErrStoreSnapshotInProgress {
		t.Fatalf("Did not get the correct error on remove during snapshot: %v", err)
	}
	if _, err := fs.EraseMsg(122); err != ErrStoreSnapshotInProgress {
		t.Fatalf("Did not get the correct error on remove during snapshot: %v", err)
	}

	// Now make sure we can do these when we close the reader and release the snapshot condition.
	sr.Reader.Close()
	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		if _, err := fs.RemoveMsg(122); err != nil {
			return fmt.Errorf("Got an error on remove after snapshot: %v", err)
		}
		return nil
	})

	// Make sure if we do not read properly then it will close the writer and report an error.
	sr, err = fs.Snapshot(25*time.Millisecond, false, false)
	if err != nil {
		t.Fatalf("Error creating snapshot")
	}

	// Cause snapshot to timeout.
	time.Sleep(30 * time.Millisecond)
	// Read should fail
	var buf [32]byte
	if _, err := sr.Reader.Read(buf[:]); err != io.EOF {
		t.Fatalf("Expected read to produce an error, got none")
	}
}

func TestFileStoreConsumer(t *testing.T) {
	storeDir, _ := ioutil.TempDir("", JetStreamStoreDir)
	os.MkdirAll(storeDir, 0755)
	defer os.RemoveAll(storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	o, err := fs.ConsumerStore("obs22", &ConsumerConfig{})
	if err != nil {
		t.Fatalf("Unexepected error: %v", err)
	}
	if state, err := o.State(); state != nil || err != nil {
		t.Fatalf("Unexpected state or error: %v", err)
	}
	state := &ConsumerState{}
	if err := o.Update(state); err == nil {
		t.Fatalf("Expected an error and got none")
	}

	updateAndCheck := func() {
		t.Helper()
		if err := o.Update(state); err != nil {
			t.Fatalf("Unexepected error updating state: %v", err)
		}
		s2, err := o.State()
		if err != nil {
			t.Fatalf("Unexepected error getting state: %v", err)
		}
		if !reflect.DeepEqual(state, s2) {
			t.Fatalf("State is not the same: wanted %+v got %+v", state, s2)
		}
	}

	shouldFail := func() {
		t.Helper()
		if err := o.Update(state); err == nil {
			t.Fatalf("Expected an error and got none")
		}
	}

	state.Delivered.ConsumerSeq = 1
	state.Delivered.StreamSeq = 22
	updateAndCheck()

	state.Delivered.ConsumerSeq = 100
	state.Delivered.StreamSeq = 122
	state.AckFloor.ConsumerSeq = 50
	state.AckFloor.StreamSeq = 123
	// This should fail, bad state.
	shouldFail()
	// So should this.
	state.AckFloor.ConsumerSeq = 200
	state.AckFloor.StreamSeq = 100
	shouldFail()

	// Should succeed
	state.AckFloor.ConsumerSeq = 50
	state.AckFloor.StreamSeq = 72
	updateAndCheck()

	tn := time.Now().UnixNano()

	// We should sanity check pending here as well, so will check if a pending value is below
	// ack floor or above delivered.
	state.Pending = map[uint64]int64{70: tn}
	shouldFail()
	state.Pending = map[uint64]int64{140: tn}
	shouldFail()
	state.Pending = map[uint64]int64{72: tn} // exact on floor should fail
	shouldFail()

	// Put timestamps a second apart.
	// We will downsample to second resolution to save space. So setup our times
	// to reflect that.
	ago := time.Now().Add(-30 * time.Second).Truncate(time.Second)
	nt := func() int64 {
		ago = ago.Add(time.Second)
		return ago.UnixNano()
	}
	// Should succeed.
	state.Pending = map[uint64]int64{75: nt(), 80: nt(), 83: nt(), 90: nt(), 111: nt()}
	updateAndCheck()

	// Now do redlivery, but first with no pending.
	state.Pending = nil
	state.Redelivered = map[uint64]uint64{22: 3, 44: 8}
	updateAndCheck()

	// All together.
	state.Pending = map[uint64]int64{75: nt(), 80: nt(), 83: nt(), 90: nt(), 111: nt()}
	updateAndCheck()

	// Large one
	state.Delivered.ConsumerSeq = 10000
	state.Delivered.StreamSeq = 10000
	state.AckFloor.ConsumerSeq = 100
	state.AckFloor.StreamSeq = 100
	// Generate 8k pending.
	state.Pending = make(map[uint64]int64)
	for len(state.Pending) < 8192 {
		seq := uint64(rand.Intn(9890) + 101)
		if _, ok := state.Pending[seq]; !ok {
			state.Pending[seq] = nt()
		}
	}
	updateAndCheck()

	state.Pending = nil
	state.AckFloor.ConsumerSeq = 10000
	state.AckFloor.StreamSeq = 10000
	updateAndCheck()
}

func TestFileStorePerf(t *testing.T) {
	// Comment out to run, holding place for now.
	t.SkipNow()

	subj, msg := "foo", make([]byte, 1024-33)
	for i := 0; i < len(msg); i++ {
		msg[i] = 'D'
	}
	storedMsgSize := fileStoreMsgSize(subj, msg)

	// 5GB
	toStore := 5 * 1024 * 1024 * 1024 / storedMsgSize

	fmt.Printf("storing %d msgs of %s each, totalling %s\n",
		toStore,
		FriendlyBytes(int64(storedMsgSize)),
		FriendlyBytes(int64(toStore*storedMsgSize)),
	)

	storeDir, _ := ioutil.TempDir("", JetStreamStoreDir)
	os.MkdirAll(storeDir, 0755)
	defer os.RemoveAll(storeDir)

	fs, err := newFileStore(
		FileStoreConfig{StoreDir: storeDir},
		StreamConfig{Name: "zzz", Storage: FileStorage},
	)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	start := time.Now()
	for i := 0; i < int(toStore); i++ {
		fs.StoreMsg(subj, msg)
	}
	fs.Stop()

	tt := time.Since(start)
	fmt.Printf("time to store is %v\n", tt)
	fmt.Printf("%.0f msgs/sec\n", float64(toStore)/tt.Seconds())
	fmt.Printf("%s per sec\n", FriendlyBytes(int64(float64(toStore*storedMsgSize)/tt.Seconds())))

	fmt.Printf("Filesystem cache flush, paused 5 seconds.\n\n")
	time.Sleep(5 * time.Second)

	fmt.Printf("Restoring..\n")
	start = time.Now()
	fs, err = newFileStore(
		FileStoreConfig{StoreDir: storeDir},
		StreamConfig{Name: "zzz", Storage: FileStorage},
	)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	fmt.Printf("time to restore is %v\n\n", time.Since(start))

	fmt.Printf("LOAD: reading %d msgs of %s each, totalling %s\n",
		toStore,
		FriendlyBytes(int64(storedMsgSize)),
		FriendlyBytes(int64(toStore*storedMsgSize)),
	)

	start = time.Now()
	for i := uint64(1); i <= toStore; i++ {
		if _, _, _, err := fs.LoadMsg(i); err != nil {
			t.Fatalf("Error loading %d: %v", i, err)
		}
	}

	tt = time.Since(start)
	fmt.Printf("time to read all back messages is %v\n", tt)
	fmt.Printf("%.0f msgs/sec\n", float64(toStore)/tt.Seconds())
	fmt.Printf("%s per sec\n", FriendlyBytes(int64(float64(toStore*storedMsgSize)/tt.Seconds())))

	// Do again to test skip for hash..
	fmt.Printf("\nSKIP CHECKSUM: reading %d msgs of %s each, totalling %s\n",
		toStore,
		FriendlyBytes(int64(storedMsgSize)),
		FriendlyBytes(int64(toStore*storedMsgSize)),
	)

	start = time.Now()
	for i := uint64(1); i <= toStore; i++ {
		if _, _, _, err := fs.LoadMsg(i); err != nil {
			t.Fatalf("Error loading %d: %v", i, err)
		}
	}

	tt = time.Since(start)
	fmt.Printf("time to read all back messages is %v\n", tt)
	fmt.Printf("%.0f msgs/sec\n", float64(toStore)/tt.Seconds())
	fmt.Printf("%s per sec\n", FriendlyBytes(int64(float64(toStore*storedMsgSize)/tt.Seconds())))

	fs.Stop()

	fs, err = newFileStore(
		FileStoreConfig{StoreDir: storeDir},
		StreamConfig{Name: "zzz", Storage: FileStorage},
	)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	fmt.Printf("\nremoving [in order] %d msgs of %s each, totalling %s\n",
		toStore,
		FriendlyBytes(int64(storedMsgSize)),
		FriendlyBytes(int64(toStore*storedMsgSize)),
	)

	start = time.Now()
	// For reverse order.
	//for i := toStore; i > 0; i-- {
	for i := uint64(1); i <= toStore; i++ {
		fs.RemoveMsg(i)
	}
	fs.Stop()

	tt = time.Since(start)
	fmt.Printf("time to remove all messages is %v\n", tt)
	fmt.Printf("%.0f msgs/sec\n", float64(toStore)/tt.Seconds())
	fmt.Printf("%s per sec\n", FriendlyBytes(int64(float64(toStore*storedMsgSize)/tt.Seconds())))

	fs, err = newFileStore(
		FileStoreConfig{StoreDir: storeDir},
		StreamConfig{Name: "zzz", Storage: FileStorage},
	)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	state := fs.State()
	if state.Msgs != 0 {
		t.Fatalf("Expected no msgs, got %d", state.Msgs)
	}
	if state.Bytes != 0 {
		t.Fatalf("Expected no bytes, got %d", state.Bytes)
	}
}

func TestFileStoreReadBackMsgPerf(t *testing.T) {
	// Comment out to run, holding place for now.
	t.SkipNow()

	subj := "foo"
	msg := []byte("ABCDEFGH") // Smaller shows problems more.

	storedMsgSize := fileStoreMsgSize(subj, msg)

	// Make sure we store 2 blocks.
	toStore := defaultStreamBlockSize * 2 / storedMsgSize

	fmt.Printf("storing %d msgs of %s each, totalling %s\n",
		toStore,
		FriendlyBytes(int64(storedMsgSize)),
		FriendlyBytes(int64(toStore*storedMsgSize)),
	)

	storeDir, _ := ioutil.TempDir("", JetStreamStoreDir)
	os.MkdirAll(storeDir, 0755)
	defer os.RemoveAll(storeDir)
	fmt.Printf("StoreDir is %q\n", storeDir)

	fs, err := newFileStore(
		FileStoreConfig{StoreDir: storeDir},
		StreamConfig{Name: "zzz", Storage: FileStorage},
	)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	start := time.Now()
	for i := 0; i < int(toStore); i++ {
		fs.StoreMsg(subj, msg)
	}

	tt := time.Since(start)
	fmt.Printf("time to store is %v\n", tt)
	fmt.Printf("%.0f msgs/sec\n", float64(toStore)/tt.Seconds())
	fmt.Printf("%s per sec\n", FriendlyBytes(int64(float64(toStore*storedMsgSize)/tt.Seconds())))

	// We should not have cached here with no reads.
	// Pick something towards end of the block.
	index := defaultStreamBlockSize/storedMsgSize - 22
	start = time.Now()
	fs.LoadMsg(index)
	fmt.Printf("Time to load first msg [%d] = %v\n", index, time.Since(start))

	start = time.Now()
	fs.LoadMsg(index + 2)
	fmt.Printf("Time to load second msg [%d] = %v\n", index+2, time.Since(start))
}

func TestFileStoreConsumerPerf(t *testing.T) {
	// Comment out to run, holding place for now.
	t.SkipNow()

	storeDir, _ := ioutil.TempDir("", JetStreamStoreDir)
	os.MkdirAll(storeDir, 0755)
	defer os.RemoveAll(storeDir)
	fmt.Printf("StoreDir is %q\n", storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	// Test Consumers.
	o, err := fs.ConsumerStore("obs22", &ConsumerConfig{})
	if err != nil {
		t.Fatalf("Unexepected error: %v", err)
	}
	state := &ConsumerState{}
	toStore := uint64(1000000)
	fmt.Printf("consumer of %d msgs for ACK NONE\n", toStore)

	start := time.Now()
	for i := uint64(1); i <= toStore; i++ {
		state.Delivered.ConsumerSeq = i
		state.Delivered.StreamSeq = i
		state.AckFloor.ConsumerSeq = i
		state.AckFloor.StreamSeq = i
		if err := o.Update(state); err != nil {
			t.Fatalf("Unexepected error updating state: %v", err)
		}
	}
	tt := time.Since(start)
	fmt.Printf("time is %v\n", tt)
	fmt.Printf("%.0f updates/sec\n", float64(toStore)/tt.Seconds())

	// We will lag behind with pending.
	state.Pending = make(map[uint64]int64)
	lag := uint64(100)
	state.AckFloor.ConsumerSeq = 0
	state.AckFloor.StreamSeq = 0

	fmt.Printf("\nconsumer of %d msgs for ACK EXPLICIT with pending lag of %d\n", toStore, lag)

	start = time.Now()
	for i := uint64(1); i <= toStore; i++ {
		state.Delivered.ConsumerSeq = i
		state.Delivered.StreamSeq = i
		state.Pending[i] = time.Now().UnixNano()
		if i > lag {
			ackseq := i - lag
			state.AckFloor.ConsumerSeq = ackseq
			state.AckFloor.StreamSeq = ackseq
			delete(state.Pending, ackseq)
		}
		if err := o.Update(state); err != nil {
			t.Fatalf("Unexepected error updating state: %v", err)
		}
	}
	tt = time.Since(start)
	fmt.Printf("time is %v\n", tt)
	fmt.Printf("%.0f updates/sec\n", float64(toStore)/tt.Seconds())
}
