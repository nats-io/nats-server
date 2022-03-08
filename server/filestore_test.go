// Copyright 2019-2021 The NATS Authors
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
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/bits"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/s2"
)

func TestFileStoreBasics(t *testing.T) {
	storeDir := createDir(t, JetStreamStoreDir)
	defer removeDir(t, storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	subj, msg := "foo", []byte("Hello World")
	now := time.Now().UnixNano()
	for i := 1; i <= 5; i++ {
		if seq, ts, err := fs.StoreMsg(subj, nil, msg); err != nil {
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
	expectedSize := 5 * fileStoreMsgSize(subj, nil, msg)
	if state.Bytes != expectedSize {
		t.Fatalf("Expected %d bytes, got %d", expectedSize, state.Bytes)
	}

	nsubj, _, nmsg, _, err := fs.LoadMsg(2)
	if err != nil {
		t.Fatalf("Unexpected error looking up msg: %v", err)
	}
	if nsubj != subj {
		t.Fatalf("Subjects don't match, original %q vs %q", subj, nsubj)
	}
	if !bytes.Equal(nmsg, msg) {
		t.Fatalf("Msgs don't match, original %q vs %q", msg, nmsg)
	}
	_, _, _, _, err = fs.LoadMsg(3)
	if err != nil {
		t.Fatalf("Unexpected error looking up msg: %v", err)
	}

	remove := func(seq, expectedMsgs uint64) {
		t.Helper()
		removed, err := fs.RemoveMsg(seq)
		if err != nil {
			t.Fatalf("Got an error on remove of %d: %v", seq, err)
		}
		if !removed {
			t.Fatalf("Expected remove to return true for %d", seq)
		}
		if state := fs.State(); state.Msgs != expectedMsgs {
			t.Fatalf("Expected %d msgs, got %d", expectedMsgs, state.Msgs)
		}
	}

	// Remove first
	remove(1, 4)
	// Remove last
	remove(5, 3)
	// Remove a middle
	remove(3, 2)
}

func TestFileStoreMsgHeaders(t *testing.T) {
	storeDir := createDir(t, JetStreamStoreDir)
	defer removeDir(t, storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	subj, hdr, msg := "foo", []byte("name:derek"), []byte("Hello World")
	elen := 22 + len(subj) + 4 + len(hdr) + len(msg) + 8
	if sz := int(fileStoreMsgSize(subj, hdr, msg)); sz != elen {
		t.Fatalf("Wrong size for stored msg with header")
	}
	fs.StoreMsg(subj, hdr, msg)
	_, shdr, smsg, _, err := fs.LoadMsg(1)
	if err != nil {
		t.Fatalf("Unexpected error looking up msg: %v", err)
	}
	if !bytes.Equal(msg, smsg) {
		t.Fatalf("Expected same msg, got %q vs %q", smsg, msg)
	}
	if !bytes.Equal(hdr, shdr) {
		t.Fatalf("Expected same hdr, got %q vs %q", shdr, hdr)
	}
	if removed, _ := fs.EraseMsg(1); !removed {
		t.Fatalf("Expected erase msg to return success")
	}
}

func TestFileStoreBasicWriteMsgsAndRestore(t *testing.T) {
	storeDir := createDir(t, JetStreamStoreDir)
	defer removeDir(t, storeDir)

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
		if seq, _, err := fs.StoreMsg(subj, nil, msg); err != nil {
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
	expectedSize := toStore * fileStoreMsgSize(subj, nil, msg22)

	if state.Bytes != expectedSize {
		t.Fatalf("Expected %d bytes, got %d", expectedSize, state.Bytes)
	}
	// Stop will flush to disk.
	fs.Stop()

	// Make sure Store call after does not work.
	if _, _, err := fs.StoreMsg(subj, nil, []byte("no work")); err == nil {
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
		if seq, _, err := fs.StoreMsg(subj, nil, msg); err != nil {
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

	fs.Purge()
	fs.Stop()

	fs, err = newFileStore(fcfg, StreamConfig{Name: "dlc", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	state = fs.State()
	if state.Msgs != 0 {
		t.Fatalf("Expected %d msgs, got %d", 0, state.Msgs)
	}
	if state.Bytes != 0 {
		t.Fatalf("Expected %d bytes, got %d", 0, state.Bytes)
	}

	seq, _, err := fs.StoreMsg(subj, nil, []byte("Hello"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	fs.RemoveMsg(seq)

	fs.Stop()
	fs, err = newFileStore(fcfg, StreamConfig{Name: "dlc", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	state = fs.State()
	if state.FirstSeq != seq+1 {
		t.Fatalf("Expected first seq to be %d, got %d", seq+1, state.FirstSeq)
	}
}

func TestFileStoreSelectNextFirst(t *testing.T) {
	storeDir := createDir(t, JetStreamStoreDir)
	defer removeDir(t, storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir, BlockSize: 256}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	numMsgs := 10
	subj, msg := "zzz", []byte("Hello World")
	for i := 0; i < numMsgs; i++ {
		fs.StoreMsg(subj, nil, msg)
	}
	if state := fs.State(); state.Msgs != uint64(numMsgs) {
		t.Fatalf("Expected %d msgs, got %d", numMsgs, state.Msgs)
	}

	// Note the 256 block size is tied to the msg size below to give us 5 messages per block.
	if fmb := fs.selectMsgBlock(1); fmb.msgs != 5 {
		t.Fatalf("Expected 5 messages per block, but got %d", fmb.msgs)
	}

	// Delete 2-7, this will cross message blocks.
	for i := 2; i <= 7; i++ {
		fs.RemoveMsg(uint64(i))
	}

	if state := fs.State(); state.Msgs != 4 || state.FirstSeq != 1 {
		t.Fatalf("Expected 4 msgs, first seq of 11, got msgs of %d and first seq of %d", state.Msgs, state.FirstSeq)
	}
	// Now close the gap which will force the system to jump underlying message blocks to find the right sequence.
	fs.RemoveMsg(1)
	if state := fs.State(); state.Msgs != 3 || state.FirstSeq != 8 {
		t.Fatalf("Expected 3 msgs, first seq of 8, got msgs of %d and first seq of %d", state.Msgs, state.FirstSeq)
	}
}

func TestFileStoreSkipMsg(t *testing.T) {
	storeDir := createDir(t, JetStreamStoreDir)
	defer removeDir(t, storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir, BlockSize: 256}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	numSkips := 10
	for i := 0; i < numSkips; i++ {
		fs.SkipMsg()
	}
	state := fs.State()
	if state.Msgs != 0 {
		t.Fatalf("Expected %d msgs, got %d", 0, state.Msgs)
	}
	if state.FirstSeq != uint64(numSkips+1) || state.LastSeq != uint64(numSkips) {
		t.Fatalf("Expected first to be %d and last to be %d. got first %d and last %d", numSkips+1, numSkips, state.FirstSeq, state.LastSeq)
	}

	fs.StoreMsg("zzz", nil, []byte("Hello World!"))
	fs.SkipMsg()
	fs.SkipMsg()
	fs.StoreMsg("zzz", nil, []byte("Hello World!"))
	fs.SkipMsg()

	state = fs.State()
	if state.Msgs != 2 {
		t.Fatalf("Expected %d msgs, got %d", 2, state.Msgs)
	}
	if state.FirstSeq != uint64(numSkips+1) || state.LastSeq != uint64(numSkips+5) {
		t.Fatalf("Expected first to be %d and last to be %d. got first %d and last %d", numSkips+1, numSkips+5, state.FirstSeq, state.LastSeq)
	}

	// Make sure we recover same state.
	fs.Stop()

	fs, err = newFileStore(FileStoreConfig{StoreDir: storeDir, BlockSize: 256}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	state = fs.State()
	if state.Msgs != 2 {
		t.Fatalf("Expected %d msgs, got %d", 2, state.Msgs)
	}
	if state.FirstSeq != uint64(numSkips+1) || state.LastSeq != uint64(numSkips+5) {
		t.Fatalf("Expected first to be %d and last to be %d. got first %d and last %d", numSkips+1, numSkips+5, state.FirstSeq, state.LastSeq)
	}

	subj, _, msg, _, err := fs.LoadMsg(11)
	if err != nil {
		t.Fatalf("Unexpected error looking up seq 11: %v", err)
	}
	if subj != "zzz" || string(msg) != "Hello World!" {
		t.Fatalf("Message did not match")
	}

	fs.SkipMsg()
	nseq, _, err := fs.StoreMsg("AAA", nil, []byte("Skip?"))
	if err != nil {
		t.Fatalf("Unexpected error looking up seq 11: %v", err)
	}
	if nseq != 17 {
		t.Fatalf("Expected seq of %d but got %d", 17, nseq)
	}

	// Make sure we recover same state.
	fs.Stop()

	fs, err = newFileStore(FileStoreConfig{StoreDir: storeDir, BlockSize: 256}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	subj, _, msg, _, err = fs.LoadMsg(nseq)
	if err != nil {
		t.Fatalf("Unexpected error looking up seq %d: %v", nseq, err)
	}
	if subj != "AAA" || string(msg) != "Skip?" {
		t.Fatalf("Message did not match")
	}
}

func TestFileStoreWriteExpireWrite(t *testing.T) {
	storeDir := createDir(t, JetStreamStoreDir)
	defer removeDir(t, storeDir)

	cexp := 10 * time.Millisecond
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: storeDir, BlockSize: 256, CacheExpire: cexp},
		StreamConfig{Name: "zzz", Storage: FileStorage},
	)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	toSend := 10
	for i := 0; i < toSend; i++ {
		fs.StoreMsg("zzz", nil, []byte("Hello World!"))
	}

	// Wait for write cache portion to go to zero.
	checkFor(t, time.Second, 20*time.Millisecond, func() error {
		if csz := fs.cacheSize(); csz != 0 {
			return fmt.Errorf("cache size not 0, got %s", friendlyBytes(int64(csz)))
		}
		return nil
	})

	for i := 0; i < toSend; i++ {
		fs.StoreMsg("zzz", nil, []byte("Hello World! - 22"))
	}

	if state := fs.State(); state.Msgs != uint64(toSend*2) {
		t.Fatalf("Expected %d msgs, got %d", toSend*2, state.Msgs)
	}

	// Make sure we recover same state.
	fs.Stop()

	fs, err = newFileStore(FileStoreConfig{StoreDir: storeDir, BlockSize: 256}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	if state := fs.State(); state.Msgs != uint64(toSend*2) {
		t.Fatalf("Expected %d msgs, got %d", toSend*2, state.Msgs)
	}

	// Now load them in and check.
	for i := 1; i <= toSend*2; i++ {
		subj, _, msg, _, err := fs.LoadMsg(uint64(i))
		if err != nil {
			t.Fatalf("Unexpected error looking up seq %d: %v", i, err)
		}
		str := "Hello World!"
		if i > toSend {
			str = "Hello World! - 22"
		}
		if subj != "zzz" || string(msg) != str {
			t.Fatalf("Message did not match")
		}
	}
}

func TestFileStoreMsgLimit(t *testing.T) {
	storeDir := createDir(t, JetStreamStoreDir)
	defer removeDir(t, storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, StreamConfig{Name: "zzz", Storage: FileStorage, MaxMsgs: 10})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	subj, msg := "foo", []byte("Hello World")
	for i := 0; i < 10; i++ {
		fs.StoreMsg(subj, nil, msg)
	}
	state := fs.State()
	if state.Msgs != 10 {
		t.Fatalf("Expected %d msgs, got %d", 10, state.Msgs)
	}
	if _, _, err := fs.StoreMsg(subj, nil, msg); err != nil {
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
	if _, _, _, _, err := fs.LoadMsg(1); err == nil {
		t.Fatalf("Expected error looking up seq 1 but got none")
	}
}

func TestFileStoreMsgLimitBug(t *testing.T) {
	storeDir := createDir(t, JetStreamStoreDir)
	defer removeDir(t, storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, StreamConfig{Name: "zzz", Storage: FileStorage, MaxMsgs: 1})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	subj, msg := "foo", []byte("Hello World")
	fs.StoreMsg(subj, nil, msg)
	fs.StoreMsg(subj, nil, msg)
	fs.Stop()

	fs, err = newFileStore(FileStoreConfig{StoreDir: storeDir}, StreamConfig{Name: "zzz", Storage: FileStorage, MaxMsgs: 1})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()
	fs.StoreMsg(subj, nil, msg)
}

func TestFileStoreBytesLimit(t *testing.T) {
	subj, msg := "foo", make([]byte, 512)
	storedMsgSize := fileStoreMsgSize(subj, nil, msg)

	toStore := uint64(1024)
	maxBytes := storedMsgSize * toStore

	storeDir := createDir(t, JetStreamStoreDir)
	defer removeDir(t, storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, StreamConfig{Name: "zzz", Storage: FileStorage, MaxBytes: int64(maxBytes)})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	for i := uint64(0); i < toStore; i++ {
		fs.StoreMsg(subj, nil, msg)
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
		if _, _, err := fs.StoreMsg(subj, nil, msg); err != nil {
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
	maxAge := 250 * time.Millisecond

	storeDir := createDir(t, JetStreamStoreDir)
	defer removeDir(t, storeDir)

	fs, err := newFileStore(
		FileStoreConfig{StoreDir: storeDir, BlockSize: 256},
		StreamConfig{Name: "zzz", Storage: FileStorage, MaxAge: maxAge},
	)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	// Store some messages. Does not really matter how many.
	subj, msg := "foo", []byte("Hello World")
	toStore := 500
	for i := 0; i < toStore; i++ {
		if _, _, err := fs.StoreMsg(subj, nil, msg); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}
	state := fs.State()
	if state.Msgs != uint64(toStore) {
		t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
	}
	checkExpired := func(t *testing.T) {
		t.Helper()
		checkFor(t, 5*time.Second, maxAge, func() error {
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
		fs.StoreMsg(subj, nil, msg)
	}
	state = fs.State()
	if state.Msgs != uint64(toStore) {
		t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
	}
	fs.RemoveMsg(502)
	fs.RemoveMsg(602)
	fs.RemoveMsg(702)
	fs.RemoveMsg(802)
	// We will measure the time to make sure expires works with interior deletes.
	start := time.Now()
	checkExpired(t)
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Took too long to expire: %v", elapsed)
	}
}

func TestFileStoreTimeStamps(t *testing.T) {
	storeDir := createDir(t, JetStreamStoreDir)
	defer removeDir(t, storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	last := time.Now().UnixNano()
	subj, msg := "foo", []byte("Hello World")
	for i := 0; i < 10; i++ {
		time.Sleep(5 * time.Millisecond)
		fs.StoreMsg(subj, nil, msg)
	}
	for seq := uint64(1); seq <= 10; seq++ {
		_, _, _, ts, err := fs.LoadMsg(seq)
		if err != nil {
			t.Fatalf("Unexpected error looking up msg [%d]: %v", seq, err)
		}
		// These should be different
		if ts <= last {
			t.Fatalf("Expected different timestamps, got last %v vs %v", last, ts)
		}
		last = ts
	}
}

func TestFileStorePurge(t *testing.T) {
	storeDir := createDir(t, JetStreamStoreDir)
	defer removeDir(t, storeDir)

	blkSize := uint64(64 * 1024)
	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir, BlockSize: blkSize}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	subj, msg := "foo", make([]byte, 8*1024)
	storedMsgSize := fileStoreMsgSize(subj, nil, msg)

	toStore := uint64(1024)
	for i := uint64(0); i < toStore; i++ {
		fs.StoreMsg(subj, nil, msg)
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
		fs.StoreMsg(subj, nil, msg)
	}
	state = fs.State()
	if state.Msgs != toStore {
		t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
	}
	if state.Bytes != storedMsgSize*toStore {
		t.Fatalf("Expected bytes to be %d, got %d", storedMsgSize*toStore, state.Bytes)
	}

	// We will simulate crashing before the purge directory is cleared.
	mdir := filepath.Join(storeDir, msgDir)
	pdir := filepath.Join(fs.fcfg.StoreDir, "ptest")
	os.Rename(mdir, pdir)
	os.MkdirAll(mdir, 0755)

	fs.Purge()
	checkPurgeState(toStore * 2)

	// Make sure we recover same state.
	fs.Stop()

	purgeDir := filepath.Join(fs.fcfg.StoreDir, purgeDir)
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

func TestFileStoreCompact(t *testing.T) {
	storeDir := createDir(t, JetStreamStoreDir)
	defer removeDir(t, storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	subj, msg := "foo", []byte("Hello World")
	for i := 0; i < 10; i++ {
		fs.StoreMsg(subj, nil, msg)
	}
	if state := fs.State(); state.Msgs != 10 {
		t.Fatalf("Expected 10 msgs, got %d", state.Msgs)
	}
	n, err := fs.Compact(6)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if n != 5 {
		t.Fatalf("Expected to have purged 5 msgs, got %d", n)
	}
	state := fs.State()
	if state.Msgs != 5 {
		t.Fatalf("Expected 5 msgs, got %d", state.Msgs)
	}
	if state.FirstSeq != 6 {
		t.Fatalf("Expected first seq of 6, got %d", state.FirstSeq)
	}
	// Now test that compact will also reset first if seq > last
	n, err = fs.Compact(100)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if n != 5 {
		t.Fatalf("Expected to have purged 5 msgs, got %d", n)
	}
	if state = fs.State(); state.FirstSeq != 100 {
		t.Fatalf("Expected first seq of 100, got %d", state.FirstSeq)
	}
}

func TestFileStoreCompactLastPlusOne(t *testing.T) {
	storeDir := createDir(t, JetStreamStoreDir)
	defer removeDir(t, storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir, BlockSize: 8192, AsyncFlush: false}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	subj, msg := "foo", make([]byte, 10_000)
	for i := 0; i < 10_000; i++ {
		if _, _, err := fs.StoreMsg(subj, nil, msg); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}
	if state := fs.State(); state.Msgs != 10_000 {
		t.Fatalf("Expected 1000000 msgs, got %d", state.Msgs)
	}
	if _, err := fs.Compact(10_001); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	state := fs.State()
	if state.Msgs != 0 {
		t.Fatalf("Expected no message but got %d", state.Msgs)
	}

	fs.StoreMsg(subj, nil, msg)
	state = fs.State()
	if state.Msgs != 1 {
		t.Fatalf("Expected one message but got %d", state.Msgs)
	}
}

func TestFileStoreCompactMsgCountBug(t *testing.T) {
	storeDir := createDir(t, JetStreamStoreDir)
	defer os.RemoveAll(storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	subj, msg := "foo", []byte("Hello World")
	for i := 0; i < 10; i++ {
		fs.StoreMsg(subj, nil, msg)
	}
	if state := fs.State(); state.Msgs != 10 {
		t.Fatalf("Expected 10 msgs, got %d", state.Msgs)
	}
	// Now delete 2,3,4.
	fs.EraseMsg(2)
	fs.EraseMsg(3)
	fs.EraseMsg(4)

	// Also delete 7,8, and 9.
	fs.RemoveMsg(7)
	fs.RemoveMsg(8)
	fs.RemoveMsg(9)

	n, err := fs.Compact(6)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// 1 & 5
	if n != 2 {
		t.Fatalf("Expected to have deleted 2 msgs, got %d", n)
	}
	if state := fs.State(); state.Msgs != 2 {
		t.Fatalf("Expected to have 2 remaining, got %d", state.Msgs)
	}
}

func TestFileStoreCompactPerf(t *testing.T) {
	t.SkipNow()

	storeDir := createDir(t, JetStreamStoreDir)
	defer removeDir(t, storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir, BlockSize: 8192, AsyncFlush: true}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	subj, msg := "foo", []byte("Hello World")
	for i := 0; i < 100_000; i++ {
		fs.StoreMsg(subj, nil, msg)
	}
	if state := fs.State(); state.Msgs != 100_000 {
		t.Fatalf("Expected 1000000 msgs, got %d", state.Msgs)
	}
	start := time.Now()
	n, err := fs.Compact(90_001)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	t.Logf("Took %v to compact\n", time.Since(start))

	if n != 90_000 {
		t.Fatalf("Expected to have purged 90_000 msgs, got %d", n)
	}
	state := fs.State()
	if state.Msgs != 10_000 {
		t.Fatalf("Expected 10_000 msgs, got %d", state.Msgs)
	}
	if state.FirstSeq != 90_001 {
		t.Fatalf("Expected first seq of 90_001, got %d", state.FirstSeq)
	}
}

func TestFileStoreStreamTruncate(t *testing.T) {
	storeDir := createDir(t, JetStreamStoreDir)
	defer removeDir(t, storeDir)

	fs, err := newFileStore(
		FileStoreConfig{StoreDir: storeDir, BlockSize: 350},
		StreamConfig{Name: "zzz", Storage: FileStorage},
	)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	subj, toStore := "foo", uint64(100)
	for i := uint64(1); i <= toStore; i++ {
		if _, _, err := fs.StoreMsg(subj, nil, []byte("ok")); err != nil {
			t.Fatalf("Error storing msg: %v", err)
		}
	}
	if state := fs.State(); state.Msgs != toStore {
		t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
	}

	// Check that sequence has to be interior.
	if err := fs.Truncate(toStore + 1); err != ErrInvalidSequence {
		t.Fatalf("Expected err of '%v', got '%v'", ErrInvalidSequence, err)
	}

	tseq := uint64(50)
	if err := fs.Truncate(tseq); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if state := fs.State(); state.Msgs != tseq {
		t.Fatalf("Expected %d msgs, got %d", tseq, state.Msgs)
	}

	// Now make sure we report properly if we have some deleted interior messages.
	fs.RemoveMsg(10)
	fs.RemoveMsg(20)
	fs.RemoveMsg(30)
	fs.RemoveMsg(40)

	tseq = uint64(25)
	if err := fs.Truncate(tseq); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if state := fs.State(); state.Msgs != tseq-2 {
		t.Fatalf("Expected %d msgs, got %d", tseq-2, state.Msgs)
	}
	expected := []uint64{10, 20}
	if state := fs.State(); !reflect.DeepEqual(state.Deleted, expected) {
		t.Fatalf("Expected deleted to be %+v, got %+v\n", expected, state.Deleted)
	}

	before := fs.State()

	// Make sure we can recover same state.
	fs.Stop()
	fs, err = newFileStore(FileStoreConfig{StoreDir: storeDir}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	if state := fs.State(); !reflect.DeepEqual(state, before) {
		t.Fatalf("Expected state of %+v, got %+v", before, state)
	}
}

func TestFileStoreRemovePartialRecovery(t *testing.T) {
	storeDir := createDir(t, JetStreamStoreDir)
	defer removeDir(t, storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	subj, msg := "foo", []byte("Hello World")
	toStore := 100
	for i := 0; i < toStore; i++ {
		fs.StoreMsg(subj, nil, msg)
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
	if !reflect.DeepEqual(state2, state) {
		t.Fatalf("Expected recovered state to be the same, got %+v vs %+v\n", state2, state)
	}
}

func TestFileStoreRemoveOutOfOrderRecovery(t *testing.T) {
	storeDir := createDir(t, JetStreamStoreDir)
	defer removeDir(t, storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	subj, msg := "foo", []byte("Hello World")
	toStore := 100
	for i := 0; i < toStore; i++ {
		fs.StoreMsg(subj, nil, msg)
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

	if _, _, _, _, err := fs.LoadMsg(1); err != nil {
		t.Fatalf("Expected to retrieve seq 1")
	}
	for i := 2; i <= toStore; i += 2 {
		if _, _, _, _, err := fs.LoadMsg(uint64(i)); err == nil {
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
	if !reflect.DeepEqual(state2, state) {
		t.Fatalf("Expected recovered states to be the same, got %+v vs %+v\n", state, state2)
	}

	if _, _, _, _, err := fs.LoadMsg(1); err != nil {
		t.Fatalf("Expected to retrieve seq 1")
	}
	for i := 2; i <= toStore; i += 2 {
		if _, _, _, _, err := fs.LoadMsg(uint64(i)); err == nil {
			t.Fatalf("Expected error looking up seq %d that should be deleted", i)
		}
	}
}

func TestFileStoreAgeLimitRecovery(t *testing.T) {
	maxAge := 10 * time.Millisecond

	storeDir := createDir(t, JetStreamStoreDir)
	defer removeDir(t, storeDir)

	fs, err := newFileStore(
		FileStoreConfig{StoreDir: storeDir, CacheExpire: 1 * time.Millisecond},
		StreamConfig{Name: "zzz", Storage: FileStorage, MaxAge: maxAge},
	)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	// Store some messages. Does not really matter how many.
	subj, msg := "foo", []byte("Hello World")
	toStore := 100
	for i := 0; i < toStore; i++ {
		fs.StoreMsg(subj, nil, msg)
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
		t.Helper()
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
	storeDir := createDir(t, JetStreamStoreDir)
	defer removeDir(t, storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	// Store some messages. Does not really matter how many.
	subj, msg := "foo", []byte("Hello World")
	toStore := 100
	for i := 0; i < toStore; i++ {
		fs.StoreMsg(subj, nil, msg)
	}
	state := fs.State()
	if state.Msgs != uint64(toStore) {
		t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
	}

	if ld := fs.checkMsgs(); ld != nil && len(ld.Msgs) > 0 {
		t.Fatalf("Expected to have no corrupt msgs, got %d", len(ld.Msgs))
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

	ld := fs.checkMsgs()
	if ld == nil || len(ld.Msgs) == 0 {
		t.Fatalf("Expected to have corrupt msgs got none: changed [%d]", index)
	}

	// Make sure we can restore.
	fs.Stop()

	fs, err = newFileStore(FileStoreConfig{StoreDir: storeDir}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	// checkMsgs will repair the underlying store, so checkMsgs should be clean now.
	if ld := fs.checkMsgs(); ld != nil {
		t.Fatalf("Expected no errors restoring checked and fixed filestore, got %+v", ld)
	}
}

func TestFileStoreEraseMsg(t *testing.T) {
	storeDir := createDir(t, JetStreamStoreDir)
	defer removeDir(t, storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	subj, msg := "foo", []byte("Hello World")
	fs.StoreMsg(subj, nil, msg)
	fs.StoreMsg(subj, nil, msg) // To keep block from being deleted.
	_, _, smsg, _, err := fs.LoadMsg(1)
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
	fs.checkAndFlushAllBlocks()

	// Now look on disk as well.
	rl := fileStoreMsgSize(subj, nil, msg)
	buf := make([]byte, rl)
	fp, err := os.Open(filepath.Join(storeDir, msgDir, fmt.Sprintf(blkScan, 1)))
	if err != nil {
		t.Fatalf("Error opening msg block file: %v", err)
	}
	defer fp.Close()

	fp.ReadAt(buf, sm.off)
	nsubj, _, nmsg, seq, ts, err := msgFromBuf(buf, nil)
	if err != nil {
		t.Fatalf("error reading message from block: %v", err)
	}
	if nsubj == subj {
		t.Fatalf("Expected the subjects to be different")
	}
	if seq != 0 && seq&ebit == 0 {
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
	storeDir := createDir(t, JetStreamStoreDir)
	defer removeDir(t, storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	subj, msg := "foo", []byte("Hello World")
	toStore := 100
	for i := 0; i < toStore; i++ {
		fs.StoreMsg(subj, nil, msg)
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
	ifn := filepath.Join(storeDir, msgDir, fmt.Sprintf(indexScan, 1))
	removeFile(t, ifn)

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
		if _, _, _, _, err := fs.LoadMsg(uint64(i)); err == nil {
			t.Fatalf("Expected error looking up seq %d that should be erased", i)
		}
	}
}

func TestFileStoreMeta(t *testing.T) {
	storeDir := createDir(t, JetStreamStoreDir)
	defer removeDir(t, storeDir)

	mconfig := StreamConfig{Name: "ZZ-22-33", Storage: FileStorage, Subjects: []string{"foo.*"}, Replicas: 22}

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, mconfig)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	metafile := filepath.Join(storeDir, JetStreamMetaFile)
	metasum := filepath.Join(storeDir, JetStreamMetaFileSum)

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

	// Now create a consumer. Same deal for them.
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

	ometafile := filepath.Join(storeDir, consumerDir, oname, JetStreamMetaFile)
	ometasum := filepath.Join(storeDir, consumerDir, oname, JetStreamMetaFileSum)

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
	if !reflect.DeepEqual(oconfig2, oconfig) {
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
	storeDir := createDir(t, JetStreamStoreDir)
	defer removeDir(t, storeDir)

	fs, err := newFileStore(
		FileStoreConfig{StoreDir: storeDir},
		StreamConfig{Name: "zzz", Storage: FileStorage},
	)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	subj, msg := "foo", []byte("Hello World!")

	for i := uint64(1); i <= 10; i++ {
		fs.StoreMsg(subj, nil, msg)
		if _, _, _, _, err := fs.LoadMsg(i); err != nil {
			t.Fatalf("Error loading %d: %v", i, err)
		}
	}
}

func TestFileStoreAndRetrieveMultiBlock(t *testing.T) {
	storeDir := createDir(t, JetStreamStoreDir)
	defer removeDir(t, storeDir)

	subj, msg := "foo", []byte("Hello World!")
	storedMsgSize := fileStoreMsgSize(subj, nil, msg)

	fs, err := newFileStore(
		FileStoreConfig{StoreDir: storeDir, BlockSize: 4 * storedMsgSize},
		StreamConfig{Name: "zzz", Storage: FileStorage},
	)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for i := 0; i < 20; i++ {
		fs.StoreMsg(subj, nil, msg)
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
	defer fs.Stop()

	for i := uint64(1); i <= 20; i++ {
		if _, _, _, _, err := fs.LoadMsg(i); err != nil {
			t.Fatalf("Error loading %d: %v", i, err)
		}
	}
}

func TestFileStoreCollapseDmap(t *testing.T) {
	storeDir := createDir(t, JetStreamStoreDir)
	defer removeDir(t, storeDir)

	subj, msg := "foo", []byte("Hello World!")
	storedMsgSize := fileStoreMsgSize(subj, nil, msg)

	fs, err := newFileStore(
		FileStoreConfig{StoreDir: storeDir, BlockSize: 4 * storedMsgSize},
		StreamConfig{Name: "zzz", Storage: FileStorage},
	)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	for i := 0; i < 10; i++ {
		fs.StoreMsg(subj, nil, msg)
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
	storedMsgSize := fileStoreMsgSize(subj, nil, msg)

	storeDir := createDir(t, JetStreamStoreDir)
	defer removeDir(t, storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir, CacheExpire: 100 * time.Millisecond}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	toStore := 500
	totalBytes := uint64(toStore) * storedMsgSize

	for i := 0; i < toStore; i++ {
		fs.StoreMsg(subj, nil, msg)
	}

	// Wait for cache to go to zero.
	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		if csz := fs.cacheSize(); csz != 0 {
			return fmt.Errorf("cache size not 0, got %s", friendlyBytes(int64(csz)))
		}
		return nil
	})

	fs.LoadMsg(1)
	if csz := fs.cacheSize(); csz != totalBytes {
		t.Fatalf("Expected all messages to be cached, got %d vs %d", csz, totalBytes)
	}
	// Should expire and be removed.
	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		if csz := fs.cacheSize(); csz != 0 {
			return fmt.Errorf("cache size not 0, got %s", friendlyBytes(int64(csz)))
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

func TestFileStorePartialCacheExpiration(t *testing.T) {
	storeDir := createDir(t, JetStreamStoreDir)
	defer removeDir(t, storeDir)

	cexp := 10 * time.Millisecond

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir, CacheExpire: cexp}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	fs.StoreMsg("foo", nil, []byte("msg1"))

	// Should expire and be removed.
	time.Sleep(2 * cexp)
	fs.StoreMsg("bar", nil, []byte("msg2"))

	// Again wait for cache to expire.
	time.Sleep(2 * cexp)
	if _, _, _, _, err := fs.LoadMsg(1); err != nil {
		t.Fatalf("Error loading message 1: %v", err)
	}
}

func TestFileStorePartialIndexes(t *testing.T) {
	storeDir := createDir(t, JetStreamStoreDir)
	defer removeDir(t, storeDir)

	cexp := 10 * time.Millisecond

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir, CacheExpire: cexp}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	toSend := 5
	for i := 0; i < toSend; i++ {
		fs.StoreMsg("foo", nil, []byte("ok-1"))
	}

	// Now wait til the cache expires, including the index.
	fs.mu.Lock()
	mb := fs.blks[0]
	fs.mu.Unlock()

	// Force idx to expire by resetting last remove ts.
	mb.mu.Lock()
	mb.lrts = mb.lrts - int64(defaultCacheIdxExpiration*2)
	mb.mu.Unlock()

	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		mb.mu.Lock()
		defer mb.mu.Unlock()
		if mb.cache == nil || len(mb.cache.idx) == 0 {
			return nil
		}
		return fmt.Errorf("Index not empty")
	})

	// Create a partial cache by adding more msgs.
	for i := 0; i < toSend; i++ {
		fs.StoreMsg("foo", nil, []byte("ok-2"))
	}
	// If we now load in a message in second half if we do not
	// detect idx is a partial correctly this will panic.
	if _, _, _, _, err := fs.LoadMsg(8); err != nil {
		t.Fatalf("Error loading %d: %v", 1, err)
	}
}

func TestFileStoreSnapshot(t *testing.T) {
	storeDir := createDir(t, JetStreamStoreDir)
	defer removeDir(t, storeDir)

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
		fs.StoreMsg(subj, nil, msg)
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
	state.Delivered.Consumer = 100
	state.Delivered.Stream = 100
	state.AckFloor.Consumer = 22
	state.AckFloor.Stream = 22

	if err := o1.Update(state); err != nil {
		t.Fatalf("Unexepected error updating state: %v", err)
	}
	state.AckFloor.Consumer = 33
	state.AckFloor.Stream = 33

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
		t.Helper()
		r := bytes.NewReader(snap)
		tr := tar.NewReader(s2.NewReader(r))

		rstoreDir := createDir(t, JetStreamStoreDir)
		defer removeDir(t, rstoreDir)

		for {
			hdr, err := tr.Next()
			if err == io.EOF {
				break // End of archive
			}
			if err != nil {
				t.Fatalf("Error getting next entry from snapshot: %v", err)
			}
			fpath := filepath.Join(rstoreDir, filepath.Clean(hdr.Name))
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
		defer fsr.Stop()
		state := fs.State()
		rstate := fsr.State()

		// FIXME(dlc)
		// Right now the upper layers in JetStream recover the consumers and do not expect
		// the lower layers to do that. So for now blank that out of our original state.
		// Will have more exhaustive tests in jetstream_test.go.
		state.Consumers = 0

		// Just check the state.
		if !reflect.DeepEqual(rstate, state) {
			t.Fatalf("Restored state does not match:\n%+v\n\n%+v", rstate, state)
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

	// Make sure compaction works with snapshots.
	fs.mu.RLock()
	for _, mb := range fs.blks {
		// Should not call compact on last msg block.
		if mb != fs.lmb {
			mb.mu.Lock()
			mb.compact()
			mb.mu.Unlock()
		}
	}
	fs.mu.RUnlock()

	snap = snapshot()
	verifySnapshot(snap)

	// Now check to make sure that we get the correct error when trying to delete or erase
	// a message when a snapshot is in progress and that closing the reader releases that condition.
	sr, err := fs.Snapshot(5*time.Second, false, true)
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
	time.Sleep(50 * time.Millisecond)
	// Read should fail
	var buf [32]byte
	if _, err := sr.Reader.Read(buf[:]); err != io.EOF {
		t.Fatalf("Expected read to produce an error, got none")
	}
}

func TestFileStoreConsumer(t *testing.T) {
	storeDir := createDir(t, JetStreamStoreDir)
	defer removeDir(t, storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	o, err := fs.ConsumerStore("obs22", &ConsumerConfig{})
	if err != nil {
		t.Fatalf("Unexepected error: %v", err)
	}
	if state, err := o.State(); err != nil || state.Delivered.Consumer != 0 {
		t.Fatalf("Unexpected state or error: %v", err)
	}

	state := &ConsumerState{}

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

	state.Delivered.Consumer = 1
	state.Delivered.Stream = 22
	updateAndCheck()

	state.Delivered.Consumer = 100
	state.Delivered.Stream = 122
	state.AckFloor.Consumer = 50
	state.AckFloor.Stream = 123
	// This should fail, bad state.
	shouldFail()
	// So should this.
	state.AckFloor.Consumer = 200
	state.AckFloor.Stream = 100
	shouldFail()

	// Should succeed
	state.AckFloor.Consumer = 50
	state.AckFloor.Stream = 72
	updateAndCheck()

	tn := time.Now().UnixNano()

	// We should sanity check pending here as well, so will check if a pending value is below
	// ack floor or above delivered.
	state.Pending = map[uint64]*Pending{70: {70, tn}}
	shouldFail()
	state.Pending = map[uint64]*Pending{140: {140, tn}}
	shouldFail()
	state.Pending = map[uint64]*Pending{72: {72, tn}} // exact on floor should fail
	shouldFail()

	// Put timestamps a second apart.
	// We will downsample to second resolution to save space. So setup our times
	// to reflect that.
	ago := time.Now().Add(-30 * time.Second).Truncate(time.Second)
	nt := func() *Pending {
		ago = ago.Add(time.Second)
		return &Pending{0, ago.UnixNano()}
	}
	// Should succeed.
	state.Pending = map[uint64]*Pending{75: nt(), 80: nt(), 83: nt(), 90: nt(), 111: nt()}
	updateAndCheck()

	// Now do redlivery, but first with no pending.
	state.Pending = nil
	state.Redelivered = map[uint64]uint64{22: 3, 44: 8}
	updateAndCheck()

	// All together.
	state.Pending = map[uint64]*Pending{75: nt(), 80: nt(), 83: nt(), 90: nt(), 111: nt()}
	updateAndCheck()

	// Large one
	state.Delivered.Consumer = 10000
	state.Delivered.Stream = 10000
	state.AckFloor.Consumer = 100
	state.AckFloor.Stream = 100
	// Generate 8k pending.
	state.Pending = make(map[uint64]*Pending)
	for len(state.Pending) < 8192 {
		seq := uint64(rand.Intn(9890) + 101)
		if _, ok := state.Pending[seq]; !ok {
			state.Pending[seq] = nt()
		}
	}
	updateAndCheck()

	state.Pending = nil
	state.AckFloor.Consumer = 10000
	state.AckFloor.Stream = 10000
	updateAndCheck()
}

func TestFileStoreConsumerEncodeDecodeRedelivered(t *testing.T) {
	state := &ConsumerState{}

	state.Delivered.Consumer = 100
	state.Delivered.Stream = 100
	state.AckFloor.Consumer = 50
	state.AckFloor.Stream = 50

	state.Redelivered = map[uint64]uint64{122: 3, 144: 8}
	buf := encodeConsumerState(state)

	rstate, err := decodeConsumerState(buf)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !reflect.DeepEqual(state, rstate) {
		t.Fatalf("States do not match: %+v vs %+v", state, rstate)
	}
}

func TestFileStoreConsumerEncodeDecodePendingBelowStreamAckFloor(t *testing.T) {
	state := &ConsumerState{}

	state.Delivered.Consumer = 1192
	state.Delivered.Stream = 10185
	state.AckFloor.Consumer = 1189
	state.AckFloor.Stream = 10815

	now := time.Now().Round(time.Second).Add(-10 * time.Second).UnixNano()
	state.Pending = map[uint64]*Pending{
		10782: {1190, now},
		10810: {1191, now + int64(time.Second)},
		10815: {1192, now + int64(2*time.Second)},
	}
	buf := encodeConsumerState(state)

	rstate, err := decodeConsumerState(buf)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(rstate.Pending) != 3 {
		t.Fatalf("Invalid pending: %v", rstate.Pending)
	}
	for k, v := range state.Pending {
		rv, ok := rstate.Pending[k]
		if !ok {
			t.Fatalf("Did not find sseq=%v", k)
		}
		if !reflect.DeepEqual(v, rv) {
			t.Fatalf("Pending for sseq=%v should be %+v, got %+v", k, v, rv)
		}
	}
	state.Pending, rstate.Pending = nil, nil
	if !reflect.DeepEqual(*state, *rstate) {
		t.Fatalf("States do not match: %+v vs %+v", state, rstate)
	}
}

func TestFileStoreWriteFailures(t *testing.T) {
	// This test should be run inside an environment where this directory
	// has a limited size.
	// E.g. Docker
	// docker run -ti --tmpfs /jswf_test:rw,size=32k --rm -v ~/Development/go/src:/go/src -w /go/src/github.com/nats-io/nats-server/ golang:1.16 /bin/bash
	tdir := filepath.Join("/", "jswf_test")
	if stat, err := os.Stat(tdir); err != nil || !stat.IsDir() {
		t.SkipNow()
	}
	defer removeDir(t, tdir)

	storeDir := filepath.Join(tdir, JetStreamStoreDir)
	os.MkdirAll(storeDir, 0755)

	subj, msg := "foo", []byte("Hello Write Failures!")
	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	var lseq uint64
	// msz about will be ~54 bytes, so if limit is 32k trying to send 1000 will fail at some point.
	for i := 1; i <= 1000; i++ {
		if _, _, err := fs.StoreMsg(subj, nil, msg); err != nil {
			lseq = uint64(i)
			break
		}
	}
	if lseq == 0 {
		t.Fatalf("Expected to get a failure but did not")
	}

	state := fs.State()

	if state.LastSeq != lseq-1 {
		t.Fatalf("Expected last seq to be %d, got %d\n", lseq-1, state.LastSeq)
	}
	if state.Msgs != lseq-1 {
		t.Fatalf("Expected total msgs to be %d, got %d\n", lseq-1, state.Msgs)
	}
	if _, _, _, _, err := fs.LoadMsg(lseq); err == nil {
		t.Fatalf("Expected error loading seq that failed, got none")
	}
	// Loading should still work.
	if _, _, _, _, err := fs.LoadMsg(1); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Make sure we recover same state.
	fs.Stop()

	fs, err = newFileStore(FileStoreConfig{StoreDir: storeDir}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	state2 := fs.State()

	// Ignore lost state.
	state.Lost, state2.Lost = nil, nil
	if !reflect.DeepEqual(state2, state) {
		t.Fatalf("Expected recovered state to be the same, got %+v vs %+v\n", state2, state)
	}

	// We should still fail here.
	if _, _, err = fs.StoreMsg(subj, nil, msg); err == nil {
		t.Fatalf("Expected to get a failure but did not")
	}

	// Purge should help.
	if _, err := fs.Purge(); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Wait for purge to complete its out of band processing.
	time.Sleep(50 * time.Millisecond)

	// Check we will fail again in same spot.
	for i := 1; i <= 1000; i++ {
		if _, _, err := fs.StoreMsg(subj, nil, msg); err != nil {
			if i != int(lseq) {
				t.Fatalf("Expected to fail after purge about the same spot, wanted %d got %d", lseq, i)
			}
			break
		}
	}
}

func TestFileStorePerf(t *testing.T) {
	// Comment out to run, holding place for now.
	t.SkipNow()

	subj, msg := "foo", make([]byte, 1024-33)
	for i := 0; i < len(msg); i++ {
		msg[i] = 'D'
	}
	storedMsgSize := fileStoreMsgSize(subj, nil, msg)

	// 5GB
	toStore := 5 * 1024 * 1024 * 1024 / storedMsgSize

	fmt.Printf("storing %d msgs of %s each, totalling %s\n",
		toStore,
		friendlyBytes(int64(storedMsgSize)),
		friendlyBytes(int64(toStore*storedMsgSize)),
	)

	storeDir := createDir(t, JetStreamStoreDir)
	defer removeDir(t, storeDir)

	fs, err := newFileStore(
		FileStoreConfig{StoreDir: storeDir, AsyncFlush: true},
		StreamConfig{Name: "zzz", Storage: FileStorage},
	)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	start := time.Now()
	for i := 0; i < int(toStore); i++ {
		fs.StoreMsg(subj, nil, msg)
	}
	fs.Stop()

	tt := time.Since(start)
	fmt.Printf("time to store is %v\n", tt)
	fmt.Printf("%.0f msgs/sec\n", float64(toStore)/tt.Seconds())
	fmt.Printf("%s per sec\n", friendlyBytes(int64(float64(toStore*storedMsgSize)/tt.Seconds())))

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
		friendlyBytes(int64(storedMsgSize)),
		friendlyBytes(int64(toStore*storedMsgSize)),
	)

	start = time.Now()
	for i := uint64(1); i <= toStore; i++ {
		if _, _, _, _, err := fs.LoadMsg(i); err != nil {
			t.Fatalf("Error loading %d: %v", i, err)
		}
	}

	tt = time.Since(start)
	fmt.Printf("time to read all back messages is %v\n", tt)
	fmt.Printf("%.0f msgs/sec\n", float64(toStore)/tt.Seconds())
	fmt.Printf("%s per sec\n", friendlyBytes(int64(float64(toStore*storedMsgSize)/tt.Seconds())))

	// Do again to test skip for hash..
	fmt.Printf("\nSKIP CHECKSUM: reading %d msgs of %s each, totalling %s\n",
		toStore,
		friendlyBytes(int64(storedMsgSize)),
		friendlyBytes(int64(toStore*storedMsgSize)),
	)

	start = time.Now()
	for i := uint64(1); i <= toStore; i++ {
		if _, _, _, _, err := fs.LoadMsg(i); err != nil {
			t.Fatalf("Error loading %d: %v", i, err)
		}
	}

	tt = time.Since(start)
	fmt.Printf("time to read all back messages is %v\n", tt)
	fmt.Printf("%.0f msgs/sec\n", float64(toStore)/tt.Seconds())
	fmt.Printf("%s per sec\n", friendlyBytes(int64(float64(toStore*storedMsgSize)/tt.Seconds())))

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
		friendlyBytes(int64(storedMsgSize)),
		friendlyBytes(int64(toStore*storedMsgSize)),
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
	fmt.Printf("%s per sec\n", friendlyBytes(int64(float64(toStore*storedMsgSize)/tt.Seconds())))

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

	storedMsgSize := fileStoreMsgSize(subj, nil, msg)

	// Make sure we store 2 blocks.
	toStore := defaultStreamBlockSize * 2 / storedMsgSize

	fmt.Printf("storing %d msgs of %s each, totalling %s\n",
		toStore,
		friendlyBytes(int64(storedMsgSize)),
		friendlyBytes(int64(toStore*storedMsgSize)),
	)

	storeDir := createDir(t, JetStreamStoreDir)
	defer removeDir(t, storeDir)

	fs, err := newFileStore(
		FileStoreConfig{StoreDir: storeDir},
		StreamConfig{Name: "zzz", Storage: FileStorage},
	)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	start := time.Now()
	for i := 0; i < int(toStore); i++ {
		fs.StoreMsg(subj, nil, msg)
	}

	tt := time.Since(start)
	fmt.Printf("time to store is %v\n", tt)
	fmt.Printf("%.0f msgs/sec\n", float64(toStore)/tt.Seconds())
	fmt.Printf("%s per sec\n", friendlyBytes(int64(float64(toStore*storedMsgSize)/tt.Seconds())))

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

// This test is testing an upper level stream with a message or byte limit.
// Even though this is 1, any limit would trigger same behavior once the limit was reached
// and we were adding and removing.
func TestFileStoreStoreLimitRemovePerf(t *testing.T) {
	// Comment out to run, holding place for now.
	t.SkipNow()

	subj, msg := "foo", make([]byte, 1024-33)
	for i := 0; i < len(msg); i++ {
		msg[i] = 'D'
	}
	storedMsgSize := fileStoreMsgSize(subj, nil, msg)

	// 1GB
	toStore := 1 * 1024 * 1024 * 1024 / storedMsgSize

	storeDir := createDir(t, JetStreamStoreDir)
	defer removeDir(t, storeDir)

	fs, err := newFileStore(
		FileStoreConfig{StoreDir: storeDir},
		StreamConfig{Name: "zzz", Storage: FileStorage},
	)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	fs.RegisterStorageUpdates(func(md, bd int64, seq uint64, subj string) {})

	fmt.Printf("storing and removing (limit 1) %d msgs of %s each, totalling %s\n",
		toStore,
		friendlyBytes(int64(storedMsgSize)),
		friendlyBytes(int64(toStore*storedMsgSize)),
	)

	start := time.Now()
	for i := 0; i < int(toStore); i++ {
		seq, _, err := fs.StoreMsg(subj, nil, msg)
		if err != nil {
			t.Fatalf("Unexpected error storing message: %v", err)
		}
		if i > 0 {
			fs.RemoveMsg(seq - 1)
		}
	}
	fs.Stop()

	tt := time.Since(start)
	fmt.Printf("time to store and remove all messages is %v\n", tt)
	fmt.Printf("%.0f msgs/sec\n", float64(toStore)/tt.Seconds())
	fmt.Printf("%s per sec\n", friendlyBytes(int64(float64(toStore*storedMsgSize)/tt.Seconds())))
}

func TestFileStorePubPerfWithSmallBlkSize(t *testing.T) {
	// Comment out to run, holding place for now.
	t.SkipNow()

	subj, msg := "foo", make([]byte, 1024-33)
	for i := 0; i < len(msg); i++ {
		msg[i] = 'D'
	}
	storedMsgSize := fileStoreMsgSize(subj, nil, msg)

	// 1GB
	toStore := 1 * 1024 * 1024 * 1024 / storedMsgSize

	fmt.Printf("storing %d msgs of %s each, totalling %s\n",
		toStore,
		friendlyBytes(int64(storedMsgSize)),
		friendlyBytes(int64(toStore*storedMsgSize)),
	)

	storeDir := createDir(t, JetStreamStoreDir)
	defer removeDir(t, storeDir)

	fs, err := newFileStore(
		FileStoreConfig{StoreDir: storeDir, BlockSize: FileStoreMinBlkSize},
		StreamConfig{Name: "zzz", Storage: FileStorage},
	)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	start := time.Now()
	for i := 0; i < int(toStore); i++ {
		fs.StoreMsg(subj, nil, msg)
	}
	fs.Stop()

	tt := time.Since(start)
	fmt.Printf("time to store is %v\n", tt)
	fmt.Printf("%.0f msgs/sec\n", float64(toStore)/tt.Seconds())
	fmt.Printf("%s per sec\n", friendlyBytes(int64(float64(toStore*storedMsgSize)/tt.Seconds())))
}

// Saw this manifest from a restart test with max delivered set for JetStream.
func TestFileStoreConsumerRedeliveredLost(t *testing.T) {
	storeDir := createDir(t, JetStreamStoreDir)
	defer removeDir(t, storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	cfg := &ConsumerConfig{AckPolicy: AckExplicit}
	o, err := fs.ConsumerStore("o22", cfg)
	if err != nil {
		t.Fatalf("Unexepected error: %v", err)
	}

	restartConsumer := func() {
		t.Helper()
		o.Stop()
		time.Sleep(20 * time.Millisecond) // Wait for all things to settle.
		o, err = fs.ConsumerStore("o22", cfg)
		if err != nil {
			t.Fatalf("Unexepected error: %v", err)
		}
		// Make sure we recovered Redelivered.
		state, err := o.State()
		if err != nil {
			t.Fatalf("Unexepected error: %v", err)
		}
		if state == nil {
			t.Fatalf("Did not recover state")
		}
		if len(state.Redelivered) == 0 {
			t.Fatalf("Did not recover redelivered")
		}
	}

	ts := time.Now().UnixNano()
	o.UpdateDelivered(1, 1, 1, ts)
	o.UpdateDelivered(2, 1, 2, ts)
	o.UpdateDelivered(3, 1, 3, ts)
	o.UpdateDelivered(4, 1, 4, ts)
	o.UpdateDelivered(5, 2, 1, ts)

	restartConsumer()

	o.UpdateDelivered(6, 2, 2, ts)
	o.UpdateDelivered(7, 3, 1, ts)

	restartConsumer()
	if state, _ := o.State(); len(state.Pending) != 3 {
		t.Fatalf("Did not recover pending correctly")
	}

	o.UpdateAcks(7, 3)
	o.UpdateAcks(6, 2)

	restartConsumer()
	o.UpdateAcks(4, 1)

	state, _ := o.State()
	if len(state.Pending) != 0 {
		t.Fatalf("Did not clear pending correctly")
	}
	if len(state.Redelivered) != 0 {
		fmt.Printf("redelivered is %+v\n", state.Redelivered)
		t.Fatalf("Did not clear redelivered correctly")
	}
}

func TestFileStoreConsumerFlusher(t *testing.T) {
	storeDir := createDir(t, JetStreamStoreDir)
	defer removeDir(t, storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	o, err := fs.ConsumerStore("o22", &ConsumerConfig{})
	if err != nil {
		t.Fatalf("Unexepected error: %v", err)
	}
	// Get the underlying impl.
	oc := o.(*consumerFileStore)
	// Wait for flusher to be running.
	checkFor(t, time.Second, 20*time.Millisecond, func() error {
		if !oc.inFlusher() {
			return fmt.Errorf("Flusher not running")
		}
		return nil
	})
	// Stop and make sure the flusher goes away
	o.Stop()
	// Wait for flusher to stop.
	checkFor(t, time.Second, 20*time.Millisecond, func() error {
		if oc.inFlusher() {
			return fmt.Errorf("Flusher still running")
		}
		return nil
	})
}

func TestFileStoreConsumerDeliveredUpdates(t *testing.T) {
	storeDir := createDir(t, JetStreamStoreDir)
	defer removeDir(t, storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	// Simple consumer, no ack policy configured.
	o, err := fs.ConsumerStore("o22", &ConsumerConfig{})
	if err != nil {
		t.Fatalf("Unexepected error: %v", err)
	}
	defer o.Stop()

	testDelivered := func(dseq, sseq uint64) {
		t.Helper()
		ts := time.Now().UnixNano()
		if err := o.UpdateDelivered(dseq, sseq, 1, ts); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		state, err := o.State()
		if err != nil {
			t.Fatalf("Error getting state: %v", err)
		}
		if state == nil {
			t.Fatalf("No state available")
		}
		expected := SequencePair{dseq, sseq}
		if state.Delivered != expected {
			t.Fatalf("Unexpected state, wanted %+v, got %+v", expected, state.Delivered)
		}
		if state.AckFloor != expected {
			t.Fatalf("Unexpected ack floor state, wanted %+v, got %+v", expected, state.AckFloor)
		}
		if len(state.Pending) != 0 {
			t.Fatalf("Did not expect any pending, got %d pending", len(state.Pending))
		}
	}

	testDelivered(1, 100)
	testDelivered(2, 110)
	testDelivered(5, 130)

	// If we try to do an ack this should err since we are not configured with ack policy.
	if err := o.UpdateAcks(1, 100); err != ErrNoAckPolicy {
		t.Fatalf("Expected a no ack policy error on update acks, got %v", err)
	}
	// Also if we do an update with a delivery count of anything but 1 here should also give same error.
	ts := time.Now().UnixNano()
	if err := o.UpdateDelivered(5, 130, 2, ts); err != ErrNoAckPolicy {
		t.Fatalf("Expected a no ack policy error on update delivered with dc > 1, got %v", err)
	}
}

func TestFileStoreConsumerDeliveredAndAckUpdates(t *testing.T) {
	storeDir := createDir(t, JetStreamStoreDir)
	defer removeDir(t, storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	// Simple consumer, no ack policy configured.
	o, err := fs.ConsumerStore("o22", &ConsumerConfig{AckPolicy: AckExplicit})
	if err != nil {
		t.Fatalf("Unexepected error: %v", err)
	}
	defer o.Stop()

	// Track pending.
	var pending int

	testDelivered := func(dseq, sseq uint64) {
		t.Helper()
		ts := time.Now().Round(time.Second).UnixNano()
		if err := o.UpdateDelivered(dseq, sseq, 1, ts); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		pending++
		state, err := o.State()
		if err != nil {
			t.Fatalf("Error getting state: %v", err)
		}
		if state == nil {
			t.Fatalf("No state available")
		}
		expected := SequencePair{dseq, sseq}
		if state.Delivered != expected {
			t.Fatalf("Unexpected delivered state, wanted %+v, got %+v", expected, state.Delivered)
		}
		if len(state.Pending) != pending {
			t.Fatalf("Expected %d pending, got %d pending", pending, len(state.Pending))
		}
	}

	testDelivered(1, 100)
	testDelivered(2, 110)
	testDelivered(3, 130)
	testDelivered(4, 150)
	testDelivered(5, 165)

	testBadAck := func(dseq, sseq uint64) {
		t.Helper()
		if err := o.UpdateAcks(dseq, sseq); err == nil {
			t.Fatalf("Expected error but got none")
		}
	}
	testBadAck(3, 101)
	testBadAck(1, 1)

	testAck := func(dseq, sseq, dflr, sflr uint64) {
		t.Helper()
		if err := o.UpdateAcks(dseq, sseq); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		pending--
		state, err := o.State()
		if err != nil {
			t.Fatalf("Error getting state: %v", err)
		}
		if state == nil {
			t.Fatalf("No state available")
		}
		if len(state.Pending) != pending {
			t.Fatalf("Expected %d pending, got %d pending", pending, len(state.Pending))
		}
		eflr := SequencePair{dflr, sflr}
		if state.AckFloor != eflr {
			t.Fatalf("Unexpected ack floor state, wanted %+v, got %+v", eflr, state.AckFloor)
		}
	}

	testAck(1, 100, 1, 100)
	testAck(3, 130, 1, 100)
	testAck(2, 110, 3, 149) // We do not track explicit state on previous stream floors, so we take last known -1
	testAck(5, 165, 3, 149)
	testAck(4, 150, 5, 165)

	testDelivered(6, 170)
	testDelivered(7, 171)
	testDelivered(8, 172)
	testDelivered(9, 173)
	testDelivered(10, 200)

	testAck(7, 171, 5, 165)
	testAck(8, 172, 5, 165)

	state, err := o.State()
	if err != nil {
		t.Fatalf("Unexpected error getting state: %v", err)
	}
	o.Stop()

	o, err = fs.ConsumerStore("o22", &ConsumerConfig{AckPolicy: AckExplicit})
	if err != nil {
		t.Fatalf("Unexepected error: %v", err)
	}
	defer o.Stop()

	nstate, err := o.State()
	if err != nil {
		t.Fatalf("Unexpected error getting state: %v", err)
	}
	if !reflect.DeepEqual(nstate, state) {
		t.Fatalf("States don't match!")
	}
}

func TestFileStoreStreamStateDeleted(t *testing.T) {
	storeDir := createDir(t, JetStreamStoreDir)
	defer removeDir(t, storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	subj, toStore := "foo", uint64(10)
	for i := uint64(1); i <= toStore; i++ {
		msg := []byte(fmt.Sprintf("[%08d] Hello World!", i))
		if _, _, err := fs.StoreMsg(subj, nil, msg); err != nil {
			t.Fatalf("Error storing msg: %v", err)
		}
	}
	state := fs.State()
	if len(state.Deleted) != 0 {
		t.Fatalf("Expected deleted to be empty")
	}
	// Now remove some interior messages.
	var expected []uint64
	for seq := uint64(2); seq < toStore; seq += 2 {
		fs.RemoveMsg(seq)
		expected = append(expected, seq)
	}
	state = fs.State()
	if !reflect.DeepEqual(state.Deleted, expected) {
		t.Fatalf("Expected deleted to be %+v, got %+v\n", expected, state.Deleted)
	}
	// Now fill the gap by deleting 1 and 3
	fs.RemoveMsg(1)
	fs.RemoveMsg(3)
	expected = expected[2:]
	state = fs.State()
	if !reflect.DeepEqual(state.Deleted, expected) {
		t.Fatalf("Expected deleted to be %+v, got %+v\n", expected, state.Deleted)
	}
	if state.FirstSeq != 5 {
		t.Fatalf("Expected first seq to be 5, got %d", state.FirstSeq)
	}
	fs.Purge()
	if state = fs.State(); len(state.Deleted) != 0 {
		t.Fatalf("Expected no deleted after purge, got %+v\n", state.Deleted)
	}
}

// We have reports that sometimes under load a stream could complain about a storage directory
// not being empty.
func TestFileStoreStreamDeleteDirNotEmpty(t *testing.T) {
	storeDir := createDir(t, JetStreamStoreDir)
	defer removeDir(t, storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	subj, toStore := "foo", uint64(10)
	for i := uint64(1); i <= toStore; i++ {
		msg := []byte(fmt.Sprintf("[%08d] Hello World!", i))
		if _, _, err := fs.StoreMsg(subj, nil, msg); err != nil {
			t.Fatalf("Error storing msg: %v", err)
		}
	}

	ready := make(chan bool)
	go func() {
		g := filepath.Join(storeDir, "g")
		ready <- true
		for i := 0; i < 100; i++ {
			ioutil.WriteFile(g, []byte("OK"), defaultFilePerms)
		}
	}()

	<-ready
	if err := fs.Delete(); err != nil {
		t.Fatalf("Delete returned an error: %v", err)
	}
}

func TestFileStoreConsumerPerf(t *testing.T) {
	// Comment out to run, holding place for now.
	t.SkipNow()

	storeDir := createDir(t, JetStreamStoreDir)
	defer removeDir(t, storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	o, err := fs.ConsumerStore("o22", &ConsumerConfig{AckPolicy: AckExplicit})
	if err != nil {
		t.Fatalf("Unexepected error: %v", err)
	}
	// Get the underlying impl.
	oc := o.(*consumerFileStore)
	// Wait for flusher to br running
	checkFor(t, time.Second, 20*time.Millisecond, func() error {
		if !oc.inFlusher() {
			return fmt.Errorf("not in flusher")
		}
		return nil
	})

	// Stop flusher for this benchmark since we will invoke directly.
	oc.mu.Lock()
	qch := oc.qch
	oc.qch = nil
	oc.mu.Unlock()
	close(qch)

	checkFor(t, time.Second, 20*time.Millisecond, func() error {
		if oc.inFlusher() {
			return fmt.Errorf("still in flusher")
		}
		return nil
	})

	toStore := uint64(1_000_000)

	start := time.Now()

	ts := start.UnixNano()

	for i := uint64(1); i <= toStore; i++ {
		if err := o.UpdateDelivered(i, i, 1, ts); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}
	tt := time.Since(start)
	fmt.Printf("time to update %d is %v\n", toStore, tt)
	fmt.Printf("%.0f updates/sec\n", float64(toStore)/tt.Seconds())

	start = time.Now()
	oc.mu.Lock()
	buf, err := oc.encodeState()
	oc.mu.Unlock()
	if err != nil {
		t.Fatalf("Error encoding state: %v", err)
	}
	fmt.Printf("time to encode %d bytes is %v\n", len(buf), time.Since(start))
	start = time.Now()
	oc.writeState(buf)
	fmt.Printf("time to write is %v\n", time.Since(start))
}

func TestFileStoreStreamIndexBug(t *testing.T) {
	// https://github.com/nats-io/jetstream/issues/406
	badIdxBytes, _ := base64.StdEncoding.DecodeString("FgGBkw7D/f8/772iDPDIgbU=")
	dir := createDir(t, "js-bad-idx-")
	defer removeDir(t, dir)
	fn := filepath.Join(dir, "1.idx")
	ioutil.WriteFile(fn, badIdxBytes, 0644)
	mb := &msgBlock{index: 1, ifn: fn}
	if err := mb.readIndexInfo(); err == nil || !strings.Contains(err.Error(), "short index") {
		t.Fatalf("Expected error during readIndexInfo(): %v", err)
	}
}

// Reported by Ivan.
func TestFileStoreStreamDeleteCacheBug(t *testing.T) {
	storeDir := createDir(t, JetStreamStoreDir)
	defer removeDir(t, storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir, CacheExpire: 50 * time.Millisecond}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	subj, msg := "foo", []byte("Hello World")

	if _, _, err := fs.StoreMsg(subj, nil, msg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, _, err := fs.StoreMsg(subj, nil, msg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if _, err := fs.EraseMsg(1); err != nil {
		t.Fatalf("Got an error on remove of %d: %v", 1, err)
	}
	time.Sleep(100 * time.Millisecond)
	if _, _, _, _, err := fs.LoadMsg(2); err != nil {
		t.Fatalf("Unexpected error looking up msg: %v", err)
	}
}

// https://github.com/nats-io/nats-server/issues/2068
func TestFileStoreStreamPurgeAndDirtyRestartBug(t *testing.T) {
	storeDir := createDir(t, JetStreamStoreDir)
	defer removeDir(t, storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	// Load up some messages.
	num, subj, hdr, msg := 100, "foo", []byte("name:derek"), []byte("Hello World")
	for i := 0; i < num; i++ {
		fs.StoreMsg(subj, hdr, msg)
	}
	// Now purge
	fs.Purge()

	// Snapshot state.
	state := fs.State()
	if state.FirstSeq != uint64(num+1) || state.LastSeq != uint64(num) {
		t.Fatalf("Unexpected state: %+v", state)
	}

	// Now we will stop the store and corrupt the index such that on restart it will do a rebuild.
	fs.mu.Lock()
	lmb := fs.lmb
	fs.mu.Unlock()

	lmb.mu.RLock()
	ifn := lmb.ifn
	lmb.mu.RUnlock()

	fs.Stop()

	fd, err := os.OpenFile(ifn, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Error opening the index file: %v", err)
	}
	defer fd.Close()
	fi, _ := fd.Stat()
	if _, err = fd.WriteAt([]byte{1, 1}, fi.Size()-2); err != nil {
		t.Fatalf("Error writing the index file: %v", err)
	}
	fd.Close()

	// Restart
	fs, err = newFileStore(FileStoreConfig{StoreDir: storeDir}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	state = fs.State()
	if state.FirstSeq != uint64(num+1) || state.LastSeq != uint64(num) {
		t.Fatalf("Unexpected state: %+v", state)
	}
}

// rip
func TestFileStoreStreamFailToRollBug(t *testing.T) {
	storeDir := createDir(t, JetStreamStoreDir)
	defer removeDir(t, storeDir)

	fs, err := newFileStore(
		FileStoreConfig{StoreDir: storeDir, BlockSize: 512},
		StreamConfig{Name: "zzz", Storage: FileStorage, MaxBytes: 300},
	)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	// Make sure we properly roll underlying blocks.
	n, msg := 200, bytes.Repeat([]byte("ABC"), 33) // ~100bytes
	for i := 0; i < n; i++ {
		if _, _, err := fs.StoreMsg("zzz", nil, msg); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	// Grab some info for introspection.
	fs.mu.RLock()
	numBlks := len(fs.blks)
	var index uint64
	var blkSize int64
	if numBlks > 0 {
		mb := fs.blks[0]
		mb.mu.RLock()
		index = mb.index
		if fi, _ := os.Stat(mb.mfn); fi != nil {
			blkSize = fi.Size()
		}
		mb.mu.RUnlock()
	}
	fs.mu.RUnlock()

	if numBlks != 1 {
		t.Fatalf("Expected only one block, got %d", numBlks)
	}
	if index < 60 {
		t.Fatalf("Expected a block index > 60, got %d", index)
	}
	if blkSize > 512 {
		t.Fatalf("Expected block to be <= 512, got %d", blkSize)
	}
}

// We had a case where a consumer state had a redelivered record that had seq of 0.
// This was causing the server to panic.
func TestBadConsumerState(t *testing.T) {
	bs := []byte("\x16\x02\x01\x01\x03\x02\x01\x98\xf4\x8a\x8a\f\x01\x03\x86\xfa\n\x01\x00\x01")
	if cs, err := decodeConsumerState(bs); err != nil || cs == nil {
		t.Fatalf("Expected to not throw error, got %v and %+v", err, cs)
	}
}

func TestFileStoreExpireMsgsOnStart(t *testing.T) {
	storeDir := createDir(t, JetStreamStoreDir)
	defer removeDir(t, storeDir)

	ttl := 250 * time.Millisecond
	cfg := StreamConfig{Name: "ORDERS", Subjects: []string{"orders.*"}, Storage: FileStorage, MaxAge: ttl}
	var fs *fileStore

	startFS := func() *fileStore {
		t.Helper()
		fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir, BlockSize: 8 * 1024}, cfg)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		return fs
	}

	newFS := func() *fileStore {
		t.Helper()
		if fs != nil {
			fs.Stop()
			fs = nil
		}
		removeDir(t, storeDir)
		return startFS()
	}

	restartFS := func(delay time.Duration) *fileStore {
		if fs != nil {
			fs.Stop()
			fs = nil
			time.Sleep(delay)
		}
		fs = startFS()
		return fs
	}

	fs = newFS()
	defer fs.Stop()

	msg := bytes.Repeat([]byte("ABC"), 33) // ~100bytes
	loadMsgs := func(n int) {
		t.Helper()
		for i := 1; i <= n; i++ {
			if _, _, err := fs.StoreMsg(fmt.Sprintf("orders.%d", i%10), nil, msg); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		}
	}

	checkState := func(msgs, first, last uint64) {
		t.Helper()
		if fs == nil {
			t.Fatalf("No fs")
			return
		}
		state := fs.State()
		if state.Msgs != msgs {
			t.Fatalf("Expected %d msgs, got %d", msgs, state.Msgs)
		}
		if state.FirstSeq != first {
			t.Fatalf("Expected %d as first, got %d", first, state.FirstSeq)
		}
		if state.LastSeq != last {
			t.Fatalf("Expected %d as last, got %d", last, state.LastSeq)
		}
	}

	checkNumBlks := func(expected int) {
		fs.mu.RLock()
		n := len(fs.blks)
		fs.mu.RUnlock()
		if n != expected {
			t.Fatalf("Expected %d msg blks, got %d", expected, n)
		}
	}

	// Check the filtered subject state and make sure that is tracked properly.
	checkFiltered := func(subject string, ss SimpleState) {
		t.Helper()
		fss := fs.FilteredState(1, subject)
		if fss != ss {
			t.Fatalf("Expected FilteredState of %+v, got %+v", ss, fss)
		}
	}

	// Make sure state on disk matches (e.g. writeIndexInfo properly called)
	checkBlkState := func(index int) {
		t.Helper()
		fs.mu.RLock()
		if index >= len(fs.blks) {
			t.Fatalf("Out of range, wanted %d but only %d blks", index, len(fs.blks))
		}
		mb := fs.blks[index]
		fs.mu.RUnlock()

		var errStr string

		mb.mu.RLock()
		// We will do a readIndex op on our clone and then compare.
		mbc := &msgBlock{fs: fs, ifn: mb.ifn}
		if err := mbc.readIndexInfo(); err != nil {
			mb.mu.RUnlock()
			t.Fatalf("Error during readIndexInfo: %v", err)
		}
		// Check state as represented by index info.
		if mb.msgs != mbc.msgs {
			errStr = fmt.Sprintf("msgs do not match: %d vs %d", mb.msgs, mbc.msgs)
		} else if mb.bytes != mbc.bytes {
			errStr = fmt.Sprintf("bytes do not match: %d vs %d", mb.bytes, mbc.bytes)
		} else if mb.first != mbc.first {
			errStr = fmt.Sprintf("first state does not match: %d vs %d", mb.first, mbc.first)
		} else if mb.last != mbc.last {
			errStr = fmt.Sprintf("last state does not match: %d vs %d", mb.last, mbc.last)
		} else if !reflect.DeepEqual(mb.dmap, mbc.dmap) {
			errStr = fmt.Sprintf("deleted map does not match: %d vs %d", mb.dmap, mbc.dmap)
		}
		mb.mu.RUnlock()
		if errStr != _EMPTY_ {
			t.Fatal(errStr)
		}
	}

	lastSeqForBlk := func(index int) uint64 {
		t.Helper()
		fs.mu.RLock()
		defer fs.mu.RUnlock()
		if len(fs.blks) == 0 {
			t.Fatalf("No blocks?")
		}
		mb := fs.blks[0]
		mb.mu.RLock()
		defer mb.mu.RUnlock()
		return mb.last.seq
	}

	// Actual testing here.

	loadMsgs(500)
	restartFS(ttl + 100*time.Millisecond)
	checkState(0, 501, 500)
	checkNumBlks(0)

	// Now check partial expires and the fss tracking state.
	// Small numbers is to keep them in one block.
	fs = newFS()
	loadMsgs(10)
	time.Sleep(100 * time.Millisecond)
	loadMsgs(10)
	restartFS(ttl - 100*time.Millisecond + 25*time.Millisecond) // Just want half
	checkState(10, 11, 20)
	checkNumBlks(1)
	checkFiltered("orders.*", SimpleState{Msgs: 10, First: 11, Last: 20})
	checkFiltered("orders.5", SimpleState{Msgs: 1, First: 15, Last: 15})
	checkBlkState(0)

	fs = newFS()
	loadMsgs(5)
	time.Sleep(100 * time.Millisecond)
	loadMsgs(15)
	restartFS(ttl - 100*time.Millisecond + 25*time.Millisecond) // Just want half
	checkState(15, 6, 20)
	checkFiltered("orders.*", SimpleState{Msgs: 15, First: 6, Last: 20})
	checkFiltered("orders.5", SimpleState{Msgs: 2, First: 10, Last: 20})

	// Now we want to test that if the end of a msg block is all deletes msgs that we do the right thing.
	fs = newFS()
	loadMsgs(150)
	time.Sleep(100 * time.Millisecond)
	loadMsgs(100)

	checkNumBlks(5)

	// Now delete 10 messages from the end of the first block which we will expire on restart.
	// We will expire up to seq 100, so delete 91-100.
	lseq := lastSeqForBlk(0)
	for seq := lseq; seq > lseq-10; seq-- {
		removed, err := fs.RemoveMsg(seq)
		if err != nil || !removed {
			t.Fatalf("Error removing message: %v", err)
		}
	}
	restartFS(ttl - 100*time.Millisecond + 25*time.Millisecond) // Just want half
	checkState(100, 151, 250)
	checkNumBlks(3) // We should only have 3 blks left.
	checkBlkState(0)

	// Now make sure that we properly clean up any internal dmap entries (sparse) when expiring.
	fs = newFS()
	loadMsgs(10)
	// Remove some in sparse fashion, adding to dmap.
	fs.RemoveMsg(2)
	fs.RemoveMsg(4)
	fs.RemoveMsg(6)
	time.Sleep(100 * time.Millisecond)
	loadMsgs(10)
	restartFS(ttl - 100*time.Millisecond + 25*time.Millisecond) // Just want half
	checkState(10, 11, 20)
	checkNumBlks(1)
	checkBlkState(0)

	// Make sure expiring a block with tail deleted messages removes the message block etc.
	fs = newFS()
	loadMsgs(7)
	time.Sleep(100 * time.Millisecond)
	loadMsgs(3)
	fs.RemoveMsg(8)
	fs.RemoveMsg(9)
	fs.RemoveMsg(10)
	restartFS(ttl - 100*time.Millisecond + 25*time.Millisecond)
	checkState(0, 11, 10)

	fs.Stop()
	// Not for start per se but since we have all the test tooling here check that Compact() does right thing as well.
	fs = newFS()
	defer fs.Stop()
	loadMsgs(100)
	checkFiltered("orders.*", SimpleState{Msgs: 100, First: 1, Last: 100})
	checkFiltered("orders.5", SimpleState{Msgs: 10, First: 5, Last: 95})
	// Check that Compact keeps fss updated, does dmap etc.
	fs.Compact(51)
	checkFiltered("orders.*", SimpleState{Msgs: 50, First: 51, Last: 100})
	checkFiltered("orders.5", SimpleState{Msgs: 5, First: 55, Last: 95})
	checkBlkState(0)
}

func TestFileStoreSparseCompaction(t *testing.T) {
	storeDir := createDir(t, JetStreamStoreDir)
	defer removeDir(t, storeDir)

	cfg := StreamConfig{Name: "KV", Subjects: []string{"kv.>"}, Storage: FileStorage}
	var fs *fileStore

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir, BlockSize: 1024 * 1024}, cfg)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	msg := bytes.Repeat([]byte("ABC"), 33) // ~100bytes
	loadMsgs := func(n int) {
		t.Helper()
		for i := 1; i <= n; i++ {
			if _, _, err := fs.StoreMsg(fmt.Sprintf("kv.%d", i%10), nil, msg); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		}
	}

	checkState := func(msgs, first, last uint64) {
		t.Helper()
		if fs == nil {
			t.Fatalf("No fs")
			return
		}
		state := fs.State()
		if state.Msgs != msgs {
			t.Fatalf("Expected %d msgs, got %d", msgs, state.Msgs)
		}
		if state.FirstSeq != first {
			t.Fatalf("Expected %d as first, got %d", first, state.FirstSeq)
		}
		if state.LastSeq != last {
			t.Fatalf("Expected %d as last, got %d", last, state.LastSeq)
		}
	}

	deleteMsgs := func(seqs ...uint64) {
		t.Helper()
		for _, seq := range seqs {
			removed, err := fs.RemoveMsg(seq)
			if err != nil || !removed {
				t.Fatalf("Got an error on remove of %d: %v", seq, err)
			}
		}
	}

	eraseMsgs := func(seqs ...uint64) {
		t.Helper()
		for _, seq := range seqs {
			removed, err := fs.EraseMsg(seq)
			if err != nil || !removed {
				t.Fatalf("Got an error on erase of %d: %v", seq, err)
			}
		}
	}

	compact := func() {
		t.Helper()
		var ssb, ssa StreamState
		fs.FastState(&ssb)
		tb, ub, _ := fs.Utilization()

		fs.mu.RLock()
		if len(fs.blks) == 0 {
			t.Fatalf("No blocks?")
		}
		mb := fs.blks[0]
		fs.mu.RUnlock()

		mb.mu.Lock()
		mb.compact()
		mb.mu.Unlock()
		fs.FastState(&ssa)
		if !reflect.DeepEqual(ssb, ssa) {
			t.Fatalf("States do not match; %+v vs %+v", ssb, ssa)
		}
		ta, ua, _ := fs.Utilization()
		if ub != ua {
			t.Fatalf("Expected used to be the same, got %d vs %d", ub, ua)
		}
		if ta >= tb {
			t.Fatalf("Expected total after to be less then before, got %d vs %d", tb, ta)
		}
	}

	// Actual testing here.
	loadMsgs(1000)
	checkState(1000, 1, 1000)

	// Now delete a few messages.
	deleteMsgs(1)
	compact()

	deleteMsgs(1000, 999, 998, 997)
	compact()

	eraseMsgs(500, 502, 504, 506, 508, 510)
	compact()

	// Now test encrypted mode.
	fs.Delete()

	prf := func(context []byte) ([]byte, error) {
		h := hmac.New(sha256.New, []byte("dlc22"))
		if _, err := h.Write(context); err != nil {
			return nil, err
		}
		return h.Sum(nil), nil
	}

	fs, err = newFileStoreWithCreated(FileStoreConfig{StoreDir: storeDir, BlockSize: 1024 * 1024}, cfg, time.Now(), prf)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	loadMsgs(1000)
	checkState(1000, 1, 1000)

	// Now delete a few messages.
	deleteMsgs(1)
	compact()

	deleteMsgs(1000, 999, 998, 997)
	compact()

	eraseMsgs(500, 502, 504, 506, 508, 510)
	compact()
}

func TestFileStoreSparseCompactionWithInteriorDeletes(t *testing.T) {
	storeDir := createDir(t, JetStreamStoreDir)
	defer removeDir(t, storeDir)

	cfg := StreamConfig{Name: "KV", Subjects: []string{"kv.>"}, Storage: FileStorage}
	var fs *fileStore

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, cfg)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for i := 1; i <= 1000; i++ {
		if _, _, err := fs.StoreMsg(fmt.Sprintf("kv.%d", i%10), nil, []byte("OK")); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	// Now do interior deletes.
	for _, seq := range []uint64{500, 600, 700, 800} {
		removed, err := fs.RemoveMsg(seq)
		if err != nil || !removed {
			t.Fatalf("Got an error on remove of %d: %v", seq, err)
		}
	}

	_, _, _, _, err = fs.LoadMsg(900)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Do compact by hand, make sure we can still access msgs past the interior deletes.
	fs.mu.RLock()
	lmb := fs.lmb
	lmb.dirtyCloseWithRemove(false)
	lmb.compact()
	fs.mu.RUnlock()

	_, _, _, _, err = fs.LoadMsg(900)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

// When messages span multiple blocks and we want to purge but keep some amount, say 1, we would remove all.
// This is because we would not break out of iterator across more message blocks.
// Issue #2622
func TestFileStorePurgeExKeepOneBug(t *testing.T) {
	storeDir := createDir(t, JetStreamStoreDir)
	defer removeDir(t, storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir, BlockSize: 128}, StreamConfig{Name: "zzz", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	fill := bytes.Repeat([]byte("X"), 128)

	fs.StoreMsg("A", nil, []byte("META"))
	fs.StoreMsg("B", nil, fill)
	fs.StoreMsg("A", nil, []byte("META"))
	fs.StoreMsg("B", nil, fill)

	if fss := fs.FilteredState(1, "A"); fss.Msgs != 2 {
		t.Fatalf("Expected to find 2 `A` msgs, got %d", fss.Msgs)
	}

	n, err := fs.PurgeEx("A", 0, 1)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if n != 1 {
		t.Fatalf("Expected PurgeEx to remove 1 `A` msgs, got %d", n)
	}
	if fss := fs.FilteredState(1, "A"); fss.Msgs != 1 {
		t.Fatalf("Expected to find 1 `A` msgs, got %d", fss.Msgs)
	}
}

func TestFileStoreRemoveLastWriteIndex(t *testing.T) {
	storeDir := createDir(t, JetStreamStoreDir)
	defer removeDir(t, storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, StreamConfig{Name: "TEST", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	for i := 0; i < 10; i++ {
		fs.StoreMsg("foo", nil, []byte("msg"))
	}
	for i := 0; i < 10; i++ {
		fs.RemoveMsg(uint64(i + 1))
	}

	fs.mu.Lock()
	fname := fs.lmb.ifn
	fs.mu.Unlock()

	fi, err := os.Stat(fname)
	if err != nil {
		t.Fatalf("Error getting stats for index file %q: %v", fname, err)
	}
	if fi.Size() == 0 {
		t.Fatalf("Index file %q size is 0", fname)
	}
}

func TestFileStoreFilteredPendingBug(t *testing.T) {
	storeDir := createDir(t, JetStreamStoreDir)
	defer removeDir(t, storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir}, StreamConfig{Name: "TEST", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	fs.StoreMsg("foo", nil, []byte("msg"))
	fs.StoreMsg("bar", nil, []byte("msg"))
	fs.StoreMsg("baz", nil, []byte("msg"))

	fs.mu.Lock()
	mb := fs.lmb
	fs.mu.Unlock()

	total, f, l := mb.filteredPending("foo", false, 3)
	if total != 0 {
		t.Fatalf("Expected total of 0 but got %d", total)
	}
	if f != 0 || l != 0 {
		t.Fatalf("Expected first and last to be 0 as well, but got %d %d", f, l)
	}
}

// Test to optimize the selectMsgBlock with lots of blocks.
func TestFileStoreFetchPerf(t *testing.T) {
	// Comment out to run.
	t.SkipNow()

	storeDir := createDir(t, JetStreamStoreDir)
	defer removeDir(t, storeDir)

	fs, err := newFileStore(FileStoreConfig{StoreDir: storeDir, BlockSize: 8192, AsyncFlush: true}, StreamConfig{Name: "TEST", Storage: FileStorage})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Stop()

	// Will create 25k msg blocks.
	n, subj, msg := 100_000, "zzz", bytes.Repeat([]byte("ABC"), 600)
	for i := 0; i < n; i++ {
		if _, _, err := fs.StoreMsg(subj, nil, msg); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	// Time how long it takes us to load all messages.
	now := time.Now()
	for i := 0; i < n; i++ {
		_, _, _, _, err := fs.LoadMsg(uint64(i))
		if err != nil {
			t.Fatalf("Unexpected error looking up seq %d: %v", i, err)
		}
	}
	fmt.Printf("Elapsed to load all messages is %v\n", time.Since(now))
}
