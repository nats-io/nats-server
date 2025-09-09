// Copyright 2019-2025 The NATS Authors
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

//go:build !skip_store_tests

package server

import (
	"archive/tar"
	"bytes"
	"crypto/hmac"
	crand "crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"math/bits"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/klauspost/compress/s2"
	"github.com/nats-io/nats-server/v2/server/ats"
	"github.com/nats-io/nats-server/v2/server/gsl"
	"github.com/nats-io/nuid"
)

func testFileStoreAllPermutations(t *testing.T, fn func(t *testing.T, fcfg FileStoreConfig)) {
	for _, fcfg := range []FileStoreConfig{
		{Cipher: NoCipher, Compression: NoCompression},
		{Cipher: NoCipher, Compression: S2Compression},
		{Cipher: AES, Compression: NoCompression},
		{Cipher: AES, Compression: S2Compression},
		{Cipher: ChaCha, Compression: NoCompression},
		{Cipher: ChaCha, Compression: S2Compression},
	} {
		subtestName := fmt.Sprintf("%s-%s", fcfg.Cipher, fcfg.Compression)
		t.Run(subtestName, func(t *testing.T) {
			fcfg.StoreDir = t.TempDir()
			fn(t, fcfg)
			time.Sleep(100 * time.Millisecond)
		})
	}
}

func prf(fcfg *FileStoreConfig) func(context []byte) ([]byte, error) {
	if fcfg.Cipher == NoCipher {
		return nil
	}
	return func(context []byte) ([]byte, error) {
		h := hmac.New(sha256.New, []byte("dlc22"))
		if _, err := h.Write(context); err != nil {
			return nil, err
		}
		return h.Sum(nil), nil
	}
}

func TestFileStoreBasics(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("Hello World")
		for i := 1; i <= 5; i++ {
			now := time.Now().UnixNano()
			if seq, ts, err := fs.StoreMsg(subj, nil, msg, 0); err != nil {
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

		var smv StoreMsg
		sm, err := fs.LoadMsg(2, &smv)
		if err != nil {
			t.Fatalf("Unexpected error looking up msg: %v", err)
		}
		if sm.subj != subj {
			t.Fatalf("Subjects don't match, original %q vs %q", subj, sm.subj)
		}
		if !bytes.Equal(sm.msg, msg) {
			t.Fatalf("Msgs don't match, original %q vs %q", msg, sm.msg)
		}
		_, err = fs.LoadMsg(3, nil)
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
	})
}

func TestFileStoreMsgHeaders(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)

		require_NoError(t, err)
		defer fs.Stop()
		subj, hdr, msg := "foo", []byte("name:derek"), []byte("Hello World")
		elen := 22 + len(subj) + 4 + len(hdr) + len(msg) + 8
		if sz := int(fileStoreMsgSize(subj, hdr, msg)); sz != elen {
			t.Fatalf("Wrong size for stored msg with header")
		}
		fs.StoreMsg(subj, hdr, msg, 0)
		var smv StoreMsg
		sm, err := fs.LoadMsg(1, &smv)
		if err != nil {
			t.Fatalf("Unexpected error looking up msg: %v", err)
		}
		if !bytes.Equal(msg, sm.msg) {
			t.Fatalf("Expected same msg, got %q vs %q", sm.msg, msg)
		}
		if !bytes.Equal(hdr, sm.hdr) {
			t.Fatalf("Expected same hdr, got %q vs %q", sm.hdr, hdr)
		}
		if removed, _ := fs.EraseMsg(1); !removed {
			t.Fatalf("Expected erase msg to return success")
		}
	})
}

func TestFileStoreBasicWriteMsgsAndRestore(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		if _, err := newFileStore(fcfg, StreamConfig{Storage: MemoryStorage}); err == nil {
			t.Fatalf("Expected an error with wrong type")
		}
		if _, err := newFileStore(fcfg, StreamConfig{Storage: FileStorage}); err == nil {
			t.Fatalf("Expected an error with no name")
		}

		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj := "foo"

		// Write 100 msgs
		toStore := uint64(100)
		for i := uint64(1); i <= toStore; i++ {
			msg := []byte(fmt.Sprintf("[%08d] Hello World!", i))
			if seq, _, err := fs.StoreMsg(subj, nil, msg, 0); err != nil {
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
		if _, _, err := fs.StoreMsg(subj, nil, []byte("no work"), 0); err == nil {
			t.Fatalf("Expected an error for StoreMsg call after Stop, got none")
		}

		// Restart
		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
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
			if seq, _, err := fs.StoreMsg(subj, nil, msg, 0); err != nil {
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
		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
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

		// Restart
		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		state = fs.State()
		if state.Msgs != 0 {
			t.Fatalf("Expected %d msgs, got %d", 0, state.Msgs)
		}
		if state.Bytes != 0 {
			t.Fatalf("Expected %d bytes, got %d", 0, state.Bytes)
		}

		seq, _, err := fs.StoreMsg(subj, nil, []byte("Hello"), 0)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		fs.RemoveMsg(seq)

		fs.Stop()

		// Restart
		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		state = fs.State()
		require_Equal(t, state.FirstSeq, seq+1)
	})
}

func TestFileStoreSelectNextFirst(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 256
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		numMsgs := 10
		subj, msg := "zzz", []byte("Hello World")
		for i := 0; i < numMsgs; i++ {
			fs.StoreMsg(subj, nil, msg, 0)
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
	})
}

func TestFileStoreSkipMsg(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 256
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
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

		fs.StoreMsg("zzz", nil, []byte("Hello World!"), 0)
		fs.SkipMsg()
		fs.SkipMsg()
		fs.StoreMsg("zzz", nil, []byte("Hello World!"), 0)
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

		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		state = fs.State()
		if state.Msgs != 2 {
			t.Fatalf("Expected %d msgs, got %d", 2, state.Msgs)
		}
		if state.FirstSeq != uint64(numSkips+1) || state.LastSeq != uint64(numSkips+5) {
			t.Fatalf("Expected first to be %d and last to be %d. got first %d and last %d", numSkips+1, numSkips+5, state.FirstSeq, state.LastSeq)
		}

		var smv StoreMsg
		sm, err := fs.LoadMsg(11, &smv)
		if err != nil {
			t.Fatalf("Unexpected error looking up seq 11: %v", err)
		}
		if sm.subj != "zzz" || string(sm.msg) != "Hello World!" {
			t.Fatalf("Message did not match")
		}

		fs.SkipMsg()
		nseq, _, err := fs.StoreMsg("AAA", nil, []byte("Skip?"), 0)
		if err != nil {
			t.Fatalf("Unexpected error looking up seq 11: %v", err)
		}
		if nseq != 17 {
			t.Fatalf("Expected seq of %d but got %d", 17, nseq)
		}

		// Make sure we recover same state.
		fs.Stop()

		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		sm, err = fs.LoadMsg(nseq, &smv)
		if err != nil {
			t.Fatalf("Unexpected error looking up seq %d: %v", nseq, err)
		}
		if sm.subj != "AAA" || string(sm.msg) != "Skip?" {
			t.Fatalf("Message did not match")
		}
	})
}

func TestFileStoreWriteExpireWrite(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cexp := 10 * time.Millisecond
		fcfg.CacheExpire = cexp
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		toSend := 10
		for i := 0; i < toSend; i++ {
			fs.StoreMsg("zzz", nil, []byte("Hello World!"), 0)
		}

		// Wait for write cache portion to go to zero.
		checkFor(t, time.Second, 20*time.Millisecond, func() error {
			if csz := fs.cacheSize(); csz != 0 {
				return fmt.Errorf("cache size not 0, got %s", friendlyBytes(int64(csz)))
			}
			return nil
		})

		for i := 0; i < toSend; i++ {
			fs.StoreMsg("zzz", nil, []byte("Hello World! - 22"), 0)
		}

		if state := fs.State(); state.Msgs != uint64(toSend*2) {
			t.Fatalf("Expected %d msgs, got %d", toSend*2, state.Msgs)
		}

		// Make sure we recover same state.
		fs.Stop()

		fcfg.CacheExpire = 0
		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if state := fs.State(); state.Msgs != uint64(toSend*2) {
			t.Fatalf("Expected %d msgs, got %d", toSend*2, state.Msgs)
		}

		// Now load them in and check.
		var smv StoreMsg
		for i := 1; i <= toSend*2; i++ {
			sm, err := fs.LoadMsg(uint64(i), &smv)
			if err != nil {
				t.Fatalf("Unexpected error looking up seq %d: %v", i, err)
			}
			str := "Hello World!"
			if i > toSend {
				str = "Hello World! - 22"
			}
			if sm.subj != "zzz" || string(sm.msg) != str {
				t.Fatalf("Message did not match")
			}
		}
	})
}

func TestFileStoreMsgLimit(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage, MaxMsgs: 10}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("Hello World")
		for i := 0; i < 10; i++ {
			fs.StoreMsg(subj, nil, msg, 0)
		}
		state := fs.State()
		if state.Msgs != 10 {
			t.Fatalf("Expected %d msgs, got %d", 10, state.Msgs)
		}
		if _, _, err := fs.StoreMsg(subj, nil, msg, 0); err != nil {
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
		if _, err := fs.LoadMsg(1, nil); err == nil {
			t.Fatalf("Expected error looking up seq 1 but got none")
		}
	})
}

func TestFileStoreMsgLimitBug(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage, MaxMsgs: 1}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("Hello World")
		fs.StoreMsg(subj, nil, msg, 0)
		fs.StoreMsg(subj, nil, msg, 0)
		fs.Stop()

		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage, MaxMsgs: 1}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()
		fs.StoreMsg(subj, nil, msg, 0)
	})
}

func TestFileStoreBytesLimit(t *testing.T) {
	subj, msg := "foo", make([]byte, 512)
	storedMsgSize := fileStoreMsgSize(subj, nil, msg)

	toStore := uint64(1024)
	maxBytes := storedMsgSize * toStore

	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage, MaxBytes: int64(maxBytes)}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		for i := uint64(0); i < toStore; i++ {
			fs.StoreMsg(subj, nil, msg, 0)
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
			if _, _, err := fs.StoreMsg(subj, nil, msg, 0); err != nil {
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
	})
}

// https://github.com/nats-io/nats-server/issues/4771
func TestFileStoreBytesLimitWithDiscardNew(t *testing.T) {
	subj, msg := "tiny", make([]byte, 7)
	storedMsgSize := fileStoreMsgSize(subj, nil, msg)

	toStore := uint64(2)
	maxBytes := 100

	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cfg := StreamConfig{Name: "zzz", Storage: FileStorage, MaxBytes: int64(maxBytes), Discard: DiscardNew}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		for i := 0; i < 10; i++ {
			_, _, err := fs.StoreMsg(subj, nil, msg, 0)
			if i < int(toStore) {
				if err != nil {
					t.Fatalf("Error storing msg: %v", err)
				}
			} else if !errors.Is(err, ErrMaxBytes) {
				t.Fatalf("Storing msg should result in: %v", ErrMaxBytes)
			}
		}
		state := fs.State()
		if state.Msgs != toStore {
			t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
		}
		if state.Bytes != storedMsgSize*toStore {
			t.Fatalf("Expected bytes to be %d, got %d", storedMsgSize*toStore, state.Bytes)
		}
	})
}

func TestFileStoreAgeLimit(t *testing.T) {
	maxAge := 1 * time.Second

	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		if fcfg.Compression != NoCompression {
			// TODO(nat): This test fails at the moment with compression enabled
			// because it takes longer to compress the blocks, by which time the
			// messages have expired. Need to think about a balanced age so that
			// the test doesn't take too long in non-compressed cases.
			t.SkipNow()
		}

		fcfg.BlockSize = 256
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage, MaxAge: maxAge}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Store some messages. Does not really matter how many.
		subj, msg := "foo", []byte("Hello World")
		toStore := 500
		for i := 0; i < toStore; i++ {
			if _, _, err := fs.StoreMsg(subj, nil, msg, 0); err != nil {
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
			fs.StoreMsg(subj, nil, msg, 0)
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
		if elapsed := time.Since(start); elapsed > 5*time.Second {
			t.Fatalf("Took too long to expire: %v", elapsed)
		}
	})
}

func TestFileStoreTimeStamps(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		last := time.Now().UnixNano()
		subj, msg := "foo", []byte("Hello World")
		for i := 0; i < 10; i++ {
			time.Sleep(5 * time.Millisecond)
			fs.StoreMsg(subj, nil, msg, 0)
		}
		var smv StoreMsg
		for seq := uint64(1); seq <= 10; seq++ {
			sm, err := fs.LoadMsg(seq, &smv)
			if err != nil {
				t.Fatalf("Unexpected error looking up msg [%d]: %v", seq, err)
			}
			// These should be different
			if sm.ts <= last {
				t.Fatalf("Expected different timestamps, got last %v vs %v", last, sm.ts)
			}
			last = sm.ts
		}
	})
}

func TestFileStorePurge(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		blkSize := uint64(64 * 1024)
		fcfg.BlockSize = blkSize
		created := time.Now()

		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", make([]byte, 8*1024)
		storedMsgSize := fileStoreMsgSize(subj, nil, msg)

		toStore := uint64(1024)
		for i := uint64(0); i < toStore; i++ {
			fs.StoreMsg(subj, nil, msg, 0)
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

		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if numBlocks := fs.numMsgBlocks(); numBlocks != 1 {
			t.Fatalf("Expected to have exactly 1 empty msg block, got %d", numBlocks)
		}

		checkPurgeState(toStore)

		// Now make sure we clean up any dangling purged messages.
		for i := uint64(0); i < toStore; i++ {
			fs.StoreMsg(subj, nil, msg, 0)
		}
		state = fs.State()
		if state.Msgs != toStore {
			t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
		}
		if state.Bytes != storedMsgSize*toStore {
			t.Fatalf("Expected bytes to be %d, got %d", storedMsgSize*toStore, state.Bytes)
		}

		// We will simulate crashing before the purge directory is cleared.
		mdir := filepath.Join(fs.fcfg.StoreDir, msgDir)
		pdir := filepath.Join(fs.fcfg.StoreDir, "ptest")
		os.Rename(mdir, pdir)
		os.MkdirAll(mdir, 0755)

		fs.Purge()
		checkPurgeState(toStore * 2)

		// Make sure we recover same state.
		fs.Stop()

		purgeDir := filepath.Join(fs.fcfg.StoreDir, purgeDir)
		os.Rename(pdir, purgeDir)

		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if numBlocks := fs.numMsgBlocks(); numBlocks != 1 {
			t.Fatalf("Expected to have exactly 1 empty msg block, got %d", numBlocks)
		}

		checkPurgeState(toStore * 2)

		checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
			if _, err := os.Stat(purgeDir); err == nil {
				return fmt.Errorf("purge directory still present")
			}
			return nil
		})
	})
}

func TestFileStoreCompact(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 350
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("Hello World")
		for i := 0; i < 10; i++ {
			fs.StoreMsg(subj, nil, msg, 0)
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

		fs.Stop()

		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if state = fs.State(); state.FirstSeq != 100 {
			t.Fatalf("Expected first seq of 100, got %d", state.FirstSeq)
		}
	})
}

func TestFileStoreCompactLastPlusOne(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 8192
		fcfg.AsyncFlush = true

		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", make([]byte, 10_000)
		for i := 0; i < 10_000; i++ {
			if _, _, err := fs.StoreMsg(subj, nil, msg, 0); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		}

		// The performance of this test is quite terrible with compression
		// if we have AsyncFlush = false, so we'll batch flushes instead.
		fs.mu.Lock()
		fs.checkAndFlushLastBlock()
		fs.mu.Unlock()

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

		fs.StoreMsg(subj, nil, msg, 0)
		state = fs.State()
		if state.Msgs != 1 {
			t.Fatalf("Expected one message but got %d", state.Msgs)
		}
	})
}

func TestFileStoreCompactMsgCountBug(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("Hello World")
		for i := 0; i < 10; i++ {
			fs.StoreMsg(subj, nil, msg, 0)
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
	})
}

func TestFileStoreCompactPerf(t *testing.T) {
	t.SkipNow()

	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 8192
		fcfg.AsyncFlush = true

		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("Hello World")
		for i := 0; i < 100_000; i++ {
			fs.StoreMsg(subj, nil, msg, 0)
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
	})
}

func TestFileStoreStreamTruncate(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 350
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage}
		created := time.Now()

		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		tseq := uint64(50)

		subj, toStore := "foo", uint64(100)
		for i := uint64(1); i < tseq; i++ {
			_, _, err := fs.StoreMsg(subj, nil, []byte("ok"), 0)
			require_NoError(t, err)
		}
		subj = "bar"
		for i := tseq; i <= toStore; i++ {
			_, _, err := fs.StoreMsg(subj, nil, []byte("ok"), 0)
			require_NoError(t, err)
		}

		if state := fs.State(); state.Msgs != toStore {
			t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
		}

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
		state := fs.State()
		if state.Msgs != tseq-2 {
			t.Fatalf("Expected %d msgs, got %d", tseq-2, state.Msgs)
		}
		expected := []uint64{10, 20}
		if !reflect.DeepEqual(state.Deleted, expected) {
			t.Fatalf("Expected deleted to be %+v, got %+v\n", expected, state.Deleted)
		}

		before := state

		// Make sure we can recover same state.
		fs.Stop()

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if state := fs.State(); !reflect.DeepEqual(state, before) {
			t.Fatalf("Expected state of %+v, got %+v", before, state)
		}

		mb := fs.getFirstBlock()
		require_True(t, mb != nil)
		require_NoError(t, mb.loadMsgs())

		// Also make sure we can recover properly with no index.db present.
		// We want to make sure we preserve tombstones from any blocks being deleted.
		fs.Stop()
		os.Remove(filepath.Join(fs.fcfg.StoreDir, msgDir, streamStreamStateFile))

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if state := fs.State(); !reflect.DeepEqual(state, before) {
			t.Fatalf("Expected state of %+v, got %+v without index.db state", before, state)
		}
	})
}

func TestFileStoreRemovePartialRecovery(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
		created := time.Now()

		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("Hello World")
		toStore := 100
		for i := 0; i < toStore; i++ {
			fs.StoreMsg(subj, nil, msg, 0)
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

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		state2 := fs.State()
		if !reflect.DeepEqual(state2, state) {
			t.Fatalf("Expected recovered state to be the same, got %+v vs %+v\n", state2, state)
		}
	})
}

func TestFileStoreRemoveOutOfOrderRecovery(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
		created := time.Now()

		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("Hello World")
		toStore := 100
		for i := 0; i < toStore; i++ {
			fs.StoreMsg(subj, nil, msg, 0)
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

		var smv StoreMsg
		if _, err := fs.LoadMsg(1, &smv); err != nil {
			t.Fatalf("Expected to retrieve seq 1")
		}
		for i := 2; i <= toStore; i += 2 {
			if _, err := fs.LoadMsg(uint64(i), &smv); err == nil {
				t.Fatalf("Expected error looking up seq %d that should be deleted", i)
			}
		}

		// Make sure we recover same state.
		fs.Stop()

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		state2 := fs.State()
		if !reflect.DeepEqual(state2, state) {
			t.Fatalf("Expected recovered states to be the same, got %+v vs %+v\n", state, state2)
		}

		if _, err := fs.LoadMsg(1, &smv); err != nil {
			t.Fatalf("Expected to retrieve seq 1")
		}
		for i := 2; i <= toStore; i += 2 {
			if _, err := fs.LoadMsg(uint64(i), nil); err == nil {
				t.Fatalf("Expected error looking up seq %d that should be deleted", i)
			}
		}
	})
}

func TestFileStoreAgeLimitRecovery(t *testing.T) {
	maxAge := 1 * time.Second

	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.CacheExpire = 1 * time.Millisecond
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage, MaxAge: maxAge}
		created := time.Now()

		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Store some messages. Does not really matter how many.
		subj, msg := "foo", []byte("Hello World")
		toStore := 100
		for i := 0; i < toStore; i++ {
			fs.StoreMsg(subj, nil, msg, 0)
		}
		state := fs.State()
		if state.Msgs != uint64(toStore) {
			t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
		}
		fs.Stop()

		time.Sleep(maxAge)

		fcfg.CacheExpire = 0
		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
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
	})
}

func TestFileStoreBitRot(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Store some messages. Does not really matter how many.
		subj, msg := "foo", []byte("Hello World")
		toStore := 100
		for i := 0; i < toStore; i++ {
			fs.StoreMsg(subj, nil, msg, 0)
		}
		state := fs.State()
		if state.Msgs != uint64(toStore) {
			t.Fatalf("Expected %d msgs, got %d", toStore, state.Msgs)
		}

		if ld := fs.checkMsgs(); ld != nil && len(ld.Msgs) > 0 {
			t.Fatalf("Expected to have no corrupt msgs, got %d", len(ld.Msgs))
		}

		for i := 0; i < 10; i++ {
			// Now twiddle some bits.
			fs.mu.Lock()
			lmb := fs.lmb
			contents, err := os.ReadFile(lmb.mfn)
			require_NoError(t, err)
			require_True(t, len(contents) > 0)

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
			os.WriteFile(lmb.mfn, contents, 0644)
			fs.mu.Unlock()

			ld := fs.checkMsgs()
			if len(ld.Msgs) > 0 {
				break
			}
			// If our bitrot caused us to not be able to recover any messages we can break as well.
			if state := fs.State(); state.Msgs == 0 {
				break
			}
			// Fail the test if we have tried the 10 times and still did not
			// get any corruption report.
			if i == 9 {
				t.Fatalf("Expected to have corrupt msgs got none: changed [%d]", index)
			}
		}

		// Make sure we can restore.
		fs.Stop()

		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// checkMsgs will repair the underlying store, so checkMsgs should be clean now.
		if ld := fs.checkMsgs(); ld != nil {
			// If we have no msgs left this will report the head msgs as lost again.
			if state := fs.State(); state.Msgs > 0 {
				t.Fatalf("Expected no errors restoring checked and fixed filestore, got %+v", ld)
			}
		}
	})
}

func TestFileStoreEraseMsg(t *testing.T) {
	// Just do no encryption, etc.
	fcfg := FileStoreConfig{StoreDir: t.TempDir()}
	fs, err := newFileStore(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	subj, msg := "foo", []byte("Hello World")
	fs.StoreMsg(subj, nil, msg, 0)
	fs.StoreMsg(subj, nil, msg, 0) // To keep block from being deleted.
	var smv StoreMsg
	sm, err := fs.LoadMsg(1, &smv)
	if err != nil {
		t.Fatalf("Unexpected error looking up msg: %v", err)
	}
	if !bytes.Equal(msg, sm.msg) {
		t.Fatalf("Expected same msg, got %q vs %q", sm.msg, msg)
	}
	if removed, _ := fs.EraseMsg(1); !removed {
		t.Fatalf("Expected erase msg to return success")
	}
	if sm2, _ := fs.msgForSeq(1, nil); sm2 != nil {
		t.Fatalf("Expected msg to be erased")
	}
	fs.checkAndFlushLastBlock()

	// Now look on disk as well.
	rl := fileStoreMsgSize(subj, nil, msg)
	buf := make([]byte, rl)
	fp, err := os.Open(filepath.Join(fcfg.StoreDir, msgDir, fmt.Sprintf(blkScan, 1)))
	if err != nil {
		t.Fatalf("Error opening msg block file: %v", err)
	}
	defer fp.Close()

	fp.ReadAt(buf, 0)
	fs.mu.RLock()
	mb := fs.blks[0]
	fs.mu.RUnlock()
	mb.mu.Lock()
	sm, err = mb.msgFromBuf(buf, nil, nil)
	mb.mu.Unlock()
	if err != nil {
		t.Fatalf("error reading message from block: %v", err)
	}
	if sm.subj == subj {
		t.Fatalf("Expected the subjects to be different")
	}
	if sm.seq != 0 && sm.seq&ebit == 0 {
		t.Fatalf("Expected seq to be 0, marking as deleted, got %d", sm.seq)
	}
	if sm.ts != 0 {
		t.Fatalf("Expected timestamp to be 0, got %d", sm.ts)
	}
	if bytes.Equal(sm.msg, msg) {
		t.Fatalf("Expected message body to be randomized")
	}
}

func TestFileStoreEraseAndNoIndexRecovery(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("Hello World")
		toStore := 100
		for i := 0; i < toStore; i++ {
			fs.StoreMsg(subj, nil, msg, 0)
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

		// Stop and remove the optional index file.
		fs.Stop()
		ifn := filepath.Join(fcfg.StoreDir, msgDir, fmt.Sprintf(indexScan, 1))
		os.Remove(ifn)

		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		state = fs.State()
		if state.Msgs != uint64(toStore/2) {
			t.Fatalf("Expected %d msgs, got %d", toStore/2, state.Msgs)
		}

		for i := 2; i <= toStore; i += 2 {
			if _, err := fs.LoadMsg(uint64(i), nil); err == nil {
				t.Fatalf("Expected error looking up seq %d that should be erased", i)
			}
		}
	})
}

func TestFileStoreMeta(t *testing.T) {
	// Just do no encryption, etc.
	fcfg := FileStoreConfig{StoreDir: t.TempDir()}
	mconfig := StreamConfig{Name: "ZZ-22-33", Storage: FileStorage, Subjects: []string{"foo.*"}, Replicas: 22}
	fs, err := newFileStore(fcfg, mconfig)
	require_NoError(t, err)
	defer fs.Stop()

	metafile := filepath.Join(fcfg.StoreDir, JetStreamMetaFile)
	metasum := filepath.Join(fcfg.StoreDir, JetStreamMetaFileSum)

	// Test to make sure meta file and checksum are present.
	if _, err := os.Stat(metafile); os.IsNotExist(err) {
		t.Fatalf("Expected metafile %q to exist", metafile)
	}
	if _, err := os.Stat(metasum); os.IsNotExist(err) {
		t.Fatalf("Expected metafile's checksum %q to exist", metasum)
	}

	buf, err := os.ReadFile(metafile)
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
	checksum, err := os.ReadFile(metasum)
	if err != nil {
		t.Fatalf("Error reading metafile checksum: %v", err)
	}

	fs.mu.Lock()
	fs.hh.Reset()
	fs.hh.Write(buf)
	mychecksum := hex.EncodeToString(fs.hh.Sum(nil))
	fs.mu.Unlock()

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
		t.Fatalf("Unexpected error: %v", err)
	}

	ometafile := filepath.Join(fcfg.StoreDir, consumerDir, oname, JetStreamMetaFile)
	ometasum := filepath.Join(fcfg.StoreDir, consumerDir, oname, JetStreamMetaFileSum)

	// Test to make sure meta file and checksum are present.
	if _, err := os.Stat(ometafile); os.IsNotExist(err) {
		t.Fatalf("Expected consumer metafile %q to exist", ometafile)
	}
	if _, err := os.Stat(ometasum); os.IsNotExist(err) {
		t.Fatalf("Expected consumer metafile's checksum %q to exist", ometasum)
	}

	buf, err = os.ReadFile(ometafile)
	if err != nil {
		t.Fatalf("Error reading consumer metafile: %v", err)
	}

	var oconfig2 ConsumerConfig
	if err := json.Unmarshal(buf, &oconfig2); err != nil {
		t.Fatalf("Error unmarshalling: %v", err)
	}
	// Since we set name we will get that back now.
	oconfig.Name = oname
	if !reflect.DeepEqual(oconfig2, oconfig) {
		t.Fatalf("Consumer configs not equal, got %+v vs %+v", oconfig2, oconfig)
	}
	checksum, err = os.ReadFile(ometasum)

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
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("Hello World!")

		for i := uint64(1); i <= 10; i++ {
			fs.StoreMsg(subj, nil, msg, 0)
			if _, err := fs.LoadMsg(i, nil); err != nil {
				t.Fatalf("Error loading %d: %v", i, err)
			}
		}
	})
}

func TestFileStoreAndRetrieveMultiBlock(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		subj, msg := "foo", []byte("Hello World!")
		storedMsgSize := fileStoreMsgSize(subj, nil, msg)

		fcfg.BlockSize = 4 * storedMsgSize
		created := time.Now()

		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		for i := 0; i < 20; i++ {
			fs.StoreMsg(subj, nil, msg, 0)
		}
		state := fs.State()
		if state.Msgs != 20 {
			t.Fatalf("Expected 20 msgs, got %d", state.Msgs)
		}
		fs.Stop()

		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		var smv StoreMsg
		for i := uint64(1); i <= 20; i++ {
			if _, err := fs.LoadMsg(i, &smv); err != nil {
				t.Fatalf("Error loading %d: %v", i, err)
			}
		}
	})
}

func TestFileStoreCollapseDmap(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		subj, msg := "foo", []byte("Hello World!")
		storedMsgSize := fileStoreMsgSize(subj, nil, msg)

		fcfg.BlockSize = 4 * storedMsgSize
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		for i := 0; i < 10; i++ {
			fs.StoreMsg(subj, nil, msg, 0)
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
	})
}

func TestFileStoreReadCache(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.CacheExpire = 100 * time.Millisecond

		subj, msg := "foo.bar", make([]byte, 1024)
		storedMsgSize := fileStoreMsgSize(subj, nil, msg)

		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		toStore := 500
		totalBytes := uint64(toStore) * storedMsgSize

		for i := 0; i < toStore; i++ {
			fs.StoreMsg(subj, nil, msg, 0)
		}

		// Wait for cache to go to zero.
		checkFor(t, time.Second, 10*time.Millisecond, func() error {
			if csz := fs.cacheSize(); csz != 0 {
				return fmt.Errorf("cache size not 0, got %s", friendlyBytes(int64(csz)))
			}
			return nil
		})

		fs.LoadMsg(1, nil)
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
		fs.LoadMsg(1, nil)
		timeout := time.Now().Add(250 * time.Millisecond)
		for time.Now().Before(timeout) {
			if cls := fs.cacheLoads(); cls != 2 {
				t.Fatalf("cache loads not 2, got %d", cls)
			}
			time.Sleep(5 * time.Millisecond)
			fs.LoadMsg(1, nil) // register activity.
		}
	})
}

func TestFileStorePartialCacheExpiration(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cexp := 10 * time.Millisecond
		fcfg.CacheExpire = cexp

		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		fs.StoreMsg("foo", nil, []byte("msg1"), 0)

		// Should expire and be removed.
		time.Sleep(2 * cexp)
		fs.StoreMsg("bar", nil, []byte("msg2"), 0)

		// Again wait for cache to expire.
		time.Sleep(2 * cexp)
		if _, err := fs.LoadMsg(1, nil); err != nil {
			t.Fatalf("Error loading message 1: %v", err)
		}
	})
}

func TestFileStorePartialIndexes(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cexp := 10 * time.Millisecond
		fcfg.CacheExpire = cexp

		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		toSend := 5
		for i := 0; i < toSend; i++ {
			fs.StoreMsg("foo", nil, []byte("ok-1"), 0)
		}

		// Now wait til the cache expires, including the index.
		fs.mu.Lock()
		mb := fs.blks[0]
		fs.mu.Unlock()

		// Force idx to expire by resetting last remove ts.
		mb.mu.Lock()
		mb.llts = mb.llts - int64(defaultCacheBufferExpiration*2)
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
			fs.StoreMsg("foo", nil, []byte("ok-2"), 0)
		}
		// If we now load in a message in second half if we do not
		// detect idx is a partial correctly this will panic.
		if _, err := fs.LoadMsg(8, nil); err != nil {
			t.Fatalf("Error loading %d: %v", 1, err)
		}
	})
}

func TestFileStoreSnapshot(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		subj, msg := "foo", []byte("Hello Snappy!")
		scfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}

		fs, err := newFileStoreWithCreated(fcfg, scfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		toSend := 2233
		for i := 0; i < toSend; i++ {
			fs.StoreMsg(subj, nil, msg, 0)
		}

		// Create a few consumers.
		o1, err := fs.ConsumerStore("o22", &ConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		o2, err := fs.ConsumerStore("o33", &ConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		state := &ConsumerState{}
		state.Delivered.Consumer = 100
		state.Delivered.Stream = 100
		state.AckFloor.Consumer = 22
		state.AckFloor.Stream = 22

		if err := o1.Update(state); err != nil {
			t.Fatalf("Unexpected error updating state: %v", err)
		}
		state.AckFloor.Consumer = 33
		state.AckFloor.Stream = 33

		if err := o2.Update(state); err != nil {
			t.Fatalf("Unexpected error updating state: %v", err)
		}

		snapshot := func() []byte {
			t.Helper()
			r, err := fs.Snapshot(5*time.Second, true, true)
			if err != nil {
				t.Fatalf("Error creating snapshot")
			}
			snapshot, err := io.ReadAll(r.Reader)
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

			rstoreDir := t.TempDir()

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

			fcfg.StoreDir = rstoreDir
			fsr, err := newFileStoreWithCreated(fcfg, scfg, time.Now(), prf(&fcfg), nil)
			require_NoError(t, err)
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
	})
}

func TestFileStoreConsumer(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		o, err := fs.ConsumerStore("obs22", &ConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if state, err := o.State(); err != nil || state.Delivered.Consumer != 0 {
			t.Fatalf("Unexpected state or error: %v", err)
		}

		state := &ConsumerState{}

		updateAndCheck := func() {
			t.Helper()
			if err := o.Update(state); err != nil {
				t.Fatalf("Unexpected error updating state: %v", err)
			}
			s2, err := o.State()
			if err != nil {
				t.Fatalf("Unexpected error getting state: %v", err)
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
	})
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
	// docker run -ti --tmpfs /jswf_test:rw,size=32k --rm -v ~/Development/go/src:/go/src -w /go/src/github.com/nats-io/nats-server/ golang:1.21 /bin/bash
	tdir := filepath.Join("/", "jswf_test")
	if stat, err := os.Stat(tdir); err != nil || !stat.IsDir() {
		t.SkipNow()
	}

	storeDir := filepath.Join(tdir, JetStreamStoreDir)
	os.MkdirAll(storeDir, 0755)

	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.StoreDir = storeDir
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
		created := time.Now()

		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("Hello Write Failures!")

		var lseq uint64
		// msz about will be ~54 bytes, so if limit is 32k trying to send 1000 will fail at some point.
		for i := 1; i <= 1000; i++ {
			if _, _, err := fs.StoreMsg(subj, nil, msg, 0); err != nil {
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
		if _, err := fs.LoadMsg(lseq, nil); err == nil {
			t.Fatalf("Expected error loading seq that failed, got none")
		}
		// Loading should still work.
		if _, err := fs.LoadMsg(1, nil); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		// Make sure we recover same state.
		fs.Stop()

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		state2 := fs.State()

		// Ignore lost state.
		state.Lost, state2.Lost = nil, nil
		if !reflect.DeepEqual(state2, state) {
			t.Fatalf("Expected recovered state to be the same\n%+v\nvs\n%+v\n", state2, state)
		}

		// We should still fail here.
		for i := 1; i <= 100; i++ {
			_, _, err = fs.StoreMsg(subj, nil, msg, 0)
			if err != nil {
				break
			}
		}
		require_Error(t, err)

		lseq = fs.State().LastSeq + 1

		// Purge should help.
		if _, err := fs.Purge(); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		// Wait for purge to complete its out of band processing.
		time.Sleep(50 * time.Millisecond)

		// Check we will fail again in same spot.
		for i := 1; i <= 1000; i++ {
			if _, _, err := fs.StoreMsg(subj, nil, msg, 0); err != nil {
				if i != int(lseq) {
					t.Fatalf("Expected to fail after purge about the same spot, wanted %d got %d", lseq, i)
				}
				break
			}
		}
	})
}

func TestFileStorePerf(t *testing.T) {
	// Comment out to run, holding place for now.
	t.SkipNow()

	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.AsyncFlush = true

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

		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		start := time.Now()
		for i := 0; i < int(toStore); i++ {
			fs.StoreMsg(subj, nil, msg, 0)
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
		fcfg.AsyncFlush = false
		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		fmt.Printf("time to restore is %v\n\n", time.Since(start))

		fmt.Printf("LOAD: reading %d msgs of %s each, totalling %s\n",
			toStore,
			friendlyBytes(int64(storedMsgSize)),
			friendlyBytes(int64(toStore*storedMsgSize)),
		)

		var smv StoreMsg
		start = time.Now()
		for i := uint64(1); i <= toStore; i++ {
			if _, err := fs.LoadMsg(i, &smv); err != nil {
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
			if _, err := fs.LoadMsg(i, &smv); err != nil {
				t.Fatalf("Error loading %d: %v", i, err)
			}
		}

		tt = time.Since(start)
		fmt.Printf("time to read all back messages is %v\n", tt)
		fmt.Printf("%.0f msgs/sec\n", float64(toStore)/tt.Seconds())
		fmt.Printf("%s per sec\n", friendlyBytes(int64(float64(toStore*storedMsgSize)/tt.Seconds())))

		fs.Stop()

		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

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

		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		state := fs.State()
		if state.Msgs != 0 {
			t.Fatalf("Expected no msgs, got %d", state.Msgs)
		}
		if state.Bytes != 0 {
			t.Fatalf("Expected no bytes, got %d", state.Bytes)
		}
	})
}

func TestFileStoreReadBackMsgPerf(t *testing.T) {
	// Comment out to run, holding place for now.
	t.SkipNow()

	subj := "foo"
	msg := []byte("ABCDEFGH") // Smaller shows problems more.

	storedMsgSize := fileStoreMsgSize(subj, nil, msg)

	// Make sure we store 2 blocks.
	toStore := defaultLargeBlockSize * 2 / storedMsgSize

	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fmt.Printf("storing %d msgs of %s each, totalling %s\n",
			toStore,
			friendlyBytes(int64(storedMsgSize)),
			friendlyBytes(int64(toStore*storedMsgSize)),
		)

		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		start := time.Now()
		for i := 0; i < int(toStore); i++ {
			fs.StoreMsg(subj, nil, msg, 0)
		}

		tt := time.Since(start)
		fmt.Printf("time to store is %v\n", tt)
		fmt.Printf("%.0f msgs/sec\n", float64(toStore)/tt.Seconds())
		fmt.Printf("%s per sec\n", friendlyBytes(int64(float64(toStore*storedMsgSize)/tt.Seconds())))

		// We should not have cached here with no reads.
		// Pick something towards end of the block.
		index := defaultLargeBlockSize/storedMsgSize - 22
		start = time.Now()
		fs.LoadMsg(index, nil)
		fmt.Printf("Time to load first msg [%d] = %v\n", index, time.Since(start))

		start = time.Now()
		fs.LoadMsg(index+2, nil)
		fmt.Printf("Time to load second msg [%d] = %v\n", index+2, time.Since(start))
	})
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

	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		fs.RegisterStorageUpdates(func(md, bd int64, seq uint64, subj string) {})

		fmt.Printf("storing and removing (limit 1) %d msgs of %s each, totalling %s\n",
			toStore,
			friendlyBytes(int64(storedMsgSize)),
			friendlyBytes(int64(toStore*storedMsgSize)),
		)

		start := time.Now()
		for i := 0; i < int(toStore); i++ {
			seq, _, err := fs.StoreMsg(subj, nil, msg, 0)
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
	})
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
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fmt.Printf("storing %d msgs of %s each, totalling %s\n",
			toStore,
			friendlyBytes(int64(storedMsgSize)),
			friendlyBytes(int64(toStore*storedMsgSize)),
		)

		fcfg.BlockSize = FileStoreMinBlkSize

		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		start := time.Now()
		for i := 0; i < int(toStore); i++ {
			fs.StoreMsg(subj, nil, msg, 0)
		}
		fs.Stop()

		tt := time.Since(start)
		fmt.Printf("time to store is %v\n", tt)
		fmt.Printf("%.0f msgs/sec\n", float64(toStore)/tt.Seconds())
		fmt.Printf("%s per sec\n", friendlyBytes(int64(float64(toStore*storedMsgSize)/tt.Seconds())))
	})
}

// Saw this manifest from a restart test with max delivered set for JetStream.
func TestFileStoreConsumerRedeliveredLost(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		cfg := &ConsumerConfig{AckPolicy: AckExplicit}
		o, err := fs.ConsumerStore("o22", cfg)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		restartConsumer := func() {
			t.Helper()
			o.Stop()
			time.Sleep(20 * time.Millisecond) // Wait for all things to settle.
			o, err = fs.ConsumerStore("o22", cfg)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			// Make sure we recovered Redelivered.
			state, err := o.State()
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
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
			t.Fatalf("Did not clear redelivered correctly")
		}
	})
}

func TestFileStoreConsumerFlusher(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		o, err := fs.ConsumerStore("o22", &ConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
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
	})
}

func TestFileStoreConsumerDeliveredUpdates(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Simple consumer, no ack policy configured.
		o, err := fs.ConsumerStore("o22", &ConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
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
	})
}

func TestFileStoreConsumerDeliveredAndAckUpdates(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Simple consumer, no ack policy configured.
		o, err := fs.ConsumerStore("o22", &ConsumerConfig{AckPolicy: AckExplicit})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
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

		testAck(1, 100, 1, 109)
		testAck(3, 130, 1, 109)
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
			t.Fatalf("Unexpected error: %v", err)
		}
		defer o.Stop()

		nstate, err := o.State()
		if err != nil {
			t.Fatalf("Unexpected error getting state: %v", err)
		}
		if !reflect.DeepEqual(nstate, state) {
			t.Fatalf("States don't match! NEW %+v OLD %+v", nstate, state)
		}
	})
}

func TestFileStoreStreamStateDeleted(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, toStore := "foo", uint64(10)
		for i := uint64(1); i <= toStore; i++ {
			msg := []byte(fmt.Sprintf("[%08d] Hello World!", i))
			if _, _, err := fs.StoreMsg(subj, nil, msg, 0); err != nil {
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
	})
}

// We have reports that sometimes under load a stream could complain about a storage directory
// not being empty.
func TestFileStoreStreamDeleteDirNotEmpty(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, toStore := "foo", uint64(10)
		for i := uint64(1); i <= toStore; i++ {
			msg := []byte(fmt.Sprintf("[%08d] Hello World!", i))
			if _, _, err := fs.StoreMsg(subj, nil, msg, 0); err != nil {
				t.Fatalf("Error storing msg: %v", err)
			}
		}

		ready := make(chan bool)
		go func() {
			g := filepath.Join(fcfg.StoreDir, "g")
			ready <- true
			for i := 0; i < 100; i++ {
				os.WriteFile(g, []byte("OK"), defaultFilePerms)
			}
		}()

		<-ready
		if err := fs.Delete(true); err != nil {
			t.Fatalf("Delete returned an error: %v", err)
		}
	})
}

func TestFileStoreConsumerPerf(t *testing.T) {
	// Comment out to run, holding place for now.
	t.SkipNow()

	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		o, err := fs.ConsumerStore("o22", &ConsumerConfig{AckPolicy: AckExplicit})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
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
	})
}

// Reported by Ivan.
func TestFileStoreStreamDeleteCacheBug(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.CacheExpire = 50 * time.Millisecond

		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("Hello World")

		if _, _, err := fs.StoreMsg(subj, nil, msg, 0); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if _, _, err := fs.StoreMsg(subj, nil, msg, 0); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if _, err := fs.EraseMsg(1); err != nil {
			t.Fatalf("Got an error on remove of %d: %v", 1, err)
		}
		time.Sleep(100 * time.Millisecond)
		if _, err := fs.LoadMsg(2, nil); err != nil {
			t.Fatalf("Unexpected error looking up msg: %v", err)
		}
	})
}

// rip
func TestFileStoreStreamFailToRollBug(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 512

		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage, MaxBytes: 300}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Make sure we properly roll underlying blocks.
		n, msg := 200, bytes.Repeat([]byte("ABC"), 33) // ~100bytes
		for i := 0; i < n; i++ {
			if _, _, err := fs.StoreMsg("zzz", nil, msg, 0); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		}

		// Grab some info for introspection.
		fs.mu.RLock()
		numBlks := len(fs.blks)
		var index uint32
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
	})
}

// We had a case where a consumer state had a redelivered record that had seq of 0.
// This was causing the server to panic.
func TestFileStoreBadConsumerState(t *testing.T) {
	bs := []byte("\x16\x02\x01\x01\x03\x02\x01\x98\xf4\x8a\x8a\f\x01\x03\x86\xfa\n\x01\x00\x01")
	if cs, err := decodeConsumerState(bs); err != nil || cs == nil {
		t.Fatalf("Expected to not throw error, got %v and %+v", err, cs)
	}
}

func TestFileStoreExpireMsgsOnStart(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 8 * 1024
		ttl := 250 * time.Millisecond
		cfg := StreamConfig{Name: "ORDERS", Subjects: []string{"orders.*"}, Storage: FileStorage, MaxAge: ttl}
		var fs *fileStore

		startFS := func() *fileStore {
			t.Helper()
			fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
			require_NoError(t, err)
			return fs
		}

		newFS := func() *fileStore {
			t.Helper()
			if fs != nil {
				fs.Stop()
				fs = nil
			}
			removeDir(t, fcfg.StoreDir)
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
				if _, _, err := fs.StoreMsg(fmt.Sprintf("orders.%d", i%10), nil, msg, 0); err != nil {
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
			t.Helper()
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
			fs.mu.RUnlock()
		}

		lastSeqForBlk := func() uint64 {
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
		// We actually hold onto the last one now to remember our starting sequence.
		checkNumBlks(1)

		// Now check partial expires and the fss tracking state.
		// Small numbers is to keep them in one block.
		fs = newFS()
		loadMsgs(10)
		time.Sleep(100 * time.Millisecond)
		loadMsgs(10)
		checkFiltered("orders.*", SimpleState{Msgs: 20, First: 1, Last: 20})

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
		lseq := lastSeqForBlk()
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
	})
}

func TestFileStoreSparseCompaction(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 1024 * 1024
		cfg := StreamConfig{Name: "KV", Subjects: []string{"kv.>"}, Storage: FileStorage}

		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		msg := bytes.Repeat([]byte("ABC"), 33) // ~100bytes
		loadMsgs := func(n int) {
			t.Helper()
			for i := 1; i <= n; i++ {
				if _, _, err := fs.StoreMsg(fmt.Sprintf("kv.%d", i%10), nil, msg, 0); err != nil {
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
	})
}

func TestFileStoreSparseCompactionWithInteriorDeletes(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cfg := StreamConfig{Name: "KV", Subjects: []string{"kv.>"}, Storage: FileStorage}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		for i := 1; i <= 1000; i++ {
			if _, _, err := fs.StoreMsg(fmt.Sprintf("kv.%d", i%10), nil, []byte("OK"), 0); err != nil {
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

		_, err = fs.LoadMsg(900, nil)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		// Do compact by hand, make sure we can still access msgs past the interior deletes.
		fs.mu.RLock()
		lmb := fs.lmb
		lmb.dirtyCloseWithRemove(false)
		lmb.compact()
		fs.mu.RUnlock()

		if _, err = fs.LoadMsg(900, nil); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	})
}

// When messages span multiple blocks and we want to purge but keep some amount, say 1, we would remove all.
// This is because we would not break out of iterator across more message blocks.
// Issue #2622
func TestFileStorePurgeExKeepOneBug(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 128
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		fill := bytes.Repeat([]byte("X"), 128)

		fs.StoreMsg("A", nil, []byte("META"), 0)
		fs.StoreMsg("B", nil, fill, 0)
		fs.StoreMsg("A", nil, []byte("META"), 0)
		fs.StoreMsg("B", nil, fill, 0)

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
	})
}

func TestFileStoreFilteredPendingBug(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		fs.StoreMsg("foo", nil, []byte("msg"), 0)
		fs.StoreMsg("bar", nil, []byte("msg"), 0)
		fs.StoreMsg("baz", nil, []byte("msg"), 0)

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
	})
}

// Test to optimize the selectMsgBlock with lots of blocks.
func TestFileStoreFetchPerf(t *testing.T) {
	// Comment out to run.
	t.SkipNow()

	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 8192
		fcfg.AsyncFlush = true
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Will create 25k msg blocks.
		n, subj, msg := 100_000, "zzz", bytes.Repeat([]byte("ABC"), 600)
		for i := 0; i < n; i++ {
			if _, _, err := fs.StoreMsg(subj, nil, msg, 0); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		}

		// Time how long it takes us to load all messages.
		var smv StoreMsg
		now := time.Now()
		for i := 0; i < n; i++ {
			_, err := fs.LoadMsg(uint64(i), &smv)
			if err != nil {
				t.Fatalf("Unexpected error looking up seq %d: %v", i, err)
			}
		}
		fmt.Printf("Elapsed to load all messages is %v\n", time.Since(now))
	})
}

// For things like raft log when we compact and have a message block that could reclaim > 50% of space for block we want to do that.
// https://github.com/nats-io/nats-server/issues/2936
func TestFileStoreCompactReclaimHeadSpace(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 4 * 1024 * 1024
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Create random bytes for payload to test for corruption vs repeated.
		msg := make([]byte, 64*1024)
		crand.Read(msg)

		// This gives us ~63 msgs in first and ~37 in second.
		n, subj := 100, "z"
		for i := 0; i < n; i++ {
			_, _, err := fs.StoreMsg(subj, nil, msg, 0)
			require_NoError(t, err)
		}

		checkNumBlocks := func(n int) {
			t.Helper()
			fs.mu.RLock()
			defer fs.mu.RUnlock()
			if len(fs.blks) != n {
				t.Fatalf("Expected to have %d blocks, got %d", n, len(fs.blks))
			}
		}

		getBlock := func(index int) *msgBlock {
			t.Helper()
			fs.mu.RLock()
			defer fs.mu.RUnlock()
			return fs.blks[index]
		}

		// Check that we did right thing and actually reclaimed since > 50%
		checkBlock := func(mb *msgBlock) {
			t.Helper()

			mb.mu.RLock()
			rbytes, mfn := mb.rbytes, mb.mfn
			fseq, lseq := mb.first.seq, mb.last.seq
			mb.mu.RUnlock()

			// Check that sizes match as long as we are not doing compression.
			if fcfg.Compression == NoCompression {
				file, err := os.Open(mfn)
				require_NoError(t, err)
				defer file.Close()
				fi, err := file.Stat()
				require_NoError(t, err)
				if rbytes != uint64(fi.Size()) {
					t.Fatalf("Expected to rbytes == fi.Size, got %d vs %d", rbytes, fi.Size())
				}
			}

			// Make sure we can pull messages and that they are ok.
			var smv StoreMsg
			sm, err := fs.LoadMsg(fseq, &smv)
			require_NoError(t, err)
			if !bytes.Equal(sm.msg, msg) {
				t.Fatalf("Msgs don't match, original %q vs %q", msg, sm.msg)
			}
			sm, err = fs.LoadMsg(lseq, &smv)
			require_NoError(t, err)
			if !bytes.Equal(sm.msg, msg) {
				t.Fatalf("Msgs don't match, original %q vs %q", msg, sm.msg)
			}
		}

		checkNumBlocks(2)
		_, err = fs.Compact(33)
		require_NoError(t, err)

		checkNumBlocks(2)
		checkBlock(getBlock(0))
		checkBlock(getBlock(1))

		_, err = fs.Compact(85)
		require_NoError(t, err)

		checkNumBlocks(1)
		checkBlock(getBlock(0))

		// Make sure we can write.
		_, _, err = fs.StoreMsg(subj, nil, msg, 0)
		require_NoError(t, err)

		checkNumBlocks(1)
		checkBlock(getBlock(0))

		// Stop and start again.
		fs.Stop()

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		checkNumBlocks(1)
		checkBlock(getBlock(0))

		// Make sure we can write.
		_, _, err = fs.StoreMsg(subj, nil, msg, 0)
		require_NoError(t, err)
	})
}

func TestFileStoreRememberLastMsgTime(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		var fs *fileStore
		cfg := StreamConfig{Name: "TEST", Storage: FileStorage, MaxAge: 1 * time.Second}

		getFS := func() *fileStore {
			t.Helper()
			fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
			require_NoError(t, err)
			return fs
		}
		restartFS := func() {
			t.Helper()
			fs.Stop()
			fs = getFS()
		}

		msg := bytes.Repeat([]byte("X"), 2*1024*1024)

		// Get first one.
		fs = getFS()
		defer fs.Stop()

		seq, ts, err := fs.StoreMsg("foo", nil, msg, 0)
		require_NoError(t, err)
		// We will test that last msg time survives from delete, purge and expires after restart.
		removed, err := fs.RemoveMsg(seq)
		require_NoError(t, err)
		require_True(t, removed)

		lt := time.Unix(0, ts).UTC()
		require_True(t, lt == fs.State().LastTime)

		// Restart
		restartFS()

		// Test that last time survived.
		require_True(t, lt == fs.State().LastTime)

		seq, ts, err = fs.StoreMsg("foo", nil, msg, 0)
		require_NoError(t, err)

		var smv StoreMsg
		_, err = fs.LoadMsg(seq, &smv)
		require_NoError(t, err)

		fs.Purge()

		// Restart
		restartFS()

		lt = time.Unix(0, ts).UTC()
		require_True(t, lt == fs.State().LastTime)

		_, _, err = fs.StoreMsg("foo", nil, msg, 0)
		require_NoError(t, err)
		seq, ts, err = fs.StoreMsg("foo", nil, msg, 0)
		require_NoError(t, err)

		require_True(t, seq == 4)

		// Wait til messages expire.
		checkFor(t, 5*time.Second, time.Second, func() error {
			state := fs.State()
			if state.Msgs == 0 {
				return nil
			}
			return fmt.Errorf("Still has %d msgs", state.Msgs)
		})

		// Restart
		restartFS()

		lt = time.Unix(0, ts).UTC()
		require_True(t, lt == fs.State().LastTime)

		// Now make sure we retain the true last seq.
		_, _, err = fs.StoreMsg("foo", nil, msg, 0)
		require_NoError(t, err)
		seq, ts, err = fs.StoreMsg("foo", nil, msg, 0)
		require_NoError(t, err)

		require_True(t, seq == 6)
		removed, err = fs.RemoveMsg(seq)
		require_NoError(t, err)
		require_True(t, removed)

		removed, err = fs.RemoveMsg(seq - 1)
		require_NoError(t, err)
		require_True(t, removed)

		// Restart
		restartFS()

		lt = time.Unix(0, ts).UTC()
		require_True(t, lt == fs.State().LastTime)
		require_True(t, seq == 6)
	})
}

func (fs *fileStore) getFirstBlock() *msgBlock {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	if len(fs.blks) == 0 {
		return nil
	}
	return fs.blks[0]
}

func TestFileStoreRebuildStateDmapAccountingBug(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 1024 * 1024

		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		for i := 0; i < 100; i++ {
			_, _, err = fs.StoreMsg("foo", nil, nil, 0)
			require_NoError(t, err)
		}
		// Delete 2-40.
		for i := 2; i <= 40; i++ {
			_, err := fs.RemoveMsg(uint64(i))
			require_NoError(t, err)
		}

		mb := fs.getFirstBlock()
		require_True(t, mb != nil)

		check := func() {
			t.Helper()
			mb.mu.RLock()
			defer mb.mu.RUnlock()
			dmapLen := uint64(mb.dmap.Size())
			if mb.msgs != (mb.last.seq-mb.first.seq+1)-dmapLen {
				t.Fatalf("Consistency check failed: %d != %d -> last %d first %d len(dmap) %d",
					mb.msgs, (mb.last.seq-mb.first.seq+1)-dmapLen, mb.last.seq, mb.first.seq, dmapLen)
			}
		}

		check()

		mb.mu.Lock()
		mb.compact()
		mb.mu.Unlock()

		// Now delete first.
		_, err = fs.RemoveMsg(1)
		require_NoError(t, err)

		mb.mu.Lock()
		_, _, err = mb.rebuildStateLocked()
		mb.mu.Unlock()
		require_NoError(t, err)

		check()
	})
}

func TestFileStorePurgeExWithSubject(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 1000
		cfg := StreamConfig{Name: "TEST", Subjects: []string{"foo.>"}, Storage: FileStorage}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		payload := make([]byte, 20)

		_, _, err = fs.StoreMsg("foo.0", nil, payload, 0)
		require_NoError(t, err)

		total := 200
		for i := 0; i < total; i++ {
			_, _, err = fs.StoreMsg("foo.1", nil, payload, 0)
			require_NoError(t, err)
		}
		_, _, err = fs.StoreMsg("foo.2", nil, []byte("xxxxxx"), 0)
		require_NoError(t, err)

		// Make sure we have our state file prior to Purge call.
		fs.forceWriteFullState()

		// Capture the current index.db file.
		sfile := filepath.Join(fcfg.StoreDir, msgDir, streamStreamStateFile)
		buf, err := os.ReadFile(sfile)
		require_NoError(t, err)
		require_True(t, len(buf) > 0)

		// This should purge all "foo.1"
		p, err := fs.PurgeEx("foo.1", 1, 0)
		require_NoError(t, err)
		require_Equal(t, p, uint64(total))

		state := fs.State()
		require_Equal(t, state.Msgs, 2)
		require_Equal(t, state.FirstSeq, 1)

		// Make sure we can recover same state.
		fs.Stop()
		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		before := state
		if state := fs.State(); !reflect.DeepEqual(state, before) {
			t.Fatalf("Expected state of %+v, got %+v", before, state)
		}

		// Also make sure we can recover properly with no index.db present.
		// We want to make sure we preserve any tombstones from the subject based purge.
		fs.Stop()
		os.Remove(filepath.Join(fs.fcfg.StoreDir, msgDir, streamStreamStateFile))

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if state := fs.State(); !reflect.DeepEqual(state, before) {
			t.Fatalf("Expected state of %+v, got %+v without index.db state", before, state)
		}

		// If we had an index.db from after PurgeEx but before Stop() would rewrite, make sure we
		// properly can recover with the old index file. This would be a crash after the PurgeEx() call.
		fs.Stop()
		err = os.WriteFile(sfile, buf, defaultFilePerms)
		require_NoError(t, err)

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if state := fs.State(); !reflect.DeepEqual(state, before) {
			t.Fatalf("Expected state of %+v, got %+v with old index.db state", before, state)
		}
	})
}

func TestFileStorePurgeExNoTombsOnBlockRemoval(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 1000
		cfg := StreamConfig{Name: "TEST", Subjects: []string{"foo.>"}, Storage: FileStorage}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		payload := make([]byte, 20)

		total := 100
		for i := 0; i < total; i++ {
			_, _, err = fs.StoreMsg("foo.1", nil, payload, 0)
			require_NoError(t, err)
		}
		_, _, err = fs.StoreMsg("foo.2", nil, payload, 0)
		require_NoError(t, err)

		require_Equal(t, fs.numMsgBlocks(), 6)

		// Make sure we have our state file prior to Purge call.
		fs.forceWriteFullState()

		// Capture the current index.db file if it exists.
		sfile := filepath.Join(fcfg.StoreDir, msgDir, streamStreamStateFile)
		buf, err := os.ReadFile(sfile)
		require_NoError(t, err)
		require_True(t, len(buf) > 0)

		// This should purge all "foo.1". This will remove the blocks so we want to make sure
		// we do not write excessive tombstones here.
		p, err := fs.PurgeEx("foo.1", 1, 0)
		require_NoError(t, err)
		require_Equal(t, p, uint64(total))

		state := fs.State()
		require_Equal(t, state.Msgs, 1)
		require_Equal(t, state.FirstSeq, 101)

		// Check that we only have 1 msg block.
		require_Equal(t, fs.numMsgBlocks(), 1)

		// Put the old index.db back. We want to make sure without the empty block tombstones that we
		// properly recover state.
		fs.Stop()
		err = os.WriteFile(sfile, buf, defaultFilePerms)
		require_NoError(t, err)

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		state = fs.State()
		require_Equal(t, state.Msgs, 1)
		require_Equal(t, state.FirstSeq, 101)
	})
}

// When the N.idx file is shorter than the previous write we could fail to recover the idx properly.
// For instance, with encryption and an expiring stream that has no messages, when a restart happens the decrypt will fail
// since their are extra bytes, and this could lead to a stream sequence reset to zero.
//
// NOTE: We do not use idx files anymore, but keeping test.
func TestFileStoreShortIndexWriteBug(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		// Encrypted mode shows, but could effect non-encrypted mode.
		cfg := StreamConfig{Name: "TEST", Storage: FileStorage, MaxAge: time.Second}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		for i := 0; i < 100; i++ {
			_, _, err = fs.StoreMsg("foo", nil, nil, 0)
			require_NoError(t, err)
		}
		// Wait til messages all go away.
		checkFor(t, 5*time.Second, 200*time.Millisecond, func() error {
			if state := fs.State(); state.Msgs != 0 {
				return fmt.Errorf("Expected no msgs, got %d", state.Msgs)
			}
			return nil
		})

		if state := fs.State(); state.FirstSeq != 101 {
			t.Fatalf("Expected first sequence of 101 vs %d", state.FirstSeq)
		}

		// Now restart..
		fs.Stop()
		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if state := fs.State(); state.FirstSeq != 101 || state.LastSeq != 100 {
			t.Fatalf("Expected first sequence of 101 vs %d", state.FirstSeq)
		}
	})
}

func TestFileStoreDoubleCompactWithWriteInBetweenEncryptedBug(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("ouch")
		for i := 0; i < 5; i++ {
			fs.StoreMsg(subj, nil, msg, 0)
		}
		_, err = fs.Compact(5)
		require_NoError(t, err)

		if state := fs.State(); state.LastSeq != 5 {
			t.Fatalf("Expected last sequence to be 5 but got %d", state.LastSeq)
		}
		for i := 0; i < 5; i++ {
			fs.StoreMsg(subj, nil, msg, 0)
		}
		_, err = fs.Compact(10)
		require_NoError(t, err)

		if state := fs.State(); state.LastSeq != 10 {
			t.Fatalf("Expected last sequence to be 10 but got %d", state.LastSeq)
		}
	})
}

// When we kept the empty block for tracking sequence, we needed to reset the bek
// counter when encrypted for subsequent writes to be correct. The bek in place could
// possibly still have a non-zero counter from previous writes.
// Happens when all messages expire and the are flushed and then subsequent writes occur.
func TestFileStoreEncryptedKeepIndexNeedBekResetBug(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		ttl := 1 * time.Second
		cfg := StreamConfig{Name: "zzz", Storage: FileStorage, MaxAge: ttl}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("ouch")
		for i := 0; i < 5; i++ {
			fs.StoreMsg(subj, nil, msg, 0)
		}

		// Want to go to 0.
		// This will leave the marker.
		checkFor(t, 5*time.Second, ttl, func() error {
			if state := fs.State(); state.Msgs != 0 {
				return fmt.Errorf("Expected no msgs, got %d", state.Msgs)
			}
			return nil
		})

		// Now write additional messages.
		for i := 0; i < 5; i++ {
			fs.StoreMsg(subj, nil, msg, 0)
		}

		// Make sure the buffer is cleared.
		fs.mu.RLock()
		mb := fs.lmb
		fs.mu.RUnlock()
		mb.mu.Lock()
		mb.clearCacheAndOffset()
		mb.mu.Unlock()

		// Now make sure we can read.
		var smv StoreMsg
		_, err = fs.LoadMsg(10, &smv)
		require_NoError(t, err)
	})
}

func (fs *fileStore) reportMeta() (hasPSIM, hasAnyFSS bool) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	hasPSIM = fs.psim != nil
	for _, mb := range fs.blks {
		mb.mu.RLock()
		hasAnyFSS = hasAnyFSS || mb.fss != nil
		mb.mu.RUnlock()
		if hasAnyFSS {
			break
		}
	}
	return hasPSIM, hasAnyFSS
}

func TestFileStoreExpireSubjectMeta(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 1024
		fcfg.CacheExpire = 500 * time.Millisecond
		fcfg.SubjectStateExpire = time.Second
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"kv.>"}, Storage: FileStorage, MaxMsgsPer: 1}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		ns := 100
		for i := 1; i <= ns; i++ {
			subj := fmt.Sprintf("kv.%d", i)
			_, _, err := fs.StoreMsg(subj, nil, []byte("value"), 0)
			require_NoError(t, err)
		}

		// Test that on restart we do not have extensize metadata but do have correct number of subjects/keys.
		// Only thing really needed for store state / stream info.
		fs.Stop()
		fs, err = newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		var ss StreamState
		fs.FastState(&ss)
		if ss.NumSubjects != ns {
			t.Fatalf("Expected NumSubjects of %d, got %d", ns, ss.NumSubjects)
		}

		// Make sure we clear mb fss meta
		checkFor(t, fcfg.SubjectStateExpire*2, 500*time.Millisecond, func() error {
			if _, hasAnyFSS := fs.reportMeta(); hasAnyFSS {
				return fmt.Errorf("Still have mb fss state")
			}
			return nil
		})

		// LoadLast, which is what KV uses, should load meta and succeed.
		_, err = fs.LoadLastMsg("kv.22", nil)
		require_NoError(t, err)
		// Make sure we clear mb fss meta
		checkFor(t, fcfg.SubjectStateExpire*2, 500*time.Millisecond, func() error {
			if _, hasAnyFSS := fs.reportMeta(); hasAnyFSS {
				return fmt.Errorf("Still have mb fss state")
			}
			return nil
		})
	})
}

func TestFileStoreMaxMsgsPerSubject(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 128
		fcfg.CacheExpire = time.Second
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"kv.>"}, Storage: FileStorage, MaxMsgsPer: 1}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		ns := 100
		for i := 1; i <= ns; i++ {
			subj := fmt.Sprintf("kv.%d", i)
			_, _, err := fs.StoreMsg(subj, nil, []byte("value"), 0)
			require_NoError(t, err)
		}

		for i := 1; i <= ns; i++ {
			subj := fmt.Sprintf("kv.%d", i)
			_, _, err := fs.StoreMsg(subj, nil, []byte("value"), 0)
			require_NoError(t, err)
		}

		if state := fs.State(); state.Msgs != 100 || state.FirstSeq != 101 || state.LastSeq != 200 || len(state.Deleted) != 0 {
			t.Fatalf("Bad state: %+v", state)
		}

		if nb := fs.numMsgBlocks(); nb != 34 {
			t.Fatalf("Expected 34 blocks, got %d", nb)
		}
	})
}

// Testing the case in https://github.com/nats-io/nats-server/issues/4247
func TestFileStoreMaxMsgsAndMaxMsgsPerSubject(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 128
		fcfg.CacheExpire = time.Second
		cfg := StreamConfig{
			Name:     "zzz",
			Subjects: []string{"kv.>"},
			Storage:  FileStorage,
			Discard:  DiscardNew, MaxMsgs: 100, // Total stream policy
			DiscardNewPer: true, MaxMsgsPer: 1, // Per-subject policy
		}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		for i := 1; i <= 101; i++ {
			subj := fmt.Sprintf("kv.%d", i)
			_, _, err := fs.StoreMsg(subj, nil, []byte("value"), 0)
			if i == 101 {
				// The 101th iteration should fail because MaxMsgs is set to
				// 100 and the policy is DiscardNew.
				require_Error(t, err)
			} else {
				require_NoError(t, err)
			}
		}

		for i := 1; i <= 100; i++ {
			subj := fmt.Sprintf("kv.%d", i)
			_, _, err := fs.StoreMsg(subj, nil, []byte("value"), 0)
			// All of these iterations should fail because MaxMsgsPer is set
			// to 1 and DiscardNewPer is set to true, forcing us to reject
			// cases where there is already a message on this subject.
			require_Error(t, err)
		}

		if state := fs.State(); state.Msgs != 100 || state.FirstSeq != 1 || state.LastSeq != 100 || len(state.Deleted) != 0 {
			// There should be 100 messages exactly, as the 101st subject
			// should have been rejected in the first loop, and any duplicates
			// on the other subjects should have been rejected in the second loop.
			t.Fatalf("Bad state: %+v", state)
		}
	})
}

func TestFileStoreSubjectStateCacheExpiration(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 32
		fcfg.CacheExpire = time.Second
		fcfg.SubjectStateExpire = time.Second
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"kv.>"}, Storage: FileStorage, MaxMsgsPer: 2}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		for i := 1; i <= 100; i++ {
			subj := fmt.Sprintf("kv.foo.%d", i)
			_, _, err := fs.StoreMsg(subj, nil, []byte("value"), 0)
			require_NoError(t, err)
		}
		for i := 1; i <= 100; i++ {
			subj := fmt.Sprintf("kv.bar.%d", i)
			_, _, err := fs.StoreMsg(subj, nil, []byte("value"), 0)
			require_NoError(t, err)
		}

		// Make sure we clear mb fss meta before asking for SubjectState.
		checkFor(t, 10*time.Second, 500*time.Millisecond, func() error {
			if _, hasAnyFSS := fs.reportMeta(); hasAnyFSS {
				return fmt.Errorf("Still have mb fss state")
			}
			return nil
		})

		if fss := fs.SubjectsState("kv.bar.>"); len(fss) != 100 {
			t.Fatalf("Expected 100 entries but got %d", len(fss))
		}

		fss := fs.SubjectsState("kv.bar.99")
		if len(fss) != 1 {
			t.Fatalf("Expected 1 entry but got %d", len(fss))
		}
		expected := SimpleState{Msgs: 1, First: 199, Last: 199}
		if ss := fss["kv.bar.99"]; ss != expected {
			t.Fatalf("Bad subject state, expected %+v but got %+v", expected, ss)
		}

		// Now add one to end and check as well for non-wildcard.
		_, _, err = fs.StoreMsg("kv.foo.1", nil, []byte("value22"), 0)
		require_NoError(t, err)

		if state := fs.State(); state.Msgs != 201 {
			t.Fatalf("Expected 201 msgs but got %+v", state)
		}

		fss = fs.SubjectsState("kv.foo.1")
		if len(fss) != 1 {
			t.Fatalf("Expected 1 entry but got %d", len(fss))
		}
		expected = SimpleState{Msgs: 2, First: 1, Last: 201}
		if ss := fss["kv.foo.1"]; ss != expected {
			t.Fatalf("Bad subject state, expected %+v but got %+v", expected, ss)
		}
	})
}

func TestFileStoreEncrypted(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("aes ftw")
		for i := 0; i < 50; i++ {
			fs.StoreMsg(subj, nil, msg, 0)
		}

		o, err := fs.ConsumerStore("o22", &ConsumerConfig{})
		require_NoError(t, err)

		state := &ConsumerState{}
		state.Delivered.Consumer = 22
		state.Delivered.Stream = 22
		state.AckFloor.Consumer = 11
		state.AckFloor.Stream = 11
		err = o.Update(state)
		require_NoError(t, err)

		fs.Stop()
		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Now make sure we can read.
		var smv StoreMsg
		sm, err := fs.LoadMsg(10, &smv)
		require_NoError(t, err)
		require_True(t, string(sm.msg) == "aes ftw")

		o, err = fs.ConsumerStore("o22", &ConsumerConfig{})
		require_NoError(t, err)
		rstate, err := o.State()
		require_NoError(t, err)

		if rstate.Delivered != state.Delivered || rstate.AckFloor != state.AckFloor {
			t.Fatalf("Bad recovered consumer state, expected %+v got %+v", state, rstate)
		}
	})
}

// Make sure we do not go through block loads when we know no subjects will exists, e.g. raft.
func TestFileStoreNoFSSWhenNoSubjects(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		n, msg := 100, []byte("raft state")
		for i := 0; i < n; i++ {
			_, _, err := fs.StoreMsg(_EMPTY_, nil, msg, 0)
			require_NoError(t, err)
		}

		state := fs.State()
		require_True(t, state.Msgs == uint64(n))

		fs.Stop()
		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Make sure we did not load the block trying to generate fss.
		fs.mu.RLock()
		mb := fs.blks[0]
		fs.mu.RUnlock()

		mb.mu.Lock()
		defer mb.mu.Unlock()

		if mb.cloads > 0 {
			t.Fatalf("Expected no cache loads but got %d", mb.cloads)
		}
		if mb.fss != nil {
			t.Fatalf("Expected fss to be nil")
		}
	})
}

func TestFileStoreNoFSSBugAfterRemoveFirst(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 8 * 1024 * 1024
		fcfg.CacheExpire = 200 * time.Millisecond
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo.bar.*"}, Storage: FileStorage}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		n, msg := 100, bytes.Repeat([]byte("ZZZ"), 33) // ~100bytes
		for i := 0; i < n; i++ {
			subj := fmt.Sprintf("foo.bar.%d", i)
			_, _, err := fs.StoreMsg(subj, nil, msg, 0)
			require_NoError(t, err)
		}

		state := fs.State()
		require_True(t, state.Msgs == uint64(n))

		// Let fss expire.
		time.Sleep(250 * time.Millisecond)

		_, err = fs.RemoveMsg(1)
		require_NoError(t, err)

		sm, _, err := fs.LoadNextMsg("foo.>", true, 1, nil)
		require_NoError(t, err)
		require_True(t, sm.subj == "foo.bar.1")

		// Make sure mb.fss does not have the entry for foo.bar.0
		fs.mu.Lock()
		mb := fs.blks[0]
		fs.mu.Unlock()
		mb.mu.RLock()
		ss, ok := mb.fss.Find([]byte("foo.bar.0"))
		mb.mu.RUnlock()

		if ok && ss != nil {
			t.Fatalf("Expected no state for %q, but got %+v\n", "foo.bar.0", ss)
		}
	})
}

// NOTE: We do not use fss files anymore, but leaving test in place.
func TestFileStoreNoFSSAfterRecover(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		n, msg := 100, []byte("no fss for you!")
		for i := 0; i < n; i++ {
			_, _, err := fs.StoreMsg(_EMPTY_, nil, msg, 0)
			require_NoError(t, err)
		}

		state := fs.State()
		require_True(t, state.Msgs == uint64(n))

		fs.Stop()
		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Make sure we did not load the block trying to generate fss.
		fs.mu.RLock()
		mb := fs.blks[0]
		fs.mu.RUnlock()

		mb.mu.Lock()
		defer mb.mu.Unlock()

		if mb.fss != nil {
			t.Fatalf("Expected no fss post recover")
		}
	})
}

func TestFileStoreFSSCloseAndKeepOnExpireOnRecoverBug(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		ttl := 100 * time.Millisecond
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage, MaxAge: ttl}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		_, _, err = fs.StoreMsg("foo", nil, nil, 0)
		require_NoError(t, err)

		fs.Stop()
		time.Sleep(2 * ttl)
		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if state := fs.State(); state.NumSubjects != 0 {
			t.Fatalf("Expected no subjects with no messages, got %d", state.NumSubjects)
		}
	})
}

func TestFileStoreExpireOnRecoverSubjectAccounting(t *testing.T) {
	const msgLen = 19
	msg := bytes.Repeat([]byte("A"), msgLen)

	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 100
		ttl := 200 * time.Millisecond
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage, MaxAge: ttl}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// These are in first block.
		fs.StoreMsg("A", nil, msg, 0)
		fs.StoreMsg("B", nil, msg, 0)
		time.Sleep(ttl / 2)
		// This one in 2nd block.
		fs.StoreMsg("C", nil, msg, 0)

		fs.Stop()
		time.Sleep(ttl/2 + 10*time.Millisecond)
		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Make sure we take into account PSIM when throwing a whole block away.
		if state := fs.State(); state.NumSubjects != 1 {
			t.Fatalf("Expected 1 subject, got %d", state.NumSubjects)
		}
	})
}

func TestFileStoreFSSExpireNumPendingBug(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cexp := 100 * time.Millisecond
		fcfg.CacheExpire = cexp
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"KV.>"}, MaxMsgsPer: 1, Storage: FileStorage}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Let FSS meta expire.
		time.Sleep(2 * cexp)

		_, _, err = fs.StoreMsg("KV.X", nil, []byte("Y"), 0)
		require_NoError(t, err)

		if fss := fs.FilteredState(1, "KV.X"); fss.Msgs != 1 {
			t.Fatalf("Expected only 1 msg, got %d", fss.Msgs)
		}
	})
}

// https://github.com/nats-io/nats-server/issues/3484
func TestFileStoreFilteredFirstMatchingBug(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo.>"}, Storage: FileStorage}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		_, _, err = fs.StoreMsg("foo.foo", nil, []byte("A"), 0)
		require_NoError(t, err)

		_, _, err = fs.StoreMsg("foo.foo", nil, []byte("B"), 0)
		require_NoError(t, err)

		_, _, err = fs.StoreMsg("foo.foo", nil, []byte("C"), 0)
		require_NoError(t, err)

		fs.mu.RLock()
		mb := fs.lmb
		fs.mu.RUnlock()

		mb.mu.Lock()
		// Simulate swapping out the fss state and reading it back in with only one subject
		// present in the block.
		if mb.fss != nil {
			mb.fss = nil
		}
		// Now load info back in.
		mb.generatePerSubjectInfo()
		mb.mu.Unlock()

		// Now add in a different subject.
		_, _, err = fs.StoreMsg("foo.bar", nil, []byte("X"), 0)
		require_NoError(t, err)

		// Now see if a filtered load would incorrectly succeed.
		sm, _, err := fs.LoadNextMsg("foo.foo", false, 4, nil)
		if err == nil || sm != nil {
			t.Fatalf("Loaded filtered message with wrong subject, wanted %q got %q", "foo.foo", sm.subj)
		}
	})
}

func TestFileStoreOutOfSpaceRebuildState(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		_, _, err = fs.StoreMsg("foo", nil, []byte("A"), 0)
		require_NoError(t, err)

		_, _, err = fs.StoreMsg("bar", nil, []byte("B"), 0)
		require_NoError(t, err)

		// Grab state.
		state := fs.State()
		ss := fs.SubjectsState(">")

		// Set mock out of space error to trip.
		fs.mu.RLock()
		mb := fs.lmb
		fs.mu.RUnlock()

		mb.mu.Lock()
		mb.mockWriteErr = true
		mb.mu.Unlock()

		_, _, err = fs.StoreMsg("baz", nil, []byte("C"), 0)
		require_Error(t, err, errors.New("mock write error"))

		nstate := fs.State()
		nss := fs.SubjectsState(">")

		if !reflect.DeepEqual(state, nstate) {
			t.Fatalf("State expected to be\n  %+v\nvs\n  %+v", state, nstate)
		}

		if !reflect.DeepEqual(ss, nss) {
			t.Fatalf("Subject state expected to be\n  %+v\nvs\n  %+v", ss, nss)
		}
	})
}

func TestFileStoreRebuildStateProperlyWithMaxMsgsPerSubject(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 4096
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo", "bar", "baz"}, Storage: FileStorage, MaxMsgsPer: 1}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Send one to baz at beginning.
		_, _, err = fs.StoreMsg("baz", nil, nil, 0)
		require_NoError(t, err)

		ns := 1000
		for i := 1; i <= ns; i++ {
			_, _, err := fs.StoreMsg("foo", nil, nil, 0)
			require_NoError(t, err)
			_, _, err = fs.StoreMsg("bar", nil, nil, 0)
			require_NoError(t, err)
		}

		var ss StreamState
		fs.FastState(&ss)
		if ss.NumSubjects != 3 {
			t.Fatalf("Expected NumSubjects of 3, got %d", ss.NumSubjects)
		}
		if ss.Msgs != 3 {
			t.Fatalf("Expected NumMsgs of 3, got %d", ss.Msgs)
		}
	})
}

func TestFileStoreUpdateMaxMsgsPerSubject(t *testing.T) {
	cfg := StreamConfig{
		Name:       "TEST",
		Storage:    FileStorage,
		Subjects:   []string{"foo"},
		MaxMsgsPer: 10,
	}

	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Make sure this is honored on an update.
		cfg.MaxMsgsPer = 50
		err = fs.UpdateConfig(&cfg)
		require_NoError(t, err)

		numStored := 22
		for i := 0; i < numStored; i++ {
			_, _, err = fs.StoreMsg("foo", nil, nil, 0)
			require_NoError(t, err)
		}

		ss := fs.SubjectsState("foo")["foo"]
		if ss.Msgs != uint64(numStored) {
			t.Fatalf("Expected to have %d stored, got %d", numStored, ss.Msgs)
		}

		// Now make sure we trunk if setting to lower value.
		cfg.MaxMsgsPer = 10
		err = fs.UpdateConfig(&cfg)
		require_NoError(t, err)

		ss = fs.SubjectsState("foo")["foo"]
		if ss.Msgs != 10 {
			t.Fatalf("Expected to have %d stored, got %d", 10, ss.Msgs)
		}
	})
}

func TestFileStoreBadFirstAndFailedExpireAfterRestart(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 256
		ttl := time.Second
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage, MaxAge: ttl}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// With block size of 256 and subject and message below, seq 8 starts new block.
		// Will double check and fail test if not the case since test depends on this.
		subj, msg := "foo", []byte("ZZ")
		// These are all instant and will expire after 1 sec.
		start := time.Now()
		for i := 0; i < 7; i++ {
			_, _, err := fs.StoreMsg(subj, nil, msg, 0)
			require_NoError(t, err)
		}

		// Put two more after a delay.
		time.Sleep(1500 * time.Millisecond)
		seq, _, err := fs.StoreMsg(subj, nil, msg, 0)
		require_NoError(t, err)
		_, _, err = fs.StoreMsg(subj, nil, msg, 0)
		require_NoError(t, err)

		// Make sure that sequence 8 is first in second block, and break test if that is not true.
		fs.mu.RLock()
		lmb := fs.lmb
		fs.mu.RUnlock()
		lmb.mu.RLock()
		first := lmb.first.seq
		lmb.mu.RUnlock()
		require_True(t, first == 8)

		// Instantly remove first one from second block.
		// On restart this will trigger expire on recover which will set fs.FirstSeq to the deleted one.
		fs.RemoveMsg(seq)

		// Stop the filstore and wait til first block expires.
		fs.Stop()
		time.Sleep(ttl - time.Since(start) + (time.Second))
		fs, err = newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Check that state is correct for first message which should be 9 and have a proper timestamp.
		var state StreamState
		fs.FastState(&state)
		ts := state.FirstTime
		require_True(t, state.Msgs == 1)
		require_True(t, state.FirstSeq == 9)
		require_True(t, !state.FirstTime.IsZero())

		// Wait and make sure expire timer is still working properly.
		time.Sleep(2 * ttl)
		fs.FastState(&state)
		require_Equal(t, state.Msgs, 0)
		require_Equal(t, state.FirstSeq, 10)
		require_Equal(t, state.LastSeq, 9)
		require_Equal(t, state.LastTime, ts)
	})
}

func TestFileStoreCompactAllWithDanglingLMB(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("ZZ")
		for i := 0; i < 100; i++ {
			_, _, err := fs.StoreMsg(subj, nil, msg, 0)
			require_NoError(t, err)
		}

		fs.RemoveMsg(100)
		purged, err := fs.Compact(100)
		require_NoError(t, err)
		require_True(t, purged == 99)

		_, _, err = fs.StoreMsg(subj, nil, msg, 0)
		require_NoError(t, err)
	})
}

func TestFileStoreStateWithBlkFirstDeleted(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 4096
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("Hello World")
		toStore := 500
		for i := 0; i < toStore; i++ {
			_, _, err := fs.StoreMsg(subj, nil, msg, 0)
			require_NoError(t, err)
		}

		// Delete some messages from the beginning of an interior block.
		fs.mu.RLock()
		require_True(t, len(fs.blks) > 2)
		fseq := fs.blks[1].first.seq
		fs.mu.RUnlock()

		// Now start from first seq of second blk and delete 10 msgs
		for seq := fseq; seq < fseq+10; seq++ {
			removed, err := fs.RemoveMsg(seq)
			require_NoError(t, err)
			require_True(t, removed)
		}

		// This bug was in normal detailed state. But check fast state too.
		var fstate StreamState
		fs.FastState(&fstate)
		require_True(t, fstate.NumDeleted == 10)
		state := fs.State()
		require_True(t, state.NumDeleted == 10)
	})
}

func TestFileStoreMsgBlkFailOnKernelFaultLostDataReporting(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 4096
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("Hello World")
		toStore := 500
		for i := 0; i < toStore; i++ {
			_, _, err := fs.StoreMsg(subj, nil, msg, 0)
			require_NoError(t, err)
		}

		// We want to make sure all of the scenarios report lost data properly.
		// Will run 3 scenarios, 1st block, last block, interior block.
		// The new system does not detect byzantine behavior by default on creating the store.
		// A LoadMsg() of checkMsgs() call will be needed now.

		// First block
		fs.mu.RLock()
		require_True(t, len(fs.blks) > 0)
		mfn := fs.blks[0].mfn
		fs.mu.RUnlock()

		require_NoError(t, fs.Stop())
		require_NoError(t, os.Remove(mfn))

		// Restart.
		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		_, err = fs.LoadMsg(1, nil)
		require_Error(t, err, ErrStoreMsgNotFound)

		// Load will rebuild fs itself async..
		checkFor(t, time.Second, 50*time.Millisecond, func() error {
			if state := fs.State(); state.Lost != nil {
				return nil
			}
			return errors.New("no ld yet")
		})

		state := fs.State()
		require_Equal(t, state.FirstSeq, 94)
		require_True(t, state.Lost != nil)
		require_Len(t, len(state.Lost.Msgs), 93)

		// Last block
		fs.mu.RLock()
		require_True(t, len(fs.blks) > 0)
		require_True(t, fs.lmb != nil)
		mfn = fs.lmb.mfn
		fs.mu.RUnlock()

		require_NoError(t, fs.Stop())
		require_NoError(t, os.Remove(mfn))

		// Restart.
		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		state = fs.State()
		require_Equal(t, state.FirstSeq, 94)
		require_Equal(t, state.LastSeq, 500)   // Make sure we do not lose last seq.
		require_Equal(t, state.NumDeleted, 35) // These are interiors
		require_True(t, state.Lost != nil)
		require_Len(t, len(state.Lost.Msgs), 35)

		// Interior block.
		fs.mu.RLock()
		require_True(t, len(fs.blks) > 3)
		mfn = fs.blks[len(fs.blks)-3].mfn
		fs.mu.RUnlock()

		require_NoError(t, fs.Stop())
		require_NoError(t, os.Remove(mfn))

		// Restart.
		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Need checkMsgs to catch interior one.
		require_True(t, fs.checkMsgs() != nil)

		state = fs.State()
		require_Equal(t, state.FirstSeq, 94)
		require_Equal(t, state.LastSeq, 500) // Make sure we do not lose last seq.
		require_Equal(t, state.NumDeleted, 128)
		require_True(t, state.Lost != nil)
		require_Len(t, len(state.Lost.Msgs), 93)
	})
}

func TestFileStoreAllFilteredStateWithDeleted(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 1024
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("Hello World")
		for i := 0; i < 100; i++ {
			_, _, err := fs.StoreMsg(subj, nil, msg, 0)
			require_NoError(t, err)
		}

		remove := func(seqs ...uint64) {
			for _, seq := range seqs {
				ok, err := fs.RemoveMsg(seq)
				require_NoError(t, err)
				require_True(t, ok)
			}
		}

		checkFilteredState := func(start, msgs, first, last int) {
			fss := fs.FilteredState(uint64(start), _EMPTY_)
			if fss.Msgs != uint64(msgs) {
				t.Fatalf("Expected %d msgs, got %d", msgs, fss.Msgs)
			}
			if fss.First != uint64(first) {
				t.Fatalf("Expected %d to be first, got %d", first, fss.First)
			}
			if fss.Last != uint64(last) {
				t.Fatalf("Expected %d to be last, got %d", last, fss.Last)
			}
		}

		checkFilteredState(1, 100, 1, 100)
		remove(2)
		checkFilteredState(2, 98, 3, 100)
		remove(3, 4, 5)
		checkFilteredState(2, 95, 6, 100)
		checkFilteredState(6, 95, 6, 100)
		remove(8, 10, 12, 14, 16, 18)
		checkFilteredState(7, 88, 7, 100)

		// Now check when purged that we return first and last sequences properly.
		fs.Purge()
		checkFilteredState(0, 0, 101, 100)
	})
}

func TestFileStoreStreamTruncateResetMultiBlock(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 128
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		subj, msg := "foo", []byte("Hello World")
		for i := 0; i < 1000; i++ {
			_, _, err := fs.StoreMsg(subj, nil, msg, 0)
			require_NoError(t, err)
		}
		fs.syncBlocks()
		require_True(t, fs.numMsgBlocks() == 500)

		// Reset everything
		require_NoError(t, fs.Truncate(0))
		require_True(t, fs.numMsgBlocks() == 0)

		state := fs.State()
		require_Equal(t, state.Msgs, 0)
		require_Equal(t, state.Bytes, 0)
		require_Equal(t, state.FirstSeq, 0)
		require_Equal(t, state.LastSeq, 0)
		require_Equal(t, state.NumSubjects, 0)
		require_Equal(t, state.NumDeleted, 0)

		for i := 0; i < 1000; i++ {
			_, _, err := fs.StoreMsg(subj, nil, msg, 0)
			require_NoError(t, err)
		}
		fs.syncBlocks()

		state = fs.State()
		require_Equal(t, state.Msgs, 1000)
		require_Equal(t, state.Bytes, 44000)
		require_Equal(t, state.FirstSeq, 1)
		require_Equal(t, state.LastSeq, 1000)
		require_Equal(t, state.NumSubjects, 1)
		require_Equal(t, state.NumDeleted, 0)
	})
}

func TestFileStoreStreamCompactMultiBlockSubjectInfo(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 128
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		for i := 0; i < 1000; i++ {
			subj := fmt.Sprintf("foo.%d", i)
			_, _, err := fs.StoreMsg(subj, nil, []byte("Hello World"), 0)
			require_NoError(t, err)
		}
		require_True(t, fs.numMsgBlocks() == 500)

		// Compact such that we know we throw blocks away from the beginning.
		deleted, err := fs.Compact(501)
		require_NoError(t, err)
		require_True(t, deleted == 500)
		require_True(t, fs.numMsgBlocks() == 250)

		// Make sure we adjusted for subjects etc.
		state := fs.State()
		require_True(t, state.NumSubjects == 500)
	})
}

func TestFileStoreSubjectsTotals(t *testing.T) {
	// No need for all permutations here.
	storeDir := t.TempDir()
	fcfg := FileStoreConfig{StoreDir: storeDir}
	fs, err := newFileStore(fcfg, StreamConfig{Name: "zzz", Subjects: []string{"*.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	fmap := make(map[int]int)
	bmap := make(map[int]int)

	var m map[int]int
	var ft string

	for i := 0; i < 10_000; i++ {
		// Flip coin for prefix
		if rand.Intn(2) == 0 {
			ft, m = "foo", fmap
		} else {
			ft, m = "bar", bmap
		}
		dt := rand.Intn(100)
		subj := fmt.Sprintf("%s.%d", ft, dt)
		m[dt]++

		_, _, err := fs.StoreMsg(subj, nil, []byte("Hello World"), 0)
		require_NoError(t, err)
	}

	// Now test SubjectsTotal
	for dt, total := range fmap {
		subj := fmt.Sprintf("foo.%d", dt)
		m := fs.SubjectsTotals(subj)
		if m[subj] != uint64(total) {
			t.Fatalf("Expected %q to have %d total, got %d", subj, total, m[subj])
		}
	}

	// Check fmap.
	if st := fs.SubjectsTotals("foo.*"); len(st) != len(fmap) {
		t.Fatalf("Expected %d subjects for %q, got %d", len(fmap), "foo.*", len(st))
	} else {
		expected := 0
		for _, n := range fmap {
			expected += n
		}
		received := uint64(0)
		for _, n := range st {
			received += n
		}
		if received != uint64(expected) {
			t.Fatalf("Expected %d total but got %d", expected, received)
		}
	}

	// Check bmap.
	if st := fs.SubjectsTotals("bar.*"); len(st) != len(bmap) {
		t.Fatalf("Expected %d subjects for %q, got %d", len(bmap), "bar.*", len(st))
	} else {
		expected := 0
		for _, n := range bmap {
			expected += n
		}
		received := uint64(0)
		for _, n := range st {
			received += n
		}
		if received != uint64(expected) {
			t.Fatalf("Expected %d total but got %d", expected, received)
		}
	}

	// All with pwc match.
	if st, expected := fs.SubjectsTotals("*.*"), len(bmap)+len(fmap); len(st) != expected {
		t.Fatalf("Expected %d subjects for %q, got %d", expected, "*.*", len(st))
	}
}

func TestFileStoreConsumerStoreEncodeAfterRestart(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		o, err := fs.ConsumerStore("o22", &ConsumerConfig{AckPolicy: AckExplicit})
		require_NoError(t, err)

		state := &ConsumerState{}
		state.Delivered.Consumer = 22
		state.Delivered.Stream = 22
		state.AckFloor.Consumer = 11
		state.AckFloor.Stream = 11
		err = o.Update(state)
		require_NoError(t, err)

		fs.Stop()

		fs, err = newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		o, err = fs.ConsumerStore("o22", &ConsumerConfig{AckPolicy: AckExplicit})
		require_NoError(t, err)

		if o.(*consumerFileStore).state.Delivered != state.Delivered {
			t.Fatalf("Consumer state is wrong %+v vs %+v", o.(*consumerFileStore).state, state)
		}
		if o.(*consumerFileStore).state.AckFloor != state.AckFloor {
			t.Fatalf("Consumer state is wrong %+v vs %+v", o.(*consumerFileStore).state, state)
		}
	})
}

func TestFileStoreNumPendingLargeNumBlks(t *testing.T) {
	// No need for all permutations here.
	storeDir := t.TempDir()
	fcfg := FileStoreConfig{
		StoreDir:  storeDir,
		BlockSize: 128, // Small on purpose to create alot of blks.
	}
	fs, err := newFileStore(fcfg, StreamConfig{Name: "zzz", Subjects: []string{"zzz"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	subj, msg := "zzz", bytes.Repeat([]byte("X"), 100)
	numMsgs := 10_000

	for i := 0; i < numMsgs; i++ {
		fs.StoreMsg(subj, nil, msg, 0)
	}

	start := time.Now()
	total, _ := fs.NumPending(4000, "zzz", false)
	require_LessThan(t, time.Since(start), 15*time.Millisecond)
	require_Equal(t, total, 6001)

	start = time.Now()
	total, _ = fs.NumPending(6000, "zzz", false)
	require_LessThan(t, time.Since(start), 25*time.Millisecond)
	require_Equal(t, total, 4001)

	// Now delete a message in first half and second half.
	fs.RemoveMsg(1000)
	fs.RemoveMsg(9000)

	start = time.Now()
	total, _ = fs.NumPending(4000, "zzz", false)
	require_LessThan(t, time.Since(start), 50*time.Millisecond)
	require_Equal(t, total, 6000)

	start = time.Now()
	total, _ = fs.NumPending(6000, "zzz", false)
	require_LessThan(t, time.Since(start), 50*time.Millisecond)
	require_Equal(t, total, 4000)
}

func TestFileStoreSkipMsgAndNumBlocks(t *testing.T) {
	// No need for all permutations here.
	storeDir := t.TempDir()
	fcfg := FileStoreConfig{
		StoreDir:  storeDir,
		BlockSize: 128, // Small on purpose to create alot of blks.
	}
	fs, err := newFileStore(fcfg, StreamConfig{Name: "zzz", Subjects: []string{"zzz"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	subj, msg := "zzz", bytes.Repeat([]byte("X"), 100)
	numMsgs := 10_000

	fs.StoreMsg(subj, nil, msg, 0)
	for i := 0; i < numMsgs; i++ {
		fs.SkipMsg()
	}
	fs.StoreMsg(subj, nil, msg, 0)
	require_Equal(t, fs.numMsgBlocks(), 3)
}

func TestFileStoreRestoreEncryptedWithNoKeyFuncFails(t *testing.T) {
	// No need for all permutations here.
	fcfg := FileStoreConfig{StoreDir: t.TempDir(), Cipher: AES}
	cfg := StreamConfig{Name: "zzz", Subjects: []string{"zzz"}, Storage: FileStorage}
	fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
	require_NoError(t, err)
	defer fs.Stop()

	subj, msg := "zzz", bytes.Repeat([]byte("X"), 100)
	numMsgs := 100
	for i := 0; i < numMsgs; i++ {
		fs.StoreMsg(subj, nil, msg, 0)
	}

	fs.Stop()

	// Make sure if we try to restore with no prf (key) that it fails.
	_, err = newFileStoreWithCreated(fcfg, cfg, time.Now(), nil, nil)
	require_Error(t, err, errNoMainKey)
}

func TestFileStoreInitialFirstSeq(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cfg := StreamConfig{Name: "zzz", Storage: FileStorage, FirstSeq: 1000}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		seq, _, err := fs.StoreMsg("A", nil, []byte("OK"), 0)
		require_NoError(t, err)
		if seq != 1000 {
			t.Fatalf("Message should have been sequence 1000 but was %d", seq)
		}

		seq, _, err = fs.StoreMsg("B", nil, []byte("OK"), 0)
		require_NoError(t, err)
		if seq != 1001 {
			t.Fatalf("Message should have been sequence 1001 but was %d", seq)
		}

		var state StreamState
		fs.FastState(&state)
		switch {
		case state.Msgs != 2:
			t.Fatalf("Expected 2 messages, got %d", state.Msgs)
		case state.FirstSeq != 1000:
			t.Fatalf("Expected first seq 1000, got %d", state.FirstSeq)
		case state.LastSeq != 1001:
			t.Fatalf("Expected last seq 1001, got %d", state.LastSeq)
		}
	})
}

func TestFileStoreRecaluclateFirstForSubjBug(t *testing.T) {
	fs, err := newFileStore(FileStoreConfig{StoreDir: t.TempDir()}, StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	fs.StoreMsg("foo", nil, nil, 0) // 1
	fs.StoreMsg("bar", nil, nil, 0) // 2
	fs.StoreMsg("foo", nil, nil, 0) // 3

	// Now remove first 2..
	fs.RemoveMsg(1)
	fs.RemoveMsg(2)

	// Now grab first (and only) block.
	fs.mu.RLock()
	mb := fs.blks[0]
	fs.mu.RUnlock()

	// Since we lazy update the first, simulate that we have not updated it as of yet.
	ss := &SimpleState{Msgs: 1, First: 1, Last: 3, firstNeedsUpdate: true}

	mb.mu.Lock()
	defer mb.mu.Unlock()

	// Flush the cache.
	mb.clearCacheAndOffset()
	// Now call with start sequence of 1, the old one
	// This will panic without the fix.
	mb.recalculateForSubj("foo", ss)
	// Make sure it was update properly.
	require_True(t, *ss == SimpleState{Msgs: 1, First: 3, Last: 3, firstNeedsUpdate: false})
}

func TestFileStoreKeepWithDeletedMsgsBug(t *testing.T) {
	fs, err := newFileStore(FileStoreConfig{StoreDir: t.TempDir()}, StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	msg := bytes.Repeat([]byte("A"), 19)
	for i := 0; i < 5; i++ {
		fs.StoreMsg("A", nil, msg, 0)
		fs.StoreMsg("B", nil, msg, 0)
	}

	n, err := fs.PurgeEx("A", 0, 0)
	require_NoError(t, err)
	require_True(t, n == 5)

	// Purge with keep.
	n, err = fs.PurgeEx(_EMPTY_, 0, 2)
	require_NoError(t, err)
	require_True(t, n == 3)
}

func TestFileStoreRestartWithExpireAndLockingBug(t *testing.T) {
	sd := t.TempDir()
	scfg := StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage}
	fs, err := newFileStore(FileStoreConfig{StoreDir: sd}, scfg)
	require_NoError(t, err)
	defer fs.Stop()

	// 20 total
	msg := []byte("HELLO WORLD")
	for i := 0; i < 10; i++ {
		fs.StoreMsg("A", nil, msg, 0)
		fs.StoreMsg("B", nil, msg, 0)
	}
	fs.Stop()

	// Now change config underneath of so we will do expires at startup.
	scfg.MaxMsgs = 15
	scfg.MaxMsgsPer = 2
	newCfg := FileStreamInfo{Created: fs.cfg.Created, StreamConfig: scfg}

	// Replace
	fs.cfg = newCfg
	require_NoError(t, fs.writeStreamMeta())

	fs, err = newFileStore(FileStoreConfig{StoreDir: sd}, scfg)
	require_NoError(t, err)
	defer fs.Stop()
}

// Test that loads from lmb under lots of writes do not return errPartialCache.
func TestFileStoreErrPartialLoad(t *testing.T) {
	fs, err := newFileStore(FileStoreConfig{StoreDir: t.TempDir()}, StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	put := func(num int) {
		for i := 0; i < num; i++ {
			fs.StoreMsg("Z", nil, []byte("ZZZZZZZZZZZZZ"), 0)
		}
	}

	put(100)

	// Dump cache of lmb.
	clearCache := func() {
		fs.mu.RLock()
		lmb := fs.lmb
		fs.mu.RUnlock()
		lmb.mu.Lock()
		lmb.clearCache()
		lmb.mu.Unlock()
	}
	clearCache()

	qch := make(chan struct{})
	defer close(qch)

	for i := 0; i < 10; i++ {
		go func() {
			for {
				select {
				case <-qch:
					return
				default:
					put(5)
				}
			}
		}()
	}

	time.Sleep(100 * time.Millisecond)

	var smv StoreMsg
	for i := 0; i < 10_000; i++ {
		fs.mu.RLock()
		lmb := fs.lmb
		fs.mu.RUnlock()
		lmb.mu.Lock()
		first, last := fs.lmb.first.seq, fs.lmb.last.seq
		if i%100 == 0 {
			lmb.clearCache()
		}
		lmb.mu.Unlock()

		if spread := int(last - first); spread > 0 {
			seq := first + uint64(rand.Intn(spread))
			_, err = fs.LoadMsg(seq, &smv)
			require_NoError(t, err)
		}
	}
}

func TestFileStoreErrPartialLoadOnSyncClose(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir(), BlockSize: 500},
		StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage},
	)
	require_NoError(t, err)
	defer fs.Stop()

	// This yields an internal record length of 50 bytes. So 10 msgs per blk.
	msgLen := 19
	msg := bytes.Repeat([]byte("A"), msgLen)

	// Load up half the block.
	for _, subj := range []string{"A", "B", "C", "D", "E"} {
		fs.StoreMsg(subj, nil, msg, 0)
	}

	// Now simulate the sync timer closing the last block.
	fs.mu.RLock()
	lmb := fs.lmb
	fs.mu.RUnlock()
	require_True(t, lmb != nil)

	lmb.mu.Lock()
	lmb.expireCacheLocked()
	lmb.dirtyCloseWithRemove(false)
	lmb.mu.Unlock()

	fs.StoreMsg("Z", nil, msg, 0)
	_, err = fs.LoadMsg(1, nil)
	require_NoError(t, err)
}

func TestFileStoreSyncIntervals(t *testing.T) {
	fcfg := FileStoreConfig{StoreDir: t.TempDir(), SyncInterval: 250 * time.Millisecond}
	fs, err := newFileStore(fcfg, StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	checkSyncFlag := func(expected bool) {
		fs.mu.RLock()
		lmb := fs.lmb
		fs.mu.RUnlock()
		lmb.mu.RLock()
		syncNeeded := lmb.needSync
		lmb.mu.RUnlock()
		if syncNeeded != expected {
			t.Fatalf("Expected needSync to be %v", expected)
		}
	}

	checkSyncFlag(false)
	fs.StoreMsg("Z", nil, []byte("hello"), 0)
	checkSyncFlag(true)
	time.Sleep(400 * time.Millisecond)
	checkSyncFlag(false)
	fs.Stop()

	// Now check always
	fcfg.SyncInterval = 10 * time.Second
	fcfg.SyncAlways = true
	fs, err = newFileStore(fcfg, StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	checkSyncFlag(false)
	fs.StoreMsg("Z", nil, []byte("hello"), 0)
	checkSyncFlag(false)
}

// https://github.com/nats-io/nats-server/issues/4529
// Run this wuth --race and you will see the unlocked access that probably caused this.
func TestFileStoreRecalcFirstSequenceBug(t *testing.T) {
	fcfg := FileStoreConfig{StoreDir: t.TempDir()}
	fs, err := newFileStore(fcfg, StreamConfig{Name: "zzz", Subjects: []string{"*"}, MaxMsgsPer: 2, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	msg := bytes.Repeat([]byte("A"), 22)

	for _, subj := range []string{"A", "A", "B", "B"} {
		fs.StoreMsg(subj, nil, msg, 0)
	}
	// Make sure the buffer is cleared.
	clearLMBCache := func() {
		fs.mu.RLock()
		mb := fs.lmb
		fs.mu.RUnlock()
		mb.mu.Lock()
		mb.clearCacheAndOffset()
		mb.mu.Unlock()
	}

	clearLMBCache()

	// Do first here.
	fs.StoreMsg("A", nil, msg, 0)

	var wg sync.WaitGroup
	start := make(chan bool)

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 1_000; i++ {
			fs.LoadLastMsg("A", nil)
			clearLMBCache()
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 1_000; i++ {
			fs.StoreMsg("A", nil, msg, 0)
		}
	}()

	close(start)
	wg.Wait()
}

///////////////////////////////////////////////////////////////////////////
// New WAL based architecture tests
///////////////////////////////////////////////////////////////////////////

func TestFileStoreFullStateBasics(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 100
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// This yields an internal record length of 50 bytes. So 2 msgs per blk.
		subj, msgLen, recLen := "A", 19, uint64(50)
		msgA := bytes.Repeat([]byte("A"), msgLen)
		msgZ := bytes.Repeat([]byte("Z"), msgLen)

		// Send 2 msgs and stop, check for presence of our full state file.
		_, _, err = fs.StoreMsg(subj, nil, msgA, 0)
		require_NoError(t, err)
		_, _, err = fs.StoreMsg(subj, nil, msgZ, 0)
		require_NoError(t, err)
		require_Equal(t, fs.numMsgBlocks(), 1)

		// Make sure there is a full state file after we do a stop.
		require_NoError(t, fs.Stop())

		sfile := filepath.Join(fcfg.StoreDir, msgDir, streamStreamStateFile)
		if _, err := os.Stat(sfile); err != nil {
			t.Fatalf("Expected stream state file but got %v", err)
		}

		// Read it in and make sure len > 0.
		buf, err := os.ReadFile(sfile)
		require_NoError(t, err)
		require_True(t, len(buf) > 0)

		// Now make sure we recover properly.
		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Make sure there are no old idx or fss files.
		matches, err := filepath.Glob(filepath.Join(fcfg.StoreDir, msgDir, "%d.fss"))
		require_NoError(t, err)
		require_Equal(t, len(matches), 0)
		matches, err = filepath.Glob(filepath.Join(fcfg.StoreDir, msgDir, "%d.idx"))
		require_NoError(t, err)
		require_Equal(t, len(matches), 0)

		state := fs.State()
		require_Equal(t, state.Msgs, 2)
		require_Equal(t, state.FirstSeq, 1)
		require_Equal(t, state.LastSeq, 2)

		// Now make sure we can read in values.
		var smv StoreMsg
		sm, err := fs.LoadMsg(1, &smv)
		require_NoError(t, err)
		require_True(t, bytes.Equal(sm.msg, msgA))

		sm, err = fs.LoadMsg(2, &smv)
		require_NoError(t, err)
		require_True(t, bytes.Equal(sm.msg, msgZ))

		// Now add in 1 more here to split the lmb.
		_, _, err = fs.StoreMsg(subj, nil, msgZ, 0)
		require_NoError(t, err)

		// Now stop the filestore and replace the old stream state and make sure we recover correctly.
		require_NoError(t, fs.Stop())

		// Regrab the stream state
		buf, err = os.ReadFile(sfile)
		require_NoError(t, err)

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Add in one more.
		_, _, err = fs.StoreMsg(subj, nil, msgZ, 0)
		require_NoError(t, err)
		require_NoError(t, fs.Stop())

		// Put old stream state back with only 3.
		err = os.WriteFile(sfile, buf, defaultFilePerms)
		require_NoError(t, err)

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		state = fs.State()
		require_Equal(t, state.Msgs, 4)
		require_Equal(t, state.Bytes, 4*recLen)
		require_Equal(t, state.FirstSeq, 1)
		require_Equal(t, state.LastSeq, 4)
		require_Equal(t, fs.numMsgBlocks(), 2)

		// Make sure we are tracking subjects correctly.
		fs.mu.RLock()
		info, _ := fs.psim.Find(stringToBytes(subj))
		psi := *info
		fs.mu.RUnlock()

		require_Equal(t, psi.total, 4)
		require_Equal(t, psi.fblk, 1)
		require_Equal(t, psi.lblk, 2)

		// Store 1 more
		_, _, err = fs.StoreMsg(subj, nil, msgA, 0)
		require_NoError(t, err)
		require_NoError(t, fs.Stop())
		// Put old stream state back with only 3.
		err = os.WriteFile(sfile, buf, defaultFilePerms)
		require_NoError(t, err)

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		state = fs.State()
		require_Equal(t, state.Msgs, 5)
		require_Equal(t, state.FirstSeq, 1)
		require_Equal(t, state.LastSeq, 5)
		require_Equal(t, fs.numMsgBlocks(), 3)
		// Make sure we are tracking subjects correctly.
		fs.mu.RLock()
		info, _ = fs.psim.Find(stringToBytes(subj))
		psi = *info
		fs.mu.RUnlock()
		require_Equal(t, psi.total, 5)
		require_Equal(t, psi.fblk, 1)
		require_Equal(t, psi.lblk, 3)
	})
}

func TestFileStoreFullStatePurge(t *testing.T) {
	testFileStoreFullStatePurge(t, false)
}

func TestFileStoreFullStatePurgeFullRecovery(t *testing.T) {
	testFileStoreFullStatePurge(t, true)
}

func testFileStoreFullStatePurge(t *testing.T, checkFullRecovery bool) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 132 // Leave room for tombstones.
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// This yields an internal record length of 50 bytes. So 2 msgs per blk.
		subj, msg := "A", bytes.Repeat([]byte("A"), 19)

		// Should be 2 per block, so 5 blocks.
		for i := 0; i < 10; i++ {
			_, _, err = fs.StoreMsg(subj, nil, msg, 0)
			require_NoError(t, err)
		}
		n, err := fs.Purge()
		require_NoError(t, err)
		require_Equal(t, n, 10)
		state := fs.State()
		require_NoError(t, fs.Stop())
		if checkFullRecovery {
			require_NoError(t, os.Remove(filepath.Join(fcfg.StoreDir, msgDir, streamStreamStateFile)))
		}

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if newState := fs.State(); !reflect.DeepEqual(state, newState) {
			t.Fatalf("Restore state after purge does not match:\n%+v\n%+v",
				state, newState)
		}

		if checkFullRecovery {
			fs.rebuildState(nil)
			if newState := fs.State(); !reflect.DeepEqual(state, newState) {
				t.Fatalf("Restore state after purge does not match:\n%+v\n%+v",
					state, newState)
			}
		}

		// Add in more 10 more total, some B some C.
		for i := 0; i < 5; i++ {
			_, _, err = fs.StoreMsg("B", nil, msg, 0)
			require_NoError(t, err)
			_, _, err = fs.StoreMsg("C", nil, msg, 0)
			require_NoError(t, err)
		}

		n, err = fs.PurgeEx("B", 0, 0)
		require_NoError(t, err)
		require_Equal(t, n, 5)

		state = fs.State()
		require_NoError(t, fs.Stop())
		if checkFullRecovery {
			require_NoError(t, os.Remove(filepath.Join(fcfg.StoreDir, msgDir, streamStreamStateFile)))
		}

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if newState := fs.State(); !reflect.DeepEqual(state, newState) {
			t.Fatalf("Restore state after purge does not match:\n%+v\n%+v",
				state, newState)
		}

		if checkFullRecovery {
			fs.rebuildState(nil)
			if newState := fs.State(); !reflect.DeepEqual(state, newState) {
				t.Fatalf("Restore state after purge does not match:\n%+v\n%+v",
					state, newState)
			}
		}

		// Purge with keep.
		n, err = fs.PurgeEx(_EMPTY_, 0, 2)
		require_NoError(t, err)
		require_Equal(t, n, 3)

		state = fs.State()

		// Do some quick checks here, keep had a bug.
		require_Equal(t, state.Msgs, 2)
		require_Equal(t, state.FirstSeq, 18)
		require_Equal(t, state.LastSeq, 20)

		require_NoError(t, fs.Stop())
		if checkFullRecovery {
			require_NoError(t, os.Remove(filepath.Join(fcfg.StoreDir, msgDir, streamStreamStateFile)))
		}

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if newState := fs.State(); !reflect.DeepEqual(state, newState) {
			t.Fatalf("Restore state after purge does not match:\n%+v\n%+v",
				state, newState)
		}

		if checkFullRecovery {
			fs.rebuildState(nil)
			if newState := fs.State(); !reflect.DeepEqual(state, newState) {
				t.Fatalf("Restore state after purge does not match:\n%+v\n%+v",
					state, newState)
			}
		}

		// Make sure we can survive a purge with no full stream state and have the correct first sequence.
		// This used to be provided by the idx file and is now tombstones and the full stream state snapshot.
		n, err = fs.Purge()
		require_NoError(t, err)
		require_Equal(t, n, 2)
		state = fs.State()
		require_NoError(t, fs.Stop())
		if checkFullRecovery {
			require_NoError(t, os.Remove(filepath.Join(fcfg.StoreDir, msgDir, streamStreamStateFile)))
		}

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if newState := fs.State(); !reflect.DeepEqual(state, newState) {
			t.Fatalf("Restore state after purge does not match:\n%+v\n%+v",
				state, newState)
		}

		if checkFullRecovery {
			fs.rebuildState(nil)
			if newState := fs.State(); !reflect.DeepEqual(state, newState) {
				t.Fatalf("Restore state after purge does not match:\n%+v\n%+v",
					state, newState)
			}
		}
	})
}

func TestFileStoreFullStateTestUserRemoveWAL(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 132 // Leave room for tombstones.
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// This yields an internal record length of 50 bytes. So 2 msgs per blk.
		msgLen := 19
		msgA := bytes.Repeat([]byte("A"), msgLen)
		msgZ := bytes.Repeat([]byte("Z"), msgLen)

		// Store 2 msgs and delete first.
		fs.StoreMsg("A", nil, msgA, 0)
		fs.StoreMsg("Z", nil, msgZ, 0)
		fs.RemoveMsg(1)

		// Check we can load things properly since the block will have a tombstone now for seq 1.
		sm, err := fs.LoadMsg(2, nil)
		require_NoError(t, err)
		require_True(t, bytes.Equal(sm.msg, msgZ))

		require_Equal(t, fs.numMsgBlocks(), 1)
		state := fs.State()
		fs.Stop()

		// Grab the state from this stop.
		sfile := filepath.Join(fcfg.StoreDir, msgDir, streamStreamStateFile)
		buf, err := os.ReadFile(sfile)
		require_NoError(t, err)

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Check we can load things properly since the block will have a tombstone now for seq 1.
		_, err = fs.LoadMsg(2, nil)
		require_NoError(t, err)
		_, err = fs.LoadMsg(1, nil)
		require_Error(t, err, ErrStoreMsgNotFound)

		if newState := fs.State(); !reflect.DeepEqual(state, newState) {
			t.Fatalf("Restore state does not match:\n%+v\n%+v",
				state, newState)
		}
		require_True(t, !state.FirstTime.IsZero())

		// Store 2 more msgs and delete 2 & 4.
		fs.StoreMsg("A", nil, msgA, 0)
		fs.StoreMsg("Z", nil, msgZ, 0)
		fs.RemoveMsg(2)
		fs.RemoveMsg(4)

		state = fs.State()
		require_Equal(t, len(state.Deleted), state.NumDeleted)
		fs.Stop()

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if newState := fs.State(); !reflect.DeepEqual(state, newState) {
			t.Fatalf("Restore state does not match:\n%+v\n%+v",
				state, newState)
		}
		require_True(t, !state.FirstTime.IsZero())

		// Now close again and put back old stream state.
		// This will test that we can remember user deletes by placing tombstones in the lmb/wal.
		fs.Stop()
		err = os.WriteFile(sfile, buf, defaultFilePerms)
		require_NoError(t, err)

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		newState := fs.State()
		// We will properly detect lost data for sequence #2 here.
		require_True(t, newState.Lost != nil)
		require_Equal(t, len(newState.Lost.Msgs), 1)
		require_Equal(t, newState.Lost.Msgs[0], 2)
		// Clear for deep equal compare below.
		newState.Lost = nil

		if !reflect.DeepEqual(state, newState) {
			t.Fatalf("Restore state does not match:\n%+v\n%+v",
				state, newState)
		}
		require_True(t, !state.FirstTime.IsZero())
	})
}

func TestFileStoreFullStateTestSysRemovals(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 100
		cfg := StreamConfig{
			Name:       "zzz",
			Subjects:   []string{"*"},
			MaxMsgs:    10,
			MaxMsgsPer: 1,
			Storage:    FileStorage,
		}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// This yields an internal record length of 50 bytes. So 2 msgs per blk.
		msgLen := 19
		msg := bytes.Repeat([]byte("A"), msgLen)

		for _, subj := range []string{"A", "B", "A", "B"} {
			fs.StoreMsg(subj, nil, msg, 0)
		}

		state := fs.State()
		require_Equal(t, state.Msgs, 2)
		require_Equal(t, state.FirstSeq, 3)
		require_Equal(t, state.LastSeq, 4)
		fs.Stop()

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if newState := fs.State(); !reflect.DeepEqual(state, newState) {
			t.Fatalf("Restore state after purge does not match:\n%+v\n%+v",
				state, newState)
		}

		for _, subj := range []string{"C", "D", "E", "F", "G", "H", "I", "J"} {
			fs.StoreMsg(subj, nil, msg, 0)
		}

		state = fs.State()
		require_Equal(t, state.Msgs, 10)
		require_Equal(t, state.FirstSeq, 3)
		require_Equal(t, state.LastSeq, 12)
		fs.Stop()

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if newState := fs.State(); !reflect.DeepEqual(state, newState) {
			t.Fatalf("Restore state after purge does not match:\n%+v\n%+v",
				state, newState)
		}

		// Goes over limit
		fs.StoreMsg("ZZZ", nil, msg, 0)

		state = fs.State()
		require_Equal(t, state.Msgs, 10)
		require_Equal(t, state.FirstSeq, 4)
		require_Equal(t, state.LastSeq, 13)
		fs.Stop()

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if newState := fs.State(); !reflect.DeepEqual(state, newState) {
			t.Fatalf("Restore state after purge does not match:\n%+v\n%+v",
				state, newState)
		}
	})
}

func TestFileStoreSelectBlockWithFirstSeqRemovals(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 100
		cfg := StreamConfig{
			Name:       "zzz",
			Subjects:   []string{"*"},
			MaxMsgsPer: 1,
			Storage:    FileStorage,
		}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// This yields an internal record length of 50 bytes. So 2 msgs per blk.
		msgLen := 19
		msg := bytes.Repeat([]byte("A"), msgLen)

		subjects := "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz+@$^"
		// We need over 32 blocks to kick in binary search. So 32*2+1 (65) msgs to get 33 blocks.
		for i := 0; i < 32*2+1; i++ {
			subj := string(subjects[i])
			fs.StoreMsg(subj, nil, msg, 0)
		}
		require_Equal(t, fs.numMsgBlocks(), 33)

		// Now we want to delete the first msg of each block to move the first sequence.
		// Want to do this via system removes, not user initiated moves.
		for i := 0; i < len(subjects); i += 2 {
			subj := string(subjects[i])
			fs.StoreMsg(subj, nil, msg, 0)
		}

		var ss StreamState
		fs.FastState(&ss)

		// We want to make sure that select always returns an index and a non-nil mb.
		for seq := ss.FirstSeq; seq <= ss.LastSeq; seq++ {
			fs.mu.RLock()
			index, mb := fs.selectMsgBlockWithIndex(seq)
			fs.mu.RUnlock()
			require_True(t, index >= 0)
			require_True(t, mb != nil)
			require_Equal(t, (seq-1)/2, uint64(index))
		}
	})
}

func TestFileStoreMsgBlockHolesAndIndexing(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir()},
		StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage, MaxMsgsPer: 1},
	)
	require_NoError(t, err)
	defer fs.Stop()

	// Grab the message block by hand and manipulate at that level.
	mb := fs.getFirstBlock()
	writeMsg := func(subj string, seq uint64) {
		rl := fileStoreMsgSize(subj, nil, []byte(subj))
		require_NoError(t, mb.writeMsgRecord(rl, seq, subj, nil, []byte(subj), time.Now().UnixNano(), true))
		fs.rebuildState(nil)
	}
	readMsg := func(seq uint64, expectedSubj string) {
		// Clear cache so we load back in from disk and need to properly process any holes.
		ld, tombs, err := mb.rebuildState()
		require_NoError(t, err)
		require_Equal(t, ld, nil)
		require_Equal(t, len(tombs), 0)
		fs.rebuildState(nil)
		sm, _, err := mb.fetchMsg(seq, nil)
		require_NoError(t, err)
		require_Equal(t, sm.subj, expectedSubj)
		require_True(t, bytes.Equal(sm.buf[:len(expectedSubj)], []byte(expectedSubj)))
	}

	writeMsg("A", 2)
	require_Equal(t, mb.first.seq, 2)
	require_Equal(t, mb.last.seq, 2)

	writeMsg("B", 4)
	require_Equal(t, mb.first.seq, 2)
	require_Equal(t, mb.last.seq, 4)

	writeMsg("C", 12)

	readMsg(4, "B")
	require_True(t, mb.dmap.Exists(3))

	readMsg(12, "C")
	readMsg(2, "A")

	// Check that we get deleted for the right ones etc.
	checkDeleted := func(seq uint64) {
		_, _, err = mb.fetchMsg(seq, nil)
		require_Error(t, err, ErrStoreMsgNotFound, errDeletedMsg)
		mb.mu.RLock()
		shouldExist, exists := seq >= mb.first.seq, mb.dmap.Exists(seq)
		mb.mu.RUnlock()
		if shouldExist {
			require_True(t, exists)
		}
	}
	checkDeleted(1)
	checkDeleted(3)
	for seq := 5; seq < 12; seq++ {
		checkDeleted(uint64(seq))
	}
}

func TestFileStoreMsgBlockCompactionAndHoles(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir()},
		StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage, MaxMsgsPer: 1},
	)
	require_NoError(t, err)
	defer fs.Stop()

	msg := bytes.Repeat([]byte("Z"), 1024)
	for _, subj := range []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"} {
		fs.StoreMsg(subj, nil, msg, 0)
	}
	// Leave first one but delete the rest.
	for seq := uint64(2); seq < 10; seq++ {
		fs.RemoveMsg(seq)
	}
	require_Equal(t, fs.numMsgBlocks(), 1)
	mb := fs.getFirstBlock()
	require_NotNil(t, mb)

	_, ub, _ := fs.Utilization()

	// Do compaction, should remove all excess now.
	mb.mu.Lock()
	mb.compact()
	mb.mu.Unlock()

	ta, ua, _ := fs.Utilization()
	require_Equal(t, ub, ua)
	require_Equal(t, ta, ua)
}

func TestFileStoreRemoveLastNoDoubleTombstones(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir()},
		StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage, MaxMsgsPer: 1},
	)
	require_NoError(t, err)
	defer fs.Stop()

	fs.StoreMsg("A", nil, []byte("hello"), 0)
	fs.mu.Lock()
	fs.removeMsgViaLimits(1)
	fs.mu.Unlock()

	require_Equal(t, fs.numMsgBlocks(), 1)
	mb := fs.getFirstBlock()
	require_NotNil(t, mb)
	mb.loadMsgs()
	rbytes, _, err := fs.Utilization()
	require_NoError(t, err)
	require_Equal(t, rbytes, emptyRecordLen)
}

func TestFileStoreFullStateMultiBlockPastWAL(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 100
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// This yields an internal record length of 50 bytes. So 2 msgs per blk.
		msgLen := 19
		msgA := bytes.Repeat([]byte("A"), msgLen)
		msgZ := bytes.Repeat([]byte("Z"), msgLen)

		// Store 2 msgs
		fs.StoreMsg("A", nil, msgA, 0)
		fs.StoreMsg("B", nil, msgZ, 0)
		require_Equal(t, fs.numMsgBlocks(), 1)
		fs.Stop()

		// Grab the state from this stop.
		sfile := filepath.Join(fcfg.StoreDir, msgDir, streamStreamStateFile)
		buf, err := os.ReadFile(sfile)
		require_NoError(t, err)

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Store 6 more msgs.
		fs.StoreMsg("C", nil, msgA, 0)
		fs.StoreMsg("D", nil, msgZ, 0)
		fs.StoreMsg("E", nil, msgA, 0)
		fs.StoreMsg("F", nil, msgZ, 0)
		fs.StoreMsg("G", nil, msgA, 0)
		fs.StoreMsg("H", nil, msgZ, 0)
		require_Equal(t, fs.numMsgBlocks(), 4)
		state := fs.State()
		fs.Stop()

		// Put back old stream state.
		// This will test that we properly walk multiple blocks past where we snapshotted state.
		err = os.WriteFile(sfile, buf, defaultFilePerms)
		require_NoError(t, err)

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if newState := fs.State(); !reflect.DeepEqual(state, newState) {
			t.Fatalf("Restore state does not match:\n%+v\n%+v",
				state, newState)
		}
		require_True(t, !state.FirstTime.IsZero())
	})
}

// This tests we can successfully recover without having to rebuild the whole stream from a mid block index.db marker
// when the updated block has a removed entry.
// Make sure this does not cause a recover of the full state.
func TestFileStoreFullStateMidBlockPastWAL(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage, MaxMsgsPer: 1}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// This yields an internal record length of 50 bytes. So 2 msgs per blk.
		msg := bytes.Repeat([]byte("Z"), 19)

		// Store 5 msgs
		fs.StoreMsg("A", nil, msg, 0)
		fs.StoreMsg("B", nil, msg, 0)
		fs.StoreMsg("C", nil, msg, 0)
		fs.StoreMsg("D", nil, msg, 0)
		fs.StoreMsg("E", nil, msg, 0)
		require_Equal(t, fs.numMsgBlocks(), 1)
		fs.Stop()

		// Grab the state from this stop.
		sfile := filepath.Join(fcfg.StoreDir, msgDir, streamStreamStateFile)
		buf, err := os.ReadFile(sfile)
		require_NoError(t, err)

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// Store 5 more messages, then remove seq 2, "B".
		fs.StoreMsg("F", nil, msg, 0)
		fs.StoreMsg("G", nil, msg, 0)
		fs.StoreMsg("H", nil, msg, 0)
		fs.StoreMsg("I", nil, msg, 0)
		fs.StoreMsg("J", nil, msg, 0)
		fs.RemoveMsg(2)

		require_Equal(t, fs.numMsgBlocks(), 1)
		state := fs.State()
		fs.Stop()

		// Put back old stream state.
		// This will test that we properly walk multiple blocks past where we snapshotted state.
		err = os.WriteFile(sfile, buf, defaultFilePerms)
		require_NoError(t, err)

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if newState := fs.State(); !reflect.DeepEqual(state, newState) {
			t.Fatalf("Restore state does not match:\n%+v\n%+v",
				state, newState)
		}
		// Check that index.db is still there. If we recover by raw data on a corrupt state we delete this.
		_, err = os.Stat(sfile)
		require_NoError(t, err)
	})
}

func TestFileStoreCompactingBlocksOnSync(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 1000 // 20 msgs per block.
		fcfg.SyncInterval = 100 * time.Millisecond
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage, MaxMsgsPer: 1}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// This yields an internal record length of 50 bytes. So 20 msgs per blk.
		msg := bytes.Repeat([]byte("Z"), 19)
		subjects := "ABCDEFGHIJKLMNOPQRST"
		for _, subj := range subjects {
			fs.StoreMsg(string(subj), nil, msg, 0)
		}
		require_Equal(t, fs.numMsgBlocks(), 1)
		total, reported, err := fs.Utilization()
		require_NoError(t, err)

		require_Equal(t, total, reported)

		// Now start removing, since we are small this should not kick in any inline logic.
		// Remove all interior messages, leave 1 and 20. So write B-S
		for i := 1; i < 19; i++ {
			fs.StoreMsg(string(subjects[i]), nil, msg, 0)
		}
		require_Equal(t, fs.numMsgBlocks(), 2)

		blkUtil := func() (uint64, uint64) {
			fs.mu.RLock()
			fmb := fs.blks[0]
			fs.mu.RUnlock()
			fmb.mu.RLock()
			defer fmb.mu.RUnlock()
			return fmb.rbytes, fmb.bytes
		}

		total, reported = blkUtil()
		require_Equal(t, reported, 100)
		// Raw bytes will be 1000, but due to compression could be less.
		if fcfg.Compression != NoCompression {
			require_True(t, total > reported)
		} else {
			require_Equal(t, total, 1000)
		}

		// Make sure the sync interval when kicked in compacts down to rbytes == 100.
		checkFor(t, time.Second, 100*time.Millisecond, func() error {
			if total, reported := blkUtil(); total <= reported {
				return nil
			}
			return fmt.Errorf("Not compacted yet, raw %v vs reported %v",
				friendlyBytes(total), friendlyBytes(reported))
		})
	})
}

// Make sure a call to Compact() updates PSIM correctly.
func TestFileStoreCompactAndPSIMWhenDeletingBlocks(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir(), BlockSize: 512},
		StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	subj, msg := "A", bytes.Repeat([]byte("ABC"), 33) // ~100bytes

	// Add in 10 As
	for i := 0; i < 10; i++ {
		fs.StoreMsg(subj, nil, msg, 0)
	}
	require_Equal(t, fs.numMsgBlocks(), 4)

	// Should leave 1.
	n, err := fs.Compact(10)
	require_NoError(t, err)
	require_Equal(t, n, 9)
	require_Equal(t, fs.numMsgBlocks(), 1)

	fs.mu.RLock()
	info, _ := fs.psim.Find(stringToBytes(subj))
	psi := *info
	fs.mu.RUnlock()

	require_Equal(t, psi.total, 1)
	require_Equal(t, psi.fblk, psi.lblk)
}

func TestFileStoreTrackSubjLenForPSIM(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd},
		StreamConfig{Name: "zzz", Subjects: []string{">"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	// Place 1000 msgs with varying subjects.
	// Make sure we track the subject length properly.
	smap := make(map[string]int, 1000)
	buf := make([]byte, 10)
	for i := 0; i < 1000; i++ {
		var b strings.Builder
		// 1-6 tokens.
		numTokens := rand.Intn(6) + 1
		for i := 0; i < numTokens; i++ {
			tlen := rand.Intn(4) + 2
			tok := buf[:tlen]
			crand.Read(tok)
			b.WriteString(hex.EncodeToString(tok))
			if i != numTokens-1 {
				b.WriteString(".")
			}
		}
		subj := b.String()
		// Avoid dupes since will cause check to fail after we delete messages.
		if _, ok := smap[subj]; ok {
			continue
		}
		smap[subj] = len(subj)
		fs.StoreMsg(subj, nil, nil, 0)
	}

	check := func() {
		t.Helper()
		var total int
		for _, slen := range smap {
			total += slen
		}
		fs.mu.RLock()
		tsl := fs.tsl
		fs.mu.RUnlock()
		require_Equal(t, tsl, total)
	}

	check()

	// Delete ~half
	var smv StoreMsg
	for i := 0; i < 500; i++ {
		seq := uint64(rand.Intn(1000) + 1)
		sm, err := fs.LoadMsg(seq, &smv)
		if err != nil {
			continue
		}
		fs.RemoveMsg(seq)
		delete(smap, sm.subj)
	}

	check()

	// Make sure we can recover same after restart.
	fs.Stop()
	fs, err = newFileStore(
		FileStoreConfig{StoreDir: sd},
		StreamConfig{Name: "zzz", Subjects: []string{">"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	check()

	// Drain the rest through purge.
	fs.Purge()
	smap = nil
	check()
}

// This was used to make sure our estimate was correct, but not needed normally.
func TestFileStoreLargeFullStatePSIM(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd},
		StreamConfig{Name: "zzz", Subjects: []string{">"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	buf := make([]byte, 20)
	for i := 0; i < 100_000; i++ {
		var b strings.Builder
		// 1-6 tokens.
		numTokens := rand.Intn(6) + 1
		for i := 0; i < numTokens; i++ {
			tlen := rand.Intn(8) + 2
			tok := buf[:tlen]
			crand.Read(tok)
			b.WriteString(hex.EncodeToString(tok))
			if i != numTokens-1 {
				b.WriteString(".")
			}
		}
		subj := b.String()
		fs.StoreMsg(subj, nil, nil, 0)
	}
	fs.Stop()
}

func TestFileStoreLargeFullStateMetaCleanup(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd},
		StreamConfig{Name: "zzz", Subjects: []string{">"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	subj, msg := "foo.bar.baz", bytes.Repeat([]byte("ABC"), 33) // ~100bytes
	for i := 0; i < 1000; i++ {
		fs.StoreMsg(subj, nil, nil, 0)
	}
	fs.Stop()

	mdir := filepath.Join(sd, msgDir)
	idxFile := filepath.Join(mdir, "1.idx")
	fssFile := filepath.Join(mdir, "1.fss")
	require_NoError(t, os.WriteFile(idxFile, msg, defaultFilePerms))
	require_NoError(t, os.WriteFile(fssFile, msg, defaultFilePerms))

	fs, err = newFileStore(
		FileStoreConfig{StoreDir: sd},
		StreamConfig{Name: "zzz", Subjects: []string{">"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	checkFor(t, time.Second, 50*time.Millisecond, func() error {
		if _, err := os.Stat(idxFile); err == nil {
			return errors.New("idx file still exists")
		}
		if _, err := os.Stat(fssFile); err == nil {
			return errors.New("fss file still exists")
		}
		return nil
	})
}

func TestFileStoreIndexDBExistsAfterShutdown(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd},
		StreamConfig{Name: "zzz", Subjects: []string{">"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	subj := "foo.bar.baz"
	for i := 0; i < 1000; i++ {
		fs.StoreMsg(subj, nil, nil, 0)
	}

	idxFile := filepath.Join(sd, msgDir, streamStreamStateFile)

	fs.mu.Lock()
	fs.dirty = 1
	if err := os.Remove(idxFile); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	fs.mu.Unlock()

	fs.Stop()

	checkFor(t, time.Second, 50*time.Millisecond, func() error {
		if _, err := os.Stat(idxFile); err != nil {
			return fmt.Errorf("%q doesn't exist", idxFile)
		}
		return nil
	})
}

// https://github.com/nats-io/nats-server/issues/4842
func TestFileStoreSubjectCorruption(t *testing.T) {
	sd, blkSize := t.TempDir(), uint64(2*1024*1024)
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd, BlockSize: blkSize},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	numSubjects := 100
	msgs := [][]byte{bytes.Repeat([]byte("ABC"), 333), bytes.Repeat([]byte("ABC"), 888), bytes.Repeat([]byte("ABC"), 555)}
	for i := 0; i < 10_000; i++ {
		subj := fmt.Sprintf("foo.%d", rand.Intn(numSubjects)+1)
		msg := msgs[rand.Intn(len(msgs))]
		fs.StoreMsg(subj, nil, msg, 0)
	}
	fs.Stop()

	require_NoError(t, os.Remove(filepath.Join(sd, msgDir, streamStreamStateFile)))

	fs, err = newFileStore(
		FileStoreConfig{StoreDir: sd, BlockSize: blkSize},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	for subj := range fs.SubjectsTotals(">") {
		var n int
		_, err := fmt.Sscanf(subj, "foo.%d", &n)
		require_NoError(t, err)
	}
}

// Since 2.10 we no longer have fss, and the approach for calculating NumPending would branch
// based on the old fss metadata being present. This meant that calculating NumPending in >= 2.10.x
// would load all blocks to complete. This test makes sure we do not do that anymore.
func TestFileStoreNumPendingLastBySubject(t *testing.T) {
	sd, blkSize := t.TempDir(), uint64(1024)
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd, BlockSize: blkSize},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	numSubjects := 20
	msg := bytes.Repeat([]byte("ABC"), 25)
	for i := 1; i <= 1000; i++ {
		subj := fmt.Sprintf("foo.%d.%d", rand.Intn(numSubjects)+1, i)
		fs.StoreMsg(subj, nil, msg, 0)
	}
	// Each block has ~8 msgs.
	require_True(t, fs.numMsgBlocks() > 100)

	calcCacheLoads := func() (cloads uint64) {
		fs.mu.RLock()
		defer fs.mu.RUnlock()
		for _, mb := range fs.blks {
			mb.mu.RLock()
			cloads += mb.cloads
			mb.mu.RUnlock()
		}
		return cloads
	}

	total, _ := fs.NumPending(0, "foo.*.*", true)
	require_Equal(t, total, 1000)
	// Make sure no blocks were loaded to calculate this as a new consumer.
	require_Equal(t, calcCacheLoads(), 0)

	checkResult := func(sseq, np uint64, filter string) {
		t.Helper()
		var checkTotal uint64
		var smv StoreMsg
		for seq := sseq; seq <= 1000; seq++ {
			sm, err := fs.LoadMsg(seq, &smv)
			require_NoError(t, err)
			if subjectIsSubsetMatch(sm.subj, filter) {
				checkTotal++
			}
		}
		require_Equal(t, np, checkTotal)
	}

	// Make sure partials work properly.
	for _, filter := range []string{"foo.10.*", "*.22.*", "*.*.222", "foo.5.999", "*.2.*"} {
		sseq := uint64(rand.Intn(250) + 200) // Between 200-450
		total, _ = fs.NumPending(sseq, filter, true)
		checkResult(sseq, total, filter)
	}
}

// We had a bug that could cause internal memory corruption of the psim keys in memory
// which could have been written to disk via index.db.
func TestFileStoreCorruptPSIMOnDisk(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	fs.StoreMsg("foo.bar", nil, []byte("ABC"), 0)
	fs.StoreMsg("foo.baz", nil, []byte("XYZ"), 0)

	// Force bad subject.
	fs.mu.Lock()
	psi, _ := fs.psim.Find(stringToBytes("foo.bar"))
	bad := []rune("foo.bar")
	bad[3] = utf8.RuneError
	fs.psim.Insert(stringToBytes(string(bad)), *psi)
	fs.psim.Delete(stringToBytes("foo.bar"))
	fs.dirty++
	fs.mu.Unlock()

	// Restart
	fs.Stop()
	fs, err = newFileStore(
		FileStoreConfig{StoreDir: sd},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	sm, err := fs.LoadLastMsg("foo.bar", nil)
	require_NoError(t, err)
	require_True(t, bytes.Equal(sm.msg, []byte("ABC")))

	sm, err = fs.LoadLastMsg("foo.baz", nil)
	require_NoError(t, err)
	require_True(t, bytes.Equal(sm.msg, []byte("XYZ")))
}

func TestFileStorePurgeExBufPool(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd, BlockSize: 1024},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	msg := bytes.Repeat([]byte("ABC"), 33) // ~100bytes
	for i := 0; i < 1000; i++ {
		fs.StoreMsg("foo.foo", nil, msg, 0)
		fs.StoreMsg("foo.bar", nil, msg, 0)
	}

	p, err := fs.PurgeEx("foo.bar", 1, 0)
	require_NoError(t, err)
	require_Equal(t, p, 1000)

	// Now make sure we do not have all of the msg blocks cache's loaded.
	var loaded int
	fs.mu.RLock()
	for _, mb := range fs.blks {
		mb.mu.RLock()
		if mb.cacheAlreadyLoaded() {
			loaded++
		}
		mb.mu.RUnlock()
	}
	fs.mu.RUnlock()
	require_Equal(t, loaded, 1)
}

func TestFileStoreFSSMeta(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd, BlockSize: 100, CacheExpire: 200 * time.Millisecond, SubjectStateExpire: time.Second},
		StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	// This yields an internal record length of 50 bytes. So 2 msgs per blk with subject len of 1, e.g. "A" or "Z".
	msg := bytes.Repeat([]byte("Z"), 19)

	// Should leave us with |A-Z| |Z-Z| |Z-Z| |Z-A|
	fs.StoreMsg("A", nil, msg, 0)
	for i := 0; i < 6; i++ {
		fs.StoreMsg("Z", nil, msg, 0)
	}
	fs.StoreMsg("A", nil, msg, 0)

	// Let cache's expire before PurgeEx which will load them back in.
	time.Sleep(500 * time.Millisecond)

	p, err := fs.PurgeEx("A", 1, 0)
	require_NoError(t, err)
	require_Equal(t, p, 2)

	// Make sure cache is not loaded.
	var stillHasCache bool
	fs.mu.RLock()
	for _, mb := range fs.blks {
		mb.mu.RLock()
		stillHasCache = stillHasCache || mb.cacheAlreadyLoaded()
		mb.mu.RUnlock()
	}
	fs.mu.RUnlock()

	require_False(t, stillHasCache)

	// Let fss expire via SubjectStateExpire.
	time.Sleep(1500 * time.Millisecond)

	var noFSS bool
	fs.mu.RLock()
	for _, mb := range fs.blks {
		mb.mu.RLock()
		noFSS = noFSS || mb.fssNotLoaded()
		mb.mu.RUnlock()
	}
	fs.mu.RUnlock()

	require_True(t, noFSS)
}

func TestFileStoreExpireCacheOnLinearWalk(t *testing.T) {
	sd := t.TempDir()
	expire := 250 * time.Millisecond
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd, CacheExpire: expire},
		StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	// This yields an internal record length of 50 bytes.
	subj, msg := "Z", bytes.Repeat([]byte("Z"), 19)

	// Store 10 messages, so 5 blocks.
	for i := 0; i < 10; i++ {
		fs.StoreMsg(subj, nil, msg, 0)
	}
	// Let them all expire. This way we load as we walk and can test that we expire all blocks without
	// needing to worry about last write times blocking forced expiration.
	time.Sleep(expire + ats.TickInterval)

	checkNoCache := func() {
		t.Helper()
		fs.mu.RLock()
		var stillHasCache bool
		for _, mb := range fs.blks {
			mb.mu.RLock()
			stillHasCache = stillHasCache || mb.cacheAlreadyLoaded()
			mb.mu.RUnlock()
		}
		fs.mu.RUnlock()
		require_False(t, stillHasCache)
	}

	// Walk forward.
	var smv StoreMsg
	for seq := uint64(1); seq <= 10; seq++ {
		_, err := fs.LoadMsg(seq, &smv)
		require_NoError(t, err)
	}
	checkNoCache()

	// No test walking backwards. We have this scenario when we search for starting points for sourced streams.
	// Noticed some memory bloat when we have to search many blocks looking for a source that may be closer to the
	// beginning of the stream (infrequently updated sourced stream).
	for seq := uint64(10); seq >= 1; seq-- {
		_, err := fs.LoadMsg(seq, &smv)
		require_NoError(t, err)
	}
	checkNoCache()

	// Now make sure still expires properly on linear scans with deleted msgs.
	// We want to make sure we track linear updates even if message deleted.
	_, err = fs.RemoveMsg(2)
	require_NoError(t, err)
	_, err = fs.RemoveMsg(9)
	require_NoError(t, err)

	// Walk forward.
	for seq := uint64(1); seq <= 10; seq++ {
		_, err := fs.LoadMsg(seq, &smv)
		if seq == 2 || seq == 9 {
			require_Error(t, err, errDeletedMsg)
		} else {
			require_NoError(t, err)
		}
	}
	checkNoCache()
}

func TestFileStoreSkipMsgs(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir(), BlockSize: 1024},
		StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	// Test on empty FS first.
	// Make sure wrong starting sequence fails.
	err = fs.SkipMsgs(10, 100)
	require_Error(t, err, ErrSequenceMismatch)

	err = fs.SkipMsgs(1, 100)
	require_NoError(t, err)

	state := fs.State()
	require_Equal(t, state.FirstSeq, 101)
	require_Equal(t, state.LastSeq, 100)
	require_Equal(t, fs.numMsgBlocks(), 1)

	// Now add alot.
	err = fs.SkipMsgs(101, 100_000)
	require_NoError(t, err)
	state = fs.State()
	require_Equal(t, state.FirstSeq, 100_101)
	require_Equal(t, state.LastSeq, 100_100)
	require_Equal(t, fs.numMsgBlocks(), 1)

	// Now add in a message, and then skip to check dmap.
	fs, err = newFileStore(
		FileStoreConfig{StoreDir: t.TempDir(), BlockSize: 1024},
		StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	fs.StoreMsg("foo", nil, nil, 0)
	err = fs.SkipMsgs(2, 10)
	require_NoError(t, err)
	state = fs.State()
	require_Equal(t, state.FirstSeq, 1)
	require_Equal(t, state.LastSeq, 11)
	require_Equal(t, state.Msgs, 1)
	require_Equal(t, state.NumDeleted, 10)
	require_Equal(t, len(state.Deleted), 10)

	// Check Fast State too.
	state.Deleted = nil
	fs.FastState(&state)
	require_Equal(t, state.FirstSeq, 1)
	require_Equal(t, state.LastSeq, 11)
	require_Equal(t, state.Msgs, 1)
	require_Equal(t, state.NumDeleted, 10)
}

func TestFileStoreOptimizeFirstLoadNextMsgWithSequenceZero(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd, BlockSize: 4096},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	msg := bytes.Repeat([]byte("ZZZ"), 33) // ~100bytes

	for i := 0; i < 5000; i++ {
		fs.StoreMsg("foo.A", nil, msg, 0)
	}
	// This will create alot of blocks, ~167.
	// Just used to check that we do not load these in when searching.
	// Now add in 10 for foo.bar at the end.
	for i := 0; i < 10; i++ {
		fs.StoreMsg("foo.B", nil, msg, 0)
	}
	// The bug would not be visible on running server per se since we would have had fss loaded
	// and that sticks around a bit longer, we would use that to skip over the early blocks. So stop
	// and restart the filestore.
	fs.Stop()
	fs, err = newFileStore(
		FileStoreConfig{StoreDir: sd, BlockSize: 4096},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	// Now fetch the next message for foo.B but set starting sequence to 0.
	_, nseq, err := fs.LoadNextMsg("foo.B", false, 0, nil)
	require_NoError(t, err)
	require_Equal(t, nseq, 5001)
	// Now check how many blks are loaded, should be only 1.
	require_Equal(t, fs.cacheLoads(), 1)
}

func TestFileStoreWriteFullStateHighSubjectCardinality(t *testing.T) {
	t.Skip()

	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd, BlockSize: 4096},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	msg := []byte{1, 2, 3}

	for i := 0; i < 1_000_000; i++ {
		subj := fmt.Sprintf("subj_%d", i)
		_, _, err := fs.StoreMsg(subj, nil, msg, 0)
		require_NoError(t, err)
	}

	start := time.Now()
	require_NoError(t, fs.writeFullState())
	t.Logf("Took %s to writeFullState", time.Since(start))
}

func TestFileStoreEraseMsgWithDbitSlots(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir()},
		StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	fs.StoreMsg("foo", nil, []byte("abd"), 0)
	for i := 0; i < 10; i++ {
		fs.SkipMsg()
	}
	fs.StoreMsg("foo", nil, []byte("abd"), 0)
	// Now grab that first block and compact away the skips which will
	// introduce dbits into our idx.
	fs.mu.RLock()
	mb := fs.blks[0]
	fs.mu.RUnlock()
	// Compact.
	mb.mu.Lock()
	mb.compact()
	mb.mu.Unlock()

	removed, err := fs.EraseMsg(1)
	require_NoError(t, err)
	require_True(t, removed)
}

func TestFileStoreEraseMsgWithAllTrailingDbitSlots(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir()},
		StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	fs.StoreMsg("foo", nil, []byte("abc"), 0)
	fs.StoreMsg("foo", nil, []byte("abcdefg"), 0)

	for i := 0; i < 10; i++ {
		fs.SkipMsg()
	}
	// Now grab that first block and compact away the skips which will
	// introduce dbits into our idx.
	fs.mu.RLock()
	mb := fs.blks[0]
	fs.mu.RUnlock()
	// Compact.
	mb.mu.Lock()
	mb.compact()
	mb.mu.Unlock()

	removed, err := fs.EraseMsg(2)
	require_NoError(t, err)
	require_True(t, removed)
}

func TestFileStoreMultiLastSeqs(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir(), BlockSize: 256}, // Make block size small to test multiblock selections with maxSeq
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*", "bar.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	msg := []byte("abc")
	for i := 0; i < 33; i++ {
		fs.StoreMsg("foo.foo", nil, msg, 0)
		fs.StoreMsg("foo.bar", nil, msg, 0)
		fs.StoreMsg("foo.baz", nil, msg, 0)
	}
	for i := 0; i < 33; i++ {
		fs.StoreMsg("bar.foo", nil, msg, 0)
		fs.StoreMsg("bar.bar", nil, msg, 0)
		fs.StoreMsg("bar.baz", nil, msg, 0)
	}

	checkResults := func(seqs, expected []uint64) {
		t.Helper()
		if len(seqs) != len(expected) {
			t.Fatalf("Expected %+v got %+v", expected, seqs)
		}
		for i := range seqs {
			if seqs[i] != expected[i] {
				t.Fatalf("Expected %+v got %+v", expected, seqs)
			}
		}
	}

	// UpTo sequence 3. Tests block split.
	seqs, err := fs.MultiLastSeqs([]string{"foo.*"}, 3, -1)
	require_NoError(t, err)
	checkResults(seqs, []uint64{1, 2, 3})
	// Up to last sequence of the stream.
	seqs, err = fs.MultiLastSeqs([]string{"foo.*"}, 0, -1)
	require_NoError(t, err)
	checkResults(seqs, []uint64{97, 98, 99})
	// Check for bar.* at the end.
	seqs, err = fs.MultiLastSeqs([]string{"bar.*"}, 0, -1)
	require_NoError(t, err)
	checkResults(seqs, []uint64{196, 197, 198})
	// This should find nothing.
	seqs, err = fs.MultiLastSeqs([]string{"bar.*"}, 99, -1)
	require_NoError(t, err)
	checkResults(seqs, nil)

	// Do multiple subjects explicitly.
	seqs, err = fs.MultiLastSeqs([]string{"foo.foo", "foo.bar", "foo.baz"}, 3, -1)
	require_NoError(t, err)
	checkResults(seqs, []uint64{1, 2, 3})
	seqs, err = fs.MultiLastSeqs([]string{"foo.foo", "foo.bar", "foo.baz"}, 0, -1)
	require_NoError(t, err)
	checkResults(seqs, []uint64{97, 98, 99})
	seqs, err = fs.MultiLastSeqs([]string{"bar.foo", "bar.bar", "bar.baz"}, 0, -1)
	require_NoError(t, err)
	checkResults(seqs, []uint64{196, 197, 198})
	seqs, err = fs.MultiLastSeqs([]string{"bar.foo", "bar.bar", "bar.baz"}, 99, -1)
	require_NoError(t, err)
	checkResults(seqs, nil)

	// Check single works
	seqs, err = fs.MultiLastSeqs([]string{"foo.foo"}, 3, -1)
	require_NoError(t, err)
	checkResults(seqs, []uint64{1})

	// Now test that we properly de-duplicate between filters.
	seqs, err = fs.MultiLastSeqs([]string{"foo.*", "foo.bar"}, 3, -1)
	require_NoError(t, err)
	checkResults(seqs, []uint64{1, 2, 3})
	seqs, err = fs.MultiLastSeqs([]string{"bar.>", "bar.bar", "bar.baz"}, 0, -1)
	require_NoError(t, err)
	checkResults(seqs, []uint64{196, 197, 198})

	// All
	seqs, err = fs.MultiLastSeqs([]string{">"}, 0, -1)
	require_NoError(t, err)
	checkResults(seqs, []uint64{97, 98, 99, 196, 197, 198})
	seqs, err = fs.MultiLastSeqs([]string{">"}, 99, -1)
	require_NoError(t, err)
	checkResults(seqs, []uint64{97, 98, 99})
}

func TestFileStoreMultiLastSeqsMaxAllowed(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir()},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	msg := []byte("abc")
	for i := 1; i <= 100; i++ {
		fs.StoreMsg(fmt.Sprintf("foo.%d", i), nil, msg, 0)
	}
	// Test that if we specify maxAllowed that we get the correct error.
	seqs, err := fs.MultiLastSeqs([]string{"foo.*"}, 0, 10)
	require_True(t, seqs == nil)
	require_Error(t, err, ErrTooManyResults)
}

// https://github.com/nats-io/nats-server/issues/5236
// Unclear how the sequences get off here, this is just forcing the situation reported.
func TestFileStoreMsgBlockFirstAndLastSeqCorrupt(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir()},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	msg := []byte("abc")
	for i := 1; i <= 10; i++ {
		fs.StoreMsg(fmt.Sprintf("foo.%d", i), nil, msg, 0)
	}
	fs.Purge()

	fs.mu.RLock()
	mb := fs.blks[0]
	fs.mu.RUnlock()

	mb.mu.Lock()
	mb.tryForceExpireCacheLocked()
	atomic.StoreUint64(&mb.last.seq, 9)
	mb.mu.Unlock()

	// We should rebuild here and return no error.
	require_NoError(t, mb.loadMsgs())
	mb.mu.RLock()
	fseq, lseq := atomic.LoadUint64(&mb.first.seq), atomic.LoadUint64(&mb.last.seq)
	mb.mu.RUnlock()
	require_Equal(t, fseq, 11)
	require_Equal(t, lseq, 10)
}

func TestFileStoreWriteFullStateAfterPurgeEx(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir()},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	msg := []byte("abc")
	for i := 1; i <= 10; i++ {
		fs.StoreMsg(fmt.Sprintf("foo.%d", i), nil, msg, 0)
	}
	fs.RemoveMsg(8)
	fs.RemoveMsg(9)
	fs.RemoveMsg(10)

	n, err := fs.PurgeEx(">", 8, 0)
	require_NoError(t, err)
	require_Equal(t, n, 7)

	var ss StreamState
	fs.FastState(&ss)
	require_Equal(t, ss.FirstSeq, 11)
	require_Equal(t, ss.LastSeq, 10)

	// Make sure this does not reset our state due to skew with msg blocks.
	fs.writeFullState()
	fs.FastState(&ss)
	require_Equal(t, ss.FirstSeq, 11)
	require_Equal(t, ss.LastSeq, 10)
}

func TestFileStoreFSSExpire(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir(), BlockSize: 8192, CacheExpire: 1 * time.Second, SubjectStateExpire: time.Second},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, MaxMsgsPer: 1, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	msg := []byte("abc")
	for i := 1; i <= 1000; i++ {
		fs.StoreMsg(fmt.Sprintf("foo.%d", i), nil, msg, 0)
	}
	// Flush fss by hand, cache should be flushed as well.
	fs.mu.RLock()
	for _, mb := range fs.blks {
		mb.mu.Lock()
		mb.fss = nil
		mb.mu.Unlock()
	}
	fs.mu.RUnlock()

	fs.StoreMsg("foo.11", nil, msg, 0)
	time.Sleep(900 * time.Millisecond)
	// This should keep fss alive in the first block..
	// As well as cache itself due to remove activity.
	fs.StoreMsg("foo.22", nil, msg, 0)
	time.Sleep(300 * time.Millisecond)
	// Check that fss and the cache are still loaded.
	fs.mu.RLock()
	mb := fs.blks[0]
	fs.mu.RUnlock()
	mb.mu.RLock()
	cache, fss := mb.ecache.Value(), mb.fss
	mb.mu.RUnlock()
	require_True(t, fss != nil)
	require_True(t, cache != nil)
}

func TestFileStoreFSSExpireNumPending(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir(), BlockSize: 8192, CacheExpire: 1 * time.Second, SubjectStateExpire: 2 * time.Second},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*.*"}, MaxMsgsPer: 1, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	msg := []byte("abc")
	for i := 1; i <= 100_000; i++ {
		fs.StoreMsg(fmt.Sprintf("foo.A.%d", i), nil, msg, 0)
		fs.StoreMsg(fmt.Sprintf("foo.B.%d", i), nil, msg, 0)
	}
	// Flush fss by hand, cache should be flushed as well.
	fs.mu.RLock()
	for _, mb := range fs.blks {
		mb.mu.Lock()
		mb.fss = nil
		mb.mu.Unlock()
	}
	fs.mu.RUnlock()

	nb := fs.numMsgBlocks()
	// Now execute NumPending() such that we load lots of blocks and make sure fss do not expire.
	start := time.Now()
	n, _ := fs.NumPending(100_000, "foo.A.*", false)
	elapsed := time.Since(start)

	require_Equal(t, n, 50_000)
	// Make sure we did not force expire the fss. We would have loaded first half of blocks.
	var noFss bool
	last := nb/2 - 1
	fs.mu.RLock()
	for i, mb := range fs.blks {
		mb.mu.RLock()
		noFss = mb.fss == nil
		mb.mu.RUnlock()
		if noFss || i == last {
			break
		}
	}
	fs.mu.RUnlock()
	require_False(t, noFss)

	// Run again, make sure faster. This is consequence of fss being loaded now.
	start = time.Now()
	fs.NumPending(100_000, "foo.A.*", false)
	require_True(t, elapsed > 2*time.Since(start))

	// Now do with start past the mid-point.
	start = time.Now()
	fs.NumPending(150_000, "foo.B.*", false)
	elapsed = time.Since(start)
	time.Sleep(time.Second)
	start = time.Now()
	fs.NumPending(150_000, "foo.B.*", false)
	require_True(t, elapsed > time.Since(start))

	// Sleep enough so that all mb.fss should expire, which is 2s above.
	time.Sleep(4 * time.Second)
	fs.mu.RLock()
	for i, mb := range fs.blks {
		mb.mu.RLock()
		fss := mb.fss
		mb.mu.RUnlock()
		if fss != nil {
			fs.mu.RUnlock()
			t.Fatalf("Detected loaded fss for mb %d (size %d)", i, fss.Size())
		}
	}
	fs.mu.RUnlock()
}

// We want to ensure that recovery of deleted messages survives no index.db and compactions.
func TestFileStoreRecoverWithRemovesAndNoIndexDB(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd, BlockSize: 250},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	msg := []byte("abc")
	for i := 1; i <= 10; i++ {
		fs.StoreMsg(fmt.Sprintf("foo.%d", i), nil, msg, 0)
	}
	fs.RemoveMsg(1)
	fs.RemoveMsg(2)
	fs.RemoveMsg(8)

	var ss StreamState
	fs.FastState(&ss)
	require_Equal(t, ss.FirstSeq, 3)
	require_Equal(t, ss.LastSeq, 10)
	require_Equal(t, ss.Msgs, 7)

	// Compact last block.
	fs.mu.RLock()
	lmb := fs.lmb
	fs.mu.RUnlock()
	lmb.mu.Lock()
	lmb.compact()
	lmb.mu.Unlock()
	// Stop but remove index.db
	sfile := filepath.Join(sd, msgDir, streamStreamStateFile)
	fs.Stop()
	os.Remove(sfile)

	fs, err = newFileStore(
		FileStoreConfig{StoreDir: sd},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	fs.FastState(&ss)
	require_Equal(t, ss.FirstSeq, 3)
	require_Equal(t, ss.LastSeq, 10)
	require_Equal(t, ss.Msgs, 7)
}

func TestFileStoreReloadAndLoseLastSequence(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	for i := 0; i < 22; i++ {
		fs.SkipMsg()
	}

	// Restart 5 times.
	for i := 0; i < 5; i++ {
		fs.Stop()
		fs, err = newFileStore(
			FileStoreConfig{StoreDir: sd},
			StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
		require_NoError(t, err)
		defer fs.Stop()
		var ss StreamState
		fs.FastState(&ss)
		require_Equal(t, ss.FirstSeq, 23)
		require_Equal(t, ss.LastSeq, 22)
	}
}

func TestFileStoreReloadAndLoseLastSequenceWithSkipMsgs(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	// Make sure same works with SkipMsgs which can kick in from delete blocks to replicas.
	require_NoError(t, fs.SkipMsgs(0, 22))

	// Restart 5 times.
	for i := 0; i < 5; i++ {
		fs.Stop()
		fs, err = newFileStore(
			FileStoreConfig{StoreDir: sd},
			StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
		require_NoError(t, err)
		defer fs.Stop()
		var ss StreamState
		fs.FastState(&ss)
		require_Equal(t, ss.FirstSeq, 23)
		require_Equal(t, ss.LastSeq, 22)
	}
}

func TestFileStoreLoadLastWildcard(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd, BlockSize: 512},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	msg := []byte("hello")
	fs.StoreMsg("foo.22.baz", nil, msg, 0)
	fs.StoreMsg("foo.22.bar", nil, msg, 0)
	for i := 0; i < 1000; i++ {
		fs.StoreMsg("foo.11.foo", nil, msg, 0)
	}

	// Make sure we remove fss since that would mask the problem that we walk
	// all the blocks.
	fs.mu.RLock()
	for _, mb := range fs.blks {
		mb.mu.Lock()
		mb.fss = nil
		mb.mu.Unlock()
	}
	fs.mu.RUnlock()

	// Attempt to load the last msg using a wildcarded subject.
	sm, err := fs.LoadLastMsg("foo.22.*", nil)
	require_NoError(t, err)
	require_Equal(t, sm.seq, 2)

	// Make sure that we took advantage of psim meta data and only load one block.
	var cloads uint64
	fs.mu.RLock()
	for _, mb := range fs.blks {
		mb.mu.RLock()
		cloads += mb.cloads
		mb.mu.RUnlock()
	}
	fs.mu.RUnlock()
	require_Equal(t, cloads, 1)
}

func TestFileStoreLoadLastWildcardWithPresenceMultipleBlocks(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd, BlockSize: 64},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	// Make sure we have "foo.222.bar" in multiple blocks to show bug.
	fs.StoreMsg("foo.22.bar", nil, []byte("hello"), 0)
	fs.StoreMsg("foo.22.baz", nil, []byte("ok"), 0)
	fs.StoreMsg("foo.22.baz", nil, []byte("ok"), 0)
	fs.StoreMsg("foo.22.bar", nil, []byte("hello22"), 0)
	require_True(t, fs.numMsgBlocks() > 1)
	sm, err := fs.LoadLastMsg("foo.*.bar", nil)
	require_NoError(t, err)
	require_Equal(t, "hello22", string(sm.msg))
}

// We want to make sure that we update psim correctly on a miss.
func TestFileStoreFilteredPendingPSIMFirstBlockUpdate(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd, BlockSize: 512},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	// When PSIM detects msgs == 1 will catch up, so msgs needs to be > 1.
	// Then create a huge block gap.
	msg := []byte("hello")
	fs.StoreMsg("foo.baz", nil, msg, 0)
	for i := 0; i < 1000; i++ {
		fs.StoreMsg("foo.foo", nil, msg, 0)
	}
	// Bookend with 2 more foo.baz
	fs.StoreMsg("foo.baz", nil, msg, 0)
	fs.StoreMsg("foo.baz", nil, msg, 0)
	// Now remove first one.
	removed, err := fs.RemoveMsg(1)
	require_NoError(t, err)
	require_True(t, removed)
	// 84 blocks.
	require_Equal(t, fs.numMsgBlocks(), 84)
	fs.mu.RLock()
	psi, ok := fs.psim.Find([]byte("foo.baz"))
	fs.mu.RUnlock()
	require_True(t, ok)
	require_Equal(t, psi.total, 2)
	require_Equal(t, psi.fblk, 1)
	require_Equal(t, psi.lblk, 84)

	// No make sure that a call to numFilterPending which will initially walk all blocks if starting from seq 1 updates psi.
	var ss SimpleState
	fs.mu.RLock()
	fs.numFilteredPending("foo.baz", &ss)
	fs.mu.RUnlock()
	require_Equal(t, ss.Msgs, 2)
	require_Equal(t, ss.First, 1002)
	require_Equal(t, ss.Last, 1003)

	// Check psi was updated. This is done in separate go routine to acquire
	// the write lock now.
	checkFor(t, time.Second, 100*time.Millisecond, func() error {
		fs.mu.RLock()
		psi, ok = fs.psim.Find([]byte("foo.baz"))
		total, fblk, lblk := psi.total, psi.fblk, psi.lblk
		fs.mu.RUnlock()
		require_True(t, ok)
		require_Equal(t, total, 2)
		require_Equal(t, lblk, 84)
		if fblk != 84 {
			return fmt.Errorf("fblk should be 84, still %d", fblk)
		}
		return nil
	})
}

func TestFileStoreWildcardFilteredPendingPSIMFirstBlockUpdate(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd, BlockSize: 512},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	// When PSIM detects msgs == 1 will catch up, so msgs needs to be > 1.
	// Then create a huge block gap.
	msg := []byte("hello")
	fs.StoreMsg("foo.22.baz", nil, msg, 0)
	fs.StoreMsg("foo.22.bar", nil, msg, 0)
	for i := 0; i < 1000; i++ {
		fs.StoreMsg("foo.1.foo", nil, msg, 0)
	}
	// Bookend with 3 more, two foo.baz and two foo.bar.
	fs.StoreMsg("foo.22.baz", nil, msg, 0)
	fs.StoreMsg("foo.22.baz", nil, msg, 0)
	fs.StoreMsg("foo.22.bar", nil, msg, 0)
	fs.StoreMsg("foo.22.bar", nil, msg, 0)

	// Now remove first one for foo.bar and foo.baz.
	removed, err := fs.RemoveMsg(1)
	require_NoError(t, err)
	require_True(t, removed)
	removed, err = fs.RemoveMsg(2)
	require_NoError(t, err)
	require_True(t, removed)

	// 92 blocks.
	require_Equal(t, fs.numMsgBlocks(), 92)
	fs.mu.RLock()
	psi, ok := fs.psim.Find([]byte("foo.22.baz"))
	total, fblk, lblk := psi.total, psi.fblk, psi.lblk
	fs.mu.RUnlock()
	require_True(t, ok)
	require_Equal(t, total, 2)
	require_Equal(t, fblk, 1)
	require_Equal(t, lblk, 92)

	fs.mu.RLock()
	psi, ok = fs.psim.Find([]byte("foo.22.bar"))
	total, fblk, lblk = psi.total, psi.fblk, psi.lblk
	fs.mu.RUnlock()
	require_True(t, ok)
	require_Equal(t, total, 2)
	require_Equal(t, fblk, 1)
	require_Equal(t, lblk, 92)

	// No make sure that a call to numFilterPending which will initially walk all blocks if starting from seq 1 updates psi.
	var ss SimpleState
	fs.mu.RLock()
	fs.numFilteredPending("foo.22.*", &ss)
	fs.mu.RUnlock()
	require_Equal(t, ss.Msgs, 4)
	require_Equal(t, ss.First, 1003)
	require_Equal(t, ss.Last, 1006)

	// Check both psi were updated.
	checkFor(t, time.Second, 100*time.Millisecond, func() error {
		fs.mu.RLock()
		psi, ok = fs.psim.Find([]byte("foo.22.baz"))
		total, fblk, lblk = psi.total, psi.fblk, psi.lblk
		fs.mu.RUnlock()
		require_True(t, ok)
		require_Equal(t, total, 2)
		require_Equal(t, lblk, 92)
		if fblk != 92 {
			return fmt.Errorf("fblk should be 92, still %d", fblk)
		}
		return nil
	})

	checkFor(t, time.Second, 100*time.Millisecond, func() error {
		fs.mu.RLock()
		psi, ok = fs.psim.Find([]byte("foo.22.bar"))
		total, fblk, lblk = psi.total, psi.fblk, psi.lblk
		fs.mu.RUnlock()
		require_True(t, ok)
		require_Equal(t, total, 2)
		require_Equal(t, fblk, 92)
		if fblk != 92 {
			return fmt.Errorf("fblk should be 92, still %d", fblk)
		}
		return nil
	})
}

// Make sure if we only miss by one for fblk that we still update it.
func TestFileStoreFilteredPendingPSIMFirstBlockUpdateNextBlock(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd, BlockSize: 128},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	msg := []byte("hello")
	// Create 4 blocks, each block holds 2 msgs
	for i := 0; i < 4; i++ {
		fs.StoreMsg("foo.22.bar", nil, msg, 0)
		fs.StoreMsg("foo.22.baz", nil, msg, 0)
	}
	require_Equal(t, fs.numMsgBlocks(), 4)

	fetch := func(subj string) *psi {
		t.Helper()
		fs.mu.RLock()
		var info psi
		psi, ok := fs.psim.Find([]byte(subj))
		if ok && psi != nil {
			info = *psi
		}
		fs.mu.RUnlock()
		require_True(t, ok)
		return &info
	}

	psi := fetch("foo.22.bar")
	require_Equal(t, psi.total, 4)
	require_Equal(t, psi.fblk, 1)
	require_Equal(t, psi.lblk, 4)

	// Now remove first instance of "foo.22.bar"
	removed, err := fs.RemoveMsg(1)
	require_NoError(t, err)
	require_True(t, removed)

	// Call into numFilterePending(), we want to make sure it updates fblk.
	var ss SimpleState
	fs.mu.Lock()
	fs.numFilteredPending("foo.22.bar", &ss)
	fs.mu.Unlock()
	require_Equal(t, ss.Msgs, 3)
	require_Equal(t, ss.First, 3)
	require_Equal(t, ss.Last, 7)

	// Now make sure that we properly updated the psim entry.
	checkFor(t, time.Second, 100*time.Millisecond, func() error {
		psi = fetch("foo.22.bar")
		require_Equal(t, psi.total, 3)
		require_Equal(t, psi.lblk, 4)
		if psi.fblk != 2 {
			return fmt.Errorf("fblk should be 2, still %d", psi.fblk)
		}
		return nil
	})

	// Now make sure wildcard calls into also update blks.
	// First remove first "foo.22.baz" which will remove first block.
	removed, err = fs.RemoveMsg(2)
	require_NoError(t, err)
	require_True(t, removed)
	// Make sure 3 blks left.
	require_Equal(t, fs.numMsgBlocks(), 3)

	psi = fetch("foo.22.baz")
	require_Equal(t, psi.total, 3)
	require_Equal(t, psi.fblk, 1)
	require_Equal(t, psi.lblk, 4)

	// Now call wildcard version of numFilteredPending to make sure it clears.
	fs.mu.Lock()
	fs.numFilteredPending("foo.*.baz", &ss)
	fs.mu.Unlock()
	require_Equal(t, ss.Msgs, 3)
	require_Equal(t, ss.First, 4)
	require_Equal(t, ss.Last, 8)

	checkFor(t, time.Second, 100*time.Millisecond, func() error {
		psi = fetch("foo.22.baz")
		require_Equal(t, psi.total, 3)
		require_Equal(t, psi.lblk, 4)
		if psi.fblk != 2 {
			return fmt.Errorf("fblk should be 2, still %d", psi.fblk)
		}
		return nil
	})
}

func TestFileStoreLargeSparseMsgsDoNotLoadAfterLast(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd, BlockSize: 128},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	msg := []byte("hello")
	// Create 2 blocks with each, each block holds 2 msgs
	for i := 0; i < 2; i++ {
		fs.StoreMsg("foo.22.bar", nil, msg, 0)
		fs.StoreMsg("foo.22.baz", nil, msg, 0)
	}
	// Now create 8 more blocks with just baz. So no matches for these 8 blocks
	// for "foo.22.bar".
	for i := 0; i < 8; i++ {
		fs.StoreMsg("foo.22.baz", nil, msg, 0)
		fs.StoreMsg("foo.22.baz", nil, msg, 0)
	}
	require_Equal(t, fs.numMsgBlocks(), 10)

	// Remove all blk cache and fss.
	fs.mu.RLock()
	for _, mb := range fs.blks {
		mb.mu.Lock()
		mb.fss, mb.cache = nil, nil
		mb.mu.Unlock()
	}
	fs.mu.RUnlock()

	// "foo.22.bar" is at sequence 1 and 3.
	// Make sure if we do a LoadNextMsg() starting at 4 that we do not load
	// all the tail blocks.
	_, _, err = fs.LoadNextMsg("foo.*.bar", true, 4, nil)
	require_Error(t, err, ErrStoreEOF)

	// Now make sure we did not load fss and cache.
	var loaded int
	fs.mu.RLock()
	for _, mb := range fs.blks {
		mb.mu.RLock()
		if mb.cache != nil || mb.fss != nil {
			loaded++
		}
		mb.mu.RUnlock()
	}
	fs.mu.RUnlock()
	// We will load first block for starting seq 4, but no others should have loaded.
	require_Equal(t, loaded, 1)
}

func TestFileStoreCheckSkipFirstBlockBug(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd, BlockSize: 128},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	msg := []byte("hello")

	fs.StoreMsg("foo.BB.bar", nil, msg, 0)
	fs.StoreMsg("foo.BB.bar", nil, msg, 0)
	fs.StoreMsg("foo.AA.bar", nil, msg, 0)
	for i := 0; i < 5; i++ {
		fs.StoreMsg("foo.BB.bar", nil, msg, 0)
	}
	fs.StoreMsg("foo.AA.bar", nil, msg, 0)
	fs.StoreMsg("foo.AA.bar", nil, msg, 0)

	// Should have created 4 blocks.
	// BB BB | AA BB | BB BB | BB BB | AA AA
	require_Equal(t, fs.numMsgBlocks(), 5)

	fs.RemoveMsg(3)
	fs.RemoveMsg(4)

	// Second block should be gone now.
	// BB BB | -- -- | BB BB | BB BB | AA AA
	require_Equal(t, fs.numMsgBlocks(), 4)

	_, _, err = fs.LoadNextMsg("foo.AA.bar", false, 4, nil)
	require_NoError(t, err)
}

// https://github.com/nats-io/nats-server/issues/5702
func TestFileStoreTombstoneRbytes(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir(), BlockSize: 1024},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	// Block can hold 24 msgs.
	// So will fill one block and half of the other
	msg := []byte("hello")
	for i := 0; i < 34; i++ {
		fs.StoreMsg("foo.22", nil, msg, 0)
	}
	require_True(t, fs.numMsgBlocks() > 1)
	// Now delete second half of first block which will place tombstones in second blk.
	for seq := 11; seq <= 24; seq++ {
		fs.RemoveMsg(uint64(seq))
	}
	// Now check that rbytes has been properly accounted for in second block.
	fs.mu.RLock()
	blk := fs.blks[1]
	fs.mu.RUnlock()

	blk.mu.RLock()
	bytes, rbytes := blk.bytes, blk.rbytes
	blk.mu.RUnlock()
	require_True(t, rbytes > bytes)
}

func TestFileStoreMsgBlockShouldCompact(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir()},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	// 127 fit into a block.
	msg := bytes.Repeat([]byte("Z"), 64*1024)
	for i := 0; i < 190; i++ {
		fs.StoreMsg("foo.22", nil, msg, 0)
	}
	require_True(t, fs.numMsgBlocks() > 1)
	// Now delete second half of first block which will place tombstones in second blk.
	for seq := 64; seq <= 127; seq++ {
		fs.RemoveMsg(uint64(seq))
	}
	fs.mu.RLock()
	fblk := fs.blks[0]
	sblk := fs.blks[1]
	fs.mu.RUnlock()

	fblk.mu.RLock()
	bytes, rbytes := fblk.bytes, fblk.rbytes
	shouldCompact := fblk.shouldCompactInline()
	fblk.mu.RUnlock()
	// Should have tripped compaction already.
	require_Equal(t, bytes, rbytes)
	require_False(t, shouldCompact)

	sblk.mu.RLock()
	shouldCompact = sblk.shouldCompactInline()
	sblk.mu.RUnlock()
	require_False(t, shouldCompact)
}

func TestFileStoreCheckSkipFirstBlockNotLoadOldBlocks(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd, BlockSize: 128},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	msg := []byte("hello")

	fs.StoreMsg("foo.BB.bar", nil, msg, 0)
	fs.StoreMsg("foo.AA.bar", nil, msg, 0)
	for i := 0; i < 6; i++ {
		fs.StoreMsg("foo.BB.bar", nil, msg, 0)
	}
	fs.StoreMsg("foo.AA.bar", nil, msg, 0) // Sequence 9
	fs.StoreMsg("foo.AA.bar", nil, msg, 0) // Sequence 10

	for i := 0; i < 4; i++ {
		fs.StoreMsg("foo.BB.bar", nil, msg, 0)
	}

	// Should have created 7 blocks.
	// BB AA | BB BB | BB BB | BB BB | AA AA | BB BB | BB BB
	require_Equal(t, fs.numMsgBlocks(), 7)

	fs.RemoveMsg(1)
	fs.RemoveMsg(2)

	// First block should be gone now.
	// -- -- | BB BB | BB BB | BB BB | AA AA | BB BB | BB BB
	require_Equal(t, fs.numMsgBlocks(), 6)

	// Remove all blk cache and fss.
	fs.mu.RLock()
	for _, mb := range fs.blks {
		mb.mu.Lock()
		mb.fss, mb.cache = nil, nil
		mb.mu.Unlock()
	}
	fs.mu.RUnlock()

	// But this means that the psim still points fblk to block 1.
	// So when we try to load AA from near the end (last AA sequence), it will not find anything and will then
	// check if we can skip ahead, but in the process reload blocks 2, 3, 4 amd 5..
	// This can trigger for an up to date consumer near the end of the stream that gets a new pull request that will pop it out of msgWait
	// and it will call LoadNextMsg() like we do here with starting sequence of 11.
	_, _, err = fs.LoadNextMsg("foo.AA.bar", false, 11, nil)
	require_Error(t, err, ErrStoreEOF)

	// Now make sure we did not load fss and cache.
	var loaded int
	fs.mu.RLock()
	for _, mb := range fs.blks {
		mb.mu.RLock()
		if mb.cache != nil || mb.fss != nil {
			loaded++
		}
		mb.mu.RUnlock()
	}
	fs.mu.RUnlock()
	// We will load last block for starting seq 9, but no others should have loaded.
	require_Equal(t, loaded, 1)
}

func TestFileStoreSyncCompressOnlyIfDirty(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd, BlockSize: 256, SyncInterval: 250 * time.Millisecond},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	msg := []byte("hello")

	// 6 msgs per block.
	// Fill 2 blocks.
	for i := 0; i < 12; i++ {
		fs.StoreMsg("foo.BB", nil, msg, 0)
	}
	// Create third block with just one message in it.
	fs.StoreMsg("foo.BB", nil, msg, 0)

	// Should have created 3 blocks.
	require_Equal(t, fs.numMsgBlocks(), 3)

	// Now delete a bunch that will will fill up 3 block with tombstones.
	for _, seq := range []uint64{2, 3, 4, 5, 8, 9, 10, 11} {
		_, err = fs.RemoveMsg(seq)
		require_NoError(t, err)
	}
	// Now make sure we add 4/5th block so syncBlocks will try to compact.
	for i := 0; i < 6; i++ {
		fs.StoreMsg("foo.BB", nil, msg, 0)
	}
	require_Equal(t, fs.numMsgBlocks(), 5)

	// All should have compact set.
	fs.mu.Lock()
	// Only check first 3 blocks.
	for i := 0; i < 3; i++ {
		mb := fs.blks[i]
		mb.mu.Lock()
		shouldCompact := mb.shouldCompactSync()
		mb.mu.Unlock()
		if !shouldCompact {
			fs.mu.Unlock()
			t.Fatalf("Expected should compact to be true for %d, got false", mb.getIndex())
		}
	}
	fs.mu.Unlock()

	// Let sync run.
	time.Sleep(300 * time.Millisecond)

	// We want to make sure the last block, which is filled with tombstones and is not compactable, returns false now.
	fs.mu.Lock()
	for _, mb := range fs.blks {
		mb.mu.Lock()
		shouldCompact := mb.shouldCompactSync()
		mb.mu.Unlock()
		if shouldCompact {
			fs.mu.Unlock()
			t.Fatalf("Expected should compact to be false for %d, got true", mb.getIndex())
		}
	}
	fs.mu.Unlock()

	// Now remove some from block 3 and verify that compact is not suppressed.
	_, err = fs.RemoveMsg(13)
	require_NoError(t, err)

	fs.mu.Lock()
	mb := fs.blks[2] // block 3.
	mb.mu.Lock()
	noCompact := mb.noCompact
	mb.mu.Unlock()
	fs.mu.Unlock()
	// Verify that since we deleted a message we should be considered for compaction again in syncBlocks().
	require_False(t, noCompact)
}

// This test is for deleted interior message tracking after compaction from limits based deletes, meaning no tombstones.
// Bug was that dmap would not be properly be hydrated after the compact from rebuild. But we did so in populateGlobalInfo.
// So this is just to fix a bug in rebuildState tracking gaps after a compact.
func TestFileStoreDmapBlockRecoverAfterCompact(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd, BlockSize: 256},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage, MaxMsgsPer: 1})
	require_NoError(t, err)
	defer fs.Stop()

	msg := []byte("hello")

	// 6 msgs per block.
	// Fill the first block.
	for i := 1; i <= 6; i++ {
		fs.StoreMsg(fmt.Sprintf("foo.%d", i), nil, msg, 0)
	}
	require_Equal(t, fs.numMsgBlocks(), 1)

	// Now create holes in the first block via the max msgs per subject of 1.
	for i := 2; i < 6; i++ {
		fs.StoreMsg(fmt.Sprintf("foo.%d", i), nil, msg, 0)
	}
	require_Equal(t, fs.numMsgBlocks(), 2)
	// Compact and rebuild the first blk. Do not have it call indexCacheBuf which will fix it up.
	mb := fs.getFirstBlock()
	mb.mu.Lock()
	mb.compact()
	// Empty out dmap state.
	mb.dmap.Empty()
	ld, tombs, err := mb.rebuildStateLocked()
	dmap := mb.dmap.Clone()
	mb.mu.Unlock()

	require_NoError(t, err)
	require_Equal(t, ld, nil)
	require_Equal(t, len(tombs), 0)
	require_Equal(t, dmap.Size(), 4)
}

func TestFileStoreRestoreIndexWithMatchButLeftOverBlocks(t *testing.T) {
	sd := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: sd, BlockSize: 256},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage, MaxMsgsPer: 1})
	require_NoError(t, err)
	defer fs.Stop()

	msg := []byte("hello")

	// 6 msgs per block.
	// Fill the first 2 blocks.
	for i := 1; i <= 12; i++ {
		fs.StoreMsg(fmt.Sprintf("foo.%d", i), nil, msg, 0)
	}
	require_Equal(t, fs.numMsgBlocks(), 2)

	// We will now stop which will create the index.db file which will
	// match the last record exactly.
	sfile := filepath.Join(sd, msgDir, streamStreamStateFile)
	fs.Stop()

	// Grab it since we will put it back.
	buf, err := os.ReadFile(sfile)
	require_NoError(t, err)
	require_True(t, len(buf) > 0)

	// Now do an additional block, but with the MaxMsgsPer this will remove the first block,
	// but leave the second so on recovery will match the checksum for the last msg in second block.

	fs, err = newFileStore(
		FileStoreConfig{StoreDir: sd, BlockSize: 256},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage, MaxMsgsPer: 1})
	require_NoError(t, err)
	defer fs.Stop()

	for i := 1; i <= 6; i++ {
		fs.StoreMsg(fmt.Sprintf("foo.%d", i), nil, msg, 0)
	}

	// Grab correct state, we will use it to make sure we do the right thing.
	var state StreamState
	fs.FastState(&state)

	require_Equal(t, state.Msgs, 12)
	require_Equal(t, state.FirstSeq, 7)
	require_Equal(t, state.LastSeq, 18)
	// This will be block 2 and 3.
	require_Equal(t, fs.numMsgBlocks(), 2)

	fs.Stop()
	// Put old stream state back.
	require_NoError(t, os.WriteFile(sfile, buf, defaultFilePerms))

	fs, err = newFileStore(
		FileStoreConfig{StoreDir: sd, BlockSize: 256},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage, MaxMsgsPer: 1})
	require_NoError(t, err)
	defer fs.Stop()

	fs.FastState(&state)
	require_Equal(t, state.Msgs, 12)
	require_Equal(t, state.FirstSeq, 7)
	require_Equal(t, state.LastSeq, 18)
}

func TestFileStoreRestoreDeleteTombstonesExceedingMaxBlkSize(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 256
		fs, err := newFileStoreWithCreated(
			fcfg,
			StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage},
			time.Now(),
			prf(&fcfg),
			nil,
		)
		require_NoError(t, err)
		defer fs.Stop()

		n, err := fs.PurgeEx(_EMPTY_, 1_000_000_000, 0)
		require_NoError(t, err)
		require_Equal(t, n, 0)

		msg := []byte("hello")
		// 6 msgs per block with blk size 256.
		for i := 1; i <= 10_000; i++ {
			fs.StoreMsg(fmt.Sprintf("foo.%d", i), nil, msg, 0)
		}
		// Now delete msgs which will write tombstones.
		for seq := uint64(1_000_000_001); seq < 1_000_000_101; seq++ {
			removed, err := fs.RemoveMsg(seq)
			require_NoError(t, err)
			require_True(t, removed)
		}

		// Check last block and make sure the tombstones did not exceed blk size maximum.
		// Check to make sure no blocks exceed blk size.
		fs.mu.RLock()
		blks := append([]*msgBlock(nil), fs.blks...)
		lmb := fs.lmb
		fs.mu.RUnlock()

		var emptyBlks []*msgBlock
		for _, mb := range blks {
			mb.mu.RLock()
			bytes, rbytes := mb.bytes, mb.rbytes
			mb.mu.RUnlock()
			require_True(t, bytes < 256)
			require_True(t, rbytes < 256)
			if bytes == 0 && mb != lmb {
				emptyBlks = append(emptyBlks, mb)
			}
		}
		// Check each block such that it signals it can be compacted but if we attempt compact here nothing should change.
		for _, mb := range emptyBlks {
			mb.mu.Lock()
			mb.ensureRawBytesLoaded()
			bytes, rbytes, shouldCompact := mb.bytes, mb.rbytes, mb.shouldCompactSync()
			// Do the compact and make sure nothing changed.
			mb.compact()
			nbytes, nrbytes := mb.bytes, mb.rbytes
			mb.mu.Unlock()
			require_True(t, shouldCompact)
			require_Equal(t, bytes, nbytes)
			require_Equal(t, rbytes, nrbytes)
		}

		// Now remove first msg which will invalidate the tombstones since they will be < first sequence.
		removed, err := fs.RemoveMsg(1_000_000_000)
		require_NoError(t, err)
		require_True(t, removed)

		// Now simulate a syncBlocks call and make sure it cleans up the tombstones that are no longer relevant.
		fs.syncBlocks()
		for _, mb := range emptyBlks {
			mb.mu.Lock()
			mb.ensureRawBytesLoaded()
			index, bytes, rbytes := mb.index, mb.bytes, mb.rbytes
			mb.mu.Unlock()
			require_Equal(t, bytes, 0)
			require_Equal(t, rbytes, 0)
			// Also make sure we removed these blks all together.
			fs.mu.RLock()
			imb := fs.bim[index]
			fs.mu.RUnlock()
			require_True(t, imb == nil)
		}
	})
}

///////////////////////////////////////////////////////////////////////////
// Benchmarks
///////////////////////////////////////////////////////////////////////////

func Benchmark_FileStoreSelectMsgBlock(b *testing.B) {
	// We use small block size to create lots of blocks for this test.
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: b.TempDir(), BlockSize: 128},
		StreamConfig{Name: "zzz", Subjects: []string{"*"}, Storage: FileStorage})
	require_NoError(b, err)
	defer fs.Stop()

	subj, msg := "A", bytes.Repeat([]byte("ABC"), 33) // ~100bytes

	// Add in a bunch of blocks.
	for i := 0; i < 1000; i++ {
		fs.StoreMsg(subj, nil, msg, 0)
	}
	if fs.numMsgBlocks() < 1000 {
		b.Fatalf("Expected at least 1000 blocks, got %d", fs.numMsgBlocks())
	}

	fs.mu.RLock()
	defer fs.mu.RUnlock()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, mb := fs.selectMsgBlockWithIndex(1)
		if mb == nil {
			b.Fatalf("Expected a non-nil mb")
		}
	}
	b.StopTimer()
}

func Benchmark_FileStoreLoadNextMsgSameFilterAsStream(b *testing.B) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: b.TempDir()},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(b, err)
	defer fs.Stop()

	// Small om purpose.
	msg := []byte("ok")

	// Add in a bunch of msgs
	for i := 0; i < 100_000; i++ {
		subj := fmt.Sprintf("foo.%d", rand.Intn(1024))
		fs.StoreMsg(subj, nil, msg, 0)
	}

	b.ResetTimer()

	var smv StoreMsg
	for i := 0; i < b.N; i++ {
		// Needs to start at ~1 to show slowdown.
		_, _, err := fs.LoadNextMsg("foo.*", true, 10, &smv)
		require_NoError(b, err)
	}
}

func Benchmark_FileStoreLoadNextMsgLiteralSubject(b *testing.B) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: b.TempDir()},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(b, err)
	defer fs.Stop()

	// Small om purpose.
	msg := []byte("ok")

	// Add in a bunch of msgs
	for i := 0; i < 100_000; i++ {
		subj := fmt.Sprintf("foo.%d", rand.Intn(1024))
		fs.StoreMsg(subj, nil, msg, 0)
	}
	// This is the one we will try to match.
	fs.StoreMsg("foo.2222", nil, msg, 0)
	// So not last and we think we are done linear scan.
	fs.StoreMsg("foo.3333", nil, msg, 0)

	b.ResetTimer()

	var smv StoreMsg
	for i := 0; i < b.N; i++ {
		_, _, err := fs.LoadNextMsg("foo.2222", false, 10, &smv)
		require_NoError(b, err)
	}
}

func Benchmark_FileStoreLoadNextMsgNoMsgsFirstSeq(b *testing.B) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: b.TempDir(), BlockSize: 8192},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(b, err)
	defer fs.Stop()

	// Small om purpose.
	msg := []byte("ok")

	// Add in a bunch of msgs
	for i := 0; i < 1_000_000; i++ {
		fs.StoreMsg("foo.bar", nil, msg, 0)
	}

	b.ResetTimer()

	var smv StoreMsg
	for i := 0; i < b.N; i++ {
		// This should error with EOF
		_, _, err := fs.LoadNextMsg("foo.baz", false, 1, &smv)
		if err != ErrStoreEOF {
			b.Fatalf("Wrong error, expected EOF got %v", err)
		}
	}
}

func Benchmark_FileStoreLoadNextMsgNoMsgsNotFirstSeq(b *testing.B) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: b.TempDir(), BlockSize: 8192},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(b, err)
	defer fs.Stop()

	// Small om purpose.
	msg := []byte("ok")

	// Add in a bunch of msgs
	for i := 0; i < 1_000_000; i++ {
		fs.StoreMsg("foo.bar", nil, msg, 0)
	}

	b.ResetTimer()

	var smv StoreMsg
	for i := 0; i < b.N; i++ {
		// This should error with EOF
		// Make sure the sequence is not first seq of 1.
		_, _, err := fs.LoadNextMsg("foo.baz", false, 10, &smv)
		if err != ErrStoreEOF {
			b.Fatalf("Wrong error, expected EOF got %v", err)
		}
	}
}

func Benchmark_FileStoreLoadNextMsgVerySparseMsgsFirstSeq(b *testing.B) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: b.TempDir(), BlockSize: 8192},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(b, err)
	defer fs.Stop()

	// Small om purpose.
	msg := []byte("ok")

	// Add in a bunch of msgs
	for i := 0; i < 1_000_000; i++ {
		fs.StoreMsg("foo.bar", nil, msg, 0)
	}
	// Make last msg one that would match.
	fs.StoreMsg("foo.baz", nil, msg, 0)

	b.ResetTimer()

	var smv StoreMsg
	for i := 0; i < b.N; i++ {
		_, _, err := fs.LoadNextMsg("foo.baz", false, 1, &smv)
		require_NoError(b, err)
	}
}

func Benchmark_FileStoreLoadNextMsgVerySparseMsgsNotFirstSeq(b *testing.B) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: b.TempDir(), BlockSize: 8192},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(b, err)
	defer fs.Stop()

	// Small om purpose.
	msg := []byte("ok")

	// Add in a bunch of msgs
	for i := 0; i < 1_000_000; i++ {
		fs.StoreMsg("foo.bar", nil, msg, 0)
	}
	// Make last msg one that would match.
	fs.StoreMsg("foo.baz", nil, msg, 0)

	b.ResetTimer()

	var smv StoreMsg
	for i := 0; i < b.N; i++ {
		// Make sure not first seq.
		_, _, err := fs.LoadNextMsg("foo.baz", false, 10, &smv)
		require_NoError(b, err)
	}
}

func Benchmark_FileStoreLoadNextMsgVerySparseMsgsInBetween(b *testing.B) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: b.TempDir(), BlockSize: 8192},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(b, err)
	defer fs.Stop()

	// Small om purpose.
	msg := []byte("ok")

	// Make first msg one that would match as well.
	fs.StoreMsg("foo.baz", nil, msg, 0)
	// Add in a bunch of msgs
	for i := 0; i < 1_000_000; i++ {
		fs.StoreMsg("foo.bar", nil, msg, 0)
	}
	// Make last msg one that would match as well.
	fs.StoreMsg("foo.baz", nil, msg, 0)

	b.ResetTimer()

	var smv StoreMsg
	for i := 0; i < b.N; i++ {
		// Make sure not first seq.
		_, _, err := fs.LoadNextMsg("foo.baz", false, 2, &smv)
		require_NoError(b, err)
	}
}

func Benchmark_FileStoreLoadNextMsgVerySparseMsgsInBetweenWithWildcard(b *testing.B) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: b.TempDir()},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*.*"}, Storage: FileStorage})
	require_NoError(b, err)
	defer fs.Stop()

	// Small om purpose.
	msg := []byte("ok")

	// Make first msg one that would match as well.
	fs.StoreMsg("foo.1.baz", nil, msg, 0)
	// Add in a bunch of msgs.
	// We need to make sure we have a range of subjects that could kick in a linear scan.
	for i := 0; i < 1_000_000; i++ {
		subj := fmt.Sprintf("foo.%d.bar", rand.Intn(100_000)+2)
		fs.StoreMsg(subj, nil, msg, 0)
	}
	// Make last msg one that would match as well.
	fs.StoreMsg("foo.1.baz", nil, msg, 0)

	b.ResetTimer()

	var smv StoreMsg
	for i := 0; i < b.N; i++ {
		// Make sure not first seq.
		_, _, err := fs.LoadNextMsg("foo.*.baz", true, 2, &smv)
		require_NoError(b, err)
	}
}

func Benchmark_FileStoreLoadNextManySubjectsWithWildcardNearLastBlock(b *testing.B) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: b.TempDir()},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*.*"}, Storage: FileStorage})
	require_NoError(b, err)
	defer fs.Stop()

	// Small om purpose.
	msg := []byte("ok")

	// Make first msg one that would match as well.
	fs.StoreMsg("foo.1.baz", nil, msg, 0)
	// Add in a bunch of msgs.
	// We need to make sure we have a range of subjects that could kick in a linear scan.
	for i := 0; i < 1_000_000; i++ {
		subj := fmt.Sprintf("foo.%d.bar", rand.Intn(100_000)+2)
		fs.StoreMsg(subj, nil, msg, 0)
	}
	// Make last msg one that would match as well.
	fs.StoreMsg("foo.1.baz", nil, msg, 0)

	b.ResetTimer()

	var smv StoreMsg
	for i := 0; i < b.N; i++ {
		// Make sure not first seq.
		_, _, err := fs.LoadNextMsg("foo.*.baz", true, 999_990, &smv)
		require_NoError(b, err)
	}
}

func Benchmark_FileStoreLoadNextMsgVerySparseMsgsLargeTail(b *testing.B) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: b.TempDir()},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*.*"}, Storage: FileStorage})
	require_NoError(b, err)
	defer fs.Stop()

	// Small om purpose.
	msg := []byte("ok")

	// Make first msg one that would match as well.
	fs.StoreMsg("foo.1.baz", nil, msg, 0)
	// Add in a bunch of msgs.
	// We need to make sure we have a range of subjects that could kick in a linear scan.
	for i := 0; i < 1_000_000; i++ {
		subj := fmt.Sprintf("foo.%d.bar", rand.Intn(64_000)+2)
		fs.StoreMsg(subj, nil, msg, 0)
	}

	b.ResetTimer()

	var smv StoreMsg
	for i := 0; i < b.N; i++ {
		// Make sure not first seq.
		_, _, err := fs.LoadNextMsg("foo.*.baz", true, 2, &smv)
		require_Error(b, err, ErrStoreEOF)
	}
}

func Benchmark_FileStoreCreateConsumerStores(b *testing.B) {
	for _, syncAlways := range []bool{true, false} {
		b.Run(fmt.Sprintf("%v", syncAlways), func(b *testing.B) {
			fs, err := newFileStore(
				FileStoreConfig{StoreDir: b.TempDir(), SyncAlways: syncAlways},
				StreamConfig{Name: "zzz", Subjects: []string{"foo.*.*"}, Storage: FileStorage})
			require_NoError(b, err)
			defer fs.Stop()

			oconfig := ConsumerConfig{
				DeliverSubject: "d",
				FilterSubject:  "foo",
				AckPolicy:      AckAll,
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				oname := fmt.Sprintf("obs22_%d", i)
				ofs, err := fs.ConsumerStore(oname, &oconfig)
				require_NoError(b, err)
				require_NoError(b, ofs.Stop())
			}
		})
	}
}

func Benchmark_FileStoreSubjectStateConsistencyOptimizationPerf(b *testing.B) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: b.TempDir()},
		StreamConfig{Name: "TEST", Subjects: []string{"foo.*"}, Storage: FileStorage, MaxMsgsPer: 1},
	)
	require_NoError(b, err)
	defer fs.Stop()

	// Do R rounds of storing N messages.
	// MaxMsgsPer=1, so every unique subject that's placed only exists in the stream once.
	// If R=2, N=3 that means we'd place foo.0, foo.1, foo.2 in the first round, and the second
	// round we'd place foo.2, foo.1, foo.0, etc. This is intentional so that without any
	// optimizations we'd need to scan either 1 in the optimal case or N in the worst case.
	// Which is way more expensive than always knowing what the sequences are and it being O(1).
	r := max(2, b.N)
	n := 40_000
	b.ResetTimer()
	for i := 0; i < r; i++ {
		for j := 0; j < n; j++ {
			d := j
			if i%2 == 0 {
				d = n - j - 1
			}
			subject := fmt.Sprintf("foo.%d", d)
			_, _, err = fs.StoreMsg(subject, nil, nil, 0)
			require_NoError(b, err)
		}
	}
}

func TestFileStoreWriteFullStateDetectCorruptState(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir()},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	msg := []byte("abc")
	for i := 1; i <= 10; i++ {
		_, _, err = fs.StoreMsg(fmt.Sprintf("foo.%d", i), nil, msg, 0)
		require_NoError(t, err)
	}

	// Simulate a change in a message block not being reflected in the fs.
	mb := fs.selectMsgBlock(2)
	mb.mu.Lock()
	mb.msgs--
	mb.mu.Unlock()

	var ss StreamState
	fs.FastState(&ss)
	require_Equal(t, ss.FirstSeq, 1)
	require_Equal(t, ss.LastSeq, 10)
	require_Equal(t, ss.Msgs, 10)

	// Make sure we detect the corrupt state and rebuild.
	err = fs.writeFullState()
	require_Error(t, err, errCorruptState)

	fs.FastState(&ss)
	require_Equal(t, ss.FirstSeq, 1)
	require_Equal(t, ss.LastSeq, 10)
	require_Equal(t, ss.Msgs, 9)
}

func TestFileStoreRecoverFullStateDetectCorruptState(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir()},
		StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	msg := []byte("abc")
	for i := 1; i <= 10; i++ {
		_, _, err = fs.StoreMsg(fmt.Sprintf("foo.%d", i), nil, msg, 0)
		require_NoError(t, err)
	}

	err = fs.writeFullState()
	require_NoError(t, err)

	sfile := filepath.Join(fs.fcfg.StoreDir, msgDir, streamStreamStateFile)
	buf, err := os.ReadFile(sfile)
	require_NoError(t, err)
	// Update to an incorrect message count.
	binary.PutUvarint(buf[2:], 0)
	// Just append a corrected checksum to the end to make it pass the checks.
	fs.hh.Reset()
	fs.hh.Write(buf)
	buf = fs.hh.Sum(buf)
	err = os.WriteFile(sfile, buf, defaultFilePerms)
	require_NoError(t, err)

	err = fs.recoverFullState()
	require_Error(t, err, errCorruptState)
}

func TestFileStoreNumPendingMulti(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir()},
		StreamConfig{Name: "zzz", Subjects: []string{"ev.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	totalMsgs := 100_000
	totalSubjects := 10_000
	numFiltered := 5000
	startSeq := uint64(5_000 + rand.Intn(90_000))

	subjects := make([]string, 0, totalSubjects)
	for i := 0; i < totalSubjects; i++ {
		subjects = append(subjects, fmt.Sprintf("ev.%s", nuid.Next()))
	}

	// Put in 100k msgs with random subjects.
	msg := bytes.Repeat([]byte("ZZZ"), 333)
	for i := 0; i < totalMsgs; i++ {
		_, _, err = fs.StoreMsg(subjects[rand.Intn(totalSubjects)], nil, msg, 0)
		require_NoError(t, err)
	}

	// Now we want to do a calculate NumPendingMulti.
	filters := gsl.NewSublist[struct{}]()
	for filters.Count() < uint32(numFiltered) {
		filter := subjects[rand.Intn(totalSubjects)]
		if !filters.HasInterest(filter) {
			filters.Insert(filter, struct{}{})
		}
	}

	// Use new function.
	total, _ := fs.NumPendingMulti(startSeq, filters, false)

	// Check our results.
	var checkTotal uint64
	var smv StoreMsg
	for seq := startSeq; seq <= uint64(totalMsgs); seq++ {
		sm, err := fs.LoadMsg(seq, &smv)
		require_NoError(t, err)
		if filters.HasInterest(sm.subj) {
			checkTotal++
		}
	}
	require_Equal(t, total, checkTotal)
}

func TestFileStoreMessageTTL(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir()},
		StreamConfig{Name: "zzz", Subjects: []string{"test"}, Storage: FileStorage, AllowMsgTTL: true})
	require_NoError(t, err)
	defer fs.Stop()

	ttl := int64(1) // 1 second

	for i := 1; i <= 10; i++ {
		_, _, err = fs.StoreMsg("test", nil, nil, ttl)
		require_NoError(t, err)
	}

	var ss StreamState
	fs.FastState(&ss)
	require_Equal(t, ss.FirstSeq, 1)
	require_Equal(t, ss.LastSeq, 10)
	require_Equal(t, ss.Msgs, 10)

	time.Sleep(time.Second * 2)

	fs.FastState(&ss)
	require_Equal(t, ss.FirstSeq, 11)
	require_Equal(t, ss.LastSeq, 10)
	require_Equal(t, ss.Msgs, 0)
}

func TestFileStoreMessageTTLRestart(t *testing.T) {
	dir := t.TempDir()

	t.Run("BeforeRestart", func(t *testing.T) {
		fs, err := newFileStore(
			FileStoreConfig{StoreDir: dir},
			StreamConfig{Name: "zzz", Subjects: []string{"test"}, Storage: FileStorage, AllowMsgTTL: true})
		require_NoError(t, err)
		defer fs.Stop()

		ttl := int64(1) // 1 second

		for i := 1; i <= 10; i++ {
			_, _, err = fs.StoreMsg("test", nil, nil, ttl)
			require_NoError(t, err)
		}

		var ss StreamState
		fs.FastState(&ss)
		require_Equal(t, ss.FirstSeq, 1)
		require_Equal(t, ss.LastSeq, 10)
		require_Equal(t, ss.Msgs, 10)
	})

	t.Run("AfterRestart", func(t *testing.T) {
		fs, err := newFileStore(
			FileStoreConfig{StoreDir: dir},
			StreamConfig{Name: "zzz", Subjects: []string{"test"}, Storage: FileStorage, AllowMsgTTL: true})
		require_NoError(t, err)
		defer fs.Stop()

		var ss StreamState
		fs.FastState(&ss)
		require_Equal(t, ss.FirstSeq, 1)
		require_Equal(t, ss.LastSeq, 10)
		require_Equal(t, ss.Msgs, 10)

		time.Sleep(time.Second * 2)

		fs.FastState(&ss)
		require_Equal(t, ss.FirstSeq, 11)
		require_Equal(t, ss.LastSeq, 10)
		require_Equal(t, ss.Msgs, 0)
	})
}

func TestFileStoreMessageTTLRecovered(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	dir := t.TempDir()

	t.Run("BeforeRestart", func(t *testing.T) {
		fs, err := newFileStore(
			FileStoreConfig{StoreDir: dir, srv: s},
			StreamConfig{Name: "zzz", Subjects: []string{"test"}, Storage: FileStorage, AllowMsgTTL: true})
		require_NoError(t, err)
		defer fs.Stop()

		ttl := int64(1) // 1 second

		// Make the first message with no ttl.
		// This exposes a bug we had in recovery.
		_, _, err = fs.StoreMsg("test", nil, nil, 0)
		require_NoError(t, err)

		for i := 0; i < 9; i++ {
			// When the timed hash wheel state is deleted, the only way we can recover
			// the TTL is to look at the original message header, therefore the TTL
			// must be in the headers for this test to work.
			hdr := fmt.Appendf(nil, "NATS/1.0\r\n%s: %d\r\n", JSMessageTTL, ttl)
			_, _, err = fs.StoreMsg("test", hdr, nil, ttl)
			require_NoError(t, err)
		}

		var ss StreamState
		fs.FastState(&ss)
		require_Equal(t, ss.FirstSeq, 1)
		require_Equal(t, ss.LastSeq, 10)
		require_Equal(t, ss.Msgs, 10)
	})

	t.Run("AfterRestart", func(t *testing.T) {
		// Delete the timed hash wheel state so that we are forced to do a linear scan
		// of message blocks containing TTL'd messages.
		fn := filepath.Join(dir, msgDir, ttlStreamStateFile)
		require_NoError(t, os.RemoveAll(fn))

		fs, err := newFileStore(
			FileStoreConfig{StoreDir: dir, srv: s},
			StreamConfig{Name: "zzz", Subjects: []string{"test"}, Storage: FileStorage, AllowMsgTTL: true})
		require_NoError(t, err)
		defer fs.Stop()

		require_Equal(t, fs.numMsgBlocks(), 1)
		fs.mu.RLock()
		mb := fs.blks[0]
		fs.mu.RUnlock()
		mb.mu.RLock()
		ttls := mb.ttls
		mb.mu.RUnlock()

		require_Equal(t, ttls, 9)

		var ss StreamState
		fs.FastState(&ss)
		require_Equal(t, ss.FirstSeq, 1)
		require_Equal(t, ss.LastSeq, 10)
		require_Equal(t, ss.Msgs, 10)

		time.Sleep(time.Second * 2)

		fs.FastState(&ss)
		require_Equal(t, ss.FirstSeq, 1)
		require_Equal(t, ss.LastSeq, 10)
		require_Equal(t, ss.Msgs, 1)
	})
}

func TestFileStoreMessageTTLRecoveredSingleMessageWithoutStreamState(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	dir := t.TempDir()

	t.Run("BeforeRestart", func(t *testing.T) {
		fs, err := newFileStore(
			FileStoreConfig{StoreDir: dir, srv: s},
			StreamConfig{Name: "zzz", Subjects: []string{"test"}, Storage: FileStorage, AllowMsgTTL: true})
		require_NoError(t, err)
		defer fs.Stop()

		ttl := int64(1) // 1 second
		hdr := fmt.Appendf(nil, "NATS/1.0\r\n%s: %d\r\n", JSMessageTTL, ttl)
		_, _, err = fs.StoreMsg("test", hdr, nil, ttl)
		require_NoError(t, err)

		var ss StreamState
		fs.FastState(&ss)
		require_Equal(t, ss.FirstSeq, 1)
		require_Equal(t, ss.LastSeq, 1)
		require_Equal(t, ss.Msgs, 1)
	})

	t.Run("AfterRestart", func(t *testing.T) {
		// Delete the stream state file so that we need to rebuild.
		fn := filepath.Join(dir, msgDir, streamStreamStateFile)
		require_NoError(t, os.RemoveAll(fn))
		// Delete the timed hash wheel state so that we are forced to do a linear scan
		// of message blocks containing TTL'd messages.
		fn = filepath.Join(dir, msgDir, ttlStreamStateFile)
		require_NoError(t, os.RemoveAll(fn))

		fs, err := newFileStore(
			FileStoreConfig{StoreDir: dir, srv: s},
			StreamConfig{Name: "zzz", Subjects: []string{"test"}, Storage: FileStorage, AllowMsgTTL: true})
		require_NoError(t, err)
		defer fs.Stop()

		var ss StreamState
		fs.FastState(&ss)
		require_Equal(t, ss.FirstSeq, 1)
		require_Equal(t, ss.LastSeq, 1)
		require_Equal(t, ss.Msgs, 1)

		time.Sleep(time.Second * 2)

		fs.FastState(&ss)
		require_Equal(t, ss.FirstSeq, 2)
		require_Equal(t, ss.LastSeq, 1)
		require_Equal(t, ss.Msgs, 0)
	})
}

func TestFileStoreMessageTTLWriteTombstone(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	dir := t.TempDir()

	fs, err := newFileStore(
		FileStoreConfig{StoreDir: dir, srv: s},
		StreamConfig{Name: "zzz", Subjects: []string{"test"}, Storage: FileStorage, AllowMsgTTL: true})
	require_NoError(t, err)
	defer fs.Stop()

	ttl := int64(1)

	// When the timed hash wheel state is deleted, the only way we can recover
	// the TTL is to look at the original message header, therefore the TTL
	// must be in the headers for this test to work.
	hdr := fmt.Appendf(nil, "NATS/1.0\r\n%s: %d\r\n", JSMessageTTL, ttl)
	_, _, err = fs.StoreMsg("test", hdr, nil, ttl)
	require_NoError(t, err)

	// Publish another message, but without TTL.
	_, _, err = fs.StoreMsg("test", nil, nil, 0)
	require_NoError(t, err)

	var ss StreamState
	fs.FastState(&ss)
	require_Equal(t, ss.Msgs, 2)
	require_Equal(t, ss.FirstSeq, 1)
	require_Equal(t, ss.LastSeq, 2)

	checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
		if fs.State().FirstSeq == 1 {
			return errors.New("message not expired yet")
		}
		return nil
	})

	fs.FastState(&ss)
	require_Equal(t, ss.Msgs, 1)
	require_Equal(t, ss.FirstSeq, 2)
	require_Equal(t, ss.LastSeq, 2)

	fs.Stop()

	// Delete the stream state file so that we need to rebuild.
	// Should have written a tombstone so we can properly recover.
	fn := filepath.Join(dir, msgDir, streamStreamStateFile)
	require_NoError(t, os.RemoveAll(fn))

	fs, err = newFileStore(
		FileStoreConfig{StoreDir: dir, srv: s},
		StreamConfig{Name: "zzz", Subjects: []string{"test"}, Storage: FileStorage, AllowMsgTTL: true})
	require_NoError(t, err)
	defer fs.Stop()

	fs.FastState(&ss)
	require_Equal(t, ss.Msgs, 1)
	require_Equal(t, ss.FirstSeq, 2)
	require_Equal(t, ss.LastSeq, 2)
}

func TestFileStoreMessageTTLRecoveredOffByOne(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	dir := t.TempDir()

	fs, err := newFileStore(
		FileStoreConfig{StoreDir: dir, srv: s},
		StreamConfig{Name: "zzz", Subjects: []string{"test"}, Storage: FileStorage, AllowMsgTTL: true})
	require_NoError(t, err)
	defer fs.Stop()

	ts := time.Now().UnixNano()
	ttl := int64(120) // 2 minutes
	expires := time.Duration(ts) + (time.Second * time.Duration(ttl))

	// When the timed hash wheel state is deleted, the only way we can recover
	// the TTL is to look at the original message header, therefore the TTL
	// must be in the headers for this test to work.
	hdr := fmt.Appendf(nil, "NATS/1.0\r\n%s: %d\r\n", JSMessageTTL, ttl)
	require_NoError(t, fs.StoreRawMsg("test", hdr, nil, 1, ts, ttl))

	var ss StreamState
	fs.FastState(&ss)
	require_Equal(t, ss.Msgs, 1)
	require_Equal(t, ss.FirstSeq, 1)
	require_Equal(t, ss.LastSeq, 1)

	fs.mu.Lock()
	ttlc := fs.ttls.Count()
	// Adding to the THW is idempotent, so we sneakily remove it here
	// so we can check it doesn't get re-added during restart.
	err = fs.ttls.Remove(1, int64(expires))
	fs.mu.Unlock()
	require_Equal(t, ttlc, 1)
	require_NoError(t, err)

	fs.Stop()
	fs, err = newFileStore(
		FileStoreConfig{StoreDir: dir, srv: s},
		StreamConfig{Name: "zzz", Subjects: []string{"test"}, Storage: FileStorage, AllowMsgTTL: true})
	require_NoError(t, err)
	defer fs.Stop()

	fs.FastState(&ss)
	require_Equal(t, ss.Msgs, 1)
	require_Equal(t, ss.FirstSeq, 1)
	require_Equal(t, ss.LastSeq, 1)

	fs.mu.Lock()
	ttlc = fs.ttls.Count()
	fs.mu.Unlock()
	require_Equal(t, ttlc, 0)
}

func TestFileStoreDontSpamCompactWhenMostlyTombstones(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir(), BlockSize: defaultMediumBlockSize},
		StreamConfig{Name: "TEST", Subjects: []string{"foo"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	// Store a bunch of messages, ensuring we get enough blocks.
	msg := bytes.Repeat([]byte("X"), 100)
	totalBlksAfterStore := 6
	expectedLseq := uint64(31_536*(totalBlksAfterStore-1) + 2)
	for seq := uint64(1); seq <= expectedLseq; seq++ {
		_, _, err = fs.StoreMsg("foo", nil, msg, 0)
		require_NoError(t, err)
	}

	// Confirms we have the required amount of blocks.
	fs.mu.RLock()
	lenBlks := len(fs.blks)
	lmb := fs.lmb
	fs.mu.RUnlock()
	require_Len(t, lenBlks, totalBlksAfterStore)

	// Ensure the last block contains the last two messages.
	lmb.mu.RLock()
	fseq, lseq := lmb.first.seq, lmb.last.seq
	lmb.mu.RUnlock()
	require_Equal(t, fseq, expectedLseq-1)
	require_Equal(t, lseq, expectedLseq)

	// Remove all messages before the last block, will fill up last block with tombstones.
	for seq := uint64(1); seq < fseq; seq++ {
		removed, err := fs.RemoveMsg(seq)
		require_NoError(t, err)
		require_True(t, removed)
	}

	// The last block will be filled with so many tombstones that
	// a new message block will be created to store the rest.
	fs.mu.RLock()
	lenBlks = len(fs.blks)
	fs.mu.RUnlock()
	require_Len(t, lenBlks, 2)

	fs.mu.Lock()
	defer fs.mu.Unlock()

	fmb := fs.blks[0]
	fmb.mu.Lock()
	defer fmb.mu.Unlock()

	// We don't call fs.RemoveMsg as that would call into compact.
	// Instead, we mark as deleted and check compaction ourselves.
	fmb.dmap.Insert(expectedLseq)
	fmb.bytes /= 2

	// This message block takes up ~4MB but contains only one ~100 bytes message, and the rest is all tombstones.
	// We should allow trying to compact.
	require_Equal(t, fmb.bytes, 133)
	require_Equal(t, fmb.cbytes, 0)
	require_True(t, fmb.shouldCompactInline())

	// Compact will be successful, but since it doesn't clean up tombstones it will be ineffective.
	fmb.compact()

	// We should not allow compacting again as we're not removing tombstones inline.
	// Otherwise, we would spam compaction.
	require_False(t, fmb.shouldCompactInline())

	// Just checking fmb.cbytes is tracking the previous value of fmb.bytes,
	// so it can block compaction until enough data has been removed.
	require_Equal(t, fmb.bytes, 133)
	require_Equal(t, fmb.cbytes, fmb.bytes)

	// Simulate having removed sufficient bytes and being allowed to compact again.
	fmb.bytes /= 2
	require_True(t, fmb.shouldCompactInline())
}

func TestFileStoreSubjectDeleteMarkers(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir()},
		StreamConfig{
			Name: "zzz", Subjects: []string{"test"}, Storage: FileStorage,
			MaxAge: time.Second, AllowMsgTTL: true,
			SubjectDeleteMarkerTTL: time.Second,
		},
	)
	require_NoError(t, err)
	defer fs.Stop()

	// Capture subject delete marker proposals.
	ch := make(chan *inMsg, 1)
	fs.rmcb = func(seq uint64) {
		_, err := fs.RemoveMsg(seq)
		require_NoError(t, err)
	}
	fs.pmsgcb = func(im *inMsg) {
		ch <- im
	}

	// Store three messages that will expire because of MaxAge.
	for i := 0; i < 3; i++ {
		_, _, err = fs.StoreMsg("test", nil, nil, 0)
		require_NoError(t, err)
	}

	// Wait for MaxAge to pass.
	time.Sleep(time.Second + time.Millisecond*500)

	// We should have placed a subject delete marker.
	im := require_ChanRead(t, ch, time.Second*5)
	require_Equal(t, bytesToString(getHeader(JSMarkerReason, im.hdr)), JSMarkerReasonMaxAge)
	require_Equal(t, bytesToString(getHeader(JSMessageTTL, im.hdr)), "1s")
}

func TestFileStoreStoreRawMessageThrowsPermissionErrorIfFSModeReadOnly(t *testing.T) {
	// Test fails in Buildkite environment. Skip it.
	skipIfBuildkite(t)

	cfg := StreamConfig{Name: "zzz", Subjects: []string{"ev.1"}, Storage: FileStorage}
	fs, err := newFileStore(FileStoreConfig{StoreDir: t.TempDir(), BlockSize: 1024}, cfg)
	require_NoError(t, err)
	defer fs.Stop()

	READONLY_MODE := os.FileMode(0o555)
	ORIGINAL_FILE_MODE, err := os.Stat(fs.fcfg.StoreDir)
	require_NoError(t, err)
	require_NoError(t, changeDirectoryPermission(fs.fcfg.StoreDir, READONLY_MODE))
	defer func() {
		require_NoError(t, changeDirectoryPermission(fs.fcfg.StoreDir, ORIGINAL_FILE_MODE.Mode()))
	}()

	totalMsgs := 10000
	msg := bytes.Repeat([]byte("Z"), 1024)
	for i := 0; i < totalMsgs; i++ {
		if _, _, err = fs.StoreMsg("ev.1", nil, msg, 0); err != nil {
			break
		}
	}
	require_Error(t, err, os.ErrPermission)
}

func TestFileStoreWriteFullStateThrowsPermissionErrorIfFSModeReadOnly(t *testing.T) {
	// Test fails in Buildkite environment. Skip it.
	skipIfBuildkite(t)

	cfg := StreamConfig{Name: "zzz", Subjects: []string{"ev.1"}, Storage: FileStorage}
	fs, err := newFileStore(FileStoreConfig{StoreDir: t.TempDir()}, cfg)
	require_NoError(t, err)
	defer fs.Stop()

	READONLY_MODE := os.FileMode(0o555)
	ORIGINAL_FILE_MODE, err := os.Stat(fs.fcfg.StoreDir)
	require_NoError(t, err)

	totalMsgs := 10000
	msg := bytes.Repeat([]byte("Z"), 1024)
	for i := 0; i < totalMsgs; i++ {
		if _, _, err := fs.StoreMsg("ev.1", nil, msg, 0); err != nil {
			break
		}
	}

	require_NoError(t, changeDirectoryPermission(fs.fcfg.StoreDir, READONLY_MODE))
	require_Error(t, fs.writeFullState(), os.ErrPermission)
	require_NoError(t, changeDirectoryPermission(fs.fcfg.StoreDir, ORIGINAL_FILE_MODE.Mode()))
}

func changeDirectoryPermission(directory string, mode fs.FileMode) error {
	err := filepath.Walk(directory, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return fmt.Errorf("error accessing path %q: %w", path, err)
		}

		// Check if the path is a directory or file and set permissions accordingly
		if info.IsDir() {
			err = os.Chmod(path, mode)
			if err != nil {
				return fmt.Errorf("error changing directory permissions for %q: %w", path, err)
			}
		} else {
			err = os.Chmod(path, mode)
			if err != nil {
				return fmt.Errorf("error changing file permissions for %q: %w", path, err)
			}
		}
		return nil
	})
	return err
}

func TestFileStoreLeftoverSkipMsgInDmap(t *testing.T) {
	storeDir := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: storeDir},
		StreamConfig{Name: "zzz", Subjects: []string{"test.*"}, Storage: FileStorage, MaxMsgsPer: 1},
	)
	require_NoError(t, err)
	defer fs.Stop()

	getLmbState := func(fs *fileStore) (uint64, uint64, int) {
		fs.mu.RLock()
		lmb := fs.lmb
		fs.mu.RUnlock()
		lmb.mu.RLock()
		fseq := atomic.LoadUint64(&lmb.first.seq)
		lseq := atomic.LoadUint64(&lmb.last.seq)
		dmaps := lmb.dmap.Size()
		lmb.mu.RUnlock()
		return fseq, lseq, dmaps
	}

	// Only skip a message.
	fs.SkipMsg()

	// Confirm state.
	state := fs.State()
	require_Equal(t, state.FirstSeq, 2)
	require_Equal(t, state.LastSeq, 1)
	require_Equal(t, state.NumDeleted, 0)
	fseq, lseq, dmaps := getLmbState(fs)
	require_Equal(t, fseq, 2)
	require_Equal(t, lseq, 1)
	require_Len(t, dmaps, 0)

	// Stop without writing index.db so we recover based on just the blk file.
	require_NoError(t, fs.stop(false, false))

	fs, err = newFileStore(
		FileStoreConfig{StoreDir: storeDir},
		StreamConfig{Name: "zzz", Subjects: []string{"test.*"}, Storage: FileStorage, MaxMsgsPer: 1},
	)
	require_NoError(t, err)
	defer fs.Stop()

	// Confirm the skipped message is not included in the deletes.
	state = fs.State()
	require_Equal(t, state.FirstSeq, 2)
	require_Equal(t, state.LastSeq, 1)
	require_Equal(t, state.NumDeleted, 0)
	fseq, lseq, dmaps = getLmbState(fs)
	require_Equal(t, fseq, 2)
	require_Equal(t, lseq, 1)
	require_Len(t, dmaps, 0)
}

func TestFileStoreRecoverOnlyBlkFiles(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		_, _, err = fs.StoreMsg("foo", nil, nil, 0)
		require_NoError(t, err)

		// Confirm state as baseline.
		before := fs.State()
		require_Equal(t, before.Msgs, 1)
		require_Equal(t, before.FirstSeq, 1)
		require_Equal(t, before.LastSeq, 1)

		// Restart should equal state.
		require_NoError(t, fs.Stop())
		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if state := fs.State(); !reflect.DeepEqual(state, before) {
			t.Fatalf("Expected state of %+v, got %+v", before, state)
		}

		// Stream state should exist.
		_, err = os.Stat(filepath.Join(fs.fcfg.StoreDir, msgDir, streamStreamStateFile))
		require_NoError(t, err)

		// Stop and write some random files, but containing ".blk", should be ignored.
		require_NoError(t, fs.Stop())
		require_NoError(t, os.WriteFile(filepath.Join(fs.fcfg.StoreDir, msgDir, "10.blk.random"), nil, defaultFilePerms))
		require_NoError(t, os.WriteFile(filepath.Join(fs.fcfg.StoreDir, msgDir, fmt.Sprintf("10.blk.%s", blkTmpSuffix)), nil, defaultFilePerms))

		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// The random files would previously result in stream state to be deleted.
		_, err = os.Stat(filepath.Join(fs.fcfg.StoreDir, msgDir, streamStreamStateFile))
		require_NoError(t, err)

		if state := fs.State(); !reflect.DeepEqual(state, before) {
			t.Fatalf("Expected state of %+v, got %+v", before, state)
		}

		// Stop and remove stream state file.
		require_NoError(t, fs.Stop())
		require_NoError(t, os.Remove(filepath.Join(fs.fcfg.StoreDir, msgDir, streamStreamStateFile)))

		// Recovering based on blocks should also ignore the random files.
		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if state := fs.State(); !reflect.DeepEqual(state, before) {
			t.Fatalf("Expected state of %+v, got %+v", before, state)
		}
	})
}

func TestFileStoreRecoverAfterRemoveOperation(t *testing.T) {
	tests := []struct {
		title    string
		action   func(fs *fileStore)
		validate func(state StreamState)
	}{
		{
			title:  "None",
			action: func(fs *fileStore) {},
			validate: func(state StreamState) {
				require_Equal(t, state.Msgs, 4)
				require_Equal(t, state.FirstSeq, 1)
				require_Equal(t, state.LastSeq, 4)
			},
		},
		{
			title: "RemoveMsg",
			action: func(fs *fileStore) {
				removed, err := fs.RemoveMsg(1)
				require_NoError(t, err)
				require_True(t, removed)
			},
			validate: func(state StreamState) {
				require_Equal(t, state.Msgs, 3)
				require_Equal(t, state.FirstSeq, 2)
				require_Equal(t, state.LastSeq, 4)
			},
		},
		{
			title: "EraseMsg",
			action: func(fs *fileStore) {
				erased, err := fs.EraseMsg(1)
				require_NoError(t, err)
				require_True(t, erased)
			},
			validate: func(state StreamState) {
				require_Equal(t, state.Msgs, 3)
				require_Equal(t, state.FirstSeq, 2)
				require_Equal(t, state.LastSeq, 4)
			},
		},
		{
			title: "Purge",
			action: func(fs *fileStore) {
				purged, err := fs.Purge()
				require_NoError(t, err)
				require_Equal(t, purged, 4)
			},
			validate: func(state StreamState) {
				require_Equal(t, state.Msgs, 0)
				require_Equal(t, state.FirstSeq, 5)
				require_Equal(t, state.LastSeq, 4)
			},
		},
		{
			title: "PurgeEx-0",
			action: func(fs *fileStore) {
				purged, err := fs.PurgeEx("foo.0", 0, 0)
				require_NoError(t, err)
				require_Equal(t, purged, 2)
			},
			validate: func(state StreamState) {
				require_Equal(t, state.Msgs, 2)
				require_Equal(t, state.FirstSeq, 2)
				require_Equal(t, state.LastSeq, 4)
				require_Equal(t, state.NumDeleted, 1)
			},
		},
		{
			title: "PurgeEx-1",
			action: func(fs *fileStore) {
				purged, err := fs.PurgeEx("foo.1", 0, 0)
				require_NoError(t, err)
				require_Equal(t, purged, 2)
			},
			validate: func(state StreamState) {
				require_Equal(t, state.Msgs, 2)
				require_Equal(t, state.FirstSeq, 1)
				require_Equal(t, state.LastSeq, 4)
				require_Equal(t, state.NumDeleted, 2)
			},
		},
		{
			title: "Compact",
			action: func(fs *fileStore) {
				purged, err := fs.Compact(3)
				require_NoError(t, err)
				require_Equal(t, purged, 2)
			},
			validate: func(state StreamState) {
				require_Equal(t, state.Msgs, 2)
				require_Equal(t, state.FirstSeq, 3)
				require_Equal(t, state.LastSeq, 4)
			},
		},
		{
			title: "Truncate",
			action: func(fs *fileStore) {
				require_NoError(t, fs.Truncate(2))
			},
			validate: func(state StreamState) {
				require_Equal(t, state.Msgs, 2)
				require_Equal(t, state.FirstSeq, 1)
				require_Equal(t, state.LastSeq, 2)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.title, func(t *testing.T) {
			testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
				cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo.*"}, Storage: FileStorage}
				created := time.Now()
				fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
				require_NoError(t, err)
				defer fs.Stop()

				for i := 0; i < 4; i++ {
					subject := fmt.Sprintf("foo.%d", i%2)
					_, _, err = fs.StoreMsg(subject, nil, nil, 0)
					require_NoError(t, err)
				}

				test.action(fs)

				// Confirm state as baseline.
				before := fs.State()
				test.validate(before)

				// Restart should equal state.
				require_NoError(t, fs.Stop())
				fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
				require_NoError(t, err)
				defer fs.Stop()

				if state := fs.State(); !reflect.DeepEqual(state, before) {
					t.Fatalf("Expected state of:\n%+v, got:\n%+v", before, state)
				}

				// Stop and remove stream state file.
				require_NoError(t, fs.Stop())
				require_NoError(t, os.Remove(filepath.Join(fs.fcfg.StoreDir, msgDir, streamStreamStateFile)))

				// Recovering based on blocks should result in the same state.
				fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
				require_NoError(t, err)
				defer fs.Stop()

				if state := fs.State(); !reflect.DeepEqual(state, before) {
					t.Fatalf("Expected state of:\n%+v, got:\n%+v", before, state)
				}

				// Rebuilding state must also result in the same state.
				fs.rebuildState(nil)
				if state := fs.State(); !reflect.DeepEqual(state, before) {
					t.Fatalf("Expected state of:\n%+v, got:\n%+v", before, state)
				}
			})
		})
	}
}

func TestFileStoreRecoverAfterCompact(t *testing.T) {
	/*
		fs.Compact may rewrite the .blk file if it's large enough. In which case we don't
		need to write tombstones. But if the rewrite isn't done, tombstones must be placed
		to ensure we can properly recover without the index.db file.
	*/
	for _, test := range []struct {
		payloadSize    int
		usesTombstones bool
	}{
		{payloadSize: 1024, usesTombstones: true},
		{payloadSize: 1024 * 1024, usesTombstones: false},
	} {
		t.Run(strconv.Itoa(test.payloadSize), func(t *testing.T) {
			testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
				cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
				created := time.Now()
				fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
				require_NoError(t, err)
				defer fs.Stop()

				subject := "foo"
				payload := make([]byte, test.payloadSize)
				for i := 0; i < 4; i++ {
					_, _, err = fs.StoreMsg(subject, nil, payload, 0)
					require_NoError(t, err)
				}

				// Confirm state before compacting.
				before := fs.State()
				require_Equal(t, before.Msgs, 4)
				require_Equal(t, before.FirstSeq, 1)
				require_Equal(t, before.LastSeq, 4)
				require_Equal(t, before.Bytes, uint64(emptyRecordLen+len(subject)+test.payloadSize)*4)

				fs.mu.RLock()
				lmb := fs.lmb
				fs.mu.RUnlock()

				// Underlying message block should report the same.
				if fcfg.Cipher == NoCipher && fcfg.Compression == NoCompression {
					lmb.mu.RLock()
					bytes, rbytes := lmb.bytes, lmb.rbytes
					lmb.mu.RUnlock()
					size := uint64(emptyRecordLen+len(subject)+test.payloadSize) * 4
					require_Equal(t, bytes, size)
					require_Equal(t, rbytes, size)
				}

				// Now compact.
				purged, err := fs.Compact(4)
				require_NoError(t, err)
				require_Equal(t, purged, 3)

				// Confirm state after compacting.
				// Bytes should reflect only having a single message left.
				before = fs.State()
				require_Equal(t, before.Msgs, 1)
				require_Equal(t, before.FirstSeq, 4)
				require_Equal(t, before.LastSeq, 4)
				require_Equal(t, before.Bytes, uint64(emptyRecordLen+len(subject)+test.payloadSize))

				// Underlying message block should report the same.
				// Unless the compact didn't rewrite the block but used tombstones,
				// in which case we expect the raw bytes to include them.
				if fcfg.Cipher == NoCipher && fcfg.Compression == NoCompression {
					lmb.mu.RLock()
					bytes, rbytes := lmb.bytes, lmb.rbytes
					lmb.mu.RUnlock()
					size := uint64(emptyRecordLen + len(subject) + test.payloadSize)
					require_Equal(t, bytes, size)
					if test.usesTombstones {
						// 4 messages, 3 tombstones
						size = uint64(emptyRecordLen+len(subject)+test.payloadSize)*4 + uint64(emptyRecordLen)*3
					}
					require_Equal(t, rbytes, size)
				}

				// Restart should equal state.
				require_NoError(t, fs.Stop())
				fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
				require_NoError(t, err)
				defer fs.Stop()

				if state := fs.State(); !reflect.DeepEqual(state, before) {
					t.Fatalf("Expected state of:\n%+v, got:\n%+v", before, state)
				}

				// Stop and remove stream state file.
				require_NoError(t, fs.Stop())
				require_NoError(t, os.Remove(filepath.Join(fs.fcfg.StoreDir, msgDir, streamStreamStateFile)))

				// Recovering based on blocks should result in the same state.
				fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
				require_NoError(t, err)
				defer fs.Stop()

				if state := fs.State(); !reflect.DeepEqual(state, before) {
					t.Fatalf("Expected state of:\n%+v, got:\n%+v", before, state)
				}

				// Rebuilding state must also result in the same state.
				fs.rebuildState(nil)
				if state := fs.State(); !reflect.DeepEqual(state, before) {
					t.Fatalf("Expected state of:\n%+v, got:\n%+v", before, state)
				}
			})
		})
	}
}

func TestFileStoreRecoverWithEmptyMessageBlock(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		// 4 messages with subject 'foo' and no payload.
		fcfg.BlockSize = 33 * 4
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		// First message block contains 4 messages.
		for i := 0; i < 4; i++ {
			_, _, err = fs.StoreMsg("foo", nil, nil, 0)
			require_NoError(t, err)
		}

		fs.mu.RLock()
		lblks := len(fs.blks)
		fs.mu.RUnlock()
		require_Len(t, lblks, 1)

		// Second (empty) message block only contains 2 tombstones.
		for i := uint64(1); i <= 2; i++ {
			removed, err := fs.RemoveMsg(i)
			require_NoError(t, err)
			require_True(t, removed)
		}

		fs.mu.RLock()
		lblks = len(fs.blks)
		fs.mu.RUnlock()
		require_Len(t, lblks, 2)

		before := fs.State()
		require_Equal(t, before.Msgs, 2)
		require_Equal(t, before.FirstSeq, 3)
		require_Equal(t, before.LastSeq, 4)

		// Restart should equal state.
		require_NoError(t, fs.Stop())
		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if state := fs.State(); !reflect.DeepEqual(state, before) {
			t.Fatalf("Expected state of:\n%+v, got:\n%+v", before, state)
		}

		// Stop and remove stream state file.
		require_NoError(t, fs.Stop())
		require_NoError(t, os.Remove(filepath.Join(fs.fcfg.StoreDir, msgDir, streamStreamStateFile)))

		// Recovering based on blocks should result in the same state.
		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		if state := fs.State(); !reflect.DeepEqual(state, before) {
			t.Fatalf("Expected state of:\n%+v, got:\n%+v", before, state)
		}

		// Rebuilding state must also result in the same state.
		fs.rebuildState(nil)
		if state := fs.State(); !reflect.DeepEqual(state, before) {
			t.Fatalf("Expected state of:\n%+v, got:\n%+v", before, state)
		}
	})
}

func TestFileStoreRemoveMsgBlockFirst(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	dir := t.TempDir()

	fs, err := newFileStore(
		FileStoreConfig{StoreDir: dir, srv: s},
		StreamConfig{Name: "zzz", Subjects: []string{"test"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	_, _, err = fs.StoreMsg("test", nil, nil, 0)
	require_NoError(t, err)

	var ss StreamState
	fs.FastState(&ss)
	require_Equal(t, ss.Msgs, 1)
	require_Equal(t, ss.FirstSeq, 1)
	require_Equal(t, ss.LastSeq, 1)

	fs.Stop()

	for _, f := range []string{streamStreamStateFile, "1.blk"} {
		fn := filepath.Join(dir, msgDir, f)
		require_NoError(t, os.RemoveAll(fn))
	}

	fs, err = newFileStore(
		FileStoreConfig{StoreDir: dir, srv: s},
		StreamConfig{Name: "zzz", Subjects: []string{"test"}, Storage: FileStorage, AllowMsgTTL: true})
	require_NoError(t, err)
	defer fs.Stop()

	// If the block is removed first, we have nothing to recover. So starting out empty would be expected.
	fs.FastState(&ss)
	require_Equal(t, ss.Msgs, 0)
	require_Equal(t, ss.FirstSeq, 0)
	require_Equal(t, ss.LastSeq, 0)
}

func TestFileStoreRemoveMsgBlockLast(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	dir := t.TempDir()

	fs, err := newFileStore(
		FileStoreConfig{StoreDir: dir, srv: s},
		StreamConfig{Name: "zzz", Subjects: []string{"test"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	_, _, err = fs.StoreMsg("test", nil, nil, 0)
	require_NoError(t, err)

	var ss StreamState
	fs.FastState(&ss)
	require_Equal(t, ss.Msgs, 1)
	require_Equal(t, ss.FirstSeq, 1)
	require_Equal(t, ss.LastSeq, 1)

	// Copy first block so we can put it back later.
	ofn := filepath.Join(dir, msgDir, "1.blk")
	nfn := filepath.Join(dir, msgDir, "1.blk.cp")
	require_NoError(t, os.Rename(ofn, nfn))

	// Removing the last message will result in '2.blk' to be created, and '1.blk' to be removed.
	_, err = fs.RemoveMsg(1)
	require_NoError(t, err)
	_, err = os.Stat(filepath.Join(dir, msgDir, "2.blk"))
	require_NoError(t, err)
	_, err = os.Stat(ofn)
	require_True(t, os.IsNotExist(err))

	fs.Stop()

	// Remove index.db so we need to recover based on blocks.
	fn := filepath.Join(dir, msgDir, streamStreamStateFile)
	require_NoError(t, os.RemoveAll(fn))

	// Put back '1.blk' file to simulate being hard killed right
	// after creating '2.blk' but before cleaning up '1.blk'.
	require_NoError(t, os.Rename(nfn, ofn))
	_, err = os.Stat(ofn)
	require_False(t, os.IsNotExist(err))

	fs, err = newFileStore(
		FileStoreConfig{StoreDir: dir, srv: s},
		StreamConfig{Name: "zzz", Subjects: []string{"test"}, Storage: FileStorage, AllowMsgTTL: true})
	require_NoError(t, err)
	defer fs.Stop()

	// Should recognize correct state, and remove '1.blk'.
	fs.FastState(&ss)
	require_Equal(t, ss.Msgs, 0)
	require_Equal(t, ss.FirstSeq, 2)
	require_Equal(t, ss.LastSeq, 1)
	_, err = os.Stat(ofn)
	require_True(t, os.IsNotExist(err))
}

func TestFileStoreAllLastSeqs(t *testing.T) {
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir()},
		StreamConfig{Name: "zzz", Subjects: []string{"*.*"}, MaxMsgsPer: 50, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	subjs := []string{"foo.foo", "foo.bar", "foo.baz", "bar.foo", "bar.bar", "bar.baz"}
	msg := []byte("abc")

	for i := 0; i < 100_000; i++ {
		subj := subjs[rand.Intn(len(subjs))]
		fs.StoreMsg(subj, nil, msg, 0)
	}

	expected := make([]uint64, 0, len(subjs))
	var smv StoreMsg
	for _, subj := range subjs {
		sm, err := fs.LoadLastMsg(subj, &smv)
		require_NoError(t, err)
		expected = append(expected, sm.seq)
	}
	slices.Sort(expected)

	seqs, err := fs.AllLastSeqs()
	require_NoError(t, err)
	require_True(t, reflect.DeepEqual(seqs, expected))
}

func TestFileStoreRecoverDoesNotResetStreamState(t *testing.T) {
	cfg := StreamConfig{Name: "zzz", Subjects: []string{"ev.1"}, Storage: FileStorage, MaxAge: 5 * time.Second, Retention: WorkQueuePolicy}
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir()},
		cfg)

	require_NoError(t, err)
	defer fs.Stop()

	subj, msg := "foo", []byte("Hello World")
	toStore := 500
	for i := 0; i < toStore; i++ {
		_, _, err := fs.StoreMsg(subj, nil, msg, 0)
		require_NoError(t, err)
	}
	time.Sleep(5 * time.Second)
	fs, err = newFileStoreWithCreated(fs.fcfg, cfg, time.Now(), prf(&fs.fcfg), nil) //Expire all messages so stream does not hold any message, this is to simulate consumer consuming all messages.
	require_NoError(t, err)
	require_NoError(t, fs.Stop())      //To Ensure there is a state file created
	require_True(t, len(fs.blks) == 1) //Since all messages are expire there should be only 1 blk file exist
	os.Remove(fs.blks[0].mfn)          // we can change it to have a consumer and consumer all messages too, but removing blk files will simulate same behavior

	//Now at this point stream has only index.db file and no blk files as all are deleted. previously it used to reset the stream state to 0
	// now it will use index.db to populate stream state if could not be recovered from blk files.
	fs, err = newFileStoreWithCreated(fs.fcfg, cfg, time.Now(), prf(&fs.fcfg), nil)
	require_NoError(t, err)
	require_True(t, fs.state.FirstSeq|fs.state.LastSeq != 0)
}

func TestFileStoreAccessTimeSpinUp(t *testing.T) {
	// In case running lots of tests.
	time.Sleep(time.Second)
	ngr := runtime.NumGoroutine()

	fs, err := newFileStore(
		FileStoreConfig{StoreDir: t.TempDir()},
		StreamConfig{Name: "zzz", Subjects: []string{"*.*"}, Storage: FileStorage})
	require_NoError(t, err)
	defer fs.Stop()

	at := ats.AccessTime()
	require_True(t, at != 0)

	// Now check we also cleanup.
	fs.Stop()
	time.Sleep(2 * ats.TickInterval)
	ngra := runtime.NumGoroutine()
	require_Equal(t, ngr, ngra)
}

func TestFileStoreUpdateConfigTTLState(t *testing.T) {
	cfg := StreamConfig{
		Name:     "zzz",
		Subjects: []string{">"},
		Storage:  FileStorage,
	}
	fs, err := newFileStore(FileStoreConfig{StoreDir: t.TempDir()}, cfg)
	require_NoError(t, err)
	defer fs.Stop()
	require_Equal(t, fs.ttls, nil)

	cfg.AllowMsgTTL = true
	require_NoError(t, fs.UpdateConfig(&cfg))
	require_NotEqual(t, fs.ttls, nil)

	cfg.AllowMsgTTL = false
	require_NoError(t, fs.UpdateConfig(&cfg))
	require_Equal(t, fs.ttls, nil)
}

func TestFileStoreSubjectForSeq(t *testing.T) {
	cfg := StreamConfig{
		Name:     "foo",
		Subjects: []string{"foo.>"},
		Storage:  FileStorage,
	}
	fs, err := newFileStore(FileStoreConfig{StoreDir: t.TempDir()}, cfg)
	require_NoError(t, err)
	defer fs.Stop()

	seq, _, err := fs.StoreMsg("foo.bar", nil, nil, 0)
	require_NoError(t, err)
	require_Equal(t, seq, 1)

	_, err = fs.SubjectForSeq(0)
	require_Error(t, err, ErrStoreMsgNotFound)

	subj, err := fs.SubjectForSeq(1)
	require_NoError(t, err)
	require_Equal(t, subj, "foo.bar")

	_, err = fs.SubjectForSeq(2)
	require_Error(t, err, ErrStoreMsgNotFound)
}

func BenchmarkFileStoreSubjectAccesses(b *testing.B) {
	fs, err := newFileStore(FileStoreConfig{StoreDir: b.TempDir()}, StreamConfig{
		Name:     "foo",
		Subjects: []string{"foo.>"},
		Storage:  FileStorage,
	})
	require_NoError(b, err)
	defer fs.Stop()

	seq, _, err := fs.StoreMsg("foo.bar", nil, []byte{1, 2, 3, 4, 5}, 0)
	require_NoError(b, err)
	require_Equal(b, seq, 1)

	b.Run("SubjectForSeq", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			subj, err := fs.SubjectForSeq(1)
			require_NoError(b, err)
			require_Equal(b, subj, "foo.bar")
		}
	})

	b.Run("LoadMsg", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			// smv is deliberately inside the loop here because that's
			// effectively what is happening with needAck.
			var smv StoreMsg
			sm, err := fs.LoadMsg(1, &smv)
			require_NoError(b, err)
			require_Equal(b, sm.subj, "foo.bar")
		}
	})
}

func TestFileStoreFirstMatchingMultiExpiry(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo.>"}, Storage: FileStorage}
		fs, err := newFileStoreWithCreated(fcfg, cfg, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		_, _, err = fs.StoreMsg("foo.foo", nil, []byte("A"), 0)
		require_NoError(t, err)

		_, _, err = fs.StoreMsg("foo.foo", nil, []byte("B"), 0)
		require_NoError(t, err)

		_, _, err = fs.StoreMsg("foo.foo", nil, []byte("C"), 0)
		require_NoError(t, err)

		fs.mu.RLock()
		mb := fs.lmb
		mb.expireCacheLocked()
		fs.mu.RUnlock()

		sl := gsl.NewSublist[struct{}]()
		sl.Insert("foo.foo", struct{}{})

		_, didLoad, err := mb.firstMatchingMulti(sl, 1, nil)
		require_NoError(t, err)
		require_False(t, didLoad)

		_, didLoad, err = mb.firstMatchingMulti(sl, 2, nil)
		require_NoError(t, err)
		require_False(t, didLoad)

		_, didLoad, err = mb.firstMatchingMulti(sl, 3, nil)
		require_NoError(t, err)
		require_True(t, didLoad) // last message, should expire
	})
}

func TestFileStoreNoPanicOnRecoverTTLWithCorruptBlocks(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage, AllowMsgTTL: true}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		hdr := genHeader(nil, JSMessageTTL, "1")
		for i := range 3 {
			if i > 0 {
				_, err = fs.newMsgBlockForWrite()
				require_NoError(t, err)
			}
			_, _, err = fs.StoreMsg("foo", hdr, []byte("A"), 1)
			require_NoError(t, err)
		}

		fs.mu.Lock()
		if blks := len(fs.blks); blks != 3 {
			fs.mu.Unlock()
			t.Fatalf("Expected 3 blocks, got %d", blks)
		}

		// Manually corrupt the blocks by removing the second and changing the
		// sequence range for the last to that of the first.
		fmb := fs.blks[0]
		smb := fs.blks[1]
		lmb := fs.lmb
		fseq, lseq := atomic.LoadUint64(&fmb.first.seq), atomic.LoadUint64(&fmb.last.seq)
		smb.mu.Lock()
		fs.removeMsgBlock(smb)
		smb.mu.Unlock()
		fs.mu.Unlock()
		atomic.StoreUint64(&lmb.first.seq, fseq)
		atomic.StoreUint64(&lmb.last.seq, lseq)

		require_NoError(t, fs.recoverTTLState())
	})
}

func TestFileStoreAsyncTruncate(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		fcfg.BlockSize = 8192
		fcfg.AsyncFlush = true

		fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		fs.mu.RLock()
		lmb := fs.lmb
		fs.mu.RUnlock()
		require_NotNil(t, lmb)

		// Wait for flusher to be ready.
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			lmb.mu.RLock()
			defer lmb.mu.RUnlock()
			if !lmb.flusher {
				return errors.New("flusher not active")
			}
			return nil
		})
		// Now shutdown flusher and wait for it to be closed.
		lmb.mu.Lock()
		if lmb.qch != nil {
			close(lmb.qch)
			lmb.qch = nil
		}
		lmb.mu.Unlock()
		checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
			lmb.mu.RLock()
			defer lmb.mu.RUnlock()
			if lmb.flusher {
				return errors.New("flusher still active")
			}
			return nil
		})

		// Write some messages, none of them will have been flushed asynchronously.
		subj, msg := "foo", make([]byte, 100)
		for i := uint64(1); i <= 2; i++ {
			seq, _, err := fs.StoreMsg(subj, nil, msg, 0)
			require_NoError(t, err)
			require_Equal(t, seq, i)
		}
		// Truncate needs to flush if the data was not yet flushed asynchronously.
		require_NoError(t, fs.Truncate(1))

		state := fs.State()
		require_Equal(t, state.Msgs, 1)
		require_Equal(t, state.FirstSeq, 1)
		require_Equal(t, state.LastSeq, 1)

		fs.mu.RLock()
		for _, mb := range fs.blks {
			if mb.pendingWriteSize() > 0 {
				fs.mu.RUnlock()
				t.Fatalf("Message block %d still has pending writes", mb.index)
			}
		}
		fs.mu.RUnlock()
	})
}

func TestFileStoreAsyncFlushOnSkipMsgs(t *testing.T) {
	for _, noFlushLoop := range []bool{false, true} {
		t.Run(fmt.Sprintf("NoFlushLoop=%v", noFlushLoop), func(t *testing.T) {
			testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
				fcfg.BlockSize = 8192
				fcfg.AsyncFlush = true

				fs, err := newFileStoreWithCreated(fcfg, StreamConfig{Name: "zzz", Storage: FileStorage}, time.Now(), prf(&fcfg), nil)
				require_NoError(t, err)
				defer fs.Stop()

				fs.mu.RLock()
				fmb := fs.lmb
				fs.mu.RUnlock()
				require_NotNil(t, fmb)

				if noFlushLoop {
					// Wait for flusher to be ready.
					checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
						fmb.mu.RLock()
						defer fmb.mu.RUnlock()
						if !fmb.flusher {
							return errors.New("flusher not active")
						}
						return nil
					})
					// Now shutdown flusher and wait for it to be closed.
					fmb.mu.Lock()
					if fmb.qch != nil {
						close(fmb.qch)
						fmb.qch = nil
					}
					fmb.mu.Unlock()
					checkFor(t, 2*time.Second, 200*time.Millisecond, func() error {
						fmb.mu.RLock()
						defer fmb.mu.RUnlock()
						if fmb.flusher {
							return errors.New("flusher still active")
						}
						return nil
					})
				}

				// Confirm no pending writes.
				require_Equal(t, fmb.pendingWriteSize(), 0)

				_, _, err = fs.StoreMsg("foo", nil, nil, 0)
				require_NoError(t, err)

				if noFlushLoop {
					// Confirm above write is pending.
					require_Equal(t, fmb.pendingWriteSize(), 33)
				}

				require_NoError(t, fs.SkipMsgs(2, 100_000))
				fs.mu.RLock()
				if blks := len(fs.blks); blks != 2 {
					fs.mu.RUnlock()
					t.Fatalf("Expected 2 blocks, got %d", blks)
				}
				lmb := fs.blks[1]
				fs.mu.RUnlock()

				// Should have immediately flushed the previous block.
				require_Equal(t, fmb.pendingWriteSize(), 0)

				// Should eventually flush the last block.
				checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
					if p := lmb.pendingWriteSize(); p > 0 {
						return fmt.Errorf("expected no pending writes, got %d", p)
					}
					return nil
				})
			})
		})
	}
}

func TestFileStoreCompressionAfterTruncate(t *testing.T) {
	tests := []struct {
		title  string
		action func(fs *fileStore, seq uint64)
	}{
		{
			title: "RemoveMsg",
			action: func(fs *fileStore, seq uint64) {
				removed, err := fs.RemoveMsg(seq)
				require_NoError(t, err)
				require_True(t, removed)
			},
		},
		{
			title: "EraseMsg",
			action: func(fs *fileStore, seq uint64) {
				erased, err := fs.EraseMsg(seq)
				require_NoError(t, err)
				require_True(t, erased)
			},
		},
		{
			title: "Tombstone",
			action: func(fs *fileStore, seq uint64) {
				removed, err := fs.removeMsg(seq, false, false, true)
				require_NoError(t, err)
				require_True(t, removed)
			},
		},
	}
	for _, test := range tests {
		for _, recompress := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/Recompress=%v", test.title, recompress), func(t *testing.T) {
				testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
					cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
					created := time.Now()
					fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
					require_NoError(t, err)
					defer fs.Stop()

					for range 2 {
						_, _, err = fs.StoreMsg("foo", nil, nil, 0)
						require_NoError(t, err)
					}

					checkCompressed := func(mb *msgBlock) (bool, error) {
						mb.mu.Lock()
						defer mb.mu.Unlock()
						buf, err := mb.loadBlock(nil)
						if err != nil {
							return false, err
						}
						if err = mb.encryptOrDecryptIfNeeded(buf); err != nil {
							return false, err
						}
						var meta CompressionInfo
						if n, err := meta.UnmarshalMetadata(buf); err != nil {
							return false, err
						} else if n == 0 {
							return false, nil
						} else {
							return meta.Algorithm != NoCompression, nil
						}
					}

					smb := fs.getFirstBlock()
					require_NotNil(t, smb)
					compressed, err := checkCompressed(smb)
					require_NoError(t, err)
					require_False(t, compressed)

					_, err = fs.newMsgBlockForWrite()
					require_NoError(t, err)

					if fcfg.Compression != NoCompression {
						checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
							if compressed, err = checkCompressed(smb); err != nil {
								return err
							} else if !compressed {
								return errors.New("block not compressed yet")
							}
							return nil
						})
					} else {
						compressed, err = checkCompressed(smb)
						require_NoError(t, err)
						require_False(t, compressed)
					}

					_, _, err = fs.StoreMsg("foo", nil, nil, 0)
					require_NoError(t, err)

					test.action(fs, 2)

					state := fs.State()
					require_Equal(t, state.Msgs, 2)
					require_Equal(t, state.FirstSeq, 1)
					require_Equal(t, state.LastSeq, 3)
					require_Equal(t, state.NumDeleted, 1)

					require_NoError(t, fs.Truncate(2))
					state = fs.State()
					require_Equal(t, state.Msgs, 1)
					require_Equal(t, state.FirstSeq, 1)
					require_Equal(t, state.LastSeq, 2)
					require_Equal(t, state.NumDeleted, 1)

					fs.mu.RLock()
					lmb := fs.lmb
					fs.mu.RUnlock()
					if smb == lmb {
						compressed, err = checkCompressed(smb)
						require_NoError(t, err)
						require_False(t, compressed)
					} else {
						compressed, err = checkCompressed(lmb)
						require_NoError(t, err)
						require_False(t, compressed)

						if fcfg.Compression != NoCompression {
							checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
								if compressed, err = checkCompressed(smb); err != nil {
									return err
								} else if !compressed {
									return errors.New("block not compressed yet")
								}
								return nil
							})
						}
					}

					require_NoError(t, fs.forceWriteFullState())

					seq, _, err := fs.StoreMsg("foo", nil, nil, 0)
					require_NoError(t, err)
					require_Equal(t, seq, 3)
					require_NoError(t, fs.forceWriteFullState())

					smb.mu.Lock()
					smb.clearCacheAndOffset()
					smb.mu.Unlock()

					require_NoError(t, smb.loadMsgsWithLock())
					compressed, err = checkCompressed(smb)
					require_NoError(t, err)
					if smb == lmb {
						require_False(t, compressed)
					} else {
						require_Equal(t, compressed, fcfg.Compression != NoCompression)
					}

					compressed, err = checkCompressed(lmb)
					require_NoError(t, err)
					require_False(t, compressed)
				})
			})
		}
	}
}

func TestFileStoreTruncateRemovedBlock(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		for i := range 3 {
			if i > 0 {
				_, err = fs.newMsgBlockForWrite()
				require_NoError(t, err)
			}
			_, _, err = fs.StoreMsg("foo", nil, nil, 0)
			require_NoError(t, err)
		}

		fs.mu.RLock()
		blks := len(fs.blks)
		fs.mu.RUnlock()
		require_Len(t, blks, 3)

		state := fs.State()
		require_Equal(t, state.Msgs, 3)
		require_Equal(t, state.FirstSeq, 1)
		require_Equal(t, state.LastSeq, 3)
		require_Equal(t, state.NumDeleted, 0)

		removed, err := fs.RemoveMsg(2)
		require_NoError(t, err)
		require_True(t, removed)

		fs.mu.RLock()
		blks = len(fs.blks)
		fs.mu.RUnlock()
		require_Len(t, blks, 2)

		fs.mu.RLock()
		blks = len(fs.blks)
		fs.mu.RUnlock()
		require_Len(t, blks, 2)

		state = fs.State()
		require_Equal(t, state.Msgs, 2)
		require_Equal(t, state.FirstSeq, 1)
		require_Equal(t, state.LastSeq, 3)
		require_Equal(t, state.NumDeleted, 1)

		require_NoError(t, fs.Truncate(2))
		state = fs.State()
		require_Equal(t, state.Msgs, 1)
		require_Equal(t, state.FirstSeq, 1)
		require_Equal(t, state.LastSeq, 2)
		require_Equal(t, state.NumDeleted, 1)
	})
}

func TestFileStoreAtomicEraseMsg(t *testing.T) {
	for _, lmb := range []bool{true, false} {
		t.Run(fmt.Sprintf("lmb=%v", lmb), func(t *testing.T) {
			testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
				cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
				created := time.Now()
				fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
				require_NoError(t, err)
				defer fs.Stop()

				for range 3 {
					_, _, err = fs.StoreMsg("foo", nil, nil, 0)
					require_NoError(t, err)
				}

				checkCompressed := func(mb *msgBlock) (bool, error) {
					mb.mu.Lock()
					defer mb.mu.Unlock()
					buf, err := mb.loadBlock(nil)
					if err != nil {
						return false, err
					}
					if err := mb.checkAndLoadEncryption(); err != nil {
						return false, err
					}
					if err = mb.encryptOrDecryptIfNeeded(buf); err != nil {
						return false, err
					}
					var meta CompressionInfo
					if n, err := meta.UnmarshalMetadata(buf); err != nil {
						return false, err
					} else if n == 0 {
						return false, nil
					} else {
						return meta.Algorithm != NoCompression, nil
					}
				}

				if !lmb {
					smb := fs.getFirstBlock()
					require_NotNil(t, smb)

					mb, err := fs.newMsgBlockForWrite()
					require_NoError(t, err)
					compressed, err := checkCompressed(mb)
					require_NoError(t, err)
					require_False(t, compressed)

					if fcfg.Compression != NoCompression {
						checkFor(t, 2*time.Second, 100*time.Millisecond, func() error {
							if compressed, err = checkCompressed(smb); err != nil {
								return err
							} else if !compressed {
								return errors.New("block not compressed yet")
							}
							return nil
						})
					}
				}

				before := fs.State()
				require_Equal(t, before.Msgs, 3)
				require_Equal(t, before.FirstSeq, 1)
				require_Equal(t, before.LastSeq, 3)
				require_Equal(t, before.NumDeleted, 0)

				removed, err := fs.EraseMsg(2)
				require_NoError(t, err)
				require_True(t, removed)

				before = fs.State()
				require_Equal(t, before.Msgs, 2)
				require_Equal(t, before.FirstSeq, 1)
				require_Equal(t, before.LastSeq, 3)
				require_Equal(t, before.NumDeleted, 1)

				seq, _, err := fs.StoreMsg("foo", nil, nil, 0)
				require_NoError(t, err)
				require_Equal(t, seq, 4)
				before = fs.State()

				validateCompressed := func() {
					t.Helper()
					fs.mu.Lock()
					defer fs.mu.Unlock()
					for _, mb := range fs.blks {
						compressed, err := checkCompressed(mb)
						require_NoError(t, err)
						if mb == fs.lmb {
							require_False(t, compressed)
						} else {
							require_Equal(t, compressed, fs.fcfg.Compression != NoCompression)
						}
					}
				}
				validateCompressed()

				// Restart should equal before.
				require_NoError(t, fs.Stop())
				fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
				require_NoError(t, err)
				defer fs.Stop()

				if state := fs.State(); !reflect.DeepEqual(state, before) {
					t.Fatalf("Expected before of:\n%+v, got:\n%+v", before, state)
				}
				validateCompressed()

				// Stop and remove stream before file.
				require_NoError(t, fs.Stop())
				require_NoError(t, os.Remove(filepath.Join(fs.fcfg.StoreDir, msgDir, streamStreamStateFile)))

				// Recovering based on blocks should result in the same before.
				fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
				require_NoError(t, err)
				defer fs.Stop()

				if state := fs.State(); !reflect.DeepEqual(state, before) {
					t.Fatalf("Expected before of:\n%+v, got:\n%+v", before, state)
				}
				validateCompressed()

				// Rebuilding before must also result in the same before.
				fs.rebuildState(nil)
				if state := fs.State(); !reflect.DeepEqual(state, before) {
					t.Fatalf("Expected before of:\n%+v, got:\n%+v", before, state)
				}
				validateCompressed()
			})
		})
	}
}

func TestFileStoreRemoveBlockWithStaleStreamState(t *testing.T) {
	testFileStoreAllPermutations(t, func(t *testing.T, fcfg FileStoreConfig) {
		cfg := StreamConfig{Name: "zzz", Subjects: []string{"foo"}, Storage: FileStorage}
		created := time.Now()
		fs, err := newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		for i := range 3 {
			if i > 0 {
				_, err = fs.newMsgBlockForWrite()
				require_NoError(t, err)
			}
			_, _, err = fs.StoreMsg("foo", nil, nil, 0)
			require_NoError(t, err)
		}

		// Get middle block.
		fs.mu.RLock()
		require_Len(t, len(fs.blks), 3)
		midfn := fs.blks[1].mfn
		fs.mu.RUnlock()

		require_NoError(t, fs.Stop())
		require_NoError(t, os.Remove(midfn))

		// Restart.
		fs, err = newFileStoreWithCreated(fcfg, cfg, created, prf(&fcfg), nil)
		require_NoError(t, err)
		defer fs.Stop()

		for i := range 3 {
			seq := uint64(i + 1)
			_, err = fs.LoadMsg(seq, nil)
			if seq == 2 {
				require_Error(t, err, ErrStoreMsgNotFound)
			} else {
				require_NoError(t, err)
			}
		}
	})
}

func TestFileStoreMessageSchedule(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	dir := t.TempDir()
	fs, err := newFileStore(
		FileStoreConfig{StoreDir: dir, srv: s},
		StreamConfig{Name: "TEST", Subjects: []string{"foo.*"}, Storage: FileStorage, AllowMsgSchedules: true})
	require_NoError(t, err)
	defer fs.Stop()

	// Capture message schedule proposals.
	ch := make(chan *inMsg, 1)
	fs.pmsgcb = func(im *inMsg) {
		ch <- im
	}

	// Store a single message schedule.
	schedule := time.Now().Add(time.Second).Format(time.RFC3339Nano)
	hdr := genHeader(nil, JSSchedulePattern, fmt.Sprintf("@at %s", schedule))
	hdr = genHeader(hdr, JSScheduleTarget, "foo.target")
	_, _, err = fs.StoreMsg("foo.schedule", hdr, nil, 0)
	require_NoError(t, err)

	// We should have published a scheduled message.
	im := require_ChanRead(t, ch, time.Second*5)
	require_Equal(t, im.subj, "foo.target")
	require_Equal(t, bytesToString(getHeader(JSScheduler, im.hdr)), "foo.schedule")
	require_Equal(t, bytesToString(getHeader(JSScheduleNext, im.hdr)), JSScheduleNextPurge)
}

func TestFileStoreMessageScheduleRecovered(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	dir := t.TempDir()
	t.Run("BeforeRestart", func(t *testing.T) {
		fs, err := newFileStore(
			FileStoreConfig{StoreDir: dir, srv: s},
			StreamConfig{Name: "TEST", Subjects: []string{"foo.*"}, Storage: FileStorage, AllowMsgSchedules: true})
		require_NoError(t, err)
		defer fs.Stop()

		schedule := time.Now().Add(time.Second).Format(time.RFC3339Nano)
		hdr := genHeader(nil, JSSchedulePattern, fmt.Sprintf("@at %s", schedule))
		hdr = genHeader(hdr, JSScheduleTarget, "foo.target")
		_, _, err = fs.StoreMsg("foo.schedule", hdr, nil, 0)
		require_NoError(t, err)

		var ss StreamState
		fs.FastState(&ss)
		require_Equal(t, ss.FirstSeq, 1)
		require_Equal(t, ss.LastSeq, 1)
		require_Equal(t, ss.Msgs, 1)
	})

	t.Run("AfterRestart", func(t *testing.T) {
		// Delete the message scheduling state so that we are forced to do a linear scan
		// of message blocks containing message schedules.
		fn := filepath.Join(dir, msgDir, msgSchedulingStreamStateFile)
		require_NoError(t, os.Remove(fn))

		fs, err := newFileStore(
			FileStoreConfig{StoreDir: dir, srv: s},
			StreamConfig{Name: "TEST", Subjects: []string{"foo.*"}, Storage: FileStorage, AllowMsgSchedules: true})
		require_NoError(t, err)
		defer fs.Stop()

		require_Equal(t, fs.numMsgBlocks(), 1)
		fs.mu.RLock()
		mb := fs.blks[0]
		fs.mu.RUnlock()
		mb.mu.RLock()
		schedules := mb.schedules
		mb.mu.RUnlock()

		require_Equal(t, schedules, 1)

		var ss StreamState
		fs.FastState(&ss)
		require_Equal(t, ss.FirstSeq, 1)
		require_Equal(t, ss.LastSeq, 1)
		require_Equal(t, ss.Msgs, 1)
	})
}

func TestFileStoreMessageScheduleEncodeDecode(t *testing.T) {
	ms := newMsgScheduling(func() {})
	now := time.Now()

	// Add many sequences.
	numSequences := 100_000
	for seq := 0; seq < numSequences; seq++ {
		ts := now.Add(time.Duration(seq) * time.Second).UnixNano()
		subj := fmt.Sprintf("foo.%d", seq)
		ms.add(uint64(seq), subj, ts)
	}

	b := ms.encode(12345)
	require_True(t, len(b) > 17) // Bigger than just the header

	nms := newMsgScheduling(func() {})
	stamp, err := nms.decode(b)
	require_NoError(t, err)
	require_Equal(t, stamp, 12345)
	require_Equal(t, ms.ttls.GetNextExpiration(math.MaxInt64), nms.ttls.GetNextExpiration(math.MaxInt64))

	require_Len(t, len(ms.seqToSubj), len(nms.seqToSubj))
	for seq, subj := range ms.seqToSubj {
		require_Equal(t, subj, nms.seqToSubj[seq])
	}

	require_Len(t, len(ms.schedules), len(nms.schedules))
	for subj, sched := range ms.schedules {
		nsched := nms.schedules[subj]
		require_NotNil(t, nsched)
		require_Equal(t, sched.ts, nsched.ts)
		require_Equal(t, sched.seq, nsched.seq)
	}
}
