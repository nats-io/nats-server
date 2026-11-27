// Copyright 2012-2026 The NATS Authors
// Copyright 2026 JEMES contributors
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

//go:build windows

package server

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"golang.org/x/sys/windows"
	"golang.org/x/sys/windows/svc"
)

// TestWinServiceWrapper reproduces the tests for service,
// with extra complication for windows API
func TestWinServiceWrapper(t *testing.T) {
	/*
		Since the windows API can't be tested through just the Run func,
		a basic mock checks the intractions of the server, as happens in svc.Run.
		This test checks :
		- that the service fails to start within an unreasonable timeframe (serverC)
		- that the service signals its state correctly to windows with svc.StartPending (mockC)
		- that no other signal is sent to the windows service API(mockC)
	*/
	var (
		wsw  = &winServiceWrapper{New(DefaultOptions())}
		args = make([]string, 0)
		// Size of 1 should be enough but don't want to block if the test fails.
		serverC = make(chan error, 10)
		mockC   = make(chan error, 10)
		changes = make(chan svc.ChangeRequest)
		status  = make(chan svc.Status)
	)
	t.Setenv("NATS_STARTUP_DELAY", "1ns") // purposely small
	// prepare mock expectations
	wsm := &winSvcMock{status: status}
	wsm.Expect(svc.StartPending)

	go func() {
		mockC <- wsm.Listen(250 * time.Millisecond)
	}()

	go func() {
		var err error
		_, exitCode := wsw.Execute(args, changes, status)
		// We expect an error...
		if exitCode == 0 {
			err = errors.New("Should have exitCode != 0")
		}
		serverC <- err
	}()

	checkErr := func(c chan error, txt string) {
		select {
		case err := <-c:
			if err != nil {
				t.Fatalf("%s: %v", txt, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Test timed out")
		}
	}
	checkErr(mockC, "windows.svc mock")
	checkErr(serverC, "server behavior")

	wsw.server.Shutdown()
}

func TestWinServiceLDMExit(t *testing.T) {
	var (
		wsw  = &winServiceWrapper{New(DefaultOptions())}
		args = make([]string, 0)
		// Size of 1 should be enough but don't want to block if the test fails.
		serverC = make(chan error, 10)
		mockC   = make(chan error, 10)
		changes = make(chan svc.ChangeRequest, 1)
		status  = make(chan svc.Status)
	)
	// prepare mock expectations
	wsm := &winSvcMock{status: status}
	wsm.Expect(svc.StartPending)
	// Expect that we will be running
	wsm.Expect(svc.Running)
	// Sending the LDM signal
	changes <- svc.ChangeRequest{Cmd: svc.Cmd(ldmCode)}
	// Expect to be stopping
	wsm.Expect(svc.StopPending)

	go func() {
		// Duration long enough for the test to complete, but not too long
		// to wait for nothing.
		mockC <- wsm.Listen(time.Second)
	}()

	go func() {
		var err error
		if _, exitCode := wsw.Execute(args, changes, status); exitCode != 0 {
			err = fmt.Errorf("exited with %v", exitCode)
		}
		serverC <- err
	}()

	checkErr := func(c chan error, txt string) {
		select {
		case err := <-c:
			if err != nil {
				t.Fatalf("%s: %v", txt, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Test timed out")
		}
	}
	checkErr(mockC, "windows.svc mock")
	checkErr(serverC, "server behavior")

	// At this point, we have proven already that the LDM signal did
	// end the NATS server's Execute() loop because we received the
	// svc.StopPending status. Still, we will verify that we get
	// notified that the server has shutdown.
	ch := make(chan struct{}, 1)
	go func() {
		wsw.server.WaitForShutdown()
		ch <- struct{}{}
	}()
	select {
	case <-ch:
		// OK
	case <-time.After(time.Second):
		t.Fatal("Did not finish shutdown")
	}
}

// winSvcMock mocks part of the golang.org/x/sys/windows/svc
// execution stack, listening to svc.Status on its chan.
type winSvcMock struct {
	status     chan svc.Status
	expectedSt []svc.State
}

// Expect allows to prepare a winSvcMock to receive a specific type of StatusMessage
func (w *winSvcMock) Expect(st svc.State) {
	w.expectedSt = append(w.expectedSt, st)
}

// Listen is the mock's mainloop, expects messages to comply with previous Expect().
func (w *winSvcMock) Listen(dur time.Duration) error {
	timeout := time.NewTimer(dur)
	defer timeout.Stop()
	for _, state := range w.expectedSt {
		select {
		case status := <-w.status:
			if status.State != state {
				return fmt.Errorf("message to winsock: expected %v, got %v", state, status.State)
			}
		case <-timeout.C:
			return errors.New("Mock timed out")
		}
	}
	select {
	case <-timeout.C:
		return nil
	case st := <-w.status:
		return fmt.Errorf("extra message to winsock: got %v", st)

	}
}

// Each test also joins its workers when an assertion fails while the file is locked.
func runConsumerTestIO(t *testing.T, release func(), op func() error) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		defer close(done)
		done <- op()
	}()
	t.Cleanup(func() {
		release()
		<-done
	})
	return done
}

func TestFileStoreDeleteDrainsSourceIndexRenameWindows(t *testing.T) {
	src := &StreamSource{Name: "ORIGIN"}
	cfg := StreamConfig{Name: "UNIT", Storage: FileStorage, Sources: []*StreamSource{src}}
	fs, err := newFileStore(FileStoreConfig{StoreDir: filepath.Join(t.TempDir(), "UNIT")}, cfg)
	require_NoError(t, err)
	t.Cleanup(func() { fs.Stop() })
	state := map[string]StreamSourceState{src.composeIName(): {Seq: 1, Ident: "first"}}
	fs.ApplySourcesState(state)
	require_NoError(t, fs.forceWriteFullState())
	release := pinWindowsStorePath(t, fs.sourcesStatePath())

	state[src.composeIName()] = StreamSourceState{Seq: 2, Ident: "second"}
	fs.ApplySourcesState(state)
	wrote := runConsumerTestIO(t, release, fs.forceWriteFullState)
	checkFor(t, time.Second*3, 2*time.Millisecond, func() error {
		_, err := os.Stat(fs.sourcesStatePath() + ".tmp")
		return err
	})
	deleted := runConsumerTestIO(t, release, func() error { return fs.Delete(true) })
	checkFor(t, time.Second, 2*time.Millisecond, func() error {
		if !fs.isClosed() {
			return errors.New("waiting for stream shutdown")
		}
		return nil
	})
	select {
	case err := <-deleted:
		t.Fatalf("stream directory removed while sources index was being written: %v", err)
	case <-time.After(175 * time.Millisecond):
	}
	_, err = os.Stat(filepath.Join(fs.fcfg.StoreDir, JetStreamMetaFile))
	require_NoError(t, err)
	release()
	select {
	case err := <-wrote:
		require_NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("index writer did not finish")
	}
	select {
	case err := <-deleted:
		require_NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("stream delete did not finish")
	}
}

func TestFileStoreConsumerStateRenameWindows(t *testing.T) {
	for _, operation := range []string{"stop", "persistent", "delete", "sync"} {
		t.Run(operation, func(t *testing.T) {
			fs, err := newFileStore(FileStoreConfig{StoreDir: t.TempDir()}, StreamConfig{Name: "S", Storage: FileStorage})
			require_NoError(t, err)
			t.Cleanup(func() { fs.Stop() })
			newConsumer := func() *consumerFileStore {
				store, err := fs.ConsumerStore("C", time.Now(), &ConsumerConfig{AckPolicy: AckExplicit})
				require_NoError(t, err)
				return store.(*consumerFileStore)
			}
			o := newConsumer()
			require_NoError(t, o.SetStarting(1))
			// Hold the real destination open without FILE_SHARE_DELETE.
			release := pinWindowsStorePath(t, o.ifn)

			// The closed-and-written temp file and its decoded message identity prove the
			// background writer entered this update before the newer ACK and close.
			require_NoError(t, o.UpdateDelivered(1, 2, 1, time.Now().UnixNano()))
			checkFor(t, time.Second*3, 2*time.Millisecond, func() error {
				buf, err := os.ReadFile(o.ifn + ".tmp")
				if err != nil {
					return err
				}
				st, err := decodeConsumerState(buf)
				if err != nil || st.Delivered.Stream != 2 || st.AckFloor.Stream != 1 {
					return fmt.Errorf("waiting for first delivery in temp file: %+v, %v", st, err)
				}
				return nil
			})
			if operation != "persistent" {
				require_NoError(t, o.UpdateAcks(1, 2, true))
			}

			op := o.Stop
			switch operation {
			case "delete":
				op = o.Delete
			case "sync":
				op = func() error { return o.SetStarting(3) }
			}
			done := runConsumerTestIO(t, release, op)
			select {
			case err := <-done:
				t.Fatalf("%s returned while pending rename was blocked: %v", operation, err)
			case <-time.After(175 * time.Millisecond):
			}
			// The final ACK must not truncate or replace the earlier writer's temp.
			if operation == "stop" || operation == "sync" {
				b, err := os.ReadFile(o.ifn + ".tmp")
				require_NoError(t, err)
				old, err := decodeConsumerState(b)
				require_NoError(t, err)
				require_Equal(t, old.Delivered.Stream, uint64(2))
				require_Equal(t, old.AckFloor.Stream, uint64(1))
			}
			if operation == "delete" {
				// The upper stream unregisters its consumer before its store finishes
				// deleting. A concurrent constructor must not claim the same directory.
				_, err := fs.ConsumerStore("C", time.Now(), &ConsumerConfig{AckPolicy: AckExplicit})
				require_Error(t, err, NewJSConsumerNameExistError())
			}
			if operation != "persistent" {
				release()
			}
			select {
			case err = <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("consumer operation did not finish")
			}
			if operation == "persistent" {
				var le *os.LinkError
				if !errors.As(err, &le) || le.Old != o.ifn+".tmp" || le.New != o.ifn ||
					(!errors.Is(le, windows.ERROR_ACCESS_DENIED) && !errors.Is(le, windows.ERROR_SHARING_VIOLATION)) {
					t.Fatalf("expected final Windows rename error, got %v", err)
				}
				release()
			} else {
				require_NoError(t, err)
			}
			if operation == "sync" {
				require_NoError(t, o.Stop())
			}
			reopened := newConsumer()
			st, err := reopened.State()
			require_NoError(t, err)
			switch operation {
			case "stop":
				require_Equal(t, st.AckFloor, SequencePair{Consumer: 1, Stream: 2})
				require_Equal(t, st.Delivered, st.AckFloor)
			case "persistent":
				require_Equal(t, st.AckFloor.Stream, uint64(1))
				require_Equal(t, st.Delivered.Stream, uint64(1))
			case "delete":
				require_Equal(t, st.Delivered, SequencePair{})
				require_NoError(t, reopened.SetStarting(10))
				require_NoError(t, reopened.Stop())
				st, err = newConsumer().State()
				require_NoError(t, err)
				require_Equal(t, st.AckFloor.Stream, uint64(10))
			case "sync":
				require_Equal(t, st.Delivered, SequencePair{Consumer: 1, Stream: 3})
				require_Equal(t, st.AckFloor, st.Delivered)
			}
		})
	}
}

// Exercise the public purge and publish requests while the Windows destination is pinned.
func TestJetStreamWindowsBlockRenamePublish(t *testing.T) {
	for _, contention := range []string{"released", "persistent"} {
		t.Run(contention, func(t *testing.T) {
			s := RunBasicJetStreamServer(t)
			defer s.Shutdown()
			nc, js := jsClientConnect(t, s)
			defer nc.Close()
			probe, other := jsClientConnect(t, s)
			defer probe.Close()
			for _, name := range []string{"EVENTS", "OTHER"} {
				_, err := js.AddStream(&nats.StreamConfig{Name: name, Subjects: []string{name}, Storage: nats.FileStorage})
				require_NoError(t, err)
			}
			payload := make([]byte, 64*1024)
			for i := 1; i <= 60; i++ {
				ack, err := js.Publish("EVENTS", payload)
				require_NoError(t, err)
				require_Equal(t, ack.Sequence, uint64(i))
			}
			stream, err := s.GlobalAccount().lookupStream("EVENTS")
			require_NoError(t, err)
			fs := stream.Store().(*fileStore)
			fs.mu.RLock()
			block := fs.selectMsgBlock(1)
			fs.mu.RUnlock()
			require_NotNil(t, block)
			target := block.mfn
			release := pinWindowsStorePath(t, target)
			done := make(chan error, 1)
			go func() { done <- js.PurgeStream("EVENTS", &nats.StreamPurgeRequest{Sequence: 40}) }()
			checkFor(t, 5*time.Second, time.Millisecond, func() error {
				_, err := os.Stat(target + ".tmp")
				return err
			})
			// Another connection and stream can publish while the rename is backing off.
			ack, err := other.Publish("OTHER", []byte("probe"))
			require_NoError(t, err)
			require_Equal(t, ack.Sequence, uint64(1))
			select {
			case err := <-done:
				t.Fatalf("purge returned with target pinned: %v", err)
			case <-time.After(100 * time.Millisecond):
			}
			if contention == "released" {
				release()
				require_NoError(t, <-done)
				ack, err = js.Publish("EVENTS", []byte("next"))
				require_NoError(t, err)
				require_Equal(t, ack.Sequence, uint64(61))
				require_True(t, s.JetStreamEnabled())
				t.Log("independent PUBACK=1 affected PUBACK=61 JetStream enabled")
				return
			}
			require_Error(t, <-done)
			ack, err = js.Publish("EVENTS", []byte("next"), nats.AckWait(time.Second))
			require_Error(t, err)
			require_True(t, ack == nil)
			checkFor(t, 5*time.Second, time.Millisecond, func() error {
				if s.JetStreamEnabled() {
					return errors.New("JetStream still enabled")
				}
				return nil
			})
			release()
			require_NoError(t, nc.FlushTimeout(time.Second))
			t.Log("independent PUBACK=1 affected PUBACK absent, JetStream disabled, core NATS responds")
		})
	}
}

func TestFileStoreWindowsBlockRenameRecovery(t *testing.T) {
	for _, path := range []string{"periodic-compact", "purge-prefix"} {
		for _, contention := range []string{"released", "persistent"} {
			t.Run(path+"/"+contention, func(t *testing.T) {
				fcfg := FileStoreConfig{StoreDir: t.TempDir(), BlockSize: 64 * 1024, SyncAlways: true, SyncInterval: time.Hour}
				cfg := StreamConfig{Name: "BLOCKS", Storage: FileStorage}
				size, count := 8*1024, 10
				if path == "purge-prefix" {
					fcfg.BlockSize, size, count = 4*1024*1024, 64*1024, 70
				}
				fs, err := newFileStore(fcfg, cfg)
				require_NoError(t, err)
				t.Cleanup(func() { fs.Stop() })
				payload := func(seq int) []byte { return bytes.Repeat([]byte{byte(seq)}, size) }
				for i := 1; i <= count; i++ {
					seq, _, err := fs.StoreMsg("event", nil, payload(i), 0)
					require_NoError(t, err)
					require_Equal(t, seq, uint64(i))
				}
				fs.mu.RLock()
				blk := fs.selectMsgBlock(1)
				fs.mu.RUnlock()
				require_NotNil(t, blk)
				tmp := blk.mfn + ".tmp"
				compact := func() error { _, err := fs.PurgeEx("", 40, 0); return err }
				if path == "periodic-compact" {
					tmp = filepath.Join(fcfg.StoreDir, msgDir, fmt.Sprintf(newScan, blk.index))
					// This non-tail block is smaller than the inline-compaction threshold.
					// Deleting four interior records makes it a periodic sync candidate.
					for i := uint64(2); i <= 5; i++ {
						_, err := fs.RemoveMsg(i)
						require_NoError(t, err)
					}
					compact = func() error { fs.syncBlocks(); return fs.werr }
				}
				original, err := os.ReadFile(blk.mfn)
				require_NoError(t, err)
				release := pinWindowsStorePath(t, blk.mfn)
				done := make(chan error, 1)
				go func() { done <- compact() }()
				if contention == "released" {
					checkFor(t, 5*time.Second, time.Millisecond, func() error {
						if info, err := os.Stat(tmp); err == nil && info.Size() > 0 {
							return nil
						}
						return fmt.Errorf("awaiting prepared %s", filepath.Base(tmp))
					})
					select {
					case err := <-done:
						t.Fatalf("compaction returned with target held: %v", err)
					case <-time.After(120 * time.Millisecond):
					}
					release()
				}
				var compactErr error
				select {
				case compactErr = <-done:
				case <-time.After(10 * time.Second):
					t.Fatal("compaction did not finish")
				}
				ack, _, publishErr := fs.StoreMsg("event", nil, payload(count+1), 0)
				if contention == "persistent" {
					var le *os.LinkError
					if !errors.As(compactErr, &le) || le.Old != tmp || le.New != blk.mfn ||
						(!errors.Is(le.Err, windows.ERROR_ACCESS_DENIED) && !errors.Is(le.Err, windows.ERROR_SHARING_VIOLATION)) {
						t.Fatalf("unexpected rename error: %v", compactErr)
					}
					if publishErr != compactErr || ack != 0 {
						t.Fatalf("failed write ack=%d err=%v want %v", ack, publishErr, compactErr)
					}
					t.Logf("%s -> %s win32=%d publish_ack=%d", filepath.Base(tmp), filepath.Base(blk.mfn), le.Err, ack)
					data, err := os.ReadFile(blk.mfn)
					require_NoError(t, err)
					require_True(t, bytes.Equal(data, original))
					release()
					require_NoError(t, fs.stop(false, false))
				} else {
					require_NoError(t, compactErr)
					require_NoError(t, publishErr)
					require_Equal(t, ack, uint64(count+1))
					require_NoError(t, fs.Stop())
					count++
				}
				_, err = os.Stat(tmp)
				require_True(t, os.IsNotExist(err))
				reopened, err := newFileStore(fcfg, cfg)
				require_NoError(t, err)
				defer reopened.Stop()
				var kept int
				for i := 1; i <= count; i++ {
					m, err := reopened.LoadMsg(uint64(i), nil)
					deleted := path == "periodic-compact" && i >= 2 && i <= 5 || path == "purge-prefix" && contention == "released" && i < 40
					if deleted {
						if !errors.Is(err, ErrStoreMsgNotFound) && !errors.Is(err, errDeletedMsg) {
							t.Fatalf("expected sequence %d to be absent, got %v", i, err)
						}
						continue
					}
					require_NoError(t, err)
					require_True(t, bytes.Equal(m.msg, payload(i)))
					kept++
				}
				state := reopened.State()
				require_Equal(t, state.Msgs, uint64(kept))
				require_Equal(t, state.LastSeq, uint64(count))
				next, _, err := reopened.StoreMsg("event", nil, payload(count+1), 0)
				require_NoError(t, err)
				require_Equal(t, next, uint64(count+1))
				t.Logf("%s -> %s reopened_first=%d last=%d contents_verified=%d next=%d", filepath.Base(tmp), filepath.Base(blk.mfn), state.FirstSeq, state.LastSeq, kept, next)
			})
		}
	}
}

func pinWindowsStorePath(t *testing.T, name string) func() {
	t.Helper()
	path, err := windows.UTF16PtrFromString(name)
	require_NoError(t, err)
	h, err := windows.CreateFile(path, windows.GENERIC_READ, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
		nil, windows.OPEN_EXISTING, windows.FILE_FLAG_BACKUP_SEMANTICS, 0)
	require_NoError(t, err)
	release := sync.OnceFunc(func() { require_NoError(t, windows.CloseHandle(h)) })
	t.Cleanup(release)
	return release
}

// Only directories created underneath this test store inherit the staging denial.
// The live msgs directory predates it and must still accept the tombstone and its key.
func denyWindowsPurgeStage(t *testing.T, root, stage string) func() {
	t.Helper()
	dacl := func(path string) *windows.ACL {
		sd, err := windows.GetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
		require_NoError(t, err)
		acl, _, err := sd.DACL()
		require_NoError(t, err)
		return acl
	}
	set := func(path string, acl *windows.ACL) {
		t.Helper()
		require_NoError(t, windows.SetNamedSecurityInfo(path, windows.SE_FILE_OBJECT,
			windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION, nil, nil, acl, nil))
	}
	msgs := filepath.Join(root, msgDir)
	set(msgs, dacl(msgs))
	original := dacl(root)
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	require_NoError(t, err)
	denial := windows.EXPLICIT_ACCESS{
		AccessMode:        windows.DENY_ACCESS,
		AccessPermissions: windows.FILE_WRITE_DATA, // FILE_ADD_FILE for a directory
		Inheritance:       windows.CONTAINER_INHERIT_ACE | windows.INHERIT_ONLY_ACE | windows.NO_PROPAGATE_INHERIT_ACE,
		Trustee:           windows.TRUSTEE{TrusteeForm: windows.TRUSTEE_IS_SID, TrusteeValue: windows.TrusteeValueFromSID(user.User.Sid)},
	}
	entries := []windows.EXPLICIT_ACCESS{denial}
	if stage == "new-directory" {
		entries[0].AccessPermissions = windows.DELETE
		denial.AccessPermissions, denial.Inheritance = 0x40, 0 // FILE_DELETE_CHILD on root
		entries = append(entries, denial)
	}
	modified, err := windows.ACLFromEntries(entries, original)
	require_NoError(t, err)
	set(root, modified)
	release := sync.OnceFunc(func() { set(root, original) })
	t.Cleanup(release)
	return release
}

// Hold the requested file or directory at the boundary where the purge promotes it.
func startWindowsPurge(t *testing.T, fs *fileStore, stage string, sequence uint64) (source, dest, ready string, release func(), done <-chan error) {
	t.Helper()
	root := fs.fcfg.StoreDir
	msgs, staging := filepath.Join(root, msgDir), filepath.Join(root, newMsgDir)
	prep := filepath.Join(staging, "prep")
	block := fmt.Sprintf(blkScan, fs.lmb.index+1)
	key := fmt.Sprintf(keyScan, fs.lmb.index+1)
	switch stage {
	case "block":
		source, dest, ready = filepath.Join(prep, block), filepath.Join(staging, block), filepath.Join(prep, block)
		if fs.prf != nil {
			ready = filepath.Join(staging, key)
		}
	case "key":
		source, dest, ready = filepath.Join(prep, key), filepath.Join(staging, key), filepath.Join(prep, block)
	case "old-directory":
		source, dest, ready = msgs, filepath.Join(root, purgeDir), filepath.Join(staging, block)
	case "new-directory":
		source, dest, ready = staging, msgs, filepath.Join(root, purgeDir)
	}
	if stage == "old-directory" {
		release = pinWindowsStorePath(t, source)
	} else {
		release = denyWindowsPurgeStage(t, root, stage)
	}
	result := make(chan error, 1)
	go func() { _, err := fs.PurgeEx("", sequence, 0); result <- err }()
	if stage == "block" {
		// Stall promotion into staging while the source is being prepared. Once it
		// exists, pin only that .blk and allow any preceding .key promotion through.
		checkFor(t, 3*time.Second, time.Millisecond, func() error {
			_, err := os.Stat(source)
			return err
		})
		unpin := pinWindowsStorePath(t, source)
		release()
		release = unpin
	}
	return source, dest, ready, release, result
}

func windowsPurgeKey(cipher StoreCipher) keyGen {
	if cipher == NoCipher {
		return nil
	}
	return (&Server{}).jsKeyGen("dlc22", "")
}

func TestFileStoreWindowsPurgeRenameRecovery(t *testing.T) {
	for _, cipher := range []StoreCipher{NoCipher, AES} {
		stages := []string{"block", "old-directory", "new-directory"}
		if cipher != NoCipher {
			stages = append(stages, "key")
		}
		for _, stage := range stages {
			locks := []string{"persistent", "released"}
			if stage == "key" || stage == "block" {
				locks = append(locks, "persistent-graceful")
			}
			for _, lock := range locks {
				t.Run(cipher.String()+"/"+stage+"/"+lock, func(t *testing.T) {
					fcfg := FileStoreConfig{StoreDir: t.TempDir(), BlockSize: 64 * 1024, SyncAlways: true, Cipher: cipher}
					cfg := StreamConfig{Name: "RECOVERY", Storage: FileStorage}
					created := time.Now()
					open := func() *fileStore {
						fs, err := newFileStoreWithCreated(fcfg, cfg, created, windowsPurgeKey(fcfg.Cipher), nil)
						require_NoError(t, err)
						t.Cleanup(func() { fs.Stop() })
						return fs
					}
					fs := open()
					const last = 4
					for i := 1; i <= last; i++ {
						_, _, err := fs.StoreMsg("event", nil, []byte(fmt.Sprint(i)), 0)
						require_NoError(t, err)
					}
					source, dest, ready, release, done := startWindowsPurge(t, fs, stage, 0)
					if lock == "released" {
						checkFor(t, 3*time.Second, time.Millisecond, func() error { _, err := os.Stat(ready); return err })
						select {
						case err := <-done:
							t.Fatalf("purge returned while %s was held: %v", stage, err)
						case <-time.After(100 * time.Millisecond):
						}
						release()
						require_NoError(t, <-done)
					} else {
						purgeErr := <-done
						var link *os.LinkError
						if !errors.As(purgeErr, &link) || link.Old != source || link.New != dest ||
							(!errors.Is(link.Err, windows.ERROR_ACCESS_DENIED) && !errors.Is(link.Err, windows.ERROR_SHARING_VIOLATION)) {
							t.Fatalf("expected locked %s -> %s, got %v", source, dest, purgeErr)
						}
						_, _, nextErr := fs.StoreMsg("event", nil, []byte("rejected"), 0)
						if fs.werr != purgeErr || nextErr != purgeErr {
							t.Fatalf("purge %v, latched %v, next publish %v", purgeErr, fs.werr, nextErr)
						}
						t.Logf("win32=%d same_werr_and_publish=true", link.Err)
						release()
					}
					// A graceful stop can persist the in-memory state to index.db.
					// Both it and a crash must retain the pre-commit message set.
					if lock != "released" && (stage == "key" || stage == "block") {
						state := fs.State()
						require_Equal(t, state.Msgs, uint64(last))
						require_Equal(t, state.FirstSeq, uint64(1))
						require_Equal(t, state.LastSeq, uint64(last))
						for i := uint64(1); i <= last; i++ {
							m, err := fs.LoadMsg(i, nil)
							require_NoError(t, err)
							require_Equal(t, string(m.msg), fmt.Sprint(i))
						}
					}
					if lock == "persistent-graceful" {
						require_NoError(t, fs.Stop())
						_, err := os.Stat(filepath.Join(fcfg.StoreDir, msgDir, streamStreamStateFile))
						require_NoError(t, err)
					} else {
						require_NoError(t, fs.stop(false, false))
					}
					if lock == "released" {
						for _, leftover := range []string{newMsgDir, filepath.Join(msgDir, "prep")} {
							_, err := os.Stat(filepath.Join(fcfg.StoreDir, leftover))
							require_True(t, os.IsNotExist(err))
						}
					}
					want := []uint64{1, 2, 3, 4}
					if lock == "released" || stage == "old-directory" || stage == "new-directory" {
						want = nil
					}
					for i := 1; i <= 2; i++ {
						fs = open()
						state := fs.State()
						var ids []uint64
						for seq := uint64(1); seq <= last; seq++ {
							m, err := fs.LoadMsg(seq, nil)
							if errors.Is(err, ErrStoreMsgNotFound) || errors.Is(err, ErrStoreEOF) || errors.Is(err, errDeletedMsg) {
								continue
							}
							require_NoError(t, err)
							require_Equal(t, string(m.msg), fmt.Sprint(seq))
							ids = append(ids, seq)
						}
						first := uint64(1)
						if len(want) == 0 {
							first = last + 1
						}
						if !slices.Equal(ids, want) || state.Msgs != uint64(len(want)) || state.FirstSeq != first || state.LastSeq != last {
							t.Fatalf("recovery %d: first=%d last=%d count=%d ids=%v, want first=%d last=%d ids=%v", i, state.FirstSeq, state.LastSeq, state.Msgs, ids, first, last, want)
						}
						if i == 1 {
							require_NoError(t, fs.stop(false, false))
						} else {
							next, _, err := fs.StoreMsg("event", nil, []byte("next"), 0)
							require_NoError(t, err)
							require_Equal(t, next, uint64(last+1))
							t.Logf("reopened_twice first=%d last=%d ids=%v next=%d", state.FirstSeq, state.LastSeq, ids, next)
						}
					}
				})
			}
		}
	}
}

func TestFileStoreWindowsPurgeDrainFailure(t *testing.T) {
	fcfg := FileStoreConfig{StoreDir: t.TempDir(), SyncAlways: true}
	cfg := StreamConfig{Name: "DRAIN", Storage: FileStorage}
	fs, err := newFileStore(fcfg, cfg)
	require_NoError(t, err)
	t.Cleanup(func() { fs.Stop() })
	_, _, err = fs.StoreMsg("event", nil, []byte("accepted"), 0)
	require_NoError(t, err)
	// Replay the pending write with the existing one-shot block fault. Its original
	// durable copy lets us independently verify state and payload across recovery.
	mb := fs.lmb
	mb.mu.Lock()
	err = mb.loadMsgsWithLock()
	if err == nil {
		mb.cache.wp = 0
		mb.mockWriteErr = true
	}
	mb.mu.Unlock()
	require_NoError(t, err)
	_, err = fs.Purge()
	if err == nil || !strings.Contains(err.Error(), "mock write error") {
		t.Fatalf("expected pending write error, got %v", err)
	}
	if fs.werr != err {
		t.Fatalf("purge %v, latched %v", err, fs.werr)
	}
	_, _, err = fs.StoreMsg("event", nil, []byte("rejected"), 0)
	require_Equal(t, err, fs.werr)
	state := fs.State()
	require_Equal(t, state.Msgs, uint64(1))
	require_Equal(t, state.FirstSeq, uint64(1))
	require_Equal(t, state.LastSeq, uint64(1))
	_, err = os.Stat(filepath.Join(fcfg.StoreDir, newMsgDir))
	require_True(t, os.IsNotExist(err))
	require_NoError(t, fs.stop(false, false))
	reopened, err := newFileStore(fcfg, cfg)
	require_NoError(t, err)
	defer reopened.Stop()
	msg, err := reopened.LoadMsg(1, nil)
	require_NoError(t, err)
	require_Equal(t, string(msg.msg), "accepted")
	next, _, err := reopened.StoreMsg("event", nil, []byte("next"), 0)
	require_NoError(t, err)
	require_Equal(t, next, uint64(2))
}

func TestFileStoreWindowsEmptyPurgeWitness(t *testing.T) {
	for _, cipher := range []StoreCipher{NoCipher, AES} {
		t.Run(cipher.String(), func(t *testing.T) {
			fcfg := FileStoreConfig{StoreDir: t.TempDir(), Cipher: cipher}
			cfg := StreamConfig{Name: "EMPTY", Storage: FileStorage}
			created := time.Now()
			open := func() *fileStore {
				fs, err := newFileStoreWithCreated(fcfg, cfg, created, windowsPurgeKey(fcfg.Cipher), nil)
				require_NoError(t, err)
				t.Cleanup(func() { fs.Stop() })
				return fs
			}
			fs := open()
			_, err := fs.Purge()
			require_NoError(t, err)
			require_NoError(t, fs.stop(false, false))
			msgs := filepath.Join(fcfg.StoreDir, msgDir)
			blks, err := filepath.Glob(filepath.Join(msgs, "*.blk"))
			require_NoError(t, err)
			require_Equal(t, len(blks), 1)
			// A crash after the directory swap can leave an empty prep subdirectory
			// next to the completed tombstone. Existing stores must scan past it.
			require_NoError(t, os.MkdirAll(filepath.Join(msgs, "prep"), defaultDirPerms))
			fs = open()
			state := fs.State()
			require_Equal(t, state.Msgs, uint64(0))
			require_Equal(t, state.LastSeq, uint64(0))
			next, _, err := fs.StoreMsg("event", nil, []byte("first"), 0)
			require_NoError(t, err)
			require_Equal(t, next, uint64(1))
			t.Logf("empty witness and prep subdirectory: first=%d last=%d next=%d", state.FirstSeq, state.LastSeq, next)
		})
	}
}

func TestFileStoreWindowsPurgeAdvancedSequence(t *testing.T) {
	for _, cipher := range []StoreCipher{NoCipher, AES} {
		for _, scenario := range []string{"empty", "committed", "aborted"} {
			t.Run(cipher.String()+"/"+scenario, func(t *testing.T) {
				fcfg := FileStoreConfig{StoreDir: t.TempDir(), Cipher: cipher, SyncAlways: true}
				cfg := StreamConfig{Name: "CURSOR", Storage: FileStorage}
				created := time.Now()
				open := func() *fileStore {
					fs, err := newFileStoreWithCreated(fcfg, cfg, created, windowsPurgeKey(fcfg.Cipher), nil)
					require_NoError(t, err)
					t.Cleanup(func() { fs.Stop() })
					return fs
				}
				fs := open()
				if scenario != "empty" {
					_, _, err := fs.StoreMsg("event", nil, []byte("original"), 0)
					require_NoError(t, err)
				}
				const advance = 100
				first, last := uint64(advance), uint64(advance-1)
				if scenario == "aborted" {
					_, _, _, release, done := startWindowsPurge(t, fs, "block", advance)
					require_Error(t, <-done)
					release()
					first, last = 1, 1
				} else {
					_, err := fs.Compact(advance)
					require_NoError(t, err)
				}
				require_NoError(t, fs.stop(false, false))
				fs = open()
				state := fs.State()
				require_Equal(t, state.FirstSeq, first)
				require_Equal(t, state.LastSeq, last)
				if scenario == "aborted" {
					msg, err := fs.LoadMsg(1, nil)
					require_NoError(t, err)
					require_Equal(t, string(msg.msg), "original")
				}
				next, _, err := fs.StoreMsg("event", nil, []byte("next"), 0)
				require_NoError(t, err)
				require_Equal(t, next, last+1)
				t.Logf("reopened_first=%d last=%d next=%d", first, last, next)
			})
		}
	}
}

// Pin the old destination until the writer has closed its temp and started retrying.
func TestNRGWindowsRenamePersistence(t *testing.T) {
	n, stop := initSingleMemRaftNode(t)
	defer stop()

	dir := n.sd
	n.Lock()
	// Isolate this node's I/O limiter so its spare slot signals an actual rename backoff.
	n.dios = newDiskIOSemaphore(minConcurrentIOs)
	for i := 1; i < n.dios.cap(); i++ {
		n.dios.acquire()
		defer n.dios.release()
	}
	n.term = 2
	n.vote = "11111111"
	err := n.writeTermVote()
	n.Unlock()
	require_NoError(t, err)

	for _, name := range []string{termVoteFile, peerStateFile} {
		t.Run(name, func(t *testing.T) {
			dst := filepath.Join(dir, name)
			h, err := os.Open(dst)
			require_NoError(t, err)
			defer h.Close()
			var stored error
			done := make(chan struct{})
			go func() {
				defer close(done)
				n.Lock()
				defer n.Unlock()
				if name == termVoteFile {
					n.term++
					stored = n.writeTermVote()
				} else {
					n.writePeerState(&peerState{knownPeers: []string{"22222222"}, clusterSize: 1, domainExt: extNotExtended})
					stored = n.werr
				}
			}()
			defer func() { h.Close(); <-done }()
			checkFor(t, time.Second, time.Millisecond, func() error {
				if _, err := os.Stat(dst + ".tmp"); err != nil {
					return err
				}
				if len(n.dios.ch) == 0 {
					return fmt.Errorf("waiting for first failed rename")
				}
				return nil
			})
			require_NoError(t, h.Close())
			<-done
			require_NoError(t, stored)
			if name == termVoteFile {
				term, vote, err := n.readTermVote()
				require_NoError(t, err)
				require_Equal(t, term, uint64(3))
				require_Equal(t, vote, "11111111")
			} else {
				ps, err := readPeerState(n.dios, dir)
				require_NoError(t, err)
				require_Equal(t, len(ps.knownPeers), 1)
				require_Equal(t, ps.knownPeers[0], "22222222")
			}
		})
	}
	t.Run("snapshot exhaustion preserves old file and node", func(t *testing.T) {
		old := &snapshot{lastTerm: 2, lastIndex: 1, data: []byte("old")}
		n.Lock()
		err := n.installSnapshot(old)
		n.Unlock()
		require_NoError(t, err)
		h, err := os.Open(n.snapfile)
		require_NoError(t, err)
		defer h.Close()
		updated := *old
		updated.data = []byte("new")
		n.Lock()
		err = n.installSnapshot(&updated)
		n.Unlock()
		require_True(t, errors.Is(err, windows.ERROR_ACCESS_DENIED))
		require_NoError(t, n.werr)
		snap, err := n.loadLastSnapshot()
		require_NoError(t, err)
		require_True(t, bytes.Equal(snap.data, old.data))
		require_NoError(t, h.Close())
		n.Lock()
		err = n.installSnapshot(&updated)
		n.Unlock()
		require_NoError(t, err)
		snap, err = n.loadLastSnapshot()
		require_NoError(t, err)
		require_True(t, bytes.Equal(snap.data, updated.data))
	})
}

func TestWindowsRenameBackoffYieldsDiskSlot(t *testing.T) {
	d := newDiskIOSemaphore(minConcurrentIOs)
	blocked, other := t.TempDir(), t.TempDir()
	path := filepath.Join(blocked, termVoteFile)
	const newTerm = uint64(5)
	buf := make([]byte, termVoteLen)
	binary.LittleEndian.PutUint64(buf, newTerm)
	require_NoError(t, os.WriteFile(path, []byte("old"), 0600))
	h, err := os.Open(path)
	require_NoError(t, err)
	defer h.Close()
	for i := 1; i < d.cap(); i++ {
		d.acquire()
		defer d.release()
	}
	done := make(chan struct{})
	var written error
	go func() { defer close(done); _, written = writeTermVote(d, blocked, buf) }()
	defer func() { h.Close(); <-done }()
	// Read the channel length only to synchronize this test: the temp exists and
	// the writer has returned its last available slot during a backoff.
	checkFor(t, time.Second, time.Millisecond, func() error {
		if _, err := os.Stat(path + ".tmp"); err != nil {
			return err
		}
		if len(d.ch) == 0 {
			return fmt.Errorf("rename still holding disk slot")
		}
		return nil
	})
	_, err = writeTermVote(d, other, buf)
	require_NoError(t, err)
	select {
	case <-done:
		t.Fatal("blocked writer finished before its handle was released")
	default:
	}
	require_NoError(t, h.Close())
	<-done
	require_NoError(t, written)
}

func TestWindowsStreamStopReportsStorageErrors(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()
	logs := &DummyLogger{AllMsgs: []string{}}
	s.SetLogger(logs, false, false)
	acc := s.GlobalAccount()
	checkReport := func(t *testing.T, expected string) {
		t.Helper()
		logs.Lock()
		defer logs.Unlock()
		require_Contains(t, strings.Join(logs.AllMsgs, "\n"), expected)
	}
	t.Run("consumer close", func(t *testing.T) {
		mset, err := acc.addStream(&StreamConfig{Name: "STOP", Storage: FileStorage})
		require_NoError(t, err)
		addConsumer := func(name string) *consumerFileStore {
			t.Helper()
			o, err := mset.addConsumer(&ConsumerConfig{Durable: name, AckPolicy: AckExplicit})
			require_NoError(t, err)
			cs := o.store.(*consumerFileStore)
			require_NoError(t, cs.SetStarting(1))
			return cs
		}
		bad, good := addConsumer("LOCKED"), addConsumer("HEALTHY")
		f, err := os.Open(bad.ifn)
		require_NoError(t, err)
		defer f.Close()
		bad.UpdateStarting(2)
		good.UpdateStarting(3)
		require_NoError(t, mset.stop(false, false))
		_, err = acc.lookupStream("STOP")
		require_Error(t, err)
		err = bad.Stop()
		var le *os.LinkError
		require_True(t, errors.As(err, &le))
		require_Equal(t, le.New, bad.ifn)
		require_True(t, mset.store.(*fileStore).isClosed())
		b, err := os.ReadFile(good.ifn)
		require_NoError(t, err)
		state, err := decodeConsumerState(b)
		require_NoError(t, err)
		require_Equal(t, state.Delivered.Stream, uint64(3))
		checkReport(t, fmt.Sprintf("finished cleanup with storage errors (delete=false, failed_consumers=1, first_consumer=\"LOCKED\"): %v", le))
	})
	t.Run("stream delete", func(t *testing.T) {
		mset, err := acc.addStream(&StreamConfig{Name: "DELETE", Storage: FileStorage})
		require_NoError(t, err)
		dir := mset.store.(*fileStore).fcfg.StoreDir
		f, err := os.Open(dir)
		require_NoError(t, err)
		defer f.Close()
		nc, js := jsClientConnect(t, s)
		defer nc.Close()
		require_NoError(t, js.DeleteStream("DELETE"))
		_, err = acc.lookupStream("DELETE")
		require_Error(t, err)
		// A separate Windows rename of the same still-locked directory gives
		// the OS-specific error text expected in the stream owner's diagnostic.
		dest := filepath.Join(filepath.Dir(dir), tsep+filepath.Base(dir))
		physicalErr := os.Rename(dir, dest)
		require_True(t, errors.Is(physicalErr, windows.ERROR_ACCESS_DENIED) || errors.Is(physicalErr, windows.ERROR_SHARING_VIOLATION))
		checkReport(t, fmt.Sprintf("finished cleanup with storage errors (delete=true, failed_consumers=0, first_consumer=\"\"): %v", physicalErr))
	})
}

func TestWindowsServiceRename(t *testing.T) {
	hold := func(t *testing.T, path string) *os.File {
		t.Helper()
		f, err := os.Open(path)
		require_NoError(t, err)
		t.Cleanup(func() { f.Close() })
		return f
	}
	checkFile := func(t *testing.T, path, want string) {
		t.Helper()
		b, err := os.ReadFile(path)
		require_NoError(t, err)
		require_Equal(t, string(b), want)
	}
	checkRenameError := func(t *testing.T, err error) {
		t.Helper()
		var le *os.LinkError
		if !errors.As(err, &le) || le.Op != "rename" {
			t.Fatalf("expected original rename error, got %v", err)
		}
	}
	t.Run("staged stream metadata", func(t *testing.T) {
		dir := t.TempDir()
		dios := defaultDiskIOSemaphore()
		meta := filepath.Join(dir, JetStreamMetaFile)
		key := filepath.Join(dir, JetStreamMetaFileKey)
		require_NoError(t, os.WriteFile(key+metaTmpSuffix, []byte("new key"), 0600))
		require_NoError(t, os.WriteFile(meta+metaTmpSuffix, []byte("new metadata"), 0600))
		require_NoError(t, os.WriteFile(meta, []byte("old metadata"), 0600))
		locked := hold(t, meta)
		checkRenameError(t, commitStagedMeta(dios, dir, true, true))
		checkFile(t, key, "new key")
		checkFile(t, meta, "old metadata")
		checkFile(t, meta+metaTmpSuffix, "new metadata")
		require_NoError(t, locked.Close())
		require_NoError(t, commitStagedMeta(dios, dir, false, true))
		checkFile(t, meta, "new metadata")
	})
	t.Run("OCSP staple", func(t *testing.T) {
		sd := t.TempDir()
		dir := filepath.Join(sd, defaultOCSPStoreDir)
		require_NoError(t, os.Mkdir(dir, 0700))
		m := &OCSPMonitor{srv: &Server{}}
		path := filepath.Join(dir, "cert")
		require_NoError(t, m.writeOCSPStatus(sd, "cert", []byte("old")))
		f := hold(t, path)
		checkRenameError(t, m.writeOCSPStatus(sd, "cert", []byte("new")))
		checkFile(t, path, "old")
		entries, err := os.ReadDir(dir)
		require_NoError(t, err)
		require_Equal(t, len(entries), 1)
		require_NoError(t, f.Close())
		require_NoError(t, m.writeOCSPStatus(sd, "cert", []byte("new")))
		checkFile(t, path, "new")
	})
	t.Run("OCSP cache", func(t *testing.T) {
		dir := t.TempDir()
		c := &LocalCache{mu: &sync.RWMutex{}, config: &OCSPResponseCacheConfig{LocalStore: dir},
			cache: map[string]OCSPResponseCacheItem{"cert": {Subject: "new"}}, dirty: true}
		path := filepath.Join(dir, OCSPResponseCacheDefaultFilename)
		require_NoError(t, os.WriteFile(path, []byte("{}"), 0600))
		f := hold(t, path)
		s := &Server{}
		c.saveCache(s)
		require_True(t, c.dirty)
		checkFile(t, path, "{}")
		require_NoError(t, f.Close())
		c.saveCache(s)
		require_False(t, c.dirty)
		b, err := os.ReadFile(path)
		require_NoError(t, err)
		var saved map[string]OCSPResponseCacheItem
		require_NoError(t, json.Unmarshal(b, &saved))
		require_Equal(t, saved["cert"].Subject, "new")
	})
	t.Run("legacy account directory", func(t *testing.T) {
		root := t.TempDir()
		old := filepath.Join(root, "ACCOUNT")
		cfg := &JetStreamConfig{StoreDir: filepath.Join(root, JetStreamStoreDir)}
		require_NoError(t, os.Mkdir(old, 0700))
		require_NoError(t, os.Mkdir(cfg.StoreDir, 0700))
		path := filepath.Join(old, "sentinel")
		require_NoError(t, os.WriteFile(path, []byte("retained"), 0600))
		s := &Server{}
		s.accounts.Store("ACCOUNT", &Account{Name: "ACCOUNT"})
		f := hold(t, old)
		checkRenameError(t, s.checkStoreDir(cfg))
		checkFile(t, path, "retained")
		require_NoError(t, f.Close())
		require_NoError(t, s.checkStoreDir(cfg))
		checkFile(t, filepath.Join(cfg.StoreDir, "ACCOUNT", "sentinel"), "retained")
	})
	t.Run("v1 restore", func(t *testing.T) {
		s := RunBasicJetStreamServer(t)
		defer s.Shutdown()
		a := s.GlobalAccount()
		_, jsa, err := a.checkForJetStream()
		require_NoError(t, err)
		dir := filepath.Join("..", "test", "configs", "jetstream", "restore_empty_R1F_stream")
		archive, err := os.ReadFile(filepath.Join(dir, "stream.tar.s2"))
		require_NoError(t, err)
		b, err := os.ReadFile(filepath.Join(dir, "backup.json"))
		require_NoError(t, err)
		var req JSApiStreamRestoreRequest
		require_NoError(t, json.Unmarshal(b, &req))
		target := filepath.Join(jsa.storeDir, streamsDir, req.Config.Name)
		require_NoError(t, os.MkdirAll(target, 0700))
		f := hold(t, target)
		_, err = a.RestoreStream(&req.Config, bytes.NewReader(archive))
		checkRenameError(t, err)
		_, err = a.lookupStream(req.Config.Name)
		require_Error(t, err)
		require_NoError(t, f.Close())
		_, err = a.RestoreStream(&req.Config, bytes.NewReader(archive))
		require_NoError(t, err)
		_, err = a.lookupStream(req.Config.Name)
		require_NoError(t, err)
	})
}
