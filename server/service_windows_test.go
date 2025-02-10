// Copyright 2012-2023 The NATS Authors
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
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

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
		wsw     = &winServiceWrapper{New(DefaultOptions())}
		args    = make([]string, 0)
		serverC = make(chan error, 1)
		mockC   = make(chan error, 1)
		changes = make(chan svc.ChangeRequest)
		status  = make(chan svc.Status)
	)
	time.Sleep(time.Millisecond)
	os.Setenv("NATS_STARTUP_DELAY", "1ms") // purposefly small
	// prepare mock expectations
	wsm := &winSvcMock{status: status}
	wsm.Expect(svc.StartPending)
	go func() {
		mockC <- wsm.Listen(50*time.Millisecond, t)
		close(mockC)
	}()

	go func() { // ensure failure with these conditions
		_, exitCode := wsw.Execute(args, changes, status)
		if exitCode == 0 { // expect error
			serverC <- errors.New("Should have exitCode != 0")
		}
		wsw.server.Shutdown()
		close(serverC)
	}()

	for expectedErrs := 2; expectedErrs >= 0; {
		select {
		case err := <-mockC:
			if err != nil {
				t.Fatalf("windows.svc mock: %v", err)
			} else {
				expectedErrs--
			}
		case err := <-serverC:
			if err != nil {
				t.Fatalf("Server behavior: %v", err)
			} else {
				expectedErrs--
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Test timed out")
		}
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
func (w *winSvcMock) Listen(dur time.Duration, t *testing.T) error {
	t.Helper()
	timeout := time.NewTimer(dur)
	defer timeout.Stop()
	for idx, state := range w.expectedSt {
		select {
		case status := <-w.status:
			if status.State == state {
				t.Logf("message %d on status, OK\n", idx)
				continue
			} else {
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
