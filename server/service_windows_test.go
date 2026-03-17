// Copyright 2012-2025 The NATS Authors
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
