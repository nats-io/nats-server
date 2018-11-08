// Copyright 2018 The NATS Authors
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

// +build !race

package server

import (
	"fmt"
	"math/rand"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/go-nats"
)

// IMPORTANT: Tests in this file are not executed when running with the -race flag.
//            The test name should be prefixed with TestNoRace so we can run only
//            those tests: go test -run=TestNoRace ...

func TestNoRaceAvoidSlowConsumerBigMessages(t *testing.T) {
	opts := DefaultOptions() // Use defaults to make sure they avoid pending slow consumer.
	s := RunServer(opts)
	defer s.Shutdown()

	nc1, err := nats.Connect(fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc1.Close()

	nc2, err := nats.Connect(fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc2.Close()

	data := make([]byte, 1024*1024) // 1MB payload
	rand.Read(data)

	expected := int32(500)
	received := int32(0)

	done := make(chan bool)

	// Create Subscription.
	nc1.Subscribe("slow.consumer", func(m *nats.Msg) {
		// Just eat it so that we are not measuring
		// code time, just delivery.
		atomic.AddInt32(&received, 1)
		if received >= expected {
			done <- true
		}
	})

	// Create Error handler
	nc1.SetErrorHandler(func(c *nats.Conn, s *nats.Subscription, err error) {
		t.Fatalf("Received an error on the subscription's connection: %v\n", err)
	})

	nc1.Flush()

	for i := 0; i < int(expected); i++ {
		nc2.Publish("slow.consumer", data)
	}
	nc2.Flush()

	select {
	case <-done:
		return
	case <-time.After(10 * time.Second):
		r := atomic.LoadInt32(&received)
		if s.NumSlowConsumers() > 0 {
			t.Fatalf("Did not receive all large messages due to slow consumer status: %d of %d", r, expected)
		}
		t.Fatalf("Failed to receive all large messages: %d of %d\n", r, expected)
	}
}

func TestNoRaceRoutedQueueAutoUnsubscribe(t *testing.T) {
	optsA, _ := ProcessConfigFile("./configs/seed.conf")
	optsA.NoSigs, optsA.NoLog = true, true
	srvA := RunServer(optsA)
	defer srvA.Shutdown()

	srvARouteURL := fmt.Sprintf("nats://%s:%d", optsA.Cluster.Host, srvA.ClusterAddr().Port)
	optsB := nextServerOpts(optsA)
	optsB.Routes = RoutesFromStr(srvARouteURL)

	srvB := RunServer(optsB)
	defer srvB.Shutdown()

	// Wait for these 2 to connect to each other
	checkClusterFormed(t, srvA, srvB)

	// Have a client connection to each server
	ncA, err := nats.Connect(fmt.Sprintf("nats://%s:%d", optsA.Host, optsA.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer ncA.Close()

	ncB, err := nats.Connect(fmt.Sprintf("nats://%s:%d", optsB.Host, optsB.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer ncB.Close()

	rbar := int32(0)
	barCb := func(m *nats.Msg) {
		atomic.AddInt32(&rbar, 1)
	}
	rbaz := int32(0)
	bazCb := func(m *nats.Msg) {
		atomic.AddInt32(&rbaz, 1)
	}

	// Create 125 queue subs with auto-unsubscribe to each server for
	// group bar and group baz. So 250 total per queue group.
	cons := []*nats.Conn{ncA, ncB}
	for _, c := range cons {
		for i := 0; i < 125; i++ {
			qsub, err := c.QueueSubscribe("foo", "bar", barCb)
			if err != nil {
				t.Fatalf("Error on subscribe: %v", err)
			}
			if err := qsub.AutoUnsubscribe(1); err != nil {
				t.Fatalf("Error on auto-unsubscribe: %v", err)
			}
			qsub, err = c.QueueSubscribe("foo", "baz", bazCb)
			if err != nil {
				t.Fatalf("Error on subscribe: %v", err)
			}
			if err := qsub.AutoUnsubscribe(1); err != nil {
				t.Fatalf("Error on auto-unsubscribe: %v", err)
			}
		}
		c.Subscribe("TEST.COMPLETE", func(m *nats.Msg) {})
	}

	// We coelasce now so for each server we will have all local (250) plus
	// two from the remote side for each queue group. We also create one more
	// and will wait til each server has 254 subscriptions, that will make sure
	// that we have everything setup.
	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		subsA := srvA.NumSubscriptions()
		subsB := srvB.NumSubscriptions()
		if subsA != 254 || subsB != 254 {
			return fmt.Errorf("Not all subs processed yet: %d and %d", subsA, subsB)
		}
		return nil
	})

	expected := int32(250)
	// Now send messages from each server
	for i := int32(0); i < expected; i++ {
		c := cons[i%2]
		c.Publish("foo", []byte("Don't Drop Me!"))
	}
	for _, c := range cons {
		c.Flush()
	}

	checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
		nbar := atomic.LoadInt32(&rbar)
		nbaz := atomic.LoadInt32(&rbaz)
		if nbar == expected && nbaz == expected {
			time.Sleep(500 * time.Millisecond)
			return nil
		}
		return fmt.Errorf("Did not receive all %d queue messages, received %d for 'bar' and %d for 'baz'",
			expected, atomic.LoadInt32(&rbar), atomic.LoadInt32(&rbaz))
	})
}

func TestNoRaceClosedSlowConsumerWriteDeadline(t *testing.T) {
	opts := DefaultOptions()
	opts.WriteDeadline = 10 * time.Millisecond // Make very small to trip.
	opts.MaxPending = 500 * 1024 * 1024        // Set high so it will not trip here.
	s := RunServer(opts)
	defer s.Shutdown()

	c, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", opts.Host, opts.Port), 3*time.Second)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer c.Close()
	if _, err := c.Write([]byte("CONNECT {}\r\nPING\r\nSUB foo 1\r\n")); err != nil {
		t.Fatalf("Error sending protocols to server: %v", err)
	}
	// Reduce socket buffer to increase reliability of data backing up in the server destined
	// for our subscribed client.
	c.(*net.TCPConn).SetReadBuffer(128)

	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	sender, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer sender.Close()

	payload := make([]byte, 1024*1024)
	for i := 0; i < 100; i++ {
		if err := sender.Publish("foo", payload); err != nil {
			t.Fatalf("Error on publish: %v", err)
		}
	}

	// Flush sender connection to ensure that all data has been sent.
	if err := sender.Flush(); err != nil {
		t.Fatalf("Error on flush: %v", err)
	}

	// At this point server should have closed connection c.
	checkClosedConns(t, s, 1, 2*time.Second)
	conns := s.closedClients()
	if lc := len(conns); lc != 1 {
		t.Fatalf("len(conns) expected to be %d, got %d\n", 1, lc)
	}
	checkReason(t, conns[0].Reason, SlowConsumerWriteDeadline)
}

func TestNoRaceClosedSlowConsumerPendingBytes(t *testing.T) {
	opts := DefaultOptions()
	opts.WriteDeadline = 30 * time.Second // Wait for long time so write deadline does not trigger slow consumer.
	opts.MaxPending = 1 * 1024 * 1024     // Set to low value (1MB) to allow SC to trip.
	s := RunServer(opts)
	defer s.Shutdown()

	c, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", opts.Host, opts.Port), 3*time.Second)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer c.Close()
	if _, err := c.Write([]byte("CONNECT {}\r\nPING\r\nSUB foo 1\r\n")); err != nil {
		t.Fatalf("Error sending protocols to server: %v", err)
	}
	// Reduce socket buffer to increase reliability of data backing up in the server destined
	// for our subscribed client.
	c.(*net.TCPConn).SetReadBuffer(128)

	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	sender, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer sender.Close()

	payload := make([]byte, 1024*1024)
	for i := 0; i < 100; i++ {
		if err := sender.Publish("foo", payload); err != nil {
			t.Fatalf("Error on publish: %v", err)
		}
	}

	// Flush sender connection to ensure that all data has been sent.
	if err := sender.Flush(); err != nil {
		t.Fatalf("Error on flush: %v", err)
	}

	// At this point server should have closed connection c.
	checkClosedConns(t, s, 1, 2*time.Second)
	conns := s.closedClients()
	if lc := len(conns); lc != 1 {
		t.Fatalf("len(conns) expected to be %d, got %d\n", 1, lc)
	}
	checkReason(t, conns[0].Reason, SlowConsumerPendingBytes)
}

func TestNoRaceSlowConsumerPendingBytes(t *testing.T) {
	opts := DefaultOptions()
	opts.WriteDeadline = 30 * time.Second // Wait for long time so write deadline does not trigger slow consumer.
	opts.MaxPending = 1 * 1024 * 1024     // Set to low value (1MB) to allow SC to trip.
	s := RunServer(opts)
	defer s.Shutdown()

	c, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", opts.Host, opts.Port), 3*time.Second)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer c.Close()
	if _, err := c.Write([]byte("CONNECT {}\r\nPING\r\nSUB foo 1\r\n")); err != nil {
		t.Fatalf("Error sending protocols to server: %v", err)
	}
	// Reduce socket buffer to increase reliability of data backing up in the server destined
	// for our subscribed client.
	c.(*net.TCPConn).SetReadBuffer(128)

	url := fmt.Sprintf("nats://%s:%d", opts.Host, opts.Port)
	sender, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer sender.Close()

	payload := make([]byte, 1024*1024)
	for i := 0; i < 100; i++ {
		if err := sender.Publish("foo", payload); err != nil {
			t.Fatalf("Error on publish: %v", err)
		}
	}

	// Flush sender connection to ensure that all data has been sent.
	if err := sender.Flush(); err != nil {
		t.Fatalf("Error on flush: %v", err)
	}

	// At this point server should have closed connection c.

	// On certain platforms, it may take more than one call before
	// getting the error.
	for i := 0; i < 100; i++ {
		if _, err := c.Write([]byte("PUB bar 5\r\nhello\r\n")); err != nil {
			// ok
			return
		}
	}
	t.Fatal("Connection should have been closed")
}
