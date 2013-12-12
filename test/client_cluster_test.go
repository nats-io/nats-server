// Copyright 2013 Apcera Inc. All rights reserved.

package test

import (
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apcera/nats"
)

func TestServerRestartReSliceIssue(t *testing.T) {
	srvA, srvB, optsA, optsB := runServers(t)
	defer srvA.Shutdown()

	urlA := fmt.Sprintf("nats://%s:%d/", optsA.Host, optsA.Port)
	urlB := fmt.Sprintf("nats://%s:%d/", optsB.Host, optsB.Port)

	// msg to send..
	msg := []byte("Hello World")

	servers := []string{urlA, urlB}

	opts := nats.DefaultOptions
	opts.Timeout = (5 * time.Second)
	opts.ReconnectWait = (50 * time.Millisecond)
	opts.MaxReconnect = 1000

	reconnects := int32(0)
	reconnectsDone := make(chan bool)
	opts.ReconnectedCB = func(nc *nats.Conn) {
		atomic.AddInt32(&reconnects, 1)
		reconnectsDone <- true
	}

	// Create 20 random clients.
	// Half connected to A and half to B..
	numClients := 20
	for i := 0; i < numClients; i++ {
		opts.Url = servers[i%2]
		nc, err := opts.Connect()
		defer nc.Close()

		if err != nil {
			t.Fatalf("Failed to create connection: %v\n", err)
		}
		// Create 10 subscriptions each..
		for x := 0; x < 10; x++ {
			subject := fmt.Sprintf("foo.%d", (rand.Int()%50)+1)
			nc.Subscribe(subject, func(m *nats.Msg) {
				// Just eat it..
			})
		}
		// Pick one subject to send to..
		subject := fmt.Sprintf("foo.%d", (rand.Int()%50)+1)
		go func() {
			time.Sleep(10 * time.Millisecond)
			for i := 1; 1 <= 100; i++ {
				nc.Publish(subject, msg)
				if i%10 == 0 {
					time.Sleep(time.Millisecond)
				}
			}
		}()
	}

	// Wait for a short bit..
	time.Sleep(20 * time.Millisecond)

	// Restart SrvB
	srvB.Shutdown()
	srvB = RunServer(optsB)
	defer srvB.Shutdown()

	select {
	case <-reconnectsDone:
		break
	case <-time.After(2 * time.Second):
		t.Fatalf("Expected %d reconnects, got %d\n", numClients/2, reconnects)
	}
}
