// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");

package server

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

type issue7801TraceLogger struct {
	DummyLogger
	mu    sync.Mutex
	lines []string
}

func (l *issue7801TraceLogger) Tracef(format string, args ...any) {
	if len(args) == 1 {
		if nested, ok := args[0].([]any); ok {
			args = nested
		}
	}
	l.mu.Lock()
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
	l.mu.Unlock()
}

func (l *issue7801TraceLogger) countIngress(subject string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	count := 0
	for _, line := range l.lines {
		if strings.Contains(line, "<<-") && strings.Contains(line, "PUB") && strings.Contains(line, subject) {
			count++
		}
	}
	return count
}

// Regression test for issue #7801. The externally observable contract is one
// publish request, one PubAck, and one stored stream message after concurrent,
// equivalent stream updates.
func TestJetStreamIssue7801ConcurrentStreamUpdatesDoNotMultiplyPublish(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	mset, err := s.GlobalAccount().addStream(&StreamConfig{
		Name:     "ISSUE_7801",
		Subjects: []string{"issue.7801.seed"},
		Storage:  FileStorage,
	})
	if err != nil {
		t.Fatalf("error adding stream: %v", err)
	}

	// The report used a large subject set and several clients updated the same
	// stream concurrently. A larger set makes the stale-snapshot race reliable
	// without inserting timing hooks into the production path.
	subjects := make([]string, 2048)
	for i := range subjects {
		subjects[i] = fmt.Sprintf("issue.7801.subject.%d", i)
	}
	publishSubject := subjects[len(subjects)/2]

	const updaters = 8
	updateClients := make([]*nats.Conn, updaters)
	for i := range updateClients {
		updateClients[i] = clientConnectToServer(t, s)
		defer updateClients[i].Close()
	}
	cfg := mset.config()
	cfg.Subjects = append([]string(nil), subjects...)
	updateRequest, err := json.Marshal(&cfg)
	if err != nil {
		t.Fatalf("error marshaling stream update: %v", err)
	}
	start := make(chan struct{})
	errs := make(chan error, updaters)
	var wg sync.WaitGroup
	for _, nc := range updateClients {
		wg.Add(1)
		go func(nc *nats.Conn) {
			defer wg.Done()
			<-start
			msg, err := nc.Request(fmt.Sprintf(JSApiStreamUpdateT, cfg.Name), updateRequest, 30*time.Second)
			if err == nil {
				var response JSApiStreamUpdateResponse
				if err = json.Unmarshal(msg.Data, &response); err == nil && response.Error != nil {
					err = response.Error
				}
			}
			errs <- err
		}(nc)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent stream update failed: %v", err)
		}
	}

	observer := clientConnectToServer(t, s)
	defer observer.Close()
	js, err := observer.JetStream()
	if err != nil {
		t.Fatalf("error creating JetStream context: %v", err)
	}
	consumer, err := js.PullSubscribe(publishSubject, "ISSUE_7801_CONSUMER",
		nats.BindStream(cfg.Name), nats.ManualAck())
	if err != nil {
		t.Fatalf("error creating observation consumer: %v", err)
	}

	trace := &issue7801TraceLogger{}
	s.SetLogger(trace, false, true)
	nc := clientConnectToServer(t, s)
	defer nc.Close()
	reply := nats.NewInbox()
	acks, err := nc.SubscribeSync(reply)
	if err != nil {
		t.Fatalf("error subscribing for publish acknowledgements: %v", err)
	}
	if err := nc.PublishRequest(publishSubject, reply, []byte("one logical publish")); err != nil {
		t.Fatalf("publish failed: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	ackCount := 0
	for {
		if _, err := acks.NextMsg(100 * time.Millisecond); err == nats.ErrTimeout {
			break
		} else if err != nil {
			t.Fatalf("error receiving publish acknowledgement: %v", err)
		}
		ackCount++
	}
	state := mset.state()
	deliveries, err := consumer.Fetch(int(state.Msgs), nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("error fetching stored messages: %v", err)
	}
	sequences := make([]uint64, 0, len(deliveries))
	redeliveryCount := 0
	for _, msg := range deliveries {
		metadata, err := msg.Metadata()
		if err != nil {
			t.Fatalf("error reading delivery metadata: %v", err)
		}
		sequences = append(sequences, metadata.Sequence.Stream)
		if metadata.NumDelivered > 1 {
			redeliveryCount++
		}
		if err := msg.Ack(); err != nil {
			t.Fatalf("error acknowledging delivery: %v", err)
		}
	}
	ingressCount := trace.countIngress(publishSubject)
	if ingressCount != 1 || ackCount != 1 || state.Msgs != 1 || state.LastSeq != 1 || len(deliveries) != 1 || redeliveryCount != 0 {
		t.Fatalf("one publish multiplied: ingresses=%d acks=%d messages=%d last_seq=%d deliveries=%d redeliveries=%d sequences=%v",
			ingressCount, ackCount, state.Msgs, state.LastSeq, len(deliveries), redeliveryCount, sequences)
	}
}
