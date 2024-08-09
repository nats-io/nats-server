package server

import (
	"crypto/rand"
	"testing"
)

func TestJetstreamAccountFileStoreLimits(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	gacc := s.GlobalAccount()

	limits := map[string]JetStreamAccountLimits{
		"R1": {
			MaxMemory:    1 << 10,
			MaxStore:     1 << 10,
			MaxStreams:   -1,
			MaxConsumers: -1,
		},
	}

	if err := gacc.UpdateJetStreamLimits(limits); err != nil {
		t.Fatalf("Unexpected error updating jetstream account limits: %v", err)
	}

	sname := "test-stream"
	sconfig := &StreamConfig{Name: sname, Storage: FileStorage, Retention: LimitsPolicy}

	sset, err := gacc.addStream(sconfig)
	if err != nil {
		t.Fatalf("Unexpected error creating stream: %v", err)
	}
	defer sset.delete()

	st := sset.state()
	if st.Msgs != 0 {
		t.Fatalf("Unexpected number of messages: expected 0, got %d (%+v)", st.Msgs, st)
	}

	nc := clientConnectToServer(t, s)
	defer nc.Close()

	data := make([]byte, 1<<8)

	for i := 0; i < 10; i++ {
		if _, err := rand.Read(data); err != nil {
			t.Fatalf("Unexpected error generating random data: %v", err)
		}

		if err = nc.Publish(sname, data); err != nil {
			t.Fatalf("Unexpected error publishing random data (iteration %d): %v", i, err)
		}

		if err = nc.Flush(); err != nil {
			t.Fatalf("Unexpected error flushing connection: %v", err)
		}

		st = sset.state()
		t.Logf("Iteration %d - # msgs = %d, size = %d", i, st.Msgs, st.Bytes)
	}

	st = sset.stateWithDetail(true)
	if int64(st.Bytes) > limits["R1"].MaxStore {
		t.Fatalf("Unexpected size of stream: got %d, expected less than %d\nstate: %#v", st.Bytes, limits["R1"].MaxStore, st)
	}
}
