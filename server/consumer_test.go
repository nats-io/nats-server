package server

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestJetStreamWorkQueueMultiConsumer(t *testing.T) {
	s := RunJetStreamServerOnPort(53951, t.TempDir())
	defer s.Shutdown()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Create a work queue stream where the second wildcard will be
	// used to filter messages to three different consumers.
	_, err := js.AddStream(&nats.StreamConfig{
		Name:      "WQ",
		Subjects:  []string{"test.*.*"},
		Retention: nats.WorkQueuePolicy,
	})
	require_NoError(t, err)

	// Start a routine to publish messages to the stream.
	// The third token in the subject is a relative counter
	// for that specific consumer, monotonically increasing.
	// **NOTE**, it is assumed that PublishAsync will
	// be received by the server and processed in order.
	go func() {
		cnt := 0
		for j := 0; j < 1000; j++ {
			// Each loop will generate 3000 messages, 1000 for each consumer.
			var pas []nats.PubAckFuture
			for i := 0; i < 1000; i++ {
				cnt++
				pa, err := js.PublishAsync(fmt.Sprintf("test.0.%d", cnt), nil)
				require_NoError(t, err)
				pas = append(pas, pa)

				pa, err = js.PublishAsync(fmt.Sprintf("test.1.%d", cnt), nil)
				require_NoError(t, err)
				pas = append(pas, pa)

				pa, err = js.PublishAsync(fmt.Sprintf("test.2.%d", cnt), nil)
				require_NoError(t, err)
				pas = append(pas, pa)
			}

			<-js.PublishAsyncComplete()

			for _, pa := range pas {
				select {
				case <-pa.Ok():
				case err := <-pa.Err():
					require_NoError(t, err)
				}
			}
		}
		t.Logf("Published %d messages", cnt*3)
	}()

	// Change to 0, 1, or 2 to test each consumer.
	worker := 0
	// Set to true to periodically unsubscribe and rebind the consumer.
	toggleUnsub := false

	// Keep track of the last message counter which is determined
	// based on the third token in the subject.
	totalRem := 1_000_000
	// Keep track of the left over messages that were requested, but not received.
	subRem := 0
	// Keep track of the last message counter that was received.
	lstCnt := 0

	// Start three consumers that will consume from the work queue
	// each with a different filter subject bound to the second token.
	_, err = js.AddConsumer("WQ", &nats.ConsumerConfig{
		Durable:       fmt.Sprintf("WQ-%d", worker),
		AckPolicy:     nats.AckExplicitPolicy,
		FilterSubject: fmt.Sprintf("test.%d.*", worker),
	})
	require_NoError(t, err)

	// Start each worker in a routine.
	t.Logf("[%d] Starting worker", worker)

	// Setup the initial pull subscription bound to the consumer.
	sub, err := js.PullSubscribe(
		fmt.Sprintf("test.%d.*", worker),
		fmt.Sprintf("WQ-%d", worker),
		nats.Bind("WQ", fmt.Sprintf("WQ-%d", worker)),
	)
	require_NoError(t, err)

	for totalRem > 0 {
		// Fetch a random number of messages up to 512.
		n := rand.Intn(512) + 1
		t0 := time.Now()
		msgs, err := sub.Fetch(n, nats.MaxWait(2*time.Second))
		if err == nats.ErrTimeout {
			t.Logf("[%d] Timeout fetching %d messages in %v", worker, n, time.Since(t0))
			continue
		}
		require_NoError(t, err)
		if len(msgs) != n {
			t.Logf("[%d] Fetched %d/%d messages in %v", worker, len(msgs), n, time.Since(t0))
		}

		for _, msg := range msgs {
			md, _ := msg.Metadata()
			if md.NumDelivered > 1 {
				t.Errorf("Expected only one delivery, got %d", md.NumDelivered)
				return
			}
			// Check that the counter is only +1 than the last.
			cnt, err := strconv.Atoi(msg.Subject[7:])
			require_NoError(t, err)
			if cnt == lstCnt+1 {
				lstCnt = cnt
				msg.Ack()
				continue
			}

			// A mismatch is detected. Log the consumer, metadata, and return.
			mdd, err := json.Marshal(md)
			require_NoError(t, err)
			ci, err := js.ConsumerInfo("WQ", fmt.Sprintf("WQ-%d", worker))
			require_NoError(t, err)
			cis := fmt.Sprintf("Delivered: %d, AckFloor: %d, Pending: %d\n", ci.Delivered.Consumer, ci.AckFloor.Consumer, ci.NumPending)

			t.Errorf("[%d] Expected %d, got %d\nMetadata:\n%s\nConsumer info:\n%s", worker, lstCnt+1, cnt, mdd, cis)
			return
		}

		// Keep track of the messages that were requested vs. received. The remaining will be fetched
		// prior to unsubscribing.
		totalRem -= len(msgs)
		subRem += (n - len(msgs))

		// Randomly unsub the consumer and rebind a fifth of the time.
		if toggleUnsub && rand.Intn(100)%20 == 0 {
			if subRem > 0 {
				t.Logf("[%d] Prepping drain.. %d remaining messages", worker, subRem)
			}

			// Ensure outstanding queued/pending messages are processed.
			for subRem > 0 {
				t0 := time.Now()
				msgs, err := sub.Fetch(1, nats.MaxWait(time.Second))
				if err == nats.ErrTimeout {
					t.Logf("[%d] Timeout fetching 1 message in %v", worker, time.Since(t0))
					continue
				}
				require_NoError(t, err)

				subRem -= len(msgs)
				totalRem -= len(msgs)

				for _, msg := range msgs {
					md, _ := msg.Metadata()
					if md.NumDelivered > 1 {
						t.Errorf("Expected only one delivery, got %d", md.NumDelivered)
						return
					}

					cnt, err := strconv.Atoi(msg.Subject[7:])
					require_NoError(t, err)
					if cnt == lstCnt+1 {
						lstCnt = cnt
						msg.Ack()
						continue
					}

					mdd, err := json.Marshal(md)
					require_NoError(t, err)
					ci, err := js.ConsumerInfo("WQ", fmt.Sprintf("WQ-%d", worker))
					require_NoError(t, err)
					cis := fmt.Sprintf("Delivered: %d, AckFloor: %d, Pending: %d\n", ci.Delivered.Consumer, ci.AckFloor.Consumer, ci.NumPending)

					t.Errorf("[%d] Expected %d, got %d\nMetadata:\n%s\nConsumer info:\n%s", worker, lstCnt+1, cnt, mdd, cis)
					return
				}
			}

			err = sub.Unsubscribe()
			require_NoError(t, err)

			// Rebind with a new subscription.
			sub, err = js.PullSubscribe(
				fmt.Sprintf("test.%d.*", worker),
				fmt.Sprintf("WQ-%d", worker),
				nats.Bind("WQ", fmt.Sprintf("WQ-%d", worker)),
			)
			require_NoError(t, err)
		}
	}

	t.Logf("[%d] Finished consuming %d messages", worker, lstCnt)

	sub.Unsubscribe()

	// Allow stream to catch up deleted messages.
	time.Sleep(2 * time.Second)

	si, err := js.StreamInfo("WQ")
	require_NoError(t, err)
	require_Equal(t, lstCnt, si.State.NumDeleted)
}
