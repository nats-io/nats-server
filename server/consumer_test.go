package server

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestJetStreamWorkQueueMultiConsumer(t *testing.T) {
	s := RunBasicJetStreamServer(t)
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
				pa, err := js.PublishAsync(fmt.Sprintf("test.1.%d", cnt), nil)
				require_NoError(t, err)
				pas = append(pas, pa)

				pa, err = js.PublishAsync(fmt.Sprintf("test.2.%d", cnt), nil)
				require_NoError(t, err)
				pas = append(pas, pa)

				pa, err = js.PublishAsync(fmt.Sprintf("test.3.%d", cnt), nil)
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

	wg := &sync.WaitGroup{}
	wg.Add(3)

	// Start three consumers that will consume from the work queue
	// each with a different filter subject bound to the second token.
	for i := 0; i < 3; i++ {
		_, err := js.AddConsumer("WQ", &nats.ConsumerConfig{
			Durable:       fmt.Sprintf("WQ-%d", i+1),
			AckPolicy:     nats.AckExplicitPolicy,
			FilterSubject: fmt.Sprintf("test.%d.*", i+1),
		})
		require_NoError(t, err)

		// Start each worker in a routine.
		go func(i int) {
			t.Logf("[%d] Starting worker", i+1)
			defer wg.Done()

			// Setup the initial pull subscription bound to the consumer.
			sub, err := js.PullSubscribe(
				fmt.Sprintf("test.%d.*", i+1),
				fmt.Sprintf("WQ-%d", i+1),
				nats.Bind("WQ", fmt.Sprintf("WQ-%d", i+1)),
			)
			require_NoError(t, err)
			defer sub.Drain()

			// Keep track of the last message counter which is determined
			// based on the third token in the subject.
			rem := 3_000_000
			lstCnt := 0
			for rem > 0 {
				// Randomly drain the consumer and rebind the consumer every so often.
				if rand.Intn(100)%5 == 0 {
					err = sub.Drain()
					require_NoError(t, err)

					// Rebind with a new subscription.
					sub, err = js.PullSubscribe(
						fmt.Sprintf("test.%d.*", i+1),
						fmt.Sprintf("WQ-%d", i+1),
						nats.Bind("WQ", fmt.Sprintf("WQ-%d", i+1)),
					)
					require_NoError(t, err)
				}

				// Fetch a random number of messages up to 100.
				n := rand.Intn(100) + 1
				msgs, err := sub.Fetch(n, nats.MaxWait(500*time.Millisecond))
				if err == nats.ErrTimeout {
					continue
				}
				require_NoError(t, err)

				for _, msg := range msgs {
					// Check that the counter is only +1 than the last.
					cnt, err := strconv.Atoi(msg.Subject[7:])
					require_NoError(t, err)
					if cnt == lstCnt+1 {
						lstCnt = cnt
						msg.Ack()
						continue
					}

					// A mismatch is detected. Log the consumer, metadata, and return.
					md, _ := msg.Metadata()
					mdd, err := json.Marshal(md)
					require_NoError(t, err)
					ci, err := js.ConsumerInfo("WQ", fmt.Sprintf("WQ-%d", i+1))
					require_NoError(t, err)
					cis := fmt.Sprintf("Delivered: %d, AckFloor: %d, Pending: %d\n", ci.Delivered.Consumer, ci.AckFloor.Consumer, ci.NumPending)

					t.Errorf("[%d] Expected %d, got %d\nMetadata:\n%s\nConsumer info:\n%s", i, lstCnt+1, cnt, mdd, cis)
					return
				}

				rem -= len(msgs)
			}
		}(i)
	}

	wg.Wait()
}
