package server

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/nats-io/nats.go"
)

var (
	serverUrl = "nats://127.0.0.1:18001,nats://127.0.0.1:18002,nats://127.0.0.1:18003,nats://127.0.0.1:18004,nats://127.0.0.1:18005"

)

func BenchmarkJetStreamPublishConcurrentJnm(b *testing.B) {

	const (
		subject    = "test-subject"
		streamName = "test-stream"
	)

	type BenchPublisher struct {
		// nats connection for this publisher
		conn *nats.Conn
		// jetstream context
		js nats.JetStreamContext
		// message buffer
		messageData []byte
		// number of publish calls
		publishCalls int
		// number of publish errors
		publishErrors int
	}

	messageSizeCases := []int64{
		10,     // 10B
		1024,   // 1KiB
		102400, // 100KiB
	}
	numPubsCases := []int{
		12,
	}

	replicasCases := []struct {
		clusterSize int
		replicas    int
	}{
		{1, 1},
		{3, 3},
	}

	workload := func(b *testing.B, numPubs int, messageSize int64, connectUrl string) {

		// create N publishers
		publishers := make([]BenchPublisher, numPubs)
		for i := range publishers {
			// create publisher connection and jetstream context
			ncPub, err := nats.Connect(connectUrl)
			if err != nil {
				b.Fatal(err)
			}
			defer ncPub.Close()
			jsPub, err := ncPub.JetStream()
			if err != nil {
				b.Fatal(err)
			}

			// initialize publisher
			publishers[i] = BenchPublisher{
				conn:          ncPub,
				js:            jsPub,
				messageData:   make([]byte, messageSize),
				publishCalls:  0,
				publishErrors: 0,
			}
			rand.New(rand.NewSource(int64(i))).Read(publishers[i].messageData)
		}

		// waits for all publishers sub-routines and for main thread to be ready
		var workloadReadyWg sync.WaitGroup
		workloadReadyWg.Add(1 + numPubs)

		// wait group blocks main thread until publish workload is completed, it is decremented after stream receives b.N messages from all publishers
		var benchCompleteWg sync.WaitGroup
		benchCompleteWg.Add(1)

		// wait group to ensure all publishers have been torn down
		var finishedPublishersWg sync.WaitGroup
		finishedPublishersWg.Add(numPubs)

		// start go routines for all publishers, wait till all publishers are initialized before starting publish workload
		for i := range publishers {

			go func(pubId int) {
				// signal that this publisher has been torn down
				defer finishedPublishersWg.Done()

				// publisher sub-routine is ready
				workloadReadyWg.Done()

				// start workload when main thread and all other publishers are ready
				workloadReadyWg.Wait()

				// publish until stream receives b.N messages
				for {
					// random bytes as payload
					fastRandomMutation(publishers[pubId].messageData, 10)
					// attempt to publish message
					pubAck, err := publishers[pubId].js.Publish(subject, publishers[pubId].messageData)
					publishers[pubId].publishCalls += 1
					if err != nil {
						publishers[pubId].publishErrors += 1
						continue
					}
					// all messages have been published to stream
					if pubAck.Sequence == uint64(b.N) {
						benchCompleteWg.Done()
					}
					// a publisher has already published b.N messages, stop publishing
					if pubAck.Sequence >= uint64(b.N) {
						return
					}
				}
			}(i)
		}

		// set bytes per operation
		b.SetBytes(messageSize)

		// main thread is ready
		workloadReadyWg.Done()
		// start the clock
		b.ResetTimer()

		// wait till termination cond reached
		benchCompleteWg.Wait()
		// stop the clock
		b.StopTimer()

		// wait for all publishers to shutdown
		finishedPublishersWg.Wait()

		// sum up publish calls and errors
		publishCalls := 0
		publishErrors := 0
		for _, pub := range publishers {
			publishCalls += pub.publishCalls
			publishErrors += pub.publishErrors
		}

		// report error rate
		errorRate := 100 * float64(publishErrors) / float64(publishCalls)
		b.ReportMetric(errorRate, "%error")
	}

	// benchmark case matrix
	for _, replicasCase := range replicasCases {
		b.Run(
			fmt.Sprintf("N=%d,R=%d", replicasCase.clusterSize, replicasCase.replicas),
			func(b *testing.B) {
				for _, messageSize := range messageSizeCases {
					b.Run(
						fmt.Sprintf("msgSz=%db", messageSize),
						func(b *testing.B) {
							for _, numPubs := range numPubsCases {
								b.Run(
									fmt.Sprintf("pubs=%d", numPubs),
									func(b *testing.B) {

										// open connection
										nc, err := nats.Connect(serverUrl, nats.MaxReconnects(-1))
										if err != nil {
											b.Fatal(err)
										}

										js, err := nc.JetStream()
										if err != nil {
											b.Fatal(err)
										}

										defer nc.Close()

										// create stream
										_, err = js.AddStream(&nats.StreamConfig{
											Name:     streamName,
											Subjects: []string{subject},
											Replicas: replicasCase.replicas,
										})
										if err != nil {
											b.Fatal(err)
										}
										defer js.DeleteStream(streamName)

										// run workload
										workload(b, numPubs, messageSize, serverUrl)
									},
								)
							}
						})
				}
			})
	}
}
