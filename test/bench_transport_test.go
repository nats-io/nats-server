package test

import (
	"fmt"
	"net"
	"os"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
)

type dialFunc func() (net.Conn, error)

func dialTCP(port int) dialFunc {
	return func() (net.Conn, error) {
		return net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 3*time.Second)
	}
}

func dialUDS(path string) dialFunc {
	return func() (net.Conn, error) {
		return net.DialTimeout("unix", path, 3*time.Second)
	}
}

func dialInProc(s *server.Server) dialFunc {
	return func() (net.Conn, error) {
		return s.InProcessConn()
	}
}

func runServerTCP(port int) *server.Server {
	opts := DefaultTestOptions
	opts.Port = port
	opts.DisableShortFirstPing = true
	s := RunServer(&opts)
	s.ReadyForConnections(5 * time.Second)
	return s
}

func runServerUDS(path string) *server.Server {
	os.Remove(path)
	opts := DefaultTestOptions
	opts.Port = -1
	opts.DontListen = true
	opts.UDS.Path = path
	opts.DisableShortFirstPing = true
	s := RunServer(&opts)
	s.ReadyForConnections(5 * time.Second)
	return s
}

func runServerInProc() *server.Server {
	opts := DefaultTestOptions
	opts.Port = -1
	opts.DontListen = true
	opts.DisableShortFirstPing = true
	s := RunServer(&opts)
	s.ReadyForConnections(5 * time.Second)
	return s
}

// Read and discard possibly multiple responses non-blocking.
// Simulate async read on client without consuming threads.
func drainRead(c net.Conn, buf []byte) {
	c.SetReadDeadline(time.Now())
	_, err := c.Read(buf)
	if err != nil {
		return
	}
	for {
		c.SetReadDeadline(time.Now())
		_, err = c.Read(buf)
		if err != nil {
			return
		}
	}
}

func sendRecv(c net.Conn, msg []byte, n int) time.Duration {
	buf := make([]byte, 4096)
	start := time.Now()
	for i := 0; i < n; i++ {
		c.Write(msg)
		drainRead(c, buf)
	}
	return time.Since(start)
}

func benchMsg(b *testing.B, dial dialFunc, msg []byte, numClients int) {
	b.StopTimer()

	conns := make([]net.Conn, numClients)
	for i := range conns {
		c, err := dial()
		if err != nil {
			b.Fatalf("dial failed: %v", err)
		}
		conns[i] = c
		doDefaultConnect(b, c)
	}
	defer func() {
		for _, c := range conns {
			c.Close()
		}
	}()

	msgsPerClient := b.N / numClients
	if msgsPerClient < 1 {
		msgsPerClient = 1
	}

	var wg sync.WaitGroup
	times := make([]time.Duration, numClients)
	start := make(chan struct{})

	for i := range conns {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			times[idx] = sendRecv(conns[idx], msg, msgsPerClient)
		}(i)
	}

	b.StartTimer()
	close(start)
	wg.Wait()
	b.StopTimer()

	nsPerMsg := make([]float64, numClients)
	for i, d := range times {
		nsPerMsg[i] = float64(d.Nanoseconds()) / float64(msgsPerClient)
	}
	slices.Sort(nsPerMsg)

	var sum float64
	for _, ns := range nsPerMsg {
		sum += ns
	}

	b.ReportMetric(nsPerMsg[0], "ns/msg-min")
	b.ReportMetric(nsPerMsg[len(nsPerMsg)-1], "ns/msg-max")
	b.ReportMetric(sum/float64(numClients), "ns/msg-avg")
	b.ReportMetric(nsPerMsg[len(nsPerMsg)/2], "ns/msg-med")
}

func benchTransports(b *testing.B, msg []byte, numClients int) {
	b.Run("TCP", func(b *testing.B) {
		s := runServerTCP(8423)
		defer s.Shutdown()
		benchMsg(b, dialTCP(8423), msg, numClients)
	})
	b.Run("UDS", func(b *testing.B) {
		s := runServerUDS("/tmp/nats-bench.sock")
		defer s.Shutdown()
		benchMsg(b, dialUDS("/tmp/nats-bench.sock"), msg, numClients)
	})
	b.Run("InProc", func(b *testing.B) {
		s := runServerInProc()
		defer s.Shutdown()
		benchMsg(b, dialInProc(s), msg, numClients)
	})
}

var ping = []byte("PING\r\n")

func makePub(size int) []byte {
	// PUB x <size>\r\n<payload>\r\n
	header := fmt.Sprintf("PUB x %d\r\n", size)
	msg := make([]byte, len(header)+size+2)
	for i := range msg {
		msg[i] = '.'
	}
	copy(msg, header)
	copy(msg[len(msg)-2:], "\r\n")
	return msg
}

var (
	pub256 = makePub(256)
	pub512 = makePub(512)
	pub1k  = makePub(1024)
)

func Benchmark_Transport_Ping_1(b *testing.B)          { benchTransports(b, ping, 1) }
func Benchmark_Transport_Ping_2(b *testing.B)          { benchTransports(b, ping, 2) }
func Benchmark_Transport_Ping_3(b *testing.B)          { benchTransports(b, ping, 3) }
func Benchmark_Transport_Ping_4(b *testing.B)          { benchTransports(b, ping, 4) }
func Benchmark_Transport_Ping_5(b *testing.B)          { benchTransports(b, ping, 5) }
func Benchmark_Transport_Ping_Cores_x0_5(b *testing.B) { benchTransports(b, ping, runtime.NumCPU()/2) }
func Benchmark_Transport_Ping_Cores_x1(b *testing.B)   { benchTransports(b, ping, runtime.NumCPU()) }
func Benchmark_Transport_Ping_Cores_x1_5(b *testing.B) {
	benchTransports(b, ping, runtime.NumCPU()*3/2)
}

func Benchmark_Transport_Pub256_1(b *testing.B) { benchTransports(b, pub256, 1) }
func Benchmark_Transport_Pub256_4(b *testing.B) { benchTransports(b, pub256, 4) }
func Benchmark_Transport_Pub256_Cores_x0_5(b *testing.B) {
	benchTransports(b, pub256, runtime.NumCPU()/2)
}
func Benchmark_Transport_Pub256_Cores_x1(b *testing.B) { benchTransports(b, pub256, runtime.NumCPU()) }
func Benchmark_Transport_Pub256_Cores_x1_5(b *testing.B) {
	benchTransports(b, pub256, runtime.NumCPU()*3/2)
}

func Benchmark_Transport_Pub512_1(b *testing.B) { benchTransports(b, pub512, 1) }
func Benchmark_Transport_Pub512_4(b *testing.B) { benchTransports(b, pub512, 4) }
func Benchmark_Transport_Pub512_Cores_x0_5(b *testing.B) {
	benchTransports(b, pub512, runtime.NumCPU()/2)
}
func Benchmark_Transport_Pub512_Cores_x1(b *testing.B) { benchTransports(b, pub512, runtime.NumCPU()) }
func Benchmark_Transport_Pub512_Cores_x1_5(b *testing.B) {
	benchTransports(b, pub512, runtime.NumCPU()*3/2)
}

func Benchmark_Transport_Pub1k_1(b *testing.B) { benchTransports(b, pub1k, 1) }
func Benchmark_Transport_Pub1k_4(b *testing.B) { benchTransports(b, pub1k, 4) }
func Benchmark_Transport_Pub1k_Cores_x0_5(b *testing.B) {
	benchTransports(b, pub1k, runtime.NumCPU()/2)
}
func Benchmark_Transport_Pub1k_Cores_x1(b *testing.B) { benchTransports(b, pub1k, runtime.NumCPU()) }
func Benchmark_Transport_Pub1k_Cores_x1_5(b *testing.B) {
	benchTransports(b, pub1k, runtime.NumCPU()*3/2)
}
