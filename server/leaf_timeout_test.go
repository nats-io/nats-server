package server

import (
    "net"
    "net/url"
    "strings"
    "testing"
    "time"
)

type slowConn struct{ net.Conn }
func (c *slowConn) Read(b []byte) (int, error)  { time.Sleep(1500 * time.Millisecond); return c.Conn.Read(b) }
func (c *slowConn) Write(b []byte) (int, error) { time.Sleep(1500 * time.Millisecond); return c.Conn.Write(b) }

type slowListener struct{ net.Listener }
func (l *slowListener) Accept() (net.Conn, error) {
    c, err := l.Listener.Accept()
    if err != nil { return nil, err }
    return &slowConn{c}, nil
}

func TestLeafNodeHighLatencyWithConfigurableTimeouts(t *testing.T) {
    // Hub server with slow leaf listener to simulate >1s latency
    oa := DefaultOptions()
    oa.Port = RANDOM_PORT
    oa.LeafNode.Port = RANDOM_PORT
    sa := RunServer(oa)
    t.Cleanup(sa.Shutdown)

    // Wrap leaf listener with injected latency
    if sa.leafNodeListener == nil {
        t.Skip("leaf node listener not initialized")
    }
    sa.leafNodeListener = &slowListener{sa.leafNodeListener}

    // Build leaf URL to hub
    addr := sa.leafNodeListener.Addr().String() // e.g. 0.0.0.0:7422 or [::]:7422
    host, port, err := net.SplitHostPort(addr)
    if err != nil { t.Fatalf("split host:port: %v", err) }
    if host == "0.0.0.0" || host == "::" || host == "[::]" { host = "127.0.0.1" }
    u, _ := url.Parse("nats://" + net.JoinHostPort(host, port))

    // Spoke server configured with longer timeouts to tolerate latency
    ob := DefaultOptions()
    ob.Port = RANDOM_PORT
    ob.LeafNode.Port = RANDOM_PORT
    // For tests we can set internal dialTimeout directly
    ob.LeafNode.dialTimeout = 3 * time.Second
    ob.LeafNode.Remotes = []*RemoteLeafOpts{{
        URLs:             []*url.URL{u},
        FirstInfoTimeout: 3 * time.Second,
    }}
    sb := RunServer(ob)
    t.Cleanup(sb.Shutdown)

    // Expect connection succeeds despite artificial delay
    deadline := time.Now().Add(5 * time.Second)
    for time.Now().Before(deadline) {
        if sb.NumLeafNodes() == 1 {
            return
        }
        time.Sleep(50 * time.Millisecond)
    }
    t.Fatalf("expected 1 leaf connection, got %d", sb.NumLeafNodes())
}
