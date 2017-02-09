package health

import (
	"net"
	"testing"

	"github.com/nats-io/gnatsd/server"
)

func TestIccTypeSwitchWorks(t *testing.T) {
	var nc net.Conn = &Icc{}
	_, isIcc := nc.(server.LocalInternalClient)
	if !isIcc {
		t.Fatalf("nc was not LocalInternalClient, as it should be!")
	}
}

func TestIccAsNetConn(t *testing.T) {

	// write to a, read from b
	a, b, err := NewInternalClientPair()
	if err != nil {
		panic(err)
	}

	msg := "hello-world"

	n, err := a.Write([]byte(msg))
	if err != nil {
		t.Errorf("err = %v", err)
	}
	if n != len(msg) {
		t.Errorf("Write truncated at %v < %v", n, len(msg))
	}

	readbuf := make([]byte, len(msg))
	m, err := b.Read(readbuf)
	if err != nil {
		t.Errorf("err = %v", err)
	}
	if m != n {
		t.Errorf("Read truncated at %v !=n %v", m, n)
	}
	back := string(readbuf[:m])
	if back != msg {
		t.Errorf("msg corrupted, wrote '%v', read '%v'", msg, back)
	}
}
