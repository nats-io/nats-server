// Copyright 2013-2015 Apcera Inc. All rights reserved.

package server

import (
	"fmt"
	"net/url"
	"reflect"
	"testing"
	"time"

	"github.com/nats-io/nats"
)

func TestRouteConfig(t *testing.T) {
	opts, err := ProcessConfigFile("./configs/cluster.conf")
	if err != nil {
		t.Fatalf("Received an error reading route config file: %v\n", err)
	}

	golden := &Options{
		Host:               "apcera.me",
		Port:               4242,
		Username:           "derek",
		Password:           "bella",
		AuthTimeout:        1.0,
		ClusterHost:        "127.0.0.1",
		ClusterPort:        4244,
		ClusterUsername:    "route_user",
		ClusterPassword:    "top_secret",
		ClusterAuthTimeout: 1.0,
		LogFile:            "/tmp/nats_cluster_test.log",
		PidFile:            "/tmp/nats_cluster_test.pid",
	}

	// Setup URLs
	r1, _ := url.Parse("nats-route://foo:bar@apcera.me:4245")
	r2, _ := url.Parse("nats-route://foo:bar@apcera.me:4246")

	golden.Routes = []*url.URL{r1, r2}

	if !reflect.DeepEqual(golden, opts) {
		t.Fatalf("Options are incorrect.\nexpected: %+v\ngot: %+v",
			golden, opts)
	}
}

func TestServerRoutesWithClients(t *testing.T) {
	optsA, _ := ProcessConfigFile("./configs/srv_a.conf")
	optsB, _ := ProcessConfigFile("./configs/srv_b.conf")

	optsA.NoSigs, optsA.NoLog = true, true
	optsB.NoSigs, optsB.NoLog = true, true

	srvA := RunServer(optsA)
	defer srvA.Shutdown()

	urlA := fmt.Sprintf("nats://%s:%d/", optsA.Host, optsA.Port)
	urlB := fmt.Sprintf("nats://%s:%d/", optsB.Host, optsB.Port)

	nc1, err := nats.Connect(urlA)
	if err != nil {
		t.Fatalf("Error creating client: %v\n", err)
	}
	defer nc1.Close()

	ch := make(chan bool)
	sub, _ := nc1.Subscribe("foo", func(m *nats.Msg) { ch <- true })
	nc1.QueueSubscribe("foo", "bar", func(m *nats.Msg) {})
	nc1.Publish("foo", []byte("Hello"))
	// Wait for message
	<-ch
	sub.Unsubscribe()

	srvB := RunServer(optsB)
	defer srvB.Shutdown()

	time.Sleep(250 * time.Millisecond)

	nc2, err := nats.Connect(urlB)
	if err != nil {
		t.Fatalf("Error creating client: %v\n", err)
	}
	defer nc2.Close()
	nc2.Publish("foo", []byte("Hello"))
	nc2.Flush()
}
