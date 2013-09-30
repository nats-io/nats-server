package server

import (
	"net/url"
	"reflect"
	"testing"
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
