#!/bin/sh

set -ex

if [ "$1" = "compile" ]; then
    # First check that NATS builds.
    go build -v;

    # Now run the linters.
    go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.64.4;
    golangci-lint run;
    if [ "$TRAVIS_TAG" != "" ]; then
        go test -v -run=TestVersionMatchesTag ./server -ldflags="-X=github.com/nats-io/nats-server/v2/server.serverVersion=$TRAVIS_TAG" -count=1 -vet=off
    fi

elif [ "$1" = "build_only" ]; then
    go build -v;

elif [ "$1" = "no_race_1_tests" ]; then

    # Run tests without the `-race` flag. By convention, those tests start
    # with `TestNoRace`. This will include all test files that have the
    # "&& !skip_no_race_1_tests" in the go build directive.

    go test -v -p=1 -run=TestNoRace ./... -tags=skip_no_race_2_tests -count=1 -vet=off -timeout=30m -failfast

elif [ "$1" = "no_race_2_tests" ]; then

    # Run tests without the `-race` flag. By convention, those tests start
    # with `TestNoRace`. This will include all test files that have the
    # "&& !skip_no_race_2_tests" in the go build directive.

    go test -v -p=1 -run=TestNoRace ./... -tags=skip_no_race_1_tests -count=1 -vet=off -timeout=30m -failfast

elif [ "$1" = "store_tests" ]; then

    # Run store tests. By convention, all file store tests start with `TestFileStore`,
    # and memory store tests start with `TestMemStore`.

    go test $RACE -v -p=1 -run=Test\(Store\|FileStore\|MemStore\) ./server -count=1 -vet=off -timeout=30m -failfast

elif [ "$1" = "js_tests" ]; then

    # Run JetStream non-clustered tests. By convention, all JS tests start
    # with `TestJetStream`. We exclude the clustered, super-clustered and
    # consumer tests by using the appropriate tags.

    go test $RACE -v -p=1 -run=TestJetStream ./server -tags=skip_js_cluster_tests,skip_js_cluster_tests_2,skip_js_cluster_tests_3,skip_js_cluster_tests_4,skip_js_super_cluster_tests,skip_js_consumer_tests -count=1 -vet=off -timeout=30m -failfast

elif [ "$1" = "js_consumer_tests" ]; then

    # Run JetStream consumer tests. All tests start with TestJetStreamConsumer prefix.

    go test $RACE -v -p=1 -run=TestJetStreamConsumer ./server -count=1 -vet=off -timeout=30m -failfast

elif [ "$1" = "raft_tests" ]; then

    # Run the RAFT tests. All tests start with TestNRG prefix.

    go test $RACE -v -p=1 ./server/... -run="^TestNRG" -count=1 -vet=off -timeout=30m -failfast

elif [ "$1" = "js_cluster_tests_1" ]; then

    # Run JetStream clustered tests. By convention, all JS cluster tests
    # start with `TestJetStreamCluster`. Will run the first batch of tests,
    # excluding others with use of proper tags.

    go test $RACE -v -p=1 -run=TestJetStreamCluster ./server -tags=skip_js_cluster_tests_2,skip_js_cluster_tests_3,skip_js_cluster_tests_4 -count=1 -vet=off -timeout=30m -failfast

elif [ "$1" = "js_cluster_tests_2" ]; then

    # Run JetStream clustered tests. By convention, all JS cluster tests
    # start with `TestJetStreamCluster`. Will run the second batch of tests,
    # excluding others with use of proper tags.

    go test $RACE -v -p=1 -run=TestJetStreamCluster ./server -tags=skip_js_cluster_tests,skip_js_cluster_tests_3,skip_js_cluster_tests_4 -count=1 -vet=off -timeout=30m -failfast

elif [ "$1" = "js_cluster_tests_3" ]; then

    # Run JetStream clustered tests. By convention, all JS cluster tests
    # start with `TestJetStreamCluster`. Will run the third batch of tests,
    # excluding others with use of proper tags.
    #

    go test $RACE -v -p=1 -run=TestJetStreamCluster ./server -tags=skip_js_cluster_tests,skip_js_cluster_tests_2,skip_js_cluster_tests_4 -count=1 -vet=off -timeout=30m -failfast

elif [ "$1" = "js_cluster_tests_4" ]; then

    # Run JetStream clustered tests. By convention, all JS cluster tests
    # start with `TestJetStreamCluster`. Will run the third batch of tests,
    # excluding others with use of proper tags.
    #

    go test $RACE -v -p=1 -run=TestJetStreamCluster ./server -tags=skip_js_cluster_tests,skip_js_cluster_tests_2,skip_js_cluster_tests_3 -count=1 -vet=off -timeout=30m -failfast

elif [ "$1" = "js_super_cluster_tests" ]; then

    # Run JetStream super clustered tests. By convention, all JS super cluster
    # tests start with `TestJetStreamSuperCluster`.

    go test $RACE -v -p=1 -run=TestJetStreamSuperCluster ./server -count=1 -vet=off -timeout=30m -failfast

elif [ "$1" = "mqtt_tests" ]; then

    # Run MQTT tests. By convention, all MQTT tests start with `TestMQTT`.

    go test $RACE -v -p=1 -run=TestMQTT ./server -count=1 -vet=off -timeout=30m -failfast

elif [ "$1" = "msgtrace_tests" ]; then

    # Run Message Tracing tests. By convention, all message tracing tests start with `TestMsgTrace`.

    go test $RACE -v -p=1 -run=TestMsgTrace ./server -count=1 -vet=off -timeout=30m -failfast

elif [ "$1" = "srv_pkg_non_js_tests" ]; then

    # Run all non JetStream tests in the server package. We exclude the
    # store tests by using the `skip_store_tests` build tag, the JS tests
    # by using `skip_js_tests`, MQTT tests by using `skip_mqtt_tests` and
    # message tracing tests by using `skip_msgtrace_tests`.
    # Ignore JWT and NRG tests here as they have their own matrix run.

    # Also including the ldflag with the version since this includes the `TestVersionMatchesTag`.
    go test $RACE -v -p=1 ./server/... -run="^Test(N[^R]|NR[^G]|J[^W]|JW[^T]|[^JN])" -ldflags="-X=github.com/nats-io/nats-server/v2/server.serverVersion=$TRAVIS_TAG" -tags=skip_store_tests,skip_js_tests,skip_mqtt_tests,skip_msgtrace_tests,skip_no_race_tests -count=1 -vet=off -timeout=30m -failfast

elif [ "$1" = "jwt_tests" ]; then

    # Run the JWT tests. All tests start with TestJWT.

    go test $RACE -v -p=1 ./server/... -run="^TestJWT" -count=1 -vet=off -timeout=30m -failfast

elif [ "$1" = "non_srv_pkg_tests" ]; then

    # Run all tests of all non server package.

    go test $RACE -v -p=1 $(go list ./... | grep -v "/server") -count=1 -vet=off -timeout=30m -failfast

fi
