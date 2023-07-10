#!/bin/sh

set -e

if [ "$1" = "compile" ]; then
    # First check that NATS builds.
    go build;

    # Now run the linters.
    go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.53.3;
    golangci-lint run;
    if [ "$TRAVIS_TAG" != "" ]; then
        go test -race -v -run=TestVersionMatchesTag ./server -count=1 -vet=off
    fi

elif [ "$1" = "build_only" ]; then
    go build;

elif [ "$1" = "no_race_tests" ]; then

    # Run tests without the `-race` flag. By convention, those tests start
    # with `TestNoRace`.

    go test -v -p=1 -run=TestNoRace ./... -count=1 -vet=off -timeout=30m -failfast

elif [ "$1" = "js_tests" ]; then

    # Run JetStream non-clustered tests. By convention, all JS tests start
    # with `TestJetStream`. We exclude the clustered and super-clustered
    # tests by using the appropriate tags.

    go test -race -v -run=TestJetStream ./server -tags=skip_js_cluster_tests,skip_js_cluster_tests_2,skip_js_cluster_tests_3,skip_js_super_cluster_tests -count=1 -vet=off -timeout=30m -failfast

elif [ "$1" = "js_cluster_tests_1" ]; then

    # Run JetStream clustered tests. By convention, all JS cluster tests
    # start with `TestJetStreamCluster`. Will run the first batch of tests,
    # excluding others with use of proper tags.

    go test -race -v -run=TestJetStreamCluster ./server -tags=skip_js_cluster_tests_2,skip_js_cluster_tests_3 -count=1 -vet=off -timeout=30m -failfast

elif [ "$1" = "js_cluster_tests_2" ]; then

    # Run JetStream clustered tests. By convention, all JS cluster tests
    # start with `TestJetStreamCluster`. Will run the second batch of tests,
    # excluding others with use of proper tags.

    go test -race -v -run=TestJetStreamCluster ./server -tags=skip_js_cluster_tests,skip_js_cluster_tests_3 -count=1 -vet=off -timeout=30m -failfast

elif [ "$1" = "js_cluster_tests_3" ]; then

    # Run JetStream clustered tests. By convention, all JS cluster tests
    # start with `TestJetStreamCluster`. Will run the third batch of tests,
    # excluding others with use of proper tags.
    #

    go test -race -v -run=TestJetStreamCluster ./server -tags=skip_js_cluster_tests,skip_js_cluster_tests_2 -count=1 -vet=off -timeout=30m -failfast

elif [ "$1" = "js_super_cluster_tests" ]; then

    # Run JetStream super clustered tests. By convention, all JS super cluster
    # tests start with `TestJetStreamSuperCluster`.

    go test -race -v -run=TestJetStreamSuperCluster ./server -count=1 -vet=off -timeout=30m -failfast

elif [ "$1" = "js_chaos_tests" ]; then

    # Run JetStream chaos tests. By convention, all JS cluster chaos tests
    # start with `TestJetStreamChaos`.

    go test -race -v -p=1 -run=TestJetStreamChaos ./server -tags=js_chaos_tests -count=1 -vet=off -timeout=30m -failfast

elif [ "$1" = "mqtt_tests" ]; then

    # Run MQTT tests. By convention, all MQTT tests start with `TestMQTT`.

    go test -race -v -run=TestMQTT ./server -count=1 -vet=off -timeout=30m -failfast

elif [ "$1" = "srv_pkg_non_js_tests" ]; then

    # Run all non JetStream tests in the server package. We exclude the
    # JS tests by using the `skip_js_tests` build tag and MQTT tests by
    # using the `skip_mqtt_tests`

    go test -race -v -p=1 ./server/... -tags=skip_js_tests,skip_mqtt_tests -count=1 -vet=off -timeout=30m -failfast

elif [ "$1" = "non_srv_pkg_tests" ]; then

    # Run all tests of all non server package.

    go test -race -v -p=1 $(go list ./... | grep -v "/server") -count=1 -vet=off -timeout=30m -failfast

fi
