#!/bin/sh

set -e

if [ "$1" = "compile" ]; then

    # We will compile and run some vet, spelling and some other checks.

    go install honnef.co/go/tools/cmd/staticcheck@v0.2.2;
    go install github.com/client9/misspell/cmd/misspell@latest;
    GO_LIST=$(go list ./...);
    go build;
    $(exit $(go fmt $GO_LIST | wc -l));
    go vet $GO_LIST;
    find . -type f -name "*.go" | xargs misspell -error -locale US;
    staticcheck $GO_LIST
    if [ "$TRAVIS_TAG" != "" ]; then
        go test -race -v -run=TestVersionMatchesTag ./server -count=1 -vet=off
    fi

elif [ "$1" = "no_race_tests" ]; then

    # Run tests without the `-race` flag. By convention, those tests start
    # with `TestNoRace`.

    go test -v -p=1 -run=TestNoRace ./... -count=1 -vet=off -timeout=30m -failfast

elif [ "$1" = "js_tests" ]; then

    # Run JetStream non-clustere tests. By convention, all JS tests start
    # with `TestJetStream`. We exclude the clustered and super-clustered
    # tests by using the `skip_js_cluster_tests` and `skip_js_super_cluster_tests`
    # build tags.

    go test -race -v -run=TestJetStream ./server -tags=skip_js_cluster_tests,skip_js_super_cluster_tests -count=1 -vet=off -timeout=30m -failfast

elif [ "$1" = "js_cluster_tests" ]; then

    # Run JetStream clustered tests. By convention, all JS cluster tests
    # start with `TestJetStreamCluster`.

    go test -race -v -run=TestJetStreamCluster ./server -count=1 -vet=off -timeout=30m -failfast

elif [ "$1" = "js_super_cluster_tests" ]; then

    # Run JetStream super clustered tests. By convention, all JS super cluster
    # tests with `TestJetStreamSuperCluster`.

    go test -race -v -run=TestJetStreamSuperCluster ./server -count=1 -vet=off -timeout=30m -failfast

elif [ "$1" = "srv_pkg_non_js_tests" ]; then

    # Run all non JetStream tests in the server package. We exclude the
    # JS tests by using the `skip_js_tests` build tag.

    go test -race -v -p=1 ./server/... -tags=skip_js_tests -count=1 -vet=off -timeout=30m -failfast

elif [ "$1" = "non_srv_pkg_tests" ]; then

    # Run all tests of all non server package.

    go test -race -v -p=1 $(go list ./... | grep -v "/server") -count=1 -vet=off -timeout=30m -failfast

fi
