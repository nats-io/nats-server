#!/bin/bash
# Run from directory above via ./scripts/cov.sh

# Do not set the -e flag because we don't a flapper to prevent the push to coverall.

export GO111MODULE="on"

go install github.com/wadey/gocovmerge@latest

rm -rf ./cov
mkdir cov
# Since it is difficult to get a full run without a flapper, do not use `-failfast`.
# It is better to have one flapper or two and still get the report than have
# to re-run the whole code coverage. One or two failed tests should not affect
# so much the code coverage.
go test -v -covermode=atomic -coverprofile=./cov/conf.out ./conf -timeout=20m -tags=skip_no_race_tests
go test -v -covermode=atomic -coverprofile=./cov/log.out ./logger -timeout=20m -tags=skip_no_race_tests
go test -v -covermode=atomic -coverprofile=./cov/server.out ./server -timeout=20m -tags=skip_no_race_tests
go test -v -covermode=atomic -coverprofile=./cov/test.out -coverpkg=./server ./test -timeout=20m -tags=skip_no_race_tests
gocovmerge ./cov/*.out > acc.out
rm -rf ./cov

# If no argument passed, launch a browser to see the results.
if [[ $1 == "" ]]; then
    go tool cover -html=acc.out
fi
