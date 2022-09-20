#!/bin/bash
# Run from directory above via ./scripts/cov.sh

check_file () {
    # If the content of the file is simply "mode: atomic", then it means that the
    # code coverage did not complete due to a panic in one of the tests.
    if [[ $(cat ./cov/$2) == "mode: atomic" ]]; then
        echo "#############################################"
        echo "## Code coverage for $1 package failed ##"
        echo "#############################################"
        exit 1
    fi
}

# Do not globally set the -e flag because we don't a flapper to prevent the push to coverall.

export GO111MODULE="on"

go install github.com/wadey/gocovmerge@latest

rm -rf ./cov
mkdir cov
#
# Since it is difficult to get a full run without a flapper, do not use `-failfast`.
# It is better to have one flapper or two and still get the report than have
# to re-run the whole code coverage. One or two failed tests should not affect
# so much the code coverage.
#
# However, we need to take into account that if there is a panic in one test, all
# other tests in that package will not run, which then would cause the code coverage
# to drastically be lowered. In that case, we don't want the code coverage to be
# uploaded.
#
go test -v -covermode=atomic -coverprofile=./cov/conf.out ./conf -timeout=20m -tags=skip_no_race_tests
check_file "conf" "conf.out"
go test -v -covermode=atomic -coverprofile=./cov/log.out ./logger -timeout=20m -tags=skip_no_race_tests
check_file "logger" "log.out"
go test -v -covermode=atomic -coverprofile=./cov/server.out ./server -timeout=20m -tags=skip_no_race_tests
check_file "server" "server.out"
go test -v -covermode=atomic -coverprofile=./cov/test.out -coverpkg=./server ./test -timeout=20m -tags=skip_no_race_tests
check_file "test" "test.out"

# At this point, if that fails, we want the caller to know about the failure.
set -e
gocovmerge ./cov/*.out > acc.out
rm -rf ./cov

# If no argument passed, launch a browser to see the results.
if [[ $1 == "" ]]; then
    go tool cover -html=acc.out
fi
set +e
