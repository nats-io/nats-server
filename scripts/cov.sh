#!/bin/bash -e
# Run from directory above via ./scripts/cov.sh

export GO111MODULE="off"

go get github.com/mattn/goveralls
go get github.com/wadey/gocovmerge

rm -rf ./cov
mkdir cov
go test -v -failfast -covermode=atomic -coverprofile=./cov/conf.out ./conf -timeout=20m
go test -v -failfast -covermode=atomic -coverprofile=./cov/log.out ./logger -timeout=20m
go test -v -failfast -covermode=atomic -coverprofile=./cov/server.out ./server -timeout=20m
go test -v -failfast -covermode=atomic -coverprofile=./cov/test.out -coverpkg=./server ./test -timeout=20m
gocovmerge ./cov/*.out > acc.out
rm -rf ./cov

# Without argument, launch browser results. We are going to push to coveralls only
# from Travis.yml and after success of the build (and result of pushing will not affect
# build result).
if [[ $1 == "" ]]; then
    go tool cover -html=acc.out
fi
