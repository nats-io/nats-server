#!/bin/sh -e

# run go fmt and exit non-zero if it detected anything
T=$(mktemp -t gofmt.XXXXX)
go fmt ./... > $T
if egrep -q '.*' "$T" ; then
	echo "go fmt failed on the following files:"
	cat "$T"
	rm $T
	exit 1
fi
rm $T
exit 0
