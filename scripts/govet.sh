#!/bin/sh -e

# run go vet and exit non-zero if it detected anything
T=$(mktemp -t govet.XXXXX)
go vet ./... > $T
if egrep -q '.*' "$T" ; then
	echo "go vet failed on the following files:"
	cat "$T"
	rm $T
	exit 1
fi
rm $T
exit 0
