#!/bin/bash
go get github.com/mitchellh/gox
go get github.com/tcnksm/ghr

export APPNAME="gnatsd"
export OSARCH="linux/386 linux/amd64 linux/arm darwin/amd64 windows/386 windows/amd64"
export DIRS="linux-386 linux-amd64 linux-arm darwin-amd64 windows-386 windows-amd64"
export OUTDIR="pkg"

# If we have an arg, assume its a version tag and rename as appropriate.
if [[ -n $1 ]]; then
    export APPNAME=$APPNAME-$1
fi

gox -osarch="$OSARCH" -ldflags="-s -w" -output "$OUTDIR/$APPNAME-{{.OS}}-{{.Arch}}/gnatsd"
for dir in $DIRS; do \
    (cp README.md $OUTDIR/$APPNAME-$dir/README.md) ;\
    (cp LICENSE $OUTDIR/$APPNAME-$dir/LICENSE) ;\
    (cd $OUTDIR && zip -q $APPNAME-$dir.zip -r $APPNAME-$dir) ;\
    echo "make $OUTDIR/$APPNAME-$dir.zip" ;\
done
