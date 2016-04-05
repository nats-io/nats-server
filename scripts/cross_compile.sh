#!/bin/bash
go get github.com/mitchellh/gox
go get github.com/tcnksm/ghr

export APPNAME="gnatsd"
export OSARCH="linux/386 linux/amd64 linux/arm solaris/amd64 windows/386 windows/amd64"
export DIRS="linux_386 linux_amd64 linux_arm solaris_amd64 windows_386 windows_amd64"
export OUTDIR="pkg"

gox -osarch="$OSARCH" -output "$OUTDIR/$APPNAME-{{.OS}}_{{.Arch}}/$APPNAME"
for dir in $DIRS; do \
	(cp README.md $OUTDIR/$APPNAME-$dir/README.md) ;\
	(cp LICENSE $OUTDIR/$APPNAME-$dir/LICENSE) ;\
	(cd $OUTDIR && zip -q $APPNAME-$dir.zip -r $APPNAME-$dir) ;\
	echo "make $OUTDIR/$APPNAME-$dir.zip" ;\
done

