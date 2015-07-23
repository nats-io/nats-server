#!/bin/bash
docker build -t apcera/gnatsd_build .
docker run --rm=true -v `pwd`/gnatsd_build:/tmp/gnatsd_build \
       -ti --name gnatsd_build apcera/gnatsd_build
docker build -f Dockerfile.final -t apcera/gnatsd .
docker rmi apcera/gnatsd_build
rm -rf gnatsd_build/
