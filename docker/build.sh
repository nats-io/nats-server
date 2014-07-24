#!/bin/bash
docker build -t apcera/gnatsd_build .
docker run -v /var/run/docker.sock:/var/run/docker.sock -v $(which docker):$(which docker) -ti --name gnatsd_build apcera/gnatsd_build
docker rm gnatsd_build
docker rmi apcera/gnatsd_build