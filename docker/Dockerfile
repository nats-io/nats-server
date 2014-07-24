FROM google/golang:1.3

MAINTAINER Derek Collison <derek@apcera.com>

RUN CGO_ENABLED=0 go get -a -ldflags '-s' github.com/apcera/gnatsd
COPY Dockerfile.final /gopath/bin/Dockerfile

CMD docker build -t apcera/gnatsd /gopath/bin
