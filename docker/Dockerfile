FROM google/golang:1.4

MAINTAINER Derek Collison <derek@apcera.com>

# Note need CGO_ENABLED=0 go build -a -installsuffix cgo for 1.4
RUN CGO_ENABLED=0 go get -a -ldflags '-s' github.com/apcera/gnatsd
COPY Dockerfile.final /gopath/bin/Dockerfile

CMD docker build -t apcera/gnatsd /gopath/bin
