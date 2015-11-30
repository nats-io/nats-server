FROM google/golang:1.4

MAINTAINER Derek Collison <derek@apcera.com>

RUN CGO_ENABLED=0 go get -a -ldflags '-s' --installsuffix cgo github.com/apcera/gnatsd

CMD cp /gopath/bin/gnatsd /tmp/gnatsd_build/. && chmod 777 /tmp/gnatsd_build/
