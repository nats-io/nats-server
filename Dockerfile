FROM golang:1.12.8

MAINTAINER Ivan Kozlovic <ivan@synadia.com>

COPY . /go/src/github.com/nats-io/nats-server
WORKDIR /go/src/github.com/nats-io/nats-server

RUN CGO_ENABLED=0 GO111MODULE=off go install -v -a -tags netgo -installsuffix netgo -ldflags "-s -w -X github.com/nats-io/nats-server/server.gitCommit=`git rev-parse --short HEAD`"

EXPOSE 4222 8222
ENTRYPOINT ["nats-server"]
CMD ["--help"]
