FROM golang:1.10.3

MAINTAINER Ivan Kozlovic <ivan@synadia.com>

COPY . /go/src/github.com/nats-io/gnatsd
WORKDIR /go/src/github.com/nats-io/gnatsd

RUN CGO_ENABLED=0 go install -v -a -tags netgo -installsuffix netgo -ldflags "-s -w -X github.com/nats-io/gnatsd/server.gitCommit=`git rev-parse --short HEAD`"

EXPOSE 4222 8222
ENTRYPOINT ["gnatsd"]
CMD ["--help"]
