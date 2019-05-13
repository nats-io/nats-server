FROM golang:1.11-alpine3.8 AS builder

WORKDIR $GOPATH/src/github.com/nats-io/nats-server

MAINTAINER Waldemar Quevedo <wally@synadia.com>

RUN apk add --update git

COPY . .

RUN CGO_ENABLED=0 GO111MODULE=off go build -v -a -tags netgo -installsuffix netgo -ldflags "-s -w -X github.com/nats-io/nats-server/server.gitCommit=`git rev-parse --short HEAD`" -o /nats-server

FROM alpine:3.8

RUN apk add --update ca-certificates && mkdir -p /nats/bin && mkdir /nats/conf

COPY docker/nats-server.conf /nats/conf/nats-server.conf
COPY --from=builder /nats-server /nats/bin/nats-server

# NOTE: For backwards compatibility, we add a symlink to /gnatsd which is
# where the binary from the scratch container image used to be located.
RUN ln -ns /nats/bin/nats-server /bin/nats-server && ln -ns /nats/bin/nats-server /nats-server && ln -ns /nats/bin/nats-server /gnatsd

# Expose client, management, cluster and gateway ports
EXPOSE 4222 8222 6222 5222

ENTRYPOINT ["/bin/nats-server"]
CMD ["-c", "/nats/conf/nats-server.conf"]
