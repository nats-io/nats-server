FROM golang:1.16.8-alpine3.14 AS builder

ENV NATS_SERVER 2.6.1

RUN apk add --update --no-cache build-base libc6-compat

COPY ./ /nats-server
RUN cd /nats-server && go build -a -v -ldflags "-w -X main.version=${NATS_SERVER}" -o /bin/nats-server

FROM alpine:3.14

RUN apk add --update --no-cache ca-certificates libc6-compat && mkdir /etc/nats/

COPY docker/nats-server.conf /etc/nats/nats-server.conf
COPY --from=builder /bin/nats-server /bin/nats-server

EXPOSE 4222 8222 6222 5222

ENTRYPOINT ["/bin/nats-server"]
CMD ["-c", "/etc/nats/nats-server.conf"]
