FROM golang:1.7.6

MAINTAINER Ivan Kozlovic <ivan.kozlovic@apcera.com>

COPY . /go/src/github.com/nats-io/gnatsd
WORKDIR /go/src/github.com/nats-io/gnatsd

RUN CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -ldflags "-s -w" -o gnatsd.exe .

ENTRYPOINT ["go"]
CMD ["version"]
