module github.com/nats-io/nats-server/v2

replace github.com/nats-io/nats.go => github.com/nats-io/nats.go v1.30.1-0.20230925101945-66fd3c30ab7b

go 1.20

require (
	github.com/klauspost/compress v1.17.0
	github.com/minio/highwayhash v1.0.2
	github.com/nats-io/jwt/v2 v2.5.2
	github.com/nats-io/nats.go v1.29.0
	github.com/nats-io/nkeys v0.4.5
	github.com/nats-io/nuid v1.0.1
	go.uber.org/automaxprocs v1.5.3
	golang.org/x/crypto v0.13.0
	golang.org/x/exp v0.0.0-20230905200255-921286631fa9
	golang.org/x/sys v0.12.0
	golang.org/x/time v0.3.0
)

require (
	github.com/golang/protobuf v1.4.2 // indirect
	google.golang.org/protobuf v1.23.0 // indirect
)
