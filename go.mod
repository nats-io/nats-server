module github.com/nats-io/nats-server/v2

go 1.22

toolchain go1.22.8

require (
	github.com/google/go-tpm v0.9.3
	github.com/klauspost/compress v1.17.11
	github.com/minio/highwayhash v1.0.3
	github.com/nats-io/jwt/v2 v2.7.3
	github.com/nats-io/nats.go v1.36.0
	github.com/nats-io/nkeys v0.4.9
	github.com/nats-io/nuid v1.0.1
	go.uber.org/automaxprocs v1.6.0
	golang.org/x/crypto v0.31.0
	golang.org/x/sys v0.28.0
	golang.org/x/time v0.8.0
)

require (
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/redis/go-redis/v9 v9.7.0 // indirect
)
