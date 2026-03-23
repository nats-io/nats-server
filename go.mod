module github.com/nats-io/nats-server/v2

go 1.25.0

toolchain go1.25.8

require (
	github.com/antithesishq/antithesis-sdk-go v0.6.0-default-no-op
	github.com/google/go-tpm v0.9.8
	github.com/klauspost/compress v1.18.4
	github.com/nats-io/jwt/v2 v2.8.1
	github.com/nats-io/nats.go v1.49.0
	github.com/nats-io/nkeys v0.4.15
	github.com/nats-io/nuid v1.0.1
	golang.org/x/crypto v0.49.0
	golang.org/x/sys v0.42.0
	golang.org/x/time v0.15.0
)

// We don't usually pin non-tagged commits but so far no release has
// been made that includes https://github.com/minio/highwayhash/pull/29.
// This will be updated if a new tag covers this in the future.
require github.com/minio/highwayhash v1.0.4
