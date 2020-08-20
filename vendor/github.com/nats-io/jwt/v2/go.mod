module github.com/nats-io/jwt/v2

require (
	github.com/nats-io/jwt v0.3.2
	github.com/nats-io/nkeys v0.2.0
	github.com/stretchr/testify v1.6.1
)

replace github.com/nats-io/jwt v0.3.2 => ../

go 1.14
