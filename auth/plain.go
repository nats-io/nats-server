package auth

import (
	"github.com/nats-io/gnatsd"
)

type Plain struct {
	Username string
	Password string
}

func (p *Plain) Check(c gnatsd.ClientAuth) bool {
	opts := c.GetOpts()
	if p.Username != opts.Username || p.Password != opts.Password {
		return false
	}

	return true
}
