package auth

import (
	"github.com/nats-io/gnatsd/server"
)

type Token struct {
	Token string
}

func (p *Token) Check(c server.ClientAuth) bool {
	opts := c.GetOpts()
	if p.Token != opts.Authorization {
		return false
	}

	return true
}
