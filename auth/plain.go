package auth

import (
	"github.com/apcera/gnatsd/server"
)

type Plain struct {
	Username string
	Password string
}

func (p *Plain) Check(c server.ClientAuth) bool {
	opts := c.GetOpts()
	if p.Username != opts.Username || p.Password != opts.Password {
		return false
	}

	return true
}
