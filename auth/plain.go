package auth

import (
	"github.com/apcera/gnatsd/server"
)

type Plain struct {
	Username string
	Password string
}

func (p *Plain) Check(c *server.Client) bool {
	opts := c.GetOpts()
	if p.Username != opts.Username || p.Password != opts.Password {
		return false
	}

	return true
}
