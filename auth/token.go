package auth

import (
	"github.com/nats-io/gnatsd/server"
	"golang.org/x/crypto/bcrypt"
)

type Token struct {
	Token string
}

func (p *Token) Check(c server.ClientAuth) bool {
	opts := c.GetOpts()
	// Check to see if the token is a bcrypt hash
	if isBcrypt(p.Token) {
		if err := bcrypt.CompareHashAndPassword([]byte(p.Token), []byte(opts.Authorization)); err != nil {
			return false
		}
	} else if p.Token != opts.Authorization {
		return false
	}

	return true
}
