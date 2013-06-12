// Copyright 2012 Apcera Inc. All rights reserved.

package server

import (
	"fmt"
)

type pubArg struct {
	subject []byte
	reply   []byte
	szb     []byte
	size    int
}

type parseState struct {
	state   int
	as      int
	drop    int
	pa      pubArg
	argBuf  []byte
	msgBuf  []byte
	scratch [MAX_CONTROL_LINE_SIZE]byte
}

const (
	OP_START = iota
	OP_C
	OP_CO
	OP_CON
	OP_CONN
	OP_CONNE
	OP_CONNEC
	OP_CONNECT
	CONNECT_ARG
	OP_P
	OP_PU
	OP_PUB
	OP_PUB_SPC
	PUB_ARG
	OP_PI
	OP_PIN
	OP_PING
	OP_PO
	OP_PON
	OP_PONG
	MSG_PAYLOAD
	MSG_END
	OP_S
	OP_SU
	OP_SUB
	OP_SUB_SPC
	SUB_ARG
	OP_U
	OP_UN
	OP_UNS
	OP_UNSU
	OP_UNSUB
	UNSUB_ARG
)

func (c *client) parse(buf []byte) error {
	var i int
	var b byte

	for i, b = range buf {
		switch c.state {
		case OP_START:
			if c.isAuthTimerSet() && b != 'C' && b != 'c' {
				goto authErr
			}
			switch b {
			case 'C', 'c':
				c.state = OP_C
			case 'P', 'p':
				c.state = OP_P
			case 'S', 's':
				c.state = OP_S
			case 'U', 'u':
				c.state = OP_U
			default:
				goto parseErr
			}
		case OP_P:
			switch b {
			case 'U', 'u':
				c.state = OP_PU
			case 'I', 'i':
				c.state = OP_PI
			case 'O', 'o':
				c.state = OP_PO
			default:
				goto parseErr
			}
		case OP_PU:
			switch b {
			case 'B', 'b':
				c.state = OP_PUB
			default:
				goto parseErr
			}
		case OP_PUB:
			switch b {
			case ' ', '\t':
				c.state = OP_PUB_SPC
			default:
				goto parseErr
			}
        case OP_PUB_SPC:
			switch b {
			case ' ', '\t':
				continue
			default:
				c.state = PUB_ARG
				c.as = i
			}
		case PUB_ARG:
			switch b {
			case '\r':
				c.drop = 1
			case '\n':
				var arg []byte
				if c.argBuf != nil {
					arg = c.argBuf
				} else {
					arg = buf[c.as : i-c.drop]
				}
				if err := c.processPub(arg); err != nil {
					return err
				}
				c.drop, c.as, c.state = 0, i+1, MSG_PAYLOAD
			default:
				if c.argBuf != nil {
					c.argBuf = append(c.argBuf, b)
				}
			}
		case MSG_PAYLOAD:
			if c.msgBuf != nil {
				if len(c.msgBuf) >= c.pa.size {
					c.processMsg(c.msgBuf)
					c.argBuf, c.msgBuf, c.state = nil, nil, MSG_END
				} else {
					c.msgBuf = append(c.msgBuf, b)
				}
			} else if i-c.as >= c.pa.size {
				c.processMsg(buf[c.as:i])
				c.argBuf, c.msgBuf, c.state = nil, nil, MSG_END
			}
		case MSG_END:
			switch b {
			case '\n':
				c.drop, c.as, c.state = 0, i+1, OP_START
			default:
				continue
			}
		case OP_S:
			switch b {
			case 'U', 'u':
				c.state = OP_SU
			default:
				goto parseErr
			}
		case OP_SU:
			switch b {
			case 'B', 'b':
				c.state = OP_SUB
			default:
				goto parseErr
			}
		case OP_SUB:
			switch b {
			case ' ', '\t':
				c.state = OP_SUB_SPC
			default:
				goto parseErr
			}
		case OP_SUB_SPC:
			switch b {
			case ' ', '\t':
				continue
			default:
				c.state = SUB_ARG
				c.as = i
			}
		case SUB_ARG:
			switch b {
			case '\r':
				c.drop = 1
			case '\n':
				var arg []byte
				if c.argBuf != nil {
					arg = c.argBuf
					c.argBuf = nil
				} else {
					arg = buf[c.as : i-c.drop]
				}
				if err := c.processSub(arg); err != nil {
					return err
				}
				c.drop, c.as, c.state = 0, i+1, OP_START
			default:
				if c.argBuf != nil {
					c.argBuf = append(c.argBuf, b)
				}
			}
		case OP_U:
			switch b {
			case 'N', 'n':
				c.state = OP_UN
			default:
				goto parseErr
			}
		case OP_UN:
			switch b {
			case 'S', 's':
				c.state = OP_UNS
			default:
				goto parseErr
			}
		case OP_UNS:
			switch b {
			case 'U', 'u':
				c.state = OP_UNSU
			default:
				goto parseErr
			}
		case OP_UNSU:
			switch b {
			case 'B', 'b':
				c.state = OP_UNSUB
			default:
				goto parseErr
			}
		case OP_UNSUB:
			switch b {
			case ' ', '\t':
				continue
			default:
				c.state = UNSUB_ARG
				c.as = i
			}
		case UNSUB_ARG:
			switch b {
			case '\r':
				c.drop = 1
			case '\n':
				var arg []byte
				if c.argBuf != nil {
					arg = c.argBuf
					c.argBuf = nil
				} else {
					arg = buf[c.as : i-c.drop]
				}
				if err := c.processUnsub(arg); err != nil {
					return err
				}
				c.drop, c.as, c.state = 0, i+1, OP_START
			default:
				if c.argBuf != nil {
					c.argBuf = append(c.argBuf, b)
				}
			}
		case OP_PI:
			switch b {
			case 'N', 'n':
				c.state = OP_PIN
			default:
				goto parseErr
			}
		case OP_PIN:
			switch b {
			case 'G', 'g':
				c.state = OP_PING
			default:
				goto parseErr
			}
		case OP_PING:
			switch b {
			case '\n':
				c.processPing()
				c.drop, c.state = 0, OP_START
			}
		case OP_PO:
			switch b {
			case 'N', 'n':
				c.state = OP_PON
			default:
				goto parseErr
			}
		case OP_PON:
			switch b {
			case 'G', 'g':
				c.state = OP_PONG
			default:
				goto parseErr
			}
		case OP_PONG:
			switch b {
			case '\n':
				c.processPong()
				c.drop, c.state = 0, OP_START
			}
		case OP_C:
			switch b {
			case 'O', 'o':
				c.state = OP_CO
			default:
				goto parseErr
			}
		case OP_CO:
			switch b {
			case 'N', 'n':
				c.state = OP_CON
			default:
				goto parseErr
			}
		case OP_CON:
			switch b {
			case 'N', 'n':
				c.state = OP_CONN
			default:
				goto parseErr
			}
		case OP_CONN:
			switch b {
			case 'E', 'e':
				c.state = OP_CONNE
			default:
				goto parseErr
			}
		case OP_CONNE:
			switch b {
			case 'C', 'c':
				c.state = OP_CONNEC
			default:
				goto parseErr
			}
		case OP_CONNEC:
			switch b {
			case 'T', 't':
				c.state = OP_CONNECT
			default:
				goto parseErr
			}
		case OP_CONNECT:
			switch b {
			case ' ', '\t':
				continue
			default:
				c.state = CONNECT_ARG
				c.as = i
			}
		case CONNECT_ARG:
			switch b {
			case '\r':
				c.drop = 1
			case '\n':
				if err := c.processConnect(buf[c.as : i-c.drop]); err != nil {
					return err
				}
				c.drop, c.state = 0, OP_START
			}
		default:
			goto parseErr
		}
	}
	// Check for split buffer scenarios for SUB and UNSUB and PUB
	if (c.state == SUB_ARG || c.state == UNSUB_ARG || c.state == PUB_ARG) && c.argBuf == nil {
		c.argBuf = c.scratch[:0]
		c.argBuf = append(c.argBuf, buf[c.as:(i+1)-c.drop]...)
		// FIXME, check max len
	}
	// Check for split msg
	if c.state == MSG_PAYLOAD && c.msgBuf == nil {
		// We need to clone the pubArg if it is still referencing the
		// read buffer and we are not able to process the msg.
		if c.argBuf == nil {
			c.clonePubArg()
		}
		// FIXME: copy better here? Make whole buf if large?
		//c.msgBuf = c.scratch[:0]
		c.msgBuf = c.scratch[len(c.argBuf):len(c.argBuf)]
		c.msgBuf = append(c.msgBuf, (buf[c.as:])...)
	}
	return nil

authErr:
	c.authViolation()
	return ErrAuthorization

parseErr:
	c.sendErr("Unknown Protocol Operation")
	stop := i + 32
	if stop > len(buf) {
		stop = len(buf)-1
	}
	return fmt.Errorf("Parse Error, state=%d,i=%d: '%s'", c.state, i, buf[i:stop])
}


// clonePubArg is used when the split buffer scenario has the pubArg in the existing read buffer, but
// we need to hold onto it into the next read.
func (c *client) clonePubArg() {
	c.argBuf = c.scratch[:0]
	c.argBuf = append(c.argBuf, c.pa.subject...)
	c.argBuf = append(c.argBuf, c.pa.reply...)
	c.argBuf = append(c.argBuf, c.pa.szb...)
	c.pa.subject = c.argBuf[:len(c.pa.subject)]
	if c.pa.reply != nil {
		c.pa.reply = c.argBuf[len(c.pa.subject) : len(c.pa.subject)+len(c.pa.reply)]
	}
	c.pa.szb = c.argBuf[len(c.pa.subject)+len(c.pa.reply):]
}
