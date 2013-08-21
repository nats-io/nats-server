// Copyright 2013 Apcera Inc. All rights reserved.

// Parser will return a map of keys to interface{}, although concrete types
// underly them. The values supported are string, bool, int64, float64, DateTime.
// Arrays and nested Maps are also supported.

// The format supported is less restrictive than today's formats.
// Supports mixed Arrays [], nested Maps {}, multiple comment types (# and //)
// Also supports key value assigments using '=' or ':' or whiteSpace()
//   e.g. foo = 2, foo : 2, foo 2
// maps can be assigned with no key separator as well
// semicolons as value terminators in key/value assignments are optional
//
// see parse_test.go for more examples.

package conf

import (
	"fmt"
	"strconv"
	"time"
)

type parser struct {
	mapping map[string]interface{}
	lx      *lexer

	// The current scoped context, can be array or map
	ctx interface{}

	// stack of contexts, either map or array/slice stack
	ctxs []interface{}

	// Keys stack
	keys []string
}

func Parse(data string) (map[string]interface{}, error) {
	p, err := parse(data)
	if err != nil {
		return nil, err
	}
	return p.mapping, nil
}

func parse(data string) (p *parser, err error) {

	p = &parser{
		mapping: make(map[string]interface{}),
		lx:      lex(data),
		ctxs:    make([]interface{}, 0, 4),
		keys:    make([]string, 0, 4),
	}
	p.pushContext(p.mapping)

	for {
		it := p.next()
		if it.typ == itemEOF {
			break
		}
		if err := p.processItem(it); err != nil {
			return nil, err
		}
	}

	return p, nil
}

func (p *parser) next() item {
	return p.lx.nextItem()
}

func (p *parser) pushContext(ctx interface{}) {
	p.ctxs = append(p.ctxs, ctx)
	p.ctx = ctx
}

func (p *parser) popContext() interface{} {
	if len(p.ctxs) == 0 {
		panic("BUG in parser, context stack empty")
	}
	li := len(p.ctxs) - 1
	last := p.ctxs[li]
	p.ctxs = p.ctxs[0:li]
	p.ctx = p.ctxs[len(p.ctxs)-1]
	return last
}

func (p *parser) pushKey(key string) {
	p.keys = append(p.keys, key)
}

func (p *parser) popKey() string {
	if len(p.keys) == 0 {
		panic("BUG in parser, keys stack empty")
	}
	li := len(p.keys) - 1
	last := p.keys[li]
	p.keys = p.keys[0:li]
	return last
}

func (p *parser) processItem(it item) error {
	switch it.typ {
	case itemError:
		return fmt.Errorf("Parse error on line %d: '%s'", it.line, it.val)
	case itemKey:
		p.pushKey(it.val)
	case itemMapStart:
		newCtx := make(map[string]interface{})
		p.pushContext(newCtx)
	case itemMapEnd:
		p.setValue(p.popContext())
	case itemString:
		p.setValue(it.val) // FIXME(dlc) sanitize string?
	case itemInteger:
		num, err := strconv.ParseInt(it.val, 10, 64)
		if err != nil {
			if e, ok := err.(*strconv.NumError); ok &&
				e.Err == strconv.ErrRange {
				return fmt.Errorf("Integer '%s' is out of the range.", it.val)
			} else {
				return fmt.Errorf("Expected integer, but got '%s'.", it.val)
			}
		}
		p.setValue(num)
	case itemFloat:
		num, err := strconv.ParseFloat(it.val, 64)
		if err != nil {
			if e, ok := err.(*strconv.NumError); ok &&
				e.Err == strconv.ErrRange {
				return fmt.Errorf("Float '%s' is out of the range.", it.val)
			} else {
				return fmt.Errorf("Expected float, but got '%s'.", it.val)
			}
		}
		p.setValue(num)
	case itemBool:
		switch it.val {
		case "true":
			p.setValue(true)
		case "false":
			p.setValue(false)
		default:
			return fmt.Errorf("Expected boolean value, but got '%s'.", it.val)
		}
	case itemDatetime:
		dt, err := time.Parse("2006-01-02T15:04:05Z", it.val)
		if err != nil {
			return fmt.Errorf(
				"Expected Zulu formatted DateTime, but got '%s'.", it.val)
		}
		p.setValue(dt)
	case itemArrayStart:
		array := make([]interface{}, 0)
		p.pushContext(array)
	case itemArrayEnd:
		array := p.ctx
		p.popContext()
		p.setValue(array)
	}

	return nil
}

func (p *parser) setValue(val interface{}) {
	// Test to see if we are on an array or a map

	// Array processing
	if ctx, ok := p.ctx.([]interface{}); ok {
		p.ctx = append(ctx, val)
		p.ctxs[len(p.ctxs)-1] = p.ctx
	}

	// Map processing
	if ctx, ok := p.ctx.(map[string]interface{}); ok {
		key := p.popKey()
		// FIXME(dlc), make sure to error if redefining same key?
		ctx[key] = val
	}
}
