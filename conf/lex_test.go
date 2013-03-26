package conf

import (
	"testing"
)

// Test to make sure we get what we expect.
func expect(t *testing.T, lx *lexer, items []item) {
	for i := 0; i < len(items); i++ {
		item := lx.nextItem()
		if item.typ == itemEOF {
			break
		} else if item.typ == itemError {
			t.Fatal(item.val)
		}
		if item != items[i] {
			t.Fatalf("Testing: '%s'\nExpected %+v, received %+v\n",
				lx.input, items[i], item)
		}
	}
}

func TestSimpleKeyStringValues(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1},
		{itemString, "bar", 1},
		{itemEOF, "", 1},
	}
	// Double quotes
	lx := lex("foo = \"bar\"")
	expect(t, lx, expectedItems)
	// Single quotes
	lx = lex("foo = 'bar'")
	expect(t, lx, expectedItems)
	// No spaces
	lx = lex("foo='bar'")
	expect(t, lx, expectedItems)
	// NL
	lx = lex("foo='bar'\r\n")
	expect(t, lx, expectedItems)
}

func TestSimpleKeyIntegerValues(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1},
		{itemInteger, "123", 1},
		{itemEOF, "", 1},
	}
	lx := lex("foo = 123")
	expect(t, lx, expectedItems)
	lx = lex("foo=123")
	expect(t, lx, expectedItems)
	lx = lex("foo=123\r\n")
	expect(t, lx, expectedItems)
}

func TestSimpleKeyFloatValues(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1},
		{itemFloat, "22.2", 1},
		{itemEOF, "", 1},
	}
	lx := lex("foo = 22.2")
	expect(t, lx, expectedItems)
	lx = lex("foo=22.2")
	expect(t, lx, expectedItems)
	lx = lex("foo=22.2\r\n")
	expect(t, lx, expectedItems)
}

func TestSimpleKeyBoolValues(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1},
		{itemBool, "true", 1},
		{itemEOF, "", 1},
	}
	lx := lex("foo = true")
	expect(t, lx, expectedItems)
	lx = lex("foo=true")
	expect(t, lx, expectedItems)
	lx = lex("foo=true\r\n")
	expect(t, lx, expectedItems)
}

func TestComments(t *testing.T) {
	expectedItems := []item{
		{itemCommentStart, "", 1},
		{itemText, " This is a comment", 1},
		{itemEOF, "", 1},
	}
	lx := lex("# This is a comment")
	expect(t, lx, expectedItems)
	lx = lex("# This is a comment\r\n")
	expect(t, lx, expectedItems)
	lx = lex("// This is a comment\r\n")
	expect(t, lx, expectedItems)
}

func TestArrays(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1},
		{itemArrayStart, "", 1},
		{itemInteger, "1", 1},
		{itemInteger, "2", 1},
		{itemInteger, "3", 1},
		{itemString, "bar", 1},
		{itemArrayEnd, "", 1},
		{itemEOF, "", 1},
	}
	lx := lex("foo = [1, 2, 3, 'bar']")
	expect(t, lx, expectedItems)
	lx = lex("foo = [1,2,3,'bar']")
	expect(t, lx, expectedItems)
	lx = lex("foo = [1, 2,3,'bar']")
	expect(t, lx, expectedItems)
}

var mlArray = `
# top level comment
foo = [
 1, # One
 2, // Two
 3 , // Three
 'bar'     ,
 "bar"
]
`
func TestMultilineArrays(t *testing.T) {
	expectedItems := []item{
		{itemCommentStart, "", 2},
		{itemText, " top level comment", 2},
		{itemKey, "foo", 3},
		{itemArrayStart, "", 3},
		{itemInteger, "1", 4},
		{itemCommentStart, "", 4},
		{itemText, " One", 4},
		{itemInteger, "2", 5},
		{itemCommentStart, "", 5},
		{itemText, " Two", 5},
		{itemInteger, "3", 6},
		{itemCommentStart, "", 6},
		{itemText, " Three", 6},
		{itemString, "bar", 7},
		{itemString, "bar", 8},
		{itemArrayEnd, "", 9},
		{itemEOF, "", 9},
	}
	lx := lex(mlArray)
	expect(t, lx, expectedItems)
}

var mlArrayNoSep = `
# top level comment
foo = [
 1
 2
 3
 'bar'
 "bar"
]
`
func TestMultilineArraysNoSep(t *testing.T) {
	expectedItems := []item{
		{itemCommentStart, "", 2},
		{itemText, " top level comment", 2},
		{itemKey, "foo", 3},
		{itemArrayStart, "", 3},
		{itemInteger, "1", 4},
		{itemInteger, "2", 5},
		{itemInteger, "3", 6},
		{itemString, "bar", 7},
		{itemString, "bar", 8},
		{itemArrayEnd, "", 9},
		{itemEOF, "", 9},
	}
	lx := lex(mlArrayNoSep)
	expect(t, lx, expectedItems)
}

func TestSimpleMap(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1},
		{itemMapStart, "", 1},
		{itemKey, "ip", 1},
		{itemString, "127.0.0.1", 1},
		{itemKey, "port", 1},
		{itemInteger, "4242", 1},
		{itemMapEnd, "", 1},
		{itemEOF, "", 1},
	}

	lx := lex("foo = {ip='127.0.0.1', port = 4242}")
	expect(t, lx, expectedItems)
}

var mlMap = `
foo = {
  ip = '127.0.0.1'
  port= 4242
}
`

func TestMultilineMap(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 2},
		{itemMapStart, "", 2},
		{itemKey, "ip", 3},
		{itemString, "127.0.0.1", 3},
		{itemKey, "port", 4},
		{itemInteger, "4242", 4},
		{itemMapEnd, "", 5},
		{itemEOF, "", 5},
	}

	lx := lex(mlMap)
	expect(t, lx, expectedItems)
}

var nestedMap = `
foo = {
  host = {
    ip = '127.0.0.1'
    port= 4242
  }
}
`

func TestNestedMaps(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 2},
		{itemMapStart, "", 2},
		{itemKey, "host", 3},
		{itemMapStart, "", 3},
		{itemKey, "ip", 4},
		{itemString, "127.0.0.1", 4},
		{itemKey, "port", 5},
		{itemInteger, "4242", 5},
		{itemMapEnd, "", 6},
		{itemMapEnd, "", 7},
		{itemEOF, "", 5},
	}

	lx := lex(nestedMap)
	expect(t, lx, expectedItems)
}

func TestColonKeySep(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1},
		{itemInteger, "123", 1},
		{itemEOF, "", 1},
	}
	lx := lex("foo : 123")
	expect(t, lx, expectedItems)
	lx = lex("foo:123")
	expect(t, lx, expectedItems)
	lx = lex("foo: 123")
	expect(t, lx, expectedItems)
	lx = lex("foo:  123\r\n")
	expect(t, lx, expectedItems)
}

func TestWhitespaceKeySep(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1},
		{itemInteger, "123", 1},
		{itemEOF, "", 1},
	}
	lx := lex("foo 123")
	expect(t, lx, expectedItems)
	lx = lex("foo 123")
	expect(t, lx, expectedItems)
	lx = lex("foo\t123")
	expect(t, lx, expectedItems)
	lx = lex("foo\t\t123\r\n")
	expect(t, lx, expectedItems)
}

var nestedWhitespaceMap = `
foo  {
  host  {
    ip = '127.0.0.1'
    port= 4242
  }
}
`

func TestNestedWhitespaceMaps(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 2},
		{itemMapStart, "", 2},
		{itemKey, "host", 3},
		{itemMapStart, "", 3},
		{itemKey, "ip", 4},
		{itemString, "127.0.0.1", 4},
		{itemKey, "port", 5},
		{itemInteger, "4242", 5},
		{itemMapEnd, "", 6},
		{itemMapEnd, "", 7},
		{itemEOF, "", 5},
	}

	lx := lex(nestedWhitespaceMap)
	expect(t, lx, expectedItems)
}

var semicolons = `
foo = 123;
bar = 'baz';
baz = 'boo'
map {
 id = 1;
}
`
func TestOptionalSemicolons(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 2},
		{itemInteger, "123", 2},
		{itemKey, "bar", 3},
		{itemString, "baz", 3},
		{itemKey, "baz", 4},
		{itemString, "boo", 4},
		{itemKey, "map", 5},
		{itemMapStart, "", 5},
		{itemKey, "id", 6},
		{itemInteger, "1", 6},
		{itemMapEnd, "", 7},
		{itemEOF, "", 5},
	}

	lx := lex(semicolons)
	expect(t, lx, expectedItems)
}

func TestSemicolonChaining(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1},
		{itemString, "1", 1},
		{itemKey, "bar", 1},
		{itemFloat, "2.2", 1},
		{itemKey, "baz", 1},
		{itemBool, "true", 1},
		{itemEOF, "", 1},
	}

	lx := lex("foo='1'; bar=2.2; baz=true;")
	expect(t, lx, expectedItems)
}







