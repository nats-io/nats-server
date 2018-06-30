package conf

import "testing"

// Test to make sure we get what we expect.
func expect(t *testing.T, lx *lexer, items []item) {
	for i := 0; i < len(items); i++ {
		item := lx.nextItem()
		_ = item.String()
		if item.typ == itemEOF {
			break
		}
		if item != items[i] {
			t.Fatalf("Testing: '%s'\nExpected %q, received %q\n",
				lx.input, items[i], item)
		}
		if item.typ == itemError {
			break
		}
	}
}

func TestPlainValue(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1},
		{itemEOF, "", 1},
	}
	lx := lex("foo")
	expect(t, lx, expectedItems)
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
	lx = lex("foo=\t'bar'\t")
	expect(t, lx, expectedItems)
}

func TestComplexStringValues(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1},
		{itemString, "bar\\r\\n  \\t", 1},
		{itemEOF, "", 2},
	}

	lx := lex("foo = 'bar\\r\\n  \\t'")
	expect(t, lx, expectedItems)
}

func TestBinaryString(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1},
		{itemString, "e", 1},
		{itemEOF, "", 1},
	}
	lx := lex("foo = \\x65")
	expect(t, lx, expectedItems)
}

func TestBinaryStringLatin1(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1},
		{itemString, "\xe9", 1},
		{itemEOF, "", 1},
	}
	lx := lex("foo = \\xe9")
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

func TestSimpleKeyNegativeIntegerValues(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1},
		{itemInteger, "-123", 1},
		{itemEOF, "", 1},
	}
	lx := lex("foo = -123")
	expect(t, lx, expectedItems)
	lx = lex("foo=-123")
	expect(t, lx, expectedItems)
	lx = lex("foo=-123\r\n")
	expect(t, lx, expectedItems)
}

func TestConvenientIntegerValues(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1},
		{itemInteger, "1k", 1},
		{itemEOF, "", 1},
	}
	lx := lex("foo = 1k")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "foo", 1},
		{itemInteger, "1K", 1},
		{itemEOF, "", 1},
	}
	lx = lex("foo = 1K")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "foo", 1},
		{itemInteger, "1m", 1},
		{itemEOF, "", 1},
	}
	lx = lex("foo = 1m")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "foo", 1},
		{itemInteger, "1M", 1},
		{itemEOF, "", 1},
	}
	lx = lex("foo = 1M")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "foo", 1},
		{itemInteger, "1g", 1},
		{itemEOF, "", 1},
	}
	lx = lex("foo = 1g")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "foo", 1},
		{itemInteger, "1G", 1},
		{itemEOF, "", 1},
	}
	lx = lex("foo = 1G")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "foo", 1},
		{itemInteger, "1MB", 1},
		{itemEOF, "", 1},
	}
	lx = lex("foo = 1MB")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "foo", 1},
		{itemInteger, "1Gb", 1},
		{itemEOF, "", 1},
	}
	lx = lex("foo = 1Gb")
	expect(t, lx, expectedItems)

	// Negative versions
	expectedItems = []item{
		{itemKey, "foo", 1},
		{itemInteger, "-1m", 1},
		{itemEOF, "", 1},
	}
	lx = lex("foo = -1m")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "foo", 1},
		{itemInteger, "-1GB", 1},
		{itemEOF, "", 1},
	}
	lx = lex("foo = -1GB ")
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

func TestBadBinaryStringEndingAfterZeroHexChars(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1},
		{itemError, "Expected two hexadecimal digits after '\\x', but hit end of line", 2},
		{itemEOF, "", 1},
	}
	lx := lex("foo = xyz\\x\n")
	expect(t, lx, expectedItems)
}

func TestBadBinaryStringEndingAfterOneHexChar(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1},
		{itemError, "Expected two hexadecimal digits after '\\x', but hit end of line", 2},
		{itemEOF, "", 1},
	}
	lx := lex("foo = xyz\\xF\n")
	expect(t, lx, expectedItems)
}

func TestBadBinaryStringWithZeroHexChars(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1},
		{itemError, "Expected two hexadecimal digits after '\\x', but got ']\"'", 1},
		{itemEOF, "", 1},
	}
	lx := lex(`foo = "[\x]"`)
	expect(t, lx, expectedItems)
}

func TestBadBinaryStringWithOneHexChar(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1},
		{itemError, "Expected two hexadecimal digits after '\\x', but got 'e]'", 1},
		{itemEOF, "", 1},
	}
	lx := lex(`foo = "[\xe]"`)
	expect(t, lx, expectedItems)
}

func TestBadFloatValues(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1},
		{itemError, "Floats must start with a digit", 1},
		{itemEOF, "", 1},
	}
	lx := lex("foo = .2")
	expect(t, lx, expectedItems)
}

func TestBadKey(t *testing.T) {
	expectedItems := []item{
		{itemError, "Unexpected key separator ':'", 1},
		{itemEOF, "", 1},
	}
	lx := lex(" :foo = 22")
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

func TestTopValuesWithComments(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1},
		{itemInteger, "123", 1},
		{itemCommentStart, "", 1},
		{itemText, " This is a comment", 1},
		{itemEOF, "", 1},
	}

	lx := lex("foo = 123 // This is a comment")
	expect(t, lx, expectedItems)
	lx = lex("foo=123    # This is a comment")
	expect(t, lx, expectedItems)
}

func TestRawString(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1},
		{itemString, "bar", 1},
		{itemEOF, "", 1},
	}

	lx := lex("foo = bar")
	expect(t, lx, expectedItems)

	lx = lex(`foo = bar' `) //'single-quote for emacs TODO: Remove me
	expect(t, lx, expectedItems)
}

func TestDateValues(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1},
		{itemDatetime, "2016-05-04T18:53:41Z", 1},
		{itemEOF, "", 1},
	}

	lx := lex("foo = 2016-05-04T18:53:41Z")
	expect(t, lx, expectedItems)
}

func TestVariableValues(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1},
		{itemVariable, "bar", 1},
		{itemEOF, "", 1},
	}
	lx := lex("foo = $bar")
	expect(t, lx, expectedItems)
	lx = lex("foo =$bar")
	expect(t, lx, expectedItems)
	lx = lex("foo $bar")
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
 3 # Three
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
 1 // foo
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
		{itemCommentStart, "", 4},
		{itemText, " foo", 4},
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
  ip = '127.0.0.1' # the IP
  port= 4242 // the port
}
`

func TestMultilineMap(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 2},
		{itemMapStart, "", 2},
		{itemKey, "ip", 3},
		{itemString, "127.0.0.1", 3},
		{itemCommentStart, "", 3},
		{itemText, " the IP", 3},
		{itemKey, "port", 4},
		{itemInteger, "4242", 4},
		{itemCommentStart, "", 4},
		{itemText, " the port", 4},
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

func TestQuotedKeys(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1},
		{itemInteger, "123", 1},
		{itemEOF, "", 1},
	}
	lx := lex("foo : 123")
	expect(t, lx, expectedItems)
	lx = lex("'foo' : 123")
	expect(t, lx, expectedItems)
	lx = lex("\"foo\" : 123")
	expect(t, lx, expectedItems)
}

func TestQuotedKeysWithSpace(t *testing.T) {
	expectedItems := []item{
		{itemKey, " foo", 1},
		{itemInteger, "123", 1},
		{itemEOF, "", 1},
	}
	lx := lex("' foo' : 123")
	expect(t, lx, expectedItems)
	lx = lex("\" foo\" : 123")
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

var escString = `
foo  = \t
bar  = \r
baz  = \n
q    = \"
bs   = \\
`

func TestEscapedString(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 2},
		{itemString, "\t", 2},
		{itemKey, "bar", 3},
		{itemString, "\r", 3},
		{itemKey, "baz", 4},
		{itemString, "\n", 4},
		{itemKey, "q", 5},
		{itemString, "\"", 5},
		{itemKey, "bs", 6},
		{itemString, "\\", 6},
		{itemEOF, "", 6},
	}
	lx := lex(escString)
	expect(t, lx, expectedItems)
}

func TestCompoundStringES(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1},
		{itemString, "\\end", 1},
		{itemEOF, "", 2},
	}
	lx := lex(`foo = "\\end"`)
	expect(t, lx, expectedItems)
}

func TestCompoundStringSE(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1},
		{itemString, "start\\", 1},
		{itemEOF, "", 2},
	}
	lx := lex(`foo = "start\\"`)
	expect(t, lx, expectedItems)
}

func TestCompoundStringEE(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1},
		{itemString, "Eq", 1},
		{itemEOF, "", 2},
	}
	lx := lex(`foo = \x45\x71`)
	expect(t, lx, expectedItems)
}

func TestCompoundStringSEE(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1},
		{itemString, "startEq", 1},
		{itemEOF, "", 2},
	}
	lx := lex(`foo = start\x45\x71`)
	expect(t, lx, expectedItems)
}

func TestCompoundStringSES(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1},
		{itemString, "start|end", 1},
		{itemEOF, "", 2},
	}
	lx := lex(`foo = start\x7Cend`)
	expect(t, lx, expectedItems)
}

func TestCompoundStringEES(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1},
		{itemString, "<>end", 1},
		{itemEOF, "", 2},
	}
	lx := lex(`foo = \x3c\x3eend`)
	expect(t, lx, expectedItems)
}

func TestCompoundStringESE(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1},
		{itemString, "<middle>", 1},
		{itemEOF, "", 2},
	}
	lx := lex(`foo = \x3cmiddle\x3E`)
	expect(t, lx, expectedItems)
}

func TestBadStringEscape(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1},
		{itemError, "Invalid escape character 'y'. Only the following escape characters are allowed: \\xXX, \\t, \\n, \\r, \\\", \\\\.", 1},
		{itemEOF, "", 2},
	}
	lx := lex(`foo = \y`)
	expect(t, lx, expectedItems)
}

func TestNonBool(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1},
		{itemString, "\\true", 1},
		{itemEOF, "", 2},
	}
	lx := lex(`foo = \\true`)
	expect(t, lx, expectedItems)
}

func TestNonVariable(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1},
		{itemString, "\\$var", 1},
		{itemEOF, "", 2},
	}
	lx := lex(`foo = \\$var`)
	expect(t, lx, expectedItems)
}

func TestEmptyStringDQ(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1},
		{itemString, "", 1},
		{itemEOF, "", 2},
	}
	lx := lex(`foo = ""`)
	expect(t, lx, expectedItems)
}

func TestEmptyStringSQ(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1},
		{itemString, "", 1},
		{itemEOF, "", 2},
	}
	lx := lex(`foo = ''`)
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

var noquotes = `
foo = 123
bar = baz
baz=boo
map {
 id:one
 id2 : onetwo
}
t true
f false
tstr "true"
tkey = two
fkey = five # This should be a string
`

func TestNonQuotedStrings(t *testing.T) {
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
		{itemString, "one", 6},
		{itemKey, "id2", 7},
		{itemString, "onetwo", 7},
		{itemMapEnd, "", 8},
		{itemKey, "t", 9},
		{itemBool, "true", 9},
		{itemKey, "f", 10},
		{itemBool, "false", 10},
		{itemKey, "tstr", 11},
		{itemString, "true", 11},
		{itemKey, "tkey", 12},
		{itemString, "two", 12},
		{itemKey, "fkey", 13},
		{itemString, "five", 13},
		{itemCommentStart, "", 13},
		{itemText, " This should be a string", 13},

		{itemEOF, "", 14},
	}
	lx := lex(noquotes)
	expect(t, lx, expectedItems)
}

func TestMapQuotedKeys(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1},
		{itemMapStart, "", 1},
		{itemKey, "bar", 1},
		{itemInteger, "4242", 1},
		{itemMapEnd, "", 1},
		{itemEOF, "", 1},
	}
	lx := lex("foo = {'bar' = 4242}")
	expect(t, lx, expectedItems)
	lx = lex("foo = {\"bar\" = 4242}")
	expect(t, lx, expectedItems)
}

func TestSpecialCharsMapQuotedKeys(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1},
		{itemMapStart, "", 1},
		{itemKey, "bar-1.2.3", 1},
		{itemMapStart, "", 1},
		{itemKey, "port", 1},
		{itemInteger, "4242", 1},
		{itemMapEnd, "", 1},
		{itemMapEnd, "", 1},
		{itemEOF, "", 1},
	}
	lx := lex("foo = {'bar-1.2.3' = { port:4242 }}")
	expect(t, lx, expectedItems)
	lx = lex("foo = {\"bar-1.2.3\" = { port:4242 }}")
	expect(t, lx, expectedItems)
}

var mlnestedmap = `
systems {
  allinone {
    description: "This is a description."
  }
}
`

func TestDoubleNestedMapsNewLines(t *testing.T) {
	expectedItems := []item{
		{itemKey, "systems", 2},
		{itemMapStart, "", 2},
		{itemKey, "allinone", 3},
		{itemMapStart, "", 3},
		{itemKey, "description", 4},
		{itemString, "This is a description.", 4},
		{itemMapEnd, "", 5},
		{itemMapEnd, "", 6},
		{itemEOF, "", 7},
	}
	lx := lex(mlnestedmap)
	expect(t, lx, expectedItems)
}

var blockexample = `
numbers (
1234567890
)
`

func TestBlockString(t *testing.T) {
	expectedItems := []item{
		{itemKey, "numbers", 2},
		{itemString, "\n1234567890\n", 4},
	}
	lx := lex(blockexample)
	expect(t, lx, expectedItems)
}

func TestBlockStringEOF(t *testing.T) {
	expectedItems := []item{
		{itemKey, "numbers", 2},
		{itemString, "\n1234567890\n", 4},
	}
	blockbytes := []byte(blockexample[0 : len(blockexample)-1])
	blockbytes = append(blockbytes, 0)
	lx := lex(string(blockbytes))
	expect(t, lx, expectedItems)
}

var mlblockexample = `
numbers (
  12(34)56
  (
    7890
  )
)
`

func TestBlockStringMultiLine(t *testing.T) {
	expectedItems := []item{
		{itemKey, "numbers", 2},
		{itemString, "\n  12(34)56\n  (\n    7890\n  )\n", 7},
	}
	lx := lex(mlblockexample)
	expect(t, lx, expectedItems)
}

func TestUnquotedIPAddr(t *testing.T) {
	expectedItems := []item{
		{itemKey, "listen", 1},
		{itemString, "127.0.0.1:4222", 1},
		{itemEOF, "", 1},
	}
	lx := lex("listen: 127.0.0.1:4222")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "listen", 1},
		{itemString, "127.0.0.1", 1},
		{itemEOF, "", 1},
	}
	lx = lex("listen: 127.0.0.1")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "listen", 1},
		{itemString, "apcera.me:80", 1},
		{itemEOF, "", 1},
	}
	lx = lex("listen: apcera.me:80")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "listen", 1},
		{itemString, "nats.io:-1", 1},
		{itemEOF, "", 1},
	}
	lx = lex("listen: nats.io:-1")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "listen", 1},
		{itemInteger, "-1", 1},
		{itemEOF, "", 1},
	}
	lx = lex("listen: -1")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "listen", 1},
		{itemString, ":-1", 1},
		{itemEOF, "", 1},
	}
	lx = lex("listen: :-1")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "listen", 1},
		{itemString, ":80", 1},
		{itemEOF, "", 1},
	}
	lx = lex("listen = :80")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "listen", 1},
		{itemArrayStart, "", 1},
		{itemString, "localhost:4222", 1},
		{itemString, "localhost:4333", 1},
		{itemArrayEnd, "", 1},
		{itemEOF, "", 1},
	}
	lx = lex("listen = [localhost:4222, localhost:4333]")
	expect(t, lx, expectedItems)
}

var arrayOfMaps = `
authorization {
    users = [
      {user: alice, password: foo}
      {user: bob,   password: bar}
    ]
    timeout: 0.5
}
`

func TestArrayOfMaps(t *testing.T) {
	expectedItems := []item{
		{itemKey, "authorization", 2},
		{itemMapStart, "", 2},
		{itemKey, "users", 3},
		{itemArrayStart, "", 3},
		{itemMapStart, "", 4},
		{itemKey, "user", 4},
		{itemString, "alice", 4},
		{itemKey, "password", 4},
		{itemString, "foo", 4},
		{itemMapEnd, "", 4},
		{itemMapStart, "", 5},
		{itemKey, "user", 5},
		{itemString, "bob", 5},
		{itemKey, "password", 5},
		{itemString, "bar", 5},
		{itemMapEnd, "", 5},
		{itemArrayEnd, "", 6},
		{itemKey, "timeout", 7},
		{itemFloat, "0.5", 7},
		{itemMapEnd, "", 8},
		{itemEOF, "", 9},
	}
	lx := lex(arrayOfMaps)
	expect(t, lx, expectedItems)
}

func TestInclude(t *testing.T) {
	expectedItems := []item{
		{itemInclude, "users.conf", 1},
		{itemEOF, "", 1},
	}
	lx := lex("include \"users.conf\"")
	expect(t, lx, expectedItems)

	lx = lex("include 'users.conf'")
	expect(t, lx, expectedItems)

	lx = lex("include users.conf")
	expect(t, lx, expectedItems)
}

func TestMapInclude(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1},
		{itemMapStart, "", 1},
		{itemInclude, "users.conf", 1},
		{itemMapEnd, "", 1},
		{itemEOF, "", 1},
	}

	lx := lex("foo { include users.conf }")
	expect(t, lx, expectedItems)

	lx = lex("foo {include users.conf}")
	expect(t, lx, expectedItems)

	lx = lex("foo { include 'users.conf' }")
	expect(t, lx, expectedItems)

	lx = lex("foo { include \"users.conf\"}")
	expect(t, lx, expectedItems)
}

func TestJSONCompat(t *testing.T) {
	for _, test := range []struct {
		name     string
		input    string
		expected []item
	}{
		{
			name: "should omit initial and final brackets at top level with a single item",
			input: `
                        {
                          "http_port": 8223
                        }
                        `,
			expected: []item{
				{itemKey, "http_port", 3},
				{itemInteger, "8223", 3},
			},
		},
		{
			name: "should omit trailing commas at top level with two items",
			input: `
                        {
                          "http_port": 8223,
                          "port": 4223
                        }
                        `,
			expected: []item{
				{itemKey, "http_port", 3},
				{itemInteger, "8223", 3},
				{itemKey, "port", 4},
				{itemInteger, "4223", 4},
			},
		},
		{
			name: "should omit trailing commas at top level with multiple items",
			input: `
                        {
                          "http_port": 8223,
                          "port": 4223,
                          "max_payload": "5MB",
                          "debug": true,
                          "max_control_line": 1024
                        }
                        `,
			expected: []item{
				{itemKey, "http_port", 3},
				{itemInteger, "8223", 3},
				{itemKey, "port", 4},
				{itemInteger, "4223", 4},
				{itemKey, "max_payload", 5},
				{itemString, "5MB", 5},
				{itemKey, "debug", 6},
				{itemBool, "true", 6},
				{itemKey, "max_control_line", 7},
				{itemInteger, "1024", 7},
			},
		},
		{
			name: "should support JSON not prettified",
			input: `{"http_port": 8224,"port": 4224}
                        `,
			expected: []item{
				{itemKey, "http_port", 1},
				{itemInteger, "8224", 1},
				{itemKey, "port", 1},
				{itemInteger, "4224", 1},
			},
		},
		{
			name: "should support JSON not prettified with final bracket after newline",
			input: `{"http_port": 8225,"port": 4225
                        }
                        `,
			expected: []item{
				{itemKey, "http_port", 1},
				{itemInteger, "8225", 1},
				{itemKey, "port", 1},
				{itemInteger, "4225", 1},
			},
		},
		{
			name: "should support uglified JSON with inner blocks",
			input: `{"http_port": 8227,"port": 4227,"write_deadline": "1h","cluster": {"port": 6222,"routes": ["nats://127.0.0.1:4222","nats://127.0.0.1:4223","nats://127.0.0.1:4224"]}}
                        `,
			expected: []item{
				{itemKey, "http_port", 1},
				{itemInteger, "8227", 1},
				{itemKey, "port", 1},
				{itemInteger, "4227", 1},
				{itemKey, "write_deadline", 1},
				{itemString, "1h", 1},
				{itemKey, "cluster", 1},
				{itemMapStart, "", 1},
				{itemKey, "port", 1},
				{itemInteger, "6222", 1},
				{itemKey, "routes", 1},
				{itemArrayStart, "", 1},
				{itemString, "nats://127.0.0.1:4222", 1},
				{itemString, "nats://127.0.0.1:4223", 1},
				{itemString, "nats://127.0.0.1:4224", 1},
				{itemArrayEnd, "", 1},
				{itemMapEnd, "", 1},
			},
		},
		{
			name: "should support prettified JSON with inner blocks",
			input: `
                        {
                          "http_port": 8227,
                          "port": 4227,
                          "write_deadline": "1h",
                          "cluster": {
                            "port": 6222,
                            "routes": [
                              "nats://127.0.0.1:4222",
                              "nats://127.0.0.1:4223",
                              "nats://127.0.0.1:4224"
                            ]
                          }
                        }
                        `,
			expected: []item{
				{itemKey, "http_port", 3},
				{itemInteger, "8227", 3},
				{itemKey, "port", 4},
				{itemInteger, "4227", 4},
				{itemKey, "write_deadline", 5},
				{itemString, "1h", 5},
				{itemKey, "cluster", 6},
				{itemMapStart, "", 6},
				{itemKey, "port", 7},
				{itemInteger, "6222", 7},
				{itemKey, "routes", 8},
				{itemArrayStart, "", 8},
				{itemString, "nats://127.0.0.1:4222", 9},
				{itemString, "nats://127.0.0.1:4223", 10},
				{itemString, "nats://127.0.0.1:4224", 11},
				{itemArrayEnd, "", 12},
				{itemMapEnd, "", 13},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			lx := lex(test.input)
			expect(t, lx, test.expected)
		})
	}
}
