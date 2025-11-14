package conf

import "testing"

// Test to make sure we get what we expect.
func expect(t *testing.T, lx *lexer, items []item) {
	t.Helper()
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
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx := lex("foo")
	expect(t, lx, expectedItems)
}

func TestSimpleKeyStringValues(t *testing.T) {
	// Double quotes
	expectedItems := []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemString, val: "bar", line: 1, pos: 7},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx := lex("foo = \"bar\"")
	expect(t, lx, expectedItems)

	// Single quotes
	expectedItems = []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemString, val: "bar", line: 1, pos: 7},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx = lex("foo = 'bar'")
	expect(t, lx, expectedItems)

	// No spaces
	expectedItems = []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemString, val: "bar", line: 1, pos: 5},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx = lex("foo='bar'")
	expect(t, lx, expectedItems)

	// NL
	expectedItems = []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemString, val: "bar", line: 1, pos: 5},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx = lex("foo='bar'\r\n")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemString, val: "bar", line: 1, pos: 6},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx = lex("foo=\t'bar'\t")
	expect(t, lx, expectedItems)
}

func TestComplexStringValues(t *testing.T) {
	expectedItems := []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemString, val: "bar\\r\\n  \\t", line: 1, pos: 7},
		{typ: itemEOF, val: "", line: 2, pos: 0},
	}

	lx := lex("foo = 'bar\\r\\n  \\t'")
	expect(t, lx, expectedItems)
}

func TestStringStartingWithNumber(t *testing.T) {
	expectedItems := []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemString, val: "3xyz", line: 1, pos: 6},
		{typ: itemEOF, val: "", line: 2, pos: 0},
	}

	lx := lex(`foo = 3xyz`)
	expect(t, lx, expectedItems)

	lx = lex(`foo = 3xyz,`)
	expect(t, lx, expectedItems)

	lx = lex(`foo = 3xyz;`)
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{typ: itemKey, val: "foo", line: 2, pos: 9},
		{typ: itemString, val: "3xyz", line: 2, pos: 15},
		{typ: itemEOF, val: "", line: 2, pos: 0},
	}
	content := `
        foo = 3xyz
        `
	lx = lex(content)
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{typ: itemKey, val: "map", line: 2, pos: 9},
		{typ: itemMapStart, val: "", line: 2, pos: 14},
		{typ: itemKey, val: "foo", line: 3, pos: 11},
		{typ: itemString, val: "3xyz", line: 3, pos: 17},
		{typ: itemMapEnd, val: "", line: 3, pos: 22},
		{typ: itemEOF, val: "", line: 2, pos: 0},
	}
	content = `
        map {
          foo = 3xyz}
        `
	lx = lex(content)
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{typ: itemKey, val: "map", line: 2, pos: 9},
		{typ: itemMapStart, val: "", line: 2, pos: 14},
		{typ: itemKey, val: "foo", line: 3, pos: 11},
		{typ: itemString, val: "3xyz", line: 3, pos: 17},
		{typ: itemMapEnd, val: "", line: 4, pos: 10},
		{typ: itemEOF, val: "", line: 2, pos: 0},
	}
	content = `
        map {
          foo = 3xyz;
        }
        `
	lx = lex(content)
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{typ: itemKey, val: "map", line: 2, pos: 9},
		{typ: itemMapStart, val: "", line: 2, pos: 14},
		{typ: itemKey, val: "foo", line: 3, pos: 11},
		{typ: itemString, val: "3xyz", line: 3, pos: 17},
		{typ: itemKey, val: "bar", line: 4, pos: 11},
		{typ: itemString, val: "4wqs", line: 4, pos: 17},
		{typ: itemMapEnd, val: "", line: 5, pos: 10},
		{typ: itemEOF, val: "", line: 2, pos: 0},
	}
	content = `
        map {
          foo = 3xyz,
          bar = 4wqs
        }
        `
	lx = lex(content)
	expect(t, lx, expectedItems)
}

func TestBinaryString(t *testing.T) {
	expectedItems := []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemString, val: "e", line: 1, pos: 9},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx := lex("foo = \\x65")
	expect(t, lx, expectedItems)
}

func TestBinaryStringLatin1(t *testing.T) {
	expectedItems := []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemString, val: "\xe9", line: 1, pos: 9},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx := lex("foo = \\xe9")
	expect(t, lx, expectedItems)
}

func TestSimpleKeyIntegerValues(t *testing.T) {
	expectedItems := []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemInteger, val: "123", line: 1, pos: 6},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx := lex("foo = 123")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemInteger, val: "123", line: 1, pos: 4},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx = lex("foo=123")
	expect(t, lx, expectedItems)
	lx = lex("foo=123\r\n")
	expect(t, lx, expectedItems)
}

func TestSimpleKeyNegativeIntegerValues(t *testing.T) {
	expectedItems := []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemInteger, val: "-123", line: 1, pos: 6},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx := lex("foo = -123")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemInteger, val: "-123", line: 1, pos: 4},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx = lex("foo=-123")
	expect(t, lx, expectedItems)
	lx = lex("foo=-123\r\n")
	expect(t, lx, expectedItems)
}

func TestConvenientIntegerValues(t *testing.T) {
	expectedItems := []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemInteger, val: "1k", line: 1, pos: 6},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx := lex("foo = 1k")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemInteger, val: "1K", line: 1, pos: 6},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx = lex("foo = 1K")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemInteger, val: "1m", line: 1, pos: 6},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx = lex("foo = 1m")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemInteger, val: "1M", line: 1, pos: 6},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx = lex("foo = 1M")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemInteger, val: "1g", line: 1, pos: 6},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx = lex("foo = 1g")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemInteger, val: "1G", line: 1, pos: 6},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx = lex("foo = 1G")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemInteger, val: "1MB", line: 1, pos: 6},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx = lex("foo = 1MB")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemInteger, val: "1Gb", line: 1, pos: 6},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx = lex("foo = 1Gb")
	expect(t, lx, expectedItems)

	// Negative versions
	expectedItems = []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemInteger, val: "-1m", line: 1, pos: 6},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx = lex("foo = -1m")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemInteger, val: "-1GB", line: 1, pos: 6},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx = lex("foo = -1GB ")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemString, val: "1Ghz", line: 1, pos: 6},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx = lex("foo = 1Ghz")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemString, val: "2Pie", line: 1, pos: 6},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx = lex("foo = 2Pie")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemString, val: "3Mbs", line: 1, pos: 6},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx = lex("foo = 3Mbs,")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemInteger, val: "4Gb", line: 1, pos: 6},
		{typ: itemKey, val: "bar", line: 1, pos: 11},
		{typ: itemString, val: "5Gø", line: 1, pos: 17},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx = lex("foo = 4Gb, bar = 5Gø")
	expect(t, lx, expectedItems)
}

func TestSimpleKeyFloatValues(t *testing.T) {
	expectedItems := []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemFloat, val: "22.2", line: 1, pos: 6},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx := lex("foo = 22.2")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemFloat, val: "22.2", line: 1, pos: 4},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx = lex("foo=22.2")
	expect(t, lx, expectedItems)
	lx = lex("foo=22.2\r\n")
	expect(t, lx, expectedItems)
}

func TestBadBinaryStringEndingAfterZeroHexChars(t *testing.T) {
	expectedItems := []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemError, val: "Expected two hexadecimal digits after '\\x', but hit end of line", line: 2, pos: 1},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx := lex("foo = xyz\\x\n")
	expect(t, lx, expectedItems)
}

func TestBadBinaryStringEndingAfterOneHexChar(t *testing.T) {
	expectedItems := []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemError, val: "Expected two hexadecimal digits after '\\x', but hit end of line", line: 2, pos: 1},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx := lex("foo = xyz\\xF\n")
	expect(t, lx, expectedItems)
}

func TestBadBinaryStringWithZeroHexChars(t *testing.T) {
	expectedItems := []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemError, val: "Expected two hexadecimal digits after '\\x', but got ']\"'", line: 1, pos: 12},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx := lex(`foo = "[\x]"`)
	expect(t, lx, expectedItems)
}

func TestBadBinaryStringWithOneHexChar(t *testing.T) {
	expectedItems := []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemError, val: "Expected two hexadecimal digits after '\\x', but got 'e]'", line: 1, pos: 12},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx := lex(`foo = "[\xe]"`)
	expect(t, lx, expectedItems)
}

func TestBadFloatValues(t *testing.T) {
	expectedItems := []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemError, val: "Floats must start with a digit", line: 1, pos: 7},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx := lex("foo = .2")
	expect(t, lx, expectedItems)
}

func TestBadKey(t *testing.T) {
	expectedItems := []item{
		{typ: itemError, val: "Unexpected key separator ':'", line: 1, pos: 1},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx := lex(" :foo = 22")
	expect(t, lx, expectedItems)
}

func TestSimpleKeyBoolValues(t *testing.T) {
	expectedItems := []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemBool, val: "true", line: 1, pos: 6},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx := lex("foo = true")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemBool, val: "true", line: 1, pos: 4},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx = lex("foo=true")
	expect(t, lx, expectedItems)
	lx = lex("foo=true\r\n")
	expect(t, lx, expectedItems)
}

func TestComments(t *testing.T) {
	expectedItems := []item{
		{typ: itemCommentStart, val: "", line: 1, pos: 1},
		{typ: itemText, val: " This is a comment", line: 1, pos: 1},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx := lex("# This is a comment")
	expect(t, lx, expectedItems)
	lx = lex("# This is a comment\r\n")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{typ: itemCommentStart, val: "", line: 1, pos: 2},
		{typ: itemText, val: " This is a comment", line: 1, pos: 2},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx = lex("// This is a comment\r\n")
	expect(t, lx, expectedItems)
}

func TestTopValuesWithComments(t *testing.T) {
	expectedItems := []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemInteger, val: "123", line: 1, pos: 6},
		{typ: itemCommentStart, val: "", line: 1, pos: 12},
		{typ: itemText, val: " This is a comment", line: 1, pos: 12},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}

	lx := lex("foo = 123 // This is a comment")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemInteger, val: "123", line: 1, pos: 4},
		{typ: itemCommentStart, val: "", line: 1, pos: 12},
		{typ: itemText, val: " This is a comment", line: 1, pos: 12},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx = lex("foo=123    # This is a comment")
	expect(t, lx, expectedItems)
}

func TestRawString(t *testing.T) {
	expectedItems := []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemString, val: "bar", line: 1, pos: 6},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx := lex("foo = bar")
	expect(t, lx, expectedItems)
	lx = lex(`foo = bar' `)
	expect(t, lx, expectedItems)
}

func TestDateValues(t *testing.T) {
	expectedItems := []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemDatetime, val: "2016-05-04T18:53:41Z", line: 1, pos: 6},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}

	lx := lex("foo = 2016-05-04T18:53:41Z")
	expect(t, lx, expectedItems)
}

func TestVariableValues(t *testing.T) {
	expectedItems := []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemVariable, val: "bar", line: 1, pos: 7},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx := lex("foo = $bar")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemVariable, val: "bar", line: 1, pos: 6},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx = lex("foo =$bar")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemVariable, val: "bar", line: 1, pos: 5},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx = lex("foo $bar")
	expect(t, lx, expectedItems)
}

func TestArrays(t *testing.T) {
	expectedItems := []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemArrayStart, val: "", line: 1, pos: 7},
		{typ: itemInteger, val: "1", line: 1, pos: 7},
		{typ: itemInteger, val: "2", line: 1, pos: 10},
		{typ: itemInteger, val: "3", line: 1, pos: 13},
		{typ: itemString, val: "bar", line: 1, pos: 17},
		{typ: itemArrayEnd, val: "", line: 1, pos: 22},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx := lex("foo = [1, 2, 3, 'bar']")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemArrayStart, val: "", line: 1, pos: 7},
		{typ: itemInteger, val: "1", line: 1, pos: 7},
		{typ: itemInteger, val: "2", line: 1, pos: 9},
		{typ: itemInteger, val: "3", line: 1, pos: 11},
		{typ: itemString, val: "bar", line: 1, pos: 14},
		{typ: itemArrayEnd, val: "", line: 1, pos: 19},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx = lex("foo = [1,2,3,'bar']")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemArrayStart, val: "", line: 1, pos: 7},
		{typ: itemInteger, val: "1", line: 1, pos: 7},
		{typ: itemInteger, val: "2", line: 1, pos: 10},
		{typ: itemInteger, val: "3", line: 1, pos: 12},
		{typ: itemString, val: "bar", line: 1, pos: 15},
		{typ: itemArrayEnd, val: "", line: 1, pos: 20},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
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
		{typ: itemCommentStart, val: "", line: 2, pos: 2},
		{typ: itemText, val: " top level comment", line: 2, pos: 2},
		{typ: itemKey, val: "foo", line: 3, pos: 1},
		{typ: itemArrayStart, val: "", line: 3, pos: 8},
		{typ: itemInteger, val: "1", line: 4, pos: 2},
		{typ: itemCommentStart, val: "", line: 4, pos: 6},
		{typ: itemText, val: " One", line: 4, pos: 6},
		{typ: itemInteger, val: "2", line: 5, pos: 2},
		{typ: itemCommentStart, val: "", line: 5, pos: 7},
		{typ: itemText, val: " Two", line: 5, pos: 7},
		{typ: itemInteger, val: "3", line: 6, pos: 2},
		{typ: itemCommentStart, val: "", line: 6, pos: 5},
		{typ: itemText, val: " Three", line: 6, pos: 5},
		{typ: itemString, val: "bar", line: 7, pos: 3},
		{typ: itemString, val: "bar", line: 8, pos: 3},
		{typ: itemArrayEnd, val: "", line: 9, pos: 2},
		{typ: itemEOF, val: "", line: 9, pos: 0},
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
		{typ: itemCommentStart, val: "", line: 2, pos: 2},
		{typ: itemText, val: " top level comment", line: 2, pos: 2},
		{typ: itemKey, val: "foo", line: 3, pos: 1},
		{typ: itemArrayStart, val: "", line: 3, pos: 8},
		{typ: itemInteger, val: "1", line: 4, pos: 2},
		{typ: itemCommentStart, val: "", line: 4, pos: 6},
		{typ: itemText, val: " foo", line: 4, pos: 6},
		{typ: itemInteger, val: "2", line: 5, pos: 2},
		{typ: itemInteger, val: "3", line: 6, pos: 2},
		{typ: itemString, val: "bar", line: 7, pos: 3},
		{typ: itemString, val: "bar", line: 8, pos: 3},
		{typ: itemArrayEnd, val: "", line: 9, pos: 2},
		{typ: itemEOF, val: "", line: 9, pos: 0},
	}
	lx := lex(mlArrayNoSep)
	expect(t, lx, expectedItems)
}

func TestSimpleMap(t *testing.T) {
	expectedItems := []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemMapStart, val: "", line: 1, pos: 7},
		{typ: itemKey, val: "ip", line: 1, pos: 7},
		{typ: itemString, val: "127.0.0.1", line: 1, pos: 11},
		{typ: itemKey, val: "port", line: 1, pos: 23},
		{typ: itemInteger, val: "4242", line: 1, pos: 30},
		{typ: itemMapEnd, val: "", line: 1, pos: 35},
		{typ: itemEOF, val: "", line: 1, pos: 0},
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
		{typ: itemKey, val: "foo", line: 2, pos: 1},
		{typ: itemMapStart, val: "", line: 2, pos: 8},
		{typ: itemKey, val: "ip", line: 3, pos: 3},
		{typ: itemString, val: "127.0.0.1", line: 3, pos: 9},
		{typ: itemCommentStart, val: "", line: 3, pos: 21},
		{typ: itemText, val: " the IP", line: 3, pos: 21},
		{typ: itemKey, val: "port", line: 4, pos: 3},
		{typ: itemInteger, val: "4242", line: 4, pos: 9},
		{typ: itemCommentStart, val: "", line: 4, pos: 16},
		{typ: itemText, val: " the port", line: 4, pos: 16},
		{typ: itemMapEnd, val: "", line: 5, pos: 2},
		{typ: itemEOF, val: "", line: 5, pos: 0},
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
		{typ: itemKey, val: "foo", line: 2, pos: 1},
		{typ: itemMapStart, val: "", line: 2, pos: 8},
		{typ: itemKey, val: "host", line: 3, pos: 3},
		{typ: itemMapStart, val: "", line: 3, pos: 11},
		{typ: itemKey, val: "ip", line: 4, pos: 5},
		{typ: itemString, val: "127.0.0.1", line: 4, pos: 11},
		{typ: itemKey, val: "port", line: 5, pos: 5},
		{typ: itemInteger, val: "4242", line: 5, pos: 11},
		{typ: itemMapEnd, val: "", line: 6, pos: 4},
		{typ: itemMapEnd, val: "", line: 7, pos: 2},
		{typ: itemEOF, val: "", line: 7, pos: 0},
	}

	lx := lex(nestedMap)
	expect(t, lx, expectedItems)
}

func TestQuotedKeys(t *testing.T) {
	expectedItems := []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemInteger, val: "123", line: 1, pos: 6},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx := lex("foo : 123")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{typ: itemKey, val: "foo", line: 1, pos: 1},
		{typ: itemInteger, val: "123", line: 1, pos: 8},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx = lex("'foo' : 123")
	expect(t, lx, expectedItems)
	lx = lex("\"foo\" : 123")
	expect(t, lx, expectedItems)
}

func TestQuotedKeysWithSpace(t *testing.T) {
	expectedItems := []item{
		{typ: itemKey, val: " foo", line: 1, pos: 1},
		{typ: itemInteger, val: "123", line: 1, pos: 9},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx := lex("' foo' : 123")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{typ: itemKey, val: " foo", line: 1, pos: 1},
		{typ: itemInteger, val: "123", line: 1, pos: 9},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx = lex("\" foo\" : 123")
	expect(t, lx, expectedItems)
}

func TestColonKeySep(t *testing.T) {
	expectedItems := []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemInteger, val: "123", line: 1, pos: 6},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx := lex("foo : 123")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemInteger, val: "123", line: 1, pos: 4},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx = lex("foo:123")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemInteger, val: "123", line: 1, pos: 5},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx = lex("foo: 123")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemInteger, val: "123", line: 1, pos: 6},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx = lex("foo:  123\r\n")
	expect(t, lx, expectedItems)
}

func TestWhitespaceKeySep(t *testing.T) {
	expectedItems := []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemInteger, val: "123", line: 1, pos: 4},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx := lex("foo 123")
	expect(t, lx, expectedItems)
	lx = lex("foo 123")
	expect(t, lx, expectedItems)
	lx = lex("foo\t123")
	expect(t, lx, expectedItems)
	expectedItems = []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemInteger, val: "123", line: 1, pos: 5},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
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
		{typ: itemKey, val: "foo", line: 2, pos: 1},
		{typ: itemString, val: "\t", line: 2, pos: 9},
		{typ: itemKey, val: "bar", line: 3, pos: 1},
		{typ: itemString, val: "\r", line: 3, pos: 9},
		{typ: itemKey, val: "baz", line: 4, pos: 1},
		{typ: itemString, val: "\n", line: 4, pos: 9},
		{typ: itemKey, val: "q", line: 5, pos: 1},
		{typ: itemString, val: "\"", line: 5, pos: 9},
		{typ: itemKey, val: "bs", line: 6, pos: 1},
		{typ: itemString, val: "\\", line: 6, pos: 9},
		{typ: itemEOF, val: "", line: 6, pos: 0},
	}
	lx := lex(escString)
	expect(t, lx, expectedItems)
}

func TestCompoundStringES(t *testing.T) {
	expectedItems := []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemString, val: "\\end", line: 1, pos: 8},
		{typ: itemEOF, val: "", line: 2, pos: 0},
	}
	lx := lex(`foo = "\\end"`)
	expect(t, lx, expectedItems)
}

func TestCompoundStringSE(t *testing.T) {
	expectedItems := []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemString, val: "start\\", line: 1, pos: 8},
		{typ: itemEOF, val: "", line: 2, pos: 0},
	}
	lx := lex(`foo = "start\\"`)
	expect(t, lx, expectedItems)
}

func TestCompoundStringEE(t *testing.T) {
	expectedItems := []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemString, val: "Eq", line: 1, pos: 12},
		{typ: itemEOF, val: "", line: 2, pos: 0},
	}
	lx := lex(`foo = \x45\x71`)
	expect(t, lx, expectedItems)
}

func TestCompoundStringSEE(t *testing.T) {
	expectedItems := []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemString, val: "startEq", line: 1, pos: 12},
		{typ: itemEOF, val: "", line: 2, pos: 0},
	}
	lx := lex(`foo = start\x45\x71`)
	expect(t, lx, expectedItems)
}

func TestCompoundStringSES(t *testing.T) {
	expectedItems := []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemString, val: "start|end", line: 1, pos: 9},
		{typ: itemEOF, val: "", line: 2, pos: 0},
	}
	lx := lex(`foo = start\x7Cend`)
	expect(t, lx, expectedItems)
}

func TestCompoundStringEES(t *testing.T) {
	expectedItems := []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemString, val: "<>end", line: 1, pos: 12},
		{typ: itemEOF, val: "", line: 2, pos: 0},
	}
	lx := lex(`foo = \x3c\x3eend`)
	expect(t, lx, expectedItems)
}

func TestCompoundStringESE(t *testing.T) {
	expectedItems := []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemString, val: "<middle>", line: 1, pos: 12},
		{typ: itemEOF, val: "", line: 2, pos: 0},
	}
	lx := lex(`foo = \x3cmiddle\x3E`)
	expect(t, lx, expectedItems)
}

func TestBadStringEscape(t *testing.T) {
	expectedItems := []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemError, val: "Invalid escape character 'y'. Only the following escape characters are allowed: \\xXX, \\t, \\n, \\r, \\\", \\\\.", line: 1, pos: 8},
		{typ: itemEOF, val: "", line: 2, pos: 0},
	}
	lx := lex(`foo = \y`)
	expect(t, lx, expectedItems)
}

func TestNonBool(t *testing.T) {
	expectedItems := []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemString, val: "\\true", line: 1, pos: 7},
		{typ: itemEOF, val: "", line: 2, pos: 0},
	}
	lx := lex(`foo = \\true`)
	expect(t, lx, expectedItems)
}

func TestNonVariable(t *testing.T) {
	expectedItems := []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemString, val: "\\$var", line: 1, pos: 7},
		{typ: itemEOF, val: "", line: 2, pos: 0},
	}
	lx := lex(`foo = \\$var`)
	expect(t, lx, expectedItems)
}

func TestEmptyStringDQ(t *testing.T) {
	expectedItems := []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemString, val: "", line: 1, pos: 7},
		{typ: itemEOF, val: "", line: 2, pos: 0},
	}
	lx := lex(`foo = ""`)
	expect(t, lx, expectedItems)
}

func TestEmptyStringSQ(t *testing.T) {
	expectedItems := []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemString, val: "", line: 1, pos: 7},
		{typ: itemEOF, val: "", line: 2, pos: 0},
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
		{typ: itemKey, val: "foo", line: 2, pos: 1},
		{typ: itemMapStart, val: "", line: 2, pos: 7},
		{typ: itemKey, val: "host", line: 3, pos: 3},
		{typ: itemMapStart, val: "", line: 3, pos: 10},
		{typ: itemKey, val: "ip", line: 4, pos: 5},
		{typ: itemString, val: "127.0.0.1", line: 4, pos: 11},
		{typ: itemKey, val: "port", line: 5, pos: 5},
		{typ: itemInteger, val: "4242", line: 5, pos: 11},
		{typ: itemMapEnd, val: "", line: 6, pos: 4},
		{typ: itemMapEnd, val: "", line: 7, pos: 2},
		{typ: itemEOF, val: "", line: 7, pos: 0},
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
		{typ: itemKey, val: "foo", line: 2, pos: 1},
		{typ: itemInteger, val: "123", line: 2, pos: 7},
		{typ: itemKey, val: "bar", line: 3, pos: 1},
		{typ: itemString, val: "baz", line: 3, pos: 8},
		{typ: itemKey, val: "baz", line: 4, pos: 1},
		{typ: itemString, val: "boo", line: 4, pos: 8},
		{typ: itemKey, val: "map", line: 5, pos: 1},
		{typ: itemMapStart, val: "", line: 5, pos: 6},
		{typ: itemKey, val: "id", line: 6, pos: 2},
		{typ: itemInteger, val: "1", line: 6, pos: 7},
		{typ: itemMapEnd, val: "", line: 7, pos: 2},
		{typ: itemEOF, val: "", line: 8, pos: 0},
	}

	lx := lex(semicolons)
	expect(t, lx, expectedItems)
}

func TestSemicolonChaining(t *testing.T) {
	expectedItems := []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemString, val: "1", line: 1, pos: 5},
		{typ: itemKey, val: "bar", line: 1, pos: 9},
		{typ: itemFloat, val: "2.2", line: 1, pos: 13},
		{typ: itemKey, val: "baz", line: 1, pos: 18},
		{typ: itemBool, val: "true", line: 1, pos: 22},
		{typ: itemEOF, val: "", line: 1, pos: 0},
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
		{typ: itemKey, val: "foo", line: 2, pos: 1},
		{typ: itemInteger, val: "123", line: 2, pos: 7},
		{typ: itemKey, val: "bar", line: 3, pos: 1},
		{typ: itemString, val: "baz", line: 3, pos: 7},
		{typ: itemKey, val: "baz", line: 4, pos: 1},
		{typ: itemString, val: "boo", line: 4, pos: 5},
		{typ: itemKey, val: "map", line: 5, pos: 1},
		{typ: itemMapStart, val: "", line: 5, pos: 6},
		{typ: itemKey, val: "id", line: 6, pos: 2},
		{typ: itemString, val: "one", line: 6, pos: 5},
		{typ: itemKey, val: "id2", line: 7, pos: 2},
		{typ: itemString, val: "onetwo", line: 7, pos: 8},
		{typ: itemMapEnd, val: "", line: 8, pos: 2},
		{typ: itemKey, val: "t", line: 9, pos: 1},
		{typ: itemBool, val: "true", line: 9, pos: 3},
		{typ: itemKey, val: "f", line: 10, pos: 1},
		{typ: itemBool, val: "false", line: 10, pos: 3},
		{typ: itemKey, val: "tstr", line: 11, pos: 1},
		{typ: itemString, val: "true", line: 11, pos: 7},
		{typ: itemKey, val: "tkey", line: 12, pos: 1},
		{typ: itemString, val: "two", line: 12, pos: 8},
		{typ: itemKey, val: "fkey", line: 13, pos: 1},
		{typ: itemString, val: "five", line: 13, pos: 8},
		{typ: itemCommentStart, val: "", line: 13, pos: 14},
		{typ: itemText, val: " This should be a string", line: 13, pos: 14},
		{typ: itemEOF, val: "", line: 14, pos: 0},
	}
	lx := lex(noquotes)
	expect(t, lx, expectedItems)
}

var danglingquote = `
listen: "localhost:4242

http: localhost:8222
`

func TestDanglingQuotedString(t *testing.T) {
	expectedItems := []item{
		{typ: itemKey, val: "listen", line: 2, pos: 1},
		{typ: itemError, val: "Unexpected EOF.", line: 5, pos: 1},
	}
	lx := lex(danglingquote)
	expect(t, lx, expectedItems)
}

var keydanglingquote = `
foo = "
listen: "

http: localhost:8222

"
`

func TestKeyDanglingQuotedString(t *testing.T) {
	expectedItems := []item{
		{typ: itemKey, val: "foo", line: 2, pos: 1},
		{typ: itemString, val: "\nlisten: ", line: 3, pos: 8},
		{typ: itemKey, val: "http", line: 5, pos: 1},
		{typ: itemString, val: "localhost:8222", line: 5, pos: 7},
		{typ: itemError, val: "Unexpected EOF.", line: 8, pos: 1},
	}
	lx := lex(keydanglingquote)
	expect(t, lx, expectedItems)
}

var danglingsquote = `
listen: 'localhost:4242

http: localhost:8222
`

func TestDanglingSingleQuotedString(t *testing.T) {
	expectedItems := []item{
		{typ: itemKey, val: "listen", line: 2, pos: 1},
		{typ: itemError, val: "Unexpected EOF.", line: 5, pos: 1},
	}
	lx := lex(danglingsquote)
	expect(t, lx, expectedItems)
}

var keydanglingsquote = `
foo = '
listen: '

http: localhost:8222

'
`

func TestKeyDanglingSingleQuotedString(t *testing.T) {
	expectedItems := []item{
		{typ: itemKey, val: "foo", line: 2, pos: 1},
		{typ: itemString, val: "\nlisten: ", line: 3, pos: 8},
		{typ: itemKey, val: "http", line: 5, pos: 1},
		{typ: itemString, val: "localhost:8222", line: 5, pos: 7},
		{typ: itemError, val: "Unexpected EOF.", line: 8, pos: 1},
	}
	lx := lex(keydanglingsquote)
	expect(t, lx, expectedItems)
}

var mapdanglingbracket = `
listen = 4222

cluster = {

  foo = bar

`

func TestMapDanglingBracket(t *testing.T) {
	expectedItems := []item{
		{typ: itemKey, val: "listen", line: 2, pos: 1},
		{typ: itemInteger, val: "4222", line: 2, pos: 10},
		{typ: itemKey, val: "cluster", line: 4, pos: 1},
		{typ: itemMapStart, val: "", line: 4, pos: 12},
		{typ: itemKey, val: "foo", line: 6, pos: 3},
		{typ: itemString, val: "bar", line: 6, pos: 9},
		{typ: itemError, val: "Unexpected EOF processing map.", line: 8, pos: 1},
	}
	lx := lex(mapdanglingbracket)
	expect(t, lx, expectedItems)
}

var blockdanglingparens = `
listen = 4222

quote = (

  foo = bar

`

func TestBlockDanglingParens(t *testing.T) {
	expectedItems := []item{
		{typ: itemKey, val: "listen", line: 2, pos: 1},
		{typ: itemInteger, val: "4222", line: 2, pos: 10},
		{typ: itemKey, val: "quote", line: 4, pos: 1},
		{typ: itemError, val: "Unexpected EOF processing block.", line: 8, pos: 1},
	}
	lx := lex(blockdanglingparens)
	expect(t, lx, expectedItems)
}

func TestMapQuotedKeys(t *testing.T) {
	expectedItems := []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemMapStart, val: "", line: 1, pos: 7},
		{typ: itemKey, val: "bar", line: 1, pos: 8},
		{typ: itemInteger, val: "4242", line: 1, pos: 15},
		{typ: itemMapEnd, val: "", line: 1, pos: 20},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx := lex("foo = {'bar' = 4242}")
	expect(t, lx, expectedItems)
	lx = lex("foo = {\"bar\" = 4242}")
	expect(t, lx, expectedItems)
}

func TestSpecialCharsMapQuotedKeys(t *testing.T) {
	expectedItems := []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemMapStart, val: "", line: 1, pos: 7},
		{typ: itemKey, val: "bar-1.2.3", line: 1, pos: 8},
		{typ: itemMapStart, val: "", line: 1, pos: 22},
		{typ: itemKey, val: "port", line: 1, pos: 23},
		{typ: itemInteger, val: "4242", line: 1, pos: 28},
		{typ: itemMapEnd, val: "", line: 1, pos: 34},
		{typ: itemMapEnd, val: "", line: 1, pos: 35},
		{typ: itemEOF, val: "", line: 1, pos: 0},
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
		{typ: itemKey, val: "systems", line: 2, pos: 1},
		{typ: itemMapStart, val: "", line: 2, pos: 10},
		{typ: itemKey, val: "allinone", line: 3, pos: 3},
		{typ: itemMapStart, val: "", line: 3, pos: 13},
		{typ: itemKey, val: "description", line: 4, pos: 5},
		{typ: itemString, val: "This is a description.", line: 4, pos: 19},
		{typ: itemMapEnd, val: "", line: 5, pos: 4},
		{typ: itemMapEnd, val: "", line: 6, pos: 2},
		{typ: itemEOF, val: "", line: 7, pos: 0},
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
		{typ: itemKey, val: "numbers", line: 2, pos: 1},
		{typ: itemString, val: "\n1234567890\n", line: 4, pos: 10},
	}
	lx := lex(blockexample)
	expect(t, lx, expectedItems)
}

func TestBlockStringEOF(t *testing.T) {
	expectedItems := []item{
		{typ: itemKey, val: "numbers", line: 2, pos: 1},
		{typ: itemString, val: "\n1234567890\n", line: 4, pos: 10},
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
		{typ: itemKey, val: "numbers", line: 2, pos: 1},
		{typ: itemString, val: "\n  12(34)56\n  (\n    7890\n  )\n", line: 7, pos: 10},
	}
	lx := lex(mlblockexample)
	expect(t, lx, expectedItems)
}

func TestUnquotedIPAddr(t *testing.T) {
	expectedItems := []item{
		{typ: itemKey, val: "listen", line: 1, pos: 0},
		{typ: itemString, val: "127.0.0.1:4222", line: 1, pos: 8},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx := lex("listen: 127.0.0.1:4222")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{typ: itemKey, val: "listen", line: 1, pos: 0},
		{typ: itemString, val: "127.0.0.1", line: 1, pos: 8},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx = lex("listen: 127.0.0.1")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{typ: itemKey, val: "listen", line: 1, pos: 0},
		{typ: itemString, val: "apcera.me:80", line: 1, pos: 8},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx = lex("listen: apcera.me:80")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{typ: itemKey, val: "listen", line: 1, pos: 0},
		{typ: itemString, val: "nats.io:-1", line: 1, pos: 8},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx = lex("listen: nats.io:-1")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{typ: itemKey, val: "listen", line: 1, pos: 0},
		{typ: itemInteger, val: "-1", line: 1, pos: 8},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx = lex("listen: -1")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{typ: itemKey, val: "listen", line: 1, pos: 0},
		{typ: itemString, val: ":-1", line: 1, pos: 8},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx = lex("listen: :-1")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{typ: itemKey, val: "listen", line: 1, pos: 0},
		{typ: itemString, val: ":80", line: 1, pos: 9},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx = lex("listen = :80")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{typ: itemKey, val: "listen", line: 1, pos: 0},
		{typ: itemArrayStart, val: "", line: 1, pos: 10},
		{typ: itemString, val: "localhost:4222", line: 1, pos: 10},
		{typ: itemString, val: "localhost:4333", line: 1, pos: 26},
		{typ: itemArrayEnd, val: "", line: 1, pos: 41},
		{typ: itemEOF, val: "", line: 1, pos: 0},
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
		{typ: itemKey, val: "authorization", line: 2, pos: 1},
		{typ: itemMapStart, val: "", line: 2, pos: 16},
		{typ: itemKey, val: "users", line: 3, pos: 5},
		{typ: itemArrayStart, val: "", line: 3, pos: 14},
		{typ: itemMapStart, val: "", line: 4, pos: 8},
		{typ: itemKey, val: "user", line: 4, pos: 8},
		{typ: itemString, val: "alice", line: 4, pos: 14},
		{typ: itemKey, val: "password", line: 4, pos: 21},
		{typ: itemString, val: "foo", line: 4, pos: 31},
		{typ: itemMapEnd, val: "", line: 4, pos: 35},
		{typ: itemMapStart, val: "", line: 5, pos: 8},
		{typ: itemKey, val: "user", line: 5, pos: 8},
		{typ: itemString, val: "bob", line: 5, pos: 14},
		{typ: itemKey, val: "password", line: 5, pos: 21},
		{typ: itemString, val: "bar", line: 5, pos: 31},
		{typ: itemMapEnd, val: "", line: 5, pos: 35},
		{typ: itemArrayEnd, val: "", line: 6, pos: 6},
		{typ: itemKey, val: "timeout", line: 7, pos: 5},
		{typ: itemFloat, val: "0.5", line: 7, pos: 14},
		{typ: itemMapEnd, val: "", line: 8, pos: 2},
		{typ: itemEOF, val: "", line: 9, pos: 0},
	}
	lx := lex(arrayOfMaps)
	expect(t, lx, expectedItems)
}

func TestInclude(t *testing.T) {
	expectedItems := []item{
		{typ: itemInclude, val: "users.conf", line: 1, pos: 9},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx := lex("include \"users.conf\"")
	expect(t, lx, expectedItems)

	lx = lex("include 'users.conf'")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{typ: itemInclude, val: "users.conf", line: 1, pos: 8},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx = lex("include users.conf")
	expect(t, lx, expectedItems)
}

func TestMapInclude(t *testing.T) {
	expectedItems := []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemMapStart, val: "", line: 1, pos: 5},
		{typ: itemInclude, val: "users.conf", line: 1, pos: 14},
		{typ: itemMapEnd, val: "", line: 1, pos: 26},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}

	lx := lex("foo { include users.conf }")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemMapStart, val: "", line: 1, pos: 5},
		{typ: itemInclude, val: "users.conf", line: 1, pos: 13},
		{typ: itemMapEnd, val: "", line: 1, pos: 24},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx = lex("foo {include users.conf}")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemMapStart, val: "", line: 1, pos: 5},
		{typ: itemInclude, val: "users.conf", line: 1, pos: 15},
		{typ: itemMapEnd, val: "", line: 1, pos: 28},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
	lx = lex("foo { include 'users.conf' }")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{typ: itemKey, val: "foo", line: 1, pos: 0},
		{typ: itemMapStart, val: "", line: 1, pos: 5},
		{typ: itemInclude, val: "users.conf", line: 1, pos: 15},
		{typ: itemMapEnd, val: "", line: 1, pos: 27},
		{typ: itemEOF, val: "", line: 1, pos: 0},
	}
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
				{typ: itemKey, val: "http_port", line: 3, pos: 28},
				{typ: itemInteger, val: "8223", line: 3, pos: 40},
				{typ: itemKey, val: "}", line: 4, pos: 25},
				{typ: itemEOF, val: "", line: 0, pos: 0},
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
				{typ: itemKey, val: "http_port", line: 3, pos: 28},
				{typ: itemInteger, val: "8223", line: 3, pos: 40},
				{typ: itemKey, val: "port", line: 4, pos: 28},
				{typ: itemInteger, val: "4223", line: 4, pos: 35},
				{typ: itemKey, val: "}", line: 5, pos: 25},
				{typ: itemEOF, val: "", line: 0, pos: 0},
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
				{typ: itemKey, val: "http_port", line: 3, pos: 28},
				{typ: itemInteger, val: "8223", line: 3, pos: 40},
				{typ: itemKey, val: "port", line: 4, pos: 28},
				{typ: itemInteger, val: "4223", line: 4, pos: 35},
				{typ: itemKey, val: "max_payload", line: 5, pos: 28},
				{typ: itemString, val: "5MB", line: 5, pos: 43},
				{typ: itemKey, val: "debug", line: 6, pos: 28},
				{typ: itemBool, val: "true", line: 6, pos: 36},
				{typ: itemKey, val: "max_control_line", line: 7, pos: 28},
				{typ: itemInteger, val: "1024", line: 7, pos: 47},
				{typ: itemKey, val: "}", line: 8, pos: 25},
				{typ: itemEOF, val: "", line: 0, pos: 0},
			},
		},
		{
			name: "should support JSON not prettified",
			input: `{"http_port": 8224,"port": 4224}
                        `,
			expected: []item{
				{typ: itemKey, val: "http_port", line: 1, pos: 2},
				{typ: itemInteger, val: "8224", line: 1, pos: 14},
				{typ: itemKey, val: "port", line: 1, pos: 20},
				{typ: itemInteger, val: "4224", line: 1, pos: 27},
				{typ: itemEOF, val: "", line: 0, pos: 0},
			},
		},
		{
			name: "should support JSON not prettified with final bracket after newline",
			input: `{"http_port": 8225,"port": 4225
                        }
                        `,
			expected: []item{
				{typ: itemKey, val: "http_port", line: 1, pos: 2},
				{typ: itemInteger, val: "8225", line: 1, pos: 14},
				{typ: itemKey, val: "port", line: 1, pos: 20},
				{typ: itemInteger, val: "4225", line: 1, pos: 27},
				{typ: itemKey, val: "}", line: 2, pos: 25},
				{typ: itemEOF, val: "", line: 0, pos: 0},
			},
		},
		{
			name: "should support uglified JSON with inner blocks",
			input: `{"http_port": 8227,"port": 4227,"write_deadline": "1h","cluster": {"port": 6222,"routes": ["nats://127.0.0.1:4222","nats://127.0.0.1:4223","nats://127.0.0.1:4224"]}}
                        `,
			expected: []item{
				{typ: itemKey, val: "http_port", line: 1, pos: 2},
				{typ: itemInteger, val: "8227", line: 1, pos: 14},
				{typ: itemKey, val: "port", line: 1, pos: 20},
				{typ: itemInteger, val: "4227", line: 1, pos: 27},
				{typ: itemKey, val: "write_deadline", line: 1, pos: 33},
				{typ: itemString, val: "1h", line: 1, pos: 51},
				{typ: itemKey, val: "cluster", line: 1, pos: 56},
				{typ: itemMapStart, val: "", line: 1, pos: 67},
				{typ: itemKey, val: "port", line: 1, pos: 68},
				{typ: itemInteger, val: "6222", line: 1, pos: 75},
				{typ: itemKey, val: "routes", line: 1, pos: 81},
				{typ: itemArrayStart, val: "", line: 1, pos: 91},
				{typ: itemString, val: "nats://127.0.0.1:4222", line: 1, pos: 92},
				{typ: itemString, val: "nats://127.0.0.1:4223", line: 1, pos: 116},
				{typ: itemString, val: "nats://127.0.0.1:4224", line: 1, pos: 140},
				{typ: itemArrayEnd, val: "", line: 1, pos: 163},
				{typ: itemMapEnd, val: "", line: 1, pos: 164},
				{typ: itemKey, val: "}", line: 14, pos: 25},
				{typ: itemEOF, val: "", line: 0, pos: 0},
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
				{typ: itemKey, val: "http_port", line: 3, pos: 28},
				{typ: itemInteger, val: "8227", line: 3, pos: 40},
				{typ: itemKey, val: "port", line: 4, pos: 28},
				{typ: itemInteger, val: "4227", line: 4, pos: 35},
				{typ: itemKey, val: "write_deadline", line: 5, pos: 28},
				{typ: itemString, val: "1h", line: 5, pos: 46},
				{typ: itemKey, val: "cluster", line: 6, pos: 28},
				{typ: itemMapStart, val: "", line: 6, pos: 39},
				{typ: itemKey, val: "port", line: 7, pos: 30},
				{typ: itemInteger, val: "6222", line: 7, pos: 37},
				{typ: itemKey, val: "routes", line: 8, pos: 30},
				{typ: itemArrayStart, val: "", line: 8, pos: 40},
				{typ: itemString, val: "nats://127.0.0.1:4222", line: 9, pos: 32},
				{typ: itemString, val: "nats://127.0.0.1:4223", line: 10, pos: 32},
				{typ: itemString, val: "nats://127.0.0.1:4224", line: 11, pos: 32},
				{typ: itemArrayEnd, val: "", line: 12, pos: 30},
				{typ: itemMapEnd, val: "", line: 13, pos: 28},
				{typ: itemKey, val: "}", line: 14, pos: 25},
				{typ: itemEOF, val: "", line: 0, pos: 0},
			},
		},
		{
			name: "should support JSON with blocks",
			input: `{
                          "jetstream": {
                            "store_dir": "/tmp/nats"
                            "max_mem": 1000000,
                          },
                          "port": 4222,
                          "server_name": "nats1"
                        }
                        `,
			expected: []item{
				{typ: itemKey, val: "jetstream", line: 2, pos: 28},
				{typ: itemMapStart, val: "", line: 2, pos: 41},
				{typ: itemKey, val: "store_dir", line: 3, pos: 30},
				{typ: itemString, val: "/tmp/nats", line: 3, pos: 43},
				{typ: itemKey, val: "max_mem", line: 4, pos: 30},
				{typ: itemInteger, val: "1000000", line: 4, pos: 40},
				{typ: itemMapEnd, val: "", line: 5, pos: 28},
				{typ: itemKey, val: "port", line: 6, pos: 28},
				{typ: itemInteger, val: "4222", line: 6, pos: 35},
				{typ: itemKey, val: "server_name", line: 7, pos: 28},
				{typ: itemString, val: "nats1", line: 7, pos: 43},
				{typ: itemKey, val: "}", line: 8, pos: 25},
				{typ: itemEOF, val: "", line: 0, pos: 0},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			lx := lex(test.input)
			expect(t, lx, test.expected)
		})
	}
}
