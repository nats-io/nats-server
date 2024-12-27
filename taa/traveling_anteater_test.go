package taa

import (
	"testing"

	"github.com/antithesishq/antithesis-sdk-go/assert"
)

func TestFoo(t *testing.T) {
	Foo()
	assert.Unreachable("Sanity check", nil)
}
