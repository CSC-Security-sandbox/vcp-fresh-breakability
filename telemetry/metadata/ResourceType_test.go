package metadata

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_ConvertsResourceTypeToStringSuccessfully(t *testing.T) {
	rt := Volume
	assert.Equal(t, "VOLUME", rt.String())
}
