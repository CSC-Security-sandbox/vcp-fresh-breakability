package metrics

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRegistry_IsNotNil(t *testing.T) {
	assert.NotNil(t, Registry, "shared Prometheus registry should be initialised")
}

func TestRegion_ReturnsStringValue(t *testing.T) {
	got := Region()
	assert.IsType(t, "", got)
	assert.Equal(t, region, got)
}
