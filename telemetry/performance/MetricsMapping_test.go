package performance

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_ReturnsCorrectMappingForKnownMetric(t *testing.T) {
	metricsMapping := GetMetricsMapping()
	triple, exists := metricsMapping[VolumeSpaceLogicalUsed]

	assert.True(t, exists)
	assert.Equal(t, "bytes_used", triple.Left)
	assert.Equal(t, "", triple.Middle)
	assert.Equal(t, "", triple.Right)
}

func Test_ReturnsUnknownStringForInvalidMetric(t *testing.T) {
	var invalidMetric VSAHarvestMetric = 999
	result := invalidMetric.String()

	assert.Equal(t, "unknown", result)
}

func Test_ReturnsCorrectStringForKnownMetric(t *testing.T) {
	result := VolumeSpaceLogicalUsed.String()

	assert.Equal(t, "volume_space_logical_used", result)
}
