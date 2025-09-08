package metadata

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMeasuredType_String(t *testing.T) {
	tests := []struct {
		name     string
		mt       MeasuredType
		expected string
	}{
		{
			name:     "PoolAllocatedSize string conversion",
			mt:       PoolAllocatedSize,
			expected: "POOL_ALLOCATED_SIZE",
		},
		{
			name:     "AllocatedUsed string conversion",
			mt:       AllocatedUsed,
			expected: "ALLOCATED_USED",
		},
		{
			name:     "FileSystemReadOps string conversion",
			mt:       FileSystemReadOps,
			expected: "FILE_SYSTEM_READ_OPS",
		},
		{
			name:     "UnknownMeasuredType string conversion",
			mt:       UnknownMeasuredType,
			expected: "UNKNOWN_MEASURED_TYPE",
		},
		{
			name:     "Custom MeasuredType string conversion",
			mt:       MeasuredType("CUSTOM_METRIC"),
			expected: "CUSTOM_METRIC",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.mt.String()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestNewMeasuredType(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		expectedType   MeasuredType
		expectedExists bool
	}{
		{
			name:           "Valid pool_allocated_size metric",
			input:          "pool_allocated_size",
			expectedType:   PoolAllocatedSize,
			expectedExists: true,
		},
		{
			name:           "Valid volume_read_ops metric",
			input:          "volume_read_ops",
			expectedType:   FileSystemReadOps,
			expectedExists: true,
		},
		{
			name:           "Valid volume_write_ops metric",
			input:          "volume_write_ops",
			expectedType:   FileSystemWriteOps,
			expectedExists: true,
		},
		{
			name:           "Valid volume_other_ops metric",
			input:          "volume_other_ops",
			expectedType:   FileSystemOtherOps,
			expectedExists: true,
		},
		{
			name:           "Valid volume_read_data metric",
			input:          "volume_read_data",
			expectedType:   ReadIo,
			expectedExists: true,
		},
		{
			name:           "Valid volume_write_data metric",
			input:          "volume_write_data",
			expectedType:   WriteIo,
			expectedExists: true,
		},
		{
			name:           "Valid volume_other_data metric",
			input:          "volume_other_data",
			expectedType:   OtherIo,
			expectedExists: true,
		},
		{
			name:           "Valid volume_read_latency metric",
			input:          "volume_read_latency",
			expectedType:   AverageReadLatency,
			expectedExists: true,
		},
		{
			name:           "Valid volume_write_latency metric",
			input:          "volume_write_latency",
			expectedType:   AverageWriteLatency,
			expectedExists: true,
		},
		{
			name:           "Valid volume_other_latency metric",
			input:          "volume_other_latency",
			expectedType:   AverageOtherLatency,
			expectedExists: true,
		},
		{
			name:           "Valid volume_total_data metric",
			input:          "volume_total_data",
			expectedType:   VolumeAllcatedThroughput,
			expectedExists: true,
		},
		{
			name:           "Valid volume_size_used metric",
			input:          "volume_size_used",
			expectedType:   LogicalSize,
			expectedExists: true,
		},
		{
			name:           "Valid volume_snapshot_reserve_used metric",
			input:          "volume_snapshot_reserve_used",
			expectedType:   SnapshotSize,
			expectedExists: true,
		},
		{
			name:           "Valid volume_size_total metric",
			input:          "volume_size_total",
			expectedType:   AllocatedSize,
			expectedExists: true,
		},
		{
			name:           "Valid volume_inode_files_total metric",
			input:          "volume_inode_files_total",
			expectedType:   VolumeInodeTotal,
			expectedExists: true,
		},
		{
			name:           "Valid volume_inode_files_used metric",
			input:          "volume_inode_files_used",
			expectedType:   VolumeInodesUsed,
			expectedExists: true,
		},
		{
			name:           "Valid unknown_measured_type metric",
			input:          "unknown_measured_type",
			expectedType:   UnknownMeasuredType,
			expectedExists: true,
		},
		{
			name:           "Case insensitive - uppercase input",
			input:          "POOL_ALLOCATED_SIZE",
			expectedType:   PoolAllocatedSize,
			expectedExists: true,
		},
		{
			name:           "Case insensitive - mixed case input",
			input:          "Volume_Read_Ops",
			expectedType:   FileSystemReadOps,
			expectedExists: true,
		},
		{
			name:           "Invalid metric type",
			input:          "invalid_metric",
			expectedType:   MeasuredType(""),
			expectedExists: false,
		},
		{
			name:           "Empty string input",
			input:          "",
			expectedType:   MeasuredType(""),
			expectedExists: false,
		},
		{
			name:           "Non-existent metric type",
			input:          "non_existent_metric_type",
			expectedType:   MeasuredType(""),
			expectedExists: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, exists := NewMeasuredType(tt.input)
			assert.Equal(t, tt.expectedExists, exists)
			if tt.expectedExists {
				assert.Equal(t, tt.expectedType, result)
			} else {
				assert.Equal(t, MeasuredType(""), result)
			}
		})
	}
}

func TestCombinedKeyResourceTypeMeasuredTypeMap_Initialization(t *testing.T) {
	// Test that the map is properly initialized
	assert.NotNil(t, CombinedKeyResourceTypeMeasuredTypeMap)
	assert.NotEmpty(t, CombinedKeyResourceTypeMeasuredTypeMap)

	// Test specific mappings
	tests := []struct {
		key                  string
		expectedMeasuredType MeasuredType
		expectedResourceType ResourceType
	}{
		{
			key:                  "pool_allocated_size",
			expectedMeasuredType: PoolAllocatedSize,
			expectedResourceType: VolumePool,
		},
		{
			key:                  "volume_read_ops",
			expectedMeasuredType: FileSystemReadOps,
			expectedResourceType: Volume,
		},
		{
			key:                  "volume_write_ops",
			expectedMeasuredType: FileSystemWriteOps,
			expectedResourceType: Volume,
		},
		{
			key:                  "volume_size_used",
			expectedMeasuredType: LogicalSize,
			expectedResourceType: Volume,
		},
		{
			key:                  "unknown_measured_type",
			expectedMeasuredType: UnknownMeasuredType,
			expectedResourceType: ResourceType(""), // Empty resource type for unknown
		},
	}

	for _, tt := range tests {
		t.Run("mapping_"+tt.key, func(t *testing.T) {
			mapping, exists := CombinedKeyResourceTypeMeasuredTypeMap[tt.key]
			assert.True(t, exists, "Key %s should exist in the map", tt.key)
			assert.Equal(t, tt.expectedMeasuredType, mapping.MeasuredType)
			assert.Equal(t, tt.expectedResourceType, mapping.ResourceType)
		})
	}
}

func TestCombinedKeyResourceTypeMeasuredType_Structure(t *testing.T) {
	// Test the structure itself
	combined := CombinedKeyResourceTypeMeasuredType{
		ResourceType: VolumePool,
		MeasuredType: PoolAllocatedSize,
	}

	assert.Equal(t, VolumePool, combined.ResourceType)
	assert.Equal(t, PoolAllocatedSize, combined.MeasuredType)
}

func TestNewMeasuredType_EdgeCases(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		expectedExists bool
	}{
		{
			name:           "Input with spaces",
			input:          " pool_allocated_size ",
			expectedExists: false, // Spaces should make it not match
		},
		{
			name:           "Input with special characters",
			input:          "pool_allocated_size!",
			expectedExists: false,
		},
		{
			name:           "Numeric input",
			input:          "123",
			expectedExists: false,
		},
		{
			name:           "Input with underscores variations",
			input:          "pool__allocated__size",
			expectedExists: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, exists := NewMeasuredType(tt.input)
			assert.Equal(t, tt.expectedExists, exists)
		})
	}
}

func TestMeasuredType_Constants(t *testing.T) {
	// Test that all constants are properly defined
	constants := []struct {
		name     string
		constant MeasuredType
		expected string
	}{
		{"UnknownMeasuredType", UnknownMeasuredType, "UNKNOWN_MEASURED_TYPE"},
		{"PoolAllocatedSize", PoolAllocatedSize, "POOL_ALLOCATED_SIZE"},
		{"AllocatedUsed", AllocatedUsed, "ALLOCATED_USED"},
		{"FileSystemReadOps", FileSystemReadOps, "FILE_SYSTEM_READ_OPS"},
		{"FileSystemWriteOps", FileSystemWriteOps, "FILE_SYSTEM_WRITE_OPS"},
		{"FileSystemOtherOps", FileSystemOtherOps, "FILE_SYSTEM_OTHER_OPS"},
		{"ReadIo", ReadIo, "READ_IO"},
		{"WriteIo", WriteIo, "WRITE_IO"},
		{"OtherIo", OtherIo, "OTHER_IO"},
		{"AverageReadLatency", AverageReadLatency, "AVERAGE_READ_LATENCY"},
		{"AverageWriteLatency", AverageWriteLatency, "AVERAGE_WRITE_LATENCY"},
		{"AverageOtherLatency", AverageOtherLatency, "AVERAGE_OTHER_LATENCY"},
		{"VolumeAllcatedThroughput", VolumeAllcatedThroughput, "VOLUME_ALLOCATED_THROUGHPUT"},
		{"LogicalSize", LogicalSize, "LOGICAL_SIZE"},
		{"SnapshotSize", SnapshotSize, "SNAPSHOT_SIZE"},
		{"AllocatedSize", AllocatedSize, "ALLOCATED_SIZE"},
		{"VolumeInodeTotal", VolumeInodeTotal, "VOLUME_INODE_TOTAL"},
		{"VolumeInodesUsed", VolumeInodesUsed, "VOLUME_INODES_USED"},
		{"TotalLogicalSize", TotalLogicalSize, "TOTAL_LOGICAL_SIZE"},
	}

	for _, c := range constants {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.expected, string(c.constant))
		})
	}
}
