package decision

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vmrs"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vmrs/config"
)

func TestNewLeastCostLargeVolumeClusterDecisionMaker(t *testing.T) {
	cases := []struct {
		name             string
		configFilename   string
		customerRequest  vmrs.CustomerRequestedPerformance
		expectedError    string
		expectedDecision *vmrs.Decision
	}{
		{
			name:           "MinimumValidLargeVolumeRequest",
			configFilename: "testdata/valid_large_volume.yaml",
			customerRequest: vmrs.CustomerRequestedPerformance{
				DesiredIOPS:             1600,  // Min IOPS
				DesiredThroughputInMiBs: 64,    // Min throughput
				DesiredCapacityInGiB:    12288, // 12 TiB minimum
			},
			expectedError: "",
			expectedDecision: &vmrs.Decision{
				ChosenVMs: []string{"c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd"},
				ClusterMetadata: &vmrs.ClusterMetadata{
					NumHAPairs:    2,
					NumNodes:      4,
					NumLIFs:       2, // Active-passive mode: only active nodes have LIFs
					IsHomogeneous: true,
					VMType:        "c3-standard-4-lssd", // With 2 HA pairs and 1.8 scaling factor
				},
			},
		},
		{
			name:           "MediumLargeVolumeRequest",
			configFilename: "testdata/valid_large_volume.yaml",
			customerRequest: vmrs.CustomerRequestedPerformance{
				DesiredIOPS:             40000, // Medium IOPS
				DesiredThroughputInMiBs: 1000,  // Medium throughput
				DesiredCapacityInGiB:    51200, // 50 TiB
			},
			expectedError: "",
			expectedDecision: &vmrs.Decision{
				ChosenVMs: []string{"c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd"},
				ClusterMetadata: &vmrs.ClusterMetadata{
					NumHAPairs:    2,
					NumNodes:      4,
					NumLIFs:       2, // Active-passive mode: only active nodes have LIFs
					IsHomogeneous: true,
					VMType:        "c3-standard-44-lssd", // With 2 HA pairs, needs higher capacity VM
				},
			},
		},
		{
			name:           "HighIOPSLargeVolumeRequest",
			configFilename: "testdata/valid_large_volume.yaml",
			customerRequest: vmrs.CustomerRequestedPerformance{
				DesiredIOPS:             160000, // Max IOPS
				DesiredThroughputInMiBs: 5000,   // High throughput
				DesiredCapacityInGiB:    204800, // 200 TiB
			},
			expectedError: "",
			expectedDecision: &vmrs.Decision{
				ChosenVMs: []string{"c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd"},
				ClusterMetadata: &vmrs.ClusterMetadata{
					NumHAPairs:    2,
					NumNodes:      4,
					NumLIFs:       2, // Active-passive mode: only active nodes have LIFs
					IsHomogeneous: true,
					VMType:        "c3-standard-88-lssd", // High IOPS requirement needs largest VM
				},
			},
		},
		{
			name:           "MaxThroughputLargeVolumeRequest",
			configFilename: "testdata/valid_large_volume.yaml",
			customerRequest: vmrs.CustomerRequestedPerformance{
				DesiredIOPS:             80000,  // Medium IOPS
				DesiredThroughputInMiBs: 61440,  // Max throughput (60 GiB/s)
				DesiredCapacityInGiB:    102400, // 100 TiB
			},
			expectedError:    "no suitable VM type found for cluster requirements",
			expectedDecision: nil, // No decision when error occurs
		},
		{
			name:           "MaxCapacityLargeVolumeRequest",
			configFilename: "testdata/valid_large_volume.yaml",
			customerRequest: vmrs.CustomerRequestedPerformance{
				DesiredIOPS:             50000,   // Medium IOPS
				DesiredThroughputInMiBs: 2000,    // Medium throughput
				DesiredCapacityInGiB:    2621440, // 20 PiB (max capacity)
			},
			expectedError:    "no suitable VM type found for cluster requirements",
			expectedDecision: nil,
		},
		{
			name:           "NoSuitableVMFound_ExceedsAllVMCapabilities",
			configFilename: "testdata/valid_large_volume.yaml",
			customerRequest: vmrs.CustomerRequestedPerformance{
				DesiredIOPS:             1000000, // 1M IOPS - exceeds all VMs even with scaling
				DesiredThroughputInMiBs: 10000,   // 10 GiB/s
				DesiredCapacityInGiB:    12582912,
			},
			expectedError:    "no suitable VM type found for cluster requirements",
			expectedDecision: nil,
		},
		{
			name:           "ValidRequestWithNoNonLinearScaling",
			configFilename: "testdata/valid_large_volume_no_scaling.yaml",
			customerRequest: vmrs.CustomerRequestedPerformance{
				DesiredIOPS:             25000,
				DesiredThroughputInMiBs: 500,
				DesiredCapacityInGiB:    12288,
			},
			expectedError:    "no non-linear scaling configuration found for active-passive mode",
			expectedDecision: nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			config, err := config.LoadConfig(tc.configFilename)
			assert.Nil(t, err, "failed to load config")

			dm := NewLeastCostLargeVolumeClusterDecisionMaker(config)
			decision, err := dm.FindOptimalVMs(config, tc.customerRequest, nil)

			if tc.expectedError != "" {
				assert.NotNil(t, err, "expected an error but got nil")
				assert.Contains(t, err.Error(), tc.expectedError, "error message should contain expected text")
				assert.Nil(t, decision, "expected no decision when error occurs")
			} else {
				assert.Nil(t, err, "expected no error but got: %v", err)
				assert.NotNil(t, decision, "expected a decision but got nil")

				if decision == nil {
					return // Don't continue if decision is nil to avoid panic
				}

				// Verify basic decision structure
				assert.NotEmpty(t, decision.ChosenVMs, "expected chosen VMs")
				assert.NotNil(t, decision.ClusterMetadata, "expected cluster metadata for large volume decision")

				// Verify we got some VMs (exact VMs may vary based on scaling logic)
				assert.GreaterOrEqual(t, len(decision.ChosenVMs), 2, "expected at least 2 VMs for large volume cluster")

				// Verify cluster metadata has reasonable values
				assert.GreaterOrEqual(t, decision.ClusterMetadata.NumHAPairs, 1, "expected at least 1 HA pair")
				assert.GreaterOrEqual(t, decision.ClusterMetadata.NumNodes, 2, "expected at least 2 nodes")
				assert.GreaterOrEqual(t, decision.ClusterMetadata.NumLIFs, 1, "expected at least 1 LIF")
				assert.True(t, decision.ClusterMetadata.IsHomogeneous, "expected homogeneous cluster")
				assert.NotEmpty(t, decision.ClusterMetadata.VMType, "expected VM type to be set")

				// If we have a specific expected decision, verify it matches
				if tc.expectedDecision != nil {
					assert.Equal(t, tc.expectedDecision.ChosenVMs, decision.ChosenVMs, "chosen VMs should match")
					assert.Equal(t, tc.expectedDecision.ClusterMetadata.NumHAPairs, decision.ClusterMetadata.NumHAPairs, "HA pairs should match")
					assert.Equal(t, tc.expectedDecision.ClusterMetadata.NumNodes, decision.ClusterMetadata.NumNodes, "number of nodes should match")
					assert.Equal(t, tc.expectedDecision.ClusterMetadata.NumLIFs, decision.ClusterMetadata.NumLIFs, "number of LIFs should match")
					assert.Equal(t, tc.expectedDecision.ClusterMetadata.IsHomogeneous, decision.ClusterMetadata.IsHomogeneous, "homogeneous flag should match")
					assert.Equal(t, tc.expectedDecision.ClusterMetadata.VMType, decision.ClusterMetadata.VMType, "VM type should match")
				}
			}
		})
	}
}

func TestLargeVolumeClusterCompareVMScalingDirection(t *testing.T) {
	config, err := config.LoadConfig("testdata/valid_large_volume.yaml")
	assert.Nil(t, err, "failed to load config")

	dm := NewLeastCostLargeVolumeClusterDecisionMaker(config)

	cases := []struct {
		name                string
		currentInstanceType string
		newInstanceType     string
		expectedIsScalingUp bool
		expectedError       string
	}{
		{
			name:                "ScalingUpFromSmallToMedium",
			currentInstanceType: "c3-standard-4-lssd",
			newInstanceType:     "c3-standard-8-lssd",
			expectedIsScalingUp: true,
			expectedError:       "",
		},
		{
			name:                "ScalingUpFromMediumToLarge",
			currentInstanceType: "c3-standard-8-lssd",
			newInstanceType:     "c3-standard-22-lssd",
			expectedIsScalingUp: true,
			expectedError:       "",
		},
		{
			name:                "ScalingUpToMaxVM",
			currentInstanceType: "c3-standard-44-lssd",
			newInstanceType:     "c3-standard-88-lssd",
			expectedIsScalingUp: true,
			expectedError:       "",
		},
		{
			name:                "ScalingDownFromLargeToMedium",
			currentInstanceType: "c3-standard-22-lssd",
			newInstanceType:     "c3-standard-8-lssd",
			expectedIsScalingUp: false,
			expectedError:       "",
		},
		{
			name:                "SameInstanceType",
			currentInstanceType: "c3-standard-8-lssd",
			newInstanceType:     "c3-standard-8-lssd",
			expectedIsScalingUp: false,
			expectedError:       "",
		},
		{
			name:                "InvalidCurrentInstanceType",
			currentInstanceType: "invalid-instance",
			newInstanceType:     "c3-standard-8-lssd",
			expectedIsScalingUp: false,
			expectedError:       "current VM type not found",
		},
		{
			name:                "InvalidNewInstanceType",
			currentInstanceType: "c3-standard-8-lssd",
			newInstanceType:     "invalid-instance",
			expectedIsScalingUp: false,
			expectedError:       "new VM type not found",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isScalingUp, err := dm.CompareVMScalingDirection(tc.currentInstanceType, tc.newInstanceType)

			if tc.expectedError != "" {
				assert.NotNil(t, err, "expected an error but got nil")
				assert.Contains(t, err.Error(), tc.expectedError, "error message should contain expected text")
			} else {
				assert.Nil(t, err, "expected no error but got one: %v", err)
				assert.Equal(t, tc.expectedIsScalingUp, isScalingUp, "scaling direction mismatch")
			}
		})
	}
}

func TestLargeVolumeClusterApplyNonLinearScaling(t *testing.T) {
	config, err := config.LoadConfig("testdata/valid_large_volume.yaml")
	assert.Nil(t, err, "failed to load config")

	dm := NewLeastCostLargeVolumeClusterDecisionMaker(config)

	cases := []struct {
		name               string
		haPairs            int
		originalIOPS       int64
		originalThroughput int64
		expectedIOPS       int64
		expectedThroughput int64
	}{
		{
			name:               "SingleHAPair_NoScaling",
			haPairs:            1,
			originalIOPS:       10000,
			originalThroughput: 100,
			expectedIOPS:       10000, // 1.0x scaling
			expectedThroughput: 100,   // 1.0x scaling
		},
		{
			name:               "SixHAPairs_DefinedScaling",
			haPairs:            6,
			originalIOPS:       10000,
			originalThroughput: 100,
			expectedIOPS:       2500, // ceil(10000 / 4.0) = 2500
			expectedThroughput: 23,   // ceil(100 / 4.5) = 23
		},
		{
			name:               "TwelveHAPairs_MaxScaling",
			haPairs:            12,
			originalIOPS:       10000,
			originalThroughput: 100,
			expectedIOPS:       2041, // ceil(10000 / 4.9) = 2041
			expectedThroughput: 18,   // ceil(100 / 5.8) = 18
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			scaledIOPS, scaledThroughput, err := dm.applyNonLinearScaling(tc.originalIOPS, tc.originalThroughput, tc.haPairs)
			assert.NoError(t, err, "applyNonLinearScaling should not return error")

			assert.Equal(t, tc.expectedIOPS, scaledIOPS, "IOPS scaling mismatch")
			assert.Equal(t, tc.expectedThroughput, scaledThroughput, "throughput scaling mismatch")
		})
	}
}

func TestLargeVolumeClusterApplyNonLinearScaling_ErrorCases(t *testing.T) {
	config, err := config.LoadConfig("testdata/valid_large_volume.yaml")
	assert.Nil(t, err, "failed to load config")

	dm := NewLeastCostLargeVolumeClusterDecisionMaker(config)

	// Test cases without exact scaling factor matches (should fail)
	errorCases := []struct {
		name               string
		haPairs            int
		originalIOPS       int64
		originalThroughput int64
	}{
		{
			name:               "ThreeHAPairs_NoExactMatch",
			haPairs:            3,
			originalIOPS:       10000,
			originalThroughput: 100,
		},
		{
			name:               "NineHAPairs_NoExactMatch",
			haPairs:            9,
			originalIOPS:       10000,
			originalThroughput: 100,
		},
		{
			name:               "FifteenHAPairs_NoExactMatch",
			haPairs:            15,
			originalIOPS:       10000,
			originalThroughput: 100,
		},
	}

	for _, tc := range errorCases {
		t.Run(tc.name, func(t *testing.T) {
			scaledIOPS, scaledThroughput, err := dm.applyNonLinearScaling(tc.originalIOPS, tc.originalThroughput, tc.haPairs)
			assert.Error(t, err, "applyNonLinearScaling should return error for unconfigured HA pair count")
			assert.Contains(t, err.Error(), fmt.Sprintf("no scaling factor configured for %d HA pairs", tc.haPairs))
			assert.Equal(t, int64(0), scaledIOPS, "IOPS should be 0 when error occurs")
			assert.Equal(t, int64(0), scaledThroughput, "throughput should be 0 when error occurs")
		})
	}
}

func TestLargeVolumeClusterApplyNonLinearScaling_NoScalingConfig(t *testing.T) {
	config, err := config.LoadConfig("testdata/valid_large_volume_no_scaling.yaml")
	assert.Nil(t, err, "failed to load config")

	dm := NewLeastCostLargeVolumeClusterDecisionMaker(config)

	// When no scaling config is present, should fall back to linear scaling
	originalIOPS := int64(10000)
	originalThroughput := int64(100)
	haPairs := 6

	scaledIOPS, scaledThroughput, err := dm.applyNonLinearScaling(originalIOPS, originalThroughput, haPairs)

	// Should return error since no scaling configuration is provided
	assert.Error(t, err, "applyNonLinearScaling should return error when no scaling config")
	assert.Contains(t, err.Error(), "no non-linear scaling configuration found")
	assert.Equal(t, int64(0), scaledIOPS, "IOPS should be 0 when error occurs")
	assert.Equal(t, int64(0), scaledThroughput, "throughput should be 0 when error occurs")
}
