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
				ChosenVMs: []string{"c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd"},
				ClusterMetadata: &vmrs.ClusterMetadata{
					NumHAPairs:    6,
					NumNodes:      12,
					NumLIFs:       6, // Active-passive mode: only active nodes have LIFs
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
				ChosenVMs: []string{"c3-standard-22-lssd", "c3-standard-22-lssd", "c3-standard-22-lssd", "c3-standard-22-lssd", "c3-standard-22-lssd", "c3-standard-22-lssd", "c3-standard-22-lssd", "c3-standard-22-lssd", "c3-standard-22-lssd", "c3-standard-22-lssd", "c3-standard-22-lssd", "c3-standard-22-lssd"},
				ClusterMetadata: &vmrs.ClusterMetadata{
					NumHAPairs:    6,
					NumNodes:      12,
					NumLIFs:       6, // Active-passive mode: only active nodes have LIFs
					IsHomogeneous: true,
					VMType:        "c3-standard-22-lssd", // With 2 HA pairs, needs higher capacity VM
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
				ChosenVMs: []string{"c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd"},
				ClusterMetadata: &vmrs.ClusterMetadata{
					NumHAPairs:    6,
					NumNodes:      12,
					NumLIFs:       6, // Active-passive mode: only active nodes have LIFs
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
				DesiredIOPS:             500000,  // Medium IOPS
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

func TestNewLeastCostLargeVolumeClusterBoundaryDecisionMaker(t *testing.T) {
	cases := []struct {
		name             string
		configFilename   string
		customerRequest  vmrs.CustomerRequestedPerformance
		expectedError    string
		expectedDecision *vmrs.Decision
	}{
		{
			name:           "C3Standard4LSSD_MinIOPS_BoundaryTest",
			configFilename: "testdata/valid_large_capacity3.yaml",
			customerRequest: vmrs.CustomerRequestedPerformance{
				DesiredIOPS:             1600,  // Minimum IOPS for large volume
				DesiredThroughputInMiBs: 100,   // Low throughput
				DesiredCapacityInGiB:    12288, // Minimum capacity
			},
			expectedError: "",
			expectedDecision: &vmrs.Decision{
				ChosenVMs: []string{"c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd"},
				ClusterMetadata: &vmrs.ClusterMetadata{
					NumHAPairs:    6,
					NumNodes:      12,
					NumLIFs:       6,
					IsHomogeneous: true,
					VMType:        "c3-standard-4-lssd",
				},
			},
		},
		{
			name:           "C3Standard4LSSD_MaxIOPS_BoundaryTest",
			configFilename: "testdata/valid_large_capacity3.yaml",
			customerRequest: vmrs.CustomerRequestedPerformance{
				DesiredIOPS:             33840, // Near max IOPS (33,840) for c3-standard-4-lssd
				DesiredThroughputInMiBs: 100,   // Low throughput
				DesiredCapacityInGiB:    50000, // Moderate capacity
			},
			expectedError: "",
			expectedDecision: &vmrs.Decision{
				ChosenVMs: []string{"c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd"},
				ClusterMetadata: &vmrs.ClusterMetadata{
					NumHAPairs:    6,
					NumNodes:      12,
					NumLIFs:       6,
					IsHomogeneous: true,
					VMType:        "c3-standard-4-lssd",
				},
			},
		},
		{
			name:           "C3Standard4LSSD_MinThroughput_BoundaryTest",
			configFilename: "testdata/valid_large_capacity3.yaml",
			customerRequest: vmrs.CustomerRequestedPerformance{
				DesiredIOPS:             5000,  // Low IOPS
				DesiredThroughputInMiBs: 64,    // Minimum throughput for large volume
				DesiredCapacityInGiB:    12288, // Minimum capacity
			},
			expectedError: "",
			expectedDecision: &vmrs.Decision{
				ChosenVMs: []string{"c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd"},
				ClusterMetadata: &vmrs.ClusterMetadata{
					NumHAPairs:    6,
					NumNodes:      12,
					NumLIFs:       6,
					IsHomogeneous: true,
					VMType:        "c3-standard-4-lssd",
				},
			},
		},
		{
			name:           "C3Standard4LSSD_MaxThroughput_BoundaryTest",
			configFilename: "testdata/valid_large_capacity3.yaml",
			customerRequest: vmrs.CustomerRequestedPerformance{
				DesiredIOPS:             5000,  // Low IOPS
				DesiredThroughputInMiBs: 337,   // Near max throughput (337.5) for c3-standard-4-lssd
				DesiredCapacityInGiB:    50000, // Moderate capacity
			},
			expectedError: "",
			expectedDecision: &vmrs.Decision{
				ChosenVMs: []string{"c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd"},
				ClusterMetadata: &vmrs.ClusterMetadata{
					NumHAPairs:    6,
					NumNodes:      12,
					NumLIFs:       6,
					IsHomogeneous: true,
					VMType:        "c3-standard-4-lssd",
				},
			},
		},
		{
			name:           "C3Standard4LSSD_MinCapacity_BoundaryTest",
			configFilename: "testdata/valid_large_capacity3.yaml",
			customerRequest: vmrs.CustomerRequestedPerformance{
				DesiredIOPS:             5000,  // Low IOPS
				DesiredThroughputInMiBs: 100,   // Low throughput
				DesiredCapacityInGiB:    12288, // Minimum capacity (12 TiB)
			},
			expectedError: "",
			expectedDecision: &vmrs.Decision{
				ChosenVMs: []string{"c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd"},
				ClusterMetadata: &vmrs.ClusterMetadata{
					NumHAPairs:    6,
					NumNodes:      12,
					NumLIFs:       6,
					IsHomogeneous: true,
					VMType:        "c3-standard-4-lssd",
				},
			},
		},
		{
			name:           "C3Standard4LSSD_MaxCapacity_BoundaryTest",
			configFilename: "testdata/valid_large_capacity3.yaml",
			customerRequest: vmrs.CustomerRequestedPerformance{
				DesiredIOPS:             5000,    // Low IOPS
				DesiredThroughputInMiBs: 100,     // Low throughput
				DesiredCapacityInGiB:    1278000, // Near max capacity (2,556,000) for c3-standard-4-lssd
			},
			expectedError: "",
			expectedDecision: &vmrs.Decision{
				ChosenVMs: []string{"c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd", "c3-standard-4-lssd"},
				ClusterMetadata: &vmrs.ClusterMetadata{
					NumHAPairs:    6,
					NumNodes:      12,
					NumLIFs:       6,
					IsHomogeneous: true,
					VMType:        "c3-standard-4-lssd",
				},
			},
		},
		// c3-standard-8-lssd boundary tests
		{
			name:           "C3Standard8LSSD_MinIOPS_BoundaryTest",
			configFilename: "testdata/valid_large_capacity3.yaml",
			customerRequest: vmrs.CustomerRequestedPerformance{
				DesiredIOPS:             33841, // Minimum IOPS
				DesiredThroughputInMiBs: 337,   // Moderate throughput
				DesiredCapacityInGiB:    50000, // Moderate capacity
			},
			expectedError: "",
			expectedDecision: &vmrs.Decision{
				ChosenVMs: []string{"c3-standard-8-lssd", "c3-standard-8-lssd", "c3-standard-8-lssd", "c3-standard-8-lssd", "c3-standard-8-lssd", "c3-standard-8-lssd", "c3-standard-8-lssd", "c3-standard-8-lssd", "c3-standard-8-lssd", "c3-standard-8-lssd", "c3-standard-8-lssd", "c3-standard-8-lssd"},
				ClusterMetadata: &vmrs.ClusterMetadata{
					NumHAPairs:    6,
					NumNodes:      12,
					NumLIFs:       6,
					IsHomogeneous: true,
					VMType:        "c3-standard-8-lssd",
				},
			},
		},
		{
			name:           "C3Standard8LSSD_MaxIOPS_BoundaryTest",
			configFilename: "testdata/valid_large_capacity3.yaml",
			customerRequest: vmrs.CustomerRequestedPerformance{
				DesiredIOPS:             70980,  // max IOPS (70,980) for c3-standard-8-lssd
				DesiredThroughputInMiBs: 500,    // Moderate throughput
				DesiredCapacityInGiB:    100000, // Moderate capacity
			},
			expectedError: "",
			expectedDecision: &vmrs.Decision{
				ChosenVMs: []string{"c3-standard-8-lssd", "c3-standard-8-lssd", "c3-standard-8-lssd", "c3-standard-8-lssd", "c3-standard-8-lssd", "c3-standard-8-lssd", "c3-standard-8-lssd", "c3-standard-8-lssd", "c3-standard-8-lssd", "c3-standard-8-lssd", "c3-standard-8-lssd", "c3-standard-8-lssd"},
				ClusterMetadata: &vmrs.ClusterMetadata{
					NumHAPairs:    6,
					NumNodes:      12,
					NumLIFs:       6,
					IsHomogeneous: true,
					VMType:        "c3-standard-8-lssd",
				},
			},
		},
		{
			name:           "C3Standard8LSSD_MinThroughput_BoundaryTest",
			configFilename: "testdata/valid_large_capacity3.yaml",
			customerRequest: vmrs.CustomerRequestedPerformance{
				DesiredIOPS:             10000, // Moderate IOPS
				DesiredThroughputInMiBs: 338,   // Minimum throughput
				DesiredCapacityInGiB:    50000, // Moderate capacity
			},
			expectedError: "",
			expectedDecision: &vmrs.Decision{
				ChosenVMs: []string{"c3-standard-8-lssd", "c3-standard-8-lssd", "c3-standard-8-lssd", "c3-standard-8-lssd", "c3-standard-8-lssd", "c3-standard-8-lssd", "c3-standard-8-lssd", "c3-standard-8-lssd", "c3-standard-8-lssd", "c3-standard-8-lssd", "c3-standard-8-lssd", "c3-standard-8-lssd"},
				ClusterMetadata: &vmrs.ClusterMetadata{
					NumHAPairs:    6,
					NumNodes:      12,
					NumLIFs:       6,
					IsHomogeneous: true,
					VMType:        "c3-standard-8-lssd",
				},
			},
		},
		{
			name:           "C3Standard8LSSD_MaxThroughput_BoundaryTest",
			configFilename: "testdata/valid_large_capacity3.yaml",
			customerRequest: vmrs.CustomerRequestedPerformance{
				DesiredIOPS:             10000,  // Moderate IOPS
				DesiredThroughputInMiBs: 1561,   // Near max throughput (1,561.5) for c3-standard-8-lssd
				DesiredCapacityInGiB:    100000, // Moderate capacity
			},
			expectedError: "",
			expectedDecision: &vmrs.Decision{
				ChosenVMs: []string{"c3-standard-8-lssd", "c3-standard-8-lssd", "c3-standard-8-lssd", "c3-standard-8-lssd", "c3-standard-8-lssd", "c3-standard-8-lssd", "c3-standard-8-lssd", "c3-standard-8-lssd", "c3-standard-8-lssd", "c3-standard-8-lssd", "c3-standard-8-lssd", "c3-standard-8-lssd"},
				ClusterMetadata: &vmrs.ClusterMetadata{
					NumHAPairs:    6,
					NumNodes:      12,
					NumLIFs:       6,
					IsHomogeneous: true,
					VMType:        "c3-standard-8-lssd",
				},
			},
		},
		// c3-standard-22-lssd boundary tests
		{
			name:           "C3Standard22LSSD_MinIOPS_BoundaryTest",
			configFilename: "testdata/valid_large_capacity3.yaml",
			customerRequest: vmrs.CustomerRequestedPerformance{
				DesiredIOPS:             70981,  // Minimum IOPS
				DesiredThroughputInMiBs: 1000,   // Moderate throughput
				DesiredCapacityInGiB:    100000, // Moderate capacity
			},
			expectedError: "",
			expectedDecision: &vmrs.Decision{
				ChosenVMs: []string{"c3-standard-22-lssd", "c3-standard-22-lssd", "c3-standard-22-lssd", "c3-standard-22-lssd", "c3-standard-22-lssd", "c3-standard-22-lssd", "c3-standard-22-lssd", "c3-standard-22-lssd", "c3-standard-22-lssd", "c3-standard-22-lssd", "c3-standard-22-lssd", "c3-standard-22-lssd"},
				ClusterMetadata: &vmrs.ClusterMetadata{
					NumHAPairs:    6,
					NumNodes:      12,
					NumLIFs:       6,
					IsHomogeneous: true,
					VMType:        "c3-standard-22-lssd",
				},
			},
		},
		{
			name:           "C3Standard22LSSD_MaxIOPS_BoundaryTest",
			configFilename: "testdata/valid_large_capacity3.yaml",
			customerRequest: vmrs.CustomerRequestedPerformance{
				DesiredIOPS:             125870, // Near max IOPS (125,872) for c3-standard-22-lssd
				DesiredThroughputInMiBs: 1000,   // Moderate throughput
				DesiredCapacityInGiB:    200000, // Moderate capacity
			},
			expectedError: "",
			expectedDecision: &vmrs.Decision{
				ChosenVMs: []string{"c3-standard-22-lssd", "c3-standard-22-lssd", "c3-standard-22-lssd", "c3-standard-22-lssd", "c3-standard-22-lssd", "c3-standard-22-lssd", "c3-standard-22-lssd", "c3-standard-22-lssd", "c3-standard-22-lssd", "c3-standard-22-lssd", "c3-standard-22-lssd", "c3-standard-22-lssd"},
				ClusterMetadata: &vmrs.ClusterMetadata{
					NumHAPairs:    6,
					NumNodes:      12,
					NumLIFs:       6,
					IsHomogeneous: true,
					VMType:        "c3-standard-22-lssd",
				},
			},
		},
		{
			name:           "C3Standard22LSSD_MinThroughput_BoundaryTest",
			configFilename: "testdata/valid_large_capacity3.yaml",
			customerRequest: vmrs.CustomerRequestedPerformance{
				DesiredIOPS:             20000,  // Moderate IOPS
				DesiredThroughputInMiBs: 1562,   // Minimum throughput
				DesiredCapacityInGiB:    100000, // Moderate capacity
			},
			expectedError: "",
			expectedDecision: &vmrs.Decision{
				ChosenVMs: []string{"c3-standard-22-lssd", "c3-standard-22-lssd", "c3-standard-22-lssd", "c3-standard-22-lssd", "c3-standard-22-lssd", "c3-standard-22-lssd", "c3-standard-22-lssd", "c3-standard-22-lssd", "c3-standard-22-lssd", "c3-standard-22-lssd", "c3-standard-22-lssd", "c3-standard-22-lssd"},
				ClusterMetadata: &vmrs.ClusterMetadata{
					NumHAPairs:    6,
					NumNodes:      12,
					NumLIFs:       6,
					IsHomogeneous: true,
					VMType:        "c3-standard-22-lssd",
				},
			},
		},
		{
			name:           "C3Standard22LSSD_MaxThroughput_BoundaryTest",
			configFilename: "testdata/valid_large_capacity3.yaml",
			customerRequest: vmrs.CustomerRequestedPerformance{
				DesiredIOPS:             20000,  // Moderate IOPS
				DesiredThroughputInMiBs: 2682,   // max throughput (2,682) for c3-standard-22-lssd
				DesiredCapacityInGiB:    200000, // Moderate capacity
			},
			expectedError: "",
			expectedDecision: &vmrs.Decision{
				ChosenVMs: []string{"c3-standard-22-lssd", "c3-standard-22-lssd", "c3-standard-22-lssd", "c3-standard-22-lssd", "c3-standard-22-lssd", "c3-standard-22-lssd", "c3-standard-22-lssd", "c3-standard-22-lssd", "c3-standard-22-lssd", "c3-standard-22-lssd", "c3-standard-22-lssd", "c3-standard-22-lssd"},
				ClusterMetadata: &vmrs.ClusterMetadata{
					NumHAPairs:    6,
					NumNodes:      12,
					NumLIFs:       6,
					IsHomogeneous: true,
					VMType:        "c3-standard-22-lssd",
				},
			},
		},
		// c3-standard-44-lssd boundary tests
		{
			name:           "C3Standard44LSSD_MinIOPS_BoundaryTest",
			configFilename: "testdata/valid_large_capacity3.yaml",
			customerRequest: vmrs.CustomerRequestedPerformance{
				DesiredIOPS:             125873, // Minimum IOPS
				DesiredThroughputInMiBs: 2000,   // Moderate throughput
				DesiredCapacityInGiB:    500000, // Moderate capacity
			},
			expectedError: "",
			expectedDecision: &vmrs.Decision{
				ChosenVMs: []string{"c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd"},
				ClusterMetadata: &vmrs.ClusterMetadata{
					NumHAPairs:    6,
					NumNodes:      12,
					NumLIFs:       6,
					IsHomogeneous: true,
					VMType:        "c3-standard-44-lssd",
				},
			},
		},
		{
			name:           "C3Standard44LSSD_MaxIOPS_BoundaryTest",
			configFilename: "testdata/valid_large_capacity3.yaml",
			customerRequest: vmrs.CustomerRequestedPerformance{
				DesiredIOPS:             254328,  // Near max IOPS (254,328) for c3-standard-44-lssd
				DesiredThroughputInMiBs: 2000,    // Moderate throughput
				DesiredCapacityInGiB:    1000000, // Moderate capacity
			},
			expectedError: "",
			expectedDecision: &vmrs.Decision{
				ChosenVMs: []string{"c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd"},
				ClusterMetadata: &vmrs.ClusterMetadata{
					NumHAPairs:    6,
					NumNodes:      12,
					NumLIFs:       6,
					IsHomogeneous: true,
					VMType:        "c3-standard-44-lssd",
				},
			},
		},
		{
			name:           "C3Standard44LSSD_MinThroughput_BoundaryTest",
			configFilename: "testdata/valid_large_capacity3.yaml",
			customerRequest: vmrs.CustomerRequestedPerformance{
				DesiredIOPS:             50000,  // Moderate IOPS
				DesiredThroughputInMiBs: 2683,   // Minimum throughput
				DesiredCapacityInGiB:    500000, // Moderate capacity
			},
			expectedError: "",
			expectedDecision: &vmrs.Decision{
				ChosenVMs: []string{"c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd"},
				ClusterMetadata: &vmrs.ClusterMetadata{
					NumHAPairs:    6,
					NumNodes:      12,
					NumLIFs:       6,
					IsHomogeneous: true,
					VMType:        "c3-standard-44-lssd",
				},
			},
		},
		{
			name:           "C3Standard44LSSD_MaxThroughput_BoundaryTest",
			configFilename: "testdata/valid_large_capacity3.yaml",
			customerRequest: vmrs.CustomerRequestedPerformance{
				DesiredIOPS:             50000,   // Moderate IOPS
				DesiredThroughputInMiBs: 4716,    // max throughput (4,716) for c3-standard-44-lssd
				DesiredCapacityInGiB:    1000000, // Moderate capacity
			},
			expectedError: "",
			expectedDecision: &vmrs.Decision{
				ChosenVMs: []string{"c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd"},
				ClusterMetadata: &vmrs.ClusterMetadata{
					NumHAPairs:    6,
					NumNodes:      12,
					NumLIFs:       6,
					IsHomogeneous: true,
					VMType:        "c3-standard-44-lssd",
				},
			},
		},
		{
			name:           "C3Standard44LSSD_MinCapacity_BoundaryTest",
			configFilename: "testdata/valid_large_capacity3.yaml",
			customerRequest: vmrs.CustomerRequestedPerformance{
				DesiredIOPS:             50000,   // Moderate IOPS
				DesiredThroughputInMiBs: 1000,    // Moderate throughput
				DesiredCapacityInGiB:    1278100, // Minimum capacity (just greater than 1,278,000)
			},
			expectedError: "",
			expectedDecision: &vmrs.Decision{
				ChosenVMs: []string{"c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd"},
				ClusterMetadata: &vmrs.ClusterMetadata{
					NumHAPairs:    6,
					NumNodes:      12,
					NumLIFs:       6,
					IsHomogeneous: true,
					VMType:        "c3-standard-44-lssd",
				},
			},
		},
		{
			name:           "C3Standard44LSSD_MaxCapacity_BoundaryTest",
			configFilename: "testdata/valid_large_capacity3.yaml",
			customerRequest: vmrs.CustomerRequestedPerformance{
				DesiredIOPS:             50000,   // Moderate IOPS
				DesiredThroughputInMiBs: 2000,    // Moderate throughput
				DesiredCapacityInGiB:    2556000, // max capacity for 6 HA pair (2,556,000) for c3-standard-44-lssd
			},
			expectedError: "",
			expectedDecision: &vmrs.Decision{
				ChosenVMs: []string{"c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd", "c3-standard-44-lssd"},
				ClusterMetadata: &vmrs.ClusterMetadata{
					NumHAPairs:    6,
					NumNodes:      12,
					NumLIFs:       6,
					IsHomogeneous: true,
					VMType:        "c3-standard-44-lssd",
				},
			},
		},
		// c3-standard-88-lssd boundary tests
		{
			name:           "C3Standard88LSSD_MinIOPS_BoundaryTest",
			configFilename: "testdata/valid_large_capacity3.yaml",
			customerRequest: vmrs.CustomerRequestedPerformance{
				DesiredIOPS:             254329,  // Minimum IOPS
				DesiredThroughputInMiBs: 5000,    // High throughput
				DesiredCapacityInGiB:    1000000, // High capacity
			},
			expectedError: "",
			expectedDecision: &vmrs.Decision{
				ChosenVMs: []string{"c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd"},
				ClusterMetadata: &vmrs.ClusterMetadata{
					NumHAPairs:    6,
					NumNodes:      12,
					NumLIFs:       6,
					IsHomogeneous: true,
					VMType:        "c3-standard-88-lssd",
				},
			},
		},
		{
			name:           "C3Standard88LSSD_MaxIOPS_BoundaryTest",
			configFilename: "testdata/valid_large_capacity3.yaml",
			customerRequest: vmrs.CustomerRequestedPerformance{
				DesiredIOPS:             644000,  // Near max IOPS (644,000) for c3-standard-88-lssd
				DesiredThroughputInMiBs: 10000,   // High throughput
				DesiredCapacityInGiB:    2000000, // High capacity
			},
			expectedError: "",
			expectedDecision: &vmrs.Decision{
				ChosenVMs: []string{"c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd"},
				ClusterMetadata: &vmrs.ClusterMetadata{
					NumHAPairs:    6,
					NumNodes:      12,
					NumLIFs:       6,
					IsHomogeneous: true,
					VMType:        "c3-standard-88-lssd",
				},
			},
		},
		{
			name:           "C3Standard88LSSD_MinThroughput_BoundaryTest",
			configFilename: "testdata/valid_large_capacity3.yaml",
			customerRequest: vmrs.CustomerRequestedPerformance{
				DesiredIOPS:             100000,  // High IOPS
				DesiredThroughputInMiBs: 4717,    // Minimum throughput
				DesiredCapacityInGiB:    1000000, // High capacity
			},
			expectedError: "",
			expectedDecision: &vmrs.Decision{
				ChosenVMs: []string{"c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd"},
				ClusterMetadata: &vmrs.ClusterMetadata{
					NumHAPairs:    6,
					NumNodes:      12,
					NumLIFs:       6,
					IsHomogeneous: true,
					VMType:        "c3-standard-88-lssd",
				},
			},
		},
		{
			name:           "C3Standard88LSSD_MaxThroughput_BoundaryTest",
			configFilename: "testdata/valid_large_capacity3.yaml",
			customerRequest: vmrs.CustomerRequestedPerformance{
				DesiredIOPS:             100000,  // High IOPS
				DesiredThroughputInMiBs: 23625,   // Near max throughput (23,625) for c3-standard-88-lssd
				DesiredCapacityInGiB:    2000000, // High capacity
			},
			expectedError: "",
			expectedDecision: &vmrs.Decision{
				ChosenVMs: []string{"c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd"},
				ClusterMetadata: &vmrs.ClusterMetadata{
					NumHAPairs:    6,
					NumNodes:      12,
					NumLIFs:       6,
					IsHomogeneous: true,
					VMType:        "c3-standard-88-lssd",
				},
			},
		},
		{
			name:           "C3Standard88LSSD_MaxCapacity_BoundaryTest",
			configFilename: "testdata/valid_large_capacity3.yaml",
			customerRequest: vmrs.CustomerRequestedPerformance{
				DesiredIOPS:             100000,  // High IOPS
				DesiredThroughputInMiBs: 10000,   // High throughput
				DesiredCapacityInGiB:    2556000, // Near max capacity (2,556,000) for c3-standard-88-lssd
			},
			expectedError: "",
			expectedDecision: &vmrs.Decision{
				ChosenVMs: []string{"c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd", "c3-standard-88-lssd"},
				ClusterMetadata: &vmrs.ClusterMetadata{
					NumHAPairs:    6,
					NumNodes:      12,
					NumLIFs:       6,
					IsHomogeneous: true,
					VMType:        "c3-standard-88-lssd",
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
	}

	_lVHaPair := LVHaPair
	LVHaPair = 6 // Set global variable for HA pairs in large volume tests
	// Reset after tests
	defer func() {
		LVHaPair = _lVHaPair
	}()
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
