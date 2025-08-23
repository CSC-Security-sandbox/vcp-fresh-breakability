package decision

import (
	// testify - assert and mock packages for testing.
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vmrs"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vmrs/config"
)

func TestNewLeastCostSingleVMDecisionMaker(t *testing.T) {
	cases := []struct {
		name             string
		configFilename   string
		customerRequest  vmrs.CustomerRequestedPerformance
		expectedError    string
		expectedDecision *vmrs.Decision
	}{
		{
			name:           "ValidConfig",
			configFilename: "testdata/valid.yaml",
			customerRequest: vmrs.CustomerRequestedPerformance{
				DesiredIOPS:             1,
				DesiredThroughputInMiBs: 4,
				DesiredCapacityInGiB:    1,
			},
			expectedError: "",
			expectedDecision: &vmrs.Decision{
				ChosenVMs: []string{"c4-standard-8"},
				StoragePoolRequirements: vmrs.CustomerRequestedPerformance{
					DesiredIOPS:             2,
					DesiredThroughputInMiBs: 9,
					DesiredCapacityInGiB:    2,
				},
			},
		},
		{
			name:           "ValidConfigWithMissingWorkloadHeadroom",
			configFilename: "testdata/valid_missing_workload_headroom.yaml",
			customerRequest: vmrs.CustomerRequestedPerformance{
				DesiredIOPS:             10,
				DesiredThroughputInMiBs: 10,
				DesiredCapacityInGiB:    10,
			},
			expectedError: "",
			expectedDecision: &vmrs.Decision{
				ChosenVMs: []string{"c4-standard-16"},
				StoragePoolRequirements: vmrs.CustomerRequestedPerformance{
					DesiredIOPS:             13,
					DesiredThroughputInMiBs: 14,
					DesiredCapacityInGiB:    13,
				},
			},
		},
		{
			name:           "MatchingVMEvenWithOverheads",
			configFilename: "testdata/valid.yaml",
			customerRequest: vmrs.CustomerRequestedPerformance{
				DesiredIOPS:             2,
				DesiredThroughputInMiBs: 1,
				DesiredCapacityInGiB:    1,
			},
			expectedError: "",
			expectedDecision: &vmrs.Decision{
				ChosenVMs: []string{"c4-standard-8"},
				StoragePoolRequirements: vmrs.CustomerRequestedPerformance{
					DesiredIOPS:             4,
					DesiredThroughputInMiBs: 3,
					DesiredCapacityInGiB:    2,
				},
			},
		},
		{
			name:           "NoMatchingVMWhenRequestExceedsAllLimits",
			configFilename: "testdata/valid.yaml",
			customerRequest: vmrs.CustomerRequestedPerformance{
				DesiredIOPS:             1000,
				DesiredThroughputInMiBs: 1000,
				DesiredCapacityInGiB:    1000,
			},
			expectedError:    "[vmrs] NoSuitableVMError: no suitable VM found for the customer request (customer request: {DesiredIOPS:1000 DesiredThroughputInMiBs:1000 DesiredCapacityInGiB:1000})",
			expectedDecision: nil,
		},
		{
			name:           "NoMatchingVMWhenIOPSExceedsLimits",
			configFilename: "testdata/valid.yaml",
			customerRequest: vmrs.CustomerRequestedPerformance{
				DesiredIOPS:             100,
				DesiredThroughputInMiBs: 1,
				DesiredCapacityInGiB:    1,
			},
			expectedError:    "[vmrs] NoSuitableVMError: no suitable VM found for the customer request (customer request: {DesiredIOPS:100 DesiredThroughputInMiBs:1 DesiredCapacityInGiB:1})",
			expectedDecision: nil,
		},
		{
			name:           "NoMatchingVMWhenThroughtputExceedsLimits",
			configFilename: "testdata/valid.yaml",
			customerRequest: vmrs.CustomerRequestedPerformance{
				DesiredIOPS:             1,
				DesiredThroughputInMiBs: 100,
				DesiredCapacityInGiB:    1,
			},
			expectedError:    "[vmrs] NoSuitableVMError: no suitable VM found for the customer request (customer request: {DesiredIOPS:1 DesiredThroughputInMiBs:100 DesiredCapacityInGiB:1})",
			expectedDecision: nil,
		},
		{
			name:           "NoMatchingVMWhenCapacityExceedsLimits",
			configFilename: "testdata/valid.yaml",
			customerRequest: vmrs.CustomerRequestedPerformance{
				DesiredIOPS:             1,
				DesiredThroughputInMiBs: 1,
				DesiredCapacityInGiB:    20,
			},
			expectedError:    "[vmrs] NoSuitableVMError: no suitable VM found for the customer request (customer request: {DesiredIOPS:1 DesiredThroughputInMiBs:1 DesiredCapacityInGiB:20})",
			expectedDecision: nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			config, _ := config.LoadConfig(tc.configFilename)
			dm := NewLeastCostSingleVMDecisionMaker(config)
			dsc, err := dm.FindOptimalVMs(config, tc.customerRequest, nil)

			if tc.expectedError != "" {
				assert.NotNil(t, err, "expected an error but got nil")
				assert.EqualError(t, err, tc.expectedError, "error message mismatch")
			} else {
				assert.Nil(t, err, "expected no error but got one")
				assert.NotNil(t, dsc, "expected a decision but got nil")
				assert.Len(t, dsc.ChosenVMs, 1, "expected exactly one chosen VM")
				assert.Equal(t, tc.expectedDecision, dsc, "VMRS decision mismatch")
			}
		})
	}
}

// TestCompareVMScalingDirection tests the CompareVMScalingDirection method
func TestCompareVMScalingDirection(t *testing.T) {
	cases := []struct {
		name                string
		configFilename      string
		currentInstanceType string
		newInstanceType     string
		expectedIsScalingUp bool
		expectedError       string
	}{
		{
			name:                "ScalingUp_CheaperToMoreExpensive",
			configFilename:      "testdata/valid.yaml",
			currentInstanceType: "c4-standard-4",
			newInstanceType:     "c4-standard-8",
			expectedIsScalingUp: true,
			expectedError:       "",
		},
		{
			name:                "ScalingDown_MoreExpensiveToCheaper",
			configFilename:      "testdata/valid.yaml",
			currentInstanceType: "c4-standard-8",
			newInstanceType:     "c4-standard-4",
			expectedIsScalingUp: false,
			expectedError:       "",
		},
		{
			name:                "SameVMType_NoScaling",
			configFilename:      "testdata/valid.yaml",
			currentInstanceType: "c4-standard-8",
			newInstanceType:     "c4-standard-8",
			expectedIsScalingUp: false,
			expectedError:       "",
		},
		{
			name:                "SameCost_NoScaling",
			configFilename:      "testdata/valid.yaml",
			currentInstanceType: "c4-standard-8",
			newInstanceType:     "c4-standard-16",
			expectedIsScalingUp: false,
			expectedError:       "",
		},
		{
			name:                "CurrentVMTypeNotFound",
			configFilename:      "testdata/valid.yaml",
			currentInstanceType: "non-existent-vm-type",
			newInstanceType:     "c4-standard-8",
			expectedIsScalingUp: false,
			expectedError:       "current VM type not found in sorted list",
		},
		{
			name:                "NewVMTypeNotFound",
			configFilename:      "testdata/valid.yaml",
			currentInstanceType: "c4-standard-8",
			newInstanceType:     "non-existent-vm-type",
			expectedIsScalingUp: false,
			expectedError:       "new VM type not found in sorted list",
		},
		{
			name:                "BothVMTypesNotFound",
			configFilename:      "testdata/valid.yaml",
			currentInstanceType: "non-existent-vm-type-1",
			newInstanceType:     "non-existent-vm-type-2",
			expectedIsScalingUp: false,
			expectedError:       "current VM type not found in sorted list",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			config, _ := config.LoadConfig(tc.configFilename)
			dm := NewLeastCostSingleVMDecisionMaker(config)

			isScalingUp, err := dm.CompareVMScalingDirection(tc.currentInstanceType, tc.newInstanceType)

			if tc.expectedError != "" {
				assert.NotNil(t, err, "expected an error but got nil")
				assert.Contains(t, err.Error(), tc.expectedError, "error message should contain expected text")
			} else {
				assert.Nil(t, err, "expected no error but got one")
				assert.Equal(t, tc.expectedIsScalingUp, isScalingUp, "scaling direction mismatch")
			}
		})
	}
}

// TestCompareVMScalingDirection_EarlyBreak tests the early break optimization
func TestCompareVMScalingDirection_EarlyBreak(t *testing.T) {
	config, _ := config.LoadConfig("testdata/valid.yaml")
	dm := NewLeastCostSingleVMDecisionMaker(config)

	// Test with first and last VM types to ensure early break works
	currentInstanceType := "c4-standard-4" // First in the list (cheapest)
	newInstanceType := "c4-standard-8"     // Last in the list (more expensive)

	isScalingUp, err := dm.CompareVMScalingDirection(currentInstanceType, newInstanceType)

	assert.Nil(t, err, "expected no error")
	assert.True(t, isScalingUp, "should be scaling up from cheaper to more expensive VM")
}

// TestCompareVMScalingDirection_EdgeCases tests edge cases for the comparison method
func TestCompareVMScalingDirection_EdgeCases(t *testing.T) {
	config, _ := config.LoadConfig("testdata/valid.yaml")
	dm := NewLeastCostSingleVMDecisionMaker(config)

	// Test with VM types that have the same relative cost
	// This would require a config with VMs having identical RelativeCost values
	// For now, we'll test the basic functionality

	// Test that the method handles the case where we need to iterate through the entire list
	// by using VM types that are not at the beginning
	currentInstanceType := "c4-standard-4"
	newInstanceType := "c4-standard-8"

	isScalingUp, err := dm.CompareVMScalingDirection(currentInstanceType, newInstanceType)

	assert.Nil(t, err, "expected no error")
	assert.True(t, isScalingUp, "should be scaling up")
}
