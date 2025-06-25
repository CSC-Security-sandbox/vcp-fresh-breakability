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
			name:           "MatchingVMEvenWithOverheads",
			configFilename: "testdata/valid.yaml",
			customerRequest: vmrs.CustomerRequestedPerformance{
				DesiredIOPS:             2,
				DesiredThroughputInMiBs: 1,
				DesiredCapacityInGiB:    1,
			},
			expectedError: "",
			expectedDecision: &vmrs.Decision{
				ChosenVMs: []string{"c4-standard-16"},
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
