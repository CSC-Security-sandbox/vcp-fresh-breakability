package decision

import (
	// testify - assert and mock packages for testing.
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vmrs"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vmrs/config"
)

func TestNewDecisionMaker(t *testing.T) {
	cases := []struct {
		name           string
		configFilePath string
		expectedError  string
		expectedType   vmrs.DecisionMaker
	}{
		{
			name:           "KnownVMSelectionStrategy",
			configFilePath: "testdata/valid.yaml",
			expectedError:  "",
			expectedType:   new(LeastCostSingleVMDecisionMaker),
		},
		{
			name:           "UnknownVMSelectionStrategy",
			configFilePath: "testdata/invalid_selection_strategy.yaml",
			expectedError:  "[vmrs] InvalidConfigError: unsupported VM selection strategy: unknown_strategy",
			expectedType:   nil,
		},
		{
			name:           "NilConfig",
			configFilePath: "",
			expectedError:  "VMRSConfig cannot be nil",
			expectedType:   nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, _ := config.LoadConfig(tc.configFilePath)
			if tc.name == "NilConfig" {
				cfg = nil
			}
			dm, err := NewDecisionMaker(cfg)
			if tc.expectedError != "" {
				assert.NotNil(t, err)
				assert.EqualError(t, err, tc.expectedError)
				assert.Nil(t, dm)
			} else {
				assert.Nil(t, err)
				assert.NotNil(t, dm)
				assert.IsType(t, dm, tc.expectedType)
			}
		})
	}
}
