// This file defines the factory function to create a DecisionMaker instance based on the VMRSConfig.

package decision

import (
	"errors"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vmrs"
)

// NewDecisionMaker creates a DecisionMaker instance based on the VM selection strategy specified in the VMRSConfig.
// It supports two strategies: LeastCostSingleVM for regular deployments and LeastCostLargeVolumeCluster for large volume (FlexGroup) deployments.
// Returns an error if the config is nil or specifies an unsupported strategy.
func NewDecisionMaker(config *vmrs.VMRSConfig) (vmrs.DecisionMaker, error) {
	if config == nil {
		return nil, errors.New("VMRSConfig cannot be nil")
	}

	switch config.HyperscalerPerfLimits.VMSelectionStrategy {
	case vmrs.LeastCostSingleVM:
		return NewLeastCostSingleVMDecisionMaker(config), nil
	case vmrs.LeastCostLargeVolumeCluster:
		return NewLeastCostLargeVolumeClusterDecisionMaker(config), nil
	default:
		return nil, &vmrs.InvalidConfigError{
			Message: fmt.Sprintf("unsupported VM selection strategy: %s", config.HyperscalerPerfLimits.VMSelectionStrategy),
		}
	}
}
