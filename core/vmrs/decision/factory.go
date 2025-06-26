// This file defines the factory function to create a DecisionMaker instance based on the VMRSConfig.

package decision

import (
	"errors"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vmrs"
)

// An implementation of the DecisionMakerFactory interface that selects a concrete type that implements the DecisionMaker interface.
func NewDecisionMaker(config *vmrs.VMRSConfig) (vmrs.DecisionMaker, error) {
	if config == nil {
		return nil, errors.New("VMRSConfig cannot be nil")
	}

	switch config.HyperscalerPerfLimits.VMSelectionStrategy {
	case vmrs.LeastCostSingleVM:
		return NewLeastCostSingleVMDecisionMaker(config), nil
	default:
		return nil, &vmrs.InvalidConfigError{
			Message: fmt.Sprintf("unsupported VM selection strategy: %s", config.HyperscalerPerfLimits.VMSelectionStrategy),
		}
	}
}
