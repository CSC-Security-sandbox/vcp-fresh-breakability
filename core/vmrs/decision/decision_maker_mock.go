package decision

import (
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/vlm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vmrs"
)

// DecisionMakerMock is a mock implementation of the DecisionMaker interface for testing purposes.
type DecisionMakerMock struct {
	mock.Mock
}

// NewDecisionMakerMock creates a new instance of DecisionMakerMock.
func NewDecisionMakerMock() *DecisionMakerMock {
	return &DecisionMakerMock{}
}

// FindOptimalVMs mocks the FindOptimalVMs method of the DecisionMaker interface.
func (m *DecisionMakerMock) FindOptimalVMs(config *vmrs.VMRSConfig, customerRequest vmrs.CustomerRequestedPerformance, currentConfig *vlm.VLMConfig) (*vmrs.Decision, error) {
	args := m.Called(config, customerRequest, currentConfig)
	if decision, ok := args.Get(0).(*vmrs.Decision); ok {
		return decision, args.Error(1)
	}
	return nil, args.Error(1)
}

// CompareVMScalingDirection mocks the CompareVMScalingDirection method of the DecisionMaker interface.
func (m *DecisionMakerMock) CompareVMScalingDirection(currentInstanceType, newInstanceType string) (bool, error) {
	args := m.Called(currentInstanceType, newInstanceType)
	return args.Bool(0), args.Error(1)
}
