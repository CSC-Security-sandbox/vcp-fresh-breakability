package jobs

import (
	"context"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/utils"
)

type ProcessUsageMetrics struct {
	Data string
}

func NewProcessUsageMetrics(data string) *ProcessUsageMetrics {
	return &ProcessUsageMetrics{
		Data: data,
	}
}

func (e ProcessUsageMetrics) Perform(p interface{}, attempt int32) error {
	proc, ok := p.(common.VCPProcessor)
	if !ok {
		return fmt.Errorf("invalid processor type: %T", p)
	}
	err := proc.ProcessUsageMetrics(context.Background())
	if err != nil {
		return err
	}
	return nil
}

func (e ProcessUsageMetrics) Load(data string) (utils.Job, error) {
	return ProcessUsageMetrics{}, nil
}
