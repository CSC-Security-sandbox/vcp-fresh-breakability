package jobs

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/processor"
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

func (e ProcessUsageMetrics) Perform(p *processor.MetricsProcessor, attempt int32) error {
	err := (*p).ProcessUsageMetrics(context.Background())
	if err != nil {
		return err
	}
	return nil
}

func (e ProcessUsageMetrics) Load(data string) (utils.Job, error) {
	return ProcessUsageMetrics{}, nil
}
