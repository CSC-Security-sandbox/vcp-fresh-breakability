package jobs

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/processor"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/utils"
)

type ProcessPerformanceMetrics struct {
	Data string
}

func NewProcessPerformanceMetrics(data string) *ProcessPerformanceMetrics {
	return &ProcessPerformanceMetrics{
		Data: data,
	}
}

func (e ProcessPerformanceMetrics) Perform(p *processor.MetricsProcessor, attempt int32) error {
	err := (*p).ProcessPerformanceMetrics(context.Background())
	if err != nil {
		return err
	}
	return nil
}

func (e ProcessPerformanceMetrics) Load(data string) (utils.Job, error) {
	return ProcessPerformanceMetrics{}, nil
}
