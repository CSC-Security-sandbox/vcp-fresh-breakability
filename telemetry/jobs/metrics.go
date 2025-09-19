package jobs

import (
	"context"
	"fmt"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/common"
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

func (e ProcessPerformanceMetrics) Perform(p interface{}, attempt int32) error {
	proc, ok := p.(common.VCPProcessor)
	if !ok {
		return fmt.Errorf("invalid processor type: %T", p)
	}
	err := proc.ProcessPerformanceMetrics(context.Background())
	if err != nil {
		return err
	}
	return nil
}

func (e ProcessPerformanceMetrics) Load(data string) (utils.Job, error) {
	return ProcessPerformanceMetrics{}, nil
}
