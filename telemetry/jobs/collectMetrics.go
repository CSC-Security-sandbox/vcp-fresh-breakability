package jobs

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/utils"
)

type CollectMetrics struct {
	Data string
}

func NewCollectMetrics(data string) *CollectMetrics {
	return &CollectMetrics{
		Data: data,
	}
}

func (e CollectMetrics) Perform(p interface{}, attempt int32) error {
	proc, ok := p.(common.VCPProcessor)
	if !ok {
		return fmt.Errorf("invalid processor type: %T", p)
	}
	var cm CollectMetrics
	err := json.Unmarshal([]byte(e.Data), &cm)
	if err != nil {
		return err
	}
	err = proc.CollectMetrics(context.Background(), cm.Data)
	if err != nil {
		return err
	}
	return nil
}

func (e CollectMetrics) Load(data string) (utils.Job, error) {
	return CollectMetrics{data}, nil
}
