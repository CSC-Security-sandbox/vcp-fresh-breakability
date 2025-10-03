package bizops

import (
	"errors"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/bizops/sink"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/bizops/sink/gcs"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/bizops/sink/terminal"
)

const (
	Invalid sink.BizOpsSinkType = iota
	GCP
	Terminal
)

var (
	bizOpsSinkMapping = map[string]sink.BizOpsSinkType{
		"gcs":      GCP,
		"terminal": Terminal,
		"invalid":  Invalid,
	}
)

var bizOpsSinkFactory = map[sink.BizOpsSinkType]func() sink.BizOpsSink{}

func init() {
	bizOpsSinkFactory[GCP] = gcs.NewGCPBizOpsSink
	bizOpsSinkFactory[Terminal] = terminal.NewTerminalBizOpsSink
}

func NewSink(bizOpsType sink.BizOpsSinkType) (sink.BizOpsSink, error) {
	if bizOpsSink, exists := bizOpsSinkFactory[bizOpsType]; exists {
		return bizOpsSink(), nil
	}
	return nil, errors.New("invalid biz ops type")
}
