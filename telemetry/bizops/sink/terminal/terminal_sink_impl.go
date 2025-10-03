package terminal

import (
	"context"
	"io"
	"os"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/bizops/sink"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/entity"
)

type TerminalBizOpsSink struct {
}

func NewTerminalBizOpsSink() sink.BizOpsSink {
	return &TerminalBizOpsSink{}
}

func (s *TerminalBizOpsSink) Ingest(ctx context.Context, sinkParams *entity.BizopsSinkParams) error {
	err := sink.ValidateSinkParams(sinkParams)
	if err != nil {
		return err
	}
	_, err = io.Copy(os.Stdout, sinkParams.Reader)
	if err != nil {
		return err
	}
	return nil
}
func (s *TerminalBizOpsSink) Type() string {
	return "terminal"
}
