package sink

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/entity"
)

type BizOpsSinkType int

type BizOpsSink interface {
	Ingest(ctx context.Context, sinkParams *entity.BizopsSinkParams) error
	Type() string
}
