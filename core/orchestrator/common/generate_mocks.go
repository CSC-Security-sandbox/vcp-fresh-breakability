package common

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

type monkeyMethods interface {
	hydrateToCffe(ctx context.Context, logger log.Logger, v any, url string, method string, token string) error
}
