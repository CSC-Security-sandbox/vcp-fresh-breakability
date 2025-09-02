package helper

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func GetProjectID(ctx context.Context) string {
	if fields, ok := ctx.Value(middleware.TemporalSLoggerKey).(log.Fields); ok {
		if v, ok := fields["customerID"].(string); ok && v != "" {
			return v
		}
	}
	return ""
}
