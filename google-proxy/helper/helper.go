package helper

import (
	"context"

	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"go.opentelemetry.io/otel/attribute"
)

var (
	gcpgenserverLabelerFromContext = gcpgenserver.LabelerFromContext
)

// AddLabelerAttributes adds custom attributes like project number, location ID, and trace URL to the OpenTelemetry labeler from the context.
func AddLabelerAttributes(ctx context.Context, projectNumber, locationId string) {
	labeler, _ := gcpgenserverLabelerFromContext(ctx)
	if loggerFields, ok := ctx.Value(middleware.TemporalSLoggerKey).(log.Fields); ok {
		if traceURL, ok := loggerFields["traceURL"].(string); ok {
			labeler.Add(attribute.String("http.route", traceURL))
		}
	}
	labeler.Add(attribute.String("locationID", locationId))
	labeler.Add(attribute.String("projectNumber", projectNumber))
}
