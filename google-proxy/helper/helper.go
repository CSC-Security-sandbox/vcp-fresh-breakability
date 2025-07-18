package helper

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"go.opentelemetry.io/otel/attribute"
)

var (
	gcpgenserverLabelerFromContext = gcpgenserver.LabelerFromContext
)

// AddLabelerAttributes adds custom attributes like project number, location ID, and trace URL to the OpenTelemetry labeler from the context.
func AddLabelerAttributes(ctx context.Context, projectNumber, locationId string, job *models.Job) {
	labeler, _ := gcpgenserverLabelerFromContext(ctx)
	labeler.Add(attribute.String("locationID", locationId))
	labeler.Add(attribute.String("projectNumber", projectNumber))
	jobType := ""
	jobState := ""
	jobTrackingID := 0

	if job != nil {
		jobType = string(job.Type)
		jobState = string(job.State)
		jobTrackingID = job.TrackingID
	}
	labeler.Add(attribute.String("Job_Type", jobType))
	labeler.Add(attribute.String("Job_State", jobState))
	labeler.Add(attribute.Int("Job_TrackingID", jobTrackingID))
}

// FindMissingUUIDs Find missing UUIDs from a list
func FindMissingUUIDs(requested []string, found map[string]struct{}) []string {
	missing := make([]string, 0, len(requested)-len(found))
	for _, uuid := range requested {
		if _, ok := found[uuid]; !ok {
			missing = append(missing, uuid)
		}
	}
	return missing
}
