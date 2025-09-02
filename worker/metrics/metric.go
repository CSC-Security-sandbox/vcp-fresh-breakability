package metrics

import (
	"context"
	"log"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/worker/helper"
)

var JobStatusCounter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "vcp_job_status_updates",
		Help: "Total number of job status updates",
	},
	[]string{"project_id", "error_details", "state"},
)

func IncJobStatusCounter(ctx context.Context, errorDetails, state string) {
	projectID := helper.GetProjectID(ctx)
	if len(errorDetails) > 1024 {
		errorDetails = errorDetails[:1024]
	}
	JobStatusCounter.WithLabelValues(
		projectID,
		errorDetails,
		state,
	).Inc()
}

func RegisterJobStatusCounter() {
	err := prometheus.Register(JobStatusCounter)
	if err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			JobStatusCounter = are.ExistingCollector.(*prometheus.CounterVec)
		} else {
			log.Printf("Failed to register JobStatusCounter: %v", err)
		}
	}
}
