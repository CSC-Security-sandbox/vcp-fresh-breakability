package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
)

// Registry is a dedicated Prometheus registry shared across OCI services.
// Using this instead of prometheus.DefaultRegisterer keeps the /metrics
// endpoint free of Go runtime, process, Temporal SDK, and ogen metrics.
var Registry = prometheus.NewRegistry()

var region = env.GetString("LOCAL_REGION", "")

// Region returns the LOCAL_REGION environment variable value, used as a
// label dimension for metrics emitted by both the proxy and workflows.
func Region() string { return region }
