package metrics

import (
	"log/slog"
	"net/http"
	"regexp"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	sharedmetrics "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/metrics"
)

// Region re-exports the shared region helper.
func Region() string { return sharedmetrics.Region() }

var APIRequestsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "api_requests_total",
		Help: "Total API requests",
	},
	[]string{"endpoint", "method", "status_code", "region"},
)

var APIRequestDurationSeconds = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "api_request_duration_seconds",
		Help:    "End-to-end handler latency",
		Buckets: prometheus.DefBuckets,
	},
	[]string{"method", "endpoint", "region"},
)

func init() {
	collectors := []prometheus.Collector{APIRequestsTotal, APIRequestDurationSeconds}

	// Primary: custom registry keeps the /metrics handler free of default
	// Go runtime / process collectors — only our application metrics appear.
	sharedmetrics.Registry.MustRegister(collectors...)

	// Secondary: also register on prometheus.DefaultRegistry so that any
	// tooling relying on the global default (e.g. OTel bridge, sidecars)
	// can discover these counters/histograms. Errors are non-fatal because
	// tests or multi-init scenarios may re-register the same collectors.
	for _, c := range collectors {
		if err := prometheus.Register(c); err != nil {
			slog.Warn("metrics: skipping default-registry registration", "error", err)
		}
	}
}

// Handler returns an http.Handler that serves only the metrics registered
// in our custom registry (excluding default Go runtime/process collectors).
func Handler() http.Handler {
	return promhttp.HandlerFor(sharedmetrics.Registry, promhttp.HandlerOpts{})
}

// Known route patterns for path normalization to prevent label cardinality explosion.
var routePatterns = []struct {
	re       *regexp.Regexp
	template string
}{
	{regexp.MustCompile(`^/v1beta/pools/[^/]+$`), "/v1beta/pools/{poolOCID}"},
	{regexp.MustCompile(`^/v1beta/pools/[^/]+/svms$`), "/v1beta/pools/{poolOCID}/svms"},
	{regexp.MustCompile(`^/v1beta/pools/[^/]+/svms/[^/]+$`), "/v1beta/pools/{poolOCID}/svms/{svmOCID}"},
	{regexp.MustCompile(`^/v1beta/workRequests/[^/]+$`), "/v1beta/workRequests/{workRequestId}"},
}

// NormalizeRoute replaces dynamic path segments with their parameter
// templates so that Prometheus labels stay bounded.
func NormalizeRoute(path string) string {
	for _, rp := range routePatterns {
		if rp.re.MatchString(path) {
			return rp.template
		}
	}
	return path
}
