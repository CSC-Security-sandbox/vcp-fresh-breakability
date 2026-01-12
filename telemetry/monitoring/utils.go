package monitoring

import (
	"github.com/prometheus/client_golang/prometheus"
)

func RegisterCollector(cs ...prometheus.Collector) {
	prometheus.MustRegister(cs...)
}
