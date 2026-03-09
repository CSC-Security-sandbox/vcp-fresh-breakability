package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/metricsinterface"
)

func TestNewPrometheusCmekBackupMetricsEmitter(t *testing.T) {
	emitter := NewPrometheusCmekBackupMetricsEmitter()
	assert.NotNil(t, emitter)

	var _ metricsinterface.CmekBackupMetricsEmitter = emitter
}

func resetCmekGauge() {
	prometheus.Unregister(CmekBackupRewriteErrorGauge)
	CmekBackupRewriteErrorGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cbs_cmek_rewrite_error_gauge",
			Help: "Number of times CMEK object rewrite error occurred",
		},
		[]string{"error", "bucket_name", "owner_id", "backup_vault_uuid"},
	)
	RegisterCmekBackupRewriteErrorGauge()
}

func TestPrometheusCmekBackupMetricsEmitter_FailureTypeIncrementsGauge(t *testing.T) {
	resetCmekGauge()

	emitter := NewPrometheusCmekBackupMetricsEmitter()
	emitter.AddCMEKRewriteErrorResult("bucket-1", "owner-1", "bv-uuid-1", "bucket_rotation_failed")

	gauge, err := CmekBackupRewriteErrorGauge.GetMetricWithLabelValues("bucket_rotation_failed", "bucket-1", "owner-1", "bv-uuid-1")
	require.NoError(t, err)

	var m dto.Metric
	require.NoError(t, gauge.Write(&m))
	assert.Equal(t, float64(1), m.GetGauge().GetValue())
}

func TestPrometheusCmekBackupMetricsEmitter_MultipleCallsIncrement(t *testing.T) {
	resetCmekGauge()

	emitter := NewPrometheusCmekBackupMetricsEmitter()
	emitter.AddCMEKRewriteErrorResult("bucket-1", "owner-1", "bv-uuid-1", "bucket_rotation_failed")
	emitter.AddCMEKRewriteErrorResult("bucket-1", "owner-1", "bv-uuid-1", "bucket_rotation_failed")

	gauge, err := CmekBackupRewriteErrorGauge.GetMetricWithLabelValues("bucket_rotation_failed", "bucket-1", "owner-1", "bv-uuid-1")
	require.NoError(t, err)

	var m dto.Metric
	require.NoError(t, gauge.Write(&m))
	assert.Equal(t, float64(2), m.GetGauge().GetValue())
}

func TestPrometheusCmekBackupMetricsEmitter_DifferentFailureTypes(t *testing.T) {
	resetCmekGauge()

	emitter := NewPrometheusCmekBackupMetricsEmitter()
	emitter.AddCMEKRewriteErrorResult("bucket-1", "owner-1", "bv-uuid-1", "bucket_rotation_failed")
	emitter.AddCMEKRewriteErrorResult("", "owner-1", "bv-uuid-1", "sde_rotation_failed")

	g1, err := CmekBackupRewriteErrorGauge.GetMetricWithLabelValues("bucket_rotation_failed", "bucket-1", "owner-1", "bv-uuid-1")
	require.NoError(t, err)
	var m1 dto.Metric
	require.NoError(t, g1.Write(&m1))
	assert.Equal(t, float64(1), m1.GetGauge().GetValue())

	g2, err := CmekBackupRewriteErrorGauge.GetMetricWithLabelValues("sde_rotation_failed", "", "owner-1", "bv-uuid-1")
	require.NoError(t, err)
	var m2 dto.Metric
	require.NoError(t, g2.Write(&m2))
	assert.Equal(t, float64(1), m2.GetGauge().GetValue())
}

func TestNoOpCmekBackupMetricsEmitter(t *testing.T) {
	emitter := &metricsinterface.NoOpCmekBackupMetricsEmitter{}
	emitter.AddCMEKRewriteErrorResult("bucket", "owner", "bv", "bucket_rotation_failed")
	emitter.AddCMEKRewriteErrorResult("bucket", "owner", "bv", "sde_rotation_failed")
}

func TestRegisterCmekBackupRewriteErrorGauge_AlreadyRegistered(t *testing.T) {
	prometheus.Unregister(CmekBackupRewriteErrorGauge)
	RegisterCmekBackupRewriteErrorGauge()
	RegisterCmekBackupRewriteErrorGauge()
}
