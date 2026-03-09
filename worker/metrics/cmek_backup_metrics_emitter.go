package metrics

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/metricsinterface"
)

var _ metricsinterface.CmekBackupMetricsEmitter = (*PrometheusCmekBackupMetricsEmitter)(nil)

// PrometheusCmekBackupMetricsEmitter is the concrete implementation of
// CmekBackupMetricsEmitter that records metrics to Prometheus.
type PrometheusCmekBackupMetricsEmitter struct{}

func NewPrometheusCmekBackupMetricsEmitter() *PrometheusCmekBackupMetricsEmitter {
	return &PrometheusCmekBackupMetricsEmitter{}
}

func (p *PrometheusCmekBackupMetricsEmitter) AddCMEKRewriteErrorResult(bucketName, ownerID, backupVaultUUID, failureType string) {
	AddCMEKRewriteErrorResult(bucketName, ownerID, backupVaultUUID, failureType)
}
