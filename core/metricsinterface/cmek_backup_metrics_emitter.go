package metricsinterface

// CmekBackupMetricsEmitter defines the interface for emitting CMEK backup
// rotation metrics. This mirrors the CBS metric cbs_cmek_rewrite_error_gauge
// so that VCP and SDE alerts stay consistent.
type CmekBackupMetricsEmitter interface {
	// AddCMEKRewriteErrorResult records a CMEK rotation failure.
	// failureType must be a fixed category string (e.g.
	// "bucket_rotation_failed") to avoid Prometheus label cardinality issues.
	AddCMEKRewriteErrorResult(bucketName, ownerID, backupVaultUUID, failureType string)
}

// NoOpCmekBackupMetricsEmitter is a no-op implementation used in tests and
// scenarios where metrics are not required.
type NoOpCmekBackupMetricsEmitter struct{}

func (n *NoOpCmekBackupMetricsEmitter) AddCMEKRewriteErrorResult(bucketName, ownerID, backupVaultUUID, failureType string) {
}
