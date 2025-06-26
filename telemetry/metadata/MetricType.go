package metadata

// MetricType represents the type of metric.
type MetricType string

const (
	AllocatedSize               MetricType = "ALLOCATED_SIZE"
	AllocatedUsed               MetricType = "ALLOCATED_USED"
	TotalLogicalSize            MetricType = "TOTAL_LOGICAL_SIZE"
	TotalLogicalSizePercentage  MetricType = "TOTAL_LOGICAL_SIZE_PERCENTAGE"
	TotalSnapshotSize           MetricType = "TOTAL_SNAPSHOT_SIZE"
	ProvisionedThroughput       MetricType = "PROVISIONED_THROUGHPUT"
	AllocatedToVolumeThroughput MetricType = "ALLOCATED_TO_VOLUME_THROUGHPUT"
)
