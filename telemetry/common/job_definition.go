package common

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
)

// AggregationJobDefinition is a description of an aggregation job that cvt is expected to run.
type AggregationJobDefinition struct {
	MeasuredType    metadata.MeasuredType
	ResourceType    metadata.ResourceType
	AggregationType JobType
	IsBillable      bool
	SKU             string
}

const (
	BillingMetricNameVolumeBackup                    = "/BackupStorageKbBillable"
	BillingMetricNameBackupNetworkTransfer           = "/BackupNetworkBytesTransferred"
	BillingMetricNameVolumeBackupManagementUsage     = "/BackupManagementFeeGbBillable"
	BillingMetricNameReplication                     = "/ReplicationBytesTransferred"
	BillingMetricNamePoolColdTierSize                = "/StoragePoolColdTierMbBillable"
	BillingMetricNamePoolHotTierSize                 = "/StoragePoolHotTierMbBillable"
	BillingMetricNamePoolColdTierNetworkTransferSize = "/AutoTierOperationsNetworkingBillable"
	BillingMetricsNamePrefix                         = "netapp.googleapis.com"
)

var DefaultAggregationJobDefinitions = map[metadata.CombinedKeyResourceTypeMeasuredType]AggregationJobDefinition{
	{ResourceType: metadata.Volume, MeasuredType: metadata.AllocatedSize}: {
		AggregationType: IntegralAggregation,
		IsBillable:      false,
	},
	{ResourceType: metadata.Volume, MeasuredType: metadata.LogicalSize}: {
		AggregationType: IntegralAggregation,
		IsBillable:      false,
	},
	{ResourceType: metadata.VolumeReplicationRelationship, MeasuredType: metadata.XregionReplicationTotalTransferBytes}: {
		AggregationType: CounterAggregation,
		IsBillable:      true,
		SKU:             BillingMetricNameReplication,
	},
	{ResourceType: metadata.VolumePool, MeasuredType: metadata.PoolAllocatedSize}: {
		AggregationType: CounterAggregation,
		IsBillable:      false,
	},
	{ResourceType: metadata.VolumePool, MeasuredType: metadata.AllocatedUsed}: {
		AggregationType: CounterAggregation,
		IsBillable:      false,
	},
	{ResourceType: metadata.Volume, MeasuredType: metadata.CbsVolumeBackupSize}: {
		AggregationType: IntegralAggregation,
		IsBillable:      true,
		SKU:             BillingMetricNameVolumeBackup,
	},
}
