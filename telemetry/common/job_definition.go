package common

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
	"time"
)

// AggregationJobDefinition is a description of an aggregation job that cvt is expected to run.
type AggregationJobDefinition struct {
	MeasuredType        metadata.MeasuredType
	ResourceType        metadata.ResourceType
	AggregationType     JobType
	IsBillable          bool
	SKU                 string
	TimeSeriesFormatter TimeSeriesFormatter
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
		TimeSeriesFormatter: &SampledMetricsFormatter{
			Mode:          Interval,
			BackfillLimit: 60 * time.Minute,
		},
	},
	{ResourceType: metadata.VolumeRegionalHA, MeasuredType: metadata.AllocatedSize}: {
		AggregationType: IntegralAggregation,
		IsBillable:      false,
		TimeSeriesFormatter: &SampledMetricsFormatter{
			Mode:          Interval,
			BackfillLimit: 60 * time.Minute,
		},
	},
	{ResourceType: metadata.Volume, MeasuredType: metadata.LogicalSize}: {
		AggregationType: IntegralAggregation,
		IsBillable:      false,
		TimeSeriesFormatter: &SampledMetricsFormatter{
			Mode:          Interval,
			BackfillLimit: 60 * time.Minute,
		},
	},
	{ResourceType: metadata.VolumeRegionalHA, MeasuredType: metadata.LogicalSize}: {
		AggregationType: IntegralAggregation,
		IsBillable:      false,
		TimeSeriesFormatter: &SampledMetricsFormatter{
			Mode:          Interval,
			BackfillLimit: 60 * time.Minute,
		},
	},
	{ResourceType: metadata.VolumeReplicationRelationship, MeasuredType: metadata.XregionReplicationTotalTransferBytes}: {
		AggregationType: CounterAggregation,
		IsBillable:      true,
		SKU:             BillingMetricNameReplication,
		TimeSeriesFormatter: &SampledMetricsFormatter{
			Mode:          Interval,
			BackfillLimit: 60 * time.Minute,
		},
	},
	{ResourceType: metadata.VolumePool, MeasuredType: metadata.PoolAllocatedSize}: {
		AggregationType: IntegralAggregation,
		IsBillable:      false,
		TimeSeriesFormatter: &SampledMetricsFormatter{
			Mode:          Interval,
			BackfillLimit: 60 * time.Minute,
		},
	},
	{ResourceType: metadata.VolumePoolRegionalHA, MeasuredType: metadata.PoolAllocatedSize}: {
		AggregationType: IntegralAggregation,
		IsBillable:      false,
		TimeSeriesFormatter: &SampledMetricsFormatter{
			Mode:          Interval,
			BackfillLimit: 60 * time.Minute,
		},
	},
	{ResourceType: metadata.VolumePool, MeasuredType: metadata.AllocatedUsed}: {
		AggregationType: IntegralAggregation,
		IsBillable:      false,
		TimeSeriesFormatter: &SampledMetricsFormatter{
			Mode:          Interval,
			BackfillLimit: 60 * time.Minute,
		},
	},
	{ResourceType: metadata.VolumePoolRegionalHA, MeasuredType: metadata.AllocatedUsed}: {
		AggregationType: IntegralAggregation,
		IsBillable:      false,
		TimeSeriesFormatter: &SampledMetricsFormatter{
			Mode:          Interval,
			BackfillLimit: 60 * time.Minute,
		},
	},
	{ResourceType: metadata.VolumePool, MeasuredType: metadata.PoolTotalThroughputMibps}: {
		AggregationType: IntegralAggregation,
		IsBillable:      false,
		TimeSeriesFormatter: &SampledMetricsFormatter{
			Mode:          Interval,
			BackfillLimit: 60 * time.Minute,
		},
	},
	{ResourceType: metadata.VolumePoolRegionalHA, MeasuredType: metadata.PoolTotalThroughputMibps}: {
		AggregationType: IntegralAggregation,
		IsBillable:      false,
		TimeSeriesFormatter: &SampledMetricsFormatter{
			Mode:          Interval,
			BackfillLimit: 60 * time.Minute,
		},
	},
	{ResourceType: metadata.VolumePool, MeasuredType: metadata.PoolTotalIops}: {
		AggregationType: IntegralAggregation,
		IsBillable:      false,
		TimeSeriesFormatter: &SampledMetricsFormatter{
			Mode:          Interval,
			BackfillLimit: 60 * time.Minute,
		},
	},
	{ResourceType: metadata.VolumePoolRegionalHA, MeasuredType: metadata.PoolTotalIops}: {
		AggregationType: IntegralAggregation,
		IsBillable:      false,
		TimeSeriesFormatter: &SampledMetricsFormatter{
			Mode:          Interval,
			BackfillLimit: 60 * time.Minute,
		},
	},
	{ResourceType: metadata.Backup, MeasuredType: metadata.BackupLogicalSize}: {
		AggregationType: IntegralAggregation,
		IsBillable:      true,
		SKU:             BillingMetricNameVolumeBackup,
		TimeSeriesFormatter: &SampledMetricsFormatter{
			Mode:          Interval,
			BackfillLimit: 60 * time.Minute,
		},
	},
	{ResourceType: metadata.Volume, MeasuredType: metadata.BackupEnabledVolumeAllocatedSize}: {
		AggregationType: IntegralAggregation,
		IsBillable:      true,
		SKU:             BillingMetricNameVolumeBackupManagementUsage,
		TimeSeriesFormatter: &SampledMetricsFormatter{
			Mode:          Interval,
			BackfillLimit: 60 * time.Minute,
		},
	},
	{ResourceType: metadata.VolumePoolRegionalHA, MeasuredType: metadata.BackupEnabledVolumeAllocatedSize}: {
		AggregationType: IntegralAggregation,
		IsBillable:      true,
		SKU:             BillingMetricNameVolumeBackupManagementUsage,
		TimeSeriesFormatter: &SampledMetricsFormatter{
			Mode:          Interval,
			BackfillLimit: 60 * time.Minute,
		},
	},
	{ResourceType: metadata.VolumePool, MeasuredType: metadata.CoolTierDataReadSizeRaw}: {
		AggregationType: CounterAggregation,
		IsBillable:      true,
		SKU:             BillingMetricNamePoolColdTierNetworkTransferSize,
		TimeSeriesFormatter: &SampledMetricsFormatter{
			Mode:          Interval,
			BackfillLimit: 60 * time.Minute,
		},
	},
	{ResourceType: metadata.VolumePoolRegionalHA, MeasuredType: metadata.CoolTierDataReadSizeRaw}: {
		AggregationType: CounterAggregation,
		IsBillable:      true,
		SKU:             BillingMetricNamePoolColdTierNetworkTransferSize,
		TimeSeriesFormatter: &SampledMetricsFormatter{
			Mode:          Interval,
			BackfillLimit: 60 * time.Minute,
		},
	},
	{ResourceType: metadata.VolumePool, MeasuredType: metadata.CoolTierDataWriteSizeRaw}: {
		AggregationType: CounterAggregation,
		IsBillable:      true,
		SKU:             BillingMetricNamePoolColdTierNetworkTransferSize,
		TimeSeriesFormatter: &SampledMetricsFormatter{
			Mode:          Interval,
			BackfillLimit: 60 * time.Minute,
		},
	},
	{ResourceType: metadata.VolumePoolRegionalHA, MeasuredType: metadata.CoolTierDataWriteSizeRaw}: {
		AggregationType: CounterAggregation,
		IsBillable:      true,
		SKU:             BillingMetricNamePoolColdTierNetworkTransferSize,
		TimeSeriesFormatter: &SampledMetricsFormatter{
			Mode:          Interval,
			BackfillLimit: 60 * time.Minute,
		},
	},
	{ResourceType: metadata.VolumePool, MeasuredType: metadata.PoolHotTierProvisionedSize}: {
		AggregationType: IntegralAggregation,
		IsBillable:      true,
		SKU:             BillingMetricNamePoolHotTierSize,
		TimeSeriesFormatter: &SampledMetricsFormatter{
			Mode:          Interval,
			BackfillLimit: 60 * time.Minute,
		},
	},
	{ResourceType: metadata.VolumePoolRegionalHA, MeasuredType: metadata.PoolHotTierProvisionedSize}: {
		AggregationType: IntegralAggregation,
		IsBillable:      true,
		SKU:             BillingMetricNamePoolHotTierSize,
		TimeSeriesFormatter: &SampledMetricsFormatter{
			Mode:          Interval,
			BackfillLimit: 60 * time.Minute,
		},
	},
	{ResourceType: metadata.VolumePool, MeasuredType: metadata.PoolCapacityTierLogicalFootprint}: {
		AggregationType: IntegralAggregation,
		IsBillable:      true,
		SKU:             BillingMetricNamePoolColdTierSize,
		TimeSeriesFormatter: &SampledMetricsFormatter{
			Mode:          Interval,
			BackfillLimit: 60 * time.Minute,
		},
	},
	{ResourceType: metadata.VolumePoolRegionalHA, MeasuredType: metadata.PoolCapacityTierLogicalFootprint}: {
		AggregationType: IntegralAggregation,
		IsBillable:      true,
		SKU:             BillingMetricNamePoolColdTierSize,
		TimeSeriesFormatter: &SampledMetricsFormatter{
			Mode:          Interval,
			BackfillLimit: 60 * time.Minute,
		},
	},
}
