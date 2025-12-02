package metadata

import (
	"strings"
)

// MeasuredType comment
type MeasuredType string

var CombinedKeyResourceTypeMeasuredTypeMap map[string]CombinedKeyResourceTypeMeasuredType

type CombinedKeyResourceTypeMeasuredType struct {
	ResourceType ResourceType
	MeasuredType MeasuredType
}

func (mt MeasuredType) String() string {
	return string(mt)
}

const (
	UnknownMeasuredType                                  MeasuredType = "UNKNOWN_MEASURED_TYPE"
	PoolAllocatedSize                                    MeasuredType = "POOL_ALLOCATED_SIZE"
	AllocatedUsed                                        MeasuredType = "ALLOCATED_USED"
	PoolTotalThroughputMibps                             MeasuredType = "POOL_TOTAL_THROUGHPUT_MIBPS"
	PoolTotalIops                                        MeasuredType = "POOL_TOTAL_IOPS"
	LogicalSize                                          MeasuredType = "LOGICAL_SIZE"
	SnapshotSize                                         MeasuredType = "SNAPSHOT_SIZE"
	AllocatedSize                                        MeasuredType = "ALLOCATED_SIZE"
	XregionReplicationHealthy                            MeasuredType = "XREGION_REPLICATION_HEALTHY"
	XregionReplicationLagTime                            MeasuredType = "XREGION_REPLICATION_LAG_TIME"
	XregionReplicationLastTransferDuration               MeasuredType = "XREGION_REPLICATION_LAST_TRANSFER_DURATION"
	XregionReplicationLastTransferSize                   MeasuredType = "XREGION_REPLICATION_LAST_TRANSFER_SIZE"
	XregionReplicationRelationshipConcurrentTransferring MeasuredType = "XREGION_REPLICATION_RELATIONSHIP_CONCURRENT_TRANSFERRING"
	XregionReplicationRelationshipProgress               MeasuredType = "XREGION_REPLICATION_RELATIONSHIP_PROGRESS"
	XregionReplicationRelationshipTransferring           MeasuredType = "XREGION_REPLICATION_RELATIONSHIP_TRANSFERRING"
	XregionReplicationReplicationSchedule                MeasuredType = "XREGION_REPLICATION_REPLICATION_SCHEDULE"
	XregionReplicationTotalTransferBytes                 MeasuredType = "XREGION_REPLICATION_TOTAL_TRANSFER_BYTES"
	BackupLogicalSize                                    MeasuredType = "VOLUME_BACKUP_SIZE"
	BackupEnabledVolumeAllocatedSize                     MeasuredType = "BACKUP_ENABLED_VOLUME_ALLOCATED_SIZE"
	TotalLogicalSize                                     MeasuredType = "TOTAL_LOGICAL_SIZE"
	VolumeAllocatedThroughput                            MeasuredType = "VOLUME_ALLOCATED_THROUGHPUT"
	AverageReadLatency                                   MeasuredType = "AVERAGE_READ_LATENCY"
	AverageWriteLatency                                  MeasuredType = "AVERAGE_WRITE_LATENCY"
	AverageOtherLatency                                  MeasuredType = "AVERAGE_OTHER_LATENCY"
	ReadIo                                               MeasuredType = "READ_IO"
	WriteIo                                              MeasuredType = "WRITE_IO"
	OtherIo                                              MeasuredType = "OTHER_IO"
	CoolTier1DataReadSize                                MeasuredType = "COOL_TIER_DATA_READ_SIZE"
	CoolTier1DataWriteSize                               MeasuredType = "COOL_TIER_DATA_WRITE_SIZE"
)

func init() {
	CombinedKeyResourceTypeMeasuredTypeMap = make(map[string]CombinedKeyResourceTypeMeasuredType)
	CombinedKeyResourceTypeMeasuredTypeMap["unknown_measured_type"] = CombinedKeyResourceTypeMeasuredType{
		MeasuredType: UnknownMeasuredType,
	}
	CombinedKeyResourceTypeMeasuredTypeMap["pool_allocated_size"] = CombinedKeyResourceTypeMeasuredType{ResourceType: VolumePool, MeasuredType: PoolAllocatedSize}
	CombinedKeyResourceTypeMeasuredTypeMap["allocated_used"] = CombinedKeyResourceTypeMeasuredType{ResourceType: VolumePool, MeasuredType: AllocatedUsed}
	CombinedKeyResourceTypeMeasuredTypeMap["pool_total_throughput_mibps"] = CombinedKeyResourceTypeMeasuredType{ResourceType: VolumePool, MeasuredType: PoolTotalThroughputMibps}
	CombinedKeyResourceTypeMeasuredTypeMap["pool_total_iops"] = CombinedKeyResourceTypeMeasuredType{ResourceType: VolumePool, MeasuredType: PoolTotalIops}
	CombinedKeyResourceTypeMeasuredTypeMap["pool_allocated_size_regional_ha"] = CombinedKeyResourceTypeMeasuredType{ResourceType: VolumePoolRegionalHA, MeasuredType: PoolAllocatedSize}
	CombinedKeyResourceTypeMeasuredTypeMap["allocated_used_regional_ha"] = CombinedKeyResourceTypeMeasuredType{ResourceType: VolumePoolRegionalHA, MeasuredType: AllocatedUsed}
	CombinedKeyResourceTypeMeasuredTypeMap["pool_total_throughput_mibps_regional_ha"] = CombinedKeyResourceTypeMeasuredType{ResourceType: VolumePoolRegionalHA, MeasuredType: PoolTotalThroughputMibps}
	CombinedKeyResourceTypeMeasuredTypeMap["pool_total_iops_regional_ha"] = CombinedKeyResourceTypeMeasuredType{ResourceType: VolumePoolRegionalHA, MeasuredType: PoolTotalIops}
	CombinedKeyResourceTypeMeasuredTypeMap["volume_space_logical_used"] = CombinedKeyResourceTypeMeasuredType{ResourceType: Volume, MeasuredType: LogicalSize}
	CombinedKeyResourceTypeMeasuredTypeMap["volume_snapshot_reserve_used"] = CombinedKeyResourceTypeMeasuredType{ResourceType: Volume, MeasuredType: SnapshotSize}
	CombinedKeyResourceTypeMeasuredTypeMap["backup_logical_size"] = CombinedKeyResourceTypeMeasuredType{ResourceType: Backup, MeasuredType: BackupLogicalSize}
	CombinedKeyResourceTypeMeasuredTypeMap["backup_volume_allocated_size"] = CombinedKeyResourceTypeMeasuredType{ResourceType: Volume, MeasuredType: BackupEnabledVolumeAllocatedSize}
	CombinedKeyResourceTypeMeasuredTypeMap["backup_volume_allocated_size_regional_ha"] = CombinedKeyResourceTypeMeasuredType{ResourceType: VolumeRegionalHA, MeasuredType: BackupEnabledVolumeAllocatedSize}
	CombinedKeyResourceTypeMeasuredTypeMap["volume_capacity"] = CombinedKeyResourceTypeMeasuredType{ResourceType: Volume, MeasuredType: AllocatedSize}
	CombinedKeyResourceTypeMeasuredTypeMap["snapmirror_total_transfer_bytes"] = CombinedKeyResourceTypeMeasuredType{ResourceType: VolumeReplicationRelationship, MeasuredType: XregionReplicationTotalTransferBytes}
	CombinedKeyResourceTypeMeasuredTypeMap["throughput_limit"] = CombinedKeyResourceTypeMeasuredType{ResourceType: Volume, MeasuredType: VolumeAllocatedThroughput}
	CombinedKeyResourceTypeMeasuredTypeMap["throughput_limit_regional_ha"] = CombinedKeyResourceTypeMeasuredType{ResourceType: VolumeRegionalHA, MeasuredType: VolumeAllocatedThroughput}
	CombinedKeyResourceTypeMeasuredTypeMap["volume_read_latency"] = CombinedKeyResourceTypeMeasuredType{ResourceType: Volume, MeasuredType: AverageReadLatency}
	CombinedKeyResourceTypeMeasuredTypeMap["volume_write_latency"] = CombinedKeyResourceTypeMeasuredType{ResourceType: Volume, MeasuredType: AverageWriteLatency}
	CombinedKeyResourceTypeMeasuredTypeMap["volume_other_latency"] = CombinedKeyResourceTypeMeasuredType{ResourceType: Volume, MeasuredType: AverageOtherLatency}
	CombinedKeyResourceTypeMeasuredTypeMap["volume_read_data"] = CombinedKeyResourceTypeMeasuredType{ResourceType: Volume, MeasuredType: ReadIo}
	CombinedKeyResourceTypeMeasuredTypeMap["volume_write_data"] = CombinedKeyResourceTypeMeasuredType{ResourceType: Volume, MeasuredType: WriteIo}
	CombinedKeyResourceTypeMeasuredTypeMap["volume_other_data"] = CombinedKeyResourceTypeMeasuredType{ResourceType: Volume, MeasuredType: OtherIo}
	CombinedKeyResourceTypeMeasuredTypeMap["wafl_volume_client_protocol_reads"] = CombinedKeyResourceTypeMeasuredType{ResourceType: Volume, MeasuredType: CoolTier1DataReadSize}
	CombinedKeyResourceTypeMeasuredTypeMap["wafl_volume_cloud_bin_operation_size"] = CombinedKeyResourceTypeMeasuredType{ResourceType: Volume, MeasuredType: CoolTier1DataWriteSize}
	CombinedKeyResourceTypeMeasuredTypeMap["pool_client_protocol_reads"] = CombinedKeyResourceTypeMeasuredType{ResourceType: VolumePool, MeasuredType: CoolTier1DataReadSize}
	CombinedKeyResourceTypeMeasuredTypeMap["pool_cloud_bin_operation_size"] = CombinedKeyResourceTypeMeasuredType{ResourceType: VolumePool, MeasuredType: CoolTier1DataWriteSize}
}

// NewMeasuredType takes a string and converts it to the defined MeasuredType. If the string is not in the map of available measured types, exists is false and the result is nil.
// If the input string is a legal measured type, the result is the measured type for that string and exists is true.
func NewMeasuredType(input string) (MeasuredType, bool) {
	var result MeasuredType
	combined, exists := CombinedKeyResourceTypeMeasuredTypeMap[strings.ToLower(input)]
	if exists {
		return combined.MeasuredType, exists
	}
	return result, exists
}
