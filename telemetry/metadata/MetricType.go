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
	CbsVolumeBackupSize                                  MeasuredType = "CBS_VOLUME_BACKUP_SIZE"
	TotalLogicalSize                                     MeasuredType = "TOTAL_LOGICAL_SIZE"
)

func init() {
	CombinedKeyResourceTypeMeasuredTypeMap = make(map[string]CombinedKeyResourceTypeMeasuredType)
	CombinedKeyResourceTypeMeasuredTypeMap["unknown_measured_type"] = CombinedKeyResourceTypeMeasuredType{
		MeasuredType: UnknownMeasuredType,
	}
	CombinedKeyResourceTypeMeasuredTypeMap["pool_allocated_size"] = CombinedKeyResourceTypeMeasuredType{ResourceType: VolumePool, MeasuredType: PoolAllocatedSize}
	CombinedKeyResourceTypeMeasuredTypeMap["allocated_used"] = CombinedKeyResourceTypeMeasuredType{ResourceType: VolumePool, MeasuredType: AllocatedUsed}
	CombinedKeyResourceTypeMeasuredTypeMap["volume_space_logical_used"] = CombinedKeyResourceTypeMeasuredType{ResourceType: Volume, MeasuredType: LogicalSize}
	CombinedKeyResourceTypeMeasuredTypeMap["volume_snapshot_reserve_used"] = CombinedKeyResourceTypeMeasuredType{ResourceType: Volume, MeasuredType: SnapshotSize}
	CombinedKeyResourceTypeMeasuredTypeMap["volume_capacity"] = CombinedKeyResourceTypeMeasuredType{ResourceType: Volume, MeasuredType: AllocatedSize}
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
