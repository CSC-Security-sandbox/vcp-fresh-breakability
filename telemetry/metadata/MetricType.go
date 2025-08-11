package metadata

import (
	"strings"
)

type CombinedKeyResourceTypeMeasuredType struct {
	ResourceType ResourceType
	MeasuredType MeasuredType
}

// MeasuredType comment
type MeasuredType string

var CombinedKeyResourceTypeMeasuredTypeMap map[string]CombinedKeyResourceTypeMeasuredType

func (mt MeasuredType) String() string {
	return string(mt)
}

const (
	UnknownMeasuredType      MeasuredType = "UNKNOWN_MEASURED_TYPE"
	PoolAllocatedSize        MeasuredType = "POOL_ALLOCATED_SIZE"
	FileSystemReadOps        MeasuredType = "FILE_SYSTEM_READ_OPS"
	FileSystemWriteOps       MeasuredType = "FILE_SYSTEM_WRITE_OPS"
	FileSystemOtherOps       MeasuredType = "FILE_SYSTEM_OTHER_OPS"
	ReadIo                   MeasuredType = "READ_IO"
	WriteIo                  MeasuredType = "WRITE_IO"
	OtherIo                  MeasuredType = "OTHER_IO"
	AverageReadLatency       MeasuredType = "AVERAGE_READ_LATENCY"
	AverageWriteLatency      MeasuredType = "AVERAGE_WRITE_LATENCY"
	AverageOtherLatency      MeasuredType = "AVERAGE_OTHER_LATENCY"
	VolumeAllcatedThroughput MeasuredType = "VOLUME_ALLOCATED_THROUGHPUT"
	LogicalSize              MeasuredType = "LOGICAL_SIZE"
	SnapshotSize             MeasuredType = "SNAPSHOT_SIZE"
	AllocatedSize            MeasuredType = "ALLOCATED_SIZE"
	VolumeInodeTotal         MeasuredType = "VOLUME_INODE_TOTAL"
	VolumeInodesUsed         MeasuredType = "VOLUME_INODES_USED"
)

func init() {
	CombinedKeyResourceTypeMeasuredTypeMap = make(map[string]CombinedKeyResourceTypeMeasuredType)
	CombinedKeyResourceTypeMeasuredTypeMap["unknown_measured_type"] = CombinedKeyResourceTypeMeasuredType{
		MeasuredType: UnknownMeasuredType,
	}
	CombinedKeyResourceTypeMeasuredTypeMap["pool_allocated_size"] = CombinedKeyResourceTypeMeasuredType{ResourceType: VolumePool, MeasuredType: PoolAllocatedSize}
	CombinedKeyResourceTypeMeasuredTypeMap["volume_read_ops"] = CombinedKeyResourceTypeMeasuredType{ResourceType: Volume, MeasuredType: FileSystemReadOps}
	CombinedKeyResourceTypeMeasuredTypeMap["volume_write_ops"] = CombinedKeyResourceTypeMeasuredType{ResourceType: Volume, MeasuredType: FileSystemWriteOps}
	CombinedKeyResourceTypeMeasuredTypeMap["volume_other_ops"] = CombinedKeyResourceTypeMeasuredType{ResourceType: Volume, MeasuredType: FileSystemOtherOps}
	CombinedKeyResourceTypeMeasuredTypeMap["volume_read_data"] = CombinedKeyResourceTypeMeasuredType{ResourceType: Volume, MeasuredType: ReadIo}
	CombinedKeyResourceTypeMeasuredTypeMap["volume_write_data"] = CombinedKeyResourceTypeMeasuredType{ResourceType: Volume, MeasuredType: WriteIo}
	CombinedKeyResourceTypeMeasuredTypeMap["volume_other_data"] = CombinedKeyResourceTypeMeasuredType{ResourceType: Volume, MeasuredType: OtherIo}
	CombinedKeyResourceTypeMeasuredTypeMap["volume_read_latency"] = CombinedKeyResourceTypeMeasuredType{ResourceType: Volume, MeasuredType: AverageReadLatency}
	CombinedKeyResourceTypeMeasuredTypeMap["volume_write_latency"] = CombinedKeyResourceTypeMeasuredType{ResourceType: Volume, MeasuredType: AverageWriteLatency}
	CombinedKeyResourceTypeMeasuredTypeMap["volume_other_latency"] = CombinedKeyResourceTypeMeasuredType{ResourceType: Volume, MeasuredType: AverageOtherLatency}
	CombinedKeyResourceTypeMeasuredTypeMap["volume_total_data"] = CombinedKeyResourceTypeMeasuredType{ResourceType: Volume, MeasuredType: VolumeAllcatedThroughput}
	CombinedKeyResourceTypeMeasuredTypeMap["volume_size_used"] = CombinedKeyResourceTypeMeasuredType{ResourceType: Volume, MeasuredType: LogicalSize}
	CombinedKeyResourceTypeMeasuredTypeMap["volume_snapshot_reserve_used"] = CombinedKeyResourceTypeMeasuredType{ResourceType: Volume, MeasuredType: SnapshotSize}
	CombinedKeyResourceTypeMeasuredTypeMap["volume_size_total"] = CombinedKeyResourceTypeMeasuredType{ResourceType: Volume, MeasuredType: AllocatedSize}
	CombinedKeyResourceTypeMeasuredTypeMap["volume_inode_files_total"] = CombinedKeyResourceTypeMeasuredType{ResourceType: Volume, MeasuredType: VolumeInodeTotal}
	CombinedKeyResourceTypeMeasuredTypeMap["volume_inode_files_used"] = CombinedKeyResourceTypeMeasuredType{ResourceType: Volume, MeasuredType: VolumeInodesUsed}
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
