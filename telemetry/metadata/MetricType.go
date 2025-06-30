package metadata

import "strings"

// MeasuredType comment
type MeasuredType string

var MeasuredTypeMap map[string]MeasuredType

func (mt MeasuredType) String() string {
	return string(mt)
}

const (
	UnknownMeasuredType MeasuredType = "UNKNOWN_MEASURED_TYPE"
	PoolAllocatedSize   MeasuredType = "POOL_ALLOCATED_SIZE"
)

func init() {
	MeasuredTypeMap = make(map[string]MeasuredType)
	MeasuredTypeMap["unknown_measured_type"] = UnknownMeasuredType
	MeasuredTypeMap["pool_allocated_size"] = PoolAllocatedSize
}

// NewMeasuredType takes a string and converts it to the defined MeasuredType. If the string is not in the map of available measured types, exists is false and the result is nil.
// If the input string is a legal measured type, the result is the measured type for that string and exists is true.
func NewMeasuredType(input string) (MeasuredType, bool) {
	var result MeasuredType
	s, exists := MeasuredTypeMap[strings.ToLower(input)]
	if exists {
		return s, exists
	}
	return result, exists
}
