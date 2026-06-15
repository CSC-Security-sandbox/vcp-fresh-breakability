package models

const (
	AllocationTypeShared    = "SHARED"
	AllocationTypePerVolume = "PER_VOLUME"
)

type VolumePerformanceGroup struct {
	BaseModel
	Name                  string // resourceId
	PoolID                string
	ThroughputMibps       int64
	Iops                  int64
	AllocationType        string
	Description           string
	LifeCycleState        string
	LifeCycleStateDetails string
	Labels                map[string]string
}

func (v *VolumePerformanceGroup) IsShared() bool {
	return v != nil && v.AllocationType == AllocationTypeShared
}

func (v *VolumePerformanceGroup) IsPerVolume() bool {
	return v != nil && v.AllocationType == AllocationTypePerVolume
}
