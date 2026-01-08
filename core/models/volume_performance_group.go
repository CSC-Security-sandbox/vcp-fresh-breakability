package models

type VolumePerformanceGroup struct {
	BaseModel
	Name            string // resourceId
	ThroughputMibps float32
	Iops            *int32
	IsShared        bool
}
