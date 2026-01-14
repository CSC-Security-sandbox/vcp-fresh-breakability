package models

type VolumePerformanceGroup struct {
	BaseModel
	Name            string // resourceId
	PoolID          string
	ThroughputMibps int64
	Iops            int64
	IsShared        bool
}
