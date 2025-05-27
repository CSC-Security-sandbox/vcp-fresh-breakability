package models

type Snapshot struct {
	BaseModel
	AccountName           string
	Name                  string
	Description           string
	VolumeUUID            string
	VolumeName            string
	SizeInBytes           uint64
	LifeCycleState        string
	LifeCycleStateDetails string
}
