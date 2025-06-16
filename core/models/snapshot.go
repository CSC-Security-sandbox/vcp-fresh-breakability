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

type SnapshotPolicy struct {
	Name      string
	Comment   string
	IsEnabled bool
	Schedules []*SnapshotPolicySchedule
}

// SnapshotPolicySchedule describes a snapshot policy schedule in the cloud volume model
type SnapshotPolicySchedule struct {
	Schedule        *Schedule
	Prefix          string
	Count           int64
	SnapmirrorLabel string
}

// Schedule describes a schedule in the cloud volume model
type Schedule struct {
	Name        string
	Description string
	Type        string
	Months      []int
	DaysOfMonth []int
	DaysOfWeek  []int
	Hours       []int
	Minutes     []int
}
