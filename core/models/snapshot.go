package models

import "time"

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
	StorageClass          string
	IsAppConsistent       bool
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

// HydrateSnapshot describes a snapshot in the hydrate message to CCFE
type HydrateSnapshot struct {
	ResourceId   string    `json:"resource_id"`
	SnapshotId   string    `json:"snapshot_id"`
	State        string    `json:"snapshot_state"`
	StateDetails string    `json:"snapshot_state_details"`
	Description  string    `json:"description"`
	UsedBytes    int64     `json:"used_bytes"`
	CreateTime   time.Time `json:"created"`
	VolumeName   string    `json:"-"`
	AccountName  string    `json:"-"`
}
