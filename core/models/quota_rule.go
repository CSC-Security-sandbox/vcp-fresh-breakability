package models

// QuotaRule represents a quota rule for volume usage limits
type QuotaRule struct {
	BaseModel
	Name                  string
	QuotaType             string
	DiskLimitInMib        int64
	QuotaTarget           string
	VolumeUUID            string
	LifeCycleState        string
	LifeCycleStateDetails string
	Description           string
}
