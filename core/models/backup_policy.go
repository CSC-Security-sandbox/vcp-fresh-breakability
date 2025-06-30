package models

import "time"

type BackupPolicy struct {
	ResourceID         string    `json:"resourceId"`
	BackupPolicyUUID   string    `json:"backupPolicyId"`
	DailyBackupLimit   int64     `json:"dailyBackupLimit"`
	WeeklyBackupLimit  int64     `json:"weeklyBackupLimit"`
	MonthlyBackupLimit int64     `json:"monthlyBackupLimit"`
	Enabled            bool      `json:"enabled"`
	Description        *string   `json:"description"`
	State              string    `json:"state"`
	CreatedAt          time.Time `json:"createdAt"`
}
