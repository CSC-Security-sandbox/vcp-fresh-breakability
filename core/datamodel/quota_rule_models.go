package datamodel

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
)

type QuotaRule struct {
	BaseModel
	Name                string               `gorm:"column:name"`
	Description         string               `gorm:"column:description"`
	State               string               `gorm:"column:state"`
	StateDetails        string               `gorm:"column:state_details"`
	AccountID           int64                `gorm:"column:account_id"`
	VolumeID            int64                `gorm:"column:volume_id"`
	QuotaType           string               `gorm:"column:quota_type"`
	QuotaTarget         string               `gorm:"column:quota_target"`
	DiskLimitInKib      int64                `gorm:"column:disk_limit_in_kib"`
	RQuota              bool                 `gorm:"column:r_quota"`
	QuotaRuleAttributes *QuotaRuleAttributes `gorm:"column:quota_rule_attributes;type:jsonb"`
}

type QuotaRuleAttributes struct {
	ExternalUUID string `json:"external_uuid"`
}

func (v *QuotaRuleAttributes) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}
	return json.Unmarshal(bytes, v)
}

func (v *QuotaRuleAttributes) Value() (driver.Value, error) {
	return json.Marshal(v)
}
