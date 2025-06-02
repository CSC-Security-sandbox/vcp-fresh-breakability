package datamodel

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

type KmsConfig struct {
	BaseModel
	Name              string          `gorm:"column:name"`
	Description       string          `gorm:"column:description"`
	State             string          `gorm:"column:state"`
	StateDetails      string          `gorm:"column:state_details"`
	KeyRing           string          `gorm:"column:key_ring"`
	KeyRingLocation   string          `gorm:"column:key_ring_location"`
	KeyName           string          `gorm:"column:key_name"`
	AccountID         int64           `gorm:"column:account_id"`
	Account           *Account        `gorm:"ForeignKey:AccountID;AssociationForeignKey:ID;OnUpdate:RESTRICT;"`
	CustomerProjectID string          `gorm:"column:customer_project_id"`
	KeyProjectID      string          `gorm:"column:key_project_id"`
	ResourceID        string          `gorm:"column:resource_id"`
	ServiceAccountID  int64           `gorm:"column:service_account_id"`
	ServiceAccount    *ServiceAccount `gorm:"ForeignKey:ServiceAccountID;AssociationForeignKey:ID;constraint:OnUpdate:RESTRICT;"`
	KmsAttributes     *KmsAttributes  `gorm:"column:kms_attributes;type:jsonb"`
}

type KmsAttributes struct {
	SdeKmsConfigUUID       string `json:"sde_kms_config_uuid"`
	SdeServiceAccountEmail string `json:"sde_service_account_email"`
	Instructions           string `json:"instructions"`
}

func (kmsAttributes *KmsAttributes) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New(fmt.Sprint("Type assertion to []byte failed for KMS attribute value: ", value))
	}
	return json.Unmarshal(bytes, &kmsAttributes)
}

func (kmsAttributes KmsAttributes) Value() (driver.Value, error) {
	return json.Marshal(kmsAttributes)
}

type ServiceAccount struct {
	BaseModel
	Name                           string                    `gorm:"column:name"`
	Description                    string                    `gorm:"column:description"`
	State                          string                    `gorm:"column:state"`
	StateDetails                   string                    `gorm:"column:state_details"`
	AccountID                      int64                     `gorm:"column:account_id"`
	ServiceName                    string                    `gorm:"column:service_name"`
	ServiceAccountEmail            string                    `gorm:"column:service_account_email"`
	ServiceAccountPasswordLocation string                    `gorm:"column:service_account_password_location"`
	ServiceAccountAttributes       *ServiceAccountAttributes `gorm:"column:service_account_attributes;type:jsonb"`
}

type ServiceAccountAttributes struct {
	// For future additions
}

func (saAttributes *ServiceAccountAttributes) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New(fmt.Sprint("Type assertion to []byte failed for Service account value: ", value))
	}
	return json.Unmarshal(bytes, &saAttributes)
}

func (saAttributes ServiceAccountAttributes) Value() (driver.Value, error) {
	return json.Marshal(saAttributes)
}
