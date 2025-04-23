package datamodel

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"time"

	"gorm.io/gorm"
)

type Pool struct {
	BaseModel
	Name           string         `gorm:"column:name"`
	Description    string         `gorm:"column:description"`
	State          string         `gorm:"column:state"`
	StateDetails   string         `gorm:"column:state_details"`
	VendorID       string         `gorm:"column:vendor_id"`
	ServiceLevel   string         `gorm:"column:service_level"`
	SizeInBytes    int64          `gorm:"column:size_in_bytes"`
	UsedBytes      int64          `gorm:"column:used_bytes"`
	Network        string         `gorm:"column:network;type:varchar(2048)"`
	CoolAccess     bool           `gorm:"column:cool_access;default:false"`
	AccountID      int64          `gorm:"column:account_id"`
	Account        *Account       `gorm:"ForeignKey:AccountID;AssociationForeignKey:ID;constraint:OnDelete:CASCADE,OnUpdate:RESTRICT;"`
	PoolAttributes JSONB          `gorm:"column:pool_attributes;type:jsonb"`
	ClusterDetails ClusterDetails `gorm:"column:cluster_details;type:jsonb"`
}

type ClusterDetails struct {
	ExternalName string `json:"external_name"`
	OntapVersion string `json:"ontap_version"`
	Nodes        []Node `json:"nodes"`
}

type Node struct {
	InstanceType      string `json:"instance_type"`
	ExternalIpAddress string `json:"external_ip_address"`
	InternalIpAddress string `json:"internal_ip_address"`
}

// JSONB is a custom type to handle JSONB columns in PostgreSQL
type JSONB map[string]interface{}

// Scan implements the Scanner interface for JSONB
func (j *JSONB) Scan(value interface{}) error {
	return json.Unmarshal(value.([]byte), j)
}

// Value implements the Valuer interface for JSONB
func (j JSONB) Value() (driver.Value, error) {
	return json.Marshal(j)
}

// Scan implements the sql.Scanner interface for ClusterDetails
func (cd *ClusterDetails) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}
	return json.Unmarshal(bytes, cd)
}

// Value implements the driver.Valuer interface for ClusterDetails
func (cd ClusterDetails) Value() (driver.Value, error) {
	return json.Marshal(cd)
}

type Volume struct {
	BaseModel
	Name             string   `gorm:"column:name"`
	Description      string   `gorm:"column:description"`
	State            string   `gorm:"column:state"`
	StateDetails     string   `gorm:"column:state_details"`
	VolumeAttributes JSONB    `gorm:"column:volume_attributes;type:jsonb"`
	AccountID        int64    `gorm:"column:account_id"`
	Account          *Account `gorm:"ForeignKey:AccountID;AssociationForeignKey:ID;constraint:OnDelete:CASCADE,OnUpdate:RESTRICT;"`
	PoolID           int64    `gorm:"column:pool_id"`
	Pool             *Pool    `gorm:"ForeignKey:PoolID;AssociationForeignKey:ID;constraint:OnDelete:CASCADE,OnUpdate:RESTRICT;"`
}

type Svm struct {
	BaseModel
	Name          string   `gorm:"column:name"`
	Description   string   `gorm:"column:description"`
	State         string   `gorm:"column:state"`
	StateDetails  string   `gorm:"column:state_details"`
	SvmAttributes JSONB    `gorm:"column:svm_attributes;type:jsonb"`
	AccountID     int64    `gorm:"column:account_id"`
	Account       *Account `gorm:"ForeignKey:AccountID;AssociationForeignKey:ID;constraint:OnDelete:CASCADE,OnUpdate:RESTRICT;"`
	PoolID        int64    `gorm:"column:pool_id"`
	Pool          *Pool    `gorm:"ForeignKey:PoolID;AssociationForeignKey:ID;constraint:OnDelete:CASCADE,OnUpdate:RESTRICT;"`
}

type Account struct {
	BaseModel
	Name         string `gorm:"column:name"`
	Description  string `gorm:"column:description"`
	State        string `json:"state"`
	StateDetails string `gorm:"column:state_details"`
	Tags         string `json:"tags" gorm:"type:text"`
}

// BaseModel describes the base model shared by all other database models
type BaseModel struct {
	ID        int64           `json:"-" gorm:"primaryKey"`
	UUID      string          `json:"uuid" gorm:"unique"`
	CreatedAt time.Time       `json:"createdAt"`
	UpdatedAt time.Time       `json:"updatedAt"`
	DeletedAt *gorm.DeletedAt `gorm:"index" json:"deletedAt"`
}

// GetID returns a sql.NullInt64 representation of the base model's ID
func (bm *BaseModel) GetID() (id sql.NullInt64) {
	if bm.ID > 0 {
		id.Int64 = bm.ID
		id.Valid = true
	}
	return
}

// Job is a struct that represents the job data model.
type Job struct {
	ID string `json:"uuid" gorm:"unique"`
	// workflowID string    `db:"workflow_id" bson:"workflow_id"`
	CustomerID string    `gorm:"type:varchar"`
	Status     string    `gorm:"type:varchar"`
	CreatedAt  time.Time `json:"createdAt"`
}
