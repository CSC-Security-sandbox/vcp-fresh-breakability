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
	Username       string         `gorm:"column:username"`
	Password       string         `gorm:"column:password"`
}

type ClusterDetails struct {
	ExternalName          string `json:"external_name"`
	OntapVersion          string `json:"ontap_version"`
	RegionalTenantProject string `json:"regional_tenant_project"`
	SnHostProject         string `json:"sn_host_project"`
}

// Node represents the public.nodes table in the database
type Node struct {
	BaseModel
	Name            string       `gorm:"column:name;type:text"`
	Description     string       `gorm:"column:description;type:text"`
	State           string       `gorm:"column:state;type:text"`
	StateDetails    string       `gorm:"column:state_details;type:text"`
	EndpointAddress string       `gorm:"column:endpoint_Address;type:text"`
	NodeAttributes  *NodeDetails `gorm:"column:node_attributes;type:jsonb"`
	PoolID          int64        `gorm:"column:pool_id;type:bigint"`
	ZoneName        string       `gorm:"column:zone_name;type:text"`
	AccountID       int64        `gorm:"column:account_id;type:bigint"`
}

// JSONB is a custom type to handle JSONB columns in PostgreSQL
type JSONB map[string]interface{}

// Scan implements the Scanner interface for JSONB
func (j *JSONB) Scan(value interface{}) error {
	if value == nil {
		*j = make(JSONB) // Initialize to an empty map
		return nil
	}
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}
	return json.Unmarshal(bytes, j)
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
	Name         string      `gorm:"column:name"`
	Description  string      `gorm:"column:description"`
	State        string      `gorm:"column:state"`
	StateDetails string      `gorm:"column:state_details"`
	SvmDetails   *SvmDetails `gorm:"column:svm_details;type:jsonb"`
	AccountID    int64       `gorm:"column:account_id"`
	Account      *Account    `gorm:"ForeignKey:AccountID;AssociationForeignKey:ID;constraint:OnDelete:CASCADE,OnUpdate:RESTRICT;"`
	PoolID       int64       `gorm:"column:pool_id"`
	Pool         *Pool       `gorm:"ForeignKey:PoolID;AssociationForeignKey:ID;constraint:OnDelete:CASCADE,OnUpdate:RESTRICT;"`
	CmekConfigID int64       `gorm:"column:cmek_config_id;type:bigint"`
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
	ID        int64           `json:"id" gorm:"primaryKey"`
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
	BaseModel
	CorrelationID string        `json:"correlationID"`
	RequestID     string        `json:"requestID"`
	Type          string        `json:"type"`
	State         string        `json:"state" gorm:"index"`
	ErrorDetails  []byte        `json:"errorDetails" gorm:"type:bytea"`
	AccountID     sql.NullInt64 `json:"-" gorm:"index"`
	IsAdminJob    bool          `json:"-" gorm:"default:false"`
	JobAttributes JSONB         `gorm:"column:job_attributes;type:jsonb"`
	WorkflowID    string        `json:"workflowID"`
	ScheduledAt   time.Time     `json:"scheduledAt"`
	ResourceName  string        `json:"resourceName"`
}

type Lif struct {
	BaseModel
	Name        string      `gorm:"column:name;type:text"`
	Description string      `gorm:"column:description;type:text"`
	LifDetails  *LifDetails `gorm:"column:lif_details;type:jsonb"`
	AccountID   int64       `gorm:"column:account_id;type:bigint"`
	Type        string      `gorm:"column:type;type:text"`
	IPAddress   string      `gorm:"column:ip_address;type:text"`
	NodeID      int64       `gorm:"column:node_id;type:bigint"`
	SubnetMask  string      `gorm:"column:subnet_mask;type:text"`
}

type SvmDetails struct {
	ExternalUUID           string `json:"external_uuid"`
	IPSpace                string `json:"ip_space"`
	NFSv364BitIdentifiers  string `json:"nf_sv364_bit_identifiers"`
	ExternalCmekConfigUUID string `json:"external_cmek_config_uuid"`
}

func (sd SvmDetails) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}
	return json.Unmarshal(bytes, &sd)
}

func (sd SvmDetails) Value() (driver.Value, error) {
	return json.Marshal(sd)
}

type LifDetails struct {
	ExternalUUID string `json:"external_uuid"`
}

func (nd LifDetails) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}
	return json.Unmarshal(bytes, &nd)
}

func (nd LifDetails) Value() (driver.Value, error) {
	return json.Marshal(nd)
}

type NodeDetails struct {
	ExternalUUID string `json:"external_uuid"`
	InstanceType string `json:"instance_type"`
}

func (nd NodeDetails) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}
	return json.Unmarshal(bytes, &nd)
}

func (nd NodeDetails) Value() (driver.Value, error) {
	return json.Marshal(nd)
}
