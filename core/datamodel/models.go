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
	Name                    string          `gorm:"column:name"`
	Description             string          `gorm:"column:description"`
	State                   string          `gorm:"column:state"`
	StateDetails            string          `gorm:"column:state_details"`
	VendorID                string          `gorm:"column:vendor_id"`
	ServiceLevel            string          `gorm:"column:service_level"`
	SizeInBytes             int64           `gorm:"column:size_in_bytes"`
	UsedBytes               int64           `gorm:"column:used_bytes"`
	Network                 string          `gorm:"column:network;type:varchar(2048)"`
	AllowAutoTiering        bool            `gorm:"column:allow_auto_tiering;default:false"`
	HotTierSizeInBytes      int64           `gorm:"column:hot_tier_size_in_bytes"`
	EnableHotTierAutoResize bool            `gorm:"column:enable_hot_tier_auto_resize;default:false"`
	AccountID               int64           `gorm:"column:account_id"`
	Account                 *Account        `gorm:"ForeignKey:AccountID;AssociationForeignKey:ID;constraint:OnDelete:CASCADE,OnUpdate:RESTRICT;"`
	PoolAttributes          *PoolAttributes `gorm:"column:pool_attributes;type:jsonb"`
	ClusterDetails          ClusterDetails  `gorm:"column:cluster_details;type:jsonb"`
	QosType                 string          `gorm:"column:qos_type"`
	Username                string          `gorm:"column:username"`
	Password                string          `gorm:"column:password"`
	AutoTierBucketName      string          `gorm:"column:auto_tier_bucket_name;type:text"`
	ServiceAccountId        string          `gorm:"column:service_account_id;type:text"`
}

type PoolView struct {
	Pool
	Throughput   float64 `json:"throughput"`
	QuotaInBytes uint64  `json:"quotaInBytes"`
	VolumeCount  int64   `json:"volumeCount"`
}

type ClusterDetails struct {
	ExternalName          string `json:"external_name"`
	OntapVersion          string `json:"ontap_version"`
	RegionalTenantProject string `json:"regional_tenant_project"`
	SnHostProject         string `json:"sn_host_project"`
	Network               string `json:"network"`
}

type PoolAttributes struct {
	ThroughputMibps int64  `json:"throughput"`
	Iops            int64  `json:"iops"`
	PrimaryZone     string `json:"primary_zone"`
	SecondaryZone   string `json:"secondary_zone"`
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

// Scan implements the Scanner interface for PoolAttributes
func (pa *PoolAttributes) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}
	return json.Unmarshal(bytes, pa)
}

// Value implements the Valuer interface for PoolAttributes
func (pa PoolAttributes) Value() (driver.Value, error) {
	return json.Marshal(pa)
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
	Name             string            `gorm:"column:name"`
	Description      string            `gorm:"column:description"`
	State            string            `gorm:"column:state"`
	StateDetails     string            `gorm:"column:state_details"`
	Health           string            `gorm:"column:health"`
	MountPath        string            `gorm:"column:mount_path"`
	SizeInBytes      int64             `gorm:"column:size_in_bytes"`
	Throughput       int64             `gorm:"column:throughput"`
	AccountID        int64             `gorm:"column:account_id"`
	PoolID           int64             `gorm:"column:pool_id"`
	SvmID            int64             `gorm:"column:svm_id"`
	Account          *Account          `gorm:"ForeignKey:AccountID;AssociationForeignKey:ID;constraint:OnDelete:CASCADE,OnUpdate:RESTRICT;"`
	Pool             *Pool             `gorm:"ForeignKey:PoolID;AssociationForeignKey:ID;constraint:OnDelete:CASCADE,OnUpdate:RESTRICT;"`
	Svm              *Svm              `gorm:"ForeignKey:SvmID;AssociationForeignKey:ID;constraint:OnDelete:CASCADE,OnUpdate:RESTRICT;"`
	VolumeAttributes *VolumeAttributes `gorm:"column:volume_attributes;type:jsonb"`
	DataProtection   *DataProtection   `gorm:"column:data_protection;type:jsonb"`
	SnapshotPolicy   *SnapshotPolicy   `gorm:"column:snapshot_policy;type:jsonb"`
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

type VolumeAttributes struct {
	CreationToken    string           `json:"creation_token"`
	Protocols        []string         `json:"protocols"`
	VendorSubnetID   string           `json:"vendor_subnet_id"`
	ExternalUUID     string           `json:"external_uuid"`
	BlockProperties  *BlockProperties `json:"block_properties"`
	IsDataProtection bool             `json:"is_data_protection"`
	Labels           *JSONB           `json:"labels"`
}

type BlockProperties struct {
	OSType          string   `json:"os_type"`
	HostGroupUUIDs  []string `json:"host_group_uuids"`
	LunSerialNumber string   `json:"serial_number"`
	LunUUID         string   `json:"lun_uuid"`
}

func (v *VolumeAttributes) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}
	return json.Unmarshal(bytes, v)
}

func (v *VolumeAttributes) Value() (driver.Value, error) {
	return json.Marshal(v)
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
	CorrelationID string         `json:"correlationID"`
	RequestID     string         `json:"requestID"`
	Type          string         `json:"type"`
	State         string         `json:"state" gorm:"index"`
	TrackingID    int            `json:"trackingID"`
	ErrorDetails  []byte         `json:"errorDetails" gorm:"type:bytea"`
	AccountID     sql.NullInt64  `json:"-" gorm:"index"`
	IsAdminJob    bool           `json:"-" gorm:"default:false"`
	WorkflowID    string         `json:"workflowID"`
	ScheduledAt   time.Time      `json:"scheduledAt"`
	ResourceName  string         `json:"resourceName"`
	JobAttributes *JobAttributes `gorm:"column:job_attributes;type:jsonb"`
}

type JobAttributes struct {
	ResourceUUID string `json:"resource_uuid"`
	PoolUUID     string `json:"pool_uuid"`
}

// Scan method for JobAttributes to handle JSONB data
func (ka *JobAttributes) Scan(value interface{}) error {
	if value == nil {
		*ka = JobAttributes{}
		return nil
	}

	b, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}

	return json.Unmarshal(b, ka)
}

// Value method for JobAttributes to handle JSONB data
func (ka JobAttributes) Value() (driver.Value, error) {
	return json.Marshal(ka)
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

type VolumeReplication struct {
	BaseModel
	Name                  string              `gorm:"column:name"`
	Description           string              `gorm:"column:description"`
	State                 string              `gorm:"column:state"`
	StateDetails          string              `gorm:"column:state_details"`
	Uri                   string              `gorm:"column:uri"`
	RemoteUri             string              `gorm:"column:remote_uri"`
	ReplicationAttributes *ReplicationDetails `gorm:"column:replication_attributes;type:jsonb"`
	MirrorState           *string             `gorm:"column:mirror_state"`
	RelationshipStatus    *string             `gorm:"column:relationship_status"`
	TotalProgress         int64               `gorm:"column:total_progress"`
	TotalTransferBytes    int64               `gorm:"column:total_transfer_bytes"`
	TotalTransferTimeSecs int64               `gorm:"column:total_transfer_time_secs"`
	LastTransferSize      int64               `gorm:"column:last_transfer_size"`
	LastTransferError     string              `gorm:"column:last_transfer_error"`
	LastTransferDuration  int64               `gorm:"column:last_transfer_duration"`
	LastTransferEndTime   *time.Time          `gorm:"column:last_transfer_end_time"`
	ProgressLastUpdated   *time.Time          `gorm:"column:progress_last_updated"`
	LastUpdatedFromOntap  time.Time           `gorm:"column:last_updated_from_ontap"`
	Healthy               bool                `gorm:"column:healthy;default:true"`
	UnhealthyReason       string              `gorm:"column:unhealthy_reason"`
	LagTime               int64               `gorm:"column:lag_time"`
	AccountID             int64               `gorm:"column:account_id"`
	Account               *Account            `gorm:"ForeignKey:AccountID;AssociationForeignKey:ID;constraint:OnDelete:CASCADE,OnUpdate:RESTRICT;"`
	VolumeID              int64               `gorm:"column:volume_id"`
	Volume                *Volume             `gorm:"ForeignKey:VolumeID;AssociationForeignKey:ID;constraint:OnDelete:CASCADE,OnUpdate:RESTRICT;"`
}

type ReplicationDetails struct {
	EndpointType               string `json:"endpoint_type"`
	ReplicationType            string `json:"replication_type"`
	ReplicationSchedule        string `json:"replication_schedule"`
	SourcePoolUUID             string `json:"source_pool_uuid"`
	SourceVolumeUUID           string `json:"source_volume_uuid"`
	SourceLocation             string `json:"source_location"`
	SourceHostName             string `json:"source_host_name"`
	SourceReplicationUUID      string `json:"source_replication_uuid"`
	SourceSvmName              string `json:"source_svm_name"`
	SourceVolumeName           string `json:"source_volume_name"`
	DestinationPoolUUID        string `json:"destination_pool_uuid"`
	DestinationVolumeUUID      string `json:"destination_volume_uuid"`
	DestinationLocation        string `json:"destination_location"`
	DestinationHostName        string `json:"destination_host_name"`
	DestinationReplicationUUID string `json:"destination_replication_uuid"`
	DestinationSvmName         string `json:"destination_svm_name"`
	DestinationVolumeName      string `json:"destination_volume_name"`
	ExternalUUID               string `json:"external_uuid"`
}

func (rd *ReplicationDetails) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}
	return json.Unmarshal(bytes, rd)
}

func (rd ReplicationDetails) Value() (driver.Value, error) {
	return json.Marshal(rd)
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

type HostGroup struct {
	BaseModel
	Name          string   `gorm:"column:name"`
	Description   string   `gorm:"column:description"`
	HostGroupType string   `gorm:"column:host_group_type"`
	OSType        string   `gorm:"column:os_type"`
	Hosts         Hosts    `gorm:"column:hosts;type:jsonb"`
	State         string   `gorm:"column:state"`
	StateDetails  string   `gorm:"column:state_details"`
	AccountID     int64    `gorm:"column:account_id"`
	Account       *Account `gorm:"ForeignKey:AccountID;AssociationForeignKey:ID;constraint:OnDelete:CASCADE,OnUpdate:RESTRICT;"`
}

type Hosts struct {
	Hosts []string `json:"hosts"`
}

func (h *Hosts) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}
	return json.Unmarshal(bytes, &h)
}

func (h Hosts) Value() (driver.Value, error) {
	return json.Marshal(h)
}

// BackupVault represents the backup vault entity with associated attributes and relationships.
type BackupVault struct {
	BaseModel
	Name                       string               `json:"name" gorm:"index"`
	Account                    *Account             `json:"-" gorm:"ForeignKey:AccountID;AssociationForeignKey:ID;constraint:OnDelete:RESTRICT,OnUpdate:RESTRICT;"`
	AccountID                  int64                `gorm:"column:account_id"`
	RegionName                 string               `json:"regionName" gorm:"-"`
	BackupRegionName           *string              `json:"backupRegionName" gorm:"type:text"`
	SourceRegionName           *string              `json:"sourceRegionName" gorm:"type:text"`
	LifeCycleState             string               `json:"lifeCycleState"`
	LifeCycleStateDetails      string               `json:"lifeCycleStateDetails" gorm:"type:text"`
	BackupVaultType            string               `json:"backupVaultType" gorm:"type:varchar(255)"`
	AccountVendorID            string               `json:"accountVendorID"`
	Description                *string              `json:"description" gorm:"type:text"`
	ImmutableAttributes        *ImmutableAttributes `gorm:"column:immutable_attributes;type:jsonb"`
	CrossRegionBackupVaultName *string              `json:"crossRegionBackupVaultName" gorm:"type:text"`
	BucketDetails              BucketDetailsArray   `gorm:"column:bucket_details;type:jsonb"`
}

type BucketDetails struct {
	BucketName          string `json:"bucket_name"`
	ServiceAccountName  string `json:"service_account_name"`
	VendorSubnetID      string `json:"vendor_subnet_id"`
	TenantProjectNumber string `json:"tenant_project_number"`
}

type BucketDetailsArray []*BucketDetails

func (b *BucketDetailsArray) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}
	return json.Unmarshal(bytes, b)
}

func (b BucketDetailsArray) Value() (driver.Value, error) {
	return json.Marshal(b)
}

type ImmutableAttributes struct {
	BackupMinimumEnforcedRetentionDuration *int64 `json:"backupMinimumEnforcedRetentionDuration" gorm:"default:0"`
	IsDailyBackupImmutable                 bool   `json:"isDailyBackupImmutable" gorm:"default:false"`
	IsWeeklyBackupImmutable                bool   `json:"isWeeklyBackupImmutable" gorm:"default:false"`
	IsMonthlyBackupImmutable               bool   `json:"isMonthlyBackupImmutable" gorm:"default:false"`
	IsAdhocBackupImmutable                 bool   `json:"isAdhocBackupImmutable" gorm:"default:false"`
}

// Scan implements the sql.Scanner interface for ImmutableAttributes
func (immutableAttributes *ImmutableAttributes) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}
	return json.Unmarshal(bytes, immutableAttributes)
}

// Value implements the driver.Valuer interface for ImmutableAttributes
func (immutableAttributes ImmutableAttributes) Value() (driver.Value, error) {
	return json.Marshal(immutableAttributes)
}

type DataProtection struct {
	ScheduledBackupEnabled *bool  `json:"scheduled_backup_enabled"`
	BackupVaultID          string `json:"backup_vault_id"`
	BackupPolicyID         string `json:"backup_policy_id"`
	BackupChainBytes       *int64 `json:"backup_chain_bytes"`
	PolicyEnforced         *bool  `json:"policy_enforced"`
}

func (dp *DataProtection) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}
	return json.Unmarshal(bytes, dp)
}

func (dp *DataProtection) Value() (driver.Value, error) {
	return json.Marshal(dp)
}
