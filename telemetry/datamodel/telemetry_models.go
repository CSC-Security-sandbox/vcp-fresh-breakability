package datamodel

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
	"time"
)

type HydratedMetrics struct {
	ID              int64                 `gorm:"primaryKey;autoIncrement" json:"id"`
	MetricTimestamp time.Time             `gorm:"column:metric_timestamp;not null;index" json:"metric_timestamp"`
	MeasuredType    metadata.MeasuredType `gorm:"column:measured_type;not null;index" json:"measured_type"`
	ResourceType    metadata.ResourceType `gorm:"column:resource_type;not null;index" json:"resource_type"`
	Quantity        float64               `gorm:"column:quantity;not null" json:"quantity"`
	ResourceName    string                `gorm:"column:resource_name;size:255;index" json:"resource_name"`
	ConsumerID      string                `gorm:"column:consumer_id;size:255;index" json:"consumer_id"`
	Location        string                `gorm:"column:location;size:255;index" json:"location"`
	Metadata        []byte                `gorm:"column:metadata;type:jsonb" json:"metadata"`
}

type AggregatedUsage struct {
	ID                     int64                 `gorm:"primaryKey;autoIncrement" json:"id"`
	AccountUuid            *string               `gorm:"column:account_uuid;size:255;index" json:"account_uuid"`
	AggregationEnd         time.Time             `gorm:"column:aggregation_end;not null;index" json:"aggregation_end"`
	AggregationStart       time.Time             `gorm:"column:aggregation_start;not null;index" json:"aggregation_start"`
	MeasuredType           metadata.MeasuredType `gorm:"column:measured_type;not null;index" json:"measured_type"`
	Quantity               float64               `gorm:"column:quantity;not null" json:"quantity"`
	ResourceName           *string               `gorm:"column:resource_name;size:255;index" json:"resource_name"`
	ResourceType           metadata.ResourceType `gorm:"column:resource_type;not null;index" json:"resource_type"`
	AggregationType        string                `gorm:"column:aggregation_type;size:100;not null" json:"aggregation_type"`
	LastCounterValue       *float64              `gorm:"column:last_counter_value" json:"last_counter_value"`
	RegionName             *string               `gorm:"column:region_name;size:255;index" json:"region_name"`
	SourceRegion           *string               `gorm:"column:source_region;size:255" json:"source_region"`
	DestinationRegion      *string               `gorm:"column:destination_region;size:255" json:"destination_region"`
	BillingLabels          *string               `gorm:"column:billing_labels;type:jsonb" json:"billing_labels"`
	ReplicationDstVolumeID *string               `gorm:"column:replication_dst_volume_id;size:255" json:"replication_dst_volume_id"`
	DoubleEncryption       *bool                 `gorm:"column:double_encryption;default:false" json:"double_encryption"`
	State                  common.TrackingState  `gorm:"column:state;not null;default:0" json:"state"`
	ErrorCount             int32                 `gorm:"column:error_count;default:0" json:"error_count"`
	ErrorMessage           *string               `gorm:"column:error_message;type:text" json:"error_message"`
	Submission             *string               `gorm:"column:submission;type:jsonb" json:"submission"`
	IsBillable             bool                  `gorm:"column:is_billable;default:false" json:"is_billable"`
	CreatedAt              time.Time             `gorm:"column:created_at;autoCreateTime" json:"created_at"`
	UpdatedAt              time.Time             `gorm:"column:updated_at;autoUpdateTime" json:"updated_at"`
}

type Job struct {
	ID       int64  `gorm:"primaryKey;autoIncrement"`
	TypeName string `gorm:"type:text"`
	Status   string `gorm:"type:text"`
	Queue    string `gorm:"type:text"`
	Data     string `gorm:"type:text"`
	Error    string `gorm:"type:text"`
	Attempt  int32  `gorm:"default:0"`

	CreatedAt   time.Time
	StartedAt   time.Time
	FinishedAt  time.Time
	ScheduledAt time.Time
}
