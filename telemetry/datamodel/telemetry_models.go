package datamodel

import (
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

type HydratedMetrics struct {
	ID              int64     `gorm:"primaryKey;autoIncrement" json:"id"`
	MetricTimestamp time.Time `gorm:"column:metric_timestamp;not null;index" json:"metric_timestamp"`
	MeasuredType    string    `gorm:"column:measured_type;not null;index" json:"measured_type"`
	ResourceType    string    `gorm:"column:resource_type;not null;index" json:"resource_type"`
	Quantity        float64   `gorm:"column:quantity;not null" json:"quantity"`
	ResourceName    string    `gorm:"column:resource_name;size:255;index" json:"resource_name"`
	ConsumerID      string    `gorm:"column:consumer_id;size:255;index" json:"consumer_id"`
	Location        string    `gorm:"column:location;size:255;index" json:"location"`
	Metadata        []byte    `gorm:"column:metadata;type:jsonb" json:"metadata"`
}
type AggregatedUsage struct {
	ID                     int64
	AccountUuid            pgtype.Text
	AggregationEnd         pgtype.Timestamptz
	AggregationStart       pgtype.Timestamptz
	MeasuredType           pgtype.Text
	Quantity               pgtype.Numeric
	DetailsID              pgtype.Int8
	ResourceUuid           pgtype.Text
	VolumeName             pgtype.Text
	AggregationGranularity pgtype.Int4
	ServiceLevel           pgtype.Text
	ResourceType           string
	AggregationType        string
	LastCounterValue       pgtype.Numeric
	CustomerID             pgtype.Text
	RegionName             pgtype.Text
	SourceRegion           pgtype.Text
	DestinationRegion      pgtype.Text
	BillingLabels          pgtype.Text
	CreatedAt              pgtype.Timestamptz
	UpdatedAt              pgtype.Timestamptz
	ReplicationDstVolumeID pgtype.Text
	DoubleEncryption       pgtype.Bool
}

type BillingGcpUsage struct {
	ID                int64
	AggregatedUsageID int64
	State             string
	ErrorCount        int32
	SentQuantity      pgtype.Numeric
	ErrorMessage      pgtype.Text
	CustomerID        string
	Submission        pgtype.Text
	CreatedAt         pgtype.Timestamptz
	UpdatedAt         pgtype.Timestamptz
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
