package datamodel

import (
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

type HydratedMetrics struct {
	MetricTimestamp       pgtype.Timestamp
	MeasuredType          string
	ResourceType          string
	Quantity              pgtype.Numeric
	ResourceUuid          string
	Metadata              []byte
	ResourcePartitionName string
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
