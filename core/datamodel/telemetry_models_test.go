package datamodel

import (
	"github.com/jackc/pgx/v5/pgtype"
	"testing"
	"time"
)

func TestHyderatedMetricsInitialization(t *testing.T) {
	now := pgtype.Timestamp{Time: time.Now(), Valid: true}
	quantity := pgtype.Numeric{}
	metrics := HydratedMetrics{
		MetricTimestamp:       now,
		MeasuredType:          "VSA_ALLOCATED_SIZE",
		ResourceType:          "VSA_BLOCK_VOLUME",
		Quantity:              quantity,
		ResourceUuid:          "123e4567-e89b-12d3-a456-426614174000",
		Metadata:              []byte("dummy metadata"),
		ResourcePartitionName: "partition-1",
	}
	if metrics.MeasuredType != "VSA_ALLOCATED_SIZE" {
		t.Errorf("Expected MeasuredType 'VSA_ALLOCATED_SIZE', got %s", metrics.MeasuredType)
	}
	if metrics.ResourceType != "VSA_BLOCK_VOLUME" {
		t.Errorf("Expected ResourceType 'VSA_BLOCK_VOLUME', got %s", metrics.ResourceType)
	}
}

func TestAggregatedUsageInitialization(t *testing.T) {
	start := pgtype.Timestamptz{Time: time.Now(), Valid: true}
	end := pgtype.Timestamptz{Time: time.Now(), Valid: true}
	usage := AggregatedUsage{
		ID:                     1,
		AccountUuid:            pgtype.Text{String: "123e4567-e89b-12d3-a456-426614174000", Valid: true},
		AggregationStart:       start,
		AggregationEnd:         end,
		MeasuredType:           pgtype.Text{String: "VSA_ALLOCATED_SIZE", Valid: true},
		Quantity:               pgtype.Numeric{},
		DetailsID:              pgtype.Int8{Int64: 42, Valid: true},
		ResourceUuid:           pgtype.Text{String: "123e4567-e89b-12d3-a456-426614174123", Valid: true},
		VolumeName:             pgtype.Text{String: "vol-1", Valid: true},
		AggregationGranularity: pgtype.Int4{Int32: 1, Valid: true},
		ServiceLevel:           pgtype.Text{String: "flex", Valid: true},
		ResourceType:           "VSA_BLOCK_VOLUME",
		AggregationType:        "HOURLY",
		LastCounterValue:       pgtype.Numeric{},
		CustomerID:             pgtype.Text{String: "cust-1", Valid: true},
		RegionName:             pgtype.Text{String: "us-west", Valid: true},
		SourceRegion:           pgtype.Text{String: "us-east", Valid: true},
		DestinationRegion:      pgtype.Text{String: "eu-west", Valid: true},
		BillingLabels:          pgtype.Text{String: "tag1,tag2", Valid: true},
		CreatedAt:              start,
		UpdatedAt:              end,
		ReplicationDstVolumeID: pgtype.Text{String: "dst-vol-1", Valid: true},
		DoubleEncryption:       pgtype.Bool{Bool: true, Valid: true},
	}
	if usage.ID != 1 {
		t.Errorf("Expected ID 1, got %d", usage.ID)
	}
	if usage.ResourceType != "VSA_BLOCK_VOLUME" {
		t.Errorf("Expected ResourceType 'VSA_BLOCK_VOLUME', got %s", usage.ResourceType)
	}
}

func TestBillingGcpUsageInitialization(t *testing.T) {
	now := pgtype.Timestamptz{Time: time.Now(), Valid: true}
	billing := BillingGcpUsage{
		ID:                10,
		AggregatedUsageID: 20,
		State:             "SUBMITTED",
		ErrorCount:        0,
		SentQuantity:      pgtype.Numeric{},
		ErrorMessage:      pgtype.Text{String: "", Valid: false},
		CustomerID:        "cust-2",
		Submission:        pgtype.Text{String: "sub-1", Valid: true},
		CreatedAt:         now,
		UpdatedAt:         now,
	}
	if billing.State != "SUBMITTED" {
		t.Errorf("Expected State 'VSA_BLOCK_VOLUME', got %s", billing.State)
	}
	if billing.CustomerID != "cust-2" {
		t.Errorf("Expected CustomerID 'cust-2', got %s", billing.CustomerID)
	}
}
