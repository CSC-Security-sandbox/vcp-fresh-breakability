package helper

import (
	"context"
	"testing"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestGetProjectID(t *testing.T) {
	// Case 1: Valid customerID
	fields := log.Fields{"customerID": "test-project-id"}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, fields)
	got := GetProjectID(ctx)
	if got != "test-project-id" {
		t.Errorf("Expected 'test-project-id', got '%s'", got)
	}

	// Case 2: customerID is empty string
	fields = log.Fields{"customerID": ""}
	ctx = context.WithValue(context.Background(), middleware.TemporalSLoggerKey, fields)
	got = GetProjectID(ctx)
	if got != "" {
		t.Errorf("Expected empty string, got '%s'", got)
	}

	// Case 3: No customerID key
	fields = log.Fields{"otherKey": "value"}
	ctx = context.WithValue(context.Background(), middleware.TemporalSLoggerKey, fields)
	got = GetProjectID(ctx)
	if got != "" {
		t.Errorf("Expected empty string, got '%s'", got)
	}

	// Case 4: No middleware key in context
	got = GetProjectID(context.Background())
	if got != "" {
		t.Errorf("Expected empty string, got '%s'", got)
	}
}
