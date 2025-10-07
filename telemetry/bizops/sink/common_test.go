package sink

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/entity"
)

func Test_GetFilePath(t *testing.T) {
	date := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	timezone := "UTC"
	expected := fmt.Sprintf("%s/%s-%s-%s.csv", region, date.Format(dateLayout), reportName, timezone)
	result := GetFilePath(date, timezone)
	if result != expected {
		t.Errorf("GetFilePath() = %s; want %s", result, expected)
	}
}

func Test_ValidateSinkParams(t *testing.T) {
	tests := []struct {
		name       string
		sinkParams *entity.BizopsSinkParams
		wantErr    bool
	}{
		{
			name:       "nil sinkParams",
			sinkParams: nil,
			wantErr:    true,
		},
		{
			name: "nil reader",
			sinkParams: &entity.BizopsSinkParams{
				Reader:   nil,
				Date:     time.Now(),
				Timezone: "UTC",
			},
			wantErr: true,
		},
		{
			name: "zero date",
			sinkParams: &entity.BizopsSinkParams{
				Reader:   bytes.NewReader([]byte("data")),
				Date:     time.Time{},
				Timezone: "UTC",
			},
			wantErr: true,
		},
		{
			name: "empty timezone",
			sinkParams: &entity.BizopsSinkParams{
				Reader:   bytes.NewReader([]byte("data")),
				Date:     time.Now(),
				Timezone: "",
			},
			wantErr: true,
		},
		{
			name: "valid params",
			sinkParams: &entity.BizopsSinkParams{
				Reader:   bytes.NewReader([]byte("data")),
				Date:     time.Now(),
				Timezone: "UTC",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateSinkParams(tt.sinkParams)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateSinkParams() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
