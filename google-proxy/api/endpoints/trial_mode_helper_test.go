package api

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/factory"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
)

func TestNewTrialModeParamsFromOpt(t *testing.T) {
	start := time.Date(2025, 5, 14, 11, 0, 0, 0, time.UTC)
	end := time.Date(2025, 6, 13, 11, 0, 0, 0, time.UTC)

	tests := []struct {
		name      string
		opt       gcpgenserver.OptTrialModeV1beta
		wantNil   bool
		wantStart time.Time
		wantEnd   time.Time
	}{
		{
			name:    "returns nil when omitted",
			opt:     gcpgenserver.OptTrialModeV1beta{},
			wantNil: true,
		},
		{
			name: "maps start and end when set",
			opt: gcpgenserver.NewOptTrialModeV1beta(gcpgenserver.TrialModeV1beta{
				StartTime: start,
				EndTime:   end,
			}),
			wantStart: start,
			wantEnd:   end,
		},
		{
			name: "maps zero times when trialMode is present with zero values",
			opt: gcpgenserver.NewOptTrialModeV1beta(gcpgenserver.TrialModeV1beta{
				StartTime: time.Time{},
				EndTime:   time.Time{},
			}),
			wantStart: time.Time{},
			wantEnd:   time.Time{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := newTrialModeParamsFromOpt(tc.opt)
			if tc.wantNil {
				assert.Nil(t, got)
				return
			}
			require.NotNil(t, got)
			require.NotNil(t, got.Start)
			require.NotNil(t, got.End)
			assert.True(t, tc.wantStart.Equal(*got.Start))
			assert.True(t, tc.wantEnd.Equal(*got.End))
			// Pointers must refer to copies, not fields inside the opt value.
			if tc.opt.IsSet() {
				assert.NotSame(t, &tc.opt.Value.StartTime, got.Start)
				assert.NotSame(t, &tc.opt.Value.EndTime, got.End)
			}
		})
	}
}

func TestStubPersistAccountTrialMetadataForCreate(t *testing.T) {
	ctx := context.Background()
	start := time.Date(2025, 5, 14, 11, 0, 0, 0, time.UTC)
	end := time.Date(2025, 6, 13, 11, 0, 0, 0, time.UTC)
	trial := &common.TrialModeParams{Start: &start, End: &end}

	tests := []struct {
		name        string
		accountName string
		trial       *common.TrialModeParams
	}{
		{name: "nil trial", accountName: "project-1", trial: nil},
		{name: "valid trial", accountName: "project-2", trial: trial},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockOrc := factory.NewMockOrchestratorFactory(t)
			stubPersistAccountTrialMetadataForCreate(mockOrc)

			err := mockOrc.PersistAccountTrialMetadataIfSet(ctx, tc.accountName, tc.trial)
			require.NoError(t, err)
		})
	}
}
