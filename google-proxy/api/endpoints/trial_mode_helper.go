package api

import (
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/factory"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
)

// newTrialModeParamsFromOpt returns nil when trialMode is omitted.
func newTrialModeParamsFromOpt(tm gcpgenserver.OptTrialModeV1beta) *common.TrialModeParams {
	if !tm.IsSet() {
		return nil
	}
	v := tm.Value
	start := v.StartTime
	end := v.EndTime
	return &common.TrialModeParams{Start: &start, End: &end}
}

// stubPersistAccountTrialMetadataForCreate allows create handlers that call PersistAccountTrialMetadataIfSet
// (including when trialMode is omitted and trial is nil).
func stubPersistAccountTrialMetadataForCreate(mockOrc *factory.MockOrchestratorFactory) {
	mockOrc.EXPECT().PersistAccountTrialMetadataIfSet(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
}
