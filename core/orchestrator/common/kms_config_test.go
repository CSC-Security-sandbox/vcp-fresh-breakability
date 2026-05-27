package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
)

func TestConvertKmsConfigStateV1beta(t *testing.T) {
	tests := []struct {
		name            string
		status          string
		stateDetails    string
		expectedState   string
		expectedDetails string
	}{
		{
			name:            "CREATED state",
			status:          models.LifeCycleStateCreated,
			stateDetails:    "",
			expectedState:   "KEY_CHECK_PENDING",
			expectedDetails: "Credentials created and key check pending",
		},
		{
			name:            "KEY_CHECK_PENDING state",
			status:          models.LifeCycleStateKeyCheckPending,
			stateDetails:    "",
			expectedState:   "KEY_CHECK_PENDING",
			expectedDetails: "Credentials created and key check pending",
		},
		{
			name:            "IN_USE state",
			status:          models.LifeCycleStateInUse,
			stateDetails:    "",
			expectedState:   "IN_USE",
			expectedDetails: "Kms config in use",
		},
		{
			name:            "DELETED state",
			status:          models.LifeCycleStateDeleted,
			stateDetails:    "",
			expectedState:   "DELETED",
			expectedDetails: "Kms config deleted",
		},
		{
			name:            "UPDATING state",
			status:          models.LifeCycleStateUpdating,
			stateDetails:    "",
			expectedState:   "UPDATING",
			expectedDetails: "Updating Kms config",
		},
		{
			name:            "DELETING state",
			status:          models.LifeCycleStateDeleting,
			stateDetails:    "",
			expectedState:   "DELETING",
			expectedDetails: "Deleting Kms config",
		},
		{
			name:            "CREATING state",
			status:          models.LifeCycleStateCreating,
			stateDetails:    "",
			expectedState:   "CREATING",
			expectedDetails: "Creating Kms config",
		},
		{
			name:            "READY state",
			status:          models.LifeCycleStateREADY,
			stateDetails:    "",
			expectedState:   "READY",
			expectedDetails: "Kms config is ready for use",
		},
		{
			name:            "MIGRATING state",
			status:          models.LifeCycleStateMigrating,
			stateDetails:    "",
			expectedState:   "MIGRATING",
			expectedDetails: "Kms config is in migrating state",
		},
		{
			name:            "Error state with error prefix",
			status:          "error-something",
			stateDetails:    "error - Some error message",
			expectedState:   "ERROR",
			expectedDetails: "Some error message",
		},
		{
			name:            "Error state without error prefix in details",
			status:          "error-other",
			stateDetails:    "Some error message",
			expectedState:   "ERROR",
			expectedDetails: "Some error message",
		},
		{
			name:            "Unknown state",
			status:          "UNKNOWN_STATE",
			stateDetails:    "",
			expectedState:   "UNKNOWN_STATE",
			expectedDetails: "",
		},
		{
			name:            "Empty status",
			status:          "",
			stateDetails:    "",
			expectedState:   "",
			expectedDetails: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state, details := ConvertKmsConfigStateV1beta(tt.status, tt.stateDetails)
			assert.Equal(t, tt.expectedState, state, "State should match")
			assert.Equal(t, tt.expectedDetails, details, "Details should match")
		})
	}
}
