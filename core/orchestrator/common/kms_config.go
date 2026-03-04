package common

import (
	"strings"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
)

// ConvertKmsConfigStateV1beta converts internal KMS config state strings to API state strings.
// This is a provider-agnostic conversion function that maps internal lifecycle states
// to the standardized API state format.
func ConvertKmsConfigStateV1beta(status, stateDetails string) (state, details string) {
	switch status {
	case models.LifeCycleStateCreated, models.LifeCycleStateKeyCheckPending:
		return "KEY_CHECK_PENDING", "Credentials created and key check pending"
	case models.LifeCycleStateInUse:
		return "IN_USE", "Kms config in use"
	case models.LifeCycleStateDeleted:
		return "DELETED", "Kms config deleted"
	case models.LifeCycleStateUpdating:
		return "UPDATING", "Updating Kms config"
	case models.LifeCycleStateDeleting:
		return "DELETING", "Deleting Kms config"
	case models.LifeCycleStateCreating:
		return "CREATING", "Creating Kms config"
	case models.LifeCycleStateREADY:
		return "READY", "Kms config is ready for use"
	case models.LifeCycleStateMigrating:
		return "MIGRATING", "Kms config is in migrating state"
	default:
		if strings.Contains(status, "error") {
			return "ERROR", strings.TrimPrefix(stateDetails, "error - ")
		}
		return status, ""
	}
}
