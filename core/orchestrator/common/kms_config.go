package common

import (
	"strings"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
)

// ConvertKmsConfigStateV1beta converts internal KMS config state strings to API state strings.
// This is a provider-agnostic conversion function that maps internal lifecycle states
// to the standardized API state format.
func ConvertKmsConfigStateV1beta(status, stateDetails string) (state, details string) {
	switch status {
	case datamodel.LifeCycleStateCreated, datamodel.LifeCycleStateKeyCheckPending:
		return "KEY_CHECK_PENDING", "Credentials created and key check pending"
	case datamodel.LifeCycleStateInUse:
		return "IN_USE", "Kms config in use"
	case datamodel.LifeCycleStateDeleted:
		return "DELETED", "Kms config deleted"
	case datamodel.LifeCycleStateUpdating:
		return "UPDATING", "Updating Kms config"
	case datamodel.LifeCycleStateDeleting:
		return "DELETING", "Deleting Kms config"
	case datamodel.LifeCycleStateCreating:
		return "CREATING", "Creating Kms config"
	case datamodel.LifeCycleStateREADY:
		return "READY", "Kms config is ready for use"
	case datamodel.LifeCycleStateMigrating:
		return "MIGRATING", "Kms config is in migrating state"
	default:
		if strings.Contains(status, "error") {
			return "ERROR", strings.TrimPrefix(stateDetails, "error - ")
		}
		return status, ""
	}
}
