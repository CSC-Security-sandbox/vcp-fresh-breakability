package orchestrator

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"testing"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

func newMonkeyMockAndPatch(t *testing.T) *monkeyMock {
	mm := newMonkeyMock(t)

	utilGetLogger = mm.utilGetLogger
	utilsGetLocationFromVendorID = mm.utilsGetLocationFromVendorID
	envIsLocalEnv = mm.envIsLocalEnv

	getOrCreateAccount = mm.getOrCreateAccount
	validateCreateVolumeParams = mm.validateCreateVolumeParams
	workflowsExecuteWorkflowSequentially = mm.workflowsExecuteWorkflowSequentially

	createFlexCacheVolume = mm.createFlexCacheVolume

	t.Cleanup(func() {
		utilGetLogger = util.GetLogger
		utilsGetLocationFromVendorID = utils.GetLocationFromVendorID
		envIsLocalEnv = env.IsLocalEnv

		getOrCreateAccount = _getOrCreateAccount
		validateCreateVolumeParams = _validateCreateVolumeParams
		workflowsExecuteWorkflowSequentially = workflows.ExecuteWorkflowSequentially

		createFlexCacheVolume = _createFlexCacheVolume
	})

	return mm
}
