package orchestrator

import (
	"testing"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

func newMonkeyMockAndPatch(t *testing.T) *monkeyMock {
	mm := newMonkeyMock(t)

	utilGetLogger = mm.utilGetLogger
	utilsGetLocationFromVendorID = mm.utilsGetLocationFromVendorID

	getOrCreateAccount = mm.getOrCreateAccount
	validateCreateVolumeParams = mm.validateCreateVolumeParams
	workflowsExecuteWorkflowSequentially = mm.workflowsExecuteWorkflowSequentially

	createFlexCacheVolume = mm.createFlexCacheVolume

	t.Cleanup(func() {
		utilGetLogger = util.GetLogger
		utilsGetLocationFromVendorID = utils.GetLocationFromVendorID

		getOrCreateAccount = _getOrCreateAccount
		validateCreateVolumeParams = _validateCreateVolumeParams
		workflowsExecuteWorkflowSequentially = workflows.ExecuteWorkflowSequentially

		createFlexCacheVolume = _createFlexCacheVolume
	})

	return mm
}
