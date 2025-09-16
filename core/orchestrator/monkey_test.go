package orchestrator

import (
	"testing"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
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

	// Volume replication methods
	getAccountWithName = mm.getAccountWithName
	utilParseAndValidateRegionAndZone = mm.utilParseAndValidateRegionAndZone
	utilsGetPairedRegionUri = mm.utilsGetPairedRegionURI
	authGetSignedJwtToken = mm.authGetSignedJwtToken
	utilsParseProjectNumberFromURI = mm.utilsParseProjectNumberFromURI
	getReplicationObjects = mm.getReplicationObjects

	t.Cleanup(func() {
		utilGetLogger = util.GetLogger
		utilsGetLocationFromVendorID = utils.GetLocationFromVendorID
		envIsLocalEnv = env.IsLocalEnv

		getOrCreateAccount = _getOrCreateAccount
		validateCreateVolumeParams = _validateCreateVolumeParams
		workflowsExecuteWorkflowSequentially = workflows.ExecuteWorkflowSequentially

		createFlexCacheVolume = _createFlexCacheVolume

		// Volume replication methods
		getAccountWithName = _getAccountWithName
		utilParseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		utilsGetPairedRegionUri = utils.GetPairedRegionURI
		authGetSignedJwtToken = auth.GetSignedJwtToken
		utilsParseProjectNumberFromURI = utils.ParseProjectNumberFromURI
		getReplicationObjects = _getReplicationObjects
	})

	return mm
}
