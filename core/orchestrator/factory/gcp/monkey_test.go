package gcp

import (
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/mqos"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

func newMonkeyMockForValidateQos(t *testing.T) *monkeyMock {
	mm := newMonkeyMock(t)
	orig := validateVolumeQosParams
	validateVolumeQosParams = mm.validateVolumeQosParams
	mm.EXPECT().validateVolumeQosParams(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return((*int64)(nil), nil)
	t.Cleanup(func() {
		validateVolumeQosParams = orig
	})
	return mm
}

func newMonkeyMockAndPatch(t *testing.T) *monkeyMock {
	mm := newMonkeyMock(t)

	utilGetLogger = mm.utilGetLogger
	utilsGetLocationFromVendorID = mm.utilsGetLocationFromVendorID
	utilsGetRequestIDFromContext = mm.utilsGetRequestIDFromContext
	utilsGetCorrelationIDFromContext = mm.utilsGetCorrelationIDFromContext
	envIsLocalEnv = mm.envIsLocalEnv

	getOrCreateAccount = mm.getOrCreateAccount
	validateCreateVolumeParams = mm.validateCreateVolumeParams
	validateVolumeQosParams = mm.validateVolumeQosParams
	validateDeleteVolumeParams = mm.validateDeleteVolumeParams
	workflowsExecuteWorkflowSequentially = mm.workflowsExecuteWorkflowSequentially
	isEstablishVolumePeeringNeeded = mm.isEstablishVolumePeeringNeeded
	verifyVolumeState = mm.verifyVolumeState
	verifyFlexCacheParameters = mm.verifyFlexCacheParameters
	verifyClusterPeering = mm.verifyClusterPeering
	checkForFlexCacheJobInProgress = mm.checkForFlexCacheJobInProgress
	verifyCommandExpiryTime = mm.verifyCommandExpiryTime

	createFlexCacheVolume = mm.createFlexCacheVolume
	establishFlexCacheVolumePeering = mm.establishFlexCacheVolumePeering
	checkAndCancelCreateWorkflowIfNeeded = mm.checkAndCancelCreateWorkflowIfNeeded

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
		utilsGetRequestIDFromContext = utils.GetRequestIDFromContext
		utilsGetCorrelationIDFromContext = utils.GetCoRelationIDFromContext
		envIsLocalEnv = env.IsLocalEnv

		getOrCreateAccount = _getOrCreateAccount
		validateCreateVolumeParams = _validateCreateVolumeParams
		validateVolumeQosParams = mqos.ValidateVolumeQosParams
		validateDeleteVolumeParams = _validateDeleteVolumeParams
		workflowsExecuteWorkflowSequentially = workflows.ExecuteWorkflowSequentially
		isEstablishVolumePeeringNeeded = _isEstablishVolumePeeringNeeded
		verifyVolumeState = _verifyVolumeState
		verifyFlexCacheParameters = _verifyFlexCacheParameters
		verifyClusterPeering = _verifyClusterPeering
		checkForFlexCacheJobInProgress = _checkForFlexCacheJobInProgress
		verifyCommandExpiryTime = _verifyCommandExpiryTime

		createFlexCacheVolume = _createFlexCacheVolume
		establishFlexCacheVolumePeering = _establishFlexCacheVolumePeering
		checkAndCancelCreateWorkflowIfNeeded = _checkAndCancelCreateWorkflowIfNeeded

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
