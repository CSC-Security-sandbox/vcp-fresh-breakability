package replication

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
)

func (_m *monkeyMock) Patch() {
	InternalUtilGetCallbackToken = _m.InternalUtilGetCallbackToken
	InternalUtilGetSignedToken = _m.InternalUtilGetSignedToken
	InternalUtilGetPairedRegionURI = _m.InternalUtilGetPairedRegionURI
	InternalParseRegionAndZone = _m.InternalParseRegionAndZone
	validateReplicationResourceId = _m.validateReplicationResourceId
	validateStoragePoolUri = _m.validateStoragePoolUri
	getDestinationPool = _m.getDestinationPool
	replicationJobInProcess = _m.replicationJobInProcess
	getQuotaLimit = _m.getQuotaLimit
	internalGetReplicationCount = _m.internalGetReplicationCount
	internalGetVolumeCount = _m.internalGetVolumeCount
	getVolume = _m.getVolume
	createReplicationObjects = _m.createReplicationObjects
}

func (_m *monkeyMock) Unpatch() {
	InternalUtilGetCallbackToken = auth.GetSignedAccessToken
	InternalUtilGetSignedToken = auth.GetSignedJwtToken
	InternalUtilGetPairedRegionURI = utils.GetPairedRegionURI
	InternalParseRegionAndZone = utils.ParseRegionAndZone
	validateReplicationResourceId = _validateReplicationResourceId
	validateStoragePoolUri = _validateStoragePoolUri
	getDestinationPool = _getDestinationPool
	replicationJobInProcess = _replicationJobInProcess
	getQuotaLimit = common.GetQuotaLimit
	internalGetReplicationCount = _internalGetReplicationCount
	internalGetVolumeCount = _internalGetVolumeCount
	getVolume = _getVolume
	createReplicationObjects = _createReplicationObjects
}
