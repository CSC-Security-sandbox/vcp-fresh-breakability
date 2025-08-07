package api

import (
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
)

var convertToVolumeReplicationsInternalV1Beta = _convertToVolumeReplicationsInternalV1Beta

func mapReplicationStateToInternalLifeCycleState(state string) gcpgenserver.VolumeReplicationInternalV1betaLifeCycleState {
	switch state {
	case models.LifeCycleStateCreating:
		return gcpgenserver.VolumeReplicationInternalV1betaLifeCycleStateCreating
	case models.LifeCycleStateAvailable:
		return gcpgenserver.VolumeReplicationInternalV1betaLifeCycleStateAvailable
	case models.LifeCycleStateDeleting:
		return gcpgenserver.VolumeReplicationInternalV1betaLifeCycleStateDeleting
	case models.LifeCycleStateDeleted:
		return gcpgenserver.VolumeReplicationInternalV1betaLifeCycleStateDeleted
	case models.LifeCycleStateError:
		return gcpgenserver.VolumeReplicationInternalV1betaLifeCycleStateError
	case models.LifeCycleStateDisabled:
		return gcpgenserver.VolumeReplicationInternalV1betaLifeCycleStateDisabled
	case models.LifeCycleStateUpdating:
		return gcpgenserver.VolumeReplicationInternalV1betaLifeCycleStateUpdating
	default:
		return gcpgenserver.VolumeReplicationInternalV1betaLifeCycleStateAvailable
	}
}

func mapEndpointTypeToInternal(endpointType string) gcpgenserver.VolumeReplicationInternalV1betaEndpointType {
	switch endpointType {
	case models.SrcEndpoint:
		return gcpgenserver.VolumeReplicationInternalV1betaEndpointTypeSrc
	case models.DstEndpoint:
		return gcpgenserver.VolumeReplicationInternalV1betaEndpointTypeDst
	default:
		return ""
	}
}

func mapMirrorStateToInternal(mirrorState string) gcpgenserver.VolumeReplicationInternalV1betaMirrorState {
	switch mirrorState {
	case models.OntapUninitialized:
		return gcpgenserver.VolumeReplicationInternalV1betaMirrorStateUNINITIALIZED
	case models.OntapBrokenOff:
		return gcpgenserver.VolumeReplicationInternalV1betaMirrorStateSTOPPED
	case models.OntapSnapmirrored:
		return gcpgenserver.VolumeReplicationInternalV1betaMirrorStateMIRRORED
	default:
		return ""
	}
}

func mapRelationshipStatusToInternal(relationshipStatus string) gcpgenserver.VolumeReplicationInternalV1betaRelationshipStatus {
	switch relationshipStatus {
	case models.SnapmirrorRelationshipIdle, models.SnapmirrorRelationshipSuccess, models.SnapmirrorRelationshipFinalizing:
		return gcpgenserver.VolumeReplicationInternalV1betaRelationshipStatusIdle
	case models.SnapmirrorRelationshipTransferring:
		return gcpgenserver.VolumeReplicationInternalV1betaRelationshipStatusTransferring
	case models.SnapmirrorRelationshipFailed:
		return gcpgenserver.VolumeReplicationInternalV1betaRelationshipStatusFailed
	case models.SnapmirrorRelationshipAborted:
		return gcpgenserver.VolumeReplicationInternalV1betaRelationshipStatusAborted
	case models.SnapmirrorRelationshipQueued:
		return gcpgenserver.VolumeReplicationInternalV1betaRelationshipStatusQueued
	case models.SnapmirrorRelationshipHardAborted:
		return gcpgenserver.VolumeReplicationInternalV1betaRelationshipStatusHardAborted
	default:
		return ""
	}
}

func mapReplicationScheduleToInternal(schedule string) gcpgenserver.VolumeReplicationInternalV1betaReplicationSchedule {
	switch schedule {
	case vsa.VolumeReplicationScheduleHourly:
		return gcpgenserver.VolumeReplicationInternalV1betaReplicationScheduleHourly
	case vsa.VolumeReplicationScheduleDaily:
		return gcpgenserver.VolumeReplicationInternalV1betaReplicationScheduleDaily
	case vsa.VolumeReplicationSchedule10Minutely:
		return gcpgenserver.VolumeReplicationInternalV1betaReplicationSchedule10minutely
	default:
		return ""
	}
}

func convertToVolumeReplicationInternalV1Beta(replication *datamodel.VolumeReplication) gcpgenserver.VolumeReplicationInternalV1beta {
	if replication == nil {
		return gcpgenserver.VolumeReplicationInternalV1beta{}
	}

	var lastTransferEndTime time.Time
	var progressLastUpdated time.Time
	if replication.LastTransferEndTime != nil {
		lastTransferEndTime = *replication.LastTransferEndTime
	}
	if replication.ProgressLastUpdated != nil {
		progressLastUpdated = *replication.ProgressLastUpdated
	}

	return gcpgenserver.VolumeReplicationInternalV1beta{
		VolumeReplicationUuid: gcpgenserver.NewOptString(replication.UUID),
		LifeCycleState:        gcpgenserver.NewOptVolumeReplicationInternalV1betaLifeCycleState(mapReplicationStateToInternalLifeCycleState(replication.State)),
		LifeCycleStateDetails: gcpgenserver.NewOptString(replication.StateDetails),
		EndpointType:          mapEndpointTypeToInternal(replication.ReplicationAttributes.EndpointType),
		ReplicationPolicy:     gcpgenserver.NewOptVolumeReplicationInternalV1betaReplicationPolicy(gcpgenserver.VolumeReplicationInternalV1betaReplicationPolicyMirrorAllSnapshots),
		ReplicationSchedule:   gcpgenserver.NewOptVolumeReplicationInternalV1betaReplicationSchedule(mapReplicationScheduleToInternal(replication.ReplicationAttributes.ReplicationSchedule)),
		SourceHostName:        replication.ReplicationAttributes.SourceHostName,
		SourceServerName:      replication.ReplicationAttributes.SourceSvmName,
		SourceVolumeName:      replication.ReplicationAttributes.SourceVolumeName,
		SourceVolumeUuid:      gcpgenserver.NewOptString(replication.ReplicationAttributes.SourceVolumeUUID),
		DestinationHostName:   replication.ReplicationAttributes.DestinationHostName,
		DestinationServerName: replication.ReplicationAttributes.DestinationSvmName,
		DestinationVolumeName: replication.ReplicationAttributes.DestinationVolumeName,
		DestinationVolumeUuid: gcpgenserver.NewOptString(replication.ReplicationAttributes.DestinationVolumeUUID),
		Name:                  gcpgenserver.NewOptString(replication.Name),
		MirrorState:           gcpgenserver.NewOptVolumeReplicationInternalV1betaMirrorState(mapMirrorStateToInternal(*replication.MirrorState)),
		RelationshipStatus:    gcpgenserver.NewOptVolumeReplicationInternalV1betaRelationshipStatus(mapRelationshipStatusToInternal(*replication.RelationshipStatus)),
		TotalProgress:         gcpgenserver.NewOptInt64(replication.TotalProgress),
		Healthy:               gcpgenserver.NewOptBool(replication.Healthy), // fix this
		TotalTransferBytes:    gcpgenserver.NewOptInt64(replication.TotalTransferBytes),
		TotalTransferTimeSecs: gcpgenserver.NewOptInt64(replication.TotalTransferTimeSecs),
		LastTransferSize:      gcpgenserver.NewOptInt64(replication.LastTransferSize),
		LastTransferError:     gcpgenserver.NewOptString(replication.LastTransferError),
		LastTransferDuration:  gcpgenserver.NewOptInt64(replication.LastTransferDuration),
		LastTransferEndTime:   gcpgenserver.NewOptDateTime(lastTransferEndTime),
		ProgressLastUpdated:   gcpgenserver.NewOptDateTime(progressLastUpdated),
		LagTime:               gcpgenserver.NewOptInt64(replication.LagTime),
		CreatedAt:             gcpgenserver.NewOptDateTime(replication.CreatedAt),
		UpdatedAt:             gcpgenserver.NewOptDateTime(replication.UpdatedAt),
		Description:           gcpgenserver.NewOptString(replication.Description),
		RemoteRegion:          replication.ReplicationAttributes.DestinationLocation,
		CcfeUri:               gcpgenserver.NewOptString(replication.Uri),
		CcfeRemoteUri:         gcpgenserver.NewOptString(replication.RemoteUri),
	}
}

func convertToPoolInternalV1Beta(pool *models.Pool) *gcpgenserver.PoolInternalV1beta {
	poolResp := &gcpgenserver.PoolInternalV1beta{
		Network:                  pool.VendorSubNetID,
		PoolId:                   gcpgenserver.NewOptString(pool.UUID),
		ResourceId:               pool.Name,
		ServiceLevel:             gcpgenserver.PoolInternalV1betaServiceLevel(pool.ServiceLevel),
		QosType:                  gcpgenserver.NewOptNilString(pool.QosType),
		SizeInBytes:              float64(pool.SizeInBytes),
		AllocatedBytes:           gcpgenserver.NewOptNilFloat64(pool.PoolAttributes.AllocatedBytes),
		TotalThroughputMibps:     gcpgenserver.NewOptNilFloat64(pool.TotalThroughputMibps),
		AvailableThroughputMibps: gcpgenserver.NewOptNilFloat64(pool.TotalThroughputMibps - pool.UtilizedThroughputMibps),
		NumberOfVolumes:          gcpgenserver.NewOptNilInt32(int32(pool.PoolAttributes.NumberOfVolumes)),
		StoragePoolState: gcpgenserver.OptPoolInternalV1betaStoragePoolState{
			Value: gcpgenserver.PoolInternalV1betaStoragePoolState(pool.State),
		},
		StoragePoolStateDetails: gcpgenserver.NewOptString(pool.StateDetails),
		CreatedAt:               gcpgenserver.NewOptDateTime(pool.CreatedAt),
		UpdatedAt:               gcpgenserver.NewOptDateTime(pool.UpdatedAt),
		StateDetails:            gcpgenserver.NewOptString(pool.StateDetails),
		Description:             gcpgenserver.NewOptNilString(pool.Description),
		Zone:                    gcpgenserver.NewOptString(pool.Zone),
		AllowAutoTiering:        gcpgenserver.NewOptNilBool(pool.AllowAutoTiering),
	}
	if pool.PoolAttributes != nil {
		poolResp.SecondaryZone = gcpgenserver.NewOptString(pool.PoolAttributes.SecondaryZone)
	}
	if pool.CustomPerformanceParams != nil {
		poolResp.CustomPerformanceEnabled = gcpgenserver.NewOptBool(pool.CustomPerformanceParams.Enabled)
		poolResp.TotalIops = gcpgenserver.NewOptNilFloat64(float64(pool.CustomPerformanceParams.Iops))
	}
	if pool.ClusterAttributes != nil {
		poolResp.InterclusterLifs = pool.ClusterAttributes.InterClusterLifs
		poolResp.ClusterName = gcpgenserver.NewOptString(pool.ClusterAttributes.ExternalName)
	}
	return poolResp
}

func _convertToVolumeReplicationsInternalV1Beta(in []*datamodel.VolumeReplication) []gcpgenserver.VolumeReplicationInternalV1beta {
	if in == nil {
		return nil
	}
	var out []gcpgenserver.VolumeReplicationInternalV1beta
	for _, replication := range in {
		out = append(out, convertToVolumeReplicationInternalV1Beta(replication))
	}
	return out
}
