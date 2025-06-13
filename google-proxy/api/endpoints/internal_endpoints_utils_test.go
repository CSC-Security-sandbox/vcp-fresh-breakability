package api

import (
	"testing"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
)

func TestMapReplicationStateToInternalLifeCycleState(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected gcpgenserver.VolumeReplicationInternalV1betaLifeCycleState
	}{
		{"Creating", models.LifeCycleStateCreating, gcpgenserver.VolumeReplicationInternalV1betaLifeCycleStateCreating},
		{"Available", models.LifeCycleStateAvailable, gcpgenserver.VolumeReplicationInternalV1betaLifeCycleStateAvailable},
		{"Deleting", models.LifeCycleStateDeleting, gcpgenserver.VolumeReplicationInternalV1betaLifeCycleStateDeleting},
		{"Deleted", models.LifeCycleStateDeleted, gcpgenserver.VolumeReplicationInternalV1betaLifeCycleStateDeleted},
		{"Error", models.LifeCycleStateError, gcpgenserver.VolumeReplicationInternalV1betaLifeCycleStateError},
		{"Disabled", models.LifeCycleStateDisabled, gcpgenserver.VolumeReplicationInternalV1betaLifeCycleStateDisabled},
		{"Updating", models.LifeCycleStateUpdating, gcpgenserver.VolumeReplicationInternalV1betaLifeCycleStateUpdating},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := mapReplicationStateToInternalLifeCycleState(tt.input)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestMapEndpointTypeToInternal(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected gcpgenserver.VolumeReplicationInternalV1betaEndpointType
	}{
		{"SrcEndpoint", models.SrcEndpoint, gcpgenserver.VolumeReplicationInternalV1betaEndpointTypeSrc},
		{"DstEndpoint", models.DstEndpoint, gcpgenserver.VolumeReplicationInternalV1betaEndpointTypeDst},
		{"Unknown", "unknown", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := mapEndpointTypeToInternal(tt.input)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestMapMirrorStateToInternal(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected gcpgenserver.VolumeReplicationInternalV1betaMirrorState
	}{
		{"Uninitialized", models.OntapUninitialized, gcpgenserver.VolumeReplicationInternalV1betaMirrorStateUNINITIALIZED},
		{"BrokenOff", models.OntapBrokenOff, gcpgenserver.VolumeReplicationInternalV1betaMirrorStateSTOPPED},
		{"Snapmirrored", models.OntapSnapmirrored, gcpgenserver.VolumeReplicationInternalV1betaMirrorStateMIRRORED},
		{"Unknown", "unknown", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := mapMirrorStateToInternal(tt.input)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestMapRelationshipStatusToInternal(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected gcpgenserver.VolumeReplicationInternalV1betaRelationshipStatus
	}{
		{"Idle", models.SnapmirrorRelationshipIdle, gcpgenserver.VolumeReplicationInternalV1betaRelationshipStatusIdle},
		{"Transferring", models.SnapmirrorRelationshipTransferring, gcpgenserver.VolumeReplicationInternalV1betaRelationshipStatusTransferring},
		{"Unknown", "unknown", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := mapRelationshipStatusToInternal(tt.input)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestConvertToVolumeReplicationInternalV1Beta(t *testing.T) {
	timeNow := time.Now()
	snapmirrored := models.OntapSnapmirrored
	snapmirrorRelationshipIdle := models.SnapmirrorRelationshipIdle

	replication := &datamodel.VolumeReplication{
		BaseModel: datamodel.BaseModel{
			ID:        123,
			UUID:      "some-uuid",
			CreatedAt: timeNow,
			UpdatedAt: timeNow,
			DeletedAt: nil,
		},
		Name:         "Test Replication",
		Description:  "Test Description",
		State:        models.LifeCycleStateCreating,
		StateDetails: "Test State Details",
		Uri:          "projects/45110233509/locations/australia-southeast1/volume/godpvolume4/replications/replication-name-6",
		RemoteUri:    "projects/45110233509/locations/us-east4/volume/gosrcvolume1/replications/replication-name-6",
		ReplicationAttributes: &datamodel.ReplicationDetails{
			EndpointType:               "src",
			ReplicationSchedule:        "daily",
			SourcePoolUUID:             "source-pool-uuid",
			SourceVolumeUUID:           "source-volume-uuid",
			SourceLocation:             "source-location",
			SourceHostName:             "source-hostname",
			SourceReplicationUUID:      "source-replication-uuid",
			SourceSvmName:              "source-svm-name",
			SourceVolumeName:           "source-volume-name",
			DestinationPoolUUID:        "destination-pool-uuid",
			DestinationVolumeUUID:      "destination-volume-uuid",
			DestinationLocation:        "destination-location",
			DestinationHostName:        "destination-hostname",
			DestinationReplicationUUID: "destination-replication-uuid",
			DestinationSvmName:         "destination-svm-name",
			DestinationVolumeName:      "destination-volume-name",
			ExternalUUID:               "external-uuid",
		},
		MirrorState:           &snapmirrored,
		RelationshipStatus:    &snapmirrorRelationshipIdle,
		TotalProgress:         100,
		TotalTransferBytes:    1000000,
		TotalTransferTimeSecs: 3600,
		LastTransferSize:      500000,
		LastTransferError:     "no error",
		LastTransferDuration:  1800,
		LastTransferEndTime:   &timeNow,
		ProgressLastUpdated:   &timeNow,
		LastUpdatedFromOntap:  timeNow,
		Healthy:               false,
		UnhealthyReason:       "No issues detected",
		LagTime:               30,
		AccountID:             1,
		VolumeID:              1,
	}

	result := convertToVolumeReplicationInternalV1Beta(replication)

	if result.VolumeReplicationUuid.Value != replication.UUID {
		t.Errorf("Expected UUID %s, got %s", replication.UUID, result.VolumeReplicationUuid.Value)
	}
	if result.LifeCycleState.Value != gcpgenserver.VolumeReplicationInternalV1betaLifeCycleStateCreating {
		t.Errorf("Expected LifeCycleState %s, got %s", gcpgenserver.VolumeReplicationInternalV1betaLifeCycleStateCreating, result.LifeCycleState.Value)
	}
	if result.LifeCycleStateDetails.Value != replication.StateDetails {
		t.Errorf("Expected LifeCycleStateDetails %s, got %s", replication.StateDetails, result.LifeCycleStateDetails.Value)
	}
	if result.EndpointType != gcpgenserver.VolumeReplicationInternalV1betaEndpointTypeSrc {
		t.Errorf("Expected EndpointType %s, got %s", gcpgenserver.VolumeReplicationInternalV1betaEndpointTypeSrc, result.EndpointType)
	}
	if result.SourceHostName != replication.ReplicationAttributes.SourceHostName {
		t.Errorf("Expected SourceHostName %s, got %s", replication.ReplicationAttributes.SourceHostName, result.SourceHostName)
	}
	if result.SourceServerName != replication.ReplicationAttributes.SourceSvmName {
		t.Errorf("Expected SourceServerName %s, got %s", replication.ReplicationAttributes.SourceSvmName, result.SourceServerName)
	}
	if result.SourceVolumeName != replication.ReplicationAttributes.SourceVolumeName {
		t.Errorf("Expected SourceVolumeName %s, got %s", replication.ReplicationAttributes.SourceVolumeName, result.SourceVolumeName)
	}
	if result.DestinationHostName != replication.ReplicationAttributes.DestinationHostName {
		t.Errorf("Expected DestinationHostName %s, got %s", replication.ReplicationAttributes.DestinationHostName, result.DestinationHostName)
	}
	if result.DestinationServerName != replication.ReplicationAttributes.DestinationSvmName {
		t.Errorf("Expected DestinationServerName %s, got %s", replication.ReplicationAttributes.DestinationSvmName, result.DestinationServerName)
	}
	if result.DestinationVolumeName != replication.ReplicationAttributes.DestinationVolumeName {
		t.Errorf("Expected DestinationVolumeName %s, got %s", replication.ReplicationAttributes.DestinationVolumeName, result.DestinationVolumeName)
	}
	if result.DestinationVolumeUuid.Value != replication.ReplicationAttributes.DestinationVolumeUUID {
		t.Errorf("Expected DestinationVolumeUuid %s, got %s", replication.ReplicationAttributes.DestinationVolumeUUID, result.DestinationVolumeUuid.Value)
	}
	if result.Name.Value != replication.Name {
		t.Errorf("Expected Name %s, got %s", replication.Name, result.Name.Value)
	}
	if result.MirrorState.Value != gcpgenserver.VolumeReplicationInternalV1betaMirrorStateMIRRORED {
		t.Errorf("Expected MirrorState %s, got %s", gcpgenserver.VolumeReplicationInternalV1betaMirrorStateMIRRORED, result.MirrorState.Value)
	}
	if result.RelationshipStatus.Value != gcpgenserver.VolumeReplicationInternalV1betaRelationshipStatusIdle {
		t.Errorf("Expected RelationshipStatus %s, got %s", gcpgenserver.VolumeReplicationInternalV1betaRelationshipStatusIdle, result.RelationshipStatus.Value)
	}
	if result.TotalProgress.Value != replication.TotalProgress {
		t.Errorf("Expected TotalProgress %d, got %d", replication.TotalProgress, result.TotalProgress.Value)
	}
	if result.Healthy.Value != replication.Healthy {
		t.Errorf("Expected Healthy %t, got %t", replication.Healthy, result.Healthy.Value)
	}
	if result.TotalTransferBytes.Value != replication.TotalTransferBytes {
		t.Errorf("Expected TotalTransferBytes %d, got %d", replication.TotalTransferBytes, result.TotalTransferBytes.Value)
	}
	if result.LagTime.Value != replication.LagTime {
		t.Errorf("Expected LagTime %d, got %d", replication.LagTime, result.LagTime.Value)
	}
	if result.LastTransferSize.Value != replication.LastTransferSize {
		t.Errorf("Expected LastTransferSize %d, got %d", replication.LastTransferSize, result.LastTransferSize.Value)
	}
	if result.LastTransferError.Value != replication.LastTransferError {
		t.Errorf("Expected LastTransferError %s, got %s", replication.LastTransferError, result.LastTransferError.Value)
	}
	if result.LastTransferDuration.Value != replication.LastTransferDuration {
		t.Errorf("Expected LastTransferDuration %d, got %d", replication.LastTransferDuration, result.LastTransferDuration.Value)
	}
	if replication.LastTransferEndTime != nil {
		if result.LastTransferEndTime.Value.Unix() != replication.LastTransferEndTime.Unix() {
			t.Errorf("Expected LastTransferEndTime %s, got %s", replication.LastTransferEndTime, result.LastTransferEndTime.Value)
		}
	}
	if replication.ProgressLastUpdated != nil {
		if result.ProgressLastUpdated.Value.Unix() != replication.ProgressLastUpdated.Unix() {
			t.Errorf("Expected ProgressLastUpdated %s, got %s", replication.ProgressLastUpdated, result.ProgressLastUpdated.Value)
		}
	}
	if result.LagTime.Value != replication.LagTime {
		t.Errorf("Expected LagTime %d, got %d", replication.LagTime, result.LagTime.Value)
	}
}

func TestConvertToPoolInternalV1Beta(t *testing.T) {
	timenow := time.Now()

	pool := &models.Pool{
		BaseModel: models.BaseModel{
			ID:        1,
			UUID:      "pool-uuid",
			CreatedAt: timenow,
			UpdatedAt: timenow,
			DeletedAt: nil,
		},
		Name:                    "Test Pool",
		Description:             "Test Pool Description",
		State:                   models.LifeCycleStateAvailable,
		StateDetails:            "Pool is available",
		SizeInBytes:             0,
		AccountName:             "test-account",
		VendorID:                "vendor-id",
		Region:                  "us-central1",
		Zone:                    "us-central1-a",
		TotalThroughputMibps:    0,
		UtilizedThroughputMibps: 0,
		AllowAutoTiering:        false,
		HotTierSizeInBytes:      0,
		EnableHotTierAutoResize: false,
		VendorSubNetID:          "vendor-subnet-id",
		QosType:                 "none",
		PoolAttributes: &models.PoolAttributes{
			NumberOfVolumes: int64(10),
		},
	}

	result := convertToPoolInternalV1Beta(pool)

	if result.PoolId.Value != pool.UUID {
		t.Errorf("Expected PoolId %s, got %s", pool.UUID, result.PoolId.Value)
	}
	if result.ResourceId != pool.Name {
		t.Errorf("Expected ResourceId %s, got %s", pool.Name, result.ResourceId)
	}
	if result.ServiceLevel != gcpgenserver.PoolInternalV1betaServiceLevel(pool.ServiceLevel) {
		t.Errorf("Expected ServiceLevel %s, got %s", pool.ServiceLevel, result.ServiceLevel)
	}
	if result.QosType.Value != pool.QosType {
		t.Errorf("Expected QosType %s, got %s", pool.QosType, result.QosType.Value)
	}
	if result.SizeInBytes != float64(pool.SizeInBytes) {
		t.Errorf("Expected SizeInBytes %d, got %f", pool.SizeInBytes, result.SizeInBytes)
	}
	if result.TotalThroughputMibps.Value != pool.TotalThroughputMibps {
		t.Errorf("Expected TotalThroughputMibps %f, got %f", pool.TotalThroughputMibps, result.TotalThroughputMibps.Value)
	}
	if result.AvailableThroughputMibps.Value != pool.TotalThroughputMibps-pool.UtilizedThroughputMibps {
		t.Errorf("Expected AvailableThroughputMibps %f, got %f", pool.TotalThroughputMibps-pool.UtilizedThroughputMibps, result.AvailableThroughputMibps.Value)
	}
	if result.NumberOfVolumes.Value != int32(pool.PoolAttributes.NumberOfVolumes) {
		t.Errorf("Expected NumberOfVolumes %d, got %d", pool.PoolAttributes.NumberOfVolumes, result.NumberOfVolumes.Value)
	}
	if result.StoragePoolState.Value != gcpgenserver.PoolInternalV1betaStoragePoolState(pool.State) {
		t.Errorf("Expected StoragePoolState %s, got %s", gcpgenserver.PoolInternalV1betaStoragePoolState(pool.State), result.StoragePoolState.Value)
	}
	if result.StoragePoolStateDetails.Value != pool.StateDetails {
		t.Errorf("Expected StoragePoolStateDetails %s, got %s", pool.StateDetails, result.StoragePoolStateDetails.Value)
	}
	if result.CreatedAt.Value.Unix() != pool.CreatedAt.Unix() {
		t.Errorf("Expected CreatedAt %s, got %s", pool.CreatedAt, result.CreatedAt.Value)
	}
	if result.UpdatedAt.Value.Unix() != pool.UpdatedAt.Unix() {
		t.Errorf("Expected UpdatedAt %s, got %s", pool.UpdatedAt, result.UpdatedAt.Value)
	}
	if result.StateDetails.Value != pool.StateDetails {
		t.Errorf("Expected StateDetails %s, got %s", pool.StateDetails, result.StateDetails.Value)
	}
	if result.Description.Value != pool.Description {
		t.Errorf("Expected Description %s, got %s", pool.Description, result.Description.Value)
	}
	if result.Zone.Value != pool.Zone {
		t.Errorf("Expected Zone %s, got %s", pool.Zone, result.Zone.Value)
	}
	if result.AllowAutoTiering.Value != pool.AllowAutoTiering {
		t.Errorf("Expected AllowAutoTiering %t, got %t", pool.AllowAutoTiering, result.AllowAutoTiering.Value)
	}
}

func TestConvertToVolumeReplicationsInternalV1Beta(t *testing.T) {
	timeNow := time.Now()
	snapmirrored := models.OntapSnapmirrored
	snapmirrorRelationshipIdle := models.SnapmirrorRelationshipIdle

	replications := []*datamodel.VolumeReplication{
		{
			BaseModel: datamodel.BaseModel{
				ID:        123,
				UUID:      "some-uuid",
				CreatedAt: timeNow,
				UpdatedAt: timeNow,
				DeletedAt: nil,
			},
			Name:         "Test Replication",
			Description:  "Test Description",
			State:        models.LifeCycleStateCreating,
			StateDetails: "Test State Details",
			Uri:          "projects/45110233509/locations/australia-southeast1/volume/godpvolume4/replications/replication-name-6",
			RemoteUri:    "projects/45110233509/locations/us-east4/volume/gosrcvolume1/replications/replication-name-6",
			ReplicationAttributes: &datamodel.ReplicationDetails{
				EndpointType:               "src",
				ReplicationSchedule:        "daily",
				SourcePoolUUID:             "source-pool-uuid",
				SourceVolumeUUID:           "source-volume-uuid",
				SourceLocation:             "source-location",
				SourceHostName:             "source-hostname",
				SourceReplicationUUID:      "source-replication-uuid",
				SourceSvmName:              "source-svm-name",
				SourceVolumeName:           "source-volume-name",
				DestinationPoolUUID:        "destination-pool-uuid",
				DestinationVolumeUUID:      "destination-volume-uuid",
				DestinationLocation:        "destination-location",
				DestinationHostName:        "destination-hostname",
				DestinationReplicationUUID: "destination-replication-uuid",
				DestinationSvmName:         "destination-svm-name",
				DestinationVolumeName:      "destination-volume-name",
				ExternalUUID:               "external-uuid",
			},
			MirrorState:           &snapmirrored,
			RelationshipStatus:    &snapmirrorRelationshipIdle,
			TotalProgress:         100,
			TotalTransferBytes:    1000000,
			TotalTransferTimeSecs: 3600,
			LastTransferSize:      500000,
			LastTransferError:     "no error",
			LastTransferDuration:  1800,
			LastTransferEndTime:   &timeNow,
			ProgressLastUpdated:   &timeNow,
			LastUpdatedFromOntap:  timeNow,
			Healthy:               false,
			UnhealthyReason:       "No issues detected",
			LagTime:               30,
			AccountID:             1,
			VolumeID:              1,
		},
	}

	result := convertToVolumeReplicationsInternalV1Beta(replications)

	if len(result) != 1 {
		t.Errorf("Expected 1 replication, got %d", len(result))
	}
	if result[0].VolumeReplicationUuid.Value != replications[0].UUID {
		t.Errorf("Expected UUID %s, got %s", replications[0].UUID, result[0].VolumeReplicationUuid.Value)
	}
	if result[0].Name.Value != replications[0].Name {
		t.Errorf("Expected Name %s, got %s", replications[0].Name, result[0].Name.Value)
	}
	if result[0].SourceHostName != replications[0].ReplicationAttributes.SourceHostName {
		t.Errorf("Expected SourceHostName %s, got %s", replications[0].ReplicationAttributes.SourceHostName, result[0].SourceHostName)
	}
	if result[0].DestinationHostName != replications[0].ReplicationAttributes.DestinationHostName {
		t.Errorf("Expected DestinationHostName %s, got %s", replications[0].ReplicationAttributes.DestinationHostName, result[0].DestinationHostName)
	}
}
