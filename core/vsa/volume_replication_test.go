package vsa

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	ontaprest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestCreateVolumeReplicationSchedule(t *testing.T) {
	t.Run("WhenGetScheduleReturnsError", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockClusterClient := new(ontaprest.MockClusterClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		ontapProvider := &OntapRestProvider{}
		returnedError := fmt.Errorf("some Error")
		schedule := VolumeReplicationSchedule10Minutely
		params := &ontaprest.ScheduleCollectionGetParams{
			Name: schedule,
		}
		mockClient.On("Cluster").Return(mockClusterClient)
		mockClusterClient.On("ScheduleCollectionGet", params, mock.Anything).Return(returnedError)

		err := createVolumeReplicationSchedule(ontapProvider, schedule)
		assert.Equal(tt, returnedError, err)
	})
	t.Run("WhenCreate10minutelyScheduleReturnsError", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockClusterClient := new(ontaprest.MockClusterClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		ontapProvider := &OntapRestProvider{}
		returnedError := fmt.Errorf("some Error")
		schedule := VolumeReplicationSchedule10Minutely
		params := &ontaprest.ScheduleCollectionGetParams{
			Name: schedule,
		}
		mockClient.On("Cluster").Return(mockClusterClient)
		mockClusterClient.On("ScheduleCollectionGet", params, mock.Anything).Return(nil)
		mockClusterClient.On("ScheduleCreate", mock.Anything).Return(returnedError)

		err := createVolumeReplicationSchedule(ontapProvider, schedule)
		assert.Equal(tt, returnedError, err)
	})
	t.Run("WhenCreateHourlyScheduleReturnsError", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockClusterClient := new(ontaprest.MockClusterClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		ontapProvider := &OntapRestProvider{}
		returnedError := fmt.Errorf("some Error")
		schedule := VolumeReplicationScheduleHourly
		params := &ontaprest.ScheduleCollectionGetParams{
			Name: schedule,
		}
		mockClient.On("Cluster").Return(mockClusterClient)
		mockClusterClient.On("ScheduleCollectionGet", params, mock.Anything).Return(nil)
		mockClusterClient.On("ScheduleCreate", mock.Anything).Return(returnedError)

		err := createVolumeReplicationSchedule(ontapProvider, schedule)
		assert.Equal(tt, returnedError, err)
	})
	t.Run("WhenCreateDailyScheduleReturnsError", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockClusterClient := new(ontaprest.MockClusterClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		ontapProvider := &OntapRestProvider{}
		returnedError := fmt.Errorf("some Error")
		schedule := VolumeReplicationScheduleDaily
		params := &ontaprest.ScheduleCollectionGetParams{
			Name: schedule,
		}
		mockClient.On("Cluster").Return(mockClusterClient)
		mockClusterClient.On("ScheduleCollectionGet", params, mock.Anything).Return(nil)
		mockClusterClient.On("ScheduleCreate", mock.Anything).Return(returnedError)

		err := createVolumeReplicationSchedule(ontapProvider, schedule)
		assert.Equal(tt, returnedError, err)
	})
	t.Run("WhenInvalidSchedule", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockClusterClient := new(ontaprest.MockClusterClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		ontapProvider := &OntapRestProvider{}
		schedule := "InvalidSchedule"
		params := &ontaprest.ScheduleCollectionGetParams{
			Name: schedule,
		}
		expectedError := errors.NewUserInputValidationErr("Unknown replication schedule")
		mockClient.On("Cluster").Return(mockClusterClient)
		mockClusterClient.On("ScheduleCollectionGet", params, mock.Anything).Return(nil)

		err := createVolumeReplicationSchedule(ontapProvider, schedule)
		assert.Equal(tt, expectedError, err)
	})
	t.Run("WhenScheduleExists", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockClusterClient := new(ontaprest.MockClusterClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		ontapProvider := &OntapRestProvider{}
		scheduleName := VolumeReplicationScheduleDaily
		params := &ontaprest.ScheduleCollectionGetParams{
			Name: scheduleName,
		}
		mockClient.On("Cluster").Return(mockClusterClient)
		mockClusterClient.On("ScheduleCollectionGet", params, mock.Anything).Return(nil)
		mockClusterClient.On("ScheduleCreate", mock.Anything).Return(nil)
		err := createVolumeReplicationSchedule(ontapProvider, scheduleName)
		assert.NoError(tt, err)
	})
}

func TestCreateVolumeReplication(t *testing.T) {
	srcVolumeUUID := "src-uuid"
	dstVolumeUUID := "dst-uuid"
	srcVolume := ontaprest.Volume{
		Volume: models.Volume{
			UUID: &srcVolumeUUID,
			Type: nillable.ToPointer(models.VolumeTypeRw),
			Name: nillable.ToPointer("srcvol"),
			Svm: &models.VolumeInlineSvm{
				Name: nillable.ToPointer("srcsvm"),
			},
		},
	}
	dstVolume := ontaprest.Volume{
		Volume: models.Volume{
			UUID: &dstVolumeUUID,
			Type: nillable.ToPointer(models.VolumeTypeDp),
			Name: nillable.ToPointer("dstvol"),
			Svm: &models.VolumeInlineSvm{
				Name: nillable.ToPointer("dstsvm"),
			},
		},
	}
	volume1 := &Volume{
		ExternalUUID: dstVolumeUUID,
	}
	volumeGetParams := &ontaprest.VolumeGetParams{UUID: *dstVolume.UUID}
	volumeReplicationCreateParams := CreateVolumeReplicationParams{
		VolumeReplication: &VolumeReplication{
			Volume:                volume1,
			ReplicationPolicy:     "MirrorAllSnapshots",
			ReplicationSchedule:   VolumeReplicationScheduleDaily,
			SourceSVMName:         *srcVolume.Svm.Name,
			SourceVolumeName:      *srcVolume.Name,
			DestinationSVMName:    *dstVolume.Svm.Name,
			DestinationVolumeName: *dstVolume.Name,
		},
	}
	var snapmirrorEmptyList []*ontaprest.SnapmirrorRelationship

	listParams := &ontaprest.SnapmirrorRelationshipListParams{}
	createParams := &ontaprest.SnapmirrorRelationshipCreateParams{
		SourcePath:      volumeReplicationCreateParams.VolumeReplication.SourcePath(),
		DestinationPath: volumeReplicationCreateParams.VolumeReplication.DestinationPath(),
		Policy:          volumeReplicationCreateParams.VolumeReplication.ReplicationPolicy,
		Schedule:        &volumeReplicationCreateParams.VolumeReplication.ReplicationSchedule,
	}

	sourcePath := nillable.GetString(srcVolume.Svm.Name, "") + ":" + nillable.GetString(srcVolume.Name, "")
	destinationPath := nillable.GetString(dstVolume.Svm.Name, "") + ":" + nillable.GetString(dstVolume.Name, "")
	snapmirror := &ontaprest.SnapmirrorRelationship{
		SnapmirrorRelationship: models.SnapmirrorRelationship{
			Source:                &models.SnapmirrorSourceEndpoint{Path: &sourcePath},
			Destination:           &models.SnapmirrorEndpoint{Path: &destinationPath},
			UUID:                  nillable.ToPointer(strfmt.UUID("uuid")),
			State:                 nillable.ToPointer(models.SnapmirrorRelationshipStateSnapmirrored),
			TotalTransferDuration: nillable.ToPointer("PT2M34S"),
			Transfer: &models.SnapmirrorRelationshipInlineTransfer{
				TotalDuration:    nillable.ToPointer("PT4M50S"),
				State:            nillable.ToPointer(models.SnapmirrorRelationshipInlineTransferStateSuccess),
				BytesTransferred: nillable.ToPointer(int64(500)),
				EndTime:          nillable.ToPointer(strfmt.DateTime(time.Now())),
			},
			Policy: &models.SnapmirrorRelationshipInlinePolicy{
				Name: nillable.ToPointer("policy"),
			},
			LagTime: nillable.ToPointer("PT20S"),
			SnapmirrorRelationshipInlineUnhealthyReason: []*models.SnapmirrorError{
				{
					Message: nillable.ToPointer("error"),
				},
			},
			TransferSchedule: &models.SnapmirrorRelationshipInlineTransferSchedule{
				Name: nillable.ToPointer("le schedule"),
			},
			Healthy:            nillable.GetBoolPtr(true),
			TotalTransferBytes: nillable.ToPointer(int64(1000)),
		},
	}
	getParams := &ontaprest.SnapmirrorRelationshipGetParams{UUID: snapmirror.UUID.String()}
	jobAccepted := &ontaprest.JobAccepted{JobUUID: "jobUUID"}
	doEnsureSvmPeering = func(provider *OntapRestProvider, params *CreateVolumeReplicationParams) error {
		return nil
	}
	doCreateVolumeReplicationScheduleIfNeeded = func(provider *OntapRestProvider, schedule string) (err error) {
		return nil
	}

	defer func() {
		doEnsureSvmPeering = ensureSvmPeering
		doCreateVolumeReplicationScheduleIfNeeded = createVolumeReplicationSchedule
	}()

	t.Run("WhenSnapmirrorCreateReturnsError", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorageClient := new(ontaprest.MockStorageClient)
		mockSnapmirrorClient := new(ontaprest.MockSnapmirrorClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		ontapProvider := &OntapRestProvider{}
		expectedError := errors.New("Faceplanting")

		mockClient.On("Storage").Return(mockStorageClient)
		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockStorageClient.On("VolumeGet", volumeGetParams).Return(&dstVolume, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipListDestinations", mock.Anything).Return(snapmirrorEmptyList, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipList", listParams).Return(snapmirrorEmptyList, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipCreate", createParams).Return(nil, nil, expectedError).Times(1)
		volumeReplication, err := ontapProvider.CreateVolumeReplication(&volumeReplicationCreateParams)
		assert.Equal(tt, expectedError, err)
		assert.Empty(tt, volumeReplication)
	})
	t.Run("WhenSnapmirrorCreatePollReturnsError", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorageClient := new(ontaprest.MockStorageClient)
		mockSnapmirrorClient := new(ontaprest.MockSnapmirrorClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		provider := &OntapRestProvider{}
		expectedError := errors.New("Faceplanting")

		mockClient.On("Storage").Return(mockStorageClient)
		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockStorageClient.On("VolumeGet", volumeGetParams).Return(&dstVolume, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipListDestinations", mock.Anything).Return(snapmirrorEmptyList, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipList", listParams).Return(snapmirrorEmptyList, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipCreate", createParams).Return(nil, jobAccepted, nil).Times(1)
		mockClient.On("Poll", jobAccepted.JobUUID).Return(expectedError).Times(1)

		volumeReplication, err := provider.CreateVolumeReplication(&volumeReplicationCreateParams)
		assert.Equal(tt, expectedError, err)
		assert.Empty(tt, volumeReplication)
	})
	t.Run("WhenSnapmirrorResyncOrInitializeReturnsError", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorageClient := new(ontaprest.MockStorageClient)
		mockSnapmirrorClient := new(ontaprest.MockSnapmirrorClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		provider := &OntapRestProvider{}
		expectedError := errors.New("Faceplanting")

		mockClient.On("Storage").Return(mockStorageClient)
		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockStorageClient.On("VolumeGet", volumeGetParams).Return(&dstVolume, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipListDestinations", mock.Anything).Return(snapmirrorEmptyList, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipList", listParams).Return(snapmirrorEmptyList, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipCreate", createParams).Return(snapmirror, jobAccepted, nil).Times(1)
		mockClient.On("Poll", jobAccepted.JobUUID).Return(nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipResyncOrInitializeOrResume", snapmirror.UUID.String()).Return(nil, nil, expectedError).Times(1)

		volumeReplication, err := provider.CreateVolumeReplication(&volumeReplicationCreateParams)
		assert.Equal(tt, expectedError, err)
		assert.Empty(tt, volumeReplication)
	})
	t.Run("WhenSnapmirrorResyncOrInitializePollReturnsError", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorageClient := new(ontaprest.MockStorageClient)
		mockSnapmirrorClient := new(ontaprest.MockSnapmirrorClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		provider := &OntapRestProvider{}
		expectedError := errors.New("Faceplanting")

		mockClient.On("Storage").Return(mockStorageClient)
		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockStorageClient.On("VolumeGet", volumeGetParams).Return(&dstVolume, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipListDestinations", mock.Anything).Return(snapmirrorEmptyList, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipList", listParams).Return(snapmirrorEmptyList, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipCreate", createParams).Return(snapmirror, jobAccepted, nil).Times(1)
		mockClient.On("Poll", jobAccepted.JobUUID).Return(nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipResyncOrInitializeOrResume", snapmirror.UUID.String()).Return(nil, jobAccepted, nil).Times(1)
		mockClient.On("Poll", jobAccepted.JobUUID).Return(expectedError).Times(1)

		volumeReplication, err := provider.CreateVolumeReplication(&volumeReplicationCreateParams)
		assert.Equal(tt, expectedError, err)
		assert.Empty(tt, volumeReplication)
	})
	t.Run("WhenSnapmirrorGetReturnsError", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorageClient := new(ontaprest.MockStorageClient)
		mockSnapmirrorClient := new(ontaprest.MockSnapmirrorClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		provider := &OntapRestProvider{}
		expectedError := errors.New("Faceplanting")

		mockClient.On("Storage").Return(mockStorageClient)
		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockStorageClient.On("VolumeGet", volumeGetParams).Return(&dstVolume, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipListDestinations", mock.Anything).Return(snapmirrorEmptyList, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipList", listParams).Return(snapmirrorEmptyList, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipCreate", createParams).Return(snapmirror, jobAccepted, nil).Times(1)
		mockClient.On("Poll", jobAccepted.JobUUID).Return(nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipResyncOrInitializeOrResume", snapmirror.UUID.String()).Return(nil, jobAccepted, nil).Times(1)
		mockClient.On("Poll", jobAccepted.JobUUID).Return(nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipGet", getParams).Return(nil, expectedError).Times(1)

		volumeReplication, err := provider.CreateVolumeReplication(&volumeReplicationCreateParams)
		assert.Equal(tt, expectedError, err)
		assert.Empty(tt, volumeReplication)
	})
	t.Run("WhenSnapmirrorWaitForMirrorStateReturnsError", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorageClient := new(ontaprest.MockStorageClient)
		mockSnapmirrorClient := new(ontaprest.MockSnapmirrorClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		provider := &OntapRestProvider{}
		expectedError := errors.New("Faceplanting")
		volumeReplicationCreateParams.ReverseResync = true

		mockClient.On("Storage").Return(mockStorageClient)
		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockStorageClient.On("VolumeGet", volumeGetParams).Return(&dstVolume, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipListDestinations", mock.Anything).Return(snapmirrorEmptyList, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipList", listParams).Return(snapmirrorEmptyList, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipCreate", createParams).Return(snapmirror, jobAccepted, nil).Times(1)
		mockClient.On("Poll", jobAccepted.JobUUID).Return(nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipResyncOrInitializeOrResume", snapmirror.UUID.String()).Return(nil, jobAccepted, nil).Times(1)
		mockClient.On("Poll", jobAccepted.JobUUID).Return(nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipGet", getParams).Return(snapmirror, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipGet", getParams).Return(nil, expectedError).Times(1)

		volumeReplication, err := provider.CreateVolumeReplication(&volumeReplicationCreateParams)
		assert.Equal(tt, expectedError, err)
		assert.Empty(tt, volumeReplication)

		volumeReplicationCreateParams.ReverseResync = false
	})
	t.Run("WhenSnapmirrorGetVolumeForLanguageReturnsError", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorageClient := new(ontaprest.MockStorageClient)
		mockSnapmirrorClient := new(ontaprest.MockSnapmirrorClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		provider := &OntapRestProvider{}
		expectedError := errors.New("Faceplanting")

		mockClient.On("Storage").Return(mockStorageClient)
		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockStorageClient.On("VolumeGet", volumeGetParams).Return(&dstVolume, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipListDestinations", mock.Anything).Return(snapmirrorEmptyList, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipList", listParams).Return(snapmirrorEmptyList, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipCreate", createParams).Return(snapmirror, jobAccepted, nil).Times(1)
		mockClient.On("Poll", jobAccepted.JobUUID).Return(nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipResyncOrInitializeOrResume", snapmirror.UUID.String()).Return(nil, jobAccepted, nil).Times(1)
		mockClient.On("Poll", jobAccepted.JobUUID).Return(nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipGet", getParams).Return(snapmirror, nil).Times(1)
		mockStorageClient.On("VolumeGet", &ontaprest.VolumeGetParams{BaseParams: ontaprest.BaseParams{Fields: []string{"language"}}, UUID: *dstVolume.UUID}).Return(nil, expectedError).Times(1)

		volumeReplication, err := provider.CreateVolumeReplication(&volumeReplicationCreateParams)
		assert.Equal(tt, expectedError, err)
		assert.Empty(tt, volumeReplication)
	})
	t.Run("WhenSuccessful", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorageClient := new(ontaprest.MockStorageClient)
		mockSnapmirrorClient := new(ontaprest.MockSnapmirrorClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		provider := &OntapRestProvider{}
		volumeReplicationCreateParams.ReverseResync = true

		mockClient.On("Storage").Return(mockStorageClient)
		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockStorageClient.On("VolumeGet", volumeGetParams).Return(&dstVolume, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipListDestinations", mock.Anything).Return(snapmirrorEmptyList, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipList", listParams).Return(snapmirrorEmptyList, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipCreate", createParams).Return(snapmirror, jobAccepted, nil).Times(1)
		mockClient.On("Poll", jobAccepted.JobUUID).Return(nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipResyncOrInitializeOrResume", snapmirror.UUID.String()).Return(nil, jobAccepted, nil).Times(1)
		mockClient.On("Poll", jobAccepted.JobUUID).Return(nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipGet", getParams).Return(snapmirror, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipGet", getParams).Return(snapmirror, nil).Times(1)

		volumeReplication, err := provider.CreateVolumeReplication(&volumeReplicationCreateParams)
		assert.NoError(tt, err)
		assert.NotEmpty(tt, volumeReplication)

		volumeReplicationCreateParams.ReverseResync = false
	})
	t.Run("WhenSuccessfulWithVolumeLanguage", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorageClient := new(ontaprest.MockStorageClient)
		mockSnapmirrorClient := new(ontaprest.MockSnapmirrorClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		provider := &OntapRestProvider{}
		languageCode := "c.utf-8"
		dstVolume.Language = &languageCode

		mockClient.On("Storage").Return(mockStorageClient)
		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockStorageClient.On("VolumeGet", volumeGetParams).Return(&dstVolume, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipListDestinations", mock.Anything).Return(snapmirrorEmptyList, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipList", listParams).Return(snapmirrorEmptyList, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipCreate", createParams).Return(snapmirror, jobAccepted, nil).Times(1)
		mockClient.On("Poll", jobAccepted.JobUUID).Return(nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipResyncOrInitializeOrResume", snapmirror.UUID.String()).Return(nil, jobAccepted, nil).Times(1)
		mockClient.On("Poll", jobAccepted.JobUUID).Return(nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipGet", getParams).Return(snapmirror, nil).Times(1)
		mockStorageClient.On("VolumeGet", &ontaprest.VolumeGetParams{BaseParams: ontaprest.BaseParams{Fields: []string{"language"}}, UUID: *dstVolume.UUID}).Return(&dstVolume, nil).Times(1)

		volumeReplication, err := provider.CreateVolumeReplication(&volumeReplicationCreateParams)
		assert.NoError(tt, err)
		assert.NotNil(tt, volumeReplication.Volume.Language)
		assert.Equal(tt, "c.utf-8", *volumeReplication.Volume.Language)
		assert.NotEmpty(tt, volumeReplication)

		dstVolume.Language = nil
	})
}

func TestConvertSnapmirrorToVolumeReplication(t *testing.T) {
	snapmirror := ontaprest.SnapmirrorRelationship{
		SnapmirrorRelationship: models.SnapmirrorRelationship{
			UUID:                  nillable.ToPointer(strfmt.UUID("uuid")),
			State:                 nillable.ToPointer(models.SnapmirrorRelationshipStateSnapmirrored),
			TotalTransferDuration: nillable.ToPointer("PT2M34S"),
			Transfer: &models.SnapmirrorRelationshipInlineTransfer{
				TotalDuration:    nillable.ToPointer("PT4M50S"),
				State:            nillable.ToPointer(models.SnapmirrorRelationshipInlineTransferStateSuccess),
				BytesTransferred: nillable.ToPointer(int64(500)),
				EndTime:          nillable.ToPointer(strfmt.DateTime(time.Now())),
			},
			Policy: &models.SnapmirrorRelationshipInlinePolicy{
				Name: nillable.ToPointer("policy"),
			},
			LagTime: nillable.ToPointer("PT20S"),
			SnapmirrorRelationshipInlineUnhealthyReason: []*models.SnapmirrorError{
				{
					Message: nillable.ToPointer("error"),
				},
			},
			TransferSchedule: &models.SnapmirrorRelationshipInlineTransferSchedule{
				Name: nillable.ToPointer("le schedule"),
			},
			Healthy:            nillable.GetBoolPtr(true),
			TotalTransferBytes: nillable.ToPointer(int64(1000)),
		},
	}
	volumeReplication := &VolumeReplication{}

	t.Run("WhenParsingISO8601Time", func(tt *testing.T) {
		volumeReplicationRes, err := convertSnapMirrorToVolumeReplication(snapmirror, volumeReplication)
		assert.NoError(tt, err)
		assert.NotEmpty(tt, volumeReplicationRes)
		assert.Equal(tt, volumeReplicationRes.TotalTransferTimeSecs, int64(154))
		assert.Equal(tt, volumeReplicationRes.LastTransferDuration, int64(290))
		assert.Equal(tt, volumeReplicationRes.LagTime, int64(20))
	})
	t.Run("WhenTotalTransferDurationReturnsError", func(tt *testing.T) {
		snapmirror.TotalTransferDuration = nillable.ToPointer("some-invalid-string")
		expectedError := errors.New("unexpected input")

		volumeReplicationRes, err := convertSnapMirrorToVolumeReplication(snapmirror, volumeReplication)
		assert.Equal(tt, expectedError, err)
		assert.Empty(tt, volumeReplicationRes)
		snapmirror.TotalTransferDuration = nillable.ToPointer("PT2M34S")
	})
	t.Run("WhenTotalDurationReturnsError", func(tt *testing.T) {
		snapmirror.Transfer.TotalDuration = nillable.ToPointer("some-invalid-string")
		expectedError := errors.New("unexpected input")

		volumeReplicationRes, err := convertSnapMirrorToVolumeReplication(snapmirror, volumeReplication)
		assert.Equal(tt, expectedError, err)
		assert.Empty(tt, volumeReplicationRes)
		snapmirror.Transfer.TotalDuration = nillable.ToPointer("PT4M50S")
	})
	t.Run("WhenLagTimeReturnsError", func(tt *testing.T) {
		snapmirror.LagTime = nillable.ToPointer("some-invalid-string")
		expectedError := errors.New("unexpected input")

		volumeReplicationRes, err := convertSnapMirrorToVolumeReplication(snapmirror, volumeReplication)
		assert.Equal(tt, expectedError, err)
		assert.Empty(tt, volumeReplicationRes)
		snapmirror.LagTime = nillable.ToPointer("PT20S")
	})
	t.Run("WhenTransferIsNil", func(tt *testing.T) {
		snapmirror.Transfer = nil
		volumeReplicationRes, err := convertSnapMirrorToVolumeReplication(snapmirror, volumeReplication)
		assert.NoError(tt, err)
		assert.NotEmpty(tt, volumeReplicationRes)
		snapmirror.Transfer = &models.SnapmirrorRelationshipInlineTransfer{
			TotalDuration:    nillable.ToPointer("PT4M50S"),
			State:            nillable.ToPointer(models.SnapmirrorRelationshipInlineTransferStateSuccess),
			BytesTransferred: nillable.ToPointer(int64(500)),
			EndTime:          nillable.ToPointer(strfmt.DateTime(time.Now())),
		}
	})
	t.Run("WhenEndTimeIsNil", func(tt *testing.T) {
		var nilEndTime *time.Time
		snapmirror.Transfer.EndTime = nil
		volumeReplicationRes, err := convertSnapMirrorToVolumeReplication(snapmirror, volumeReplication)
		assert.NoError(tt, err)
		assert.NotEmpty(tt, volumeReplicationRes)
		assert.Equal(tt, nilEndTime, volumeReplicationRes.LastTransferEndTime)
		snapmirror.Transfer.EndTime = nillable.ToPointer(strfmt.DateTime(time.Now()))
	})
	t.Run("WhenConversionSuccessful", func(tt *testing.T) {
		volumeReplicationRes, err := convertSnapMirrorToVolumeReplication(snapmirror, volumeReplication)
		assert.NoError(tt, err)
		assert.NotEmpty(tt, volumeReplicationRes)
	})
}

func TestAuthorizeVolumeReplication(t *testing.T) {
	t.Run("WhenDoEnsureSvmPeeringReturnsError", func(tt *testing.T) {
		provider := &OntapRestProvider{}
		params := &CreateVolumeReplicationParams{}
		doEnsureSvmPeering = func(provider *OntapRestProvider, params *CreateVolumeReplicationParams) error {
			return errors.New("some error")
		}
		defer func() { doEnsureSvmPeering = ensureSvmPeering }()
		volRep, err := provider.AuthorizeVolumeReplication(params)
		assert.Error(tt, err)
		assert.Nil(tt, volRep)
	})
	t.Run("WhenSuccessful", func(tt *testing.T) {
		provider := &OntapRestProvider{}
		params := &CreateVolumeReplicationParams{}
		doEnsureSvmPeering = func(provider *OntapRestProvider, params *CreateVolumeReplicationParams) error {
			return nil
		}
		defer func() { doEnsureSvmPeering = ensureSvmPeering }()
		volRep, err := provider.AuthorizeVolumeReplication(params)
		assert.NoError(tt, err)
		assert.Nil(tt, volRep)
	})
}

func TestDeleteVolumeReplication(t *testing.T) {
	testUUID := utils.RandomUUID()
	formattedUUID := strfmt.UUID(testUUID)
	delParams := &ontaprest.SnapmirrorRelationshipDeleteParams{UUID: testUUID}
	getParams := &ontaprest.SnapmirrorRelationshipGetParams{UUID: testUUID}
	listParams := &ontaprest.SnapmirrorRelationshipListParams{DestinationPath: ":", SourcePath: ":"}
	jobAccepted := &ontaprest.JobAccepted{JobUUID: "jobUUID"}
	mirrorState := "idle"
	volumeReplicationDeleteParams := DeleteVolumeReplicationParams{VolumeReplication: &VolumeReplication{RelationshipID: testUUID}}
	returnSnapmirror := ontaprest.SnapmirrorRelationship{SnapmirrorRelationship: models.SnapmirrorRelationship{UUID: &formattedUUID, State: &mirrorState}}
	t.Run("WhenClientReturnsError", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorageClient := new(ontaprest.MockStorageClient)
		mockSnapmirrorClient := new(ontaprest.MockSnapmirrorClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		provider := &OntapRestProvider{}
		expectedError := errors.New("Faceplanting")

		mockClient.On("Storage").Return(mockStorageClient)
		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipGet", getParams).Return(nil, expectedError).Times(1)
		volumeReplication, err := provider.DeleteVolumeReplication(&volumeReplicationDeleteParams)
		assert.Equal(tt, expectedError, err)
		assert.Empty(tt, volumeReplication)
	})
	t.Run("WhenEntryNotFound", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockSnapmirrorClient := new(ontaprest.MockSnapmirrorClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		provider := &OntapRestProvider{}
		doCleanupSvmPeering = func(provider *OntapRestProvider, params *DeleteVolumeReplicationParams) error {
			return nil
		}
		expectedError := errors.NewNotFoundErr("", nil)

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipGet", getParams).Return(nil, expectedError).Times(1)

		volumeReplication, err := provider.DeleteVolumeReplication(&volumeReplicationDeleteParams)
		assert.NoError(tt, err)
		assert.Equal(tt, volumeReplication, &VolumeReplication{RelationshipID: testUUID})
		doCleanupSvmPeering = cleanupSvmPeering
	})
	t.Run("WhenInMirroredRelationship", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockSnapmirrorClient := new(ontaprest.MockSnapmirrorClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		provider := &OntapRestProvider{}
		snapmirrorState := SnapmirrorStateMirrored

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipGet", getParams).Return(&ontaprest.SnapmirrorRelationship{
			SnapmirrorRelationship: models.SnapmirrorRelationship{
				State: &snapmirrorState,
				UUID:  &formattedUUID,
			},
		}, nil).Times(1)

		_, err := provider.DeleteVolumeReplication(&volumeReplicationDeleteParams)
		assert.Error(tt, err, "Cannot delete a relationship in the current mirror state")
	})
	t.Run("WhenDeleteReturnsAnError", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockSnapmirrorClient := new(ontaprest.MockSnapmirrorClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		provider := &OntapRestProvider{}
		doCleanupSvmPeering = func(provider *OntapRestProvider, params *DeleteVolumeReplicationParams) error {
			return nil
		}

		expectedError := errors.New("Faceplanting")

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipGet", getParams).Return(&returnSnapmirror, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipDelete", delParams).Return(false, nil, expectedError).Times(1)
		volumeReplication, err := provider.DeleteVolumeReplication(&volumeReplicationDeleteParams)
		assert.Equal(tt, expectedError, err)
		assert.Empty(tt, volumeReplication)
		doCleanupSvmPeering = cleanupSvmPeering
	})
	t.Run("WhenDeleteReturnsEntryNotFound", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockSnapmirrorClient := new(ontaprest.MockSnapmirrorClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		provider := &OntapRestProvider{}

		doCleanupSvmPeering = func(provider *OntapRestProvider, params *DeleteVolumeReplicationParams) error {
			return nil
		}
		expectedError := errors.New("entry not found")

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipGet", getParams).Return(&returnSnapmirror, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipDelete", delParams).Return(false, nil, expectedError).Times(1)
		volumeReplication, err := provider.DeleteVolumeReplication(&volumeReplicationDeleteParams)
		assert.NoError(tt, err)
		assert.NotNil(tt, volumeReplication)
		doCleanupSvmPeering = cleanupSvmPeering
	})
	t.Run("WhenPollingFails", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockSnapmirrorClient := new(ontaprest.MockSnapmirrorClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		provider := &OntapRestProvider{}

		doCleanupSvmPeering = func(provider *OntapRestProvider, params *DeleteVolumeReplicationParams) error {
			return nil
		}
		expectedError := errors.New("Faceplanting")

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipGet", getParams).Return(&returnSnapmirror, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipDelete", delParams).Return(false, jobAccepted, nil).Times(1)
		mockClient.On("Poll", jobAccepted.JobUUID).Return(expectedError).Times(1)
		volumeReplication, err := provider.DeleteVolumeReplication(&volumeReplicationDeleteParams)
		assert.Equal(tt, expectedError, err)
		assert.Empty(tt, volumeReplication)
		doCleanupSvmPeering = cleanupSvmPeering
	})
	t.Run("WhenPollingReturnsNotFound", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockSnapmirrorClient := new(ontaprest.MockSnapmirrorClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		provider := &OntapRestProvider{}

		doCleanupSvmPeering = func(provider *OntapRestProvider, params *DeleteVolumeReplicationParams) error {
			return nil
		}
		expectedError := errors.New("entry not found")

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipGet", getParams).Return(&returnSnapmirror, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipDelete", delParams).Return(false, jobAccepted, nil).Times(1)
		mockClient.On("Poll", jobAccepted.JobUUID).Return(expectedError).Times(1)

		volumeReplication, err := provider.DeleteVolumeReplication(&volumeReplicationDeleteParams)
		assert.NoError(tt, err)
		assert.NotNil(tt, volumeReplication)
		doCleanupSvmPeering = cleanupSvmPeering
	})
	t.Run("WhenSVMCleanupFails", func(tt *testing.T) {
		expectedError := errors.New("Faceplanting")
		mockClient := new(ontaprest.MockRESTClient)
		mockSnapmirrorClient := new(ontaprest.MockSnapmirrorClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		provider := &OntapRestProvider{}

		doCleanupSvmPeering = func(provider *OntapRestProvider, params *DeleteVolumeReplicationParams) error {
			return expectedError
		}

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipGet", getParams).Return(&returnSnapmirror, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipDelete", delParams).Return(false, jobAccepted, nil).Times(1)
		mockClient.On("Poll", jobAccepted.JobUUID).Return(nil).Times(1)

		volumeReplication, err := provider.DeleteVolumeReplication(&volumeReplicationDeleteParams)
		assert.Equal(tt, expectedError, err)
		assert.Empty(tt, volumeReplication)
		doCleanupSvmPeering = cleanupSvmPeering
	})
	t.Run("WhenSuccessful", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockSnapmirrorClient := new(ontaprest.MockSnapmirrorClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		provider := &OntapRestProvider{}

		doCleanupSvmPeering = func(provider *OntapRestProvider, params *DeleteVolumeReplicationParams) error {
			return nil
		}

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipGet", getParams).Return(&returnSnapmirror, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipDelete", delParams).Return(false, jobAccepted, nil).Times(1)
		mockClient.On("Poll", jobAccepted.JobUUID).Return(nil).Times(1)

		volumeReplication, err := provider.DeleteVolumeReplication(&volumeReplicationDeleteParams)
		assert.NoError(tt, err)
		assert.NotNil(tt, volumeReplication)
		doCleanupSvmPeering = cleanupSvmPeering
	})
	t.Run("WithoutRelationshipIDReturnsEmpty", func(tt *testing.T) {
		replicationDeleteParams := DeleteVolumeReplicationParams{VolumeReplication: &VolumeReplication{}}
		mockClient := new(ontaprest.MockRESTClient)
		mockSnapmirrorClient := new(ontaprest.MockSnapmirrorClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		provider := &OntapRestProvider{}

		doCleanupSvmPeering = func(provider *OntapRestProvider, params *DeleteVolumeReplicationParams) error {
			return nil
		}

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipList", listParams).Return([]*ontaprest.SnapmirrorRelationship{}, nil).Times(1)
		volumeReplication, err := provider.DeleteVolumeReplication(&replicationDeleteParams)
		assert.NoError(tt, err)
		assert.Empty(tt, volumeReplication)
		doCleanupSvmPeering = cleanupSvmPeering
	})
	t.Run("WithoutRelationshipIDReturnsError", func(tt *testing.T) {
		replicationDeleteParams := DeleteVolumeReplicationParams{VolumeReplication: &VolumeReplication{}}
		mockClient := new(ontaprest.MockRESTClient)
		mockSnapmirrorClient := new(ontaprest.MockSnapmirrorClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		provider := &OntapRestProvider{}

		doCleanupSvmPeering = func(provider *OntapRestProvider, params *DeleteVolumeReplicationParams) error {
			return nil
		}
		expectedError := errors.New("Faceplanting")

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipList", listParams).Return(nil, expectedError).Times(1)

		volumeReplication, err := provider.DeleteVolumeReplication(&replicationDeleteParams)
		assert.Error(tt, err, expectedError)
		assert.Empty(tt, volumeReplication)
	})
	t.Run("WithoutRelationshipIDSuccessful", func(tt *testing.T) {
		replicationDeleteParams := DeleteVolumeReplicationParams{VolumeReplication: &VolumeReplication{}}
		mockClient := new(ontaprest.MockRESTClient)
		mockSnapmirrorClient := new(ontaprest.MockSnapmirrorClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		provider := &OntapRestProvider{}

		doCleanupSvmPeering = func(provider *OntapRestProvider, params *DeleteVolumeReplicationParams) error {
			return nil
		}

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipList", listParams).Return([]*ontaprest.SnapmirrorRelationship{&returnSnapmirror}, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipDelete", delParams).Return(false, jobAccepted, nil).Times(1)
		mockClient.On("Poll", jobAccepted.JobUUID).Return(nil).Times(1)

		volumeReplication, err := provider.DeleteVolumeReplication(&replicationDeleteParams)
		assert.NoError(tt, err)
		assert.Empty(tt, volumeReplication)
		doCleanupSvmPeering = cleanupSvmPeering
	})
}

func TestUpdateVolumeReplication(t *testing.T) {
	t.Run("WhenReplicationScheduleIsEmpty", func(tt *testing.T) {
		provider := &OntapRestProvider{}
		params := VolumeReplication{ReplicationSchedule: ""}

		result, err := provider.UpdateVolumeReplication(&params)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
	})
	t.Run("WhenCreateVolumeReplicationScheduleFails", func(tt *testing.T) {
		provider := &OntapRestProvider{}
		params := VolumeReplication{ReplicationSchedule: "daily"}

		defer func() {
			doCreateVolumeReplicationScheduleIfNeeded = createVolumeReplicationSchedule
		}()
		doCreateVolumeReplicationScheduleIfNeeded = func(provider *OntapRestProvider, schedule string) (err error) {
			return errors.New("failed to create schedule")
		}
		result, err := provider.UpdateVolumeReplication(&params)
		assert.Error(tt, err)
		assert.Nil(tt, result)
	})
	t.Run("WhenClientReturnsError", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockSnapmirrorClient := new(ontaprest.MockSnapmirrorClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		provider := &OntapRestProvider{}
		expectedErr := errors.New("RandomError")

		params := &VolumeReplication{ReplicationSchedule: "daily", RelationshipID: "123"}
		modifyParams := &ontaprest.SnapmirrorRelationshipModifyParams{TransferSchedule: &params.ReplicationSchedule, UUID: params.RelationshipID}
		defer func() {
			doCreateVolumeReplicationScheduleIfNeeded = createVolumeReplicationSchedule
		}()
		doCreateVolumeReplicationScheduleIfNeeded = func(provider *OntapRestProvider, schedule string) (err error) {
			return nil
		}

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipModify", modifyParams).Return(nil, nil, expectedErr).Times(1)
		volumeReplication, err := provider.UpdateVolumeReplication(params)
		assert.Equal(tt, expectedErr, err)
		assert.Empty(tt, volumeReplication)
	})
	t.Run("WhenPollReturnsError", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockSnapmirrorClient := new(ontaprest.MockSnapmirrorClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		provider := &OntapRestProvider{}
		expectedErr := errors.New("RandomError")

		params := &VolumeReplication{ReplicationSchedule: "daily", RelationshipID: "123"}
		jobAccepted := &ontaprest.JobAccepted{JobUUID: "jobUUID"}
		modifyParams := &ontaprest.SnapmirrorRelationshipModifyParams{TransferSchedule: &params.ReplicationSchedule, UUID: params.RelationshipID}
		defer func() {
			doCreateVolumeReplicationScheduleIfNeeded = createVolumeReplicationSchedule
		}()
		doCreateVolumeReplicationScheduleIfNeeded = func(provider *OntapRestProvider, schedule string) (err error) {
			return nil
		}

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipModify", modifyParams).Return(nil, jobAccepted, nil).Times(1)
		mockClient.On("Poll", jobAccepted.JobUUID).Return(expectedErr).Times(1)
		volumeReplication, err := provider.UpdateVolumeReplication(params)
		assert.Equal(tt, expectedErr, err)
		assert.Empty(tt, volumeReplication)
	})
	t.Run("WhenGetSnapmirrorReturnsError", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockSnapmirrorClient := new(ontaprest.MockSnapmirrorClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		provider := &OntapRestProvider{}
		expectedErr := errors.New("RandomError")

		params := &VolumeReplication{ReplicationSchedule: "daily", RelationshipID: "123"}
		jobAccepted := &ontaprest.JobAccepted{JobUUID: "jobUUID"}

		modifyParams := &ontaprest.SnapmirrorRelationshipModifyParams{TransferSchedule: &params.ReplicationSchedule, UUID: params.RelationshipID}
		getParams := &ontaprest.SnapmirrorRelationshipGetParams{UUID: params.RelationshipID}
		defer func() {
			doCreateVolumeReplicationScheduleIfNeeded = createVolumeReplicationSchedule
		}()
		doCreateVolumeReplicationScheduleIfNeeded = func(provider *OntapRestProvider, schedule string) (err error) {
			return nil
		}

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipModify", modifyParams).Return(nil, jobAccepted, nil).Times(1)
		mockClient.On("Poll", jobAccepted.JobUUID).Return(nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipGet", getParams).Return(nil, expectedErr).Times(1)
		volumeReplication, err := provider.UpdateVolumeReplication(params)
		assert.Equal(tt, expectedErr, err)
		assert.Empty(tt, volumeReplication)
	})
	t.Run("WhenSuccess", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockSnapmirrorClient := new(ontaprest.MockSnapmirrorClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		provider := &OntapRestProvider{}

		jobAccepted := &ontaprest.JobAccepted{JobUUID: "jobUUID"}
		replicationSchedule := "daily"
		snapmirrorPolicyName := "policyName"
		relationshipID := strfmt.UUID(uuid.New().String())
		boolTrue := true
		snapMirrorSchedule := models.SnapmirrorRelationshipInlineTransferSchedule{Name: &replicationSchedule}
		expectedSnapmirrorRelationship := &ontaprest.SnapmirrorRelationship{
			SnapmirrorRelationship: models.SnapmirrorRelationship{
				TransferSchedule: &snapMirrorSchedule,
				Policy:           &models.SnapmirrorRelationshipInlinePolicy{Name: &snapmirrorPolicyName},
				UUID:             &relationshipID,
				Healthy:          &boolTrue,
			},
		}

		params := &VolumeReplication{ReplicationSchedule: replicationSchedule, RelationshipID: relationshipID.String()}

		modifyParams := &ontaprest.SnapmirrorRelationshipModifyParams{TransferSchedule: &params.ReplicationSchedule, UUID: params.RelationshipID}
		getParams := &ontaprest.SnapmirrorRelationshipGetParams{UUID: params.RelationshipID}

		defer func() {
			doCreateVolumeReplicationScheduleIfNeeded = createVolumeReplicationSchedule
		}()
		doCreateVolumeReplicationScheduleIfNeeded = func(provider *OntapRestProvider, schedule string) (err error) {
			return nil
		}

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipModify", modifyParams).Return(nil, jobAccepted, nil).Times(1)
		mockClient.On("Poll", jobAccepted.JobUUID).Return(nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipGet", getParams).Return(expectedSnapmirrorRelationship, nil).Times(1)

		volumeReplication, err := provider.UpdateVolumeReplication(params)
		assert.NoError(tt, err)
		assert.Equal(tt, replicationSchedule, volumeReplication.ReplicationSchedule)
		assert.Equal(tt, relationshipID.String(), volumeReplication.RelationshipID)
		assert.Equal(tt, snapmirrorPolicyName, volumeReplication.ReplicationPolicy)
		assert.Equal(tt, true, volumeReplication.Healthy)
	})
}

func TestReleaseVolumeReplication(t *testing.T) {
	srcVolumeUUID := "src-uuid"
	dstVolumeUUID := "dst-uuid"
	srcVolume := ontaprest.Volume{
		Volume: models.Volume{
			UUID: &srcVolumeUUID,
			Type: nillable.ToPointer(models.VolumeTypeRw),
			Name: nillable.ToPointer("srcvol"),
			Svm: &models.VolumeInlineSvm{
				Name: nillable.ToPointer("srcsvm"),
			},
		},
	}
	dstVolume := ontaprest.Volume{
		Volume: models.Volume{
			UUID: &dstVolumeUUID,
			Type: nillable.ToPointer(models.VolumeTypeDp),
			Name: nillable.ToPointer("dstvol"),
			Svm: &models.VolumeInlineSvm{
				Name: nillable.ToPointer("dstsvm"),
			},
		},
	}
	online := "online"
	volume1 := &Volume{
		Volume:            models.Volume{State: &online},
		ExternalUUID:      dstVolumeUUID,
		IsOnPremMigration: true,
	}
	volumeReplicationCreateParams := CreateVolumeReplicationParams{
		VolumeReplication: &VolumeReplication{
			Volume:                volume1,
			ReplicationPolicy:     "MirrorAllSnapshots",
			SourceSVMName:         *srcVolume.Svm.Name,
			SourceVolumeName:      *srcVolume.Name,
			DestinationSVMName:    *dstVolume.Svm.Name,
			DestinationVolumeName: *dstVolume.Name,
			ClusterPeerID:         nillable.ToPointer(uint64(1)),
			EndpointType:          "src",
			ReplicationType:       "ExternalDisasterRecovery",
		},
	}
	listDestinationParams := &ontaprest.SnapmirrorRelationshipListDestinationsParams{
		DestinationPath: nillable.GetStringPtr(volumeReplicationCreateParams.VolumeReplication.DestinationPath()),
		SourcePath:      nillable.GetStringPtr(volumeReplicationCreateParams.VolumeReplication.SourcePath()),
	}
	uuid := strfmt.UUID("snapmirror-uuid")
	destinations := []*ontaprest.SnapmirrorRelationship{
		{
			SnapmirrorRelationship: models.SnapmirrorRelationship{
				UUID: &uuid,
			},
		},
	}
	releaseParams := &ontaprest.SnapmirrorRelationshipReleaseParams{
		UUID: destinations[0].UUID.String(),
	}
	jobAccepted := &ontaprest.JobAccepted{JobUUID: "jobUUID"}
	oldSnapmirrorErrorIntervalRetrySeconds := snapmirrorErrorIntervalRetrySeconds
	defer func() {
		snapmirrorErrorIntervalRetrySeconds = oldSnapmirrorErrorIntervalRetrySeconds
	}()

	t.Run("WhenListDestinationsReturnsError", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockSnapmirrorClient := new(ontaprest.MockSnapmirrorClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		provider := &OntapRestProvider{}
		expectedError := errors.New("faceplanting")

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipListDestinations", listDestinationParams).Return(nil, expectedError).Times(1)
		volumeReplication, err := provider.ReleaseVolumeReplication(&volumeReplicationCreateParams)
		assert.Equal(tt, expectedError, err)
		assert.Empty(tt, volumeReplication)
	})
	t.Run("WhenReleaseReturnsError", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockSnapmirrorClient := new(ontaprest.MockSnapmirrorClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		provider := &OntapRestProvider{}
		expectedError := errors.New("faceplanting")

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipListDestinations", listDestinationParams).Return(destinations, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipRelease", releaseParams).Return(false, nil, expectedError).Times(1)
		volumeReplication, err := provider.ReleaseVolumeReplication(&volumeReplicationCreateParams)
		assert.Equal(tt, expectedError, err)
		assert.Empty(tt, volumeReplication)
	})
	t.Run("WhenPollReturnsError", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockSnapmirrorClient := new(ontaprest.MockSnapmirrorClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		provider := &OntapRestProvider{}
		expectedError := errors.New("faceplanting")

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipListDestinations", listDestinationParams).Return(destinations, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipRelease", releaseParams).Return(false, jobAccepted, nil).Times(1)
		mockClient.On("Poll", jobAccepted.JobUUID).Return(expectedError).Times(1)
		volumeReplication, err := provider.ReleaseVolumeReplication(&volumeReplicationCreateParams)
		assert.Equal(tt, expectedError, err)
		assert.Empty(tt, volumeReplication)
	})
	t.Run("WhenReleaseReturnsAnotherOperationError", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockSnapmirrorClient := new(ontaprest.MockSnapmirrorClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		provider := &OntapRestProvider{}
		expectedError := errors.New("Another SnapMirror operation is in progress")

		snapmirrorErrorIntervalRetrySeconds = 0
		doCleanupSvmPeering = func(provider *OntapRestProvider, params *DeleteVolumeReplicationParams) error {
			return nil
		}
		defer func() {
			doCleanupSvmPeering = cleanupSvmPeering
		}()

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipListDestinations", listDestinationParams).Return(destinations, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipRelease", releaseParams).Return(false, nil, expectedError).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipRelease", releaseParams).Return(false, jobAccepted, nil).Times(1)
		mockClient.On("Poll", jobAccepted.JobUUID).Return(nil).Times(1)
		volumeReplication, err := provider.ReleaseVolumeReplication(&volumeReplicationCreateParams)
		assert.NoError(tt, err)
		assert.NotEmpty(tt, volumeReplication)
	})
	t.Run("WhenReleaseReturnsEntryNotFoundError", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockSnapmirrorClient := new(ontaprest.MockSnapmirrorClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		provider := &OntapRestProvider{}
		expectedError := errors.New("entry not found")

		snapmirrorErrorIntervalRetrySeconds = 0

		doCleanupSvmPeering = func(provider *OntapRestProvider, params *DeleteVolumeReplicationParams) error {
			return nil
		}
		defer func() {
			doCleanupSvmPeering = cleanupSvmPeering
		}()

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipListDestinations", listDestinationParams).Return(destinations, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipRelease", releaseParams).Return(false, nil, expectedError).Times(1)
		volumeReplication, err := provider.ReleaseVolumeReplication(&volumeReplicationCreateParams)
		assert.NoError(tt, err)
		assert.NotEmpty(tt, volumeReplication)
	})
	t.Run("WhenReleaseJobReturnsAnotherOperationError", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockSnapmirrorClient := new(ontaprest.MockSnapmirrorClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		provider := &OntapRestProvider{}
		expectedError := errors.New("Another SnapMirror operation is in progress")

		snapmirrorErrorIntervalRetrySeconds = 0

		doCleanupSvmPeering = func(provider *OntapRestProvider, params *DeleteVolumeReplicationParams) error {
			return nil
		}
		defer func() {
			doCleanupSvmPeering = cleanupSvmPeering
		}()

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipListDestinations", listDestinationParams).Return(destinations, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipRelease", releaseParams).Return(false, jobAccepted, nil).Times(1)
		mockClient.On("Poll", jobAccepted.JobUUID).Return(expectedError).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipRelease", releaseParams).Return(false, jobAccepted, nil).Times(1)
		mockClient.On("Poll", jobAccepted.JobUUID).Return(nil).Times(1)
		volumeReplication, err := provider.ReleaseVolumeReplication(&volumeReplicationCreateParams)
		assert.NoError(tt, err)
		assert.NotEmpty(tt, volumeReplication)
	})
	t.Run("WhenVolumeOfflineAndSuccessful", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockSnapmirrorClient := new(ontaprest.MockSnapmirrorClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		provider := &OntapRestProvider{}

		doCleanupSvmPeering = func(provider *OntapRestProvider, params *DeleteVolumeReplicationParams) error {
			return nil
		}

		defer func() {
			doCleanupSvmPeering = cleanupSvmPeering
		}()
		offline := "offline"
		offlineVol := &Volume{
			ExternalUUID: dstVolumeUUID,
			Volume:       models.Volume{State: &offline},
		}
		createParams := CreateVolumeReplicationParams{
			VolumeReplication: &VolumeReplication{
				Volume:                offlineVol,
				ReplicationPolicy:     "MirrorAllSnapshots",
				SourceSVMName:         *srcVolume.Svm.Name,
				SourceVolumeName:      *srcVolume.Name,
				DestinationSVMName:    *dstVolume.Svm.Name,
				DestinationVolumeName: *dstVolume.Name,
			},
		}
		createParams.VolumeReplication.Volume = offlineVol
		offlineReleaseParams := &ontaprest.SnapmirrorRelationshipReleaseParams{
			UUID:           destinations[0].UUID.String(),
			SourceInfoOnly: nillable.GetBoolPtr(true),
		}

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipListDestinations", listDestinationParams).Return(destinations, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipRelease", offlineReleaseParams).Return(false, jobAccepted, nil).Times(1)
		mockClient.On("Poll", jobAccepted.JobUUID).Return(nil).Times(1)
		volumeReplication, err := provider.ReleaseVolumeReplication(&createParams)
		assert.NoError(tt, err)
		assert.NotEmpty(tt, volumeReplication)
	})
	t.Run("WhenCleanupSVMPeeringFails", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockSnapmirrorClient := new(ontaprest.MockSnapmirrorClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		provider := &OntapRestProvider{Logger: log.NewLogger().(*log.Slogger)}

		doCleanupSvmPeering = func(provider *OntapRestProvider, params *DeleteVolumeReplicationParams) error {
			return errors.New("SVM cleanup error")
		}
		defer func() {
			doCleanupSvmPeering = cleanupSvmPeering
		}()

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipListDestinations", listDestinationParams).Return(destinations, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipRelease", releaseParams).Return(false, jobAccepted, nil).Times(1)
		mockClient.On("Poll", jobAccepted.JobUUID).Return(nil).Times(1)
		volumeReplication, err := provider.ReleaseVolumeReplication(&volumeReplicationCreateParams)
		assert.Equal(tt, errors.New("SVM cleanup error"), err)
		assert.Empty(tt, volumeReplication)
	})
	t.Run("WhenReplicationTypeIsNotExternal", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockSnapmirrorClient := new(ontaprest.MockSnapmirrorClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		provider := &OntapRestProvider{}

		volumeReplicationCreateParams.VolumeReplication.ClusterPeerID = nillable.ToPointer(uint64(0))
		svmPeeringCleanupCalled := false

		doCleanupSvmPeering = func(provider *OntapRestProvider, params *DeleteVolumeReplicationParams) error {
			svmPeeringCleanupCalled = true
			return nil
		}
		defer func() {
			doCleanupSvmPeering = cleanupSvmPeering
		}()
		volumeReplicationCreateParams.VolumeReplication.ReplicationType = ""
		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipListDestinations", listDestinationParams).Return(destinations, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipRelease", releaseParams).Return(false, jobAccepted, nil).Times(1)
		mockClient.On("Poll", jobAccepted.JobUUID).Return(nil).Times(1)
		volumeReplication, err := provider.ReleaseVolumeReplication(&volumeReplicationCreateParams)
		assert.NoError(tt, err)
		assert.NotEmpty(tt, volumeReplication)
		assert.Equal(tt, false, svmPeeringCleanupCalled, "SVM Peering Cleanup should not have been called")
	})
	t.Run("WhenSuccessful", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockSnapmirrorClient := new(ontaprest.MockSnapmirrorClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		provider := &OntapRestProvider{}

		doCleanupSvmPeering = func(provider *OntapRestProvider, params *DeleteVolumeReplicationParams) error {
			return nil
		}
		defer func() {
			doCleanupSvmPeering = cleanupSvmPeering
		}()

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipListDestinations", listDestinationParams).Return(destinations, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipRelease", releaseParams).Return(false, jobAccepted, nil).Times(1)
		mockClient.On("Poll", jobAccepted.JobUUID).Return(nil).Times(1)
		volumeReplication, err := provider.ReleaseVolumeReplication(&volumeReplicationCreateParams)
		assert.NoError(tt, err)
		assert.NotEmpty(tt, volumeReplication)
	})
}

func TestResyncVolumeReplicationWithGSDisabled(t *testing.T) {
	relationshipID := "volumereplication-1"

	srcVolumeUUID := "src-uuid"
	dstVolumeUUID := "dst-uuid"

	srcVolumeName := "srcvol"
	dstVolumeName := "dstvol"

	params := &VolumeReplication{
		MirrorState:    SnapmirrorStateBroken,
		RelationshipID: relationshipID,
		Volume: &Volume{
			// VolumeID:      dstVolumeUUID,
			// CreationToken: dstVolumeName,
			ExternalUUID: srcVolumeUUID,
		},
	}

	srcVolume := ontaprest.Volume{
		Volume: models.Volume{
			UUID: &srcVolumeUUID,
			Type: nillable.ToPointer(models.VolumeTypeRw),
			Name: nillable.ToPointer(srcVolumeName),
			Svm: &models.VolumeInlineSvm{
				Name: nillable.ToPointer("srcsvm"),
				UUID: nillable.ToPointer("srcsvm-uuid"),
			},
		},
	}
	dstVolume := ontaprest.Volume{
		Volume: models.Volume{
			UUID: &dstVolumeUUID,
			Type: nillable.ToPointer(models.VolumeTypeDp),
			Name: nillable.ToPointer(dstVolumeName),
			Svm: &models.VolumeInlineSvm{
				Name: nillable.ToPointer("dstsvm"),
			},
		},
	}
	sourcePath := nillable.GetString(srcVolume.Svm.Name, "") + ":" + nillable.GetString(srcVolume.Name, "")
	destinationPath := nillable.GetString(dstVolume.Svm.Name, "") + ":" + nillable.GetString(dstVolume.Name, "")
	snapmirror := &ontaprest.SnapmirrorRelationship{
		SnapmirrorRelationship: models.SnapmirrorRelationship{
			Source:                &models.SnapmirrorSourceEndpoint{Path: &sourcePath},
			Destination:           &models.SnapmirrorEndpoint{Path: &destinationPath},
			UUID:                  nillable.ToPointer(strfmt.UUID("uuid")),
			State:                 nillable.ToPointer(models.SnapmirrorRelationshipStateSnapmirrored),
			TotalTransferDuration: nillable.ToPointer("PT2M34S"),
			Transfer: &models.SnapmirrorRelationshipInlineTransfer{
				TotalDuration:    nillable.ToPointer("PT4M50S"),
				State:            nillable.ToPointer(models.SnapmirrorRelationshipInlineTransferStateSuccess),
				BytesTransferred: nillable.ToPointer(int64(500)),
				EndTime:          nillable.ToPointer(strfmt.DateTime(time.Now())),
			},
			Policy: &models.SnapmirrorRelationshipInlinePolicy{
				Name: nillable.ToPointer("policy"),
			},
			LagTime: nillable.ToPointer("PT20S"),
			SnapmirrorRelationshipInlineUnhealthyReason: []*models.SnapmirrorError{
				{
					Message: nillable.ToPointer("error"),
				},
			},
			TransferSchedule: &models.SnapmirrorRelationshipInlineTransferSchedule{
				Name: nillable.ToPointer("le schedule"),
			},
			Healthy:            nillable.GetBoolPtr(true),
			TotalTransferBytes: nillable.ToPointer(int64(1000)),
		},
	}
	setupMocks := func(tt *testing.T) (provider *OntapRestProvider, mockClient *ontaprest.MockRESTClient, mockSnapmirrorClient *ontaprest.MockSnapmirrorClient, mockStorageClient *ontaprest.MockStorageClient, mockSANClient *ontaprest.MockSANClient) {
		mockClient = new(ontaprest.MockRESTClient)
		mockStorageClient = new(ontaprest.MockStorageClient)
		mockSnapmirrorClient = new(ontaprest.MockSnapmirrorClient)
		mockSANClient = new(ontaprest.MockSANClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		provider = &OntapRestProvider{}
		return
	}

	t.Run("WhenGetSnapmirrorReturnsError", func(tt *testing.T) {
		provider, mockClient, mockSnapmirrorClient, _, _ := setupMocks(tt)

		expectedError := errors.New("Faceplanting")

		getParams := &ontaprest.SnapmirrorRelationshipGetParams{UUID: relationshipID}

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipGet", getParams).Return(nil, expectedError).Times(1)
		volumeReplication, err := provider.ResyncVolumeReplication(params)

		assert.Equal(tt, expectedError, err)
		assert.Empty(tt, volumeReplication)
	})
	t.Run("WhenSnapmirrorIsNotInStateBroken", func(tt *testing.T) {
		provider, mockClient, mockSnapmirrorClient, _, _ := setupMocks(tt)

		expectedError := errors.NewConflictErr("Cannot perform a resync operation in this mirror state")

		getParams := &ontaprest.SnapmirrorRelationshipGetParams{UUID: relationshipID}

		params.MirrorState = SnapmirrorStateMirrored

		defer func() {
			params.MirrorState = SnapmirrorStateBroken
		}()

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipGet", getParams).Return(snapmirror, nil).Times(1)
		volumeReplication, err := provider.ResyncVolumeReplication(params)

		assert.Equal(tt, expectedError, err)
		assert.Empty(tt, volumeReplication)
	})
	t.Run("WhenGettingVolumeReturnsError", func(tt *testing.T) {
		provider, mockClient, mockSnapmirrorClient, mockStorageClient, _ := setupMocks(tt)

		expectedError := errors.New("Faceplanting")

		snapmirorGetParams := &ontaprest.SnapmirrorRelationshipGetParams{UUID: relationshipID}
		volumeGetParams := &ontaprest.VolumeGetParams{UUID: *srcVolume.UUID}

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockClient.On("Storage").Return(mockStorageClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipGet", snapmirorGetParams).Return(snapmirror, nil).Times(1)
		mockStorageClient.On("VolumeGet", volumeGetParams).Return(nil, expectedError).Times(1)

		volumeReplication, err := provider.ResyncVolumeReplication(params)

		assert.Equal(tt, expectedError, err)
		assert.Empty(tt, volumeReplication)
	})
	t.Run("WhenResyncOrInitializeOrResumeSnapmirrorReturnsError", func(tt *testing.T) {
		provider, mockClient, mockSnapmirrorClient, mockStorageClient, _ := setupMocks(tt)

		expectedError := errors.New("Faceplanting")

		snapmirorGetParams := &ontaprest.SnapmirrorRelationshipGetParams{UUID: relationshipID}
		volumeGetParams := &ontaprest.VolumeGetParams{UUID: *srcVolume.UUID}
		snapmirorResumeParams := snapmirror.UUID

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockClient.On("Storage").Return(mockStorageClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipGet", snapmirorGetParams).Return(snapmirror, nil).Times(1)
		mockStorageClient.On("VolumeGet", volumeGetParams).Return(&srcVolume, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipResyncOrInitializeOrResume", snapmirorResumeParams.String()).Return(nil, nil, expectedError).Times(1)
		volumeReplication, err := provider.ResyncVolumeReplication(params)

		assert.Equal(tt, expectedError, err)
		assert.Empty(tt, volumeReplication)
	})
	t.Run("WhenGettingSnapmirrorInsideRetryLoopReturnsError", func(tt *testing.T) {
		provider, mockClient, mockSnapmirrorClient, mockStorageClient, _ := setupMocks(tt)

		expectedError := errors.New("Faceplanting")

		snapmirorGetParams := &ontaprest.SnapmirrorRelationshipGetParams{UUID: relationshipID}
		volumeGetParams := &ontaprest.VolumeGetParams{UUID: *srcVolume.UUID}
		snapmirorResumeParams := snapmirror.UUID

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockClient.On("Storage").Return(mockStorageClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipGet", snapmirorGetParams).Return(snapmirror, nil).Times(1)
		mockStorageClient.On("VolumeGet", volumeGetParams).Return(&srcVolume, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipResyncOrInitializeOrResume", snapmirorResumeParams.String()).Return(snapmirror, nil, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipGet", snapmirorGetParams).Return(nil, expectedError).Times(1)
		volumeReplication, err := provider.ResyncVolumeReplication(params)

		assert.Equal(tt, expectedError, err)
		assert.Empty(tt, volumeReplication)
	})
	t.Run("WhenUnmountVolumeReturnsError", func(tt *testing.T) {
		provider, mockClient, mockSnapmirrorClient, mockStorageClient, _ := setupMocks(tt)

		expectedError := errors.New("Faceplanting")

		snapmirorGetParams := &ontaprest.SnapmirrorRelationshipGetParams{UUID: relationshipID}
		volumeGetParams := &ontaprest.VolumeGetParams{UUID: *srcVolume.UUID}
		snapmirorResumeParams := snapmirror.UUID
		doUnmountVolume = func(provider *OntapRestProvider, volume *ontaprest.Volume, volRep *VolumeReplication) error {
			return expectedError
		}

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockClient.On("Storage").Return(mockStorageClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipGet", snapmirorGetParams).Return(snapmirror, nil).Times(1)
		mockStorageClient.On("VolumeGet", volumeGetParams).Return(&srcVolume, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipResyncOrInitializeOrResume", snapmirorResumeParams.String()).Return(snapmirror, nil, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipGet", snapmirorGetParams).Return(snapmirror, nil).Times(1)
		volumeReplication, err := provider.ResyncVolumeReplication(params)

		assert.Equal(tt, expectedError, err)
		assert.Empty(tt, volumeReplication)
	})
	t.Run("WhenFinallyGettingSnapmirrorReturnsError", func(tt *testing.T) {
		provider, mockClient, mockSnapmirrorClient, mockStorageClient, _ := setupMocks(tt)

		expectedError := errors.New("Faceplanting")

		snapmirorGetParams := &ontaprest.SnapmirrorRelationshipGetParams{UUID: relationshipID}
		volumeGetParams := &ontaprest.VolumeGetParams{UUID: *srcVolume.UUID}
		snapmirorResumeParams := snapmirror.UUID

		// unmountJobAccepted := &ontaprest.JobAccepted{JobUUID: "job-uuid"}
		doUnmountVolume = func(provider *OntapRestProvider, volume *ontaprest.Volume, volRep *VolumeReplication) error {
			return nil
		}

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockClient.On("Storage").Return(mockStorageClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipGet", snapmirorGetParams).Return(snapmirror, nil).Times(1)
		mockStorageClient.On("VolumeGet", volumeGetParams).Return(&srcVolume, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipResyncOrInitializeOrResume", snapmirorResumeParams.String()).Return(snapmirror, nil, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipGet", snapmirorGetParams).Return(snapmirror, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipGet", snapmirorGetParams).Return(nil, expectedError).Times(1)
		volumeReplication, err := provider.ResyncVolumeReplication(params)

		assert.Equal(tt, expectedError, err)
		assert.Empty(tt, volumeReplication)
	})
	t.Run("WhenWaitingForJobReturnsError", func(tt *testing.T) {
		provider, mockClient, mockSnapmirrorClient, mockStorageClient, _ := setupMocks(tt)

		expectedError := errors.New("Faceplanting")

		snapmirorGetParams := &ontaprest.SnapmirrorRelationshipGetParams{UUID: relationshipID}
		volumeGetParams := &ontaprest.VolumeGetParams{UUID: *srcVolume.UUID}
		snapmirorResumeParams := snapmirror.UUID
		resyncJobAccepted := &ontaprest.JobAccepted{JobUUID: "job-uuid"}

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockClient.On("Storage").Return(mockStorageClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipGet", snapmirorGetParams).Return(snapmirror, nil).Times(1)
		mockStorageClient.On("VolumeGet", volumeGetParams).Return(&srcVolume, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipResyncOrInitializeOrResume", snapmirorResumeParams.String()).Return(nil, resyncJobAccepted, nil).Times(1)
		mockClient.On("Poll", resyncJobAccepted.JobUUID).Return(expectedError).Times(1)
		volumeReplication, err := provider.ResyncVolumeReplication(params)

		assert.Equal(tt, expectedError, err)
		assert.Empty(tt, volumeReplication)
	})
	t.Run("WhenSuccessful", func(tt *testing.T) {
		provider, mockClient, mockSnapmirrorClient, mockStorageClient, _ := setupMocks(tt)

		snapmirorGetParams := &ontaprest.SnapmirrorRelationshipGetParams{UUID: relationshipID}
		volumeGetParams := &ontaprest.VolumeGetParams{UUID: *srcVolume.UUID}
		snapmirorResumeParams := snapmirror.UUID

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockClient.On("Storage").Return(mockStorageClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipGet", snapmirorGetParams).Return(snapmirror, nil).Times(1)
		mockStorageClient.On("VolumeGet", volumeGetParams).Return(&srcVolume, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipResyncOrInitializeOrResume", snapmirorResumeParams.String()).Return(snapmirror, nil, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipGet", snapmirorGetParams).Return(snapmirror, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipGet", snapmirorGetParams).Return(snapmirror, nil).Times(1)

		volumeReplication, err := provider.ResyncVolumeReplication(params)

		assert.NoError(tt, err)
		assert.NotNil(tt, volumeReplication)
	})
	t.Run("WhenEnsureBillablePersist", func(tt *testing.T) {
		provider, mockClient, mockSnapmirrorClient, mockStorageClient, _ := setupMocks(tt)

		snapmirorGetParams := &ontaprest.SnapmirrorRelationshipGetParams{UUID: relationshipID}
		volumeGetParams := &ontaprest.VolumeGetParams{UUID: *srcVolume.UUID}
		snapmirorResumeParams := snapmirror.UUID

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockClient.On("Storage").Return(mockStorageClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipGet", snapmirorGetParams).Return(snapmirror, nil).Times(1)
		mockStorageClient.On("VolumeGet", volumeGetParams).Return(&srcVolume, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipResyncOrInitializeOrResume", snapmirorResumeParams.String()).Return(snapmirror, nil, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipGet", snapmirorGetParams).Return(snapmirror, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipGet", snapmirorGetParams).Return(snapmirror, nil).Times(1)

		volumeReplication, err := provider.ResyncVolumeReplication(params)

		assert.NoError(tt, err)
		assert.NotNil(tt, volumeReplication)
	})
}

func TestListSnapmirrorDestinations(t *testing.T) {
	t.Run("WhenSnapmirrorRelationshipListDestinationsReturnsError", func(tt *testing.T) {
		mrc := new(ontaprest.MockRESTClient)
		msmc := new(ontaprest.MockSnapmirrorClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mrc
		}
		prov := &OntapRestProvider{}

		expectedError := errors.New("some error")

		mrc.On("Snapmirror").Return(msmc)
		msmc.On("SnapmirrorRelationshipListDestinations", (*ontaprest.SnapmirrorRelationshipListDestinationsParams)(nil)).Return(nil, expectedError).Times(1)
		res, err := _listSnapmirrorDestinations(prov)
		assert.EqualError(tt, err, expectedError.Error())
		assert.Nil(tt, res)
	})
	t.Run("WhenSnapmirrorDestinationsExistSuccessful", func(tt *testing.T) {
		mrc := new(ontaprest.MockRESTClient)
		msmc := new(ontaprest.MockSnapmirrorClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mrc
		}
		prov := &OntapRestProvider{}
		uuid := strfmt.UUID("1")
		destinations := []*ontaprest.SnapmirrorRelationship{
			{
				SnapmirrorRelationship: models.SnapmirrorRelationship{
					UUID:  &uuid,
					State: nillable.ToPointer("initialized"),
					Source: &models.SnapmirrorSourceEndpoint{
						Path: nillable.ToPointer("first-svm:first-volume"),
						Svm:  &models.SnapmirrorSourceEndpointInlineSvm{UUID: nillable.ToPointer("svm-1"), Name: nillable.ToPointer("first-svm")},
					},
					Destination: &models.SnapmirrorEndpoint{
						Path: nillable.ToPointer("first-svm:first-volume"),
						Svm:  &models.SnapmirrorEndpointInlineSvm{UUID: nillable.ToPointer("svm-1"), Name: nillable.ToPointer("first-svm")},
					},
				},
			},
		}

		mrc.On("Snapmirror").Return(msmc)
		msmc.On("SnapmirrorRelationshipListDestinations", (*ontaprest.SnapmirrorRelationshipListDestinationsParams)(nil)).Return(destinations, nil).Times(1)
		res, err := listSnapmirrorDestinations(prov)
		assert.NoError(tt, err)
		assert.Len(tt, res, 1)
		assert.Equal(tt, res[0].SourcePath, *destinations[0].Source.Path)
		assert.Equal(tt, res[0].DestinationSVMName, *destinations[0].Destination.Svm.Name)
	})
	t.Run("WhenNoSnapmirrorDestinationsExistSuccessful", func(tt *testing.T) {
		mrc := new(ontaprest.MockRESTClient)
		msmc := new(ontaprest.MockSnapmirrorClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mrc
		}
		prov := &OntapRestProvider{}
		var destinations []*ontaprest.SnapmirrorRelationship

		mrc.On("Snapmirror").Return(msmc)
		msmc.On("SnapmirrorRelationshipListDestinations", (*ontaprest.SnapmirrorRelationshipListDestinationsParams)(nil)).Return(destinations, nil).Times(1)
		res, err := listSnapmirrorDestinations(prov)
		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		assert.Len(tt, res, 0)
	})
}

func TestCleanupSvmPeering(t *testing.T) {
	srcVolumeUUID := "src-uuid"
	dstVolumeUUID := "dst-uuid"
	srcVolume := ontaprest.Volume{
		Volume: models.Volume{
			UUID: &srcVolumeUUID,
			Type: nillable.ToPointer(models.VolumeTypeRw),
			Name: nillable.ToPointer("srcvol"),
			Svm: &models.VolumeInlineSvm{
				Name: nillable.ToPointer("srcSVM"),
			},
		},
	}
	dstVolume := ontaprest.Volume{
		Volume: models.Volume{
			UUID: &dstVolumeUUID,
			Type: nillable.ToPointer(models.VolumeTypeDp),
			Name: nillable.ToPointer("dstvol"),
			Svm: &models.VolumeInlineSvm{
				Name: nillable.ToPointer("dstSVM"),
			},
		},
	}
	sourcePath := nillable.GetString(srcVolume.Svm.Name, "") + ":" + nillable.GetString(srcVolume.Name, "")
	destinationPath := nillable.GetString(dstVolume.Svm.Name, "") + ":" + nillable.GetString(dstVolume.Name, "")

	snapmirror := &ontaprest.SnapmirrorRelationship{
		SnapmirrorRelationship: models.SnapmirrorRelationship{
			Source:                &models.SnapmirrorSourceEndpoint{Svm: &models.SnapmirrorSourceEndpointInlineSvm{Name: nillable.ToPointer("srcSVM")}, Path: &sourcePath},
			Destination:           &models.SnapmirrorEndpoint{Svm: &models.SnapmirrorEndpointInlineSvm{Name: nillable.ToPointer("dstSVM")}, Path: &destinationPath},
			UUID:                  nillable.ToPointer(strfmt.UUID("uuid")),
			State:                 nillable.ToPointer(models.SnapmirrorRelationshipStateSnapmirrored),
			TotalTransferDuration: nillable.ToPointer("PT2M34S"),
			Transfer: &models.SnapmirrorRelationshipInlineTransfer{
				TotalDuration:    nillable.ToPointer("PT4M50S"),
				State:            nillable.ToPointer(models.SnapmirrorRelationshipInlineTransferStateSuccess),
				BytesTransferred: nillable.ToPointer(int64(500)),
				EndTime:          nillable.ToPointer(strfmt.DateTime(time.Now())),
			},
			Policy: &models.SnapmirrorRelationshipInlinePolicy{
				Name: nillable.ToPointer("policy"),
			},
			LagTime: nillable.ToPointer("PT20S"),
			SnapmirrorRelationshipInlineUnhealthyReason: []*models.SnapmirrorError{
				{
					Message: nillable.ToPointer("error"),
				},
			},
			TransferSchedule: &models.SnapmirrorRelationshipInlineTransferSchedule{
				Name: nillable.ToPointer("le schedule"),
			},
			Healthy:            nillable.GetBoolPtr(true),
			TotalTransferBytes: nillable.ToPointer(int64(1000)),
		},
	}

	snapmirror2 := &ontaprest.SnapmirrorRelationship{
		SnapmirrorRelationship: models.SnapmirrorRelationship{
			Source:                &models.SnapmirrorSourceEndpoint{Svm: &models.SnapmirrorSourceEndpointInlineSvm{Name: nillable.ToPointer("dstSVM")}},
			Destination:           &models.SnapmirrorEndpoint{Svm: &models.SnapmirrorEndpointInlineSvm{Name: nillable.ToPointer("srcSVM")}},
			UUID:                  nillable.ToPointer(strfmt.UUID("uuid")),
			State:                 nillable.ToPointer(models.SnapmirrorRelationshipStateSnapmirrored),
			TotalTransferDuration: nillable.ToPointer("PT2M34S"),
			Transfer: &models.SnapmirrorRelationshipInlineTransfer{
				TotalDuration:    nillable.ToPointer("PT4M50S"),
				State:            nillable.ToPointer(models.SnapmirrorRelationshipInlineTransferStateSuccess),
				BytesTransferred: nillable.ToPointer(int64(500)),
				EndTime:          nillable.ToPointer(strfmt.DateTime(time.Now())),
			},
			Policy: &models.SnapmirrorRelationshipInlinePolicy{
				Name: nillable.ToPointer("policy"),
			},
			LagTime: nillable.ToPointer("PT20S"),
			SnapmirrorRelationshipInlineUnhealthyReason: []*models.SnapmirrorError{
				{
					Message: nillable.ToPointer("error"),
				},
			},
			TransferSchedule: &models.SnapmirrorRelationshipInlineTransferSchedule{
				Name: nillable.ToPointer("le schedule"),
			},
			Healthy:            nillable.GetBoolPtr(true),
			TotalTransferBytes: nillable.ToPointer(int64(1000)),
		},
	}
	snapmirrorList := []*ontaprest.SnapmirrorRelationship{snapmirror}
	snapmirrorList2 := []*ontaprest.SnapmirrorRelationship{snapmirror2}

	params := &DeleteVolumeReplicationParams{
		VolumeReplication: &VolumeReplication{
			UUID:               "",
			SourceSVMName:      "srcSVM",
			DestinationSVMName: "dstSVM",
		},
	}
	params1 := &DeleteVolumeReplicationParams{
		VolumeReplication: &VolumeReplication{
			UUID:               "",
			SourceSVMName:      "srcSVM",
			DestinationSVMName: "dstSVM",
			ClusterPeerID:      nillable.ToPointer(uint64(1)),
			EndpointType:       "src",
		},
	}

	t.Run("WhenListingSnapMirrorsReturnsError", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockSnapmirrorClient := new(ontaprest.MockSnapmirrorClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		provider := &OntapRestProvider{}

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipList", &ontaprest.SnapmirrorRelationshipListParams{}).Return(nil, errors.New("Error listing snapmirrors")).Times(1)
		err := cleanupSvmPeering(provider, params)
		if err == nil {
			tt.Error("Error not returned")
		} else if err.Error() != "Error listing snapmirrors" {
			tt.Error("Wrong error returned")
		}
	})
	t.Run("WhenListingSnapMirrorDestinationsReturnsError", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockSnapmirrorClient := new(ontaprest.MockSnapmirrorClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		provider := &OntapRestProvider{}

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipList", &ontaprest.SnapmirrorRelationshipListParams{}).Return(nil, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipListDestinations", &ontaprest.SnapmirrorRelationshipListDestinationsParams{}).Return(nil, errors.New("Error listing snapmirror destinations")).Times(1)
		err := cleanupSvmPeering(provider, params)
		if err == nil {
			tt.Error("Error not returned")
		} else if err.Error() != "Error listing snapmirror destinations" {
			tt.Error("Wrong error returned")
		}
	})
	t.Run("WhenShouldNotDeletePeerDueToExistingSnapmirror", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockSnapmirrorClient := new(ontaprest.MockSnapmirrorClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		provider := &OntapRestProvider{}

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipList", &ontaprest.SnapmirrorRelationshipListParams{}).Return(snapmirrorList, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipListDestinations", &ontaprest.SnapmirrorRelationshipListDestinationsParams{}).Return(nil, nil).Times(1)

		err := cleanupSvmPeering(provider, params)
		if err != nil {
			tt.Error("Error unexpectedly returned")
		}
	})
	t.Run("WhenShouldNotDeletePeerDueToExistingSnapmirrorHybridRep", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockSnapmirrorClient := new(ontaprest.MockSnapmirrorClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		provider := &OntapRestProvider{}

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipList", &ontaprest.SnapmirrorRelationshipListParams{}).Return(snapmirrorList2, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipListDestinations", &ontaprest.SnapmirrorRelationshipListDestinationsParams{}).Return(nil, nil).Times(1)

		params1.VolumeReplication.ReplicationType = VolumeReplicationTypeExternalDisasterRecovery
		err := cleanupSvmPeering(provider, params1)
		if err != nil {
			tt.Error("Error unexpectedly returned")
		}
	})
	t.Run("WhenShouldNotDeletePeerDueToExistingDestination", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockSnapmirrorClient := new(ontaprest.MockSnapmirrorClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		provider := &OntapRestProvider{}

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipList", &ontaprest.SnapmirrorRelationshipListParams{}).Return(nil, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipListDestinations", &ontaprest.SnapmirrorRelationshipListDestinationsParams{}).Return(snapmirrorList2, nil).Times(1)

		err := cleanupSvmPeering(provider, params)
		if err != nil {
			tt.Error("Error unexpectedly returned")
		}
	})
	t.Run("WhenShouldNotDeletePeerDueToExistingDestinationHybridRep", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockSnapmirrorClient := new(ontaprest.MockSnapmirrorClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		provider := &OntapRestProvider{}

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipList", &ontaprest.SnapmirrorRelationshipListParams{}).Return(nil, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipListDestinations", &ontaprest.SnapmirrorRelationshipListDestinationsParams{}).Return(snapmirrorList, nil).Times(1)

		err := cleanupSvmPeering(provider, params1)
		if err != nil {
			tt.Error("Error unexpectedly returned")
		}
	})
	t.Run("WhenShouldNotDeletePeerDueToReverseResyncPerformedDuringCleanupLoop", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockSnapmirrorClient := new(ontaprest.MockSnapmirrorClient)
		mockSvmClient := new(ontaprest.MockSVMClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		provider := &OntapRestProvider{}
		svmPeerUuid := "svm-peer-uuid"

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockClient.On("SVM").Return(mockSvmClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipList", &ontaprest.SnapmirrorRelationshipListParams{}).Return(nil, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipListDestinations", &ontaprest.SnapmirrorRelationshipListDestinationsParams{}).Return(snapmirrorList, nil).Times(1)
		mockSvmClient.On("SvmPeerCollectionGet")
		getSvmPeer = func(provider *OntapRestProvider, srcSVMName, dstSVMName string) (*SvmPeer, error) {
			return &SvmPeer{UUID: svmPeerUuid}, nil
		}
		deleteSvmPeer = func(provider *OntapRestProvider, svmPeerUUID string, force bool) error {
			return errors.New("Relationship is in use by SnapMirror in local cluster. Use the \"snapmirror list-destinations\" command to view the relationships. " +
				"Use the \"snapmirror release\" command to release the application, then try the command again.")
		}
		err := cleanupSvmPeering(provider, params)
		if err != nil {
			tt.Error("Error unexpectedly returned")
		}
	})
	t.Run("WhenVserverPeerDoesntExistDuringCleanupLoop", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockSnapmirrorClient := new(ontaprest.MockSnapmirrorClient)
		mockSvmClient := new(ontaprest.MockSVMClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		provider := &OntapRestProvider{}
		svmPeerUuid := "svm-peer-uuid"

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockClient.On("SVM").Return(mockSvmClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipList", &ontaprest.SnapmirrorRelationshipListParams{}).Return(nil, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipListDestinations", &ontaprest.SnapmirrorRelationshipListDestinationsParams{}).Return(snapmirrorList, nil).Times(1)
		getSvmPeer = func(provider *OntapRestProvider, srcSVMName, dstSVMName string) (*SvmPeer, error) {
			return &SvmPeer{UUID: svmPeerUuid}, nil
		}
		deleteSvmPeer = func(provider *OntapRestProvider, svmPeerUUID string, force bool) error {
			return errors.New("Vserver peer relationship does not exist on the local cluster")
		}

		err := cleanupSvmPeering(provider, params)
		if err != nil {
			tt.Errorf("Unexpected error returned: %s", err.Error())
		}
	})
	t.Run("WhenFailedToLoadJobDuringCleanupLoopSuccess", func(tt *testing.T) {
		svmPeerPollIntervalSeconds = 1
		defer func() { svmPeerPollIntervalSeconds = 15 }()

		mockClient := new(ontaprest.MockRESTClient)
		mockSnapmirrorClient := new(ontaprest.MockSnapmirrorClient)
		mockSvmClient := new(ontaprest.MockSVMClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		provider := &OntapRestProvider{}
		svmPeerUuid := "svm-peer-uuid"

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockClient.On("SVM").Return(mockSvmClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipList", &ontaprest.SnapmirrorRelationshipListParams{}).Return(nil, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipListDestinations", &ontaprest.SnapmirrorRelationshipListDestinationsParams{}).Return(snapmirrorList, nil).Times(1)
		getSvmPeer = func(provider *OntapRestProvider, srcSVMName, dstSVMName string) (*SvmPeer, error) {
			return &SvmPeer{UUID: svmPeerUuid}, nil
		}
		count := 0
		deleteSvmPeer = func(provider *OntapRestProvider, svmPeerUUID string, force bool) error {
			if count == 0 {
				count = 1
				return errors.New("Relationship is in use by SnapMirror in peer cluster")
			}
			return nil
		}

		err := cleanupSvmPeering(provider, params)
		if err != nil {
			tt.Errorf("Unexpected error returned: %s", err.Error())
		}
	})
	t.Run("WhenRelationshipNeedsToBeReleasedOnPeeredSVMDuringCleanupLoopSuccess", func(tt *testing.T) {
		svmPeerPollIntervalSeconds = 0
		defer func() { svmPeerPollIntervalSeconds = 15 }()

		mockClient := new(ontaprest.MockRESTClient)
		mockSnapmirrorClient := new(ontaprest.MockSnapmirrorClient)
		mockSvmClient := new(ontaprest.MockSVMClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		provider := &OntapRestProvider{}
		svmPeerUuid := "svm-peer-uuid"

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockClient.On("SVM").Return(mockSvmClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipList", &ontaprest.SnapmirrorRelationshipListParams{}).Return(nil, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipListDestinations", &ontaprest.SnapmirrorRelationshipListDestinationsParams{}).Return(snapmirrorList, nil).Times(1)
		getSvmPeer = func(provider *OntapRestProvider, srcSVMName, dstSVMName string) (*SvmPeer, error) {
			return &SvmPeer{UUID: svmPeerUuid}, nil
		}
		deleteSvmPeer = func(provider *OntapRestProvider, svmPeerUUID string, force bool) error {
			return errors.New("A relationship on the peer cluster needs to be released")
		}

		err := cleanupSvmPeering(provider, params)
		if err == nil {
			tt.Error("Expected an error")
		} else {
			if err.Error() != "A source relationship on the Vserver peer needs to be released in peer cluster" {
				tt.Errorf("Unexpected error: %s", err.Error())
			}
			if !errors.IsConflictErr(err) {
				tt.Error("Expected a ConflictError")
			}
			if errors.GetTrackingID(err) != errors.StaleSnapmirrorCleanupNeeded {
				tt.Error("Wrong tracking ID returned")
			}
		}
	})
	t.Run("WhenFailedToLoadJobDuringCleanupLoopSuccess", func(tt *testing.T) {
		svmPeerPollIntervalSeconds = 1
		defer func() { svmPeerPollIntervalSeconds = 15 }()

		mockClient := new(ontaprest.MockRESTClient)
		mockSnapmirrorClient := new(ontaprest.MockSnapmirrorClient)
		mockSvmClient := new(ontaprest.MockSVMClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		provider := &OntapRestProvider{}
		svmPeerUuid := "svm-peer-uuid"

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockClient.On("SVM").Return(mockSvmClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipList", &ontaprest.SnapmirrorRelationshipListParams{}).Return(nil, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipListDestinations", &ontaprest.SnapmirrorRelationshipListDestinationsParams{}).Return(snapmirrorList, nil).Times(1)
		getSvmPeer = func(provider *OntapRestProvider, srcSVMName, dstSVMName string) (*SvmPeer, error) {
			return &SvmPeer{UUID: svmPeerUuid}, nil
		}
		count := 0
		deleteSvmPeer = func(provider *OntapRestProvider, svmPeerUUID string, force bool) error {
			if count == 0 {
				count = 1
				return errors.New("Failed to load job for Deleting a Vserver peer relationship between svm_bob and svm_rob")
			}
			return nil
		}

		err := cleanupSvmPeering(provider, params)
		if err != nil {
			tt.Errorf("Unexpected error returned: %s", err.Error())
		}
	})
	t.Run("WhenFailedToContactPeerCluster", func(tt *testing.T) {
		defer func() { params.VolumeReplication.Volume = nil }()

		params.VolumeReplication.Volume = &Volume{
			IsOnPremMigration: true,
		}
		mockClient := new(ontaprest.MockRESTClient)
		mockSnapmirrorClient := new(ontaprest.MockSnapmirrorClient)
		mockSvmClient := new(ontaprest.MockSVMClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		provider := &OntapRestProvider{}
		svmPeerUuid := "svm-peer-uuid"

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockClient.On("SVM").Return(mockSvmClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipList", &ontaprest.SnapmirrorRelationshipListParams{}).Return(nil, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipListDestinations", &ontaprest.SnapmirrorRelationshipListDestinationsParams{}).Return(snapmirrorList, nil).Times(1)
		getSvmPeer = func(provider *OntapRestProvider, srcSVMName, dstSVMName string) (*SvmPeer, error) {
			return &SvmPeer{UUID: svmPeerUuid}, nil
		}
		count := 0
		deleteSvmPeer = func(provider *OntapRestProvider, svmPeerUUID string, force bool) error {
			if count == 0 {
				count = 1
				return errors.New("errors.New(\"Failed to contact peer cluster \\\\\\\"peer-cluster\\\\\\\" at addresses: 1.2.3.4, 1.2.4.5. RPC: Remote system error [from mgwd on node \\\\\\\"node-name\\\\\\\" (VSID: -1) to mgwd at 1.2.3.14, 1.2.3.11]\")")
			}
			return nil
		}

		err := cleanupSvmPeering(provider, params)
		if err != nil {
			tt.Errorf("Unexpected error returned: %s", err.Error())
		}
	})
	t.Run("WhenDeleteSVMPeerReturnsUnknownError", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockSnapmirrorClient := new(ontaprest.MockSnapmirrorClient)
		mockSvmClient := new(ontaprest.MockSVMClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		provider := &OntapRestProvider{}
		svmPeerUuid := "svm-peer-uuid"

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockClient.On("SVM").Return(mockSvmClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipList", &ontaprest.SnapmirrorRelationshipListParams{}).Return(nil, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipListDestinations", &ontaprest.SnapmirrorRelationshipListDestinationsParams{}).Return(snapmirrorList, nil).Times(1)
		getSvmPeer = func(provider *OntapRestProvider, srcSVMName, dstSVMName string) (*SvmPeer, error) {
			return &SvmPeer{UUID: svmPeerUuid}, nil
		}
		deleteSvmPeer = func(provider *OntapRestProvider, svmPeerUUID string, force bool) error {
			return errors.New("Unknown error")
		}

		err := cleanupSvmPeering(provider, params)
		if err == nil {
			tt.Error("Error not returned")
		} else if err.Error() != "Unknown error" {
			tt.Error("Wrong error returned")
		}
	})
	t.Run("WhenEntryDoesntExistShouldIgnore", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockSnapmirrorClient := new(ontaprest.MockSnapmirrorClient)
		mockSvmClient := new(ontaprest.MockSVMClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		provider := &OntapRestProvider{}
		svmPeerUuid := "svm-peer-uuid"

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockClient.On("SVM").Return(mockSvmClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipList", &ontaprest.SnapmirrorRelationshipListParams{}).Return(nil, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipListDestinations", &ontaprest.SnapmirrorRelationshipListDestinationsParams{}).Return(snapmirrorList, nil).Times(1)
		getSvmPeer = func(provider *OntapRestProvider, srcSVMName, dstSVMName string) (*SvmPeer, error) {
			return &SvmPeer{UUID: svmPeerUuid}, nil
		}
		deleteSvmPeer = func(provider *OntapRestProvider, svmPeerUUID string, force bool) error {
			return errors.New("entry doesn't exist")
		}

		err := cleanupSvmPeering(provider, params)
		if err != nil {
			tt.Error("Error not returned")
		}
	})
	t.Run("WhenSuccessful", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockSnapmirrorClient := new(ontaprest.MockSnapmirrorClient)
		mockSvmClient := new(ontaprest.MockSVMClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		provider := &OntapRestProvider{}
		svmPeerUuid := "svm-peer-uuid"

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockClient.On("SVM").Return(mockSvmClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipList", &ontaprest.SnapmirrorRelationshipListParams{}).Return(nil, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipListDestinations", &ontaprest.SnapmirrorRelationshipListDestinationsParams{}).Return(snapmirrorList, nil).Times(1)
		getSvmPeer = func(provider *OntapRestProvider, srcSVMName, dstSVMName string) (*SvmPeer, error) {
			return &SvmPeer{UUID: svmPeerUuid}, nil
		}
		deleteSvmPeer = func(provider *OntapRestProvider, svmPeerUUID string, force bool) error {
			return nil
		}

		err := cleanupSvmPeering(provider, params)
		if err != nil {
			tt.Error("Error not returned")
		}
	})
	t.Run("WhenSuccessfulHybridRep", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockSnapmirrorClient := new(ontaprest.MockSnapmirrorClient)
		mockSvmClient := new(ontaprest.MockSVMClient)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		provider := &OntapRestProvider{}
		svmPeerUuid := "svm-peer-uuid"

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockClient.On("SVM").Return(mockSvmClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipList", &ontaprest.SnapmirrorRelationshipListParams{}).Return(nil, nil).Times(1)
		mockSnapmirrorClient.On("SnapmirrorRelationshipListDestinations", &ontaprest.SnapmirrorRelationshipListDestinationsParams{}).Return(snapmirrorList, nil).Times(1)
		getSvmPeer = func(provider *OntapRestProvider, srcSVMName, dstSVMName string) (*SvmPeer, error) {
			return &SvmPeer{UUID: svmPeerUuid}, nil
		}
		deleteSvmPeer = func(provider *OntapRestProvider, svmPeerUUID string, force bool) error {
			return nil
		}

		err := cleanupSvmPeering(provider, params1)
		if err != nil {
			tt.Error("Error not returned")
		}
	})
}
