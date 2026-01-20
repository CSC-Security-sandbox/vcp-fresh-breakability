package vsa

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	ontaprest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestCreateFlexCacheVolume(t *testing.T) {
	t.Run("Success", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(t)
		mockStorage := new(ontaprest.MockStorageClient)
		mockClient := new(ontaprest.MockRESTClient)
		rc := &OntapRestProvider{}

		volumeName := "testVolume"
		params := CreateFlexCacheVolumeParams{
			Name:             volumeName,
			SvmName:          "testSVM",
			AggregateName:    "testAggregate",
			OriginSVMName:    "originSVM",
			OriginVolumeName: "originVolume",
		}

		mockJob := &ontaprest.JobAccepted{
			JobUUID:      "testJobUUID",
			ResourceUUID: "testResourceUUID",
		}
		mockVolume := &ontaprest.Flexcache{
			Flexcache: models.Flexcache{
				UUID: nillable.ToPointer("testUUID"),
				Name: &volumeName,
			},
		}

		mm.EXPECT().getOntapClientFunc(mock.Anything).Return(mockClient, nil)
		mockClient.EXPECT().Storage().Return(mockStorage)
		mockStorage.EXPECT().FlexCacheVolumeCreate(mock.Anything).Return(mockVolume, mockJob, nil)
		mockClient.EXPECT().Poll(mockJob.JobUUID).Return(nil)

		resp, err := rc.CreateFlexCacheVolume(params)

		assert.NoError(t, err)
		assert.NotNil(t, resp)
		assert.Equal(t, volumeName, resp.Name)
		assert.Equal(t, "testUUID", resp.ExternalUUID)

		mockStorage.AssertExpectations(t)
		mockClient.AssertExpectations(t)
	})

	t.Run("Success_WithExportPolicy", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(t)
		mockStorage := new(ontaprest.MockStorageClient)
		mockClient := new(ontaprest.MockRESTClient)
		rc := &OntapRestProvider{}

		volumeName := "testVolume"
		exportPolicyName := "testPolicy"
		params := CreateFlexCacheVolumeParams{
			Name:             volumeName,
			SvmName:          "testSVM",
			AggregateName:    "testAggregate",
			OriginSVMName:    "originSVM",
			OriginVolumeName: "originVolume",
			ExportPolicy:     nillable.ToPointer(exportPolicyName),
		}

		mockJob := &ontaprest.JobAccepted{
			JobUUID:      "testJobUUID",
			ResourceUUID: "testResourceUUID",
		}
		mockVolume := &ontaprest.Flexcache{
			Flexcache: models.Flexcache{
				UUID: nillable.ToPointer("testUUID"),
				Name: &volumeName,
			},
		}

		mm.EXPECT().getOntapClientFunc(mock.Anything).Return(mockClient, nil)
		mockClient.EXPECT().Storage().Return(mockStorage)
		mockStorage.EXPECT().FlexCacheVolumeCreate(mock.Anything).Return(mockVolume, mockJob, nil)
		mockClient.EXPECT().Poll(mockJob.JobUUID).Return(nil)
		mockStorage.EXPECT().VolumeModify(mock.Anything).Return(false, &ontaprest.JobAccepted{JobUUID: "uuid"}, nil)
		mockClient.EXPECT().Poll("uuid").Return(nil)

		resp, err := rc.CreateFlexCacheVolume(params)

		assert.NoError(t, err)
		assert.NotNil(t, resp)
		assert.Equal(t, volumeName, resp.Name)
		assert.Equal(t, "testUUID", resp.ExternalUUID)

		mockStorage.AssertExpectations(t)
		mockClient.AssertExpectations(t)
	})

	t.Run("WhenGetOntapClientFuncError", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		rc := &OntapRestProvider{}
		params := CreateFlexCacheVolumeParams{}
		errMsg := "client error"
		mm.EXPECT().getOntapClientFunc(mock.Anything).Return(nil, errors.New(errMsg))

		resp, err := rc.CreateFlexCacheVolume(params)
		assert.Nil(tt, resp)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), errMsg)
	})

	t.Run("WhenFlexCacheVolumeCreateError", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		mockStorage := new(ontaprest.MockStorageClient)
		mockClient := new(ontaprest.MockRESTClient)
		rc := &OntapRestProvider{}
		params := CreateFlexCacheVolumeParams{}
		errMsg := "create error"
		mm.EXPECT().getOntapClientFunc(mock.Anything).Return(mockClient, nil)
		mockClient.EXPECT().Storage().Return(mockStorage)
		mockStorage.EXPECT().FlexCacheVolumeCreate(mock.Anything).Return(nil, nil, errors.New(errMsg))

		resp, err := rc.CreateFlexCacheVolume(params)
		assert.Nil(tt, resp)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), errMsg)
	})

	t.Run("WhenPollError", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		mockStorage := new(ontaprest.MockStorageClient)
		mockClient := new(ontaprest.MockRESTClient)
		rc := &OntapRestProvider{}
		params := CreateFlexCacheVolumeParams{}
		mockJob := &ontaprest.JobAccepted{JobUUID: "jobUUID"}
		mockVolume := &ontaprest.Flexcache{Flexcache: models.Flexcache{UUID: nillable.ToPointer("uuid"), Name: nillable.ToPointer("name")}}
		errMsg := "poll error"
		mm.EXPECT().getOntapClientFunc(mock.Anything).Return(mockClient, nil)
		mockClient.EXPECT().Storage().Return(mockStorage)
		mockStorage.EXPECT().FlexCacheVolumeCreate(mock.Anything).Return(mockVolume, mockJob, nil)
		mockClient.EXPECT().Poll(mockJob.JobUUID).Return(errors.New(errMsg))

		resp, err := rc.CreateFlexCacheVolume(params)
		assert.Nil(tt, resp)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), errMsg)
	})

	t.Run("WhenInvalidResponse", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		mockStorage := new(ontaprest.MockStorageClient)
		mockClient := new(ontaprest.MockRESTClient)
		rc := &OntapRestProvider{}
		params := CreateFlexCacheVolumeParams{}
		mm.EXPECT().getOntapClientFunc(mock.Anything).Return(mockClient, nil)
		mockClient.EXPECT().Storage().Return(mockStorage)
		// Return nil volume
		mockStorage.EXPECT().FlexCacheVolumeCreate(mock.Anything).Return(nil, nil, nil)

		resp, err := rc.CreateFlexCacheVolume(params)
		assert.Nil(tt, resp)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "invalid Volume response")
	})

	t.Run("WhenExportPolicyUpdateError", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		mockStorage := new(ontaprest.MockStorageClient)
		mockClient := new(ontaprest.MockRESTClient)
		rc := &OntapRestProvider{}

		volumeName := "testVolume"
		params := CreateFlexCacheVolumeParams{
			Name:             volumeName,
			SvmName:          "testSVM",
			AggregateName:    "testAggregate",
			OriginSVMName:    "originSVM",
			OriginVolumeName: "originVolume",
			ExportPolicy:     nillable.ToPointer("testPolicy"),
		}

		mockJob := &ontaprest.JobAccepted{
			JobUUID:      "testJobUUID",
			ResourceUUID: "testResourceUUID",
		}
		mockVolume := &ontaprest.Flexcache{
			Flexcache: models.Flexcache{
				UUID: nillable.ToPointer("testUUID"),
				Name: &volumeName,
			},
		}

		mm.EXPECT().getOntapClientFunc(mock.Anything).Return(mockClient, nil)
		mockClient.EXPECT().Storage().Return(mockStorage)
		mockStorage.EXPECT().FlexCacheVolumeCreate(mock.Anything).Return(mockVolume, mockJob, nil)
		mockClient.EXPECT().Poll(mockJob.JobUUID).Return(nil)
		// Simulate error during export policy update
		mockStorage.EXPECT().VolumeModify(mock.Anything).Return(false, nil, errors.New("update error"))

		resp, err := rc.CreateFlexCacheVolume(params)

		assert.Nil(tt, resp)
		assert.Error(tt, err)

		mockStorage.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	// Add these test cases to the existing TestCreateFlexCacheVolume function

	t.Run("Success_WithCacheConfig", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		mockStorage := new(ontaprest.MockStorageClient)
		mockClient := new(ontaprest.MockRESTClient)
		rc := &OntapRestProvider{}

		volumeName := "testVolume"
		writebackEnabled := true
		atimeScrubEnabled := true
		atimeScrubDays := int16(30)
		cifsChangeNotifyEnabled := false

		params := CreateFlexCacheVolumeParams{
			Name:                    volumeName,
			SvmName:                 "testSVM",
			AggregateName:           "testAggregate",
			OriginSVMName:           "originSVM",
			OriginVolumeName:        "originVolume",
			WritebackEnabled:        &writebackEnabled,
			AtimeScrubEnabled:       &atimeScrubEnabled,
			AtimeScrubDays:          &atimeScrubDays,
			CifsChangeNotifyEnabled: &cifsChangeNotifyEnabled,
		}

		mockJob := &ontaprest.JobAccepted{
			JobUUID:      "testJobUUID",
			ResourceUUID: "testResourceUUID",
		}
		mockVolume := &ontaprest.Flexcache{
			Flexcache: models.Flexcache{
				UUID: nillable.ToPointer("testUUID"),
				Name: &volumeName,
			},
		}

		mm.EXPECT().getOntapClientFunc(mock.Anything).Return(mockClient, nil)
		mockClient.EXPECT().Storage().Return(mockStorage)
		mockStorage.EXPECT().FlexCacheVolumeCreate(mock.MatchedBy(func(p *ontaprest.FlexCacheVolumeCreateParams) bool {
			return p.WritebackEnabled != nil && *p.WritebackEnabled == true &&
				p.AtimeScrubEnabled != nil && *p.AtimeScrubEnabled == true &&
				p.AtimeScrubPeriod != nil && *p.AtimeScrubPeriod == int16(30) &&
				p.CifsChangeNotifyEnabled != nil && *p.CifsChangeNotifyEnabled == false
		})).Return(mockVolume, mockJob, nil)
		mockClient.EXPECT().Poll(mockJob.JobUUID).Return(nil)

		resp, err := rc.CreateFlexCacheVolume(params)

		assert.NoError(tt, err)
		assert.NotNil(tt, resp)
		assert.Equal(tt, volumeName, resp.Name)
		assert.Equal(tt, "testUUID", resp.ExternalUUID)

		mockStorage.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("Success_WithGlobalFileLockingEnabled", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		mockStorage := new(ontaprest.MockStorageClient)
		mockClient := new(ontaprest.MockRESTClient)
		rc := &OntapRestProvider{}

		volumeName := "testVolume"
		globalFileLocking := true

		params := CreateFlexCacheVolumeParams{
			Name:                     volumeName,
			SvmName:                  "testSVM",
			AggregateName:            "testAggregate",
			OriginSVMName:            "originSVM",
			OriginVolumeName:         "originVolume",
			GlobalFileLockingEnabled: &globalFileLocking,
		}

		mockJob := &ontaprest.JobAccepted{
			JobUUID:      "testJobUUID",
			ResourceUUID: "testResourceUUID",
		}
		mockVolume := &ontaprest.Flexcache{
			Flexcache: models.Flexcache{
				UUID: nillable.ToPointer("testUUID"),
				Name: &volumeName,
			},
		}

		mm.EXPECT().getOntapClientFunc(mock.Anything).Return(mockClient, nil)
		mockClient.EXPECT().Storage().Return(mockStorage)
		mockStorage.EXPECT().FlexCacheVolumeCreate(mock.MatchedBy(func(p *ontaprest.FlexCacheVolumeCreateParams) bool {
			return p.GlobalFileLockingEnabled != nil && *p.GlobalFileLockingEnabled == true
		})).Return(mockVolume, mockJob, nil)
		mockClient.EXPECT().Poll(mockJob.JobUUID).Return(nil)

		resp, err := rc.CreateFlexCacheVolume(params)

		assert.NoError(tt, err)
		assert.NotNil(tt, resp)
		assert.Equal(tt, volumeName, resp.Name)
		assert.Equal(tt, "testUUID", resp.ExternalUUID)

		mockStorage.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("Success_WithAllCacheConfigAndGlobalFileLocking", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		mockStorage := new(ontaprest.MockStorageClient)
		mockClient := new(ontaprest.MockRESTClient)
		rc := &OntapRestProvider{}

		volumeName := "testVolume"
		junctionPath := "/vol/flexcache"
		exportPolicy := "testPolicy"
		writebackEnabled := true
		atimeScrubEnabled := true
		atimeScrubDays := int16(60)
		cifsChangeNotifyEnabled := true
		globalFileLocking := true

		params := CreateFlexCacheVolumeParams{
			Name:                     volumeName,
			SvmName:                  "testSVM",
			AggregateName:            "testAggregate",
			OriginSVMName:            "originSVM",
			OriginVolumeName:         "originVolume",
			JunctionPath:             &junctionPath,
			ExportPolicy:             &exportPolicy,
			WritebackEnabled:         &writebackEnabled,
			AtimeScrubEnabled:        &atimeScrubEnabled,
			AtimeScrubDays:           &atimeScrubDays,
			CifsChangeNotifyEnabled:  &cifsChangeNotifyEnabled,
			GlobalFileLockingEnabled: &globalFileLocking,
		}

		mockJob := &ontaprest.JobAccepted{
			JobUUID:      "testJobUUID",
			ResourceUUID: "testResourceUUID",
		}
		mockVolume := &ontaprest.Flexcache{
			Flexcache: models.Flexcache{
				UUID: nillable.ToPointer("testUUID"),
				Name: &volumeName,
			},
		}

		mm.EXPECT().getOntapClientFunc(mock.Anything).Return(mockClient, nil)
		mockClient.EXPECT().Storage().Return(mockStorage)
		mockStorage.EXPECT().FlexCacheVolumeCreate(mock.MatchedBy(func(p *ontaprest.FlexCacheVolumeCreateParams) bool {
			return p.Name == volumeName &&
				p.SvmName == "testSVM" &&
				p.OriginSvmName == "originSVM" &&
				p.OriginVolumeName == "originVolume" &&
				p.Path != nil && *p.Path == junctionPath &&
				p.WritebackEnabled != nil && *p.WritebackEnabled == true &&
				p.AtimeScrubEnabled != nil && *p.AtimeScrubEnabled == true &&
				p.AtimeScrubPeriod != nil && *p.AtimeScrubPeriod == int16(60) &&
				p.CifsChangeNotifyEnabled != nil && *p.CifsChangeNotifyEnabled == true &&
				p.GlobalFileLockingEnabled != nil && *p.GlobalFileLockingEnabled == true
		})).Return(mockVolume, mockJob, nil)
		mockClient.EXPECT().Poll(mockJob.JobUUID).Return(nil)
		mockStorage.EXPECT().VolumeModify(mock.Anything).Return(false, &ontaprest.JobAccepted{JobUUID: "uuid"}, nil)
		mockClient.EXPECT().Poll("uuid").Return(nil)

		resp, err := rc.CreateFlexCacheVolume(params)

		assert.NoError(tt, err)
		assert.NotNil(tt, resp)
		assert.Equal(tt, volumeName, resp.Name)
		assert.Equal(tt, "testUUID", resp.ExternalUUID)

		mockStorage.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("Success_WithPartialCacheConfig_OnlyWriteback", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		mockStorage := new(ontaprest.MockStorageClient)
		mockClient := new(ontaprest.MockRESTClient)
		rc := &OntapRestProvider{}

		volumeName := "testVolume"
		writebackEnabled := true

		params := CreateFlexCacheVolumeParams{
			Name:             volumeName,
			SvmName:          "testSVM",
			AggregateName:    "testAggregate",
			OriginSVMName:    "originSVM",
			OriginVolumeName: "originVolume",
			WritebackEnabled: &writebackEnabled,
			// Other CacheConfig fields are nil
		}

		mockJob := &ontaprest.JobAccepted{
			JobUUID:      "testJobUUID",
			ResourceUUID: "testResourceUUID",
		}
		mockVolume := &ontaprest.Flexcache{
			Flexcache: models.Flexcache{
				UUID: nillable.ToPointer("testUUID"),
				Name: &volumeName,
			},
		}

		mm.EXPECT().getOntapClientFunc(mock.Anything).Return(mockClient, nil)
		mockClient.EXPECT().Storage().Return(mockStorage)
		mockStorage.EXPECT().FlexCacheVolumeCreate(mock.MatchedBy(func(p *ontaprest.FlexCacheVolumeCreateParams) bool {
			return p.WritebackEnabled != nil && *p.WritebackEnabled == true &&
				p.AtimeScrubEnabled == nil &&
				p.AtimeScrubPeriod == nil &&
				p.CifsChangeNotifyEnabled == nil &&
				p.GlobalFileLockingEnabled == nil
		})).Return(mockVolume, mockJob, nil)
		mockClient.EXPECT().Poll(mockJob.JobUUID).Return(nil)

		resp, err := rc.CreateFlexCacheVolume(params)

		assert.NoError(tt, err)
		assert.NotNil(tt, resp)
		assert.Equal(tt, volumeName, resp.Name)
		assert.Equal(tt, "testUUID", resp.ExternalUUID)

		mockStorage.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("Success_NoCacheConfig_BackwardsCompatibility", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		mockStorage := new(ontaprest.MockStorageClient)
		mockClient := new(ontaprest.MockRESTClient)
		rc := &OntapRestProvider{}

		volumeName := "testVolume"
		params := CreateFlexCacheVolumeParams{
			Name:             volumeName,
			SvmName:          "testSVM",
			AggregateName:    "testAggregate",
			OriginSVMName:    "originSVM",
			OriginVolumeName: "originVolume",
			// No CacheConfig fields - all nil
		}

		mockJob := &ontaprest.JobAccepted{
			JobUUID:      "testJobUUID",
			ResourceUUID: "testResourceUUID",
		}
		mockVolume := &ontaprest.Flexcache{
			Flexcache: models.Flexcache{
				UUID: nillable.ToPointer("testUUID"),
				Name: &volumeName,
			},
		}

		mm.EXPECT().getOntapClientFunc(mock.Anything).Return(mockClient, nil)
		mockClient.EXPECT().Storage().Return(mockStorage)
		mockStorage.EXPECT().FlexCacheVolumeCreate(mock.MatchedBy(func(p *ontaprest.FlexCacheVolumeCreateParams) bool {
			// All CacheConfig fields should be nil for backwards compatibility
			return p.Name == volumeName &&
				p.WritebackEnabled == nil &&
				p.AtimeScrubEnabled == nil &&
				p.AtimeScrubPeriod == nil &&
				p.CifsChangeNotifyEnabled == nil &&
				p.GlobalFileLockingEnabled == nil
		})).Return(mockVolume, mockJob, nil)
		mockClient.EXPECT().Poll(mockJob.JobUUID).Return(nil)

		resp, err := rc.CreateFlexCacheVolume(params)

		assert.NoError(tt, err)
		assert.NotNil(tt, resp)
		assert.Equal(tt, volumeName, resp.Name)
		assert.Equal(tt, "testUUID", resp.ExternalUUID)

		mockStorage.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("Success_WithAtimeScrubConfig", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		mockStorage := new(ontaprest.MockStorageClient)
		mockClient := new(ontaprest.MockRESTClient)
		rc := &OntapRestProvider{}

		volumeName := "testVolume"
		atimeScrubEnabled := true
		atimeScrubDays := int16(45)

		params := CreateFlexCacheVolumeParams{
			Name:              volumeName,
			SvmName:           "testSVM",
			AggregateName:     "testAggregate",
			OriginSVMName:     "originSVM",
			OriginVolumeName:  "originVolume",
			AtimeScrubEnabled: &atimeScrubEnabled,
			AtimeScrubDays:    &atimeScrubDays,
		}

		mockJob := &ontaprest.JobAccepted{
			JobUUID:      "testJobUUID",
			ResourceUUID: "testResourceUUID",
		}
		mockVolume := &ontaprest.Flexcache{
			Flexcache: models.Flexcache{
				UUID: nillable.ToPointer("testUUID"),
				Name: &volumeName,
			},
		}

		mm.EXPECT().getOntapClientFunc(mock.Anything).Return(mockClient, nil)
		mockClient.EXPECT().Storage().Return(mockStorage)
		mockStorage.EXPECT().FlexCacheVolumeCreate(mock.MatchedBy(func(p *ontaprest.FlexCacheVolumeCreateParams) bool {
			return p.AtimeScrubEnabled != nil && *p.AtimeScrubEnabled == true &&
				p.AtimeScrubPeriod != nil && *p.AtimeScrubPeriod == int16(45)
		})).Return(mockVolume, mockJob, nil)
		mockClient.EXPECT().Poll(mockJob.JobUUID).Return(nil)

		resp, err := rc.CreateFlexCacheVolume(params)

		assert.NoError(tt, err)
		assert.NotNil(tt, resp)
		assert.Equal(tt, volumeName, resp.Name)
		assert.Equal(tt, "testUUID", resp.ExternalUUID)

		mockStorage.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})
}

func TestDeleteFlexCacheVolume(t *testing.T) {
	t.Run("Success", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		mockStorage := new(ontaprest.MockStorageClient)
		mockClient := new(ontaprest.MockRESTClient)
		rc := &OntapRestProvider{}

		volumeUUID := "testUUID"
		volumeName := "testVolume"
		accepted := &ontaprest.JobAccepted{JobUUID: ""}

		mm.EXPECT().getOntapClientFunc(mock.Anything).Return(mockClient, nil)
		mockClient.EXPECT().Storage().Return(mockStorage)
		mockStorage.EXPECT().FlexCacheVolumeDelete(mock.Anything).Return(accepted, nil)

		resp, err := rc.DeleteFlexCacheVolume(volumeUUID, volumeName)

		assert.NoError(tt, err)
		assert.NotNil(tt, resp)

		mockStorage.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenGetOntapClientFuncError", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		rc := &OntapRestProvider{}
		errMsg := "client error"
		mm.EXPECT().getOntapClientFunc(mock.Anything).Return(nil, errors.New(errMsg))

		resp, err := rc.DeleteFlexCacheVolume("uuid", "name")
		assert.Error(tt, err)
		assert.Nil(tt, resp)
		assert.Contains(tt, err.Error(), errMsg)
	})

	t.Run("WhenFlexCacheVolumeDeleteError", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		mockStorage := new(ontaprest.MockStorageClient)
		mockClient := new(ontaprest.MockRESTClient)
		rc := &OntapRestProvider{}
		errMsg := "delete error"
		mm.EXPECT().getOntapClientFunc(mock.Anything).Return(mockClient, nil)
		mockClient.EXPECT().Storage().Return(mockStorage)
		mockStorage.EXPECT().FlexCacheVolumeDelete(mock.Anything).Return(nil, errors.New(errMsg))

		resp, err := rc.DeleteFlexCacheVolume("uuid", "name")
		assert.Error(tt, err)
		assert.Nil(tt, resp)
		assert.Contains(tt, err.Error(), errMsg)
	})

	t.Run("WhenNoJobAcceptedReturned", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		mockStorage := new(ontaprest.MockStorageClient)
		mockClient := new(ontaprest.MockRESTClient)
		rc := &OntapRestProvider{}
		mm.EXPECT().getOntapClientFunc(mock.Anything).Return(mockClient, nil)
		mockClient.EXPECT().Storage().Return(mockStorage)
		// Return nil accepted
		mockStorage.EXPECT().FlexCacheVolumeDelete(mock.Anything).Return(nil, nil)

		resp, err := rc.DeleteFlexCacheVolume("uuid", "name")
		assert.Nil(tt, resp)
		assert.Nil(tt, err)
	})
}

func TestUpdateFlexCacheVolume(t *testing.T) {
	t.Run("GetOntapClientError", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		rc := &OntapRestProvider{}

		params := UpdateFlexCacheVolumeParams{
			UUID: "uuid",
		}

		mm.EXPECT().getOntapClientFunc(mock.Anything).Return(nil, errors.New("client error"))

		result, err := rc.UpdateFlexCacheVolume(params)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "client error")
	})

	t.Run("FlexCacheVolumeModifyError", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		mockStorage := new(ontaprest.MockStorageClient)
		mockClient := new(ontaprest.MockRESTClient)
		rc := &OntapRestProvider{}

		params := UpdateFlexCacheVolumeParams{
			UUID: "uuid",
		}

		mm.EXPECT().getOntapClientFunc(mock.Anything).Return(mockClient, nil)
		mockClient.EXPECT().Storage().Return(mockStorage)
		mockStorage.EXPECT().FlexCacheVolumeModify(mock.Anything).Return(false, nil, errors.New("FlexCacheVolumeModify error"))

		result, err := rc.UpdateFlexCacheVolume(params)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "FlexCacheVolumeModify error")

		mockStorage.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("Success_WithJobUUID", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		mockStorage := new(ontaprest.MockStorageClient)
		mockClient := new(ontaprest.MockRESTClient)
		rc := &OntapRestProvider{}

		params := UpdateFlexCacheVolumeParams{
			UUID: "uuid",
		}

		jobUUID := "async-job-uuid"

		mm.EXPECT().getOntapClientFunc(mock.Anything).Return(mockClient, nil)
		mockClient.EXPECT().Storage().Return(mockStorage)
		mockStorage.EXPECT().FlexCacheVolumeModify(mock.Anything).Return(true, &ontaprest.JobAccepted{
			JobUUID: jobUUID,
		}, nil)

		result, err := rc.UpdateFlexCacheVolume(params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, jobUUID, result.JobUUID)

		mockStorage.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("Success_SynchronousCompletion", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		mockStorage := new(ontaprest.MockStorageClient)
		mockClient := new(ontaprest.MockRESTClient)
		rc := &OntapRestProvider{}

		params := UpdateFlexCacheVolumeParams{
			UUID: "uuid",
		}

		mm.EXPECT().getOntapClientFunc(mock.Anything).Return(mockClient, nil)
		mockClient.EXPECT().Storage().Return(mockStorage)
		mockStorage.EXPECT().FlexCacheVolumeModify(mock.Anything).Return(true, nil, nil)

		result, err := rc.UpdateFlexCacheVolume(params)

		assert.NoError(tt, err)
		assert.Nil(tt, result)

		mockStorage.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("Success_EmptyJobUUID", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		mockStorage := new(ontaprest.MockStorageClient)
		mockClient := new(ontaprest.MockRESTClient)
		rc := &OntapRestProvider{}

		params := UpdateFlexCacheVolumeParams{
			UUID: "uuid",
		}

		mm.EXPECT().getOntapClientFunc(mock.Anything).Return(mockClient, nil)
		mockClient.EXPECT().Storage().Return(mockStorage)
		// Return job with empty UUID - should be treated as synchronous
		mockStorage.EXPECT().FlexCacheVolumeModify(mock.Anything).Return(true, &ontaprest.JobAccepted{
			JobUUID: "",
		}, nil)

		result, err := rc.UpdateFlexCacheVolume(params)

		assert.NoError(tt, err)
		assert.Nil(tt, result)

		mockStorage.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("FailureNoJob", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		mockStorage := new(ontaprest.MockStorageClient)
		mockClient := new(ontaprest.MockRESTClient)
		rc := &OntapRestProvider{}

		params := UpdateFlexCacheVolumeParams{
			UUID: "uuid",
		}

		mm.EXPECT().getOntapClientFunc(mock.Anything).Return(mockClient, nil)
		mockClient.EXPECT().Storage().Return(mockStorage)
		mockStorage.EXPECT().FlexCacheVolumeModify(mock.Anything).Return(false, nil, nil)

		result, err := rc.UpdateFlexCacheVolume(params)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "FlexCache volume update failed")

		mockStorage.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("Success_JobSubmitted", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		mockStorage := new(ontaprest.MockStorageClient)
		mockClient := new(ontaprest.MockRESTClient)
		rc := &OntapRestProvider{}

		params := UpdateFlexCacheVolumeParams{
			UUID: "uuid",
		}

		jobUUID := "jobUUID"

		mm.EXPECT().getOntapClientFunc(mock.Anything).Return(mockClient, nil)
		mockClient.EXPECT().Storage().Return(mockStorage)
		mockStorage.EXPECT().FlexCacheVolumeModify(mock.Anything).Return(false, &ontaprest.JobAccepted{
			JobUUID: jobUUID,
		}, nil)

		result, err := rc.UpdateFlexCacheVolume(params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, jobUUID, result.JobUUID)

		mockStorage.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})
}
