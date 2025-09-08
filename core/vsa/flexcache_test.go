package vsa

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	ontaprest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"testing"
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
}
