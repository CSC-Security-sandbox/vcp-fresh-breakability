package ontap_rest

import (
	"github.com/go-openapi/strfmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/storage"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestFlexCacheCreate(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		response, job, err := client.FlexCacheVolumeCreate(&FlexCacheVolumeCreateParams{})
		assert.EqualError(tt, err, transport.err.Error())
		assert.Nil(tt, response)
		assert.Nil(tt, job)
	})

	t.Run("WhenResponseHasNoFlexCacheInfo_ThenReturnUnexpectedResponseError", func(tt *testing.T) {
		transport := &mockTransport{response: &storage.FlexcacheCreateCreated{
			Payload: &models.FlexcacheJobLinkResponse{
				Records: []*models.Flexcache{},
			},
		}}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		response, job, err := client.FlexCacheVolumeCreate(&FlexCacheVolumeCreateParams{})
		assert.EqualError(tt, err, "unexpected response from server while creating FlexCache volume - did not receive exactly one FlexCache volume")
		assert.Nil(tt, response)
		assert.Nil(tt, job)
	})

	t.Run("WhenResponseHasMultipleFlexCaches_ThenReturnUnexpectedResponseError", func(tt *testing.T) {
		flexName1 := "flexcache1"
		flexName2 := "flexcache2"
		transport := &mockTransport{response: &storage.FlexcacheCreateCreated{
			Payload: &models.FlexcacheJobLinkResponse{
				Records: []*models.Flexcache{
					{Name: &flexName1},
					{Name: &flexName2},
				},
			},
		}}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		response, job, err := client.FlexCacheVolumeCreate(&FlexCacheVolumeCreateParams{})
		assert.EqualError(tt, err, "unexpected response from server while creating FlexCache volume - did not receive exactly one FlexCache volume")
		assert.Nil(tt, response)
		assert.Nil(tt, job)
	})

	t.Run("WhenSuccessfulWithCreatedResponse_ThenReturnFlexCache", func(tt *testing.T) {
		flexName := "test-flexcache"
		transport := &mockTransport{response: &storage.FlexcacheCreateCreated{
			Payload: &models.FlexcacheJobLinkResponse{
				Records: []*models.Flexcache{{Name: &flexName}},
			},
		}}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		response, job, err := client.FlexCacheVolumeCreate(&FlexCacheVolumeCreateParams{})
		assert.NoError(tt, err)
		assert.NotNil(tt, response)
		assert.Nil(tt, job)
		assert.Equal(tt, flexName, *response.Name)
	})

	t.Run("WhenSuccessfulWithAcceptedResponse_ThenReturnFlexCacheAndJob", func(tt *testing.T) {
		flexName := "test-flexcache"
		jobUUID := "job-uuid"
		transport := &mockTransport{response: &storage.FlexcacheCreateAccepted{
			Payload: &models.FlexcacheJobLinkResponse{
				Records: []*models.Flexcache{{Name: &flexName}},
				Job:     &models.JobLink{UUID: nillable.ToPointer(strfmt.UUID(jobUUID))},
			},
		}}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		response, job, err := client.FlexCacheVolumeCreate(&FlexCacheVolumeCreateParams{})
		assert.NoError(tt, err)
		assert.NotNil(tt, response)
		assert.NotNil(tt, job)
		assert.Equal(tt, flexName, *response.Name)
		assert.Equal(tt, jobUUID, job.JobUUID)
	})

	t.Run("WhenEmptyRecordsInResponse_ThenThrowError", func(tt *testing.T) {
		transport := &mockTransport{response: &storage.FlexcacheCreateAccepted{
			Payload: &models.FlexcacheJobLinkResponse{
				Records: []*models.Flexcache{},
			},
		}}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		response, job, err := client.FlexCacheVolumeCreate(&FlexCacheVolumeCreateParams{})
		assert.EqualError(tt, err, "unexpected response from server while creating FlexCache volume - did not receive exactly one FlexCache volume")
		assert.Nil(tt, response)
		assert.Nil(tt, job)
	})

	t.Run("WhenMoreThanOneRecordsInResponse_ThenThrowError", func(tt *testing.T) {
		flexName := "test-flexcache"
		transport := &mockTransport{response: &storage.FlexcacheCreateAccepted{
			Payload: &models.FlexcacheJobLinkResponse{
				Records: []*models.Flexcache{{Name: &flexName}, {Name: &flexName}},
			},
		}}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		response, job, err := client.FlexCacheVolumeCreate(&FlexCacheVolumeCreateParams{})
		assert.EqualError(tt, err, "unexpected response from server while creating FlexCache volume - did not receive exactly one FlexCache volume")
		assert.Nil(tt, response)
		assert.Nil(tt, job)
	})
}

func TestFlexCacheVolumeDelete(t *testing.T) {
	t.Run("WhenRESTCallFails", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		job, err := client.FlexCacheVolumeDelete(&FlexCacheVolumeDeleteParams{UUID: "someUUID"})
		assert.EqualError(tt, err, transport.err.Error())
		assert.Nil(tt, job)
	})

	t.Run("WhenRestCallFailsForCollectionDelete", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		job, err := client.FlexCacheVolumeDelete(&FlexCacheVolumeDeleteParams{Name: "someName"})
		assert.EqualError(tt, err, transport.err.Error())
		assert.Nil(tt, job)
	})

	t.Run("WhenUuidAndNameAreEmpty", func(tt *testing.T) {
		transport := &mockTransport{response: &storage.FlexcacheDeleteCollectionOK{}}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		job, err := client.FlexCacheVolumeDelete(&FlexCacheVolumeDeleteParams{})
		assert.Error(tt, err)
		assert.Nil(tt, job)
		assert.EqualError(tt, err, "no name filter provided for FlexCacheDeleteCollection")
	})

	t.Run("WhenUnexpectedResponse", func(tt *testing.T) {
		transport := &mockTransport{
			response: &storage.FlexcacheDeleteCollectionOK{},
		}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		job, err := client.FlexCacheVolumeDelete(&FlexCacheVolumeDeleteParams{Name: "Name"})
		assert.Error(tt, err)
		assert.Nil(tt, job)
		assert.EqualError(tt, err, "unexpected response from server while deleting FlexCache volume")
	})

	t.Run("WhenFlexCacheUUIDIsPassed", func(tt *testing.T) {
		transport := &mockTransport{
			response: &storage.FlexcacheDeleteAccepted{
				Payload: &models.FlexcacheJobLinkResponse{
					Job: &models.JobLink{UUID: nillable.ToPointer(strfmt.UUID("jobUUID"))},
				},
			},
		}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		job, err := client.FlexCacheVolumeDelete(&FlexCacheVolumeDeleteParams{UUID: "someUUID"})
		assert.NoError(tt, err)
		assert.NotNil(tt, job)
	})

	t.Run("WhenFlexCacheUUIDIsPassedAndDeletedResponse", func(tt *testing.T) {
		transport := &mockTransport{
			response: &storage.FlexcacheDeleteOK{},
		}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		job, err := client.FlexCacheVolumeDelete(&FlexCacheVolumeDeleteParams{UUID: "someUUID"})
		assert.NoError(tt, err)
		assert.Nil(tt, job)
	})

	t.Run("WhenFlexCacheNameIsPassed", func(tt *testing.T) {
		transport := &mockTransport{
			response: &storage.FlexcacheDeleteCollectionAccepted{
				Payload: &models.FlexcacheJobLinkResponse{
					Job: &models.JobLink{UUID: nillable.ToPointer(strfmt.UUID("jobUUID"))},
				},
			},
		}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		job, err := client.FlexCacheVolumeDelete(&FlexCacheVolumeDeleteParams{Name: "flexcacheName"})
		assert.NoError(tt, err)
		assert.NotNil(tt, job)
	})
}

func TestFlexCacheVolumeModify(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("internal server error")}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		response, job, err := client.FlexCacheVolumeModify(&FlexcacheModifyParams{})
		assert.EqualError(tt, err, transport.err.Error())
		assert.False(tt, response)
		assert.Nil(tt, job)
	})

	t.Run("WhenRESTCall_ReturnsFlexCacheModifyOK", func(tt *testing.T) {
		transport := &mockTransport{response: &storage.FlexcacheModifyOK{
			Payload: &models.FlexcacheJobLinkResponse{
				Records: []*models.Flexcache{},
			},
		}}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		response, job, err := client.FlexCacheVolumeModify(&FlexcacheModifyParams{})
		assert.True(tt, response)
		assert.Nil(tt, job)
		assert.Nil(tt, err)
	})

	t.Run("WhenRESTCall_ReturnsFlexCacheModifyAccepted", func(tt *testing.T) {
		jobUUID := "job-uuid"
		transport := &mockTransport{response: &storage.FlexcacheModifyAccepted{
			Payload: &models.FlexcacheJobLinkResponse{
				Job: &models.JobLink{UUID: nillable.ToPointer(strfmt.UUID(jobUUID))},
			},
		}}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		response, job, err := client.FlexCacheVolumeModify(&FlexcacheModifyParams{})
		assert.NoError(tt, err)
		assert.False(tt, response)
		assert.NotNil(tt, job)
		assert.Equal(tt, jobUUID, job.JobUUID)
	})
}
