package ontap_rest

import (
	"errors"
	"github.com/go-openapi/strfmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/cloud"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestCloudTargetCreate(t *testing.T) {
	t.Run("WhenRESTCallFails", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		clust := cloud.New(transport, nil)
		client := &cloudClient{api: clust}
		name := "target1"
		params := &CloudTargetCreateParams{Name: &name}
		response, job, err := client.CloudTargetCreate(params)
		assert.EqualError(tt, err, transport.err.Error())
		assert.Nil(tt, response)
		assert.Nil(tt, job)
	})
	t.Run("WhenSuccessful", func(tt *testing.T) {
		transport := &mockTransport{response: &cloud.CloudTargetCreateCreated{
			Payload: &models.CloudTargetJobLinkResponse{
				Records: []*models.CloudTarget{
					{Name: nillable.ToPointer("target1")},
				},
			},
		}}
		clust := cloud.New(transport, nil)
		client := &cloudClient{api: clust}
		name := "target1"
		params := &CloudTargetCreateParams{Name: &name}
		response, job, err := client.CloudTargetCreate(params)
		assert.NoError(tt, err)
		assert.NotNil(tt, response)
		assert.Nil(tt, job)
		assert.Equal(tt, "target1", *response.Name)
	})
	t.Run("WhenSuccessfulAsyncJob", func(tt *testing.T) {
		transport := &mockTransport{response: &cloud.CloudTargetCreateAccepted{
			Payload: &models.CloudTargetJobLinkResponse{
				Job: &models.JobLink{UUID: nillable.ToPointer(strfmt.UUID("job-uuid"))},
				Records: []*models.CloudTarget{
					{Name: nillable.ToPointer("target1")},
				},
			},
		}}
		clust := cloud.New(transport, nil)
		client := &cloudClient{api: clust}
		name := "target1"
		params := &CloudTargetCreateParams{Name: &name}
		response, job, err := client.CloudTargetCreate(params)
		assert.NoError(tt, err)
		assert.NotNil(tt, response)
		assert.NotNil(tt, job)
		assert.Equal(tt, "job-uuid", job.JobUUID)
	})
}

func TestCloudTargetGet(t *testing.T) {
	t.Run("WhenRESTCallFails", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		clust := cloud.New(transport, nil)
		client := &cloudClient{api: clust}
		name := "target1"
		response, err := client.CloudTargetGet(&name)
		assert.EqualError(tt, err, transport.err.Error())
		assert.Nil(tt, response)
	})
	t.Run("WhenNotFound", func(tt *testing.T) {
		transport := &mockTransport{response: &cloud.CloudTargetCollectionGetOK{
			Payload: &models.CloudTargetResponse{
				CloudTargetResponseInlineRecords: []*models.CloudTarget{},
			},
		}}
		clust := cloud.New(transport, nil)
		client := &cloudClient{api: clust}
		name := "nonexistent"
		response, err := client.CloudTargetGet(&name)
		assert.EqualError(tt, err, "cloud target not found")
		assert.Nil(tt, response)
	})
	t.Run("WhenSuccessful", func(tt *testing.T) {
		transport := &mockTransport{response: &cloud.CloudTargetCollectionGetOK{
			Payload: &models.CloudTargetResponse{
				CloudTargetResponseInlineRecords: []*models.CloudTarget{
					{Name: nillable.ToPointer("target1")},
				},
			},
		}}
		clust := cloud.New(transport, nil)
		client := &cloudClient{api: clust}
		name := "target1"
		response, err := client.CloudTargetGet(&name)
		assert.NoError(tt, err)
		assert.NotNil(tt, response)
		assert.Equal(tt, "target1", *response.Name)
	})
	t.Run("WhenNameNotSame", func(tt *testing.T) {
		name := "target1"
		transport := &mockTransport{response: &cloud.CloudTargetCollectionGetOK{
			Payload: &models.CloudTargetResponse{
				CloudTargetResponseInlineRecords: []*models.CloudTarget{
					{Name: nillable.ToPointer("target2")},
				},
			},
		}}
		clust := cloud.New(transport, nil)
		client := &cloudClient{api: clust}
		response, err := client.CloudTargetGet(&name)
		assert.EqualError(tt, err, "cloud target not found")
		assert.Nil(tt, response)
	})
}
