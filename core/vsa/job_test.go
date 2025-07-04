package vsa

import (
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/cluster"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	ontaprest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestJobGet(t *testing.T) {
	t.Run("onSuccessfulJobGet", func(t *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockClusterClient := new(ontaprest.MockClusterClient)
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		mockClient.On("Cluster").Return(mockClusterClient)
		mockClusterClient.On("GetJob", "job-uuid-1234").Return(&cluster.JobGetOK{
			Payload: &models.Job{
				UUID: nillable.ToPointer(strfmt.UUID("job-uuid-1234")),
			},
		}, nil)

		ontapRestProvider := &OntapRestProvider{}
		job, err := ontapRestProvider.JobGet("job-uuid-1234")
		assert.Nil(t, err)
		assert.NotNil(t, job)
		assert.Equal(t, "job-uuid-1234", job.UUID)
	})
	t.Run("onFailureJobGet", func(t *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockClusterClient := new(ontaprest.MockClusterClient)
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		mockClient.On("Cluster").Return(mockClusterClient)
		mockClusterClient.On("GetJob", "job-uuid-1234").Return(&cluster.JobGetOK{
			Payload: &models.Job{
				UUID: nillable.ToPointer(strfmt.UUID("job-uuid-1234")),
				Error: &models.JobInlineError{
					Code:    strPtr("400"),
					Message: strPtr("failed"),
				},
			},
		}, nil)

		ontapRestProvider := &OntapRestProvider{}
		job, err := ontapRestProvider.JobGet("job-uuid-1234")
		assert.Nil(t, err)
		assert.NotNil(t, job)
		assert.Equal(t, "job-uuid-1234", job.UUID)
		assert.NotNil(t, job.Error)
		assert.Equal(t, "400", job.Error.Code)
		assert.Equal(t, "failed", job.Error.Message)
	})
}
