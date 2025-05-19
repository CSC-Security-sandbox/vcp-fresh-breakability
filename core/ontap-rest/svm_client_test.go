package ontap_rest

import (
	"errors"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/svm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestSvmGet(t *testing.T) {
	t.Run("WhenParamsAreNil_ThenReturnError", func(tt *testing.T) {
		client := &svmClient{}
		response, err := client.SvmGet(nil)
		assert.EqualError(tt, err, "params for SvmGet cannot be nil")
		assert.Nil(tt, response)
	})

	t.Run("WhenSvmNameIsNil_ThenReturnError", func(tt *testing.T) {
		client := &svmClient{}
		response, err := client.SvmGet(&SvmGetParams{})
		assert.EqualError(tt, err, "params.SvmName cannot be empty")
		assert.Nil(tt, response)
	})

	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		svmAPI := svm.New(transport, nil)
		client := &svmClient{api: svmAPI}
		response, err := client.SvmGet(&SvmGetParams{SvmName: "test"})
		assert.EqualError(tt, err, transport.err.Error())
		assert.Nil(tt, response)
	})

	t.Run("WhenResponseRecordIsEmpty_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{response: &svm.SvmCollectionGetOK{
			Payload: &models.SvmResponse{
				SvmResponseInlineRecords: []*models.Svm{},
			},
		}}
		svmAPI := svm.New(transport, nil)
		client := &svmClient{api: svmAPI}
		response, err := client.SvmGet(&SvmGetParams{SvmName: "test"})
		assert.EqualError(tt, err, "svm 'test' not found")
		assert.Nil(tt, response)
	})

	t.Run("WhenResponseRecordIsGreaterThan1_ThenReturnError", func(tt *testing.T) {
		name := "test-svm"
		transport := &mockTransport{response: &svm.SvmCollectionGetOK{
			Payload: &models.SvmResponse{
				SvmResponseInlineRecords: []*models.Svm{
					{Name: &name},
					{Name: &name},
				},
			},
		}}
		svmAPI := svm.New(transport, nil)
		client := &svmClient{api: svmAPI}
		response, err := client.SvmGet(&SvmGetParams{SvmName: "test"})
		assert.EqualError(tt, err, "unexpected number of svms returned")
		assert.Nil(tt, response)
	})

	t.Run("WhenSingleRecordReturned_ThenReturnSvm", func(tt *testing.T) {
		name := "test-svm"
		transport := &mockTransport{response: &svm.SvmCollectionGetOK{
			Payload: &models.SvmResponse{
				SvmResponseInlineRecords: []*models.Svm{
					{Name: &name},
				},
			},
		}}
		svmAPI := svm.New(transport, nil)
		client := &svmClient{api: svmAPI}
		response, err := client.SvmGet(&SvmGetParams{SvmName: "test"})
		assert.NoError(tt, err)
		assert.NotNil(tt, response)
		assert.Equal(tt, name, *response.Name)
	})
}

func TestSvmCreate(t *testing.T) {
	t.Run("WhenParamsAreNil_ThenReturnError", func(tt *testing.T) {
		client := &svmClient{}
		response, job, err := client.SvmCreate(nil)
		assert.EqualError(tt, err, "params for SvmCreate cannot be nil")
		assert.Nil(tt, response)
		assert.Nil(tt, job)
	})

	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		svmAPI := svm.New(transport, nil)
		client := &svmClient{api: svmAPI}
		response, job, err := client.SvmCreate(&SvmCreateParams{Name: "test"})
		assert.EqualError(tt, err, transport.err.Error())
		assert.Nil(tt, response)
		assert.Nil(tt, job)
	})

	t.Run("WhenSvmCreateIsSuccessful_ThenReturnCreatedSvm", func(tt *testing.T) {
		name := "test-svm"
		transport := &mockTransport{response: &svm.SvmCreateCreated{
			Payload: &models.SvmJobLinkResponse{
				Records: []*models.Svm{{Name: &name}},
			},
		}}
		svmAPI := svm.New(transport, nil)
		client := &svmClient{api: svmAPI}
		response, job, err := client.SvmCreate(&SvmCreateParams{Name: "test"})
		assert.NoError(tt, err)
		assert.NotNil(tt, response)
		assert.Nil(tt, job)
		assert.Equal(tt, name, *response.Name)
	})

	t.Run("WhenSvmCreateIsInProgress_ThenReturnJob", func(tt *testing.T) {
		svmUUID := "svmUUID"
		jobUUID := "jobUUID"
		transport := &mockTransport{response: &svm.SvmCreateAccepted{
			Payload: &models.SvmJobLinkResponse{
				Records: []*models.Svm{{UUID: &svmUUID}},
				Job: &models.JobLink{
					UUID: nillable.ToPointer(strfmt.UUID(jobUUID)),
				},
			},
		}}
		svmAPI := svm.New(transport, nil)
		client := &svmClient{api: svmAPI}
		response, job, err := client.SvmCreate(&SvmCreateParams{Name: "test"})
		assert.NoError(tt, err)
		assert.NotNil(tt, response)
		assert.NotNil(tt, job)
		assert.Equal(tt, svmUUID, *response.UUID)
		assert.Equal(tt, svmUUID, job.ResourceUUID)
		assert.Equal(tt, jobUUID, job.JobUUID)
	})
}

func TestSvmDelete(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		svmAPI := svm.New(transport, nil)
		client := &svmClient{api: svmAPI}
		done, job, err := client.SvmDelete("someUUID")
		assert.EqualError(tt, err, transport.err.Error())
		assert.False(tt, done)
		assert.Nil(tt, job)
	})

	t.Run("WhenSvmDeleteIsSuccessful_ThenReturnTrue", func(tt *testing.T) {
		transport := &mockTransport{response: &svm.SvmDeleteOK{}}
		svmAPI := svm.New(transport, nil)
		client := &svmClient{api: svmAPI}
		done, job, err := client.SvmDelete("someUUID")
		assert.True(tt, done)
		assert.NoError(tt, err)
		assert.Nil(tt, job)
	})

	t.Run("WhenSvmDeleteJobIsAccepted_ThenReturnTrue", func(tt *testing.T) {
		transport := &mockTransport{response: &svm.SvmDeleteAccepted{
			Payload: &models.SvmJobLinkResponse{
				Job: &models.JobLink{UUID: nillable.ToPointer(strfmt.UUID("jobUUID"))},
			},
		}}
		svmAPI := svm.New(transport, nil)
		client := &svmClient{api: svmAPI}
		done, job, err := client.SvmDelete("someUUID")
		assert.NoError(tt, err)
		assert.False(tt, done)
		assert.NotNil(tt, job)
		assert.Equal(tt, "jobUUID", job.JobUUID)
	})
}

func TestSvmModify(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		svmAPI := svm.New(transport, nil)
		client := &svmClient{api: svmAPI}
		done, job, err := client.SvmModify(&SvmModifyParams{})
		assert.EqualError(tt, err, transport.err.Error())
		assert.False(tt, done)
		assert.Nil(tt, job)
	})

	t.Run("WhenSvmModifyJobIsAccepted_ThenReturnTrue", func(tt *testing.T) {
		transport := &mockTransport{response: &svm.SvmModifyAccepted{
			Payload: &models.SvmJobLinkResponse{
				Job: &models.JobLink{UUID: nillable.ToPointer(strfmt.UUID("jobUUID"))},
			},
		}}
		svmAPI := svm.New(transport, nil)
		client := &svmClient{api: svmAPI}
		done, job, err := client.SvmModify(&SvmModifyParams{})
		assert.NoError(tt, err)
		assert.False(tt, done)
		assert.NotNil(tt, job)
		assert.Equal(tt, "jobUUID", job.JobUUID)
	})

	t.Run("WhenSvmModifyIsSuccessful_ThenReturnTrue", func(tt *testing.T) {
		transport := &mockTransport{response: &svm.SvmModifyOK{}}
		svmAPI := svm.New(transport, nil)
		client := &svmClient{api: svmAPI}
		done, job, err := client.SvmModify(&SvmModifyParams{})
		assert.NoError(tt, err)
		assert.True(tt, done)
		assert.Nil(tt, job)
	})
}
