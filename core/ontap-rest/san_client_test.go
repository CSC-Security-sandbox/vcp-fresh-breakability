package ontap_rest

import (
	"errors"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	san "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/s_a_n"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestIscsiServiceGet(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		sanAPI := san.New(transport, nil)
		client := &sanClient{api: sanAPI}
		response, err := client.IscsiServiceGet(&IscsiGetParams{})
		assert.EqualError(tt, err, transport.err.Error())
		assert.Nil(tt, response)
	})

	t.Run("WhenNoRecordsReturned_ThenReturnNotFoundError", func(tt *testing.T) {
		transport := &mockTransport{response: &san.IscsiServiceCollectionGetOK{
			Payload: &models.IscsiServiceResponse{
				IscsiServiceResponseInlineRecords: []*models.IscsiService{},
			},
		}}
		sanAPI := san.New(transport, nil)
		client := &sanClient{api: sanAPI}
		response, err := client.IscsiServiceGet(&IscsiGetParams{})
		assert.EqualError(tt, err, "iscsi service not found")
		assert.Nil(tt, response)
	})

	t.Run("WhenMultipleRecordsReturned_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{response: &san.IscsiServiceCollectionGetOK{
			Payload: &models.IscsiServiceResponse{
				IscsiServiceResponseInlineRecords: []*models.IscsiService{
					{}, {},
				},
			},
		}}
		sanAPI := san.New(transport, nil)
		client := &sanClient{api: sanAPI}
		response, err := client.IscsiServiceGet(&IscsiGetParams{})
		assert.EqualError(tt, err, "unexpected response when querying for iscsi service")
		assert.Nil(tt, response)
	})

	t.Run("WhenSingleRecordReturned_ThenReturnIscsiService", func(tt *testing.T) {
		iscsiService := &models.IscsiService{}
		transport := &mockTransport{response: &san.IscsiServiceCollectionGetOK{
			Payload: &models.IscsiServiceResponse{
				IscsiServiceResponseInlineRecords: []*models.IscsiService{iscsiService},
			},
		}}
		sanAPI := san.New(transport, nil)
		client := &sanClient{api: sanAPI}
		response, err := client.IscsiServiceGet(&IscsiGetParams{})
		assert.NoError(tt, err)
		assert.NotNil(tt, response)
		assert.Equal(tt, iscsiService, &response.IscsiService)
	})
}

func TestIscsiServiceCreate(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		sanAPI := san.New(transport, nil)
		client := &sanClient{api: sanAPI}
		err := client.IscsiServiceCreate(&IscsiCreateParams{})
		assert.EqualError(tt, err, transport.err.Error())
	})

	t.Run("WhenSuccessful_ThenNoError", func(tt *testing.T) {
		transport := &mockTransport{response: &san.IscsiServiceCreateCreated{}}
		sanAPI := san.New(transport, nil)
		client := &sanClient{api: sanAPI}
		err := client.IscsiServiceCreate(&IscsiCreateParams{})
		assert.NoError(tt, err)
	})
}

func TestIGroupCreate(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		sanAPI := san.New(transport, nil)
		client := &sanClient{api: sanAPI}
		_, err := client.IGroupCreate(&IgroupCreateParams{})
		assert.EqualError(tt, err, transport.err.Error())
	})

	t.Run("WhenSuccessful_ThenReturnIgroupName", func(tt *testing.T) {
		igroupName := "igroup1"
		transport := &mockTransport{response: &san.IgroupCreateCreated{
			Payload: &models.IgroupResponse{
				IgroupResponseInlineRecords: []*models.Igroup{
					{Name: &igroupName},
				},
			},
		}}
		sanAPI := san.New(transport, nil)
		client := &sanClient{api: sanAPI}
		name, err := client.IGroupCreate(&IgroupCreateParams{})
		assert.NoError(tt, err)
		assert.Equal(tt, igroupName, name)
	})
}

func TestLunCreate(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		sanAPI := san.New(transport, nil)
		client := &sanClient{api: sanAPI}
		_, err := client.LunCreate(&LunCreateParams{})
		assert.EqualError(tt, err, transport.err.Error())
	})

	t.Run("WhenAcceptedResponse_ThenPollAndReturnLun", func(tt *testing.T) {
		poller := NewMockPoller(tt)
		poller.Mock.On("Poll", mock.Anything).Return(nil)
		lunName := "lun1"
		jobUUID := "job-uuid"
		transport := &mockTransport{response: &san.LunCreateAccepted{
			Payload: &models.LunJobLinkResponse{
				Records: []*models.Lun{{Name: &lunName}},
				Job:     &models.JobLink{UUID: nillable.ToPointer(strfmt.UUID(jobUUID))},
			},
		}}
		sanAPI := san.New(transport, nil)
		client := &sanClient{api: sanAPI, poller: poller}
		lun, err := client.LunCreate(&LunCreateParams{})
		assert.NoError(tt, err)
		assert.NotNil(tt, lun)
		assert.Equal(tt, lunName, *lun.Name)
	})

	t.Run("WhenCreatedResponse_ThenReturnLun", func(tt *testing.T) {
		lunName := "lun1"
		transport := &mockTransport{response: &san.LunCreateCreated{
			Payload: &models.LunResponse{
				LunResponseInlineRecords: []*models.Lun{{Name: &lunName}},
			},
		}}
		sanAPI := san.New(transport, nil)
		client := &sanClient{api: sanAPI}
		lun, err := client.LunCreate(&LunCreateParams{})
		assert.NoError(tt, err)
		assert.NotNil(tt, lun)
		assert.Equal(tt, lunName, *lun.Name)
	})

	t.Run("WhenPollFails_ThenReturnError", func(tt *testing.T) {
		poller := NewMockPoller(tt)
		poller.Mock.On("Poll", mock.Anything).Return(errors.New("polling failed"))
		jobUUID := "job-uuid"
		transport := &mockTransport{response: &san.LunCreateAccepted{
			Payload: &models.LunJobLinkResponse{
				Records: []*models.Lun{{}},
				Job:     &models.JobLink{UUID: nillable.ToPointer(strfmt.UUID(jobUUID))},
			},
		}}
		sanAPI := san.New(transport, nil)
		client := &sanClient{api: sanAPI, poller: poller}

		lun, err := client.LunCreate(&LunCreateParams{})
		assert.EqualError(tt, err, "polling failed")
		assert.Nil(tt, lun)
		poller.AssertCalled(tt, "Poll", jobUUID)
	})
}

func TestLunMapCreate(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		sanAPI := san.New(transport, nil)
		client := &sanClient{api: sanAPI}
		err := client.LunMapCreate(&LunMapCreateParams{})
		assert.EqualError(tt, err, transport.err.Error())
	})

	t.Run("WhenSuccessful_ThenNoError", func(tt *testing.T) {
		transport := &mockTransport{
			response: &san.LunMapCreateCreated{},
		}
		sanAPI := san.New(transport, nil)
		client := &sanClient{api: sanAPI}
		err := client.LunMapCreate(&LunMapCreateParams{})
		assert.NoError(tt, err)
	})
}

func TestIGroupGet(t *testing.T) {
	t.Run("WhenNameIsMissing_ThenReturnError", func(tt *testing.T) {
		sanAPI := san.New(&mockTransport{}, nil)
		client := &sanClient{api: sanAPI}
		_, err := client.IGroupGet(&IgroupGetParams{})
		assert.EqualError(tt, err, "missing required parameter 'name' when getting a specific Igroup")
	})

	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		sanAPI := san.New(transport, nil)
		client := &sanClient{api: sanAPI}
		_, err := client.IGroupGet(&IgroupGetParams{Name: nillable.ToPointer("igroup1")})
		assert.EqualError(tt, err, transport.err.Error())
	})

	t.Run("WhenNoRecordsReturned_ThenReturnNotFoundError", func(tt *testing.T) {
		transport := &mockTransport{response: &san.IgroupCollectionGetOK{
			Payload: &models.IgroupResponse{
				IgroupResponseInlineRecords: []*models.Igroup{},
			},
		}}
		sanAPI := san.New(transport, nil)
		client := &sanClient{api: sanAPI}
		_, err := client.IGroupGet(&IgroupGetParams{Name: nillable.ToPointer("igroup1")})
		assert.EqualError(tt, err, "igroup not found")
	})

	t.Run("WhenMultipleRecordsReturned_ThenReturnError", func(tt *testing.T) {
		igroupName := "igroup1"
		transport := &mockTransport{response: &san.IgroupCollectionGetOK{
			Payload: &models.IgroupResponse{
				IgroupResponseInlineRecords: []*models.Igroup{
					{Name: &igroupName},
					{Name: &igroupName},
				},
			},
		}}
		sanAPI := san.New(transport, nil)
		client := &sanClient{api: sanAPI}
		_, err := client.IGroupGet(&IgroupGetParams{Name: nillable.ToPointer(igroupName)})
		assert.EqualError(tt, err, "unexpected response when querying igroup")
	})

	t.Run("WhenSingleRecordReturned_ThenReturnIgroup", func(tt *testing.T) {
		igroupName := "igroup1"
		transport := &mockTransport{response: &san.IgroupCollectionGetOK{
			Payload: &models.IgroupResponse{
				IgroupResponseInlineRecords: []*models.Igroup{
					{Name: &igroupName},
				},
			},
		}}
		sanAPI := san.New(transport, nil)
		client := &sanClient{api: sanAPI}
		igroup, err := client.IGroupGet(&IgroupGetParams{Name: nillable.ToPointer(igroupName)})
		assert.NoError(tt, err)
		assert.NotNil(tt, igroup)
		assert.Equal(tt, igroupName, *igroup.Name)
	})
}
