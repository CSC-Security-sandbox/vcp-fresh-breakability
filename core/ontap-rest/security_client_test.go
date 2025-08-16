package ontap_rest

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/security"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestGcpKmsCreate(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		securityAPI := security.New(transport, nil)
		client := &securityClient{api: &securityAPI}
		response, err := client.GcpKmsCreate(&GcpKmsCreateParams{})
		assert.EqualError(tt, err, transport.err.Error())
		assert.Nil(tt, response)
	})

	t.Run("WhenResponseIsNil_ThenReturnUnhandledResponseError", func(tt *testing.T) {
		transport := &mockTransport{response: &security.GcpKmsCreateCreated{}}
		securityAPI := security.New(transport, nil)
		client := &securityClient{api: &securityAPI}
		response, err := client.GcpKmsCreate(&GcpKmsCreateParams{})
		assert.EqualError(tt, err, "unexpected response from GcpKmsCreate")
		assert.Nil(tt, response)
	})

	t.Run("WhenResponseHasRecords_ThenReturnGcpKmsList", func(tt *testing.T) {
		gcpKms := &models.GcpKms{}
		transport := &mockTransport{response: &security.GcpKmsCreateCreated{
			Payload: &models.GcpKmsResponse{
				NumRecords: nillable.ToPointer(int64(1)),
				GcpKmsResponseInlineRecords: []*models.GcpKms{
					gcpKms,
				},
			},
		}}
		securityAPI := security.New(transport, nil)
		client := &securityClient{api: &securityAPI}
		response, err := client.GcpKmsCreate(&GcpKmsCreateParams{})
		assert.NoError(tt, err)
		assert.NotNil(tt, response)
		assert.Equal(tt, 1, len(response))
		assert.Equal(tt, gcpKms, &response[0].GcpKms)
	})
}
func TestGcpKmsGet(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("rest call failed")}
		securityAPI := security.New(transport, nil)
		client := &securityClient{api: &securityAPI}
		response, err := client.GcpKmsGet(&GcpKmsGetParams{})
		assert.EqualError(tt, err, transport.err.Error())
		assert.Nil(tt, response)
	})

	t.Run("WhenResponseIsNil_ThenPanicOrReturnError", func(tt *testing.T) {
		transport := &mockTransport{response: nil}
		securityAPI := security.New(transport, nil)
		client := &securityClient{api: &securityAPI}
		defer func() {
			if r := recover(); r == nil {
				tt.Errorf("Expected panic when response.Payload is nil")
			}
		}()
		_, err := client.GcpKmsGet(&GcpKmsGetParams{})
		assert.Error(tt, err)
	})

	t.Run("WhenResponseHasPayload_ThenReturnGcpKms", func(tt *testing.T) {
		gcpKms := &models.GcpKms{}
		transport := &mockTransport{response: &security.GcpKmsGetOK{
			Payload: gcpKms,
		}}
		securityAPI := security.New(transport, nil)
		client := &securityClient{api: &securityAPI}
		response, err := client.GcpKmsGet(&GcpKmsGetParams{})
		assert.NoError(tt, err)
		assert.NotNil(tt, response)
		assert.Equal(tt, gcpKms, &response.GcpKms)
	})
}
func TestSecurityLogForwardingCreate(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		securityAPI := security.New(transport, nil)
		client := &securityClient{api: &securityAPI}
		response, err := client.SecurityLogForwardingCreate(&SecurityLogForwardingCreateParams{})
		assert.EqualError(tt, err, transport.err.Error())
		assert.Nil(tt, response)
	})

	t.Run("WhenResponseIsNil_ThenReturnUnhandledResponseError", func(tt *testing.T) {
		transport := &mockTransport{response: &security.SecurityLogForwardingCreateAccepted{}}
		securityAPI := security.New(transport, nil)
		client := &securityClient{api: &securityAPI}
		response, err := client.SecurityLogForwardingCreate(&SecurityLogForwardingCreateParams{})
		assert.EqualError(tt, err, "unexpected response from SecurityLogForwardingCreate")
		assert.Nil(tt, response)
	})

	t.Run("WhenResponseHasRecords_ThenReturnGcpAuditLogForwardList", func(tt *testing.T) {
		address := "test-address"
		gcpAuditLogForward := &models.SecurityAuditLogForward{
			Address: &address,
		}
		transport := &mockTransport{response: &security.SecurityLogForwardingCreateAccepted{
			Payload: &models.SecurityAuditLogForwardResponse{
				NumRecords: nillable.ToPointer(int64(1)),
				SecurityAuditLogForwardResponseInlineRecords: []*models.SecurityAuditLogForward{
					gcpAuditLogForward,
				},
			},
		}}
		securityAPI := security.New(transport, nil)
		client := &securityClient{api: &securityAPI}
		response, err := client.SecurityLogForwardingCreate(&SecurityLogForwardingCreateParams{})
		assert.NoError(tt, err)
		assert.NotNil(tt, response)
		assert.Equal(tt, 1, len(response))
		assert.Equal(tt, &address, response[0].Address)
	})
}
func TestSecurityLogForwardingGet(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("rest call failed")}
		securityAPI := security.New(transport, nil)
		client := &securityClient{api: &securityAPI}
		response, err := client.SecurityLogForwardingGet(&SecurityLogForwardingGetParams{})
		assert.EqualError(tt, err, transport.err.Error())
		assert.Nil(tt, response)
	})

	t.Run("WhenResponseIsNil_ThenPanicOrReturnError", func(tt *testing.T) {
		transport := &mockTransport{response: nil}
		securityAPI := security.New(transport, nil)
		client := &securityClient{api: &securityAPI}
		defer func() {
			if r := recover(); r == nil {
				tt.Errorf("Expected panic when response.Payload is nil")
			}
		}()
		_, err := client.SecurityLogForwardingGet(&SecurityLogForwardingGetParams{})
		assert.Error(tt, err)
	})

	t.Run("WhenResponseHasPayload_ThenReturnGcpAuditLogForward", func(tt *testing.T) {
		address := "test-address"
		gcpAuditLogForward := &models.SecurityAuditLogForward{
			Address: &address,
		}
		transport := &mockTransport{response: &security.SecurityLogForwardingGetOK{
			Payload: gcpAuditLogForward,
		}}
		securityAPI := security.New(transport, nil)
		client := &securityClient{api: &securityAPI}
		response, err := client.SecurityLogForwardingGet(&SecurityLogForwardingGetParams{})
		assert.NoError(tt, err)
		assert.NotNil(tt, response)
		assert.Equal(tt, gcpAuditLogForward.Address, response.Address)
	})
}

func TestSecurityAuditUpdate(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		securityAPI := security.New(transport, nil)
		client := &securityClient{api: &securityAPI}
		response, err := client.SecurityAuditUpdate(&SecurityAuditUpdateParams{})
		assert.EqualError(tt, err, transport.err.Error())
		assert.Nil(tt, response)
	})

	t.Run("WhenResponseIsNil_ThenReturnUnhandledResponseError", func(tt *testing.T) {
		transport := &mockTransport{response: &security.SecurityAuditModifyOK{}}
		securityAPI := security.New(transport, nil)
		client := &securityClient{api: &securityAPI}
		response, err := client.SecurityAuditUpdate(&SecurityAuditUpdateParams{})
		assert.EqualError(tt, err, "unexpected response from SecurityAuditUpdate")
		assert.Nil(tt, response)
	})

	t.Run("WhenResponseHasRecords_ThenReturnGcpAuditLogForwardList", func(tt *testing.T) {
		transport := &mockTransport{response: &security.SecurityAuditModifyOK{
			Payload: &models.SecurityAudit{
				Cli:    nillable.ToPointer(true),
				HTTP:   nillable.ToPointer(true),
				Ontapi: nillable.ToPointer(true),
			},
		}}
		securityAPI := security.New(transport, nil)
		client := &securityClient{api: &securityAPI}
		response, err := client.SecurityAuditUpdate(&SecurityAuditUpdateParams{})
		assert.NoError(tt, err)
		assert.NotNil(tt, response)
		assert.True(tt, nillable.GetBool(response.HTTP, false))
		assert.True(tt, nillable.GetBool(response.Cli, false))
		assert.True(tt, nillable.GetBool(response.Ontapi, false))
	})
}
func TestSecurityAuditGet(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("rest call failed")}
		securityAPI := security.New(transport, nil)
		client := &securityClient{api: &securityAPI}
		response, err := client.SecurityAuditGet()
		assert.EqualError(tt, err, transport.err.Error())
		assert.Nil(tt, response)
	})

	t.Run("WhenResponseIsNil_ThenPanicOrReturnError", func(tt *testing.T) {
		transport := &mockTransport{response: nil}
		securityAPI := security.New(transport, nil)
		client := &securityClient{api: &securityAPI}
		defer func() {
			if r := recover(); r == nil {
				tt.Errorf("Expected panic when response.Payload is nil")
			}
		}()
		_, err := client.SecurityAuditGet()
		assert.Error(tt, err)
	})

	t.Run("WhenResponseHasPayload_ThenReturnGcpAuditLogForward", func(tt *testing.T) {
		address := "test-address"
		gcpAuditLogForward := &models.SecurityAuditLogForward{
			Address: &address,
		}
		transport := &mockTransport{response: &security.SecurityLogForwardingGetOK{
			Payload: gcpAuditLogForward,
		}}
		securityAPI := security.New(transport, nil)
		client := &securityClient{api: &securityAPI}
		response, err := client.SecurityLogForwardingGet(&SecurityLogForwardingGetParams{})
		assert.NoError(tt, err)
		assert.NotNil(tt, response)
		assert.Equal(tt, gcpAuditLogForward.Address, response.Address)
	})
}

func TestGcpKmsDelete(t *testing.T) {
	t.Run("WhenGcpKmsDeleteReturnsError", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("rest call failed")}
		securityAPI := security.New(transport, nil)
		client := &securityClient{api: &securityAPI}

		err := client.GcpKmsDelete(&GcpKmsDeleteParams{})
		assert.Error(tt, err)
		assert.Errorf(tt, err, transport.err.Error())
	})
	t.Run("WhenGcpKmsDeleteReturnsNilResponse", func(tt *testing.T) {
		transport := &mockTransport{response: &security.GcpKmsDeleteAccepted{}}
		securityAPI := security.New(transport, nil)
		client := &securityClient{api: &securityAPI}

		err := client.GcpKmsDelete(&GcpKmsDeleteParams{})
		assert.Error(tt, err)
		assert.Errorf(tt, err, "ontap-rest response for GcpKmsDelete is nil")
	})
	t.Run("WhenGcpKmsDeleteReturnsWithoutError", func(tt *testing.T) {
		transport := &mockTransport{response: &security.GcpKmsDeleteOK{}}
		securityAPI := security.New(transport, nil)
		client := &securityClient{api: &securityAPI}

		err := client.GcpKmsDelete(&GcpKmsDeleteParams{})
		assert.NoError(tt, err)
		assert.Nil(tt, err)
	})
}

func TestGcpKmsModify(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("rest call failed")}
		securityAPI := security.New(transport, nil)
		client := &securityClient{api: &securityAPI}
		gcpKms, job, err := client.GcpKmsModify(&GcpKmsModifyParams{})
		assert.EqualError(tt, err, transport.err.Error())
		assert.Nil(tt, gcpKms)
		assert.Nil(tt, job)
	})

	t.Run("WhenResponseOKIsNotNil_ThenReturnEmptyGcpKms", func(tt *testing.T) {
		transport := &mockTransport{response: &security.GcpKmsModifyOK{}}
		securityAPI := security.New(transport, nil)
		client := &securityClient{api: &securityAPI}
		gcpKms, job, err := client.GcpKmsModify(&GcpKmsModifyParams{})
		assert.NoError(tt, err)
		assert.NotNil(tt, gcpKms)
		assert.Nil(tt, job)
	})
}
