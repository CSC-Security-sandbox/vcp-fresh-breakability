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
