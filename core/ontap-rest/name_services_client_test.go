package ontap_rest

import (
	"errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/name_services"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNameServicesClient_DnsCreate(t *testing.T) {
	t.Run("WhenRESTCallFails", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		n := name_services.New(transport, nil)
		client := &nameServicesClient{api: &n}
		response, err := client.DnsCreate(nil)
		assert.EqualError(tt, err, transport.err.Error())
		assert.Nil(tt, response)
	})
	t.Run("Success", func(t *testing.T) {
		expected := &models.DNSResponse{}
		transport := &mockTransport{response: &name_services.DNSCreateCreated{
			Payload: expected,
		}}
		n := name_services.New(transport, nil)
		client := &nameServicesClient{api: &n}
		resp, err := client.DnsCreate(&DNSCreateParams{})
		assert.NoError(t, err)
		assert.Equal(t, expected, resp)
	})
	t.Run("NilPayload", func(t *testing.T) {
		transport := &mockTransport{response: &name_services.DNSCreateCreated{
			Payload: nil,
		}}
		n := name_services.New(transport, nil)
		client := &nameServicesClient{api: &n}
		resp, err := client.DnsCreate(&DNSCreateParams{})
		assert.Error(t, err)
		assert.Nil(t, resp)
	})
}
