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

func TestNameServicesClient_LdapCreate(t *testing.T) {
	t.Run("WhenRESTCallFails", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		n := name_services.New(transport, nil)
		client := &nameServicesClient{api: &n}
		response, err := client.LdapCreate(nil)
		assert.EqualError(tt, err, transport.err.Error())
		assert.Nil(tt, response)
	})
	t.Run("Success", func(t *testing.T) {
		expected := &models.LdapServiceResponse{}
		transport := &mockTransport{response: &name_services.LdapCreateCreated{
			Payload: expected,
		}}
		n := name_services.New(transport, nil)
		client := &nameServicesClient{api: &n}
		resp, err := client.LdapCreate(&LdapCreateParams{})
		assert.NoError(t, err)
		assert.Equal(t, expected, resp)
	})
	t.Run("NilPayload", func(t *testing.T) {
		transport := &mockTransport{response: &name_services.LdapCreateCreated{
			Payload: nil,
		}}
		n := name_services.New(transport, nil)
		client := &nameServicesClient{api: &n}
		resp, err := client.LdapCreate(&LdapCreateParams{})
		assert.Error(t, err)
		assert.Nil(t, resp)
	})
}

func TestNameServicesClient_LdapGet(t *testing.T) {
	t.Run("WhenRESTCallFails", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		n := name_services.New(transport, nil)
		client := &nameServicesClient{api: &n}
		response, err := client.LdapGet(nil)
		assert.EqualError(tt, err, transport.err.Error())
		assert.Nil(tt, response)
	})
	t.Run("Success", func(t *testing.T) {
		expected := &LdapService{models.LdapService{}}
		transport := &mockTransport{response: &name_services.LdapGetOK{
			Payload: &expected.LdapService,
		}}
		n := name_services.New(transport, nil)
		client := &nameServicesClient{api: &n}
		resp, err := client.LdapGet(&LdapGetParams{})
		assert.NoError(t, err)
		assert.Equal(t, expected, resp)
	})
}

func TestNameServicesClient_LdapSchemaCreate(t *testing.T) {
	t.Run("WhenRESTCallFails", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		n := name_services.New(transport, nil)
		client := &nameServicesClient{api: &n}
		err := client.LdapSchemaCreate(nil)
		assert.EqualError(tt, err, transport.err.Error())
	})
	t.Run("Success", func(t *testing.T) {
		transport := &mockTransport{response: &name_services.LdapSchemaCreateCreated{}}
		n := name_services.New(transport, nil)
		client := &nameServicesClient{api: &n}
		err := client.LdapSchemaCreate(&LdapSchemaCreateParams{})
		assert.NoError(t, err)
	})
}

func TestNameServicesClient_LdapSchemaModify(t *testing.T) {
	t.Run("WhenRESTCallFails", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		n := name_services.New(transport, nil)
		client := &nameServicesClient{api: &n}
		err := client.LdapSchemaModify(nil)
		assert.EqualError(tt, err, transport.err.Error())
	})
	t.Run("Success", func(t *testing.T) {
		transport := &mockTransport{response: &name_services.LdapSchemaModifyOK{}}
		n := name_services.New(transport, nil)
		client := &nameServicesClient{api: &n}
		err := client.LdapSchemaModify(&LdapSchemaModifyParams{})
		assert.NoError(t, err)
	})
}
