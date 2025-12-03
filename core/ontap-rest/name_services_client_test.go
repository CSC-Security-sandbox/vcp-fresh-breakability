package ontap_rest

import (
	"errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/name_services"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
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

func TestLdapModifyParamsToONTAPExtended(t *testing.T) {
	t.Run("WhenParamsSetWithServers", func(tt *testing.T) {
		params := &LdapModifyParams{
			SvmUUID:     "svm-uuid-123",
			LdapServers: []*string{nillable.ToPointer("ldap1.example.com"), nillable.ToPointer("ldap2.example.com")},
		}
		otParams := ldapModifyParamsToONTAP(params)
		assert.NotNil(tt, otParams)
		assert.Equal(tt, "svm-uuid-123", otParams.SvmUUID)
		assert.NotNil(tt, otParams.Info)
		assert.Len(tt, otParams.Info.LdapServiceInlineServers, 2)
		assert.Equal(tt, "ldap1.example.com", *otParams.Info.LdapServiceInlineServers[0])
		assert.Equal(tt, "ldap2.example.com", *otParams.Info.LdapServiceInlineServers[1])
	})

	t.Run("WhenParamsSetWithBaseDnAndSchema", func(tt *testing.T) {
		params := &LdapModifyParams{
			SvmUUID: "svm-uuid-789",
			BaseDN:  nillable.ToPointer("dc=example,dc=com"),
			Schema:  nillable.ToPointer("RFC-2307"),
		}
		otParams := ldapModifyParamsToONTAP(params)
		assert.NotNil(tt, otParams)
		assert.Equal(tt, "svm-uuid-789", otParams.SvmUUID)
		assert.NotNil(tt, otParams.Info)
		assert.Equal(tt, "dc=example,dc=com", *otParams.Info.BaseDn)
		assert.Equal(tt, "RFC-2307", *otParams.Info.Schema)
	})

	t.Run("WhenParamsSetWithPreferredAdServers", func(tt *testing.T) {
		params := &LdapModifyParams{
			SvmUUID:                       "svm-uuid-101",
			PreferredServersForLdapClient: []*string{nillable.ToPointer("ad1.example.com"), nillable.ToPointer("ad2.example.com")},
		}
		otParams := ldapModifyParamsToONTAP(params)
		assert.NotNil(tt, otParams)
		assert.Equal(tt, "svm-uuid-101", otParams.SvmUUID)
		assert.NotNil(tt, otParams.Info)
		assert.Len(tt, otParams.Info.LdapServiceInlinePreferredAdServers, 2)
		assert.Equal(tt, "ad1.example.com", *otParams.Info.LdapServiceInlinePreferredAdServers[0])
		assert.Equal(tt, "ad2.example.com", *otParams.Info.LdapServiceInlinePreferredAdServers[1])
	})

	t.Run("WhenParamsSetWithTLSEnabled", func(tt *testing.T) {
		tlsEnabled := true
		params := &LdapModifyParams{
			SvmUUID:    "svm-uuid-505",
			TLSEnabled: &tlsEnabled,
		}
		otParams := ldapModifyParamsToONTAP(params)
		assert.NotNil(tt, otParams)
		assert.Equal(tt, "svm-uuid-505", otParams.SvmUUID)
		assert.NotNil(tt, otParams.Info)
		assert.True(tt, *otParams.Info.UseStartTLS)
	})
}

func TestNameServicesClient_LdapModify(t *testing.T) {
	t.Run("WhenRESTCallFails", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		n := name_services.New(transport, nil)
		client := &nameServicesClient{api: &n}
		err := client.LdapModify(nil)
		assert.EqualError(tt, err, transport.err.Error())
	})
	t.Run("Success", func(t *testing.T) {
		transport := &mockTransport{response: &name_services.LdapModifyOK{}}
		n := name_services.New(transport, nil)
		client := &nameServicesClient{api: &n}
		err := client.LdapModify(&LdapModifyParams{})
		assert.NoError(t, err)
	})
}

func TestNameServicesClient_LdapModifyPreferredAdServers(t *testing.T) {
	t.Run("WhenRESTCallFails", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		n := name_services.New(transport, nil)
		client := &nameServicesClient{api: &n}
		err := client.LdapModifyPreferredAdServers(nil)
		assert.EqualError(tt, err, transport.err.Error())
	})
	t.Run("Success", func(t *testing.T) {
		transport := &mockTransport{response: &name_services.LdapModifyOK{}}
		n := name_services.New(transport, nil)
		client := &nameServicesClient{api: &n}
		err := client.LdapModifyPreferredAdServers(&LdapModifyParams{})
		assert.NoError(t, err)
	})
}

func TestNameServicesClient_NameMappingCollectionGet(t *testing.T) {
	t.Run("WhenRESTCallFails", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("api error")}
		n := name_services.New(transport, nil)
		client := &nameServicesClient{api: &n}
		svmUUID := "test-uuid"
		pattern := "(.+)\\$@EXAMPLE.COM"
		direction := "krb-unix"
		params := &NameMappingCollectionGetParams{
			SvmUUID:   &svmUUID,
			Pattern:   &pattern,
			Direction: &direction,
		}
		result, err := client.NameMappingCollectionGet(params)
		assert.Nil(tt, result)
		assert.EqualError(tt, err, transport.err.Error())
	})

	t.Run("WhenPayloadIsNil_ThenReturnEmptySlice", func(tt *testing.T) {
		transport := &mockTransport{response: &name_services.NameMappingCollectionGetOK{Payload: nil}}
		n := name_services.New(transport, nil)
		client := &nameServicesClient{api: &n}
		svmUUID := "test-uuid"
		pattern := "(.+)\\$@EXAMPLE.COM"
		direction := "krb-unix"
		params := &NameMappingCollectionGetParams{
			SvmUUID:   &svmUUID,
			Pattern:   &pattern,
			Direction: &direction,
		}
		result, err := client.NameMappingCollectionGet(params)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Len(tt, result, 0)
	})

	t.Run("WhenRecordsIsNil_ThenReturnEmptySlice", func(tt *testing.T) {
		resp := &name_services.NameMappingCollectionGetOK{
			Payload: &models.NameMappingResponse{
				NameMappingResponseInlineRecords: nil,
			},
		}
		transport := &mockTransport{response: resp}
		n := name_services.New(transport, nil)
		client := &nameServicesClient{api: &n}
		svmUUID := "test-uuid"
		pattern := "(.+)\\$@EXAMPLE.COM"
		direction := "krb-unix"
		params := &NameMappingCollectionGetParams{
			SvmUUID:   &svmUUID,
			Pattern:   &pattern,
			Direction: &direction,
		}
		result, err := client.NameMappingCollectionGet(params)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Len(tt, result, 0)
	})

	t.Run("WhenMappingsExist_ThenReturnMappings", func(tt *testing.T) {
		pattern := "(.+)\\$@EXAMPLE.COM"
		replacement := "root"
		direction := "krb-unix"
		index := int64(1)
		resp := &name_services.NameMappingCollectionGetOK{
			Payload: &models.NameMappingResponse{
				NameMappingResponseInlineRecords: []*models.NameMapping{
					{
						Pattern:     &pattern,
						Replacement: &replacement,
						Direction:   &direction,
						Index:       &index,
					},
				},
			},
		}
		transport := &mockTransport{response: resp}
		n := name_services.New(transport, nil)
		client := &nameServicesClient{api: &n}
		svmUUID := "test-uuid"
		params := &NameMappingCollectionGetParams{
			SvmUUID:   &svmUUID,
			Pattern:   &pattern,
			Direction: &direction,
		}
		result, err := client.NameMappingCollectionGet(params)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Len(tt, result, 1)
		assert.Equal(tt, pattern, *result[0].Pattern)
		assert.Equal(tt, replacement, *result[0].Replacement)
		assert.Equal(tt, direction, *result[0].Direction)
		assert.Equal(tt, index, *result[0].Index)
	})

	t.Run("WhenMultipleMappingsExist_ThenReturnAllMappings", func(tt *testing.T) {
		pattern1 := "(.+)\\$@EXAMPLE.COM"
		pattern2 := "(.+)\\$@TEST.COM"
		replacement := "root"
		direction := "krb-unix"
		index1 := int64(1)
		index2 := int64(2)
		resp := &name_services.NameMappingCollectionGetOK{
			Payload: &models.NameMappingResponse{
				NameMappingResponseInlineRecords: []*models.NameMapping{
					{
						Pattern:     &pattern1,
						Replacement: &replacement,
						Direction:   &direction,
						Index:       &index1,
					},
					{
						Pattern:     &pattern2,
						Replacement: &replacement,
						Direction:   &direction,
						Index:       &index2,
					},
				},
			},
		}
		transport := &mockTransport{response: resp}
		n := name_services.New(transport, nil)
		client := &nameServicesClient{api: &n}
		svmUUID := "test-uuid"
		params := &NameMappingCollectionGetParams{
			SvmUUID:   &svmUUID,
			Direction: &direction,
		}
		result, err := client.NameMappingCollectionGet(params)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Len(tt, result, 2)
		assert.Equal(tt, pattern1, *result[0].Pattern)
		assert.Equal(tt, pattern2, *result[1].Pattern)
	})
}

func TestNameServicesClient_NameMappingCreate(t *testing.T) {
	t.Run("WhenRESTCallFails", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("api error")}
		n := name_services.New(transport, nil)
		client := &nameServicesClient{api: &n}
		svmUUID := "test-uuid"
		pattern := "(.+)\\$@EXAMPLE.COM"
		replacement := "root"
		direction := "krb-unix"
		index := int64(1)
		params := &NameMappingCreateParams{
			SvmUUID:     &svmUUID,
			Pattern:     &pattern,
			Replacement: &replacement,
			Direction:   &direction,
			Index:       index,
		}
		err := client.NameMappingCreate(params)
		assert.EqualError(tt, err, transport.err.Error())
	})

	t.Run("Success", func(t *testing.T) {
		transport := &mockTransport{response: &name_services.NameMappingCreateCreated{}}
		n := name_services.New(transport, nil)
		client := &nameServicesClient{api: &n}
		svmUUID := "test-uuid"
		pattern := "(.+)\\$@EXAMPLE.COM"
		replacement := "root"
		direction := "krb-unix"
		index := int64(1)
		params := &NameMappingCreateParams{
			SvmUUID:     &svmUUID,
			Pattern:     &pattern,
			Replacement: &replacement,
			Direction:   &direction,
			Index:       index,
		}
		err := client.NameMappingCreate(params)
		assert.NoError(t, err)
	})
}
