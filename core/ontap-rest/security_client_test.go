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
func TestEnableAutoVolOfflineCronForGCPKMS(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("rest call failed")}
		securityAPI := security.New(transport, nil)
		client := &securityClient{api: &securityAPI}

		err := client.EnableAutoVolOfflineCronForGCPKMS()
		assert.EqualError(tt, err, "rest call failed")
	})

	t.Run("WhenResponseIsNil_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{response: nil}
		securityAPI := security.New(transport, nil)
		client := &securityClient{api: &securityAPI}
		defer func() {
			if r := recover(); r == nil {
				tt.Errorf("Expected panic when response.Payload is nil")
			}
		}()
		err := client.EnableAutoVolOfflineCronForGCPKMS()
		assert.Error(tt, err)
	})
	t.Run("WhenResponseIsSuccessful_ThenReturnNoError", func(tt *testing.T) {
		transport := &mockTransport{response: &security.KeyManagerConfigModifyOK{}}
		securityAPI := security.New(transport, nil)
		client := &securityClient{api: &securityAPI}

		err := client.EnableAutoVolOfflineCronForGCPKMS()
		assert.NoError(tt, err)
	})
}

func TestRoleCreate(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("rest call failed")}
		securityAPI := security.New(transport, nil)
		client := &securityClient{api: &securityAPI}
		response, err := client.RoleCreate(&RoleCreateParams{})
		assert.EqualError(tt, err, transport.err.Error())
		assert.Equal(tt, "", response)
	})

	t.Run("WhenResponseHasLocation_ThenReturnLocation", func(tt *testing.T) {
		expectedLocation := "/api/security/roles/test-role"
		transport := &mockTransport{response: &security.RoleCreateCreated{
			Location: expectedLocation,
		}}
		securityAPI := security.New(transport, nil)
		client := &securityClient{api: &securityAPI}
		response, err := client.RoleCreate(&RoleCreateParams{})
		assert.NoError(tt, err)
		assert.Equal(tt, expectedLocation, response)
	})

	t.Run("WhenResponseHasEmptyLocation_ThenReturnEmptyString", func(tt *testing.T) {
		transport := &mockTransport{response: &security.RoleCreateCreated{
			Location: "",
		}}
		securityAPI := security.New(transport, nil)
		client := &securityClient{api: &securityAPI}
		response, err := client.RoleCreate(&RoleCreateParams{})
		assert.NoError(tt, err)
		assert.Equal(tt, "", response)
	})
}

func TestRoleGet(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("rest call failed")}
		securityAPI := security.New(transport, nil)
		client := &securityClient{api: &securityAPI}
		response, err := client.RoleGet(&RoleGetParams{})
		assert.EqualError(tt, err, transport.err.Error())
		assert.Nil(tt, response)
	})

	t.Run("WhenResponseIsNil_ThenReturnUnhandledResponseError", func(tt *testing.T) {
		transport := &mockTransport{response: &security.RoleGetOK{}}
		securityAPI := security.New(transport, nil)
		client := &securityClient{api: &securityAPI}
		response, err := client.RoleGet(&RoleGetParams{})
		assert.EqualError(tt, err, "unexpected response from RoleGet")
		assert.Nil(tt, response)
	})

	t.Run("WhenResponseHasPayload_ThenReturnRole", func(tt *testing.T) {
		roleName := "test-role"
		role := &models.Role{
			Name: &roleName,
		}
		transport := &mockTransport{response: &security.RoleGetOK{
			Payload: role,
		}}
		securityAPI := security.New(transport, nil)
		client := &securityClient{api: &securityAPI}
		response, err := client.RoleGet(&RoleGetParams{})
		assert.NoError(tt, err)
		assert.NotNil(tt, response)
		assert.Equal(tt, role, &response.Role)
	})
}

func TestRolePrivilegeModify(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("rest call failed")}
		securityAPI := security.New(transport, nil)
		client := &securityClient{api: &securityAPI}
		err := client.RolePrivilegeModify(&RolePrivilegeModifyParams{})
		assert.EqualError(tt, err, transport.err.Error())
	})

	t.Run("WhenResponseIsSuccessful_ThenReturnNoError", func(tt *testing.T) {
		transport := &mockTransport{response: &security.RolePrivilegeModifyOK{}}
		securityAPI := security.New(transport, nil)
		client := &securityClient{api: &securityAPI}
		err := client.RolePrivilegeModify(&RolePrivilegeModifyParams{})
		assert.NoError(tt, err)
	})

	t.Run("WhenResponseIsAccepted_ThenReturnNoError", func(tt *testing.T) {
		transport := &mockTransport{response: &security.RolePrivilegeModifyOK{}}
		securityAPI := security.New(transport, nil)
		client := &securityClient{api: &securityAPI}
		err := client.RolePrivilegeModify(&RolePrivilegeModifyParams{})
		assert.NoError(tt, err)
	})
}

func TestRoleCollectionGet(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("rest call failed")}
		securityAPI := security.New(transport, nil)
		client := &securityClient{api: &securityAPI}
		response, err := client.RoleCollectionGet(&RoleCollectionGetParams{})
		assert.EqualError(tt, err, transport.err.Error())
		assert.Nil(tt, response)
	})

	t.Run("WhenResponseIsNil_ThenReturnUnhandledResponseError", func(tt *testing.T) {
		transport := &mockTransport{response: &security.RoleCollectionGetOK{}}
		securityAPI := security.New(transport, nil)
		client := &securityClient{api: &securityAPI}
		response, err := client.RoleCollectionGet(&RoleCollectionGetParams{})
		assert.EqualError(tt, err, "unexpected response from RoleCollectionGet")
		assert.Nil(tt, response)
	})

	t.Run("WhenResponseHasPayload_ThenReturnRoleCollectionGetResponse", func(tt *testing.T) {
		roleName := "test-role"
		role := &models.Role{
			Name: &roleName,
		}
		transport := &mockTransport{response: &security.RoleCollectionGetOK{
			Payload: &models.RoleResponse{
				NumRecords: nillable.ToPointer(int64(1)),
				RoleResponseInlineRecords: []*models.Role{
					role,
				},
			},
		}}
		securityAPI := security.New(transport, nil)
		client := &securityClient{api: &securityAPI}
		response, err := client.RoleCollectionGet(&RoleCollectionGetParams{})
		assert.NoError(tt, err)
		assert.NotNil(tt, response)
		assert.Equal(tt, &security.RoleCollectionGetOK{
			Payload: &models.RoleResponse{
				NumRecords: nillable.ToPointer(int64(1)),
				RoleResponseInlineRecords: []*models.Role{
					role,
				},
			},
		}, response.RoleCollectionGetOK)
	})
}
