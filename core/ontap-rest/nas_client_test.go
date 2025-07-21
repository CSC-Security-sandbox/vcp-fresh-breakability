package ontap_rest

import (
	"errors"
	"testing"

	"github.com/go-openapi/runtime"
	"github.com/stretchr/testify/assert"
	nas "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/n_a_s"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
)

// mockNASClient implements the nas.ClientService interface for testing
type mockNASClient struct {
	err      error
	response interface{}
}

// Implement only the methods we need for testing
func (m *mockNASClient) ExportPolicyCreate(params *nas.ExportPolicyCreateParams, _ runtime.ClientAuthInfoWriter, opts ...nas.ClientOption) (*nas.ExportPolicyCreateCreated, error) {
	if m.err != nil {
		return nil, m.err
	}
	if resp, ok := m.response.(*nas.ExportPolicyCreateCreated); ok {
		return resp, nil
	}
	return &nas.ExportPolicyCreateCreated{}, nil
}

func (m *mockNASClient) ExportPolicyCollectionGet(params *nas.ExportPolicyCollectionGetParams, _ runtime.ClientAuthInfoWriter, opts ...nas.ClientOption) (*nas.ExportPolicyCollectionGetOK, error) {
	if m.err != nil {
		return nil, m.err
	}
	if resp, ok := m.response.(*nas.ExportPolicyCollectionGetOK); ok {
		return resp, nil
	}
	return &nas.ExportPolicyCollectionGetOK{}, nil
}

func (m *mockNASClient) ExportPolicyModify(params *nas.ExportPolicyModifyParams, _ runtime.ClientAuthInfoWriter, opts ...nas.ClientOption) (*nas.ExportPolicyModifyOK, error) {
	if m.err != nil {
		return nil, m.err
	}
	return &nas.ExportPolicyModifyOK{}, nil
}

func (m *mockNASClient) ExportPolicyDeleteCollection(params *nas.ExportPolicyDeleteCollectionParams, _ runtime.ClientAuthInfoWriter, opts ...nas.ClientOption) (*nas.ExportPolicyDeleteCollectionOK, error) {
	if m.err != nil {
		return nil, m.err
	}
	return &nas.ExportPolicyDeleteCollectionOK{}, nil
}

func (m *mockNASClient) NfsGet(params *nas.NfsGetParams, _ runtime.ClientAuthInfoWriter, opts ...nas.ClientOption) (*nas.NfsGetOK, error) {
	if m.err != nil {
		return nil, m.err
	}
	if resp, ok := m.response.(*nas.NfsGetOK); ok {
		return resp, nil
	}
	return &nas.NfsGetOK{}, nil
}

func (m *mockNASClient) NfsCreate(params *nas.NfsCreateParams, _ runtime.ClientAuthInfoWriter, opts ...nas.ClientOption) (*nas.NfsCreateCreated, error) {
	if m.err != nil {
		return nil, m.err
	}
	return &nas.NfsCreateCreated{}, nil
}

func (m *mockNASClient) NfsModify(params *nas.NfsModifyParams, _ runtime.ClientAuthInfoWriter, opts ...nas.ClientOption) (*nas.NfsModifyOK, error) {
	if m.err != nil {
		return nil, m.err
	}
	return &nas.NfsModifyOK{}, nil
}

func (m *mockNASClient) CifsServiceCollectionGet(params *nas.CifsServiceCollectionGetParams, _ runtime.ClientAuthInfoWriter, opts ...nas.ClientOption) (*nas.CifsServiceCollectionGetOK, error) {
	if m.err != nil {
		return nil, m.err
	}
	if resp, ok := m.response.(*nas.CifsServiceCollectionGetOK); ok {
		return resp, nil
	}
	return &nas.CifsServiceCollectionGetOK{}, nil
}

func (m *mockNASClient) CifsServiceCreate(params *nas.CifsServiceCreateParams, _ runtime.ClientAuthInfoWriter, opts ...nas.ClientOption) (*nas.CifsServiceCreateCreated, *nas.CifsServiceCreateAccepted, error) {
	if m.err != nil {
		return nil, nil, m.err
	}
	return &nas.CifsServiceCreateCreated{}, nil, nil
}

func (m *mockNASClient) CifsServiceModify(params *nas.CifsServiceModifyParams, _ runtime.ClientAuthInfoWriter, opts ...nas.ClientOption) (*nas.CifsServiceModifyOK, *nas.CifsServiceModifyAccepted, error) {
	if m.err != nil {
		return nil, nil, m.err
	}
	return &nas.CifsServiceModifyOK{}, nil, nil
}

// Implement the remaining interface methods with empty implementations
func (m *mockNASClient) AuditCreate(params *nas.AuditCreateParams, authInfo runtime.ClientAuthInfoWriter, opts ...nas.ClientOption) (*nas.AuditCreateCreated, *nas.AuditCreateAccepted, error) {
	return nil, nil, nil
}

func (m *mockNASClient) AuditDelete(params *nas.AuditDeleteParams, authInfo runtime.ClientAuthInfoWriter, opts ...nas.ClientOption) (*nas.AuditDeleteOK, *nas.AuditDeleteAccepted, error) {
	return nil, nil, nil
}

func (m *mockNASClient) AuditLogRedirectCreate(params *nas.AuditLogRedirectCreateParams, authInfo runtime.ClientAuthInfoWriter, opts ...nas.ClientOption) (*nas.AuditLogRedirectCreateCreated, error) {
	return nil, nil
}

func (m *mockNASClient) AuditLogRedirectDelete(params *nas.AuditLogRedirectDeleteParams, authInfo runtime.ClientAuthInfoWriter, opts ...nas.ClientOption) (*nas.AuditLogRedirectDeleteOK, error) {
	return nil, nil
}

func (m *mockNASClient) AuditLogRedirectGet(params *nas.AuditLogRedirectGetParams, authInfo runtime.ClientAuthInfoWriter, opts ...nas.ClientOption) (*nas.AuditLogRedirectGetOK, error) {
	return nil, nil
}

func (m *mockNASClient) AuditModify(params *nas.AuditModifyParams, authInfo runtime.ClientAuthInfoWriter, opts ...nas.ClientOption) (*nas.AuditModifyOK, *nas.AuditModifyAccepted, error) {
	return nil, nil, nil
}

func (m *mockNASClient) CifsDomainGet(params *nas.CifsDomainGetParams, authInfo runtime.ClientAuthInfoWriter, opts ...nas.ClientOption) (*nas.CifsDomainGetOK, error) {
	return nil, nil
}

func (m *mockNASClient) CifsDomainModify(params *nas.CifsDomainModifyParams, authInfo runtime.ClientAuthInfoWriter, opts ...nas.ClientOption) (*nas.CifsDomainModifyOK, error) {
	return nil, nil
}

func (m *mockNASClient) CifsDomainPreferredDcCreate(params *nas.CifsDomainPreferredDcCreateParams, authInfo runtime.ClientAuthInfoWriter, opts ...nas.ClientOption) (*nas.CifsDomainPreferredDcCreateCreated, error) {
	return nil, nil
}

func (m *mockNASClient) CifsDomainPreferredDcDelete(params *nas.CifsDomainPreferredDcDeleteParams, authInfo runtime.ClientAuthInfoWriter, opts ...nas.ClientOption) (*nas.CifsDomainPreferredDcDeleteOK, error) {
	return nil, nil
}

func (m *mockNASClient) CifsServiceDelete(params *nas.CifsServiceDeleteParams, authInfo runtime.ClientAuthInfoWriter, opts ...nas.ClientOption) (*nas.CifsServiceDeleteOK, *nas.CifsServiceDeleteAccepted, error) {
	return nil, nil, nil
}

func (m *mockNASClient) CifsServiceModifyCollection(params *nas.CifsServiceModifyCollectionParams, authInfo runtime.ClientAuthInfoWriter, opts ...nas.ClientOption) (*nas.CifsServiceModifyCollectionOK, *nas.CifsServiceModifyCollectionAccepted, error) {
	return nil, nil, nil
}

func (m *mockNASClient) CifsSessionCollectionGet(params *nas.CifsSessionCollectionGetParams, authInfo runtime.ClientAuthInfoWriter, opts ...nas.ClientOption) (*nas.CifsSessionCollectionGetOK, error) {
	return nil, nil
}

func (m *mockNASClient) CifsShareACLDelete(params *nas.CifsShareACLDeleteParams, authInfo runtime.ClientAuthInfoWriter, opts ...nas.ClientOption) (*nas.CifsShareACLDeleteOK, error) {
	return nil, nil
}

func (m *mockNASClient) CifsShareCollectionGet(params *nas.CifsShareCollectionGetParams, authInfo runtime.ClientAuthInfoWriter, opts ...nas.ClientOption) (*nas.CifsShareCollectionGetOK, error) {
	return nil, nil
}

func (m *mockNASClient) CifsShareCreate(params *nas.CifsShareCreateParams, authInfo runtime.ClientAuthInfoWriter, opts ...nas.ClientOption) (*nas.CifsShareCreateCreated, error) {
	return nil, nil
}

func (m *mockNASClient) CifsShareDelete(params *nas.CifsShareDeleteParams, authInfo runtime.ClientAuthInfoWriter, opts ...nas.ClientOption) (*nas.CifsShareDeleteOK, error) {
	return nil, nil
}

func (m *mockNASClient) CifsShareGet(params *nas.CifsShareGetParams, authInfo runtime.ClientAuthInfoWriter, opts ...nas.ClientOption) (*nas.CifsShareGetOK, error) {
	return nil, nil
}

func (m *mockNASClient) CifsShareModify(params *nas.CifsShareModifyParams, authInfo runtime.ClientAuthInfoWriter, opts ...nas.ClientOption) (*nas.CifsShareModifyOK, error) {
	return nil, nil
}

func (m *mockNASClient) ClientLockDeleteCollection(params *nas.ClientLockDeleteCollectionParams, authInfo runtime.ClientAuthInfoWriter, opts ...nas.ClientOption) (*nas.ClientLockDeleteCollectionOK, error) {
	return nil, nil
}

func (m *mockNASClient) ExportPolicyGet(params *nas.ExportPolicyGetParams, authInfo runtime.ClientAuthInfoWriter, opts ...nas.ClientOption) (*nas.ExportPolicyGetOK, error) {
	return nil, nil
}

func (m *mockNASClient) KerberosInterfaceCollectionGet(params *nas.KerberosInterfaceCollectionGetParams, authInfo runtime.ClientAuthInfoWriter, opts ...nas.ClientOption) (*nas.KerberosInterfaceCollectionGetOK, error) {
	return nil, nil
}

func (m *mockNASClient) KerberosInterfaceModify(params *nas.KerberosInterfaceModifyParams, authInfo runtime.ClientAuthInfoWriter, opts ...nas.ClientOption) (*nas.KerberosInterfaceModifyOK, error) {
	return nil, nil
}

func (m *mockNASClient) KerberosRealmCreate(params *nas.KerberosRealmCreateParams, authInfo runtime.ClientAuthInfoWriter, opts ...nas.ClientOption) (*nas.KerberosRealmCreateCreated, error) {
	return nil, nil
}

func (m *mockNASClient) KerberosRealmDelete(params *nas.KerberosRealmDeleteParams, authInfo runtime.ClientAuthInfoWriter, opts ...nas.ClientOption) (*nas.KerberosRealmDeleteOK, error) {
	return nil, nil
}

func (m *mockNASClient) KerberosRealmGet(params *nas.KerberosRealmGetParams, authInfo runtime.ClientAuthInfoWriter, opts ...nas.ClientOption) (*nas.KerberosRealmGetOK, error) {
	return nil, nil
}

func (m *mockNASClient) KerberosRealmModify(params *nas.KerberosRealmModifyParams, authInfo runtime.ClientAuthInfoWriter, opts ...nas.ClientOption) (*nas.KerberosRealmModifyOK, error) {
	return nil, nil
}

func (m *mockNASClient) LocalCifsGroupCollectionGet(params *nas.LocalCifsGroupCollectionGetParams, authInfo runtime.ClientAuthInfoWriter, opts ...nas.ClientOption) (*nas.LocalCifsGroupCollectionGetOK, error) {
	return nil, nil
}

func (m *mockNASClient) LocalCifsGroupMembersBulkDelete(params *nas.LocalCifsGroupMembersBulkDeleteParams, authInfo runtime.ClientAuthInfoWriter, opts ...nas.ClientOption) (*nas.LocalCifsGroupMembersBulkDeleteOK, error) {
	return nil, nil
}

func (m *mockNASClient) LocalCifsGroupMembersCollectionGet(params *nas.LocalCifsGroupMembersCollectionGetParams, authInfo runtime.ClientAuthInfoWriter, opts ...nas.ClientOption) (*nas.LocalCifsGroupMembersCollectionGetOK, error) {
	return nil, nil
}

func (m *mockNASClient) LocalCifsGroupMembersCreate(params *nas.LocalCifsGroupMembersCreateParams, authInfo runtime.ClientAuthInfoWriter, opts ...nas.ClientOption) (*nas.LocalCifsGroupMembersCreateCreated, error) {
	return nil, nil
}

func (m *mockNASClient) LocalCifsGroupMembersDelete(params *nas.LocalCifsGroupMembersDeleteParams, authInfo runtime.ClientAuthInfoWriter, opts ...nas.ClientOption) (*nas.LocalCifsGroupMembersDeleteOK, error) {
	return nil, nil
}

func (m *mockNASClient) NfsClientsGet(params *nas.NfsClientsGetParams, authInfo runtime.ClientAuthInfoWriter, opts ...nas.ClientOption) (*nas.NfsClientsGetOK, error) {
	return nil, nil
}

func (m *mockNASClient) UserGroupPrivilegesCollectionGet(params *nas.UserGroupPrivilegesCollectionGetParams, authInfo runtime.ClientAuthInfoWriter, opts ...nas.ClientOption) (*nas.UserGroupPrivilegesCollectionGetOK, error) {
	return nil, nil
}

func (m *mockNASClient) UserGroupPrivilegesCreate(params *nas.UserGroupPrivilegesCreateParams, authInfo runtime.ClientAuthInfoWriter, opts ...nas.ClientOption) (*nas.UserGroupPrivilegesCreateCreated, error) {
	return nil, nil
}

func (m *mockNASClient) UserGroupPrivilegesModify(params *nas.UserGroupPrivilegesModifyParams, authInfo runtime.ClientAuthInfoWriter, opts ...nas.ClientOption) (*nas.UserGroupPrivilegesModifyOK, error) {
	return nil, nil
}

func (m *mockNASClient) SetTransport(transport runtime.ClientTransport) {
	// No-op for testing
}

func TestExportPolicyCreate(t *testing.T) {
	policyName := "test-policy"

	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		api := &mockNASClient{err: errors.New("something went wrong")}
		client := &nasClient{api: api}
		name, err := client.ExportPolicyCreate(&ExportPolicyCreateParams{})
		assert.EqualError(tt, err, api.err.Error())
		assert.Empty(tt, name)
	})

	t.Run("WhenSuccessful_ThenReturnPolicyName", func(tt *testing.T) {
		resp := &nas.ExportPolicyCreateCreated{
			Payload: &models.ExportPolicyResponse{
				ExportPolicyResponseInlineRecords: []*models.ExportPolicy{
					{Name: &policyName},
				},
			},
		}
		api := &mockNASClient{response: resp}
		client := &nasClient{api: api}
		name, err := client.ExportPolicyCreate(&ExportPolicyCreateParams{})
		assert.NoError(tt, err)
		assert.Equal(tt, policyName, name)
	})
}

func TestExportPolicyGet(t *testing.T) {
	policyName := "test-policy"

	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		api := &mockNASClient{err: errors.New("api error")}
		client := &nasClient{api: api}
		params := &ExportPolicyGetParams{Name: &policyName}
		policy, err := client.ExportPolicyGet(params)
		assert.EqualError(tt, err, "api error")
		assert.Nil(tt, policy)
	})

	t.Run("WhenNoRecords_ThenReturnNotFound", func(tt *testing.T) {
		resp := &nas.ExportPolicyCollectionGetOK{
			Payload: &models.ExportPolicyResponse{
				ExportPolicyResponseInlineRecords: []*models.ExportPolicy{},
			},
		}
		api := &mockNASClient{response: resp}
		client := &nasClient{api: api}
		params := &ExportPolicyGetParams{Name: &policyName}
		policy, err := client.ExportPolicyGet(params)
		assert.Error(tt, err)
		assert.Nil(tt, policy)
	})

	t.Run("WhenMultipleRecords_ThenReturnError", func(tt *testing.T) {
		resp := &nas.ExportPolicyCollectionGetOK{
			Payload: &models.ExportPolicyResponse{
				ExportPolicyResponseInlineRecords: []*models.ExportPolicy{{}, {}},
			},
		}
		api := &mockNASClient{response: resp}
		client := &nasClient{api: api}
		params := &ExportPolicyGetParams{Name: &policyName}
		policy, err := client.ExportPolicyGet(params)
		assert.Error(tt, err)
		assert.Nil(tt, policy)
	})

	t.Run("WhenSingleRecord_ThenReturnPolicy", func(tt *testing.T) {
		resp := &nas.ExportPolicyCollectionGetOK{
			Payload: &models.ExportPolicyResponse{
				ExportPolicyResponseInlineRecords: []*models.ExportPolicy{{Name: &policyName}},
			},
		}
		api := &mockNASClient{response: resp}
		client := &nasClient{api: api}
		params := &ExportPolicyGetParams{Name: &policyName}
		policy, err := client.ExportPolicyGet(params)
		assert.NoError(tt, err)
		assert.NotNil(tt, policy)
		assert.Equal(tt, policyName, *policy.Name)
	})
}

func TestExportPolicyModify(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		api := &mockNASClient{err: errors.New("api error")}
		client := &nasClient{api: api}
		err := client.ExportPolicyModify(&ExportPolicyModifyParams{})
		assert.EqualError(tt, err, "api error")
	})

	t.Run("WhenSuccessful_ThenNoError", func(tt *testing.T) {
		api := &mockNASClient{}
		client := &nasClient{api: api}
		err := client.ExportPolicyModify(&ExportPolicyModifyParams{})
		assert.NoError(tt, err)
	})
}

func TestExportPolicyDelete(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		api := &mockNASClient{err: errors.New("api error")}
		client := &nasClient{api: api}
		err := client.ExportPolicyDelete(&ExportPolicyDeleteParams{})
		assert.EqualError(tt, err, "api error")
	})

	t.Run("WhenSuccessful_ThenNoError", func(tt *testing.T) {
		api := &mockNASClient{}
		client := &nasClient{api: api}
		err := client.ExportPolicyDelete(&ExportPolicyDeleteParams{})
		assert.NoError(tt, err)
	})
}

func TestNfsServiceGet(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		api := &mockNASClient{err: errors.New("api error")}
		client := &nasClient{api: api}
		nfs, err := client.NfsServiceGet(&NfsServiceGetParams{})
		assert.EqualError(tt, err, "api error")
		assert.Nil(tt, nfs)
	})

	t.Run("WhenSuccessful_ThenReturnNfsService", func(tt *testing.T) {
		val := true
		resp := &nas.NfsGetOK{Payload: &models.NfsService{Enabled: &val}}
		api := &mockNASClient{response: resp}
		client := &nasClient{api: api}
		nfs, err := client.NfsServiceGet(&NfsServiceGetParams{})
		assert.NoError(tt, err)
		assert.NotNil(tt, nfs)
		assert.Equal(tt, &val, nfs.Enabled)
	})
}

func TestNfsServiceCreate(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		api := &mockNASClient{err: errors.New("api error")}
		client := &nasClient{api: api}
		err := client.NfsServiceCreate(&NfsServiceCreateParams{})
		assert.EqualError(tt, err, "api error")
	})

	t.Run("WhenSuccessful_ThenNoError", func(tt *testing.T) {
		api := &mockNASClient{}
		client := &nasClient{api: api}
		err := client.NfsServiceCreate(&NfsServiceCreateParams{})
		assert.NoError(tt, err)
	})
}

func TestNfsServiceModify(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		api := &mockNASClient{err: errors.New("api error")}
		client := &nasClient{api: api}
		err := client.NfsServiceModify(&NfsServiceModifyParams{})
		assert.EqualError(tt, err, "api error")
	})

	t.Run("WhenSuccessful_ThenNoError", func(tt *testing.T) {
		api := &mockNASClient{}
		client := &nasClient{api: api}
		err := client.NfsServiceModify(&NfsServiceModifyParams{})
		assert.NoError(tt, err)
	})
}

func TestCifsServiceGet(t *testing.T) {
	cifsName := "cifs-service"
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		api := &mockNASClient{err: errors.New("api error")}
		client := &nasClient{api: api}
		cifs, err := client.CifsServiceGet(&CifsServiceGetParams{})
		assert.EqualError(tt, err, "api error")
		assert.Nil(tt, cifs)
	})

	t.Run("WhenNoRecords_ThenReturnNotFound", func(tt *testing.T) {
		resp := &nas.CifsServiceCollectionGetOK{
			Payload: &models.CifsServiceResponse{
				CifsServiceResponseInlineRecords: []*models.CifsService{},
			},
		}
		api := &mockNASClient{response: resp}
		client := &nasClient{api: api}
		cifs, err := client.CifsServiceGet(&CifsServiceGetParams{})
		assert.Error(tt, err)
		assert.Nil(tt, cifs)
	})

	t.Run("WhenMultipleRecords_ThenReturnError", func(tt *testing.T) {
		resp := &nas.CifsServiceCollectionGetOK{
			Payload: &models.CifsServiceResponse{
				CifsServiceResponseInlineRecords: []*models.CifsService{{}, {}},
			},
		}
		api := &mockNASClient{response: resp}
		client := &nasClient{api: api}
		cifs, err := client.CifsServiceGet(&CifsServiceGetParams{})
		assert.Error(tt, err)
		assert.Nil(tt, cifs)
	})

	t.Run("WhenSingleRecord_ThenReturnCifsService", func(tt *testing.T) {
		resp := &nas.CifsServiceCollectionGetOK{
			Payload: &models.CifsServiceResponse{
				CifsServiceResponseInlineRecords: []*models.CifsService{{Name: &cifsName}},
			},
		}
		api := &mockNASClient{response: resp}
		client := &nasClient{api: api}
		cifs, err := client.CifsServiceGet(&CifsServiceGetParams{})
		assert.NoError(tt, err)
		assert.NotNil(tt, cifs)
		assert.Equal(tt, cifsName, *cifs.Name)
	})
}

func TestCifsServiceCreate(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		api := &mockNASClient{err: errors.New("api error")}
		client := &nasClient{api: api}
		err := client.CifsServiceCreate(&CifsServiceCreateParams{})
		assert.EqualError(tt, err, "api error")
	})

	t.Run("WhenSuccessful_ThenNoError", func(tt *testing.T) {
		api := &mockNASClient{}
		client := &nasClient{api: api}
		err := client.CifsServiceCreate(&CifsServiceCreateParams{})
		assert.NoError(tt, err)
	})
}

func TestCifsServiceModify(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		api := &mockNASClient{err: errors.New("api error")}
		client := &nasClient{api: api}
		err := client.CifsServiceModify(&CifsServiceModifyParams{})
		assert.EqualError(tt, err, "api error")
	})

	t.Run("WhenSuccessful_ThenNoError", func(tt *testing.T) {
		api := &mockNASClient{}
		client := &nasClient{api: api}
		err := client.CifsServiceModify(&CifsServiceModifyParams{})
		assert.NoError(tt, err)
	})
}
