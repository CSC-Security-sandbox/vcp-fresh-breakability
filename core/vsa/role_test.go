package vsa

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/security"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	ontaprest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestCreateRole(t *testing.T) {
	t.Run("Success", func(tt *testing.T) {
		mockSecurity := new(ontaprest.MockSecurityClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("Security").Return(mockSecurity)

		originalgetOntapClientFunc := getOntapClientFunc
		defer func() {
			getOntapClientFunc = originalgetOntapClientFunc
		}()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}

		params := CreateRoleParams{
			Name: "test-role",
			Privileges: []*RolePrivilege{
				{
					Path:   "/api/storage/volumes",
					Access: "readonly",
					Query:  "",
				},
				{
					Path:   "/api/storage/snapshots",
					Access: "all",
					Query:  "-fields name,state",
				},
			},
		}

		expectedLocation := "/api/security/roles/test-role"
		mockSecurity.On("RoleCreate", mock.Anything).Return(expectedLocation, nil)

		location, err := rc.CreateRole(params)

		assert.NoError(tt, err)
		assert.Equal(tt, expectedLocation, location)

		mockSecurity.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("ErrorWhenGetOntapClientFails", func(tt *testing.T) {
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() {
			getOntapClientFunc = originalgetOntapClientFunc
		}()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return nil, errors.New("client creation failed")
		}

		rc := &OntapRestProvider{}

		params := CreateRoleParams{
			Name: "test-role",
		}

		location, err := rc.CreateRole(params)

		assert.Error(tt, err)
		assert.Equal(tt, "", location)
		assert.Contains(tt, err.Error(), "client creation failed")
	})

	t.Run("ErrorWhenRoleCreateFails", func(tt *testing.T) {
		mockSecurity := new(ontaprest.MockSecurityClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("Security").Return(mockSecurity)

		originalgetOntapClientFunc := getOntapClientFunc
		defer func() {
			getOntapClientFunc = originalgetOntapClientFunc
		}()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}

		params := CreateRoleParams{
			Name: "test-role",
		}

		mockSecurity.On("RoleCreate", mock.Anything).Return("", errors.New("role creation failed"))

		location, err := rc.CreateRole(params)

		assert.Error(tt, err)
		assert.Equal(tt, "", location)
		assert.Contains(tt, err.Error(), "role creation failed")

		mockSecurity.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("SuccessWithEmptyPrivileges", func(tt *testing.T) {
		mockSecurity := new(ontaprest.MockSecurityClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("Security").Return(mockSecurity)

		originalgetOntapClientFunc := getOntapClientFunc
		defer func() {
			getOntapClientFunc = originalgetOntapClientFunc
		}()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}

		params := CreateRoleParams{
			Name:       "test-role",
			Privileges: []*RolePrivilege{},
		}

		expectedLocation := "/api/security/roles/test-role"
		mockSecurity.On("RoleCreate", mock.Anything).Return(expectedLocation, nil)

		location, err := rc.CreateRole(params)

		assert.NoError(tt, err)
		assert.Equal(tt, expectedLocation, location)

		mockSecurity.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})
}

func TestGetRole(t *testing.T) {
	t.Run("Success", func(tt *testing.T) {
		mockSecurity := new(ontaprest.MockSecurityClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("Security").Return(mockSecurity)

		originalgetOntapClientFunc := getOntapClientFunc
		defer func() {
			getOntapClientFunc = originalgetOntapClientFunc
		}()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}

		params := GetRoleParams{
			Name:      "test-role",
			OwnerUUID: nillable.ToPointer("owner-uuid"),
		}

		roleName := "test-role"
		ownerUUID := "owner-uuid"
		privilegePath1 := "/api/storage/volumes"
		privilegePath2 := "/api/storage/snapshots"
		accessReadonly := models.RolePrivilegeLevelReadonly
		accessAll := models.RolePrivilegeLevelAll
		query := "-fields name,state"

		mockOntapRole := &ontaprest.Role{
			Role: models.Role{
				Name: &roleName,
				Owner: &models.RoleInlineOwner{
					UUID: &ownerUUID,
				},
				RoleInlinePrivileges: []*models.RolePrivilege{
					{
						Path:   &privilegePath1,
						Access: &accessReadonly,
						Query:  nil,
					},
					{
						Path:   &privilegePath2,
						Access: &accessAll,
						Query:  &query,
					},
				},
			},
		}

		mockSecurity.On("RoleGet", mock.Anything).Return(mockOntapRole, nil)

		role, err := rc.GetRole(params)

		assert.NoError(tt, err)
		assert.NotNil(tt, role)
		assert.Equal(tt, "test-role", role.Name)
		assert.Equal(tt, "owner-uuid", role.OwnerID)
		assert.Len(tt, role.Privileges, 2)
		assert.Equal(tt, "/api/storage/volumes", role.Privileges[0].Path)
		assert.Equal(tt, "readonly", role.Privileges[0].Access)
		assert.Equal(tt, "", role.Privileges[0].Query)
		assert.Equal(tt, "/api/storage/snapshots", role.Privileges[1].Path)
		assert.Equal(tt, "all", role.Privileges[1].Access)
		assert.Equal(tt, "-fields name,state", role.Privileges[1].Query)

		mockSecurity.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("ErrorWhenGetOntapClientFails", func(tt *testing.T) {
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() {
			getOntapClientFunc = originalgetOntapClientFunc
		}()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return nil, errors.New("client creation failed")
		}

		rc := &OntapRestProvider{}

		params := GetRoleParams{
			Name: "test-role",
		}

		role, err := rc.GetRole(params)

		assert.Error(tt, err)
		assert.Nil(tt, role)
		assert.Contains(tt, err.Error(), "client creation failed")
	})

	t.Run("ErrorWhenRoleGetFails", func(tt *testing.T) {
		mockSecurity := new(ontaprest.MockSecurityClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("Security").Return(mockSecurity)

		originalgetOntapClientFunc := getOntapClientFunc
		defer func() {
			getOntapClientFunc = originalgetOntapClientFunc
		}()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}

		params := GetRoleParams{
			Name: "test-role",
		}

		mockSecurity.On("RoleGet", mock.Anything).Return(nil, errors.New("role get failed"))

		role, err := rc.GetRole(params)

		assert.Error(tt, err)
		assert.Nil(tt, role)
		assert.Contains(tt, err.Error(), "role get failed")

		mockSecurity.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("SuccessWithNilOwner", func(tt *testing.T) {
		mockSecurity := new(ontaprest.MockSecurityClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("Security").Return(mockSecurity)

		originalgetOntapClientFunc := getOntapClientFunc
		defer func() {
			getOntapClientFunc = originalgetOntapClientFunc
		}()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}

		params := GetRoleParams{
			Name: "test-role",
		}

		roleName := "test-role"
		privilegePath := "/api/storage/volumes"
		accessReadonly := models.RolePrivilegeLevelReadonly

		mockOntapRole := &ontaprest.Role{
			Role: models.Role{
				Name:  &roleName,
				Owner: nil, // No owner
				RoleInlinePrivileges: []*models.RolePrivilege{
					{
						Path:   &privilegePath,
						Access: &accessReadonly,
						Query:  nil,
					},
				},
			},
		}

		mockSecurity.On("RoleGet", mock.Anything).Return(mockOntapRole, nil)

		role, err := rc.GetRole(params)

		assert.NoError(tt, err)
		assert.NotNil(tt, role)
		assert.Equal(tt, "test-role", role.Name)
		assert.Equal(tt, "", role.OwnerID) // Should be empty when owner is nil
		assert.Len(tt, role.Privileges, 1)

		mockSecurity.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})
}

func TestModifyRolePrivilege(t *testing.T) {
	t.Run("Success", func(tt *testing.T) {
		mockSecurity := new(ontaprest.MockSecurityClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("Security").Return(mockSecurity)

		originalgetOntapClientFunc := getOntapClientFunc
		defer func() {
			getOntapClientFunc = originalgetOntapClientFunc
		}()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}

		params := ModifyRolePrivilegeParams{
			OwnerID: "owner-uuid",
			Name:    "test-role",
			Path:    "/api/storage/volumes",
			Access:  "readonly",
			Query:   "-fields name,state",
		}

		mockSecurity.On("RolePrivilegeModify", mock.Anything).Return(nil)

		err := rc.ModifyRolePrivilege(params)

		assert.NoError(tt, err)

		mockSecurity.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("ErrorWhenGetOntapClientFails", func(tt *testing.T) {
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() {
			getOntapClientFunc = originalgetOntapClientFunc
		}()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return nil, errors.New("client creation failed")
		}

		rc := &OntapRestProvider{}

		params := ModifyRolePrivilegeParams{
			Name: "test-role",
		}

		err := rc.ModifyRolePrivilege(params)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "client creation failed")
	})

	t.Run("ErrorWhenRolePrivilegeModifyFails", func(tt *testing.T) {
		mockSecurity := new(ontaprest.MockSecurityClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("Security").Return(mockSecurity)

		originalgetOntapClientFunc := getOntapClientFunc
		defer func() {
			getOntapClientFunc = originalgetOntapClientFunc
		}()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}

		params := ModifyRolePrivilegeParams{
			Name: "test-role",
		}

		mockSecurity.On("RolePrivilegeModify", mock.Anything).Return(errors.New("privilege modify failed"))

		err := rc.ModifyRolePrivilege(params)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "privilege modify failed")

		mockSecurity.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})
}

func TestGetRoleCollection(t *testing.T) {
	t.Run("Success", func(tt *testing.T) {
		mockSecurity := new(ontaprest.MockSecurityClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("Security").Return(mockSecurity)

		originalgetOntapClientFunc := getOntapClientFunc
		defer func() {
			getOntapClientFunc = originalgetOntapClientFunc
		}()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}

		params := GetRoleCollectionParams{
			Name: nillable.ToPointer("test-role"),
		}

		roleName1 := "test-role-1"
		roleName2 := "test-role-2"
		ownerUUID1 := "owner-uuid-1"
		privilegePath := "/api/storage/volumes"
		accessReadonly := models.RolePrivilegeLevelReadonly

		mockResponse := &ontaprest.RoleCollectionGetResponse{
			RoleCollectionGetOK: &security.RoleCollectionGetOK{
				Payload: &models.RoleResponse{
					NumRecords: nillable.ToPointer(int64(2)),
					RoleResponseInlineRecords: []*models.Role{
						{
							Name: &roleName1,
							Owner: &models.RoleInlineOwner{
								UUID: &ownerUUID1,
							},
							RoleInlinePrivileges: []*models.RolePrivilege{
								{
									Path:   &privilegePath,
									Access: &accessReadonly,
									Query:  nil,
								},
							},
						},
						{
							Name:  &roleName2,
							Owner: nil, // No owner
						},
					},
				},
			},
		}

		mockSecurity.On("RoleCollectionGet", mock.Anything).Return(mockResponse, nil)

		roles, err := rc.GetRoleCollection(params)

		assert.NoError(tt, err)
		assert.NotNil(tt, roles)
		assert.Len(tt, roles, 2)

		// Check first role
		assert.Equal(tt, "test-role-1", roles[0].Name)
		assert.Equal(tt, "owner-uuid-1", roles[0].OwnerID)
		assert.Len(tt, roles[0].Privileges, 1)
		assert.Equal(tt, "/api/storage/volumes", roles[0].Privileges[0].Path)
		assert.Equal(tt, "readonly", roles[0].Privileges[0].Access)
		assert.Equal(tt, "", roles[0].Privileges[0].Query)

		// Check second role
		assert.Equal(tt, "test-role-2", roles[1].Name)
		assert.Equal(tt, "", roles[1].OwnerID) // Should be empty when owner is nil
		assert.Len(tt, roles[1].Privileges, 0) // No privileges

		mockSecurity.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("ErrorWhenGetOntapClientFails", func(tt *testing.T) {
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() {
			getOntapClientFunc = originalgetOntapClientFunc
		}()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return nil, errors.New("client creation failed")
		}

		rc := &OntapRestProvider{}

		params := GetRoleCollectionParams{}

		roles, err := rc.GetRoleCollection(params)

		assert.Error(tt, err)
		assert.Nil(tt, roles)
		assert.Contains(tt, err.Error(), "client creation failed")
	})

	t.Run("ErrorWhenRoleCollectionGetFails", func(tt *testing.T) {
		mockSecurity := new(ontaprest.MockSecurityClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("Security").Return(mockSecurity)

		originalgetOntapClientFunc := getOntapClientFunc
		defer func() {
			getOntapClientFunc = originalgetOntapClientFunc
		}()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}

		params := GetRoleCollectionParams{}

		mockSecurity.On("RoleCollectionGet", mock.Anything).Return(nil, errors.New("collection get failed"))

		roles, err := rc.GetRoleCollection(params)

		assert.Error(tt, err)
		assert.Nil(tt, roles)
		assert.Contains(tt, err.Error(), "collection get failed")

		mockSecurity.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("SuccessWithEmptyResponse", func(tt *testing.T) {
		mockSecurity := new(ontaprest.MockSecurityClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("Security").Return(mockSecurity)

		originalgetOntapClientFunc := getOntapClientFunc
		defer func() {
			getOntapClientFunc = originalgetOntapClientFunc
		}()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}

		params := GetRoleCollectionParams{}

		mockResponse := &ontaprest.RoleCollectionGetResponse{
			RoleCollectionGetOK: &security.RoleCollectionGetOK{
				Payload: &models.RoleResponse{
					NumRecords:                nillable.ToPointer(int64(0)),
					RoleResponseInlineRecords: []*models.Role{},
				},
			},
		}

		mockSecurity.On("RoleCollectionGet", mock.Anything).Return(mockResponse, nil)

		roles, err := rc.GetRoleCollection(params)

		assert.NoError(tt, err)
		assert.Nil(tt, roles) // Should be nil when no valid records

		mockSecurity.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("SuccessWithNilPayload", func(tt *testing.T) {
		mockSecurity := new(ontaprest.MockSecurityClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("Security").Return(mockSecurity)

		originalgetOntapClientFunc := getOntapClientFunc
		defer func() {
			getOntapClientFunc = originalgetOntapClientFunc
		}()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}

		params := GetRoleCollectionParams{}

		mockResponse := &ontaprest.RoleCollectionGetResponse{
			RoleCollectionGetOK: &security.RoleCollectionGetOK{
				Payload: nil,
			},
		}

		mockSecurity.On("RoleCollectionGet", mock.Anything).Return(mockResponse, nil)

		roles, err := rc.GetRoleCollection(params)

		assert.NoError(tt, err)
		assert.Nil(tt, roles) // Should be nil when no valid records

		mockSecurity.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("SuccessWithNilRecords", func(tt *testing.T) {
		mockSecurity := new(ontaprest.MockSecurityClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("Security").Return(mockSecurity)

		originalgetOntapClientFunc := getOntapClientFunc
		defer func() {
			getOntapClientFunc = originalgetOntapClientFunc
		}()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}

		params := GetRoleCollectionParams{}

		mockResponse := &ontaprest.RoleCollectionGetResponse{
			RoleCollectionGetOK: &security.RoleCollectionGetOK{
				Payload: &models.RoleResponse{
					NumRecords:                nillable.ToPointer(int64(1)),
					RoleResponseInlineRecords: nil,
				},
			},
		}

		mockSecurity.On("RoleCollectionGet", mock.Anything).Return(mockResponse, nil)

		roles, err := rc.GetRoleCollection(params)

		assert.NoError(tt, err)
		assert.Nil(tt, roles) // Should be nil when no valid records

		mockSecurity.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("SuccessWithNilRoleName", func(tt *testing.T) {
		mockSecurity := new(ontaprest.MockSecurityClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("Security").Return(mockSecurity)

		originalgetOntapClientFunc := getOntapClientFunc
		defer func() {
			getOntapClientFunc = originalgetOntapClientFunc
		}()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}

		params := GetRoleCollectionParams{}

		// Role with nil name should be skipped
		mockResponse := &ontaprest.RoleCollectionGetResponse{
			RoleCollectionGetOK: &security.RoleCollectionGetOK{
				Payload: &models.RoleResponse{
					NumRecords: nillable.ToPointer(int64(1)),
					RoleResponseInlineRecords: []*models.Role{
						{
							Name:  nil, // Nil name - should be skipped
							Owner: nil,
						},
					},
				},
			},
		}

		mockSecurity.On("RoleCollectionGet", mock.Anything).Return(mockResponse, nil)

		roles, err := rc.GetRoleCollection(params)

		assert.NoError(tt, err)
		assert.Nil(tt, roles) // Should be nil when no valid records // Should be 0 because nil name role is skipped

		mockSecurity.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("SuccessWithNilRole", func(tt *testing.T) {
		mockSecurity := new(ontaprest.MockSecurityClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("Security").Return(mockSecurity)

		originalgetOntapClientFunc := getOntapClientFunc
		defer func() {
			getOntapClientFunc = originalgetOntapClientFunc
		}()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}

		params := GetRoleCollectionParams{}

		// Nil role should be skipped
		mockResponse := &ontaprest.RoleCollectionGetResponse{
			RoleCollectionGetOK: &security.RoleCollectionGetOK{
				Payload: &models.RoleResponse{
					NumRecords: nillable.ToPointer(int64(1)),
					RoleResponseInlineRecords: []*models.Role{
						nil, // Nil role - should be skipped
					},
				},
			},
		}

		mockSecurity.On("RoleCollectionGet", mock.Anything).Return(mockResponse, nil)

		roles, err := rc.GetRoleCollection(params)

		assert.NoError(tt, err)
		assert.Nil(tt, roles) // Should be nil when no valid records // Should be 0 because nil role is skipped

		mockSecurity.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("SuccessWithInvalidPrivileges", func(tt *testing.T) {
		mockSecurity := new(ontaprest.MockSecurityClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("Security").Return(mockSecurity)

		originalgetOntapClientFunc := getOntapClientFunc
		defer func() {
			getOntapClientFunc = originalgetOntapClientFunc
		}()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}

		params := GetRoleCollectionParams{}

		roleName := "test-role"
		privilegePath := "/api/storage/volumes"
		accessReadonly := models.RolePrivilegeLevelReadonly

		mockResponse := &ontaprest.RoleCollectionGetResponse{
			RoleCollectionGetOK: &security.RoleCollectionGetOK{
				Payload: &models.RoleResponse{
					NumRecords: nillable.ToPointer(int64(1)),
					RoleResponseInlineRecords: []*models.Role{
						{
							Name: &roleName,
							Owner: &models.RoleInlineOwner{
								UUID: nillable.ToPointer("owner-uuid"),
							},
							RoleInlinePrivileges: []*models.RolePrivilege{
								{
									Path:   &privilegePath,
									Access: &accessReadonly,
									Query:  nil,
								},
								nil, // Nil privilege - should be skipped
								{
									Path:   nil, // Nil path - should be skipped
									Access: &accessReadonly,
									Query:  nil,
								},
								{
									Path:   &privilegePath,
									Access: nil, // Nil access - should be skipped
									Query:  nil,
								},
							},
						},
					},
				},
			},
		}

		mockSecurity.On("RoleCollectionGet", mock.Anything).Return(mockResponse, nil)

		roles, err := rc.GetRoleCollection(params)

		assert.NoError(tt, err)
		assert.NotNil(tt, roles)
		assert.Len(tt, roles, 1)
		assert.Equal(tt, "test-role", roles[0].Name)
		assert.Len(tt, roles[0].Privileges, 1) // Only valid privilege should be included
		assert.Equal(tt, "/api/storage/volumes", roles[0].Privileges[0].Path)
		assert.Equal(tt, "readonly", roles[0].Privileges[0].Access)

		mockSecurity.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})
}

func TestCreateRolePrivilege(t *testing.T) {
	t.Run("Success", func(tt *testing.T) {
		mockSecurity := new(ontaprest.MockSecurityClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("Security").Return(mockSecurity)

		originalgetOntapClientFunc := getOntapClientFunc
		defer func() {
			getOntapClientFunc = originalgetOntapClientFunc
		}()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}

		params := CreateRolePrivilegeParams{
			OwnerID: "owner-uuid",
			Name:    "test-role",
			Path:    "/api/storage/volumes",
			Access:  "readonly",
			Query:   "-fields name,state",
		}

		expectedLocation := "/api/security/roles/owner-uuid/test-role/privileges/api/storage/volumes"
		mockSecurity.On("RolePrivilegeCreate", mock.Anything).Return(expectedLocation, nil)

		location, err := rc.CreateRolePrivilege(params)

		assert.NoError(tt, err)
		assert.Equal(tt, expectedLocation, location)

		mockSecurity.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("ErrorWhenGetOntapClientFails", func(tt *testing.T) {
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() {
			getOntapClientFunc = originalgetOntapClientFunc
		}()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return nil, errors.New("client creation failed")
		}

		rc := &OntapRestProvider{}

		params := CreateRolePrivilegeParams{
			OwnerID: "owner-uuid",
			Name:    "test-role",
			Path:    "/api/storage/volumes",
			Access:  "readonly",
		}

		location, err := rc.CreateRolePrivilege(params)

		assert.Error(tt, err)
		assert.Equal(tt, "", location)
		assert.Contains(tt, err.Error(), "client creation failed")
	})

	t.Run("ErrorWhenRolePrivilegeCreateFails", func(tt *testing.T) {
		mockSecurity := new(ontaprest.MockSecurityClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("Security").Return(mockSecurity)

		originalgetOntapClientFunc := getOntapClientFunc
		defer func() {
			getOntapClientFunc = originalgetOntapClientFunc
		}()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}

		params := CreateRolePrivilegeParams{
			OwnerID: "owner-uuid",
			Name:    "test-role",
			Path:    "/api/storage/volumes",
			Access:  "readonly",
		}

		mockSecurity.On("RolePrivilegeCreate", mock.Anything).Return("", errors.New("privilege create failed"))

		location, err := rc.CreateRolePrivilege(params)

		assert.Error(tt, err)
		assert.Equal(tt, "", location)
		assert.Contains(tt, err.Error(), "privilege create failed")

		mockSecurity.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("SuccessWithEmptyQuery", func(tt *testing.T) {
		mockSecurity := new(ontaprest.MockSecurityClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("Security").Return(mockSecurity)

		originalgetOntapClientFunc := getOntapClientFunc
		defer func() {
			getOntapClientFunc = originalgetOntapClientFunc
		}()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}

		params := CreateRolePrivilegeParams{
			OwnerID: "owner-uuid",
			Name:    "test-role",
			Path:    "/api/storage/volumes",
			Access:  "all",
			Query:   "",
		}

		expectedLocation := "/api/security/roles/owner-uuid/test-role/privileges/api/storage/volumes"
		mockSecurity.On("RolePrivilegeCreate", mock.Anything).Return(expectedLocation, nil)

		location, err := rc.CreateRolePrivilege(params)

		assert.NoError(tt, err)
		assert.Equal(tt, expectedLocation, location)

		mockSecurity.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("SuccessWithCommandPath", func(tt *testing.T) {
		mockSecurity := new(ontaprest.MockSecurityClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("Security").Return(mockSecurity)

		originalgetOntapClientFunc := getOntapClientFunc
		defer func() {
			getOntapClientFunc = originalgetOntapClientFunc
		}()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}

		params := CreateRolePrivilegeParams{
			OwnerID: "owner-uuid",
			Name:    "test-role",
			Path:    "snaplock compliance-clock",
			Access:  "all",
			Query:   "-fields name,state",
		}

		expectedLocation := "/api/security/roles/owner-uuid/test-role/privileges/snaplock%20compliance-clock"
		mockSecurity.On("RolePrivilegeCreate", mock.Anything).Return(expectedLocation, nil)

		location, err := rc.CreateRolePrivilege(params)

		assert.NoError(tt, err)
		assert.Equal(tt, expectedLocation, location)

		mockSecurity.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})
}

func TestDeleteRole(t *testing.T) {
	t.Run("Success", func(tt *testing.T) {
		mockSecurity := new(ontaprest.MockSecurityClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("Security").Return(mockSecurity)

		originalgetOntapClientFunc := getOntapClientFunc
		defer func() {
			getOntapClientFunc = originalgetOntapClientFunc
		}()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}

		ownerUUID := "owner-uuid-123"
		params := DeleteRoleParams{
			Name:      "test-role",
			OwnerUUID: &ownerUUID,
		}

		mockSecurity.On("RoleDelete", mock.MatchedBy(func(p *ontaprest.RoleDeleteParams) bool {
			return p.Name == "test-role" && p.OwnerUUID != nil && *p.OwnerUUID == ownerUUID
		})).Return(nil)

		err := rc.DeleteRole(params)

		assert.NoError(tt, err)

		mockSecurity.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("SuccessWithNilOwnerUUID", func(tt *testing.T) {
		mockSecurity := new(ontaprest.MockSecurityClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("Security").Return(mockSecurity)

		originalgetOntapClientFunc := getOntapClientFunc
		defer func() {
			getOntapClientFunc = originalgetOntapClientFunc
		}()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}

		params := DeleteRoleParams{
			Name:      "test-role",
			OwnerUUID: nil,
		}

		mockSecurity.On("RoleDelete", mock.MatchedBy(func(p *ontaprest.RoleDeleteParams) bool {
			return p.Name == "test-role" && p.OwnerUUID == nil
		})).Return(nil)

		err := rc.DeleteRole(params)

		assert.NoError(tt, err)

		mockSecurity.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("ErrorWhenGetOntapClientFails", func(tt *testing.T) {
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() {
			getOntapClientFunc = originalgetOntapClientFunc
		}()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return nil, errors.New("client creation failed")
		}

		rc := &OntapRestProvider{}

		params := DeleteRoleParams{
			Name: "test-role",
		}

		err := rc.DeleteRole(params)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "client creation failed")
	})

	t.Run("ErrorWhenRoleDeleteFails", func(tt *testing.T) {
		mockSecurity := new(ontaprest.MockSecurityClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("Security").Return(mockSecurity)

		originalgetOntapClientFunc := getOntapClientFunc
		defer func() {
			getOntapClientFunc = originalgetOntapClientFunc
		}()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}

		rc := &OntapRestProvider{}

		params := DeleteRoleParams{
			Name: "test-role",
		}

		mockSecurity.On("RoleDelete", mock.Anything).Return(errors.New("role delete failed"))

		err := rc.DeleteRole(params)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "role delete failed")

		mockSecurity.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})
}
