package active_directory_activities

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	ontaprestmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	utilerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"go.temporal.io/sdk/temporal"
)

func TestCreateNameMappingForKerberosActivity_Success(t *testing.T) {
	ctx := context.Background()
	mockClient := new(ontapRest.MockRESTClient)
	mockNameSvc := new(ontapRest.MockNameServicesClient)
	mockClient.On("NameServices").Return(mockNameSvc).Times(3)

	// First call - check if mapping exists (returns empty)
	mockNameSvc.On("NameMappingCollectionGet", mock.MatchedBy(func(params *ontapRest.NameMappingCollectionGetParams) bool {
		return params.Pattern != nil && *params.Pattern == "(.+)\\$@EXAMPLE.COM"
	})).Return([]*ontapRest.NameMapping{}, utilerrors.NewNotFoundErr("name mapping", nil)).Once()

	// Second call - get all mappings to find index
	mockNameSvc.On("NameMappingCollectionGet", mock.MatchedBy(func(params *ontapRest.NameMappingCollectionGetParams) bool {
		return params.Direction != nil && *params.Direction == "krb-unix"
	})).Return([]*ontapRest.NameMapping{}, nil).Once()

	// Create name mapping
	mockNameSvc.On("NameMappingCreate", mock.Anything).Return(nil).Once()

	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{})
	defer cleanup()

	activity := ActiveDirectoryActivity{}
	err := activity.CreateNameMappingForKerberosActivity(ctx, &models.Node{}, "svm-uuid", "example.com")

	require.NoError(t, err)
	mockClient.AssertExpectations(t)
	mockNameSvc.AssertExpectations(t)
}

func TestCreateNameMappingForKerberosActivity_MappingAlreadyExists(t *testing.T) {
	ctx := context.Background()
	mockClient := new(ontapRest.MockRESTClient)
	mockNameSvc := new(ontapRest.MockNameServicesClient)
	mockClient.On("NameServices").Return(mockNameSvc).Once()

	pattern := "(.+)\\$@EXAMPLE.COM"
	direction := "krb-unix"
	index := int64(1)
	existingMapping := &ontapRest.NameMapping{
		NameMapping: ontaprestmodels.NameMapping{
			Pattern:   &pattern,
			Direction: &direction,
			Index:     &index,
		},
	}
	mockNameSvc.On("NameMappingCollectionGet", mock.Anything).Return([]*ontapRest.NameMapping{existingMapping}, nil).Once()

	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{})
	defer cleanup()

	activity := ActiveDirectoryActivity{}
	err := activity.CreateNameMappingForKerberosActivity(ctx, &models.Node{}, "svm-uuid", "example.com")

	require.NoError(t, err)
	mockClient.AssertExpectations(t)
	mockNameSvc.AssertExpectations(t)
	mockNameSvc.AssertNotCalled(t, "NameMappingCreate", mock.Anything)
}

func TestCreateNameMappingForKerberosActivity_GetOntapProviderError(t *testing.T) {
	ctx := context.Background()
	originalGetter := getOntapRestProvider
	defer func() { getOntapRestProvider = originalGetter }()

	getOntapRestProvider = func(context.Context, *models.Node) (*vsa.OntapRestProvider, error) {
		return nil, errors.New("failed to get provider")
	}

	activity := ActiveDirectoryActivity{}
	err := activity.CreateNameMappingForKerberosActivity(ctx, &models.Node{}, "svm-uuid", "example.com")

	require.Error(t, err)
	var appErr *temporal.ApplicationError
	if assert.True(t, errors.As(err, &appErr)) {
		var tid int
		var origMsg string
		if assert.NoError(t, appErr.Details(&tid, &origMsg)) {
			assert.Equal(t, vsaerrors.ErrKerberosUnclassified, tid)
			assert.Contains(t, origMsg, "failed to get provider")
		}
	}
}

func TestCreateNameMappingForKerberosActivity_CreateError(t *testing.T) {
	ctx := context.Background()
	mockClient := new(ontapRest.MockRESTClient)
	mockNameSvc := new(ontapRest.MockNameServicesClient)
	mockClient.On("NameServices").Return(mockNameSvc).Times(3)

	mockNameSvc.On("NameMappingCollectionGet", mock.Anything).Return([]*ontapRest.NameMapping{}, utilerrors.NewNotFoundErr("name mapping", nil)).Once()
	mockNameSvc.On("NameMappingCollectionGet", mock.Anything).Return([]*ontapRest.NameMapping{}, nil).Once()
	mockNameSvc.On("NameMappingCreate", mock.Anything).Return(errors.New("create failed")).Once()

	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{})
	defer cleanup()

	activity := ActiveDirectoryActivity{}
	err := activity.CreateNameMappingForKerberosActivity(ctx, &models.Node{}, "svm-uuid", "example.com")

	require.Error(t, err)
	mockClient.AssertExpectations(t)
	mockNameSvc.AssertExpectations(t)
}

func TestCheckKerberosRealmExistsActivity_Success_Exists(t *testing.T) {
	ctx := context.Background()
	mockClient := new(ontapRest.MockRESTClient)
	mockNas := new(ontapRest.MockNASClient)
	mockClient.On("NAS").Return(mockNas).Once()

	realmStr := "EXAMPLE.COM"
	realm := &ontapRest.KerberosRealm{
		KerberosRealm: ontaprestmodels.KerberosRealm{
			Name: &realmStr,
		},
	}
	mockNas.On("KerberosRealmGet", mock.Anything).Return([]*ontapRest.KerberosRealm{realm}, nil).Once()

	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{})
	defer cleanup()

	activity := ActiveDirectoryActivity{}
	exists, err := activity.CheckKerberosRealmExistsActivity(ctx, &models.Node{}, "svm-uuid", "EXAMPLE.COM")

	require.NoError(t, err)
	assert.True(t, exists)
	mockClient.AssertExpectations(t)
	mockNas.AssertExpectations(t)
}

func TestCheckKerberosRealmExistsActivity_Success_NotExists(t *testing.T) {
	ctx := context.Background()
	mockClient := new(ontapRest.MockRESTClient)
	mockNas := new(ontapRest.MockNASClient)
	mockClient.On("NAS").Return(mockNas).Once()

	mockNas.On("KerberosRealmGet", mock.Anything).Return([]*ontapRest.KerberosRealm{}, utilerrors.NewNotFoundErr("realm", nil)).Once()

	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{})
	defer cleanup()

	activity := ActiveDirectoryActivity{}
	exists, err := activity.CheckKerberosRealmExistsActivity(ctx, &models.Node{}, "svm-uuid", "EXAMPLE.COM")

	require.NoError(t, err)
	assert.False(t, exists)
	mockClient.AssertExpectations(t)
	mockNas.AssertExpectations(t)
}

func TestCheckKerberosRealmExistsActivity_Error(t *testing.T) {
	ctx := context.Background()
	mockClient := new(ontapRest.MockRESTClient)
	mockNas := new(ontapRest.MockNASClient)
	mockClient.On("NAS").Return(mockNas).Once()

	mockNas.On("KerberosRealmGet", mock.Anything).Return(nil, errors.New("get failed")).Once()

	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{})
	defer cleanup()

	activity := ActiveDirectoryActivity{}
	exists, err := activity.CheckKerberosRealmExistsActivity(ctx, &models.Node{}, "svm-uuid", "EXAMPLE.COM")

	require.Error(t, err)
	assert.False(t, exists)
	mockClient.AssertExpectations(t)
	mockNas.AssertExpectations(t)
}

func TestCreateKerberosRealmActivity_Success(t *testing.T) {
	ctx := context.Background()
	mockClient := new(ontapRest.MockRESTClient)
	mockNas := new(ontapRest.MockNASClient)
	mockClient.On("NAS").Return(mockNas).Once()

	realmKdcPort := vsa.RealmKdcPort
	realmClockSkew := vsa.RealmClockSkew
	realmKdcVendor := vsa.RealmKdcVendor
	realmAdminServerPort := vsa.RealmAdminServerPort
	realmPasswordServerPort := vsa.RealmPasswordServerPort
	realmParams := vsa.KerberosRealmCreateParams{
		Realm:              "EXAMPLE.COM",
		KdcIP:              "192.168.1.1",
		RealmKDCPort:       &realmKdcPort,
		RealmClockSkew:     &realmClockSkew,
		RealmKDCVendor:     &realmKdcVendor,
		AdminServerIP:      strPtr("192.168.1.1"),
		AdminServerPort:    &realmAdminServerPort,
		PasswordServerIP:   strPtr("192.168.1.1"),
		PasswordServerPort: &realmPasswordServerPort,
		ADServerIP:         strPtr("192.168.1.1"),
		ADServerName:       strPtr("ad-server"),
		AdName:             "ad-server",
		SvmUUID:            "svm-uuid",
	}

	mockNas.On("KerberosRealmCreate", mock.Anything).Return(nil).Once()

	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{})
	defer cleanup()

	activity := ActiveDirectoryActivity{}
	err := activity.CreateKerberosRealmActivity(ctx, &models.Node{}, "svm-uuid", realmParams)

	require.NoError(t, err)
	mockClient.AssertExpectations(t)
	mockNas.AssertExpectations(t)
}

func TestCreateKerberosRealmActivity_Error(t *testing.T) {
	ctx := context.Background()
	mockClient := new(ontapRest.MockRESTClient)
	mockNas := new(ontapRest.MockNASClient)
	mockClient.On("NAS").Return(mockNas).Once()

	realmParams := vsa.KerberosRealmCreateParams{
		Realm:   "EXAMPLE.COM",
		KdcIP:   "192.168.1.1",
		SvmUUID: "svm-uuid",
	}

	mockNas.On("KerberosRealmCreate", mock.Anything).Return(errors.New("create failed")).Once()

	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{})
	defer cleanup()

	activity := ActiveDirectoryActivity{}
	err := activity.CreateKerberosRealmActivity(ctx, &models.Node{}, "svm-uuid", realmParams)

	require.Error(t, err)
	mockClient.AssertExpectations(t)
	mockNas.AssertExpectations(t)
}

func TestGetDataLifsForSVMActivity_Success(t *testing.T) {
	ctx := context.Background()
	mockClient := new(ontapRest.MockRESTClient)
	mockNetworking := new(ontapRest.MockNetworkingClient)
	mockClient.On("Networking").Return(mockNetworking).Once()

	lifName := "test-lif"
	lifIP := "192.168.1.10"
	ipAddress := ontaprestmodels.IPAddress(lifIP)
	lif := &ontapRest.IPInterface{
		IPInterface: ontaprestmodels.IPInterface{
			Name: &lifName,
			IP: &ontaprestmodels.IPInfo{
				Address: &ipAddress,
			},
		},
	}

	mockNetworking.On("NetworkIPInterfacesGet", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		callback := args.Get(1).(ontapRest.UserCallbackFunc[[]*ontapRest.IPInterface])
		err := callback([]*ontapRest.IPInterface{lif})
		if err != nil {
			return
		}
	}).Return(nil).Once()

	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{})
	defer cleanup()

	activity := ActiveDirectoryActivity{}
	dataLifs, err := activity.GetDataLifsForSVMActivity(ctx, &models.Node{}, "svm-uuid", "svm-name")

	require.NoError(t, err)
	require.Len(t, dataLifs, 1)
	assert.Equal(t, lifName, *dataLifs[0].Name)
	mockClient.AssertExpectations(t)
	mockNetworking.AssertExpectations(t)
}

func TestGetDataLifsForSVMActivity_Error(t *testing.T) {
	ctx := context.Background()
	mockClient := new(ontapRest.MockRESTClient)
	mockNetworking := new(ontapRest.MockNetworkingClient)
	mockClient.On("Networking").Return(mockNetworking).Once()

	mockNetworking.On("NetworkIPInterfacesGet", mock.Anything, mock.Anything).Return(errors.New("get failed")).Once()

	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{})
	defer cleanup()

	activity := ActiveDirectoryActivity{}
	dataLifs, err := activity.GetDataLifsForSVMActivity(ctx, &models.Node{}, "svm-uuid", "svm-name")

	require.Error(t, err)
	assert.Nil(t, dataLifs)
	mockClient.AssertExpectations(t)
	mockNetworking.AssertExpectations(t)
}

func TestEnableKerberosOnInterfaceActivity_Success(t *testing.T) {
	ctx := context.Background()

	// Mock password decryption
	originalDecryptPassword := utils.DecryptPassword
	decryptedPassword := "decrypted-password"
	utils.DecryptPassword = func(password log.Secret) (*string, error) {
		return &decryptedPassword, nil
	}
	defer func() { utils.DecryptPassword = originalDecryptPassword }()

	mockClient := new(ontapRest.MockRESTClient)
	mockNas := new(ontapRest.MockNASClient)
	mockClient.On("NAS").Return(mockNas).Once()

	interfaceUUID := "interface-uuid"
	enabled := true
	kerberosInterface := &ontapRest.KerberosInterface{
		KerberosInterface: ontaprestmodels.KerberosInterface{
			Enabled: &enabled,
			Interface: &ontaprestmodels.KerberosInterfaceInlineInterface{
				UUID: &interfaceUUID,
			},
		},
	}
	mockNas.On("KerberosInterfaceCollectionGet", mock.Anything).Return([]*ontapRest.KerberosInterface{kerberosInterface}, nil).Once()

	// First call - already enabled, should return early
	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{})
	defer cleanup()

	activity := ActiveDirectoryActivity{}
	ad := &vsa.ActiveDirectory{
		Password:           log.Secret("encrypted"),
		Username:           "admin",
		OrganizationalUnit: "OU=test",
	}
	err := activity.EnableKerberosOnInterfaceActivity(ctx, &models.Node{}, "svm-uuid", "svm-name", "lif-name", "192.168.1.10", "server.example.com", "EXAMPLE.COM", ad)

	require.NoError(t, err)
	mockClient.AssertExpectations(t)
	mockNas.AssertExpectations(t)
}

func TestEnableKerberosOnInterfaceActivity_Success_Enable(t *testing.T) {
	ctx := context.Background()

	// Mock password decryption
	originalDecryptPassword := utils.DecryptPassword
	decryptedPassword := "decrypted-password"
	utils.DecryptPassword = func(password log.Secret) (*string, error) {
		return &decryptedPassword, nil
	}
	defer func() { utils.DecryptPassword = originalDecryptPassword }()

	mockClient := new(ontapRest.MockRESTClient)
	mockNas := new(ontapRest.MockNASClient)
	mockClient.On("NAS").Return(mockNas).Times(2)

	interfaceUUID := "interface-uuid"
	enabled := false
	kerberosInterface := &ontapRest.KerberosInterface{
		KerberosInterface: ontaprestmodels.KerberosInterface{
			Enabled: &enabled,
			Interface: &ontaprestmodels.KerberosInterfaceInlineInterface{
				UUID: &interfaceUUID,
			},
		},
	}
	mockNas.On("KerberosInterfaceCollectionGet", mock.Anything).Return([]*ontapRest.KerberosInterface{kerberosInterface}, nil).Once()
	mockNas.On("KerberosInterfaceModify", mock.Anything).Return(nil).Once()

	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{})
	defer cleanup()

	activity := ActiveDirectoryActivity{}
	ad := &vsa.ActiveDirectory{
		Password:           log.Secret("encrypted"),
		Username:           "admin",
		OrganizationalUnit: "OU=test",
	}
	err := activity.EnableKerberosOnInterfaceActivity(ctx, &models.Node{}, "svm-uuid", "svm-name", "lif-name", "192.168.1.10", "server.example.com", "EXAMPLE.COM", ad)

	require.NoError(t, err)
	mockClient.AssertExpectations(t)
	mockNas.AssertExpectations(t)
}

func TestEnableKerberosOnInterfaceActivity_NoInterfaceFound(t *testing.T) {
	ctx := context.Background()
	mockClient := new(ontapRest.MockRESTClient)
	mockNas := new(ontapRest.MockNASClient)
	mockClient.On("NAS").Return(mockNas).Once()

	mockNas.On("KerberosInterfaceCollectionGet", mock.Anything).Return([]*ontapRest.KerberosInterface{}, nil).Once()

	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{})
	defer cleanup()

	activity := ActiveDirectoryActivity{}
	ad := &vsa.ActiveDirectory{
		Password:           log.Secret("encrypted"),
		Username:           "admin",
		OrganizationalUnit: "OU=test",
	}
	err := activity.EnableKerberosOnInterfaceActivity(ctx, &models.Node{}, "svm-uuid", "svm-name", "lif-name", "192.168.1.10", "server.example.com", "EXAMPLE.COM", ad)

	require.Error(t, err)
	var appErr1 *temporal.ApplicationError
	if assert.True(t, errors.As(err, &appErr1)) {
		var tid int
		var origMsg string
		if assert.NoError(t, appErr1.Details(&tid, &origMsg)) {
			assert.Equal(t, vsaerrors.ErrKerberosUnclassified, tid)
			assert.Contains(t, origMsg, "no Kerberos interface found")
		}
	}
	mockClient.AssertExpectations(t)
	mockNas.AssertExpectations(t)
}

func TestEnableKerberosOnInterfaceActivity_NoInterfaceUUID(t *testing.T) {
	ctx := context.Background()
	mockClient := new(ontapRest.MockRESTClient)
	mockNas := new(ontapRest.MockNASClient)
	mockClient.On("NAS").Return(mockNas).Once()

	enabled := false
	kerberosInterface := &ontapRest.KerberosInterface{
		KerberosInterface: ontaprestmodels.KerberosInterface{
			Enabled:   &enabled,
			Interface: nil, // No interface UUID
		},
	}
	mockNas.On("KerberosInterfaceCollectionGet", mock.Anything).Return([]*ontapRest.KerberosInterface{kerberosInterface}, nil).Once()

	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{})
	defer cleanup()

	activity := ActiveDirectoryActivity{}
	ad := &vsa.ActiveDirectory{
		Password:           log.Secret("encrypted"),
		Username:           "admin",
		OrganizationalUnit: "OU=test",
	}
	err := activity.EnableKerberosOnInterfaceActivity(ctx, &models.Node{}, "svm-uuid", "svm-name", "lif-name", "192.168.1.10", "server.example.com", "EXAMPLE.COM", ad)

	require.Error(t, err)
	var appErr2 *temporal.ApplicationError
	if assert.True(t, errors.As(err, &appErr2)) {
		var tid int
		var origMsg string
		if assert.NoError(t, appErr2.Details(&tid, &origMsg)) {
			assert.Equal(t, vsaerrors.ErrKerberosUnclassified, tid)
			assert.Contains(t, origMsg, "interface UUID not found")
		}
	}
	mockClient.AssertExpectations(t)
	mockNas.AssertExpectations(t)
}

func TestEnableKerberosOnInterfaceActivity_DecryptPasswordError(t *testing.T) {
	ctx := context.Background()

	// Mock password decryption to fail
	originalDecryptPassword := utils.DecryptPassword
	utils.DecryptPassword = func(password log.Secret) (*string, error) {
		return nil, errors.New("decrypt failed")
	}
	defer func() { utils.DecryptPassword = originalDecryptPassword }()

	mockClient := new(ontapRest.MockRESTClient)
	mockNas := new(ontapRest.MockNASClient)
	mockClient.On("NAS").Return(mockNas).Once()

	interfaceUUID := "interface-uuid"
	enabled := false
	kerberosInterface := &ontapRest.KerberosInterface{
		KerberosInterface: ontaprestmodels.KerberosInterface{
			Enabled: &enabled,
			Interface: &ontaprestmodels.KerberosInterfaceInlineInterface{
				UUID: &interfaceUUID,
			},
		},
	}
	mockNas.On("KerberosInterfaceCollectionGet", mock.Anything).Return([]*ontapRest.KerberosInterface{kerberosInterface}, nil).Once()

	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{})
	defer cleanup()

	activity := ActiveDirectoryActivity{}
	ad := &vsa.ActiveDirectory{
		Password:           log.Secret("encrypted"),
		Username:           "admin",
		OrganizationalUnit: "OU=test",
	}
	err := activity.EnableKerberosOnInterfaceActivity(ctx, &models.Node{}, "svm-uuid", "svm-name", "lif-name", "192.168.1.10", "server.example.com", "EXAMPLE.COM", ad)

	require.Error(t, err)
	var appErr3 *temporal.ApplicationError
	if assert.True(t, errors.As(err, &appErr3)) {
		var tid int
		var origMsg string
		if assert.NoError(t, appErr3.Details(&tid, &origMsg)) {
			assert.Equal(t, vsaerrors.ErrKerberosUnclassified, tid)
			assert.Contains(t, origMsg, "failed to decrypt AD password")
		}
	}
	mockClient.AssertExpectations(t)
	mockNas.AssertExpectations(t)
}

func TestEnableKerberosOnInterfaceActivity_AlreadyEnabledError(t *testing.T) {
	ctx := context.Background()

	// Mock password decryption
	originalDecryptPassword := utils.DecryptPassword
	decryptedPassword := "decrypted-password"
	utils.DecryptPassword = func(password log.Secret) (*string, error) {
		return &decryptedPassword, nil
	}
	defer func() { utils.DecryptPassword = originalDecryptPassword }()

	mockClient := new(ontapRest.MockRESTClient)
	mockNas := new(ontapRest.MockNASClient)
	mockClient.On("NAS").Return(mockNas).Times(2)

	interfaceUUID := "interface-uuid"
	enabled := false
	kerberosInterface := &ontapRest.KerberosInterface{
		KerberosInterface: ontaprestmodels.KerberosInterface{
			Enabled: &enabled,
			Interface: &ontaprestmodels.KerberosInterfaceInlineInterface{
				UUID: &interfaceUUID,
			},
		},
	}
	mockNas.On("KerberosInterfaceCollectionGet", mock.Anything).Return([]*ontapRest.KerberosInterface{kerberosInterface}, nil).Once()
	// Return error indicating already enabled
	mockNas.On("KerberosInterfaceModify", mock.Anything).Return(errors.New("Kerberos is already enabled")).Once()

	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{})
	defer cleanup()

	activity := ActiveDirectoryActivity{}
	ad := &vsa.ActiveDirectory{
		Password:           log.Secret("encrypted"),
		Username:           "admin",
		OrganizationalUnit: "OU=test",
	}
	err := activity.EnableKerberosOnInterfaceActivity(ctx, &models.Node{}, "svm-uuid", "svm-name", "lif-name", "192.168.1.10", "server.example.com", "EXAMPLE.COM", ad)

	require.NoError(t, err) // Should handle "already enabled" gracefully
	mockClient.AssertExpectations(t)
	mockNas.AssertExpectations(t)
}

func TestEnableKerberosOnInterfaceActivity_ModifyError(t *testing.T) {
	ctx := context.Background()

	// Mock password decryption
	originalDecryptPassword := utils.DecryptPassword
	decryptedPassword := "decrypted-password"
	utils.DecryptPassword = func(password log.Secret) (*string, error) {
		return &decryptedPassword, nil
	}
	defer func() { utils.DecryptPassword = originalDecryptPassword }()

	mockClient := new(ontapRest.MockRESTClient)
	mockNas := new(ontapRest.MockNASClient)
	mockClient.On("NAS").Return(mockNas).Times(2)

	interfaceUUID := "interface-uuid"
	enabled := false
	kerberosInterface := &ontapRest.KerberosInterface{
		KerberosInterface: ontaprestmodels.KerberosInterface{
			Enabled: &enabled,
			Interface: &ontaprestmodels.KerberosInterfaceInlineInterface{
				UUID: &interfaceUUID,
			},
		},
	}
	mockNas.On("KerberosInterfaceCollectionGet", mock.Anything).Return([]*ontapRest.KerberosInterface{kerberosInterface}, nil).Once()
	mockNas.On("KerberosInterfaceModify", mock.Anything).Return(errors.New("modify failed")).Once()

	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{})
	defer cleanup()

	activity := ActiveDirectoryActivity{}
	ad := &vsa.ActiveDirectory{
		Password:           log.Secret("encrypted"),
		Username:           "admin",
		OrganizationalUnit: "OU=test",
	}
	err := activity.EnableKerberosOnInterfaceActivity(ctx, &models.Node{}, "svm-uuid", "svm-name", "lif-name", "192.168.1.10", "server.example.com", "EXAMPLE.COM", ad)

	require.Error(t, err)
	mockClient.AssertExpectations(t)
	mockNas.AssertExpectations(t)
}

func TestCreateKerberosRealmActivity_GetOntapProviderError(t *testing.T) {
	ctx := context.Background()
	originalGetter := getOntapRestProvider
	defer func() { getOntapRestProvider = originalGetter }()

	getOntapRestProvider = func(context.Context, *models.Node) (*vsa.OntapRestProvider, error) {
		return nil, errors.New("provider unavailable")
	}

	activity := ActiveDirectoryActivity{}
	err := activity.CreateKerberosRealmActivity(ctx, &models.Node{}, "svm-uuid", vsa.KerberosRealmCreateParams{})

	require.Error(t, err)
	var appErr *temporal.ApplicationError
	if assert.True(t, errors.As(err, &appErr)) {
		var tid int
		var origMsg string
		if assert.NoError(t, appErr.Details(&tid, &origMsg)) {
			assert.Equal(t, vsaerrors.ErrKerberosUnclassified, tid)
			assert.Contains(t, origMsg, "provider unavailable")
		}
	}
}

func TestEnableKerberosOnInterfaceActivity_GetOntapProviderError(t *testing.T) {
	ctx := context.Background()
	originalGetter := getOntapRestProvider
	defer func() { getOntapRestProvider = originalGetter }()

	getOntapRestProvider = func(context.Context, *models.Node) (*vsa.OntapRestProvider, error) {
		return nil, errors.New("provider unavailable")
	}

	activity := ActiveDirectoryActivity{}
	ad := &vsa.ActiveDirectory{Password: log.Secret("pw"), Username: "admin"}
	err := activity.EnableKerberosOnInterfaceActivity(ctx, &models.Node{}, "svm-uuid", "svm-name", "lif-name", "192.168.1.10", "server.example.com", "EXAMPLE.COM", ad)

	require.Error(t, err)
	var appErr *temporal.ApplicationError
	if assert.True(t, errors.As(err, &appErr)) {
		var tid int
		var origMsg string
		if assert.NoError(t, appErr.Details(&tid, &origMsg)) {
			assert.Equal(t, vsaerrors.ErrKerberosUnclassified, tid)
			assert.Contains(t, origMsg, "provider unavailable")
		}
	}
}

func TestEnableKerberosOnInterfaceActivity_GetKerberosInterfacesError(t *testing.T) {
	ctx := context.Background()
	mockClient := new(ontapRest.MockRESTClient)
	mockNas := new(ontapRest.MockNASClient)
	mockClient.On("NAS").Return(mockNas).Once()

	mockNas.On("KerberosInterfaceCollectionGet", mock.Anything).Return(nil, errors.New("interfaces unavailable")).Once()

	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{})
	defer cleanup()

	activity := ActiveDirectoryActivity{}
	ad := &vsa.ActiveDirectory{Password: log.Secret("pw"), Username: "admin"}
	err := activity.EnableKerberosOnInterfaceActivity(ctx, &models.Node{}, "svm-uuid", "svm-name", "lif-name", "192.168.1.10", "server.example.com", "EXAMPLE.COM", ad)

	require.Error(t, err)
	var appErr *temporal.ApplicationError
	if assert.True(t, errors.As(err, &appErr)) {
		var tid int
		var origMsg string
		if assert.NoError(t, appErr.Details(&tid, &origMsg)) {
			assert.Equal(t, vsaerrors.ErrKerberosUnclassified, tid)
			assert.Contains(t, origMsg, "interfaces unavailable")
		}
	}
	mockClient.AssertExpectations(t)
	mockNas.AssertExpectations(t)
}
