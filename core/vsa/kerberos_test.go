package vsa

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	ontaprestmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	ontaprest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	utilerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

func TestGetUniqueMachineAccount(t *testing.T) {
	t.Run("short_fqdn", func(t *testing.T) {
		fqdn := "server.example.com"
		result := GetUniqueMachineAccount(fqdn)
		// "NFS-server.example.com" -> "NFS-server-example-com" -> "NFS-SERVER-EXAMPLE-COM" -> truncated to 15 chars
		expected := "NFS-SERVER-EXAM"
		assert.Equal(t, expected, result)
	})

	t.Run("long_fqdn", func(t *testing.T) {
		fqdn := "very-long-server-name.example.com"
		result := GetUniqueMachineAccount(fqdn)
		// Should be truncated to 15 characters
		assert.LessOrEqual(t, len(result), 15)
		assert.True(t, len(result) > 0)
	})

	t.Run("fqdn_with_special_chars", func(t *testing.T) {
		fqdn := "server.test.example.com"
		result := GetUniqueMachineAccount(fqdn)
		// Dots should be replaced with dashes
		assert.NotContains(t, result, ".")
		assert.Contains(t, result, "NFS")
	})
}

func TestCreateNameMappingForKerberos_Success(t *testing.T) {
	mockClient := new(ontaprest.MockRESTClient)
	mockNameSvc := new(ontaprest.MockNameServicesClient)
	mockClient.On("NameServices").Return(mockNameSvc).Times(3)

	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return mockClient, nil
	}

	// First call - check if mapping exists (returns empty)
	mockNameSvc.On("NameMappingCollectionGet", mock.MatchedBy(func(params *ontaprest.NameMappingCollectionGetParams) bool {
		return params.Pattern != nil && *params.Pattern == "(.+)\\$@EXAMPLE.COM"
	})).Return([]*ontaprest.NameMapping{}, utilerrors.NewNotFoundErr("name mapping", nil)).Once()

	// Second call - get all mappings to find index
	mockNameSvc.On("NameMappingCollectionGet", mock.MatchedBy(func(params *ontaprest.NameMappingCollectionGetParams) bool {
		return params.Direction != nil && *params.Direction == "krb-unix"
	})).Return([]*ontaprest.NameMapping{}, nil).Once()

	// Create name mapping
	mockNameSvc.On("NameMappingCreate", mock.Anything).Return(nil).Once()

	rc := &OntapRestProvider{}
	err := rc.CreateNameMappingForKerberos("svm-uuid", "example.com")

	require.NoError(t, err)
	mockClient.AssertExpectations(t)
	mockNameSvc.AssertExpectations(t)
}

func TestCreateNameMappingForKerberos_MappingAlreadyExists(t *testing.T) {
	mockClient := new(ontaprest.MockRESTClient)
	mockNameSvc := new(ontaprest.MockNameServicesClient)
	mockClient.On("NameServices").Return(mockNameSvc).Once()

	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return mockClient, nil
	}

	pattern := "(.+)\\$@EXAMPLE.COM"
	direction := "krb-unix"
	index := int64(1)
	existingMapping := &ontaprest.NameMapping{
		NameMapping: ontaprestmodels.NameMapping{
			Pattern:   &pattern,
			Direction: &direction,
			Index:     &index,
		},
	}
	mockNameSvc.On("NameMappingCollectionGet", mock.Anything).Return([]*ontaprest.NameMapping{existingMapping}, nil).Once()

	rc := &OntapRestProvider{}
	err := rc.CreateNameMappingForKerberos("svm-uuid", "example.com")

	require.NoError(t, err)
	mockClient.AssertExpectations(t)
	mockNameSvc.AssertExpectations(t)
	mockNameSvc.AssertNotCalled(t, "NameMappingCreate", mock.Anything)
}

func TestCreateNameMappingForKerberos_FindIndex(t *testing.T) {
	mockClient := new(ontaprest.MockRESTClient)
	mockNameSvc := new(ontaprest.MockNameServicesClient)
	mockClient.On("NameServices").Return(mockNameSvc).Times(3)

	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return mockClient, nil
	}

	mockNameSvc.On("NameMappingCollectionGet", mock.Anything).Return([]*ontaprest.NameMapping{}, utilerrors.NewNotFoundErr("name mapping", nil)).Once()

	// Return existing mappings with indices
	index1 := int64(1)
	index2 := int64(3)
	existingMappings := []*ontaprest.NameMapping{
		{NameMapping: ontaprestmodels.NameMapping{Index: &index1}},
		{NameMapping: ontaprestmodels.NameMapping{Index: &index2}},
	}
	mockNameSvc.On("NameMappingCollectionGet", mock.Anything).Return(existingMappings, nil).Once()

	// Should create with index 4 (max index + 1)
	mockNameSvc.On("NameMappingCreate", mock.MatchedBy(func(params *ontaprest.NameMappingCreateParams) bool {
		return params.Index == 4
	})).Return(nil).Once()

	rc := &OntapRestProvider{}
	err := rc.CreateNameMappingForKerberos("svm-uuid", "example.com")

	require.NoError(t, err)
	mockClient.AssertExpectations(t)
	mockNameSvc.AssertExpectations(t)
}

func TestCreateNameMappingForKerberos_Error(t *testing.T) {
	mockClient := new(ontaprest.MockRESTClient)
	mockNameSvc := new(ontaprest.MockNameServicesClient)
	mockClient.On("NameServices").Return(mockNameSvc).Times(3)

	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return mockClient, nil
	}

	mockNameSvc.On("NameMappingCollectionGet", mock.Anything).Return([]*ontaprest.NameMapping{}, utilerrors.NewNotFoundErr("name mapping", nil)).Once()
	mockNameSvc.On("NameMappingCollectionGet", mock.Anything).Return([]*ontaprest.NameMapping{}, nil).Once()
	mockNameSvc.On("NameMappingCreate", mock.Anything).Return(errors.New("create failed")).Once()

	rc := &OntapRestProvider{}
	err := rc.CreateNameMappingForKerberos("svm-uuid", "example.com")

	require.Error(t, err)
	mockClient.AssertExpectations(t)
	mockNameSvc.AssertExpectations(t)
}

func TestDoesKerberosRealmExist_Success_Exists(t *testing.T) {
	mockClient := new(ontaprest.MockRESTClient)
	mockNas := new(ontaprest.MockNASClient)
	mockClient.On("NAS").Return(mockNas).Once()

	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return mockClient, nil
	}

	realmStr := "EXAMPLE.COM"
	realmObj := &ontaprest.KerberosRealm{
		KerberosRealm: ontaprestmodels.KerberosRealm{
			Name: &realmStr,
		},
	}
	mockNas.On("KerberosRealmGet", mock.Anything).Return([]*ontaprest.KerberosRealm{realmObj}, nil).Once()

	rc := &OntapRestProvider{}
	exists, err := rc.DoesKerberosRealmExist("svm-uuid", "EXAMPLE.COM")

	require.NoError(t, err)
	assert.True(t, exists)
	mockClient.AssertExpectations(t)
	mockNas.AssertExpectations(t)
}

func TestDoesKerberosRealmExist_Success_NotExists(t *testing.T) {
	mockClient := new(ontaprest.MockRESTClient)
	mockNas := new(ontaprest.MockNASClient)
	mockClient.On("NAS").Return(mockNas).Once()

	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return mockClient, nil
	}

	mockNas.On("KerberosRealmGet", mock.Anything).Return([]*ontaprest.KerberosRealm{}, utilerrors.NewNotFoundErr("realm", nil)).Once()

	rc := &OntapRestProvider{}
	exists, err := rc.DoesKerberosRealmExist("svm-uuid", "EXAMPLE.COM")

	require.NoError(t, err)
	assert.False(t, exists)
	mockClient.AssertExpectations(t)
	mockNas.AssertExpectations(t)
}

func TestDoesKerberosRealmExist_Error(t *testing.T) {
	mockClient := new(ontaprest.MockRESTClient)
	mockNas := new(ontaprest.MockNASClient)
	mockClient.On("NAS").Return(mockNas).Once()

	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return mockClient, nil
	}

	mockNas.On("KerberosRealmGet", mock.Anything).Return(nil, errors.New("get failed")).Once()

	rc := &OntapRestProvider{}
	exists, err := rc.DoesKerberosRealmExist("svm-uuid", "EXAMPLE.COM")

	require.Error(t, err)
	assert.False(t, exists)
	mockClient.AssertExpectations(t)
	mockNas.AssertExpectations(t)
}

func TestCreateKerberosRealm_Success(t *testing.T) {
	mockClient := new(ontaprest.MockRESTClient)
	mockNas := new(ontaprest.MockNASClient)
	mockClient.On("NAS").Return(mockNas).Once()

	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return mockClient, nil
	}

	realmKdcPort := RealmKdcPort
	realmClockSkew := RealmClockSkew
	realmKdcVendor := RealmKdcVendor
	realmAdminServerPort := RealmAdminServerPort
	realmPasswordServerPort := RealmPasswordServerPort
	adminIP := "192.168.1.1"
	adName := "ad-server"
	params := KerberosRealmCreateParams{
		Realm:              "EXAMPLE.COM",
		KdcIP:              "192.168.1.1",
		RealmKDCPort:       &realmKdcPort,
		RealmClockSkew:     &realmClockSkew,
		RealmKDCVendor:     &realmKdcVendor,
		AdminServerIP:      &adminIP,
		AdminServerPort:    &realmAdminServerPort,
		PasswordServerIP:   &adminIP,
		PasswordServerPort: &realmPasswordServerPort,
		ADServerIP:         &adminIP,
		ADServerName:       &adName,
		AdName:             adName,
		SvmUUID:            "svm-uuid",
	}

	mockNas.On("KerberosRealmCreate", mock.Anything).Return(nil).Once()

	rc := &OntapRestProvider{}
	err := rc.CreateKerberosRealm(params)

	require.NoError(t, err)
	mockClient.AssertExpectations(t)
	mockNas.AssertExpectations(t)
}

func TestCreateKerberosRealm_WithDefaults(t *testing.T) {
	mockClient := new(ontaprest.MockRESTClient)
	mockNas := new(ontaprest.MockNASClient)
	mockClient.On("NAS").Return(mockNas).Once()

	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return mockClient, nil
	}

	params := KerberosRealmCreateParams{
		Realm:   "EXAMPLE.COM",
		KdcIP:   "192.168.1.1",
		AdName:  "ad-server",
		SvmUUID: "svm-uuid",
		// No optional fields set, should use defaults
	}

	mockNas.On("KerberosRealmCreate", mock.MatchedBy(func(params *ontaprest.KerberosRealmCreateParams) bool {
		return params.RealmKDCPort != nil && *params.RealmKDCPort == RealmKdcPort &&
			params.RealmClockSkew != nil && *params.RealmClockSkew == RealmClockSkew &&
			params.RealmKDCVendor != nil && *params.RealmKDCVendor == RealmKdcVendor
	})).Return(nil).Once()

	rc := &OntapRestProvider{}
	err := rc.CreateKerberosRealm(params)

	require.NoError(t, err)
	mockClient.AssertExpectations(t)
	mockNas.AssertExpectations(t)
}

func TestCreateKerberosRealm_Error(t *testing.T) {
	mockClient := new(ontaprest.MockRESTClient)
	mockNas := new(ontaprest.MockNASClient)
	mockClient.On("NAS").Return(mockNas).Once()

	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return mockClient, nil
	}

	params := KerberosRealmCreateParams{
		Realm:   "EXAMPLE.COM",
		KdcIP:   "192.168.1.1",
		AdName:  "ad-server",
		SvmUUID: "svm-uuid",
	}

	mockNas.On("KerberosRealmCreate", mock.Anything).Return(errors.New("create failed")).Once()

	rc := &OntapRestProvider{}
	err := rc.CreateKerberosRealm(params)

	require.Error(t, err)
	mockClient.AssertExpectations(t)
	mockNas.AssertExpectations(t)
}

func TestGetKerberosInterfaces_Success(t *testing.T) {
	mockClient := new(ontaprest.MockRESTClient)
	mockNas := new(ontaprest.MockNASClient)
	mockClient.On("NAS").Return(mockNas).Once()

	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return mockClient, nil
	}

	interfaceName := "test-interface"
	enabled := true
	interfaces := []*ontaprest.KerberosInterface{
		{
			KerberosInterface: ontaprestmodels.KerberosInterface{
				Enabled: &enabled,
			},
		},
	}
	mockNas.On("KerberosInterfaceCollectionGet", mock.Anything).Return(interfaces, nil).Once()

	rc := &OntapRestProvider{}
	result, err := rc.GetKerberosInterfaces("svm-uuid", "svm-name", interfaceName)

	require.NoError(t, err)
	require.Len(t, result, 1)
	mockClient.AssertExpectations(t)
	mockNas.AssertExpectations(t)
}

func TestGetKerberosInterfaces_Error(t *testing.T) {
	mockClient := new(ontaprest.MockRESTClient)
	mockNas := new(ontaprest.MockNASClient)
	mockClient.On("NAS").Return(mockNas).Once()

	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return mockClient, nil
	}

	mockNas.On("KerberosInterfaceCollectionGet", mock.Anything).Return(nil, errors.New("get failed")).Once()

	rc := &OntapRestProvider{}
	result, err := rc.GetKerberosInterfaces("svm-uuid", "svm-name", "interface-name")

	require.Error(t, err)
	assert.Nil(t, result)
	mockClient.AssertExpectations(t)
	mockNas.AssertExpectations(t)
}

func TestEnableKerberosOnInterface_Success(t *testing.T) {
	mockClient := new(ontaprest.MockRESTClient)
	mockNas := new(ontaprest.MockNASClient)
	mockClient.On("NAS").Return(mockNas).Once()

	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return mockClient, nil
	}

	params := KerberosInterfaceModifyParams{
		SvmUUID:        "svm-uuid",
		InterfaceUUID:  "interface-uuid",
		Spn:            "nfs/server.example.com@EXAMPLE.COM",
		MachineAccount: "NFS-SERVER",
		AdminUsername:  "admin",
		AdminPassword:  "password",
		OU:             "OU=test",
	}

	mockNas.On("KerberosInterfaceModify", mock.MatchedBy(func(p *ontaprest.KerberosInterfaceModifyParams) bool {
		return p.SvmUUID == "svm-uuid" &&
			p.InterfaceUUID != nil && *p.InterfaceUUID == "interface-uuid" &&
			p.IsKerberosEnabled != nil && *p.IsKerberosEnabled == true &&
			p.Spn != nil && *p.Spn == params.Spn
	})).Return(nil).Once()

	rc := &OntapRestProvider{}
	err := rc.EnableKerberosOnInterface(params)

	require.NoError(t, err)
	mockClient.AssertExpectations(t)
	mockNas.AssertExpectations(t)
}

func TestEnableKerberosOnInterface_Error(t *testing.T) {
	mockClient := new(ontaprest.MockRESTClient)
	mockNas := new(ontaprest.MockNASClient)
	mockClient.On("NAS").Return(mockNas).Once()

	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return mockClient, nil
	}

	params := KerberosInterfaceModifyParams{
		SvmUUID:        "svm-uuid",
		InterfaceUUID:  "interface-uuid",
		Spn:            "nfs/server.example.com@EXAMPLE.COM",
		MachineAccount: "NFS-SERVER",
		AdminUsername:  "admin",
		AdminPassword:  "password",
		OU:             "OU=test",
	}

	mockNas.On("KerberosInterfaceModify", mock.Anything).Return(errors.New("modify failed")).Once()

	rc := &OntapRestProvider{}
	err := rc.EnableKerberosOnInterface(params)

	require.Error(t, err)
	mockClient.AssertExpectations(t)
	mockNas.AssertExpectations(t)
}

func TestGetDataLifsForSVM_Success(t *testing.T) {
	mockClient := new(ontaprest.MockRESTClient)
	mockNetworking := new(ontaprest.MockNetworkingClient)
	mockClient.On("Networking").Return(mockNetworking).Once()

	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return mockClient, nil
	}

	lifName := "test-lif"
	lifIPStr := "192.168.1.10"
	lifIP := ontaprestmodels.IPAddress(lifIPStr)
	lif := &ontaprest.IPInterface{
		IPInterface: ontaprestmodels.IPInterface{
			Name: &lifName,
			IP: &ontaprestmodels.IPInfo{
				Address: &lifIP,
			},
		},
	}

	mockNetworking.On("NetworkIPInterfacesGet", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		callback := args.Get(1).(ontaprest.UserCallbackFunc[[]*ontaprest.IPInterface])
		err := callback([]*ontaprest.IPInterface{lif})
		if err != nil {
			return
		}
	}).Return(nil).Once()

	rc := &OntapRestProvider{}
	result, err := rc.GetDataLifsForSVM("svm-uuid", "svm-name")

	require.NoError(t, err)
	require.Len(t, result, 1)
	assert.Equal(t, lifName, *result[0].Name)
	mockClient.AssertExpectations(t)
	mockNetworking.AssertExpectations(t)
}

func TestGetDataLifsForSVM_MultipleLIFs(t *testing.T) {
	mockClient := new(ontaprest.MockRESTClient)
	mockNetworking := new(ontaprest.MockNetworkingClient)
	mockClient.On("Networking").Return(mockNetworking).Once()

	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return mockClient, nil
	}

	lifName1 := "lif1"
	lifName2 := "lif2"
	lifIP1Str := "192.168.1.10"
	lifIP2Str := "192.168.1.11"
	lifIP1 := ontaprestmodels.IPAddress(lifIP1Str)
	lifIP2 := ontaprestmodels.IPAddress(lifIP2Str)
	lifs := []*ontaprest.IPInterface{
		{
			IPInterface: ontaprestmodels.IPInterface{
				Name: &lifName1,
				IP: &ontaprestmodels.IPInfo{
					Address: &lifIP1,
				},
			},
		},
		{
			IPInterface: ontaprestmodels.IPInterface{
				Name: &lifName2,
				IP: &ontaprestmodels.IPInfo{
					Address: &lifIP2,
				},
			},
		},
	}

	mockNetworking.On("NetworkIPInterfacesGet", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		callback := args.Get(1).(ontaprest.UserCallbackFunc[[]*ontaprest.IPInterface])
		err := callback(lifs)
		if err != nil {
			return
		}
	}).Return(nil).Once()

	rc := &OntapRestProvider{}
	result, err := rc.GetDataLifsForSVM("svm-uuid", "svm-name")

	require.NoError(t, err)
	require.Len(t, result, 2)
	mockClient.AssertExpectations(t)
	mockNetworking.AssertExpectations(t)
}

func TestGetDataLifsForSVM_Error(t *testing.T) {
	mockClient := new(ontaprest.MockRESTClient)
	mockNetworking := new(ontaprest.MockNetworkingClient)
	mockClient.On("Networking").Return(mockNetworking).Once()

	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return mockClient, nil
	}

	mockNetworking.On("NetworkIPInterfacesGet", mock.Anything, mock.Anything).Return(errors.New("get failed")).Once()

	rc := &OntapRestProvider{}
	result, err := rc.GetDataLifsForSVM("svm-uuid", "svm-name")

	require.Error(t, err)
	assert.Nil(t, result)
	mockClient.AssertExpectations(t)
	mockNetworking.AssertExpectations(t)
}

func TestGetDataLifsForSVM_GetClientError(t *testing.T) {
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return nil, errors.New("client error")
	}

	rc := &OntapRestProvider{}
	result, err := rc.GetDataLifsForSVM("svm-uuid", "svm-name")

	require.Error(t, err)
	assert.Nil(t, result)
}
