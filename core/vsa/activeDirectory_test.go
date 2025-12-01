package vsa

import (
	stdErrors "errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	ontapModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	log "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func newTestProvider() *OntapRestProvider {
	return &OntapRestProvider{Logger: log.NewLogger()}
}

func withMockOntapClient(t *testing.T, client ontapRest.RESTClient, err error, fn func()) {
	t.Helper()
	original := getOntapClientFunc
	t.Cleanup(func() { getOntapClientFunc = original })
	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return client, err
	}
	fn()
}

func TestDiscoveryModeConstants(t *testing.T) {
	tests := []struct {
		name     string
		mode     DiscoveryMode
		expected string
	}{
		{
			name:     "DiscoveryModeAll",
			mode:     DiscoveryModeAll,
			expected: "all",
		},
		{
			name:     "DiscoveryModeSite",
			mode:     DiscoveryModeSite,
			expected: "site",
		},
		{
			name:     "DiscoveryModeNone",
			mode:     DiscoveryModeNone,
			expected: "none",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, string(tt.mode), "DiscoveryMode should have correct string value")
		})
	}
}

func TestDiscoveryModeType(t *testing.T) {
	// Test that DiscoveryMode type can be used
	var mode DiscoveryMode
	mode = DiscoveryModeAll
	assert.Equal(t, DiscoveryMode("all"), mode)

	mode = DiscoveryModeSite
	assert.Equal(t, DiscoveryMode("site"), mode)

	mode = DiscoveryModeNone
	assert.Equal(t, DiscoveryMode("none"), mode)

	// Test type conversion
	assert.Equal(t, "all", string(DiscoveryModeAll))
	assert.Equal(t, "site", string(DiscoveryModeSite))
	assert.Equal(t, "none", string(DiscoveryModeNone))
}

func TestCreateOrModifyDNS_CreatesWhenMissing(t *testing.T) {
	provider := newTestProvider()

	mockREST := &ontapRest.MockRESTClient{}
	mockNS := &ontapRest.MockNameServicesClient{}
	mockREST.On("NameServices").Return(mockNS)
	mockNS.On("DNSGet", mock.Anything).Return((*ontapRest.DNS)(nil), nil)
	mockNS.On("DnsCreate", mock.MatchedBy(func(params *ontapRest.DNSCreateParams) bool {
		return params.SvmUUID == "svm-uuid" &&
			assert.ElementsMatch(t, []string{"example.com"}, params.Domains) &&
			assert.ElementsMatch(t, []string{"1.1.1.1", "2.2.2.2"}, params.DNSServers)
	})).Return(&ontapModels.DNSResponse{}, nil).Once()

	withMockOntapClient(t, mockREST, nil, func() {
		err := createOrModifyDNS(provider, "svm-uuid", []string{"example.com"}, []string{"1.1.1.1", "2.2.2.2"})
		require.NoError(t, err)
	})

	mockNS.AssertExpectations(t)
	mockREST.AssertExpectations(t)
}

func TestCreateOrModifyDNS_ModifiesWhenDifferent(t *testing.T) {
	provider := newTestProvider()

	mockREST := &ontapRest.MockRESTClient{}
	mockNS := &ontapRest.MockNameServicesClient{}
	mockREST.On("NameServices").Return(mockNS)

	existing := &ontapRest.DNS{DNS: ontapModels.DNS{
		Servers: ontapModels.NameServersArrayInline{nillable.ToPointer("1.1.1.1")},
		Domains: ontapModels.DNSDomainsArrayInline{nillable.ToPointer("legacy.com")},
	}}
	mockNS.On("DNSGet", mock.Anything).Return(existing, nil)
	mockNS.On("DNSModify", mock.MatchedBy(func(params *ontapRest.DNSModifyParams) bool {
		if params.SvmUUID != "svm-uuid" {
			return false
		}
		if !assert.ElementsMatch(t, []string{"example.com"}, params.Domains) {
			return false
		}
		if !assert.ElementsMatch(t, []string{"3.3.3.3"}, params.NameServers) {
			return false
		}
		return true
	})).Return(nil).Once()

	withMockOntapClient(t, mockREST, nil, func() {
		err := createOrModifyDNS(provider, "svm-uuid", []string{"example.com"}, []string{"3.3.3.3"})
		require.NoError(t, err)
	})

	mockNS.AssertExpectations(t)
	mockREST.AssertExpectations(t)
}

func TestCreateOrModifyDNS_NoOpWhenUnchanged(t *testing.T) {
	provider := newTestProvider()

	mockREST := &ontapRest.MockRESTClient{}
	mockNS := &ontapRest.MockNameServicesClient{}
	mockREST.On("NameServices").Return(mockNS)

	existing := &ontapRest.DNS{DNS: ontapModels.DNS{
		Servers: ontapModels.NameServersArrayInline{nillable.ToPointer("1.1.1.1"), nillable.ToPointer("2.2.2.2")},
		Domains: ontapModels.DNSDomainsArrayInline{nillable.ToPointer("example.com")},
	}}
	mockNS.On("DNSGet", mock.Anything).Return(existing, nil)

	withMockOntapClient(t, mockREST, nil, func() {
		err := createOrModifyDNS(provider, "svm-uuid", []string{"example.com"}, []string{"1.1.1.1", "2.2.2.2"})
		require.NoError(t, err)
	})

	mockNS.AssertNotCalled(t, "DNSModify", mock.Anything)
	mockNS.AssertNotCalled(t, "DnsCreate", mock.Anything)
}

func TestCreateOrModifyDNS_ReturnsErrorOnGetFailure(t *testing.T) {
	provider := newTestProvider()

	mockREST := &ontapRest.MockRESTClient{}
	mockNS := &ontapRest.MockNameServicesClient{}
	mockREST.On("NameServices").Return(mockNS)

	mockErr := stdErrors.New("transport failure")
	mockNS.On("DNSGet", mock.Anything).Return((*ontapRest.DNS)(nil), mockErr)

	withMockOntapClient(t, mockREST, nil, func() {
		err := createOrModifyDNS(provider, "svm-uuid", []string{"example.com"}, []string{"1.1.1.1"})
		require.ErrorIs(t, err, mockErr)
	})
}

func TestCreateOrModifyADDNS_NormalizesInput(t *testing.T) {
	provider := newTestProvider()
	domains := []string{"example.com"}
	servers := []string{"10.0.0.1", "10.0.0.2"}

	original := createOrModifyDNS
	t.Cleanup(func() { createOrModifyDNS = original })

	captured := struct {
		domains []string
		servers []string
	}{}

	createOrModifyDNS = func(rc *OntapRestProvider, svmUUID string, d, s []string) error {
		captured.domains = append([]string{}, d...)
		captured.servers = append([]string{}, s...)
		return nil
	}

	ad := &ActiveDirectory{Domain: "example.com", DNS: " 10.0.0.1 , 10.0.0.2 "}
	err := createOrModifyADDNS(provider, "svm-uuid", ad)
	require.NoError(t, err)
	assert.Equal(t, domains, captured.domains)
	assert.Equal(t, servers, captured.servers)
}

func TestEnsureCifsServerNamePostFix_PopulatesExistingEntry(t *testing.T) {
	logger := log.NewLogger()
	mockREST := &ontapRest.MockRESTClient{}
	mockNAS := &ontapRest.MockNASClient{}
	mockREST.On("NAS").Return(mockNAS)

	svmName := "svm1"
	svmUUID := "uuid1"
	netBios := "myserver"
	cifsName := "MYSERVER-abcd"
	mockNAS.On("CifsServiceList", mock.Anything).Return([]*ontapRest.CifsService{
		{CifsService: ontapModels.CifsService{
			Name: nillable.ToPointer(cifsName),
			Svm:  &ontapModels.CifsServiceInlineSvm{Name: nillable.ToPointer(svmName), UUID: nillable.ToPointer(svmUUID)},
		}},
	}, nil)

	ad := &ActiveDirectory{NetBIOS: netBios, CIFSServers: []*CIFSServer{{SVMName: svmName}}}

	err := ensureCifsServerNamePostFix(logger, mockREST, ad, svmName)
	require.NoError(t, err)
	require.Len(t, ad.CIFSServers, 1)
	assert.Equal(t, "abcd", ad.CIFSServers[0].ServerNamePostfix)
}

func TestEnsureCifsServerNamePostFix_AppendsMissingServers(t *testing.T) {
	logger := log.NewLogger()
	mockREST := &ontapRest.MockRESTClient{}
	mockNAS := &ontapRest.MockNASClient{}
	mockREST.On("NAS").Return(mockNAS)

	svmName := "svm2"
	svmUUID := "uuid2"
	cifsName := "MYSERVER-1234"
	mockNAS.On("CifsServiceList", mock.Anything).Return([]*ontapRest.CifsService{
		{CifsService: ontapModels.CifsService{
			Name: nillable.ToPointer(cifsName),
			Svm:  &ontapModels.CifsServiceInlineSvm{Name: nillable.ToPointer(svmName), UUID: nillable.ToPointer(svmUUID)},
		}},
	}, nil)

	ad := &ActiveDirectory{NetBIOS: "myserver"}
	err := ensureCifsServerNamePostFix(logger, mockREST, ad, svmName)
	require.NoError(t, err)
	require.Len(t, ad.CIFSServers, 1)
	assert.Equal(t, svmUUID, ad.CIFSServers[0].SVMUUID)
	assert.Equal(t, "1234", ad.CIFSServers[0].ServerNamePostfix)
}

func buildTestActiveDirectory() *ActiveDirectory {
	aes := nillable.ToPointer(true)
	sign := nillable.ToPointer(true)
	encrypt := nillable.ToPointer(true)
	ldapTLS := nillable.ToPointer(false)
	return &ActiveDirectory{
		Username:           "aduser",
		Password:           log.Secret("password"),
		Domain:             "example.com",
		DNS:                "1.1.1.1",
		NetBIOS:            "gcnvtest",
		OrganizationalUnit: "OU=Servers",
		Users: map[string][]string{
			"BUILTIN\\Administrators":                {"ops", "Administrator"},
			utils.ActiveDirectorySeSecurityPrivilege: {"secops"},
		},
		AesEncryption:        aes,
		LdapSigning:          sign,
		EncryptDCConnections: encrypt,
		LdapOverTLS:          ldapTLS,
	}
}

func TestCreateAndSetupCIFSServer_Success(t *testing.T) {
	tracelog := log.NewLogger()
	ad := buildTestActiveDirectory()
	mockREST := &ontapRest.MockRESTClient{}
	mockNAS := &ontapRest.MockNASClient{}
	mockREST.On("NAS").Return(mockNAS)
	mockNAS.On("CifsServiceAddMembers", mock.MatchedBy(func(params *ontapRest.CifsServiceModifyGroupMembersParams) bool {
		if params.SvmUUID != "svm-uuid" {
			return false
		}
		if params.Sid != "S-1-5-32-544" {
			return false
		}
		return assert.Equal(t, []string{"example.com\\ops"}, params.Members)
	})).Return(nil).Once()
	mockNAS.On("CifsDomainModify", mock.Anything).Return(nil).Once()

	originalGenerate := generateCIFSServerNamePostfix
	originalCreate := createCIFSServer
	originalSetup := cifsServerSetup
	originalAddSec := addSecurityPrivilegesForUser
	originalDDNS := ddnsModify
	originalDelete := deleteCIFSServer

	t.Cleanup(func() {
		generateCIFSServerNamePostfix = originalGenerate
		createCIFSServer = originalCreate
		cifsServerSetup = originalSetup
		addSecurityPrivilegesForUser = originalAddSec
		ddnsModify = originalDDNS
		deleteCIFSServer = originalDelete
	})

	generateCIFSServerNamePostfix = func(_ []*CIFSServer) string { return "abcd" }

	var createdName string
	createCIFSServer = func(_ log.Logger, api ontapRest.RESTClient, svmUUID, svmName, name string, _ *ActiveDirectory) error {
		require.Equal(t, "svm-uuid", svmUUID)
		require.Equal(t, "svmName", svmName)
		createdName = name
		return nil
	}

	cifsServerSetup = func(_ log.Logger, nas ontapRest.NASClient, svmUUID string) error {
		require.Equal(t, "svm-uuid", svmUUID)
		require.Equal(t, mockNAS, nas)
		return nil
	}

	var securityAssigned []string
	addSecurityPrivilegesForUser = func(_ ontapRest.RESTClient, user, svmUUID string) error {
		securityAssigned = append(securityAssigned, user)
		require.Equal(t, "svm-uuid", svmUUID)
		return nil
	}

	ddnsModify = func(_ ontapRest.RESTClient, svmUUID, fqdn string) error {
		require.Equal(t, "svm-uuid", svmUUID)
		require.Equal(t, "gcnvtest-abcd.example.com", fqdn)
		return nil
	}

	deleteCIFSServer = func(_ log.Logger, _ ontapRest.RESTClient, _ string, _ string, _ string) error {
		t.Fatalf("deleteCIFSServer should not be called on success")
		return nil
	}

	fqdn, err := createAndSetupCIFSServer(tracelog, mockREST, ad, "svm-uuid", "svmName")
	require.NoError(t, err)
	assert.Equal(t, "gcnvtest-abcd.example.com", fqdn)
	assert.Equal(t, "gcnvtest-abcd", createdName)
	assert.ElementsMatch(t, []string{"example.com\\secops"}, securityAssigned)
	require.Len(t, ad.CIFSServers, 1)
	assert.Equal(t, "abcd", ad.CIFSServers[0].ServerNamePostfix)
}

func TestCreateAndSetupCIFSServer_CleansUpOnMemberError(t *testing.T) {
	tracelog := log.NewLogger()
	ad := buildTestActiveDirectory()
	mockREST := &ontapRest.MockRESTClient{}
	mockNAS := &ontapRest.MockNASClient{}
	mockREST.On("NAS").Return(mockNAS)

	originalGenerate := generateCIFSServerNamePostfix
	originalCreate := createCIFSServer
	originalSetup := cifsServerSetup
	originalDelete := deleteCIFSServer

	t.Cleanup(func() {
		generateCIFSServerNamePostfix = originalGenerate
		createCIFSServer = originalCreate
		cifsServerSetup = originalSetup
		deleteCIFSServer = originalDelete
	})

	generateCIFSServerNamePostfix = func(_ []*CIFSServer) string { return "efgh" }
	createCIFSServer = func(_ log.Logger, _ ontapRest.RESTClient, _, _, _ string, _ *ActiveDirectory) error { return nil }
	cifsServerSetup = func(_ log.Logger, _ ontapRest.NASClient, _ string) error { return nil }

	mockNAS.On("CifsServiceAddMembers", mock.Anything).Return(fmt.Errorf("boom")).Once()
	// privilege calls may occur for SeSecurityPrivilege group; make them succeed
	mockNAS.On("CifsServiceAddSecurityPrivilege", mock.Anything).Return(nil).Maybe()

	deleteCalled := false
	deleteCIFSServer = func(_ log.Logger, _ ontapRest.RESTClient, svmUUID, username, password string) error {
		deleteCalled = true
		require.Equal(t, "svm-uuid", svmUUID)
		require.Equal(t, "aduser", username)
		require.Equal(t, ad.Password.String(), password)
		return nil
	}

	_, err := createAndSetupCIFSServer(tracelog, mockREST, ad, "svm-uuid", "svmName")
	require.Error(t, err)
	assert.True(t, deleteCalled)
}

func TestCreateAndSetupCIFSServer_LimitExceeded(t *testing.T) {
	ad := buildTestActiveDirectory()
	// Pre-populate 1024 servers to trigger limit check
	for i := 0; i < 1024; i++ {
		ad.CIFSServers = append(ad.CIFSServers, &CIFSServer{ServerNamePostfix: fmt.Sprintf("%04x", i)})
	}
	_, err := createAndSetupCIFSServer(log.NewLogger(), &ontapRest.MockRESTClient{}, ad, "svm-uuid", "svmName")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "limit exceeded")
}

func TestCreateAndSetupCIFSServer_DDNSModifyFailure(t *testing.T) {
	tracelog := log.NewLogger()
	ad := buildTestActiveDirectory()
	mockREST := &ontapRest.MockRESTClient{}
	mockNAS := &ontapRest.MockNASClient{}
	mockREST.On("NAS").Return(mockNAS)
	mockNAS.On("CifsServiceAddMembers", mock.Anything).Return(nil)
	mockNAS.On("CifsDomainModify", mock.Anything).Return(nil)
	// Expect security privilege assignment
	mockNAS.On("CifsServiceAddSecurityPrivilege", mock.Anything).Return(nil)

	originalGenerate := generateCIFSServerNamePostfix
	originalCreate := createCIFSServer
	originalSetup := cifsServerSetup
	originalDDNS := ddnsModify
	t.Cleanup(func() {
		generateCIFSServerNamePostfix = originalGenerate
		createCIFSServer = originalCreate
		cifsServerSetup = originalSetup
		ddnsModify = originalDDNS
	})
	generateCIFSServerNamePostfix = func(_ []*CIFSServer) string { return "abcd" }
	createCIFSServer = func(_ log.Logger, _ ontapRest.RESTClient, _, _, _ string, _ *ActiveDirectory) error { return nil }
	cifsServerSetup = func(_ log.Logger, _ ontapRest.NASClient, _ string) error { return nil }
	ddnsModify = func(_ ontapRest.RESTClient, _, _ string) error { return fmt.Errorf("ddns fail") }

	_, err := createAndSetupCIFSServer(tracelog, mockREST, ad, "svm-uuid", "svmName")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ddns fail")
}

func TestCreateAndSetupCIFSServer_AddSecurityPrivilegeFailure(t *testing.T) {
	tracelog := log.NewLogger()
	ad := buildTestActiveDirectory()
	ad.Users[utils.ActiveDirectorySeSecurityPrivilege] = []string{"secops"}
	mockREST := &ontapRest.MockRESTClient{}
	mockNAS := &ontapRest.MockNASClient{}
	mockREST.On("NAS").Return(mockNAS)
	mockNAS.On("CifsServiceAddMembers", mock.Anything).Return(nil)
	mockNAS.On("CifsDomainModify", mock.Anything).Return(nil)

	originalGenerate := generateCIFSServerNamePostfix
	originalCreate := createCIFSServer
	originalSetup := cifsServerSetup
	originalAddSec := addSecurityPrivilegesForUser
	originalDelete := deleteCIFSServer
	t.Cleanup(func() {
		generateCIFSServerNamePostfix = originalGenerate
		createCIFSServer = originalCreate
		cifsServerSetup = originalSetup
		addSecurityPrivilegesForUser = originalAddSec
		deleteCIFSServer = originalDelete
	})
	generateCIFSServerNamePostfix = func(_ []*CIFSServer) string { return "efgh" }
	createCIFSServer = func(_ log.Logger, _ ontapRest.RESTClient, _, _, _ string, _ *ActiveDirectory) error { return nil }
	cifsServerSetup = func(_ log.Logger, _ ontapRest.NASClient, _ string) error { return nil }
	addSecurityPrivilegesForUser = func(_ ontapRest.RESTClient, _, _ string) error { return fmt.Errorf("priv fail") }
	deleteCalled := false
	deleteCIFSServer = func(_ log.Logger, _ ontapRest.RESTClient, svmUUID, username, password string) error {
		deleteCalled = true
		return nil
	}
	_, err := createAndSetupCIFSServer(tracelog, mockREST, ad, "svm-uuid", "svmName")
	require.Error(t, err)
	assert.True(t, deleteCalled)
	assert.Contains(t, err.Error(), "priv fail")
}

func TestGenerateCIFSServerNamePostfix_Unique(t *testing.T) {
	existing := []*CIFSServer{{ServerNamePostfix: "abcd"}, {ServerNamePostfix: "1234"}}
	postfix := generateCIFSServerNamePostfix(existing)
	require.NotEmpty(t, postfix)
	for _, e := range existing {
		assert.NotEqual(t, e.ServerNamePostfix, postfix)
	}
	assert.Len(t, postfix, 4)
}

func TestPrependDomainToUser(t *testing.T) {
	assert.Equal(t, "example.com\\user", prependDomainToUser("user", "example.com"))
}

func TestCreateJunctionPathForCifsShare_Error(t *testing.T) {
	mockREST := &ontapRest.MockRESTClient{}
	mockNAS := &ontapRest.MockNASClient{}
	mockREST.On("NAS").Return(mockNAS)
	mockNAS.On("CifsShareCreate", mock.Anything).Return(fmt.Errorf("share create fail"))
	err := createJunctionPathForCifsShare(mockREST, "svm", "/data")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "share create fail")
}

func TestDDNSModify_Error(t *testing.T) {
	mockREST := &ontapRest.MockRESTClient{}
	mockNS := &ontapRest.MockNameServicesClient{}
	mockREST.On("NameServices").Return(mockNS)
	mockNS.On("DNSModify", mock.Anything).Return(fmt.Errorf("dns modify fail"))
	err := ddnsModify(mockREST, "svm-uuid", "fqdn.example.com")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dns modify fail")
}

func TestAddSecurityPrivilegesForUser_Error(t *testing.T) {
	mockREST := &ontapRest.MockRESTClient{}
	mockNAS := &ontapRest.MockNASClient{}
	mockREST.On("NAS").Return(mockNAS)
	mockNAS.On("CifsServiceAddSecurityPrivilege", mock.Anything).Return(fmt.Errorf("privilege fail"))
	err := addSecurityPrivilegesForUser(mockREST, "domain\\user", "svm-uuid")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "privilege fail")
}

func TestCreateOrModifyADDNS_SpacesParsing(t *testing.T) {
	provider := newTestProvider()
	ad := &ActiveDirectory{Domain: "example.com", DNS: "  10.0.0.1 ,10.0.0.2,  10.0.0.3  "}
	original := createOrModifyDNS
	t.Cleanup(func() { createOrModifyDNS = original })
	var capturedServers []string
	createOrModifyDNS = func(_ *OntapRestProvider, _ string, _ []string, servers []string) error {
		capturedServers = append([]string{}, servers...)
		return nil
	}
	err := createOrModifyADDNS(provider, "svm-uuid", ad)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"}, capturedServers)
}

func TestEnsureCIFSShare_ErrorPaths(t *testing.T) {
	logger := log.NewLogger()
	provider := &OntapRestProvider{Logger: logger}
	ad := buildTestActiveDirectory()

	// Case 1: get client error
	withMockOntapClient(t, nil, fmt.Errorf("client fail"), func() {
		_, err := provider.EnsureCIFSShare(ConfigActiveDirectoryParams{ActiveDirectory: ad, ExternalSVMUUID: "svm", SVMName: "svm", JunctionPath: "/x"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "client fail")
	})

	// Prepare mocks for remaining cases
	mockREST := &ontapRest.MockRESTClient{}
	mockNAS := &ontapRest.MockNASClient{}
	mockNS := &ontapRest.MockNameServicesClient{}
	mockREST.On("NAS").Return(mockNAS)
	mockREST.On("NameServices").Return(mockNS)
	// Generic DNSGet expectations (may not be called in every branch)
	mockNS.On("DNSGet", mock.Anything).Return((*ontapRest.DNS)(nil), vsaerrors.NewNotFoundErr("dns", nil)).Maybe()
	mockNS.On("DnsCreate", mock.Anything).Return(&ontapModels.DNSResponse{}, nil).Maybe()
	mockNS.On("DNSModify", mock.Anything).Return(nil).Maybe()

	originalGetClient := getOntapClientFunc
	t.Cleanup(func() { getOntapClientFunc = originalGetClient })
	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) { return mockREST, nil }
	originalAddDNS := createOrModifyADDNS
	// baseline: stub createOrModifyADDNS to bypass DNS for cases except explicit DNS error case
	createOrModifyADDNS = func(_ *OntapRestProvider, _ string, _ *ActiveDirectory) error { return nil }

	// Case 2: ensureCifsServerNamePostFix error (stub DNS to avoid NameServices mock)
	originalEnsure := ensureCifsServerNamePostFix
	mockNAS.On("CifsServiceList", mock.Anything).Return([]*ontapRest.CifsService{}, nil).Maybe()
	ensureCifsServerNamePostFix = func(_ log.Logger, _ ontapRest.RESTClient, _ *ActiveDirectory, _ string) error {
		return fmt.Errorf("ensure fail")
	}
	createOrModifyADDNS = func(_ *OntapRestProvider, _ string, _ *ActiveDirectory) error { return nil }
	_, err := provider.EnsureCIFSShare(ConfigActiveDirectoryParams{ActiveDirectory: ad, ExternalSVMUUID: "svm", SVMName: "svm", JunctionPath: "/x"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ensure fail")
	ensureCifsServerNamePostFix = originalEnsure
	createOrModifyADDNS = originalAddDNS

	// Case 3: createOrModifyADDNS error
	createOrModifyADDNS = func(_ *OntapRestProvider, _ string, _ *ActiveDirectory) error { return fmt.Errorf("dns add fail") }
	_, err = provider.EnsureCIFSShare(ConfigActiveDirectoryParams{ActiveDirectory: ad, ExternalSVMUUID: "svm", SVMName: "svm", JunctionPath: "/x"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dns add fail")
	// restore stub (not original) for subsequent cases
	createOrModifyADDNS = func(_ *OntapRestProvider, _ string, _ *ActiveDirectory) error { return nil }

	// Case 4: cifsServiceGet returns not found then createAndSetupCIFSServer fails
	mockNAS.On("CifsServiceGet", mock.Anything).Return((*ontapRest.CifsService)(nil), vsaerrors.NewNotFoundErr("cifs", nil))
	originalCreate := createAndSetupCIFSServer
	createAndSetupCIFSServer = func(_ log.Logger, _ ontapRest.RESTClient, _ *ActiveDirectory, _, _ string) (string, error) {
		return "", fmt.Errorf("create fail")
	}
	_, err = provider.EnsureCIFSShare(ConfigActiveDirectoryParams{ActiveDirectory: ad, ExternalSVMUUID: "svm", SVMName: "svm", JunctionPath: "/x"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "create fail")
	createAndSetupCIFSServer = originalCreate

	// Case 5: ddnsModify error when existing service present and DDNS disabled
	mockNAS.ExpectedCalls = nil // reset expectations
	mockNAS.On("CifsServiceList", mock.Anything).Return([]*ontapRest.CifsService{}, nil).Maybe()
	mockNAS.On("CifsServiceGet", mock.Anything).Return(&ontapRest.CifsService{CifsService: ontapModels.CifsService{Name: nillable.ToPointer("MYSERVER-0001"), AdDomain: &ontapModels.AdDomain{Fqdn: nillable.ToPointer("example.com")}}}, nil)
	// Stub createOrModifyADDNS to bypass DNS client usage
	createOrModifyADDNS = func(_ *OntapRestProvider, _ string, _ *ActiveDirectory) error { return nil }
	originalIsDDNS := isDDNSEnabled
	originalDDNS := ddnsModify
	isDDNSEnabled = func(_ log.Logger, _ ontapRest.RESTClient, _ string) bool { return false }
	ddnsModify = func(_ ontapRest.RESTClient, _, _ string) error { return fmt.Errorf("ddns update fail") }
	_, err = provider.EnsureCIFSShare(ConfigActiveDirectoryParams{ActiveDirectory: ad, ExternalSVMUUID: "svm", SVMName: "svm", JunctionPath: "/x"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ddns update fail")
	isDDNSEnabled = originalIsDDNS
	ddnsModify = originalDDNS
	createOrModifyADDNS = func(_ *OntapRestProvider, _ string, _ *ActiveDirectory) error { return nil }

	// Case 6: junction path creation error
	mockNAS.ExpectedCalls = nil
	mockNAS.On("CifsServiceList", mock.Anything).Return([]*ontapRest.CifsService{}, nil).Maybe()
	mockNAS.On("CifsServiceGet", mock.Anything).Return(&ontapRest.CifsService{CifsService: ontapModels.CifsService{Name: nillable.ToPointer("MYSERVER-0001"), AdDomain: &ontapModels.AdDomain{Fqdn: nillable.ToPointer("example.com")}}}, nil)
	createOrModifyADDNS = func(_ *OntapRestProvider, _ string, _ *ActiveDirectory) error { return nil }
	originalJunction := createJunctionPathForCifsShare
	createJunctionPathForCifsShare = func(_ ontapRest.RESTClient, _, _ string) error { return fmt.Errorf("junction fail") }
	_, err = provider.EnsureCIFSShare(ConfigActiveDirectoryParams{ActiveDirectory: ad, ExternalSVMUUID: "svm", SVMName: "svm", JunctionPath: "/x"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "junction fail")
	createJunctionPathForCifsShare = originalJunction
	createOrModifyADDNS = originalAddDNS
}

// ---------------- createCIFSServer specific tests -----------------

func TestCreateCIFSServer_InvalidNameNoPostfix(t *testing.T) {
	tracelog := log.NewLogger()
	ad := buildTestActiveDirectory()
	mockREST := &ontapRest.MockRESTClient{}
	mockNAS := &ontapRest.MockNASClient{}
	mockREST.On("NAS").Return(mockNAS)
	// Expect domain discovery mode modification before regex check
	mockNAS.On("CifsDomainModify", mock.MatchedBy(func(params *ontapRest.CifsDomainModifyParams) bool {
		return params.DiscoveryMode != nil && *params.DiscoveryMode == "all"
	})).Return(nil).Once()
	err := createCIFSServer(tracelog, mockREST, "svm-uuid", "svmName", "INVALIDNAME", ad)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "postfix is missing")
}

func TestCreateCIFSServer_SiteDiscoveryModeAndTLSInstall(t *testing.T) {
	tracelog := log.NewLogger()
	site := nillable.ToPointer("mysite")
	ad := buildTestActiveDirectory()
	ad.Site = site
	// Enable TLS requirement
	originalLDAPOverTLSGlobal := isLDAPOverTLS
	isLDAPOverTLS = true
	t.Cleanup(func() { isLDAPOverTLS = originalLDAPOverTLSGlobal })
	trueVal := nillable.ToPointer(true)
	ad.LdapOverTLS = trueVal
	cert := "---CERT---"
	ad.ServerRootCaCertificate = &cert

	mockREST := &ontapRest.MockRESTClient{}
	mockNAS := &ontapRest.MockNASClient{}
	mockSec := &ontapRest.MockSecurityClient{}
	mockREST.On("NAS").Return(mockNAS)
	mockREST.On("Security").Return(mockSec)

	// Certificate get returns not found -> triggers install
	mockSec.On("ServerRootCACertificateGet", mock.Anything).Return(nil, vsaerrors.NewNotFoundErr("cert", nil))
	mockSec.On("ServerRootCACertificateInstall", mock.Anything).Return(&ontapRest.ServerRootCACertificate{}, nil)

	// discovery mode site
	mockNAS.On("CifsDomainModify", mock.MatchedBy(func(params *ontapRest.CifsDomainModifyParams) bool {
		return params.DiscoveryMode != nil && *params.DiscoveryMode == "site" && params.SvmUUID == "svm-uuid"
	})).Return(nil).Once()

	// CifsServiceCreate returns done immediately
	mockNAS.On("CifsServiceCreate", mock.Anything).Return(true, (*ontapRest.JobAccepted)(nil), nil).Once()
	mockNAS.On("CifsServiceModify", mock.Anything).Return(nil).Once()
	mockNAS.On("CifsServiceAddMembers", mock.Anything).Return(nil).Once()
	mockNAS.On("CifsDomainModify", mock.MatchedBy(func(params *ontapRest.CifsDomainModifyParams) bool {
		return params.ScheduleEnabled != nil && *params.ScheduleEnabled
	})).Return(nil).Once()

	name := "SERVERTST-abcd" // length <=15 and matches regex
	err := createCIFSServer(tracelog, mockREST, "svm-uuid", "svmName", name, ad)
	require.NoError(t, err)
	mockNAS.AssertExpectations(t)
	mockSec.AssertExpectations(t)
}

func TestCreateCIFSServer_JobPollPath(t *testing.T) {
	tracelog := log.NewLogger()
	ad := buildTestActiveDirectory()
	mockREST := &ontapRest.MockRESTClient{}
	mockNAS := &ontapRest.MockNASClient{}
	mockREST.On("NAS").Return(mockNAS)
	// discovery mode all (no site set)
	mockNAS.On("CifsDomainModify", mock.MatchedBy(func(params *ontapRest.CifsDomainModifyParams) bool {
		return params.DiscoveryMode != nil && *params.DiscoveryMode == "all"
	})).Return(nil).Once()
	// Return job for creation
	job := &ontapRest.JobAccepted{JobUUID: "job-123"}
	mockNAS.On("CifsServiceCreate", mock.Anything).Return(false, job, nil).Once()
	mockREST.On("Poll", "job-123").Return(nil).Once()
	mockNAS.On("CifsServiceModify", mock.Anything).Return(nil).Once()
	mockNAS.On("CifsServiceAddMembers", mock.Anything).Return(nil).Once()
	mockNAS.On("CifsDomainModify", mock.MatchedBy(func(params *ontapRest.CifsDomainModifyParams) bool {
		return params.ScheduleEnabled != nil && *params.ScheduleEnabled
	})).Return(nil).Once()

	name := "SERVERTST-abcd"
	err := createCIFSServer(tracelog, mockREST, "svm-uuid", "svmName", name, ad)
	require.NoError(t, err)
	mockREST.AssertExpectations(t)
	mockNAS.AssertExpectations(t)
}

func TestCifsServerSetup_Success(t *testing.T) {
	mockNAS := &ontapRest.MockNASClient{}
	mockNAS.On("CifsServiceModify", mock.MatchedBy(func(params *ontapRest.CifsServiceModifyParams) bool {
		return params.SvmUUID != nil && *params.SvmUUID == "svm-uuid" &&
			params.CopyOffload != nil && !*params.CopyOffload &&
			params.RestrictAnonymous != nil && *params.RestrictAnonymous == "no_access"
	})).Return(nil).Once()
	mockNAS.On("CifsShareACLDelete", mock.Anything).Return(stdErrors.New("entry doesn't exist"))

	err := cifsServerSetup(log.NewLogger(), mockNAS, "svm-uuid")
	require.NoError(t, err)
}

func TestCifsServerSetup_ReturnsErrorOnModifyFailure(t *testing.T) {
	mockNAS := &ontapRest.MockNASClient{}
	mockNAS.On("CifsServiceModify", mock.Anything).Return(fmt.Errorf("fail"))

	err := cifsServerSetup(log.NewLogger(), mockNAS, "svm-uuid")
	require.Error(t, err)
}

func TestAddSecurityPrivilegesForUser(t *testing.T) {
	mockREST := &ontapRest.MockRESTClient{}
	mockNAS := &ontapRest.MockNASClient{}
	mockREST.On("NAS").Return(mockNAS)
	mockNAS.On("CifsServiceAddSecurityPrivilege", mock.MatchedBy(func(params *ontapRest.CifsServiceModifySecurityPrivilegeParams) bool {
		return params.Member == "domain\\user" && params.SvmUUID == "svm-uuid"
	})).Return(nil).Once()

	err := addSecurityPrivilegesForUser(mockREST, "domain\\user", "svm-uuid")
	require.NoError(t, err)
}

// Test duplicate entry warning path in createAndSetupCIFSServer
func TestCreateAndSetupCIFSServer_DuplicateEntryWarning(t *testing.T) {
	tracelog := log.NewLogger()
	ad := buildTestActiveDirectory()
	ad.Users = map[string][]string{"BUILTIN\\Administrators": {"user1"}, utils.ActiveDirectorySeSecurityPrivilege: {}} // ensure non-priv group triggers add members

	mockREST := &ontapRest.MockRESTClient{}
	mockNAS := &ontapRest.MockNASClient{}
	mockREST.On("NAS").Return(mockNAS)

	// Setup expectations (two CifsServiceModify calls: one during createCIFSServer, one during cifsServerSetup)
	mockNAS.On("CifsServiceModify", mock.Anything).Return(nil).Times(2)
	mockNAS.On("CifsShareACLDelete", mock.Anything).Return(stdErrors.New("entry doesn't exist")).Maybe()
	// First call: azure admin group addition succeeds
	mockNAS.On("CifsServiceAddMembers", mock.MatchedBy(func(p *ontapRest.CifsServiceModifyGroupMembersParams) bool {
		return p != nil && len(p.Members) == 1 && strings.HasSuffix(p.Members[0], "AAD DC Administrators")
	})).Return(nil).Once()
	// Second call: regular group member addition triggers duplicate entry warning path
	mockNAS.On("CifsServiceAddMembers", mock.MatchedBy(func(p *ontapRest.CifsServiceModifyGroupMembersParams) bool {
		return p != nil && len(p.Members) == 1 && strings.HasSuffix(p.Members[0], "user1")
	})).Return(fmt.Errorf("Reason: duplicate entry.")).Once()
	mockNAS.On("CifsServiceAddSecurityPrivilege", mock.Anything).Return(nil).Maybe()
	mockNAS.On("CifsDomainModify", mock.Anything).Return(nil).Maybe()
	mockNAS.On("CifsServiceCreate", mock.Anything).Return(true, (*ontapRest.JobAccepted)(nil), nil).Once()
	// ddns modify stub
	originalDDNS := ddnsModify
	ddnsModify = func(_ ontapRest.RESTClient, _, _ string) error { return nil }
	t.Cleanup(func() { ddnsModify = originalDDNS })

	// call createAndSetupCIFSServer
	fqdn, err := createAndSetupCIFSServer(tracelog, mockREST, ad, "svm-uuid", "svmName")
	require.NoError(t, err)
	assert.Contains(t, fqdn, ad.Domain)
	// Ensure server appended despite duplicate entry warning
	require.Len(t, ad.CIFSServers, 1)
}

// Test netBIOS truncation path in ensureCifsServerNamePostFix
func TestEnsureCifsServerNamePostFix_NetBIOSTruncation(t *testing.T) {
	tracelog := log.NewLogger()
	longNetBIOS := "VERYLONGNETBIOSNAME" // >10 chars
	ad := &ActiveDirectory{NetBIOS: longNetBIOS, CIFSServers: []*CIFSServer{{SVMUUID: "uuid1", SVMName: "svmName"}}}
	mockREST := &ontapRest.MockRESTClient{}
	mockNAS := &ontapRest.MockNASClient{}
	mockREST.On("NAS").Return(mockNAS)
	upper := strings.ToUpper(longNetBIOS[0:10]) + "-"
	// existing CIFS service name uses truncated netBIOS prefix
	mockNAS.On("CifsServiceList", mock.Anything).Return([]*ontapRest.CifsService{{CifsService: ontapModels.CifsService{Name: nillable.ToPointer(upper + "abcd"), Svm: &ontapModels.CifsServiceInlineSvm{Name: nillable.ToPointer("svmName"), UUID: nillable.ToPointer("uuid1")}}}}, nil)
	err := ensureCifsServerNamePostFix(tracelog, mockREST, ad, "svmName")
	require.NoError(t, err)
	// postfix should be set to 'abcd'
	assert.Equal(t, "abcd", ad.CIFSServers[0].ServerNamePostfix)
}

// Test certificate deletion defer path triggered when subsequent NAS domain modify fails
func TestCreateCIFSServer_TLSCertDeleteOnFailure(t *testing.T) {
	tracelog := log.NewLogger()
	originalLDAPOverTLSGlobal := isLDAPOverTLS
	isLDAPOverTLS = true
	t.Cleanup(func() { isLDAPOverTLS = originalLDAPOverTLSGlobal })
	ad := buildTestActiveDirectory()
	ad.LdapOverTLS = nillable.ToPointer(true)
	cert := "---CERT---"
	ad.ServerRootCaCertificate = &cert

	mockREST := &ontapRest.MockRESTClient{}
	mockNAS := &ontapRest.MockNASClient{}
	mockSec := &ontapRest.MockSecurityClient{}
	mockREST.On("NAS").Return(mockNAS)
	mockREST.On("Security").Return(mockSec)

	// First get returns not found -> triggers install
	mockSec.On("ServerRootCACertificateGet", mock.Anything).Return(nil, vsaerrors.NewNotFoundErr("cert", nil)).Once()
	mockSec.On("ServerRootCACertificateInstall", mock.Anything).Return(&ontapRest.ServerRootCACertificate{}, nil).Once()
	// Deferred cleanup path should re-get successfully then delete
	mockSec.On("ServerRootCACertificateGet", mock.Anything).Return(&ontapRest.ServerRootCACertificate{}, nil).Once()
	mockSec.On("ServerRootCACertificateDelete", mock.Anything).Return(nil).Once()

	// Domain modify all success
	mockNAS.On("CifsDomainModify", mock.Anything).Return(nil).Once()
	// Fail CifsServiceCreate to trigger deferred cleanup
	mockNAS.On("CifsServiceCreate", mock.Anything).Return(true, (*ontapRest.JobAccepted)(nil), fmt.Errorf("create failed"))

	err := createCIFSServer(tracelog, mockREST, "svm-uuid", "svmName", "NETBIOS-aaaa", ad)
	require.Error(t, err)
	// Assert at least one delete attempt occurred (some implementations may retry)
	deleteCalls := 0
	for _, c := range mockSec.Calls {
		if c.Method == "ServerRootCACertificateDelete" {
			deleteCalls++
		}
	}
	require.GreaterOrEqual(t, deleteCalls, 1, "expected at least one server root CA certificate deletion attempt")
}

func TestDeleteCifsServer(t *testing.T) {
	logger := log.NewLogger()
	mockREST := &ontapRest.MockRESTClient{}
	mockNAS := &ontapRest.MockNASClient{}
	mockNS := &ontapRest.MockNameServicesClient{}
	mockREST.On("NameServices").Return(mockNS)
	mockREST.On("NAS").Return(mockNAS)

	mockNS.On("DNSModify", mock.MatchedBy(func(params *ontapRest.DNSModifyParams) bool {
		return params.SvmUUID == "svm-uuid" &&
			params.DDNSModifyParams.Enabled != nil && !*params.DDNSModifyParams.Enabled &&
			params.DDNSModifyParams.UseSecure != nil && !*params.DDNSModifyParams.UseSecure
	})).Return(nil).Once()

	mockNAS.On("CifsServiceDelete", mock.MatchedBy(func(params *ontapRest.CifsServiceDeleteParams) bool {
		return params.SvmUUID == "svm-uuid" && params.AdminUsername == "user" && params.AdminPassword == "pwd" && params.Force
	})).Return(nil).Once()

	err := deleteCIFSServer(logger, mockREST, "svm-uuid", "user", "pwd")
	require.NoError(t, err)
}

func TestDDNSModify(t *testing.T) {
	mockREST := &ontapRest.MockRESTClient{}
	mockNS := &ontapRest.MockNameServicesClient{}
	mockREST.On("NameServices").Return(mockNS)

	fqdn := "server.example.com"
	mockNS.On("DNSModify", mock.MatchedBy(func(params *ontapRest.DNSModifyParams) bool {
		return params.SvmUUID == "svm-uuid" &&
			params.DDNSModifyParams.Fqdn != nil && *params.DDNSModifyParams.Fqdn == fqdn &&
			params.DDNSModifyParams.Enabled != nil && *params.DDNSModifyParams.Enabled
	})).Return(nil).Once()

	err := ddnsModify(mockREST, "svm-uuid", fqdn)
	require.NoError(t, err)
}

func TestPrependDomainToUsers(t *testing.T) {
	users := []string{"user1", "Administrator", "Domain Admins", "AAD DC Administrators", "user2"}
	result := prependDomainToUsers(users, "example.com")
	assert.Equal(t, []string{"example.com\\user1", "example.com\\user2"}, result)
}

func TestGetSidFromGroupName(t *testing.T) {
	assert.Equal(t, "S-1-5-32-544", getSidFromGroupName("BUILTIN\\Administrators"))
	assert.Equal(t, "", getSidFromGroupName("Unknown"))
}

func TestGetSessionSecurity(t *testing.T) {
	sign := getSessionSecurity(true)
	none := getSessionSecurity(false)
	require.NotNil(t, sign)
	require.NotNil(t, none)
	assert.Equal(t, "sign", *sign)
	assert.Equal(t, "none", *none)
}

func TestIsTLSRequired(t *testing.T) {
	original := isLDAPOverTLS
	t.Cleanup(func() { isLDAPOverTLS = original })

	ad := buildTestActiveDirectory()
	*ad.LdapOverTLS = true

	isLDAPOverTLS = true
	assert.True(t, isTLSRequired(ad))

	isLDAPOverTLS = false
	assert.False(t, isTLSRequired(ad))

	isLDAPOverTLS = true
	*ad.LdapOverTLS = false
	assert.False(t, isTLSRequired(ad))
}

func TestIsDDNSEnabled(t *testing.T) {
	logger := log.NewLogger()
	mockREST := &ontapRest.MockRESTClient{}
	mockNS := &ontapRest.MockNameServicesClient{}
	mockREST.On("NameServices").Return(mockNS)

	mockNS.On("DNSGet", mock.Anything).Return((*ontapRest.DNS)(nil), fmt.Errorf("fail")).Once()
	assert.True(t, isDDNSEnabled(logger, mockREST, "svm-uuid"))

	dnsWithoutDynamic := &ontapRest.DNS{DNS: ontapModels.DNS{}}
	mockNS.On("DNSGet", mock.Anything).Return(dnsWithoutDynamic, nil).Once()
	assert.False(t, isDDNSEnabled(logger, mockREST, "svm-uuid"))

	dnsEnabled := &ontapRest.DNS{DNS: ontapModels.DNS{DynamicDNS: &ontapModels.DNSInlineDynamicDNS{Enabled: nillable.ToPointer(true)}}}
	mockNS.On("DNSGet", mock.Anything).Return(dnsEnabled, nil).Once()
	assert.True(t, isDDNSEnabled(logger, mockREST, "svm-uuid"))
}

func TestCreateJunctionPathForCifsShare(t *testing.T) {
	mockREST := &ontapRest.MockRESTClient{}
	mockNAS := &ontapRest.MockNASClient{}
	mockREST.On("NAS").Return(mockNAS)
	mockNAS.On("CifsShareCreate", mock.MatchedBy(func(params *ontapRest.CifsShareCreateParams) bool {
		return params.Path == "/data" && params.Name == "data" && params.SvmName != nil && *params.SvmName == "svm"
	})).Return(nil).Once()

	err := createJunctionPathForCifsShare(mockREST, "svm", "/data")
	require.NoError(t, err)
}

func TestEnsureCIFSShare_SuccessExistingService(t *testing.T) {
	logger := log.NewLogger()
	provider := &OntapRestProvider{Logger: logger}
	ad := buildTestActiveDirectory()

	mockREST := &ontapRest.MockRESTClient{}
	mockNAS := &ontapRest.MockNASClient{}
	mockREST.On("NAS").Return(mockNAS)

	cifsName := "MYSERVER-0001"
	mockNAS.On("CifsServiceGet", mock.Anything).Return(&ontapRest.CifsService{CifsService: ontapModels.CifsService{
		Name:     nillable.ToPointer(cifsName),
		AdDomain: &ontapModels.AdDomain{Fqdn: nillable.ToPointer("example.com")},
	}}, nil).Once()

	mockNAS.On("CifsShareCreate", mock.Anything).Return(nil)

	originalGetClient := getOntapClientFunc
	t.Cleanup(func() { getOntapClientFunc = originalGetClient })
	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) { return mockREST, nil }

	originalEnsure := ensureCifsServerNamePostFix
	originalAddDNS := createOrModifyADDNS
	originalIsDDNS := isDDNSEnabled
	originalDDNS := ddnsModify
	originalJunction := createJunctionPathForCifsShare

	t.Cleanup(func() {
		ensureCifsServerNamePostFix = originalEnsure
		createOrModifyADDNS = originalAddDNS
		isDDNSEnabled = originalIsDDNS
		ddnsModify = originalDDNS
		createJunctionPathForCifsShare = originalJunction
	})

	ensureCalled := false
	ensureCifsServerNamePostFix = func(_ log.Logger, _ ontapRest.RESTClient, _ *ActiveDirectory, _ string) error {
		ensureCalled = true
		return nil
	}

	addDNSCalled := false
	createOrModifyADDNS = func(_ *OntapRestProvider, _ string, _ *ActiveDirectory) error {
		addDNSCalled = true
		return nil
	}

	isDDNSEnabled = func(_ log.Logger, _ ontapRest.RESTClient, _ string) bool { return false }

	ddnsCalled := false
	ddnsModify = func(_ ontapRest.RESTClient, svmUUID, fqdn string) error {
		ddnsCalled = true
		assert.Equal(t, "MYSERVER-0001.example.com", fqdn)
		return nil
	}

	junctionCalled := false
	createJunctionPathForCifsShare = func(_ ontapRest.RESTClient, svmName, junction string) error {
		junctionCalled = true
		assert.Equal(t, "svmName", svmName)
		assert.Equal(t, "/junction", junction)
		return nil
	}

	params := ConfigActiveDirectoryParams{ActiveDirectory: ad, ExternalSVMUUID: "svm-uuid", SVMName: "svmName", JunctionPath: "/junction"}

	_, err := provider.EnsureCIFSShare(params)
	require.NoError(t, err)
	assert.True(t, ensureCalled)
	assert.True(t, addDNSCalled)
	assert.True(t, ddnsCalled)
	assert.True(t, junctionCalled)
}

func TestEnsureCIFSShare_CreatesServiceWhenMissing(t *testing.T) {
	logger := log.NewLogger()
	provider := &OntapRestProvider{Logger: logger}
	ad := buildTestActiveDirectory()

	mockREST := &ontapRest.MockRESTClient{}
	mockNAS := &ontapRest.MockNASClient{}
	mockREST.On("NAS").Return(mockNAS)

	mockNAS.On("CifsServiceGet", mock.Anything).Return((*ontapRest.CifsService)(nil), vsaerrors.NewNotFoundErr("cifs", nil)).Once()
	mockNAS.On("CifsShareCreate", mock.Anything).Return(nil)

	originalGetClient := getOntapClientFunc
	t.Cleanup(func() { getOntapClientFunc = originalGetClient })
	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) { return mockREST, nil }

	originalEnsure := ensureCifsServerNamePostFix
	originalAddDNS := createOrModifyADDNS
	originalCreate := createAndSetupCIFSServer
	originalIsDDNS := isDDNSEnabled
	originalDDNS := ddnsModify
	originalJunction := createJunctionPathForCifsShare

	t.Cleanup(func() {
		ensureCifsServerNamePostFix = originalEnsure
		createOrModifyADDNS = originalAddDNS
		createAndSetupCIFSServer = originalCreate
		isDDNSEnabled = originalIsDDNS
		ddnsModify = originalDDNS
		createJunctionPathForCifsShare = originalJunction
	})

	ensureCifsServerNamePostFix = func(_ log.Logger, _ ontapRest.RESTClient, _ *ActiveDirectory, _ string) error { return nil }
	createOrModifyADDNS = func(_ *OntapRestProvider, _ string, _ *ActiveDirectory) error { return nil }
	isDDNSEnabled = func(_ log.Logger, _ ontapRest.RESTClient, _ string) bool { return true }
	ddnsModify = func(_ ontapRest.RESTClient, _, _ string) error { return nil }

	createCalled := false
	createAndSetupCIFSServer = func(_ log.Logger, _ ontapRest.RESTClient, _ *ActiveDirectory, svmUUID, svmName string) (string, error) {
		createCalled = true
		assert.Equal(t, "svm-uuid", svmUUID)
		assert.Equal(t, "svmName", svmName)
		return "fqdn", nil
	}

	createJunctionPathForCifsShare = func(_ ontapRest.RESTClient, _, _ string) error { return nil }

	params := ConfigActiveDirectoryParams{ActiveDirectory: ad, ExternalSVMUUID: "svm-uuid", SVMName: "svmName", JunctionPath: "/junction"}

	_, err := provider.EnsureCIFSShare(params)
	require.NoError(t, err)
	assert.True(t, createCalled)
}

func TestUpdateCIFSServer_Success(t *testing.T) {
	provider := newTestProvider()

	mockREST := &ontapRest.MockRESTClient{}
	mockNAS := &ontapRest.MockNASClient{}
	mockREST.On("NAS").Return(mockNAS)

	mockNAS.On("CifsShareModify", mock.MatchedBy(func(params *ontapRest.CifsShareModifyParams) bool {
		return params.SvmUUID == "test-svm-uuid" &&
			params.ShareName == "test-share" &&
			assert.ElementsMatch(t, []string{"browsable", "encrypt_data"}, params.ShareProperties)
	})).Return(nil).Once()

	withMockOntapClient(t, mockREST, nil, func() {
		err := provider.UpdateCIFSServer("test-svm-uuid", "test-share", []string{"browsable", "encrypt_data"})
		require.NoError(t, err)
	})

	mockNAS.AssertExpectations(t)
	mockREST.AssertExpectations(t)
}

func TestUpdateCIFSServer_GetClientError(t *testing.T) {
	provider := newTestProvider()

	expectedErr := stdErrors.New("failed to get client")
	withMockOntapClient(t, nil, expectedErr, func() {
		err := provider.UpdateCIFSServer("test-svm-uuid", "test-share", []string{"browsable"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get ONTAP client")
	})
}

func TestUpdateCIFSServer_ModifyError(t *testing.T) {
	provider := newTestProvider()

	mockREST := &ontapRest.MockRESTClient{}
	mockNAS := &ontapRest.MockNASClient{}
	mockREST.On("NAS").Return(mockNAS)

	expectedErr := stdErrors.New("modify failed")
	mockNAS.On("CifsShareModify", mock.Anything).Return(expectedErr).Once()

	withMockOntapClient(t, mockREST, nil, func() {
		err := provider.UpdateCIFSServer("test-svm-uuid", "test-share", []string{"browsable"})
		require.Error(t, err)
		assert.Equal(t, expectedErr, err)
	})

	mockNAS.AssertExpectations(t)
	mockREST.AssertExpectations(t)
}

func TestCifsShareCollectionGet_Success(t *testing.T) {
	provider := newTestProvider()

	mockREST := &ontapRest.MockRESTClient{}
	mockNAS := &ontapRest.MockNASClient{}
	mockREST.On("NAS").Return(mockNAS)

	expectedResponse := &ontapRest.CifsShareGetResponse{
		ShareProperties: []string{"browsable", "continuously_available", "encrypt_data"},
	}

	mockNAS.On("CifsShareCollectionGet", mock.MatchedBy(func(params *ontapRest.CifsShareCollectionGetParams) bool {
		return params.SvmUUID == "test-svm-uuid" &&
			params.ShareName == "test-share" &&
			assert.ElementsMatch(t, []string{"continuously_available"}, params.Fields)
	})).Return(expectedResponse, nil).Once()

	withMockOntapClient(t, mockREST, nil, func() {
		result, err := provider.CifsShareCollectionGet("test-svm-uuid", "test-share", []string{"continuously_available"})
		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.ElementsMatch(t, []string{"browsable", "continuously_available", "encrypt_data"}, result)
	})

	mockNAS.AssertExpectations(t)
	mockREST.AssertExpectations(t)
}

func TestCifsShareCollectionGet_GetClientError(t *testing.T) {
	provider := newTestProvider()

	expectedErr := stdErrors.New("failed to get client")
	withMockOntapClient(t, nil, expectedErr, func() {
		result, err := provider.CifsShareCollectionGet("test-svm-uuid", "test-share", []string{"browsable"})
		require.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "failed to get ONTAP client")
	})
}

func TestCifsShareCollectionGet_GetError(t *testing.T) {
	provider := newTestProvider()

	mockREST := &ontapRest.MockRESTClient{}
	mockNAS := &ontapRest.MockNASClient{}
	mockREST.On("NAS").Return(mockNAS)

	expectedErr := stdErrors.New("share not found")
	mockNAS.On("CifsShareCollectionGet", mock.Anything).Return(nil, expectedErr).Once()

	withMockOntapClient(t, mockREST, nil, func() {
		result, err := provider.CifsShareCollectionGet("test-svm-uuid", "non-existent-share", []string{"browsable"})
		require.Error(t, err)
		assert.Nil(t, result)
		assert.Equal(t, expectedErr, err)
	})

	mockNAS.AssertExpectations(t)
	mockREST.AssertExpectations(t)
}

func Test_updateCIFSShareProperties_Success(t *testing.T) {
	logger := log.NewLogger()

	mockREST := &ontapRest.MockRESTClient{}
	mockNAS := &ontapRest.MockNASClient{}
	mockREST.On("NAS").Return(mockNAS)

	mockNAS.On("CifsShareModify", mock.MatchedBy(func(params *ontapRest.CifsShareModifyParams) bool {
		return params.SvmUUID == "test-svm-uuid" &&
			params.ShareName == "test-share" &&
			assert.ElementsMatch(t, []string{"browsable", "oplocks"}, params.ShareProperties)
	})).Return(nil).Once()

	err := _updateCIFSShareProperties(logger, mockREST, "test-svm-uuid", "test-share", []string{"browsable", "oplocks"})
	require.NoError(t, err)

	mockNAS.AssertExpectations(t)
	mockREST.AssertExpectations(t)
}

func Test_updateCIFSShareProperties_Error(t *testing.T) {
	logger := log.NewLogger()

	mockREST := &ontapRest.MockRESTClient{}
	mockNAS := &ontapRest.MockNASClient{}
	mockREST.On("NAS").Return(mockNAS)

	expectedErr := stdErrors.New("modification failed")
	mockNAS.On("CifsShareModify", mock.Anything).Return(expectedErr).Once()

	err := _updateCIFSShareProperties(logger, mockREST, "test-svm-uuid", "test-share", []string{"browsable"})
	require.Error(t, err)
	assert.Equal(t, expectedErr, err)

	mockNAS.AssertExpectations(t)
	mockREST.AssertExpectations(t)
}
