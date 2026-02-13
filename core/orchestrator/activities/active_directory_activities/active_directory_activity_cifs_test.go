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
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	utilerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/testsuite"
)

func setupOntapProvider(t *testing.T, ctx context.Context, client ontapRest.RESTClient, extraHooks vsa.TestHooks) func() {
	t.Helper()

	originalGetter := getOntapRestProvider
	logger := util.GetLogger(ctx)
	provider := &vsa.OntapRestProvider{
		ClientParams: ontapRest.RESTClientParams{Trace: logger, Ctx: ctx},
		Logger:       logger,
	}

	getOntapRestProvider = func(context.Context, *models.Node) (*vsa.OntapRestProvider, error) {
		return provider, nil
	}

	hooks := vsa.TestHooks{
		GetOntapClient: func(ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
			return client, nil
		},
		EnsureCifsServerNamePostFix: func(log.Logger, ontapRest.RESTClient, *vsa.ActiveDirectory, string) error {
			return nil
		},
		CreateAndSetupCIFSServer: func(log.Logger, ontapRest.RESTClient, *vsa.ActiveDirectory, string, string) (string, error) {
			return "", nil
		},
		IsDDNSEnabled: func(log.Logger, ontapRest.RESTClient, string) bool {
			return false
		},
	}

	if extraHooks.EnsureCifsServerNamePostFix != nil {
		hooks.EnsureCifsServerNamePostFix = extraHooks.EnsureCifsServerNamePostFix
	}
	if extraHooks.CreateAndSetupCIFSServer != nil {
		hooks.CreateAndSetupCIFSServer = extraHooks.CreateAndSetupCIFSServer
	}
	if extraHooks.IsDDNSEnabled != nil {
		hooks.IsDDNSEnabled = extraHooks.IsDDNSEnabled
	}

	cleanupHooks := vsa.SetTestHooks(hooks)

	return func() {
		cleanupHooks()
		getOntapRestProvider = originalGetter
	}
}

func strPtr(value string) *string {
	return &value
}

func TestCreateOrModifyADDNS_CreatesDNSWhenMissing(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockClient := new(ontapRest.MockRESTClient)
	mockNameSvc := new(ontapRest.MockNameServicesClient)
	mockClient.On("NameServices").Return(mockNameSvc).Times(2)
	mockNameSvc.On("DNSGet", mock.Anything).Return((*ontapRest.DNS)(nil), nil).Once()
	mockNameSvc.On("DnsCreate", mock.Anything).Return((*ontaprestmodels.DNSResponse)(nil), nil).Once()

	ctx := context.Background()
	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{})
	defer cleanup()

	activity := ActiveDirectoryActivity{}
	env.RegisterActivity(activity.CreateOrModifyADDNS)

	ad := &vsa.ActiveDirectory{DNS: "10.0.0.1,20.0.0.2", Domain: "example.com"}
	_, err := env.ExecuteActivity(activity.CreateOrModifyADDNS, &models.Node{}, ad, "svm", "svm-uuid")

	require.NoError(t, err)
	mockClient.AssertExpectations(t)
	mockNameSvc.AssertExpectations(t)
}

func TestCreateOrModifyADDNS_ModifiesWhenConfigDiffers(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockClient := new(ontapRest.MockRESTClient)
	mockNameSvc := new(ontapRest.MockNameServicesClient)
	mockClient.On("NameServices").Return(mockNameSvc).Times(2)

	existing := &ontapRest.DNS{
		DNS: ontaprestmodels.DNS{
			Servers: ontaprestmodels.NameServersArrayInline{strPtr("1.1.1.1")},
			Domains: ontaprestmodels.DNSDomainsArrayInline{strPtr("old.example.com")},
		},
	}
	mockNameSvc.On("DNSGet", mock.Anything).Return(existing, nil).Once()
	mockNameSvc.On("DNSModify", mock.MatchedBy(func(params *ontapRest.DNSModifyParams) bool {
		assert.Equal(t, "svm-uuid", params.SvmUUID)
		assert.Equal(t, []string{"example.com"}, params.Domains)
		assert.Equal(t, []string{"2.2.2.2", "3.3.3.3"}, params.NameServers)
		return true
	})).Return(nil).Once()

	ctx := context.Background()
	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{})
	defer cleanup()

	activity := ActiveDirectoryActivity{}
	env.RegisterActivity(activity.CreateOrModifyADDNS)

	ad := &vsa.ActiveDirectory{DNS: "2.2.2.2,3.3.3.3", Domain: "example.com"}
	_, err := env.ExecuteActivity(activity.CreateOrModifyADDNS, &models.Node{}, ad, "svm", "svm-uuid")

	require.NoError(t, err)
	mockClient.AssertExpectations(t)
	mockNameSvc.AssertExpectations(t)
}

func TestCreateOrModifyADDNS_NoChangeWhenConfigMatches(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockClient := new(ontapRest.MockRESTClient)
	mockNameSvc := new(ontapRest.MockNameServicesClient)
	mockClient.On("NameServices").Return(mockNameSvc).Once()

	matching := &ontapRest.DNS{
		DNS: ontaprestmodels.DNS{
			Servers: ontaprestmodels.NameServersArrayInline{strPtr("9.9.9.9"), strPtr("8.8.8.8")},
			Domains: ontaprestmodels.DNSDomainsArrayInline{strPtr("example.com")},
		},
	}
	mockNameSvc.On("DNSGet", mock.Anything).Return(matching, nil).Once()

	ctx := context.Background()
	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{})
	defer cleanup()

	activity := ActiveDirectoryActivity{}
	env.RegisterActivity(activity.CreateOrModifyADDNS)

	ad := &vsa.ActiveDirectory{DNS: "9.9.9.9,8.8.8.8", Domain: "example.com"}
	_, err := env.ExecuteActivity(activity.CreateOrModifyADDNS, &models.Node{}, ad, "svm", "svm-uuid")

	require.NoError(t, err)
	mockClient.AssertExpectations(t)
	mockNameSvc.AssertExpectations(t)
	mockNameSvc.AssertNotCalled(t, "DNSModify", mock.Anything)
}

func TestGetOrCreateCifsService_CreatesWhenMissing(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	// Mock password decryption
	originalDecryptPassword := utils.DecryptPassword
	utils.DecryptPassword = func(password log.Secret) (*string, error) {
		decrypted := "decrypted-password"
		return &decrypted, nil
	}
	defer func() { utils.DecryptPassword = originalDecryptPassword }()

	mockClient := new(ontapRest.MockRESTClient)
	mockNas := new(ontapRest.MockNASClient)
	mockClient.On("NAS").Return(mockNas).Once()

	mockNas.On("CifsServiceGet", mock.Anything).Return((*ontapRest.CifsService)(nil), utilerrors.NewNotFoundErr("cifs service", nil)).Once()

	ctx := context.Background()
	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{
		CreateAndSetupCIFSServer: func(_ log.Logger, _ ontapRest.RESTClient, _ *vsa.ActiveDirectory, _, _ string) (string, error) {
			return "created.example.com", nil
		},
		IsDDNSEnabled: func(log.Logger, ontapRest.RESTClient, string) bool { return false },
	})
	defer cleanup()

	activity := ActiveDirectoryActivity{}
	env.RegisterActivity(activity.GetOrCreateCifsService)

	ad := &vsa.ActiveDirectory{Domain: "example.com"}
	val, err := env.ExecuteActivity(activity.GetOrCreateCifsService, &models.Node{}, ad, "svm", "svm-uuid")

	require.NoError(t, err)
	var result *GetOrCreateCifsServiceResult
	_ = val.Get(&result)
	require.NotNil(t, result)
	assert.Equal(t, "created.example.com", result.FQDN)
	assert.False(t, result.NeedsDDNS)
	mockNas.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestGetOrCreateCifsService_ReturnsExistingRequestsDDNS(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	// Mock password decryption
	originalDecryptPassword := utils.DecryptPassword
	utils.DecryptPassword = func(password log.Secret) (*string, error) {
		decrypted := "decrypted-password"
		return &decrypted, nil
	}
	defer func() { utils.DecryptPassword = originalDecryptPassword }()

	mockClient := new(ontapRest.MockRESTClient)
	mockNas := new(ontapRest.MockNASClient)
	mockClient.On("NAS").Return(mockNas).Once()

	name := "NETBIOS"
	fqdn := "example.com"
	existing := &ontapRest.CifsService{
		CifsService: ontaprestmodels.CifsService{
			Name: &name,
			AdDomain: &ontaprestmodels.AdDomain{
				Fqdn: &fqdn,
			},
		},
	}
	mockNas.On("CifsServiceGet", mock.Anything).Return(existing, nil).Once()

	ctx := context.Background()
	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{
		IsDDNSEnabled: func(log.Logger, ontapRest.RESTClient, string) bool { return false },
	})
	defer cleanup()

	activity := ActiveDirectoryActivity{}
	env.RegisterActivity(activity.GetOrCreateCifsService)

	ad := &vsa.ActiveDirectory{Domain: "example.com"}
	val, err := env.ExecuteActivity(activity.GetOrCreateCifsService, &models.Node{}, ad, "svm", "svm-uuid")

	require.NoError(t, err)
	var result *GetOrCreateCifsServiceResult
	_ = val.Get(&result)
	require.NotNil(t, result)
	assert.True(t, result.NeedsDDNS)
	assert.Equal(t, name, result.CifsServiceName)
	assert.Equal(t, fqdn, result.AdDomain)
	mockNas.AssertExpectations(t)
}

func TestGetOrCreateCifsService_ReturnsExistingNoDDNS(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	// Mock password decryption
	originalDecryptPassword := utils.DecryptPassword
	utils.DecryptPassword = func(password log.Secret) (*string, error) {
		decrypted := "decrypted-password"
		return &decrypted, nil
	}
	defer func() { utils.DecryptPassword = originalDecryptPassword }()

	mockClient := new(ontapRest.MockRESTClient)
	mockNas := new(ontapRest.MockNASClient)
	mockClient.On("NAS").Return(mockNas).Once()

	name := "NETBIOS"
	fqdn := "example.com"
	existing := &ontapRest.CifsService{
		CifsService: ontaprestmodels.CifsService{
			Name:     &name,
			AdDomain: &ontaprestmodels.AdDomain{Fqdn: &fqdn},
		},
	}
	mockNas.On("CifsServiceGet", mock.Anything).Return(existing, nil).Once()

	ctx := context.Background()
	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{
		IsDDNSEnabled: func(log.Logger, ontapRest.RESTClient, string) bool { return true },
	})
	defer cleanup()

	activity := ActiveDirectoryActivity{}
	env.RegisterActivity(activity.GetOrCreateCifsService)

	ad := &vsa.ActiveDirectory{Domain: "example.com"}
	val, err := env.ExecuteActivity(activity.GetOrCreateCifsService, &models.Node{}, ad, "svm", "svm-uuid")

	require.NoError(t, err)
	var result *GetOrCreateCifsServiceResult
	_ = val.Get(&result)
	require.NotNil(t, result)
	assert.False(t, result.NeedsDDNS)
	mockNas.AssertExpectations(t)
}

func TestDdnsModify_SetsSecureDDNS(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockClient := new(ontapRest.MockRESTClient)
	mockNameSvc := new(ontapRest.MockNameServicesClient)
	mockClient.On("NameServices").Return(mockNameSvc).Once()
	mockNameSvc.On("DNSModify", mock.MatchedBy(func(params *ontapRest.DNSModifyParams) bool {
		assert.Equal(t, "svm-uuid", params.SvmUUID)
		require.NotNil(t, params.DDNSModifyParams.UseSecure)
		assert.True(t, *params.DDNSModifyParams.UseSecure)
		require.NotNil(t, params.DDNSModifyParams.Enabled)
		assert.True(t, *params.DDNSModifyParams.Enabled)
		require.NotNil(t, params.DDNSModifyParams.Fqdn)
		assert.Equal(t, "fqdn.example.com", *params.DDNSModifyParams.Fqdn)
		return true
	})).Return(nil).Once()

	ctx := context.Background()
	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{})
	defer cleanup()

	activity := ActiveDirectoryActivity{}
	env.RegisterActivity(activity.DdnsModify)

	_, err := env.ExecuteActivity(activity.DdnsModify, &models.Node{}, "svm-uuid", "fqdn.example.com")

	require.NoError(t, err)
	mockClient.AssertExpectations(t)
	mockNameSvc.AssertExpectations(t)
}

func TestCreateJunctionPathForCifsShare_Succeeds(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockClient := new(ontapRest.MockRESTClient)
	mockNas := new(ontapRest.MockNASClient)
	mockClient.On("NAS").Return(mockNas).Once()
	mockNas.On("CifsShareCreate", mock.MatchedBy(func(params *ontapRest.CifsShareCreateParams) bool {
		assert.Equal(t, "svm", *params.SvmName)
		assert.Equal(t, "/junction", params.Path)
		assert.Equal(t, "junction", params.Name)
		return true
	})).Return(nil).Once()

	ctx := context.Background()
	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{})
	defer cleanup()

	activity := ActiveDirectoryActivity{}
	env.RegisterActivity(activity.CreateJunctionPathForCifsShare)

	_, err := env.ExecuteActivity(activity.CreateJunctionPathForCifsShare, &models.Node{}, "svm", "/junction", []string{})

	require.NoError(t, err)
	mockNas.AssertExpectations(t)
}

func TestCreateJunctionPathForCifsShare_PropagatesError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockClient := new(ontapRest.MockRESTClient)
	mockNas := new(ontapRest.MockNASClient)
	mockClient.On("NAS").Return(mockNas).Once()
	mockNas.On("CifsShareCreate", mock.Anything).Return(errors.New("create failed")).Once()

	ctx := context.Background()
	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{})
	defer cleanup()

	activity := ActiveDirectoryActivity{}
	env.RegisterActivity(activity.CreateJunctionPathForCifsShare)

	_, err := env.ExecuteActivity(activity.CreateJunctionPathForCifsShare, &models.Node{}, "svm", "/junction", []string{})

	require.Error(t, err)
	mockNas.AssertExpectations(t)
}

func TestCreateJunctionPathForCifsShare_WithSMBShareProperties(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockClient := new(ontapRest.MockRESTClient)
	mockNas := new(ontapRest.MockNASClient)
	mockClient.On("NAS").Return(mockNas).Once()

	expectedProperties := []string{"browsable", "encrypt_data", "oplocks"}
	mockNas.On("CifsShareCreate", mock.MatchedBy(func(params *ontapRest.CifsShareCreateParams) bool {
		assert.Equal(t, "svm", *params.SvmName)
		assert.Equal(t, "/test_share", params.Path)
		assert.Equal(t, "test_share", params.Name)
		assert.ElementsMatch(t, expectedProperties, params.ShareProperties)
		return true
	})).Return(nil).Once()

	ctx := context.Background()
	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{})
	defer cleanup()

	activity := ActiveDirectoryActivity{}
	env.RegisterActivity(activity.CreateJunctionPathForCifsShare)

	_, err := env.ExecuteActivity(activity.CreateJunctionPathForCifsShare, &models.Node{}, "svm", "/test_share", expectedProperties)

	require.NoError(t, err)
	mockNas.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestMapCreateCIFSServerError(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantID     int
		wantMatch  bool
	}{
		{"nil error", nil, 0, false},
		{"Invalid Credentials", errors.New("Invalid Credentials"), vsaerrors.ErrADInvalidCredentials, true},
		{"KRB5KDC_ERR_PREAUTH_FAILED", errors.New("KRB5KDC_ERR_PREAUTH_FAILED"), vsaerrors.ErrADInvalidCredentials, true},
		{"password not in sync", errors.New("does not match password stored in Active Directory"), vsaerrors.ErrADPasswordNotInSync, true},
		{"Invalid credentials were given", errors.New("Invalid credentials were given"), vsaerrors.ErrADIncorrectUsername, true},
		{"Username format not supported", errors.New("Username format not supported"), vsaerrors.ErrADIncorrectUsername, true},
		{"Reason: Invalid credentials.", errors.New("Reason: Invalid credentials."), vsaerrors.ErrADIncorrectUsername, true},
		{"credentials have been revoked", errors.New("Clients credentials have been revoked"), vsaerrors.ErrADUserDisabled, true},
		{"KDC has no support for encryption type", errors.New("KDC has no support for encryption type"), vsaerrors.ErrADAESEncryptionSettingsInvalid, true},
		{"msDS-SupportedEncryptionTypes Insufficient access", errors.New("msDS-SupportedEncryptionTypes and Insufficient access"), vsaerrors.ErrADAESEncryptionSettingsInvalid, true},
		{"Failed to bind service principal name on LIF", errors.New("Failed to bind service principal name on LIF"), vsaerrors.ErrADKDCUnreachable, true},
		{"KDC Unreachable Details", errors.New("KDC Unreachable Details"), vsaerrors.ErrADKDCUnreachable, true},
		{"Cannot find any domain controllers", errors.New("Cannot find any domain controllers"), vsaerrors.ErrADDomainControllersUnreachable, true},
		{"no server available SecD", errors.New("no server available SecD"), vsaerrors.ErrADDomainControllersUnreachable, true},
		{"RESULT_ERROR_LDAPSERVER_SERVER_DOWN", errors.New("RESULT_ERROR_LDAPSERVER_SERVER_DOWN Can't contact LDAP server"), vsaerrors.ErrADLDAPUnreachable, true},
		{"ou not found", errors.New("ou not found"), vsaerrors.ErrADInvalidOU, true},
		{"Lookup of organizational_unit failed", errors.New("Lookup of organizational_unit failed"), vsaerrors.ErrADInvalidOU, true},
		{"insufficient access rights", errors.New("insufficient access rights"), vsaerrors.ErrADInsufficientPermission, true},
		{"insufficient privilege", errors.New("insufficient privilege"), vsaerrors.ErrADInsufficientPermission, true},
		{"LDAP constraint", errors.New("LDAP constraint"), vsaerrors.ErrADInsufficientPermission, true},
		{"cannot find the indicated default site", errors.New("cannot find the indicated default site"), vsaerrors.ErrADDefaultSiteInvalid, true},
		{"Unable to connect to NetLogon", errors.New("Unable to connect to NetLogon"), vsaerrors.ErrADNetLogonError, true},
		{"RESULT_ERROR_SPINCLIENT", errors.New("RESULT_ERROR_SPINCLIENT"), vsaerrors.ErrADNetLogonError, true},
		{"Operation timed out domain controllers", errors.New("Operation timed out domain controllers"), vsaerrors.ErrADLDAPNetworkIssue, true},
		{"Unable to connect to any domain controllers", errors.New("Unable to connect to any domain controllers"), vsaerrors.ErrADLDAPNetworkIssue, true},
		{"unmapped error", errors.New("some other error"), 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotID, gotMatch := mapCreateCIFSServerError(tt.err)
			assert.Equal(t, tt.wantID, gotID)
			assert.Equal(t, tt.wantMatch, gotMatch)
		})
	}
}

func TestGetOntapRestProvider_GetProviderByNodeError(t *testing.T) {
	ctx := context.Background()
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return nil, errors.New("provider error")
	}

	_, err := _getOntapRestProvider(ctx, &models.Node{})
	require.Error(t, err)
}

func TestGetOntapRestProvider_ProviderTypeMismatch(t *testing.T) {
	ctx := context.Background()
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		// Return a mock provider that is not OntapRestProvider
		return &vsa.MockProvider{}, nil
	}

	_, err := _getOntapRestProvider(ctx, &models.Node{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "provider is not OntapRestProvider")
}

func TestCreateOrModifyADDNS_GetOntapRestProviderError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	originalGetter := getOntapRestProvider
	defer func() { getOntapRestProvider = originalGetter }()

	getOntapRestProvider = func(context.Context, *models.Node) (*vsa.OntapRestProvider, error) {
		return nil, errors.New("provider error")
	}

	activity := ActiveDirectoryActivity{}
	env.RegisterActivity(activity.CreateOrModifyADDNS)

	ad := &vsa.ActiveDirectory{DNS: "10.0.0.1", Domain: "example.com"}
	_, err := env.ExecuteActivity(activity.CreateOrModifyADDNS, &models.Node{}, ad, "svm", "svm-uuid")
	require.Error(t, err)
}

func TestCreateOrModifyADDNS_CreateRESTClientError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	originalGetter := getOntapRestProvider
	defer func() { getOntapRestProvider = originalGetter }()

	ctx := context.Background()
	logger := util.GetLogger(ctx)
	provider := &vsa.OntapRestProvider{
		ClientParams: ontapRest.RESTClientParams{Trace: logger, Ctx: ctx},
		Logger:       logger,
	}

	getOntapRestProvider = func(context.Context, *models.Node) (*vsa.OntapRestProvider, error) {
		return provider, nil
	}

	originalHooks := vsa.SetTestHooks(vsa.TestHooks{
		GetOntapClient: func(ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
			return nil, errors.New("client creation failed")
		},
	})
	defer originalHooks()

	activity := ActiveDirectoryActivity{}
	env.RegisterActivity(activity.CreateOrModifyADDNS)

	ad := &vsa.ActiveDirectory{DNS: "10.0.0.1", Domain: "example.com"}
	_, err := env.ExecuteActivity(activity.CreateOrModifyADDNS, &models.Node{}, ad, "svm", "svm-uuid")
	require.Error(t, err)
}

func TestCreateOrModifyADDNS_EnsureCifsServerNamePostFixError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	ctx := context.Background()
	mockClient := new(ontapRest.MockRESTClient)

	originalHooks := vsa.SetTestHooks(vsa.TestHooks{
		GetOntapClient: func(ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
			return mockClient, nil
		},
		EnsureCifsServerNamePostFix: func(log.Logger, ontapRest.RESTClient, *vsa.ActiveDirectory, string) error {
			return errors.New("ensure postfix failed")
		},
	})
	defer originalHooks()

	originalGetter := getOntapRestProvider
	defer func() { getOntapRestProvider = originalGetter }()

	logger := util.GetLogger(ctx)
	provider := &vsa.OntapRestProvider{
		ClientParams: ontapRest.RESTClientParams{Trace: logger, Ctx: ctx},
		Logger:       logger,
	}

	getOntapRestProvider = func(context.Context, *models.Node) (*vsa.OntapRestProvider, error) {
		return provider, nil
	}

	activity := ActiveDirectoryActivity{}
	env.RegisterActivity(activity.CreateOrModifyADDNS)

	ad := &vsa.ActiveDirectory{DNS: "10.0.0.1", Domain: "example.com"}
	_, err := env.ExecuteActivity(activity.CreateOrModifyADDNS, &models.Node{}, ad, "svm", "svm-uuid")
	require.Error(t, err)
}

func TestCreateOrModifyADDNS_DNSGetError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockClient := new(ontapRest.MockRESTClient)
	mockNameSvc := new(ontapRest.MockNameServicesClient)
	mockClient.On("NameServices").Return(mockNameSvc).Once()
	mockNameSvc.On("DNSGet", mock.Anything).Return(nil, errors.New("dns get failed")).Once()

	ctx := context.Background()
	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{})
	defer cleanup()

	activity := ActiveDirectoryActivity{}
	env.RegisterActivity(activity.CreateOrModifyADDNS)

	ad := &vsa.ActiveDirectory{DNS: "10.0.0.1", Domain: "example.com"}
	_, err := env.ExecuteActivity(activity.CreateOrModifyADDNS, &models.Node{}, ad, "svm", "svm-uuid")
	require.Error(t, err)
	mockClient.AssertExpectations(t)
	mockNameSvc.AssertExpectations(t)
}

func TestCreateOrModifyADDNS_DnsCreateError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockClient := new(ontapRest.MockRESTClient)
	mockNameSvc := new(ontapRest.MockNameServicesClient)
	mockClient.On("NameServices").Return(mockNameSvc).Times(2)
	mockNameSvc.On("DNSGet", mock.Anything).Return((*ontapRest.DNS)(nil), utilerrors.NewNotFoundErr("dns", nil)).Once()
	mockNameSvc.On("DnsCreate", mock.Anything).Return(nil, errors.New("dns create failed")).Once()

	ctx := context.Background()
	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{})
	defer cleanup()

	activity := ActiveDirectoryActivity{}
	env.RegisterActivity(activity.CreateOrModifyADDNS)

	ad := &vsa.ActiveDirectory{DNS: "10.0.0.1", Domain: "example.com"}
	_, err := env.ExecuteActivity(activity.CreateOrModifyADDNS, &models.Node{}, ad, "svm", "svm-uuid")
	require.Error(t, err)
	mockClient.AssertExpectations(t)
	mockNameSvc.AssertExpectations(t)
}

func TestCreateOrModifyADDNS_DnsCreateError_DNSServerUnreachable(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockClient := new(ontapRest.MockRESTClient)
	mockNameSvc := new(ontapRest.MockNameServicesClient)
	mockClient.On("NameServices").Return(mockNameSvc).Times(2)
	mockNameSvc.On("DNSGet", mock.Anything).Return((*ontapRest.DNS)(nil), utilerrors.NewNotFoundErr("dns", nil)).Once()

	// Simulate DNS server unreachable error from ONTAP
	dnsUnreachableErr := errors.New(`The DNS specified for SVM "gcnv-test-svm-01" cannot be reached. Reason: 10.150.0.2: Operation timed out`)
	mockNameSvc.On("DnsCreate", mock.Anything).Return(nil, dnsUnreachableErr).Once()

	ctx := context.Background()
	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{})
	defer cleanup()

	activity := ActiveDirectoryActivity{}
	env.RegisterActivity(activity.CreateOrModifyADDNS)

	ad := &vsa.ActiveDirectory{DNS: "10.150.0.2", Domain: "example.com"}
	_, err := env.ExecuteActivity(activity.CreateOrModifyADDNS, &models.Node{}, ad, "svm", "svm-uuid")

	require.Error(t, err)
	customErr := vsaerrors.ExtractCustomError(err)
	require.NotNil(t, customErr)
	assert.Equal(t, vsaerrors.ErrDNSServerUnreachable, customErr.TrackingID)
	assert.Equal(t, "The DNS IP address specified in your Active Directory policy cannot be reached. Make sure the DNS IP is correct and the firewall on your DNS server allows access.", customErr.Error())
	mockClient.AssertExpectations(t)
	mockNameSvc.AssertExpectations(t)
}

func TestCreateOrModifyADDNS_DnsCreateError_DNSServerUnreachable_ConnectionRefused(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockClient := new(ontapRest.MockRESTClient)
	mockNameSvc := new(ontapRest.MockNameServicesClient)
	mockClient.On("NameServices").Return(mockNameSvc).Times(2)
	mockNameSvc.On("DNSGet", mock.Anything).Return((*ontapRest.DNS)(nil), utilerrors.NewNotFoundErr("dns", nil)).Once()

	// Simulate another variant of DNS unreachable error
	dnsUnreachableErr := errors.New(`The DNS server cannot be reached. Connection refused.`)
	mockNameSvc.On("DnsCreate", mock.Anything).Return(nil, dnsUnreachableErr).Once()

	ctx := context.Background()
	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{})
	defer cleanup()

	activity := ActiveDirectoryActivity{}
	env.RegisterActivity(activity.CreateOrModifyADDNS)

	ad := &vsa.ActiveDirectory{DNS: "10.150.0.2", Domain: "example.com"}
	_, err := env.ExecuteActivity(activity.CreateOrModifyADDNS, &models.Node{}, ad, "svm", "svm-uuid")

	require.Error(t, err)
	customErr := vsaerrors.ExtractCustomError(err)
	require.NotNil(t, customErr)
	assert.Equal(t, vsaerrors.ErrDNSServerUnreachable, customErr.TrackingID)
	assert.Equal(t, "The DNS IP address specified in your Active Directory policy cannot be reached. Make sure the DNS IP is correct and the firewall on your DNS server allows access.", customErr.Error())
	mockClient.AssertExpectations(t)
	mockNameSvc.AssertExpectations(t)
}

func TestCreateOrModifyADDNS_DnsCreateError_OtherError_NotDNSUnreachable(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockClient := new(ontapRest.MockRESTClient)
	mockNameSvc := new(ontapRest.MockNameServicesClient)
	mockClient.On("NameServices").Return(mockNameSvc).Times(2)
	mockNameSvc.On("DNSGet", mock.Anything).Return((*ontapRest.DNS)(nil), utilerrors.NewNotFoundErr("dns", nil)).Once()

	// Simulate a different DNS error that is NOT "cannot be reached"
	otherDnsErr := errors.New(`Domain name cannot be an IP address`)
	mockNameSvc.On("DnsCreate", mock.Anything).Return(nil, otherDnsErr).Once()

	ctx := context.Background()
	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{})
	defer cleanup()

	activity := ActiveDirectoryActivity{}
	env.RegisterActivity(activity.CreateOrModifyADDNS)

	ad := &vsa.ActiveDirectory{DNS: "10.150.0.2", Domain: "192.168.1.1"}
	_, err := env.ExecuteActivity(activity.CreateOrModifyADDNS, &models.Node{}, ad, "svm", "svm-uuid")

	require.Error(t, err)
	customErr := vsaerrors.ExtractCustomError(err)
	require.NotNil(t, customErr)
	assert.NotEqual(t, vsaerrors.ErrDNSServerUnreachable, customErr.TrackingID)
	mockClient.AssertExpectations(t)
	mockNameSvc.AssertExpectations(t)
}

func TestCreateOrModifyADDNS_DNSModifyError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockClient := new(ontapRest.MockRESTClient)
	mockNameSvc := new(ontapRest.MockNameServicesClient)
	mockClient.On("NameServices").Return(mockNameSvc).Times(2)

	existing := &ontapRest.DNS{
		DNS: ontaprestmodels.DNS{
			Servers: ontaprestmodels.NameServersArrayInline{strPtr("1.1.1.1")},
			Domains: ontaprestmodels.DNSDomainsArrayInline{strPtr("old.example.com")},
		},
	}
	mockNameSvc.On("DNSGet", mock.Anything).Return(existing, nil).Once()
	mockNameSvc.On("DNSModify", mock.Anything).Return(errors.New("dns modify failed")).Once()

	ctx := context.Background()
	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{})
	defer cleanup()

	activity := ActiveDirectoryActivity{}
	env.RegisterActivity(activity.CreateOrModifyADDNS)

	ad := &vsa.ActiveDirectory{DNS: "2.2.2.2", Domain: "example.com"}
	_, err := env.ExecuteActivity(activity.CreateOrModifyADDNS, &models.Node{}, ad, "svm", "svm-uuid")
	require.Error(t, err)
	mockClient.AssertExpectations(t)
	mockNameSvc.AssertExpectations(t)
}

func TestGetOrCreateCifsService_GetOntapRestProviderError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	originalGetter := getOntapRestProvider
	defer func() { getOntapRestProvider = originalGetter }()

	getOntapRestProvider = func(context.Context, *models.Node) (*vsa.OntapRestProvider, error) {
		return nil, errors.New("provider error")
	}

	activity := ActiveDirectoryActivity{}
	env.RegisterActivity(activity.GetOrCreateCifsService)

	ad := &vsa.ActiveDirectory{Domain: "example.com"}
	_, err := env.ExecuteActivity(activity.GetOrCreateCifsService, &models.Node{}, ad, "svm", "svm-uuid")
	require.Error(t, err)
}

func TestGetOrCreateCifsService_CreateRESTClientError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	originalGetter := getOntapRestProvider
	defer func() { getOntapRestProvider = originalGetter }()

	// Mock password decryption
	originalDecryptPassword := utils.DecryptPassword
	utils.DecryptPassword = func(password log.Secret) (*string, error) {
		decrypted := "decrypted-password"
		return &decrypted, nil
	}
	defer func() { utils.DecryptPassword = originalDecryptPassword }()

	ctx := context.Background()
	logger := util.GetLogger(ctx)
	provider := &vsa.OntapRestProvider{
		ClientParams: ontapRest.RESTClientParams{Trace: logger, Ctx: ctx},
		Logger:       logger,
	}

	getOntapRestProvider = func(context.Context, *models.Node) (*vsa.OntapRestProvider, error) {
		return provider, nil
	}

	originalHooks := vsa.SetTestHooks(vsa.TestHooks{
		GetOntapClient: func(ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
			return nil, errors.New("client creation failed")
		},
	})
	defer originalHooks()

	activity := ActiveDirectoryActivity{}
	env.RegisterActivity(activity.GetOrCreateCifsService)

	ad := &vsa.ActiveDirectory{Domain: "example.com"}
	_, err := env.ExecuteActivity(activity.GetOrCreateCifsService, &models.Node{}, ad, "svm", "svm-uuid")
	require.Error(t, err)
}

func TestGetOrCreateCifsService_EnsureCifsServerNamePostFixError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	ctx := context.Background()
	mockClient := new(ontapRest.MockRESTClient)

	// Mock password decryption
	originalDecryptPassword := utils.DecryptPassword
	utils.DecryptPassword = func(password log.Secret) (*string, error) {
		decrypted := "decrypted-password"
		return &decrypted, nil
	}
	defer func() { utils.DecryptPassword = originalDecryptPassword }()

	originalHooks := vsa.SetTestHooks(vsa.TestHooks{
		GetOntapClient: func(ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
			return mockClient, nil
		},
		EnsureCifsServerNamePostFix: func(log.Logger, ontapRest.RESTClient, *vsa.ActiveDirectory, string) error {
			return errors.New("ensure postfix failed")
		},
	})
	defer originalHooks()

	originalGetter := getOntapRestProvider
	defer func() { getOntapRestProvider = originalGetter }()

	logger := util.GetLogger(ctx)
	provider := &vsa.OntapRestProvider{
		ClientParams: ontapRest.RESTClientParams{Trace: logger, Ctx: ctx},
		Logger:       logger,
	}

	getOntapRestProvider = func(context.Context, *models.Node) (*vsa.OntapRestProvider, error) {
		return provider, nil
	}

	activity := ActiveDirectoryActivity{}
	env.RegisterActivity(activity.GetOrCreateCifsService)

	ad := &vsa.ActiveDirectory{Domain: "example.com"}
	_, err := env.ExecuteActivity(activity.GetOrCreateCifsService, &models.Node{}, ad, "svm", "svm-uuid")
	require.Error(t, err)
}

func TestGetOrCreateCifsService_CifsServiceGetError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	// Mock password decryption
	originalDecryptPassword := utils.DecryptPassword
	utils.DecryptPassword = func(password log.Secret) (*string, error) {
		decrypted := "decrypted-password"
		return &decrypted, nil
	}
	defer func() { utils.DecryptPassword = originalDecryptPassword }()

	mockClient := new(ontapRest.MockRESTClient)
	mockNas := new(ontapRest.MockNASClient)
	mockClient.On("NAS").Return(mockNas).Once()
	mockNas.On("CifsServiceGet", mock.Anything).Return(nil, errors.New("cifs get failed")).Once()

	ctx := context.Background()
	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{})
	defer cleanup()

	activity := ActiveDirectoryActivity{}
	env.RegisterActivity(activity.GetOrCreateCifsService)

	ad := &vsa.ActiveDirectory{Domain: "example.com"}
	_, err := env.ExecuteActivity(activity.GetOrCreateCifsService, &models.Node{}, ad, "svm", "svm-uuid")
	require.Error(t, err)
	mockNas.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestGetOrCreateCifsService_CreateAndSetupCIFSServerError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	// Mock password decryption
	originalDecryptPassword := utils.DecryptPassword
	utils.DecryptPassword = func(password log.Secret) (*string, error) {
		decrypted := "decrypted-password"
		return &decrypted, nil
	}
	defer func() { utils.DecryptPassword = originalDecryptPassword }()

	mockClient := new(ontapRest.MockRESTClient)
	mockNas := new(ontapRest.MockNASClient)
	mockClient.On("NAS").Return(mockNas).Once()
	mockNas.On("CifsServiceGet", mock.Anything).Return(nil, utilerrors.NewNotFoundErr("cifs service", nil)).Once()

	ctx := context.Background()
	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{
		CreateAndSetupCIFSServer: func(_ log.Logger, _ ontapRest.RESTClient, _ *vsa.ActiveDirectory, _, _ string) (string, error) {
			return "", errors.New("create failed")
		},
	})
	defer cleanup()

	activity := ActiveDirectoryActivity{}
	env.RegisterActivity(activity.GetOrCreateCifsService)

	ad := &vsa.ActiveDirectory{Domain: "example.com"}
	_, err := env.ExecuteActivity(activity.GetOrCreateCifsService, &models.Node{}, ad, "svm", "svm-uuid")
	require.Error(t, err)
	customErr := vsaerrors.ExtractCustomError(err)
	require.NotNil(t, customErr)
	assert.NotEqual(t, vsaerrors.ErrADInvalidCredentials, customErr.TrackingID, "generic create error should not be mapped to ErrADInvalidCredentials")
	mockNas.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestGetOrCreateCifsService_CreateAndSetupCIFSServerError_InvalidCredentials(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	originalDecryptPassword := utils.DecryptPassword
	utils.DecryptPassword = func(password log.Secret) (*string, error) {
		decrypted := "decrypted-password"
		return &decrypted, nil
	}
	defer func() { utils.DecryptPassword = originalDecryptPassword }()

	mockClient := new(ontapRest.MockRESTClient)
	mockNas := new(ontapRest.MockNASClient)
	mockClient.On("NAS").Return(mockNas).Once()
	mockNas.On("CifsServiceGet", mock.Anything).Return(nil, utilerrors.NewNotFoundErr("cifs service", nil)).Once()

	invalidCredsErr := errors.New("Failed to create the Active Directory machine account \"MULTIAD1-7D9A\". Reason: Kerberos Error: Pre-authentication information was invalid Details: Error: Machine account creation procedure failed FAILURE: Could not authenticate as 'Administrator@MULTIAD1.COM': Invalid Credentials (KRB5KDC_ERR_PREAUTH_FAILED).")
	ctx := context.Background()
	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{
		CreateAndSetupCIFSServer: func(_ log.Logger, _ ontapRest.RESTClient, _ *vsa.ActiveDirectory, _, _ string) (string, error) {
			return "", invalidCredsErr
		},
	})
	defer cleanup()

	activity := ActiveDirectoryActivity{}
	env.RegisterActivity(activity.GetOrCreateCifsService)

	ad := &vsa.ActiveDirectory{Domain: "example.com"}
	_, err := env.ExecuteActivity(activity.GetOrCreateCifsService, &models.Node{}, ad, "svm", "svm-uuid")
	require.Error(t, err)
	customErr := vsaerrors.ExtractCustomError(err)
	require.NotNil(t, customErr)
	assert.Equal(t, vsaerrors.ErrADInvalidCredentials, customErr.TrackingID)
	assert.Equal(t, "Active Directory credentials are invalid. Verify the username and password in your Active Directory configuration.", customErr.Error())
	mockNas.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestGetOrCreateCifsService_CreateAndSetupCIFSServerError_PreauthFailed(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	originalDecryptPassword := utils.DecryptPassword
	utils.DecryptPassword = func(password log.Secret) (*string, error) {
		decrypted := "decrypted-password"
		return &decrypted, nil
	}
	defer func() { utils.DecryptPassword = originalDecryptPassword }()

	mockClient := new(ontapRest.MockRESTClient)
	mockNas := new(ontapRest.MockNASClient)
	mockClient.On("NAS").Return(mockNas).Once()
	mockNas.On("CifsServiceGet", mock.Anything).Return(nil, utilerrors.NewNotFoundErr("cifs service", nil)).Once()

	preauthErr := errors.New("KRB5KDC_ERR_PREAUTH_FAILED: wrong password")
	ctx := context.Background()
	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{
		CreateAndSetupCIFSServer: func(_ log.Logger, _ ontapRest.RESTClient, _ *vsa.ActiveDirectory, _, _ string) (string, error) {
			return "", preauthErr
		},
	})
	defer cleanup()

	activity := ActiveDirectoryActivity{}
	env.RegisterActivity(activity.GetOrCreateCifsService)

	ad := &vsa.ActiveDirectory{Domain: "example.com"}
	_, err := env.ExecuteActivity(activity.GetOrCreateCifsService, &models.Node{}, ad, "svm", "svm-uuid")
	require.Error(t, err)
	customErr := vsaerrors.ExtractCustomError(err)
	require.NotNil(t, customErr)
	assert.Equal(t, vsaerrors.ErrADInvalidCredentials, customErr.TrackingID)
	assert.Equal(t, "Active Directory credentials are invalid. Verify the username and password in your Active Directory configuration.", customErr.Error())
	mockNas.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestGetOrCreateCifsService_CreateAndSetupCIFSServerError_PasswordNotInSync(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	originalDecryptPassword := utils.DecryptPassword
	utils.DecryptPassword = func(password log.Secret) (*string, error) {
		decrypted := "decrypted-password"
		return &decrypted, nil
	}
	defer func() { utils.DecryptPassword = originalDecryptPassword }()

	mockClient := new(ontapRest.MockRESTClient)
	mockNas := new(ontapRest.MockNASClient)
	mockClient.On("NAS").Return(mockNas).Once()
	mockNas.On("CifsServiceGet", mock.Anything).Return(nil, utilerrors.NewNotFoundErr("cifs service", nil)).Once()

	passwordNotInSyncErr := errors.New("does not match password stored in Active Directory")
	ctx := context.Background()
	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{
		CreateAndSetupCIFSServer: func(_ log.Logger, _ ontapRest.RESTClient, _ *vsa.ActiveDirectory, _, _ string) (string, error) {
			return "", passwordNotInSyncErr
		},
	})
	defer cleanup()

	activity := ActiveDirectoryActivity{}
	env.RegisterActivity(activity.GetOrCreateCifsService)

	ad := &vsa.ActiveDirectory{Domain: "example.com"}
	_, err := env.ExecuteActivity(activity.GetOrCreateCifsService, &models.Node{}, ad, "svm", "svm-uuid")
	require.Error(t, err)
	customErr := vsaerrors.ExtractCustomError(err)
	require.NotNil(t, customErr)
	assert.Equal(t, vsaerrors.ErrADPasswordNotInSync, customErr.TrackingID)
	mockNas.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestGetOrCreateCifsService_CreateAndSetupCIFSServerError_KDCUnreachable(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	originalDecryptPassword := utils.DecryptPassword
	utils.DecryptPassword = func(password log.Secret) (*string, error) {
		decrypted := "decrypted-password"
		return &decrypted, nil
	}
	defer func() { utils.DecryptPassword = originalDecryptPassword }()

	mockClient := new(ontapRest.MockRESTClient)
	mockNas := new(ontapRest.MockNASClient)
	mockClient.On("NAS").Return(mockNas).Once()
	mockNas.On("CifsServiceGet", mock.Anything).Return(nil, utilerrors.NewNotFoundErr("cifs service", nil)).Once()

	kdcUnreachableErr := errors.New("Failed to bind service principal name on LIF")
	ctx := context.Background()
	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{
		CreateAndSetupCIFSServer: func(_ log.Logger, _ ontapRest.RESTClient, _ *vsa.ActiveDirectory, _, _ string) (string, error) {
			return "", kdcUnreachableErr
		},
	})
	defer cleanup()

	activity := ActiveDirectoryActivity{}
	env.RegisterActivity(activity.GetOrCreateCifsService)

	ad := &vsa.ActiveDirectory{Domain: "example.com"}
	_, err := env.ExecuteActivity(activity.GetOrCreateCifsService, &models.Node{}, ad, "svm", "svm-uuid")
	require.Error(t, err)
	customErr := vsaerrors.ExtractCustomError(err)
	require.NotNil(t, customErr)
	assert.Equal(t, vsaerrors.ErrADKDCUnreachable, customErr.TrackingID)
	mockNas.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestDdnsModify_GetOntapRestProviderError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	originalGetter := getOntapRestProvider
	defer func() { getOntapRestProvider = originalGetter }()

	getOntapRestProvider = func(context.Context, *models.Node) (*vsa.OntapRestProvider, error) {
		return nil, errors.New("provider error")
	}

	activity := ActiveDirectoryActivity{}
	env.RegisterActivity(activity.DdnsModify)

	_, err := env.ExecuteActivity(activity.DdnsModify, &models.Node{}, "svm-uuid", "fqdn.example.com")
	require.Error(t, err)
}

func TestDdnsModify_CreateRESTClientError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	originalGetter := getOntapRestProvider
	defer func() { getOntapRestProvider = originalGetter }()

	ctx := context.Background()
	logger := util.GetLogger(ctx)
	provider := &vsa.OntapRestProvider{
		ClientParams: ontapRest.RESTClientParams{Trace: logger, Ctx: ctx},
		Logger:       logger,
	}

	getOntapRestProvider = func(context.Context, *models.Node) (*vsa.OntapRestProvider, error) {
		return provider, nil
	}

	originalHooks := vsa.SetTestHooks(vsa.TestHooks{
		GetOntapClient: func(ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
			return nil, errors.New("client creation failed")
		},
	})
	defer originalHooks()

	activity := ActiveDirectoryActivity{}
	env.RegisterActivity(activity.DdnsModify)

	_, err := env.ExecuteActivity(activity.DdnsModify, &models.Node{}, "svm-uuid", "fqdn.example.com")
	require.Error(t, err)
}

func TestDdnsModify_DNSModifyError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockClient := new(ontapRest.MockRESTClient)
	mockNameSvc := new(ontapRest.MockNameServicesClient)
	mockClient.On("NameServices").Return(mockNameSvc).Once()
	mockNameSvc.On("DNSModify", mock.Anything).Return(errors.New("dns modify failed")).Once()

	ctx := context.Background()
	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{})
	defer cleanup()

	activity := ActiveDirectoryActivity{}
	env.RegisterActivity(activity.DdnsModify)

	_, err := env.ExecuteActivity(activity.DdnsModify, &models.Node{}, "svm-uuid", "fqdn.example.com")
	require.Error(t, err)
	mockClient.AssertExpectations(t)
	mockNameSvc.AssertExpectations(t)
}

func TestCreateJunctionPathForCifsShare_GetOntapRestProviderError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	originalGetter := getOntapRestProvider
	defer func() { getOntapRestProvider = originalGetter }()

	getOntapRestProvider = func(context.Context, *models.Node) (*vsa.OntapRestProvider, error) {
		return nil, errors.New("provider error")
	}

	activity := ActiveDirectoryActivity{}
	env.RegisterActivity(activity.CreateJunctionPathForCifsShare)

	_, err := env.ExecuteActivity(activity.CreateJunctionPathForCifsShare, &models.Node{}, "svm", "/junction", []string{})
	require.Error(t, err)
}

func TestCreateJunctionPathForCifsShare_CreateRESTClientError(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	originalGetter := getOntapRestProvider
	defer func() { getOntapRestProvider = originalGetter }()

	ctx := context.Background()
	logger := util.GetLogger(ctx)
	provider := &vsa.OntapRestProvider{
		ClientParams: ontapRest.RESTClientParams{Trace: logger, Ctx: ctx},
		Logger:       logger,
	}

	getOntapRestProvider = func(context.Context, *models.Node) (*vsa.OntapRestProvider, error) {
		return provider, nil
	}

	originalHooks := vsa.SetTestHooks(vsa.TestHooks{
		GetOntapClient: func(ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
			return nil, errors.New("client creation failed")
		},
	})
	defer originalHooks()

	activity := ActiveDirectoryActivity{}
	env.RegisterActivity(activity.CreateJunctionPathForCifsShare)

	_, err := env.ExecuteActivity(activity.CreateJunctionPathForCifsShare, &models.Node{}, "svm", "/junction", []string{})
	require.Error(t, err)
}
