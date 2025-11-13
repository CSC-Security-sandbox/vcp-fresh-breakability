package active_directory_activities

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	ontaprestmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	utilerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
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
	ctx := context.Background()

	mockClient := new(ontapRest.MockRESTClient)
	mockNameSvc := new(ontapRest.MockNameServicesClient)
	mockClient.On("NameServices").Return(mockNameSvc).Times(2)
	mockNameSvc.On("DNSGet", mock.Anything).Return((*ontapRest.DNS)(nil), nil).Once()
	mockNameSvc.On("DnsCreate", mock.Anything).Return((*ontaprestmodels.DNSResponse)(nil), nil).Once()

	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{})
	defer cleanup()

	ad := &vsa.ActiveDirectory{DNS: "10.0.0.1,20.0.0.2", Domain: "example.com"}
	err := (ActiveDirectoryActivity{}).CreateOrModifyADDNS(ctx, &models.Node{}, ad, "svm", "svm-uuid")

	require.NoError(t, err)
	mockClient.AssertExpectations(t)
	mockNameSvc.AssertExpectations(t)
}

func TestCreateOrModifyADDNS_ModifiesWhenConfigDiffers(t *testing.T) {
	ctx := context.Background()

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

	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{})
	defer cleanup()

	ad := &vsa.ActiveDirectory{DNS: "2.2.2.2,3.3.3.3", Domain: "example.com"}
	err := (ActiveDirectoryActivity{}).CreateOrModifyADDNS(ctx, &models.Node{}, ad, "svm", "svm-uuid")

	require.NoError(t, err)
	mockClient.AssertExpectations(t)
	mockNameSvc.AssertExpectations(t)
}

func TestCreateOrModifyADDNS_NoChangeWhenConfigMatches(t *testing.T) {
	ctx := context.Background()

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

	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{})
	defer cleanup()

	ad := &vsa.ActiveDirectory{DNS: "9.9.9.9,8.8.8.8", Domain: "example.com"}
	err := (ActiveDirectoryActivity{}).CreateOrModifyADDNS(ctx, &models.Node{}, ad, "svm", "svm-uuid")

	require.NoError(t, err)
	mockClient.AssertExpectations(t)
	mockNameSvc.AssertExpectations(t)
	mockNameSvc.AssertNotCalled(t, "DNSModify", mock.Anything)
}

func TestGetOrCreateCifsService_CreatesWhenMissing(t *testing.T) {
	ctx := context.Background()

	mockClient := new(ontapRest.MockRESTClient)
	mockNas := new(ontapRest.MockNASClient)
	mockClient.On("NAS").Return(mockNas).Once()

	mockNas.On("CifsServiceGet", mock.Anything).Return((*ontapRest.CifsService)(nil), utilerrors.NewNotFoundErr("cifs service", nil)).Once()

	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{
		CreateAndSetupCIFSServer: func(_ log.Logger, _ ontapRest.RESTClient, _ *vsa.ActiveDirectory, _, _ string) (string, error) {
			return "created.example.com", nil
		},
		IsDDNSEnabled: func(log.Logger, ontapRest.RESTClient, string) bool { return false },
	})
	defer cleanup()

	ad := &vsa.ActiveDirectory{Domain: "example.com"}
	result, err := (ActiveDirectoryActivity{}).GetOrCreateCifsService(ctx, &models.Node{}, ad, "svm", "svm-uuid")

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, "created.example.com", result.FQDN)
	assert.False(t, result.NeedsDDNS)
	mockNas.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestGetOrCreateCifsService_ReturnsExistingRequestsDDNS(t *testing.T) {
	ctx := context.Background()

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

	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{
		IsDDNSEnabled: func(log.Logger, ontapRest.RESTClient, string) bool { return false },
	})
	defer cleanup()

	ad := &vsa.ActiveDirectory{Domain: "example.com"}
	result, err := (ActiveDirectoryActivity{}).GetOrCreateCifsService(ctx, &models.Node{}, ad, "svm", "svm-uuid")

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.NeedsDDNS)
	assert.Equal(t, name, result.CifsServiceName)
	assert.Equal(t, fqdn, result.AdDomain)
	mockNas.AssertExpectations(t)
}

func TestGetOrCreateCifsService_ReturnsExistingNoDDNS(t *testing.T) {
	ctx := context.Background()

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

	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{
		IsDDNSEnabled: func(log.Logger, ontapRest.RESTClient, string) bool { return true },
	})
	defer cleanup()

	ad := &vsa.ActiveDirectory{Domain: "example.com"}
	result, err := (ActiveDirectoryActivity{}).GetOrCreateCifsService(ctx, &models.Node{}, ad, "svm", "svm-uuid")

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.NeedsDDNS)
	mockNas.AssertExpectations(t)
}

func TestDdnsModify_SetsSecureDDNS(t *testing.T) {
	ctx := context.Background()

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

	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{})
	defer cleanup()

	err := (ActiveDirectoryActivity{}).DdnsModify(ctx, &models.Node{}, "svm-uuid", "fqdn.example.com")

	require.NoError(t, err)
	mockClient.AssertExpectations(t)
	mockNameSvc.AssertExpectations(t)
}

func TestCreateJunctionPathForCifsShare_Succeeds(t *testing.T) {
	ctx := context.Background()

	mockClient := new(ontapRest.MockRESTClient)
	mockNas := new(ontapRest.MockNASClient)
	mockClient.On("NAS").Return(mockNas).Once()
	mockNas.On("CifsShareCreate", mock.MatchedBy(func(params *ontapRest.CifsShareCreateParams) bool {
		assert.Equal(t, "svm", *params.SvmName)
		assert.Equal(t, "/junction", params.Path)
		assert.Equal(t, "junction", params.Name)
		return true
	})).Return(nil).Once()

	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{})
	defer cleanup()

	err := (ActiveDirectoryActivity{}).CreateJunctionPathForCifsShare(ctx, &models.Node{}, "svm", "/junction")

	require.NoError(t, err)
	mockNas.AssertExpectations(t)
}

func TestCreateJunctionPathForCifsShare_PropagatesError(t *testing.T) {
	ctx := context.Background()

	mockClient := new(ontapRest.MockRESTClient)
	mockNas := new(ontapRest.MockNASClient)
	mockClient.On("NAS").Return(mockNas).Once()
	mockNas.On("CifsShareCreate", mock.Anything).Return(errors.New("create failed")).Once()

	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{})
	defer cleanup()

	err := (ActiveDirectoryActivity{}).CreateJunctionPathForCifsShare(ctx, &models.Node{}, "svm", "/junction")

	require.Error(t, err)
	mockNas.AssertExpectations(t)
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
	ctx := context.Background()
	originalGetter := getOntapRestProvider
	defer func() { getOntapRestProvider = originalGetter }()

	getOntapRestProvider = func(context.Context, *models.Node) (*vsa.OntapRestProvider, error) {
		return nil, errors.New("provider error")
	}

	ad := &vsa.ActiveDirectory{DNS: "10.0.0.1", Domain: "example.com"}
	err := (ActiveDirectoryActivity{}).CreateOrModifyADDNS(ctx, &models.Node{}, ad, "svm", "svm-uuid")
	require.Error(t, err)
}

func TestCreateOrModifyADDNS_CreateRESTClientError(t *testing.T) {
	ctx := context.Background()
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

	originalHooks := vsa.SetTestHooks(vsa.TestHooks{
		GetOntapClient: func(ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
			return nil, errors.New("client creation failed")
		},
	})
	defer originalHooks()

	ad := &vsa.ActiveDirectory{DNS: "10.0.0.1", Domain: "example.com"}
	err := (ActiveDirectoryActivity{}).CreateOrModifyADDNS(ctx, &models.Node{}, ad, "svm", "svm-uuid")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get ONTAP client")
}

func TestCreateOrModifyADDNS_EnsureCifsServerNamePostFixError(t *testing.T) {
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

	ad := &vsa.ActiveDirectory{DNS: "10.0.0.1", Domain: "example.com"}
	err := (ActiveDirectoryActivity{}).CreateOrModifyADDNS(ctx, &models.Node{}, ad, "svm", "svm-uuid")
	require.Error(t, err)
}

func TestCreateOrModifyADDNS_DNSGetError(t *testing.T) {
	ctx := context.Background()
	mockClient := new(ontapRest.MockRESTClient)
	mockNameSvc := new(ontapRest.MockNameServicesClient)
	mockClient.On("NameServices").Return(mockNameSvc).Once()
	mockNameSvc.On("DNSGet", mock.Anything).Return(nil, errors.New("dns get failed")).Once()

	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{})
	defer cleanup()

	ad := &vsa.ActiveDirectory{DNS: "10.0.0.1", Domain: "example.com"}
	err := (ActiveDirectoryActivity{}).CreateOrModifyADDNS(ctx, &models.Node{}, ad, "svm", "svm-uuid")
	require.Error(t, err)
	mockClient.AssertExpectations(t)
	mockNameSvc.AssertExpectations(t)
}

func TestCreateOrModifyADDNS_DnsCreateError(t *testing.T) {
	ctx := context.Background()
	mockClient := new(ontapRest.MockRESTClient)
	mockNameSvc := new(ontapRest.MockNameServicesClient)
	mockClient.On("NameServices").Return(mockNameSvc).Times(2)
	mockNameSvc.On("DNSGet", mock.Anything).Return((*ontapRest.DNS)(nil), utilerrors.NewNotFoundErr("dns", nil)).Once()
	mockNameSvc.On("DnsCreate", mock.Anything).Return(nil, errors.New("dns create failed")).Once()

	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{})
	defer cleanup()

	ad := &vsa.ActiveDirectory{DNS: "10.0.0.1", Domain: "example.com"}
	err := (ActiveDirectoryActivity{}).CreateOrModifyADDNS(ctx, &models.Node{}, ad, "svm", "svm-uuid")
	require.Error(t, err)
	mockClient.AssertExpectations(t)
	mockNameSvc.AssertExpectations(t)
}

func TestCreateOrModifyADDNS_DNSModifyError(t *testing.T) {
	ctx := context.Background()
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

	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{})
	defer cleanup()

	ad := &vsa.ActiveDirectory{DNS: "2.2.2.2", Domain: "example.com"}
	err := (ActiveDirectoryActivity{}).CreateOrModifyADDNS(ctx, &models.Node{}, ad, "svm", "svm-uuid")
	require.Error(t, err)
	mockClient.AssertExpectations(t)
	mockNameSvc.AssertExpectations(t)
}

func TestGetOrCreateCifsService_GetOntapRestProviderError(t *testing.T) {
	ctx := context.Background()
	originalGetter := getOntapRestProvider
	defer func() { getOntapRestProvider = originalGetter }()

	getOntapRestProvider = func(context.Context, *models.Node) (*vsa.OntapRestProvider, error) {
		return nil, errors.New("provider error")
	}

	ad := &vsa.ActiveDirectory{Domain: "example.com"}
	_, err := (ActiveDirectoryActivity{}).GetOrCreateCifsService(ctx, &models.Node{}, ad, "svm", "svm-uuid")
	require.Error(t, err)
}

func TestGetOrCreateCifsService_CreateRESTClientError(t *testing.T) {
	ctx := context.Background()
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

	originalHooks := vsa.SetTestHooks(vsa.TestHooks{
		GetOntapClient: func(ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
			return nil, errors.New("client creation failed")
		},
	})
	defer originalHooks()

	ad := &vsa.ActiveDirectory{Domain: "example.com"}
	_, err := (ActiveDirectoryActivity{}).GetOrCreateCifsService(ctx, &models.Node{}, ad, "svm", "svm-uuid")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get ONTAP client")
}

func TestGetOrCreateCifsService_EnsureCifsServerNamePostFixError(t *testing.T) {
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

	ad := &vsa.ActiveDirectory{Domain: "example.com"}
	_, err := (ActiveDirectoryActivity{}).GetOrCreateCifsService(ctx, &models.Node{}, ad, "svm", "svm-uuid")
	require.Error(t, err)
}

func TestGetOrCreateCifsService_CifsServiceGetError(t *testing.T) {
	ctx := context.Background()
	mockClient := new(ontapRest.MockRESTClient)
	mockNas := new(ontapRest.MockNASClient)
	mockClient.On("NAS").Return(mockNas).Once()
	mockNas.On("CifsServiceGet", mock.Anything).Return(nil, errors.New("cifs get failed")).Once()

	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{})
	defer cleanup()

	ad := &vsa.ActiveDirectory{Domain: "example.com"}
	_, err := (ActiveDirectoryActivity{}).GetOrCreateCifsService(ctx, &models.Node{}, ad, "svm", "svm-uuid")
	require.Error(t, err)
	mockNas.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestGetOrCreateCifsService_CreateAndSetupCIFSServerError(t *testing.T) {
	ctx := context.Background()
	mockClient := new(ontapRest.MockRESTClient)
	mockNas := new(ontapRest.MockNASClient)
	mockClient.On("NAS").Return(mockNas).Once()
	mockNas.On("CifsServiceGet", mock.Anything).Return(nil, utilerrors.NewNotFoundErr("cifs service", nil)).Once()

	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{
		CreateAndSetupCIFSServer: func(_ log.Logger, _ ontapRest.RESTClient, _ *vsa.ActiveDirectory, _, _ string) (string, error) {
			return "", errors.New("create failed")
		},
	})
	defer cleanup()

	ad := &vsa.ActiveDirectory{Domain: "example.com"}
	_, err := (ActiveDirectoryActivity{}).GetOrCreateCifsService(ctx, &models.Node{}, ad, "svm", "svm-uuid")
	require.Error(t, err)
	mockNas.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestDdnsModify_GetOntapRestProviderError(t *testing.T) {
	ctx := context.Background()
	originalGetter := getOntapRestProvider
	defer func() { getOntapRestProvider = originalGetter }()

	getOntapRestProvider = func(context.Context, *models.Node) (*vsa.OntapRestProvider, error) {
		return nil, errors.New("provider error")
	}

	err := (ActiveDirectoryActivity{}).DdnsModify(ctx, &models.Node{}, "svm-uuid", "fqdn.example.com")
	require.Error(t, err)
}

func TestDdnsModify_CreateRESTClientError(t *testing.T) {
	ctx := context.Background()
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

	originalHooks := vsa.SetTestHooks(vsa.TestHooks{
		GetOntapClient: func(ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
			return nil, errors.New("client creation failed")
		},
	})
	defer originalHooks()

	err := (ActiveDirectoryActivity{}).DdnsModify(ctx, &models.Node{}, "svm-uuid", "fqdn.example.com")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get ONTAP client")
}

func TestDdnsModify_DNSModifyError(t *testing.T) {
	ctx := context.Background()
	mockClient := new(ontapRest.MockRESTClient)
	mockNameSvc := new(ontapRest.MockNameServicesClient)
	mockClient.On("NameServices").Return(mockNameSvc).Once()
	mockNameSvc.On("DNSModify", mock.Anything).Return(errors.New("dns modify failed")).Once()

	cleanup := setupOntapProvider(t, ctx, mockClient, vsa.TestHooks{})
	defer cleanup()

	err := (ActiveDirectoryActivity{}).DdnsModify(ctx, &models.Node{}, "svm-uuid", "fqdn.example.com")
	require.Error(t, err)
	mockClient.AssertExpectations(t)
	mockNameSvc.AssertExpectations(t)
}

func TestCreateJunctionPathForCifsShare_GetOntapRestProviderError(t *testing.T) {
	ctx := context.Background()
	originalGetter := getOntapRestProvider
	defer func() { getOntapRestProvider = originalGetter }()

	getOntapRestProvider = func(context.Context, *models.Node) (*vsa.OntapRestProvider, error) {
		return nil, errors.New("provider error")
	}

	err := (ActiveDirectoryActivity{}).CreateJunctionPathForCifsShare(ctx, &models.Node{}, "svm", "/junction")
	require.Error(t, err)
}

func TestCreateJunctionPathForCifsShare_CreateRESTClientError(t *testing.T) {
	ctx := context.Background()
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

	originalHooks := vsa.SetTestHooks(vsa.TestHooks{
		GetOntapClient: func(ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
			return nil, errors.New("client creation failed")
		},
	})
	defer originalHooks()

	err := (ActiveDirectoryActivity{}).CreateJunctionPathForCifsShare(ctx, &models.Node{}, "svm", "/junction")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get ONTAP client")
}
