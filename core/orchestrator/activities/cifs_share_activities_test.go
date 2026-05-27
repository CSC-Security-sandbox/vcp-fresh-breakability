package activities_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	ontapModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/active_directory_activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	utilErrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"go.temporal.io/sdk/testsuite"
)

// Helper to create context with logger
func makeCIFSTestContext() context.Context {
	return context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
}

// Helper to create test AD
func makeTestAD() *vsa.ActiveDirectory {
	domain := "example.com"
	dns := "8.8.8.8"
	netBIOS := "NETBIOS"
	username := "admin"
	password := log.Secret("password")
	managedAD := true
	primaryAD := true
	ou := "OU=test"
	site := "site1"
	aesEncryption := true
	ldapOverTLS := false
	encryptDC := false
	cert := "cert"
	ldapSigning := false

	return &vsa.ActiveDirectory{
		UUID:                    "ad-uuid",
		Domain:                  domain,
		DNS:                     dns,
		NetBIOS:                 netBIOS,
		Username:                username,
		Password:                password,
		ManagedAD:               &managedAD,
		PrimaryAD:               &primaryAD,
		OrganizationalUnit:      ou,
		Site:                    &site,
		Users:                   map[string][]string{},
		AesEncryption:           &aesEncryption,
		LdapOverTLS:             &ldapOverTLS,
		EncryptDCConnections:    &encryptDC,
		ServerRootCaCertificate: &cert,
		LdapSigning:             &ldapSigning,
	}
}

// Helper to create test node
func makeTestNode() *models.Node {
	return &models.Node{Name: "node-1"}
}

func overrideProvider(t *testing.T, provider vsa.Provider) {
	originalGetProvider := vsa.GetProviderByNode
	vsa.GetProviderByNode = func(_ context.Context, _ *models.Node) (vsa.Provider, error) {
		return provider, nil
	}
	t.Cleanup(func() { vsa.GetProviderByNode = originalGetProvider })
}

func newOntapProvider(ctx context.Context) *vsa.OntapRestProvider {
	return vsa.NewProvider(ctx, vsa.ProviderDetails{Hosts: map[string]string{"host": "host"}})
}

// TestGetOrCreateCifsService tests the GetOrCreateCifsService activity
func TestGetOrCreateCifsService(t *testing.T) {
	ctx := makeCIFSTestContext()
	activity := active_directory_activities.ActiveDirectoryActivity{}
	node := makeTestNode()
	ad := makeTestAD()
	svmName := "test-svm"
	externalSVMUUID := "svm-uuid"

	t.Run("success_service_exists_and_needs_ddns", func(t *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.GetOrCreateCifsService)

		// Mock password decryption
		originalDecryptPassword := utils.DecryptPassword
		utils.DecryptPassword = func(password log.Secret) (*string, error) {
			decrypted := "decrypted-password"
			return &decrypted, nil
		}
		defer func() { utils.DecryptPassword = originalDecryptPassword }()

		mockClient := new(ontapRest.MockRESTClient)
		mockNAS := new(ontapRest.MockNASClient)
		mockClient.On("NAS").Return(mockNAS)

		serviceName := "NETBIOS"
		domainFQDN := "example.com"
		mockNAS.On("CifsServiceGet", mock.Anything).Return(&ontapRest.CifsService{
			CifsService: ontapModels.CifsService{
				Name:     &serviceName,
				AdDomain: &ontapModels.AdDomain{Fqdn: nillable.ToPointer(domainFQDN)},
			},
		}, nil)

		ensureCalled := false
		cleanupHooks := vsa.SetTestHooks(vsa.TestHooks{
			GetOntapClient: func(ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
				return mockClient, nil
			},
			EnsureCifsServerNamePostFix: func(_ log.Logger, _ ontapRest.RESTClient, _ *vsa.ActiveDirectory, receivedSVM string) error {
				ensureCalled = true
				assert.Equal(t, svmName, receivedSVM)
				return nil
			},
			IsDDNSEnabled: func(_ log.Logger, _ ontapRest.RESTClient, receivedSVM string) bool {
				assert.Equal(t, externalSVMUUID, receivedSVM)
				return false
			},
		})
		t.Cleanup(cleanupHooks)

		overrideProvider(t, newOntapProvider(ctx))

		val, err := env.ExecuteActivity(activity.GetOrCreateCifsService, node, ad, svmName, externalSVMUUID)
		assert.NoError(t, err)
		assert.True(t, ensureCalled)

		var result *active_directory_activities.GetOrCreateCifsServiceResult
		_ = val.Get(&result)
		assert.NotNil(t, result)
		assert.True(t, result.NeedsDDNS)
		assert.Empty(t, result.FQDN)
		assert.Equal(t, serviceName, result.CifsServiceName)
		assert.Equal(t, domainFQDN, result.AdDomain)

		mockClient.AssertExpectations(t)
		mockNAS.AssertExpectations(t)
	})

	t.Run("success_creates_service_when_missing", func(t *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.GetOrCreateCifsService)

		// Mock password decryption
		originalDecryptPassword := utils.DecryptPassword
		utils.DecryptPassword = func(password log.Secret) (*string, error) {
			decrypted := "decrypted-password"
			return &decrypted, nil
		}
		defer func() { utils.DecryptPassword = originalDecryptPassword }()

		mockClient := new(ontapRest.MockRESTClient)
		mockNAS := new(ontapRest.MockNASClient)
		mockClient.On("NAS").Return(mockNAS)
		mockNAS.On("CifsServiceGet", mock.Anything).Return(nil, utilErrors.NewNotFoundErr("cifs service", nil))

		ensureCalled := false
		createCalled := false
		expectedFQDN := "generated.example.com"

		cleanupHooks := vsa.SetTestHooks(vsa.TestHooks{
			GetOntapClient: func(ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
				return mockClient, nil
			},
			EnsureCifsServerNamePostFix: func(_ log.Logger, _ ontapRest.RESTClient, _ *vsa.ActiveDirectory, receivedSVM string) error {
				ensureCalled = true
				assert.Equal(t, svmName, receivedSVM)
				return nil
			},
			CreateAndSetupCIFSServer: func(_ log.Logger, _ ontapRest.RESTClient, dir *vsa.ActiveDirectory, svmUUID, receivedSVM string) (string, error) {
				createCalled = true
				assert.Equal(t, ad.UUID, dir.UUID)
				assert.Equal(t, ad.Domain, dir.Domain)
				assert.Equal(t, externalSVMUUID, svmUUID)
				assert.Equal(t, svmName, receivedSVM)
				return expectedFQDN, nil
			},
		})
		t.Cleanup(cleanupHooks)

		overrideProvider(t, newOntapProvider(ctx))

		val, err := env.ExecuteActivity(activity.GetOrCreateCifsService, node, ad, svmName, externalSVMUUID)
		assert.NoError(t, err)
		assert.True(t, ensureCalled)
		assert.True(t, createCalled)

		var result *active_directory_activities.GetOrCreateCifsServiceResult
		_ = val.Get(&result)
		assert.NotNil(t, result)
		assert.Equal(t, expectedFQDN, result.FQDN)
		assert.False(t, result.NeedsDDNS)
		assert.Empty(t, result.CifsServiceName)
		assert.Empty(t, result.AdDomain)

		mockClient.AssertExpectations(t)
		mockNAS.AssertExpectations(t)
	})

	t.Run("error_provider_not_ontap", func(t *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.GetOrCreateCifsService)

		// Mock password decryption
		originalDecryptPassword := utils.DecryptPassword
		utils.DecryptPassword = func(password log.Secret) (*string, error) {
			decrypted := "decrypted-password"
			return &decrypted, nil
		}
		defer func() { utils.DecryptPassword = originalDecryptPassword }()

		mockProvider := vsa.NewMockProvider(t)
		originalGetProvider := vsa.GetProviderByNode
		vsa.GetProviderByNode = func(_ context.Context, _ *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		defer func() { vsa.GetProviderByNode = originalGetProvider }()

		_, err := env.ExecuteActivity(activity.GetOrCreateCifsService, node, ad, svmName, externalSVMUUID)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "provider is not OntapRestProvider")
	})

	t.Run("error_provider_lookup_fails", func(t *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.GetOrCreateCifsService)

		// Mock password decryption
		originalDecryptPassword := utils.DecryptPassword
		utils.DecryptPassword = func(password log.Secret) (*string, error) {
			decrypted := "decrypted-password"
			return &decrypted, nil
		}
		defer func() { utils.DecryptPassword = originalDecryptPassword }()

		originalGetProvider := vsa.GetProviderByNode
		vsa.GetProviderByNode = func(_ context.Context, _ *models.Node) (vsa.Provider, error) {
			return nil, errors.New("provider error")
		}
		defer func() { vsa.GetProviderByNode = originalGetProvider }()

		_, err := env.ExecuteActivity(activity.GetOrCreateCifsService, node, ad, svmName, externalSVMUUID)
		assert.Error(t, err)
	})
}

// TestDdnsModify tests the DdnsModify activity
func TestDdnsModify(t *testing.T) {
	ctx := makeCIFSTestContext()
	activity := active_directory_activities.ActiveDirectoryActivity{}
	node := makeTestNode()
	externalSVMUUID := "svm-uuid"
	fqdn := "server.example.com"

	t.Run("success_enables_ddns", func(t *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.DdnsModify)

		mockClient := new(ontapRest.MockRESTClient)
		mockNameServices := new(ontapRest.MockNameServicesClient)
		mockClient.On("NameServices").Return(mockNameServices)

		mockNameServices.On("DNSModify", mock.MatchedBy(func(params *ontapRest.DNSModifyParams) bool {
			if params == nil || params.SvmUUID != externalSVMUUID {
				return false
			}
			if params.DDNSModifyParams.Enabled == nil || !*params.DDNSModifyParams.Enabled {
				return false
			}
			if params.DDNSModifyParams.Fqdn == nil || *params.DDNSModifyParams.Fqdn != fqdn {
				return false
			}
			return true
		})).Return(nil)

		cleanupHooks := vsa.SetTestHooks(vsa.TestHooks{
			GetOntapClient: func(ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
				return mockClient, nil
			},
		})
		t.Cleanup(cleanupHooks)

		overrideProvider(t, newOntapProvider(ctx))

		_, err := env.ExecuteActivity(activity.DdnsModify, node, externalSVMUUID, fqdn)
		assert.NoError(t, err)

		mockClient.AssertExpectations(t)
		mockNameServices.AssertExpectations(t)
	})

	t.Run("error_provider_not_ontap", func(t *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.DdnsModify)

		mockProvider := vsa.NewMockProvider(t)
		originalGetProvider := vsa.GetProviderByNode
		vsa.GetProviderByNode = func(_ context.Context, _ *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		defer func() { vsa.GetProviderByNode = originalGetProvider }()

		_, err := env.ExecuteActivity(activity.DdnsModify, node, externalSVMUUID, fqdn)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "provider is not OntapRestProvider")
	})

	t.Run("error_provider_lookup_fails", func(t *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.DdnsModify)

		originalGetProvider := vsa.GetProviderByNode
		vsa.GetProviderByNode = func(_ context.Context, _ *models.Node) (vsa.Provider, error) {
			return nil, errors.New("provider error")
		}
		defer func() { vsa.GetProviderByNode = originalGetProvider }()

		_, err := env.ExecuteActivity(activity.DdnsModify, node, externalSVMUUID, fqdn)
		assert.Error(t, err)
	})
}

// TestCreateJunctionPathForCifsShare tests the CreateJunctionPathForCifsShare activity
func TestCreateJunctionPathForCifsShare(t *testing.T) {
	ctx := makeCIFSTestContext()
	activity := active_directory_activities.ActiveDirectoryActivity{}
	node := makeTestNode()
	svmName := "test-svm"
	junctionPath := "/test/junction"

	t.Run("success_creates_share", func(t *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.CreateJunctionPathForCifsShare)

		mockClient := new(ontapRest.MockRESTClient)
		mockNAS := new(ontapRest.MockNASClient)
		mockClient.On("NAS").Return(mockNAS)

		mockNAS.On("CifsShareCreate", mock.MatchedBy(func(params *ontapRest.CifsShareCreateParams) bool {
			if params == nil || params.SvmName == nil || *params.SvmName != svmName {
				return false
			}
			return params.Path == junctionPath && params.Name == junctionPath[1:]
		})).Return(nil)

		cleanupHooks := vsa.SetTestHooks(vsa.TestHooks{
			GetOntapClient: func(ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
				return mockClient, nil
			},
		})
		t.Cleanup(cleanupHooks)

		overrideProvider(t, newOntapProvider(ctx))

		_, err := env.ExecuteActivity(activity.CreateJunctionPathForCifsShare, node, svmName, junctionPath, []string{})
		assert.NoError(t, err)

		mockClient.AssertExpectations(t)
		mockNAS.AssertExpectations(t)
	})

	t.Run("error_provider_not_ontap", func(t *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.CreateJunctionPathForCifsShare)

		mockProvider := vsa.NewMockProvider(t)
		originalGetProvider := vsa.GetProviderByNode
		vsa.GetProviderByNode = func(_ context.Context, _ *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		defer func() { vsa.GetProviderByNode = originalGetProvider }()

		_, err := env.ExecuteActivity(activity.CreateJunctionPathForCifsShare, node, svmName, junctionPath, []string{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "provider is not OntapRestProvider")
	})

	t.Run("error_provider_lookup_fails", func(t *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.CreateJunctionPathForCifsShare)

		originalGetProvider := vsa.GetProviderByNode
		vsa.GetProviderByNode = func(_ context.Context, _ *models.Node) (vsa.Provider, error) {
			return nil, errors.New("provider error")
		}
		defer func() { vsa.GetProviderByNode = originalGetProvider }()

		_, err := env.ExecuteActivity(activity.CreateJunctionPathForCifsShare, node, svmName, junctionPath, []string{})
		assert.Error(t, err)
	})
}
