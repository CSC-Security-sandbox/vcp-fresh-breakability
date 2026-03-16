package workflows

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	ontaprestmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/active_directory_activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

// setupTestWorkflowEnvironment and overrideActiveDirectoryActivityFactory are defined in ensure_cifs_share_workflow_test.go

// extractOriginalErrMsg extracts the original error message from a workflow error
// that was wrapped via WrapErrorForChildWorkflow → WrapAsTemporalApplicationError.
// The details layout is: [trackingID int, originalErrMsg string].
func extractOriginalErrMsg(t *testing.T, workflowErr error) string {
	t.Helper()
	var appErr *temporal.ApplicationError
	if !errors.As(workflowErr, &appErr) {
		t.Fatalf("expected temporal.ApplicationError, got %T", workflowErr)
	}
	var trackingID int
	var originalMsg string
	if err := appErr.Details(&trackingID, &originalMsg); err != nil {
		t.Fatalf("failed to extract details from ApplicationError: %v", err)
	}
	assert.Equal(t, vsaerrors.ErrInternalServerError, trackingID)
	return originalMsg
}

func TestEnsureKerberosConfigWorkflow_Success(t *testing.T) {
	var ts testsuite.WorkflowTestSuite

	node := &models.Node{Name: "node-1"}
	ad := &vsa.ActiveDirectory{
		Domain: "example.com",
		KdcIP:  "192.168.1.1",
		AdName: "ad-server",
	}
	svmName := "test-svm"
	externalSVMUUID := "svm-uuid"

	t.Run("success_all_steps_realm_does_not_exist", func(t *testing.T) {
		env := ts.NewTestWorkflowEnvironment()
		setupTestWorkflowEnvironment(t, env)

		adActivity := &active_directory_activities.ActiveDirectoryActivity{}
		restoreFactory := overrideActiveDirectoryActivityFactory(adActivity)
		defer restoreFactory()

		env.RegisterWorkflow(EnsureKerberosConfigWorkflow)
		env.RegisterActivity(adActivity.CreateNameMappingForKerberosActivity)
		env.RegisterActivity(adActivity.CheckKerberosRealmExistsActivity)
		env.RegisterActivity(adActivity.CreateKerberosRealmActivity)
		env.RegisterActivity(adActivity.CreateOrModifyADDNS)
		env.RegisterActivity(adActivity.GetOrCreateCifsService)
		env.RegisterActivity(adActivity.GetDataLifsForSVMActivity)
		env.RegisterActivity(adActivity.EnableKerberosOnInterfaceActivity)

		// Step 1: Create name mapping
		env.OnActivity(adActivity.CreateNameMappingForKerberosActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

		// Step 2: Check realm - doesn't exist
		env.OnActivity(adActivity.CheckKerberosRealmExistsActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(false, nil).Once()

		// Step 3: Create realm
		env.OnActivity(adActivity.CreateKerberosRealmActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

		// Step 4: Create or modify AD DNS
		env.OnActivity(adActivity.CreateOrModifyADDNS, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

		// Step 5: Get or create CIFS service
		cifsResult := &active_directory_activities.GetOrCreateCifsServiceResult{
			FQDN: "server.example.com",
		}
		env.OnActivity(adActivity.GetOrCreateCifsService, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(cifsResult, nil).Once()

		// Step 6: Get data LIFs
		lifName := "test-svm-ilbnas"
		lifIPStr := "192.168.1.10"
		lifIP := ontaprestmodels.IPAddress(lifIPStr)
		dataLifs := []*ontapRest.IPInterface{
			{
				IPInterface: ontaprestmodels.IPInterface{
					Name: &lifName,
					IP: &ontaprestmodels.IPInfo{
						Address: &lifIP,
					},
				},
			},
		}
		env.OnActivity(adActivity.GetDataLifsForSVMActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(dataLifs, nil).Once()

		// Step 7: Enable Kerberos on interface
		env.OnActivity(adActivity.EnableKerberosOnInterfaceActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

		env.ExecuteWorkflow(EnsureKerberosConfigWorkflow, node, ad, svmName, externalSVMUUID)

		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("success_realm_already_exists", func(t *testing.T) {
		env := ts.NewTestWorkflowEnvironment()
		setupTestWorkflowEnvironment(t, env)

		adActivity := &active_directory_activities.ActiveDirectoryActivity{}
		restoreFactory := overrideActiveDirectoryActivityFactory(adActivity)
		defer restoreFactory()

		env.RegisterWorkflow(EnsureKerberosConfigWorkflow)
		env.RegisterActivity(adActivity.CreateNameMappingForKerberosActivity)
		env.RegisterActivity(adActivity.CheckKerberosRealmExistsActivity)
		env.RegisterActivity(adActivity.CreateOrModifyADDNS)
		env.RegisterActivity(adActivity.GetOrCreateCifsService)
		env.RegisterActivity(adActivity.GetDataLifsForSVMActivity)
		env.RegisterActivity(adActivity.EnableKerberosOnInterfaceActivity)

		env.OnActivity(adActivity.CreateNameMappingForKerberosActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		env.OnActivity(adActivity.CheckKerberosRealmExistsActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(true, nil).Once()
		env.OnActivity(adActivity.CreateOrModifyADDNS, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

		cifsResult := &active_directory_activities.GetOrCreateCifsServiceResult{
			CifsServiceName: "server",
			AdDomain:        "example.com",
		}
		env.OnActivity(adActivity.GetOrCreateCifsService, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(cifsResult, nil).Once()

		lifName := "test-svm-ilbnas"
		lifIPStr := "192.168.1.10"
		lifIP := ontaprestmodels.IPAddress(lifIPStr)
		dataLifs := []*ontapRest.IPInterface{
			{
				IPInterface: ontaprestmodels.IPInterface{
					Name: &lifName,
					IP: &ontaprestmodels.IPInfo{
						Address: &lifIP,
					},
				},
			},
		}
		env.OnActivity(adActivity.GetDataLifsForSVMActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(dataLifs, nil).Once()
		env.OnActivity(adActivity.EnableKerberosOnInterfaceActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

		env.ExecuteWorkflow(EnsureKerberosConfigWorkflow, node, ad, svmName, externalSVMUUID)

		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("success_no_data_lifs", func(t *testing.T) {
		env := ts.NewTestWorkflowEnvironment()
		setupTestWorkflowEnvironment(t, env)

		adActivity := &active_directory_activities.ActiveDirectoryActivity{}
		restoreFactory := overrideActiveDirectoryActivityFactory(adActivity)
		defer restoreFactory()

		env.RegisterWorkflow(EnsureKerberosConfigWorkflow)
		env.RegisterActivity(adActivity.CreateNameMappingForKerberosActivity)
		env.RegisterActivity(adActivity.CheckKerberosRealmExistsActivity)
		env.RegisterActivity(adActivity.CreateKerberosRealmActivity)
		env.RegisterActivity(adActivity.CreateOrModifyADDNS)
		env.RegisterActivity(adActivity.GetOrCreateCifsService)
		env.RegisterActivity(adActivity.GetDataLifsForSVMActivity)

		env.OnActivity(adActivity.CreateNameMappingForKerberosActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		env.OnActivity(adActivity.CheckKerberosRealmExistsActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(false, nil).Once()
		env.OnActivity(adActivity.CreateKerberosRealmActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		env.OnActivity(adActivity.CreateOrModifyADDNS, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

		cifsResult := &active_directory_activities.GetOrCreateCifsServiceResult{
			FQDN: "server.example.com",
		}
		env.OnActivity(adActivity.GetOrCreateCifsService, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(cifsResult, nil).Once()
		env.OnActivity(adActivity.GetDataLifsForSVMActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*ontapRest.IPInterface{}, nil).Once()

		env.ExecuteWorkflow(EnsureKerberosConfigWorkflow, node, ad, svmName, externalSVMUUID)

		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
}

func TestEnsureKerberosConfigWorkflow_Errors(t *testing.T) {
	var ts testsuite.WorkflowTestSuite

	node := &models.Node{Name: "node-1"}
	ad := &vsa.ActiveDirectory{
		Domain: "example.com",
		KdcIP:  "192.168.1.1",
		AdName: "ad-server",
	}
	svmName := "test-svm"
	externalSVMUUID := "svm-uuid"

	t.Run("error_create_name_mapping", func(t *testing.T) {
		env := ts.NewTestWorkflowEnvironment()
		setupTestWorkflowEnvironment(t, env)

		adActivity := &active_directory_activities.ActiveDirectoryActivity{}
		restoreFactory := overrideActiveDirectoryActivityFactory(adActivity)
		defer restoreFactory()

		env.RegisterWorkflow(EnsureKerberosConfigWorkflow)
		env.RegisterActivity(adActivity.CreateNameMappingForKerberosActivity)

		env.OnActivity(adActivity.CreateNameMappingForKerberosActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("create name mapping failed")).Once()

		env.ExecuteWorkflow(EnsureKerberosConfigWorkflow, node, ad, svmName, externalSVMUUID)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("error_check_realm", func(t *testing.T) {
		env := ts.NewTestWorkflowEnvironment()
		setupTestWorkflowEnvironment(t, env)

		adActivity := &active_directory_activities.ActiveDirectoryActivity{}
		restoreFactory := overrideActiveDirectoryActivityFactory(adActivity)
		defer restoreFactory()

		env.RegisterWorkflow(EnsureKerberosConfigWorkflow)
		env.RegisterActivity(adActivity.CreateNameMappingForKerberosActivity)
		env.RegisterActivity(adActivity.CheckKerberosRealmExistsActivity)

		env.OnActivity(adActivity.CreateNameMappingForKerberosActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		env.OnActivity(adActivity.CheckKerberosRealmExistsActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(false, errors.New("check realm failed")).Once()

		env.ExecuteWorkflow(EnsureKerberosConfigWorkflow, node, ad, svmName, externalSVMUUID)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("error_missing_kdc_ip", func(t *testing.T) {
		env := ts.NewTestWorkflowEnvironment()
		setupTestWorkflowEnvironment(t, env)

		adActivity := &active_directory_activities.ActiveDirectoryActivity{}
		restoreFactory := overrideActiveDirectoryActivityFactory(adActivity)
		defer restoreFactory()

		adNoKdcIP := &vsa.ActiveDirectory{
			Domain: "example.com",
			KdcIP:  "", // Missing KDC IP
			AdName: "ad-server",
		}

		env.RegisterWorkflow(EnsureKerberosConfigWorkflow)
		env.RegisterActivity(adActivity.CreateNameMappingForKerberosActivity)
		env.RegisterActivity(adActivity.CheckKerberosRealmExistsActivity)

		env.OnActivity(adActivity.CreateNameMappingForKerberosActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		env.OnActivity(adActivity.CheckKerberosRealmExistsActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(false, nil).Once()

		env.ExecuteWorkflow(EnsureKerberosConfigWorkflow, node, adNoKdcIP, svmName, externalSVMUUID)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		assert.Equal(t, "KDC IP is required for Kerberos realm creation but not found in Active Directory", extractOriginalErrMsg(t, env.GetWorkflowError()))
		env.AssertExpectations(t)
	})

	t.Run("error_create_realm", func(t *testing.T) {
		env := ts.NewTestWorkflowEnvironment()
		setupTestWorkflowEnvironment(t, env)

		adActivity := &active_directory_activities.ActiveDirectoryActivity{}
		restoreFactory := overrideActiveDirectoryActivityFactory(adActivity)
		defer restoreFactory()

		env.RegisterWorkflow(EnsureKerberosConfigWorkflow)
		env.RegisterActivity(adActivity.CreateNameMappingForKerberosActivity)
		env.RegisterActivity(adActivity.CheckKerberosRealmExistsActivity)
		env.RegisterActivity(adActivity.CreateKerberosRealmActivity)

		env.OnActivity(adActivity.CreateNameMappingForKerberosActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		env.OnActivity(adActivity.CheckKerberosRealmExistsActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(false, nil).Once()
		env.OnActivity(adActivity.CreateKerberosRealmActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("create realm failed")).Once()

		env.ExecuteWorkflow(EnsureKerberosConfigWorkflow, node, ad, svmName, externalSVMUUID)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("error_create_dns", func(t *testing.T) {
		env := ts.NewTestWorkflowEnvironment()
		setupTestWorkflowEnvironment(t, env)

		adActivity := &active_directory_activities.ActiveDirectoryActivity{}
		restoreFactory := overrideActiveDirectoryActivityFactory(adActivity)
		defer restoreFactory()

		env.RegisterWorkflow(EnsureKerberosConfigWorkflow)
		env.RegisterActivity(adActivity.CreateNameMappingForKerberosActivity)
		env.RegisterActivity(adActivity.CheckKerberosRealmExistsActivity)
		env.RegisterActivity(adActivity.CreateKerberosRealmActivity)
		env.RegisterActivity(adActivity.CreateOrModifyADDNS)

		env.OnActivity(adActivity.CreateNameMappingForKerberosActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		env.OnActivity(adActivity.CheckKerberosRealmExistsActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(false, nil).Once()
		env.OnActivity(adActivity.CreateKerberosRealmActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		env.OnActivity(adActivity.CreateOrModifyADDNS, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("create DNS failed")).Once()

		env.ExecuteWorkflow(EnsureKerberosConfigWorkflow, node, ad, svmName, externalSVMUUID)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("error_get_cifs_service", func(t *testing.T) {
		env := ts.NewTestWorkflowEnvironment()
		setupTestWorkflowEnvironment(t, env)

		adActivity := &active_directory_activities.ActiveDirectoryActivity{}
		restoreFactory := overrideActiveDirectoryActivityFactory(adActivity)
		defer restoreFactory()

		env.RegisterWorkflow(EnsureKerberosConfigWorkflow)
		env.RegisterActivity(adActivity.CreateNameMappingForKerberosActivity)
		env.RegisterActivity(adActivity.CheckKerberosRealmExistsActivity)
		env.RegisterActivity(adActivity.CreateKerberosRealmActivity)
		env.RegisterActivity(adActivity.CreateOrModifyADDNS)
		env.RegisterActivity(adActivity.GetOrCreateCifsService)

		env.OnActivity(adActivity.CreateNameMappingForKerberosActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		env.OnActivity(adActivity.CheckKerberosRealmExistsActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(false, nil).Once()
		env.OnActivity(adActivity.CreateKerberosRealmActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		env.OnActivity(adActivity.CreateOrModifyADDNS, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		env.OnActivity(adActivity.GetOrCreateCifsService, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("get CIFS service failed")).Once()

		env.ExecuteWorkflow(EnsureKerberosConfigWorkflow, node, ad, svmName, externalSVMUUID)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("error_unable_to_determine_fqdn", func(t *testing.T) {
		env := ts.NewTestWorkflowEnvironment()
		setupTestWorkflowEnvironment(t, env)

		adActivity := &active_directory_activities.ActiveDirectoryActivity{}
		restoreFactory := overrideActiveDirectoryActivityFactory(adActivity)
		defer restoreFactory()

		env.RegisterWorkflow(EnsureKerberosConfigWorkflow)
		env.RegisterActivity(adActivity.CreateNameMappingForKerberosActivity)
		env.RegisterActivity(adActivity.CheckKerberosRealmExistsActivity)
		env.RegisterActivity(adActivity.CreateKerberosRealmActivity)
		env.RegisterActivity(adActivity.CreateOrModifyADDNS)
		env.RegisterActivity(adActivity.GetOrCreateCifsService)

		env.OnActivity(adActivity.CreateNameMappingForKerberosActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		env.OnActivity(adActivity.CheckKerberosRealmExistsActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(false, nil).Once()
		env.OnActivity(adActivity.CreateKerberosRealmActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		env.OnActivity(adActivity.CreateOrModifyADDNS, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

		// Return result without FQDN or service name
		cifsResult := &active_directory_activities.GetOrCreateCifsServiceResult{
			FQDN:            "",
			CifsServiceName: "",
			AdDomain:        "",
		}
		env.OnActivity(adActivity.GetOrCreateCifsService, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(cifsResult, nil).Once()

		env.ExecuteWorkflow(EnsureKerberosConfigWorkflow, node, ad, svmName, externalSVMUUID)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		assert.Equal(t, "unable to determine FQDN for Kerberos configuration", extractOriginalErrMsg(t, env.GetWorkflowError()))
		env.AssertExpectations(t)
	})

	t.Run("error_get_data_lifs", func(t *testing.T) {
		env := ts.NewTestWorkflowEnvironment()
		setupTestWorkflowEnvironment(t, env)

		adActivity := &active_directory_activities.ActiveDirectoryActivity{}
		restoreFactory := overrideActiveDirectoryActivityFactory(adActivity)
		defer restoreFactory()

		env.RegisterWorkflow(EnsureKerberosConfigWorkflow)
		env.RegisterActivity(adActivity.CreateNameMappingForKerberosActivity)
		env.RegisterActivity(adActivity.CheckKerberosRealmExistsActivity)
		env.RegisterActivity(adActivity.CreateKerberosRealmActivity)
		env.RegisterActivity(adActivity.CreateOrModifyADDNS)
		env.RegisterActivity(adActivity.GetOrCreateCifsService)
		env.RegisterActivity(adActivity.GetDataLifsForSVMActivity)

		env.OnActivity(adActivity.CreateNameMappingForKerberosActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		env.OnActivity(adActivity.CheckKerberosRealmExistsActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(false, nil).Once()
		env.OnActivity(adActivity.CreateKerberosRealmActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		env.OnActivity(adActivity.CreateOrModifyADDNS, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

		cifsResult := &active_directory_activities.GetOrCreateCifsServiceResult{
			FQDN: "server.example.com",
		}
		env.OnActivity(adActivity.GetOrCreateCifsService, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(cifsResult, nil).Once()
		env.OnActivity(adActivity.GetDataLifsForSVMActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("get data LIFs failed")).Once()

		env.ExecuteWorkflow(EnsureKerberosConfigWorkflow, node, ad, svmName, externalSVMUUID)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("error_missing_ip_address", func(t *testing.T) {
		env := ts.NewTestWorkflowEnvironment()
		setupTestWorkflowEnvironment(t, env)

		adActivity := &active_directory_activities.ActiveDirectoryActivity{}
		restoreFactory := overrideActiveDirectoryActivityFactory(adActivity)
		defer restoreFactory()

		env.RegisterWorkflow(EnsureKerberosConfigWorkflow)
		env.RegisterActivity(adActivity.CreateNameMappingForKerberosActivity)
		env.RegisterActivity(adActivity.CheckKerberosRealmExistsActivity)
		env.RegisterActivity(adActivity.CreateKerberosRealmActivity)
		env.RegisterActivity(adActivity.CreateOrModifyADDNS)
		env.RegisterActivity(adActivity.GetOrCreateCifsService)
		env.RegisterActivity(adActivity.GetDataLifsForSVMActivity)

		env.OnActivity(adActivity.CreateNameMappingForKerberosActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		env.OnActivity(adActivity.CheckKerberosRealmExistsActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(false, nil).Once()
		env.OnActivity(adActivity.CreateKerberosRealmActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		env.OnActivity(adActivity.CreateOrModifyADDNS, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

		cifsResult := &active_directory_activities.GetOrCreateCifsServiceResult{
			FQDN: "server.example.com",
		}
		env.OnActivity(adActivity.GetOrCreateCifsService, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(cifsResult, nil).Once()

		// Return LIF without IP address
		lifName := "test-svm-ilbnas"
		dataLifs := []*ontapRest.IPInterface{
			{
				IPInterface: ontaprestmodels.IPInterface{
					Name: &lifName,
					IP:   nil, // Missing IP
				},
			},
		}
		env.OnActivity(adActivity.GetDataLifsForSVMActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(dataLifs, nil).Once()

		env.ExecuteWorkflow(EnsureKerberosConfigWorkflow, node, ad, svmName, externalSVMUUID)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		assert.Equal(t, "IP address not found for NAS LIF: test-svm-ilbnas", extractOriginalErrMsg(t, env.GetWorkflowError()))
		env.AssertExpectations(t)
	})

	t.Run("error_populate_retry_policy", func(t *testing.T) {
		env := ts.NewTestWorkflowEnvironment()
		setupTestWorkflowEnvironment(t, env)

		adActivity := &active_directory_activities.ActiveDirectoryActivity{}
		restoreFactory := overrideActiveDirectoryActivityFactory(adActivity)
		defer restoreFactory()

		env.RegisterWorkflow(EnsureKerberosConfigWorkflow)

		origTimeout := StartToCloseTimeout
		StartToCloseTimeout = "invalid"
		defer func() { StartToCloseTimeout = origTimeout }()

		env.ExecuteWorkflow(EnsureKerberosConfigWorkflow, node, ad, svmName, externalSVMUUID)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
	})

	t.Run("error_enable_kerberos_on_interface", func(t *testing.T) {
		env := ts.NewTestWorkflowEnvironment()
		setupTestWorkflowEnvironment(t, env)

		adActivity := &active_directory_activities.ActiveDirectoryActivity{}
		restoreFactory := overrideActiveDirectoryActivityFactory(adActivity)
		defer restoreFactory()

		env.RegisterWorkflow(EnsureKerberosConfigWorkflow)
		env.RegisterActivity(adActivity.CreateNameMappingForKerberosActivity)
		env.RegisterActivity(adActivity.CheckKerberosRealmExistsActivity)
		env.RegisterActivity(adActivity.CreateKerberosRealmActivity)
		env.RegisterActivity(adActivity.CreateOrModifyADDNS)
		env.RegisterActivity(adActivity.GetOrCreateCifsService)
		env.RegisterActivity(adActivity.GetDataLifsForSVMActivity)
		env.RegisterActivity(adActivity.EnableKerberosOnInterfaceActivity)

		env.OnActivity(adActivity.CreateNameMappingForKerberosActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		env.OnActivity(adActivity.CheckKerberosRealmExistsActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(false, nil).Once()
		env.OnActivity(adActivity.CreateKerberosRealmActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		env.OnActivity(adActivity.CreateOrModifyADDNS, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

		cifsResult := &active_directory_activities.GetOrCreateCifsServiceResult{
			FQDN: "server.example.com",
		}
		env.OnActivity(adActivity.GetOrCreateCifsService, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(cifsResult, nil).Once()

		lifName := "test-svm-ilbnas"
		lifIPStr := "192.168.1.10"
		lifIP := ontaprestmodels.IPAddress(lifIPStr)
		dataLifs := []*ontapRest.IPInterface{
			{
				IPInterface: ontaprestmodels.IPInterface{
					Name: &lifName,
					IP: &ontaprestmodels.IPInfo{
						Address: &lifIP,
					},
				},
			},
		}
		env.OnActivity(adActivity.GetDataLifsForSVMActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(dataLifs, nil).Once()
		env.OnActivity(adActivity.EnableKerberosOnInterfaceActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("enable failed")).Once()

		env.ExecuteWorkflow(EnsureKerberosConfigWorkflow, node, ad, svmName, externalSVMUUID)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
}
