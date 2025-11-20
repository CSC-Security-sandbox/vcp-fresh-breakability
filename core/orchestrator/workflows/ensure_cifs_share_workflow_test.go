package workflows

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/active_directory_activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

// TestEnsureCIFSShareWorkflow tests the EnsureCIFSShareWorkflow
func TestEnsureCIFSShareWorkflow(t *testing.T) {
	var ts testsuite.WorkflowTestSuite

	volume := &datamodel.Volume{
		Name: "test-volume",
		Svm:  &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			FileProperties: &datamodel.FileProperties{
				JunctionPath: "/test/junction",
			},
			Protocols: []string{"SMB"},
		},
	}
	node := &models.Node{Name: "node-1"}
	ad := &vsa.ActiveDirectory{
		Domain: "example.com",
		DNS:    "8.8.8.8",
	}
	svmName := "test-svm"
	externalSVMUUID := "svm-uuid"

	t.Run("success_all_steps", func(t *testing.T) {
		env := ts.NewTestWorkflowEnvironment()
		setupTestWorkflowEnvironment(t, env)

		adActivity := &active_directory_activities.ActiveDirectoryActivity{}
		restoreFactory := overrideActiveDirectoryActivityFactory(adActivity)
		defer restoreFactory()

		// Register workflow
		env.RegisterWorkflow(EnsureCIFSShareWorkflow)

		// Register activities
		env.RegisterActivity(adActivity.CreateOrModifyADDNS)
		env.RegisterActivity(adActivity.GetOrCreateCifsService)
		env.RegisterActivity(adActivity.CreateJunctionPathForCifsShare)

		// Mock CreateOrModifyADDNS
		env.OnActivity(adActivity.CreateOrModifyADDNS, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

		// Mock GetOrCreateCifsService - service created
		cifsResult := &active_directory_activities.GetOrCreateCifsServiceResult{
			FQDN:      "server.example.com",
			NeedsDDNS: false,
		}
		env.OnActivity(adActivity.GetOrCreateCifsService, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(cifsResult, nil).Once()

		// Mock CreateJunctionPathForCifsShare
		env.OnActivity(adActivity.CreateJunctionPathForCifsShare, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

		env.ExecuteWorkflow(EnsureCIFSShareWorkflow, volume, node, ad, svmName, externalSVMUUID)

		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("success_service_exists_needs_ddns", func(t *testing.T) {
		env := ts.NewTestWorkflowEnvironment()
		setupTestWorkflowEnvironment(t, env)

		adActivity := &active_directory_activities.ActiveDirectoryActivity{}
		restoreFactory := overrideActiveDirectoryActivityFactory(adActivity)
		defer restoreFactory()

		// Register workflow
		env.RegisterWorkflow(EnsureCIFSShareWorkflow)

		// Register activities
		env.RegisterActivity(adActivity.CreateOrModifyADDNS)
		env.RegisterActivity(adActivity.GetOrCreateCifsService)
		env.RegisterActivity(adActivity.DdnsModify)
		env.RegisterActivity(adActivity.CreateJunctionPathForCifsShare)

		// Mock CreateOrModifyADDNS
		env.OnActivity(adActivity.CreateOrModifyADDNS, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

		// Mock GetOrCreateCifsService - service exists and needs DDNS
		cifsResult := &active_directory_activities.GetOrCreateCifsServiceResult{
			FQDN:            "",
			NeedsDDNS:       true,
			CifsServiceName: "CIFS-SERVER",
			AdDomain:        "example.com",
		}
		env.OnActivity(adActivity.GetOrCreateCifsService, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(cifsResult, nil).Once()

		// Mock DdnsModify
		env.OnActivity(adActivity.DdnsModify, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

		// Mock CreateJunctionPathForCifsShare
		env.OnActivity(adActivity.CreateJunctionPathForCifsShare, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

		env.ExecuteWorkflow(EnsureCIFSShareWorkflow, volume, node, ad, svmName, externalSVMUUID)

		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("success_service_exists_ddns_already_enabled", func(t *testing.T) {
		env := ts.NewTestWorkflowEnvironment()
		setupTestWorkflowEnvironment(t, env)

		adActivity := &active_directory_activities.ActiveDirectoryActivity{}
		restoreFactory := overrideActiveDirectoryActivityFactory(adActivity)
		defer restoreFactory()

		// Register workflow
		env.RegisterWorkflow(EnsureCIFSShareWorkflow)

		// Register activities
		env.RegisterActivity(adActivity.CreateOrModifyADDNS)
		env.RegisterActivity(adActivity.GetOrCreateCifsService)
		env.RegisterActivity(adActivity.CreateJunctionPathForCifsShare)

		// Mock CreateOrModifyADDNS
		env.OnActivity(adActivity.CreateOrModifyADDNS, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

		// Mock GetOrCreateCifsService - service exists, DDNS already enabled
		cifsResult := &active_directory_activities.GetOrCreateCifsServiceResult{
			FQDN:            "",
			NeedsDDNS:       false,
			CifsServiceName: "CIFS-SERVER",
			AdDomain:        "example.com",
		}
		env.OnActivity(adActivity.GetOrCreateCifsService, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(cifsResult, nil).Once()

		// Mock CreateJunctionPathForCifsShare
		env.OnActivity(adActivity.CreateJunctionPathForCifsShare, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

		env.ExecuteWorkflow(EnsureCIFSShareWorkflow, volume, node, ad, svmName, externalSVMUUID)

		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("error_create_dns_fails", func(t *testing.T) {
		env := ts.NewTestWorkflowEnvironment()
		setupTestWorkflowEnvironment(t, env)

		adActivity := &active_directory_activities.ActiveDirectoryActivity{}
		restoreFactory := overrideActiveDirectoryActivityFactory(adActivity)
		defer restoreFactory()

		// Register workflow
		env.RegisterWorkflow(EnsureCIFSShareWorkflow)

		// Register activities
		env.RegisterActivity(adActivity.CreateOrModifyADDNS)

		// Mock CreateOrModifyADDNS to fail - allow retries
		env.OnActivity(adActivity.CreateOrModifyADDNS, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(assert.AnError)

		env.ExecuteWorkflow(EnsureCIFSShareWorkflow, volume, node, ad, svmName, externalSVMUUID)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("error_get_or_create_cifs_fails", func(t *testing.T) {
		env := ts.NewTestWorkflowEnvironment()
		setupTestWorkflowEnvironment(t, env)

		adActivity := &active_directory_activities.ActiveDirectoryActivity{}
		restoreFactory := overrideActiveDirectoryActivityFactory(adActivity)
		defer restoreFactory()

		// Register workflow
		env.RegisterWorkflow(EnsureCIFSShareWorkflow)

		// Register activities
		env.RegisterActivity(adActivity.CreateOrModifyADDNS)
		env.RegisterActivity(adActivity.GetOrCreateCifsService)

		// Mock CreateOrModifyADDNS
		env.OnActivity(adActivity.CreateOrModifyADDNS, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

		// Mock GetOrCreateCifsService to fail - allow retries
		env.OnActivity(adActivity.GetOrCreateCifsService, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, assert.AnError)

		env.ExecuteWorkflow(EnsureCIFSShareWorkflow, volume, node, ad, svmName, externalSVMUUID)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("error_ddns_modify_fails", func(t *testing.T) {
		env := ts.NewTestWorkflowEnvironment()
		setupTestWorkflowEnvironment(t, env)

		adActivity := &active_directory_activities.ActiveDirectoryActivity{}
		restoreFactory := overrideActiveDirectoryActivityFactory(adActivity)
		defer restoreFactory()

		// Register workflow
		env.RegisterWorkflow(EnsureCIFSShareWorkflow)

		// Register activities
		env.RegisterActivity(adActivity.CreateOrModifyADDNS)
		env.RegisterActivity(adActivity.GetOrCreateCifsService)
		env.RegisterActivity(adActivity.DdnsModify)

		// Mock CreateOrModifyADDNS
		env.OnActivity(adActivity.CreateOrModifyADDNS, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

		// Mock GetOrCreateCifsService - service exists and needs DDNS
		cifsResult := &active_directory_activities.GetOrCreateCifsServiceResult{
			FQDN:            "",
			NeedsDDNS:       true,
			CifsServiceName: "CIFS-SERVER",
			AdDomain:        "example.com",
		}
		env.OnActivity(adActivity.GetOrCreateCifsService, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(cifsResult, nil).Once()

		// Mock DdnsModify to fail - allow retries
		env.OnActivity(adActivity.DdnsModify, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(assert.AnError)

		env.ExecuteWorkflow(EnsureCIFSShareWorkflow, volume, node, ad, svmName, externalSVMUUID)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("error_create_junction_path_fails", func(t *testing.T) {
		env := ts.NewTestWorkflowEnvironment()
		setupTestWorkflowEnvironment(t, env)

		adActivity := &active_directory_activities.ActiveDirectoryActivity{}
		restoreFactory := overrideActiveDirectoryActivityFactory(adActivity)
		defer restoreFactory()

		// Register workflow
		env.RegisterWorkflow(EnsureCIFSShareWorkflow)

		// Register activities
		env.RegisterActivity(adActivity.CreateOrModifyADDNS)
		env.RegisterActivity(adActivity.GetOrCreateCifsService)
		env.RegisterActivity(adActivity.CreateJunctionPathForCifsShare)

		// Mock CreateOrModifyADDNS
		env.OnActivity(adActivity.CreateOrModifyADDNS, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

		// Mock GetOrCreateCifsService
		cifsResult := &active_directory_activities.GetOrCreateCifsServiceResult{
			FQDN:      "server.example.com",
			NeedsDDNS: false,
		}
		env.OnActivity(adActivity.GetOrCreateCifsService, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(cifsResult, nil).Once()

		// Mock CreateJunctionPathForCifsShare to fail - allow retries
		env.OnActivity(adActivity.CreateJunctionPathForCifsShare, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(assert.AnError)

		env.ExecuteWorkflow(EnsureCIFSShareWorkflow, volume, node, ad, svmName, externalSVMUUID)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("skips_when_no_file_properties", func(t *testing.T) {
		env := ts.NewTestWorkflowEnvironment()
		setupTestWorkflowEnvironment(t, env)

		// Register workflow
		env.RegisterWorkflow(EnsureCIFSShareWorkflow)

		blockVolume := &datamodel.Volume{
			Name:             "block-volume",
			VolumeAttributes: &datamodel.VolumeAttributes{},
		}

		env.ExecuteWorkflow(EnsureCIFSShareWorkflow, blockVolume, node, ad, svmName, externalSVMUUID)

		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
}

func overrideActiveDirectoryActivityFactory(activity *active_directory_activities.ActiveDirectoryActivity) func() {
	originalFactory := newActiveDirectoryActivity
	newActiveDirectoryActivity = func() *active_directory_activities.ActiveDirectoryActivity {
		return activity
	}
	return func() {
		newActiveDirectoryActivity = originalFactory
	}
}

// setupTestWorkflowEnvironment sets up common test environment configuration
func setupTestWorkflowEnvironment(t *testing.T, env *testsuite.TestWorkflowEnvironment) {
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
	mockHeader := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{
			"logParam": encodedValue,
		},
	}
	env.SetHeader(mockHeader)
}
