package workflows

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	hyperscaler "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestADCWorkflow(t *testing.T) {
	t.Run("onSuccess", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})

		// Set up test data
		params := &common.DeleteBackupParams{
			BackupVaultUUID: "vault-uuid",
			BackupUUID:      "backup-uuid",
			AccountName:     "test-account",
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "account-uuid"},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			Name: "test-backup-vault",
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{
					BucketName:          "test-bucket",
					ServiceAccountName:  "sa-test",
					VendorSubnetID:      "subnet-12345",
					TenantProjectNumber: "123456789",
				},
			},
			Account: account,
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			Name:          "test-backup",
			VolumeUUID:    "test-vol",
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket",
				EndpointUUID: "endpoint-uuid",
				SnapshotID:   "snapshot-uuid",
			},
		}

		// Mock activity responses
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("20231201120000", nil)
		env.OnActivity("CreateServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.ServiceAccount{Email: "adc-sa@test-project.iam.gserviceaccount.com"}, nil)
		env.OnActivity("IsServiceAccountCreated", mock.Anything, mock.Anything).Return(true, nil)
		env.OnActivity("AttachRolesToServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateHmacKeys", mock.Anything, mock.Anything).Return(&common.HmacKeys{
			AccessKey: "dGVzdC1hY2Nlc3Mta2V5",
			SecretKey: "dGVzdC1zZWNyZXQta2V5",
		}, nil)
		env.OnActivity("DeployADCCloudRunService", mock.Anything, mock.Anything).Return(&hyperscaler.CloudRunOperationResponse{
			OperationName: "operations/test-operation",
			Status:        "RUNNING",
		}, nil)
		env.OnActivity("CheckOperationStatus", mock.Anything, mock.Anything).Return(true, nil)
		env.OnActivity("GetADCServiceURL", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("https://adc-svc-20231201120000-abc123.run.app", nil)
		env.OnActivity("InitialDeleteRequestWithCloudRun", mock.Anything, mock.Anything, mock.Anything).Return(&common.ADCResponse{
			StatusCode: http.StatusOK,
		}, nil)
		env.OnActivity("CleanupADCCloudRunService", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.CloudRunOperationResponse{
			OperationName: "operations/cleanup-operation-123",
			Status:        "RUNNING",
		}, nil)
		env.OnActivity("CheckOperationStatus", mock.Anything, "operations/cleanup-operation-123").Return(true, nil)
		env.OnActivity("RemoveRolesFromServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteSA", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		// Step 9: Logical size calculation activities
		env.OnActivity("IsLatestBackupAnyStateActivity", mock.Anything, mock.Anything, mock.Anything).Return(false, nil)
		env.OnActivity("FetchLogicalSizeAndUpdateActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		// Execute workflow
		env.ExecuteWorkflow(ADCWorkflow, params, backupVault, backup, account)
		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}
		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("GenerateResourceTimestampFailure", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})

		// Set up test data
		params := &common.DeleteBackupParams{
			BackupVaultUUID: "vault-uuid",
			BackupUUID:      "backup-uuid",
			AccountName:     "test-account",
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "account-uuid"},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			Name: "test-backup-vault",
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{
					BucketName:          "test-bucket",
					ServiceAccountName:  "sa-test",
					VendorSubnetID:      "subnet-12345",
					TenantProjectNumber: "123456789",
				},
			},
			Account: account,
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			Name:          "test-backup",
			VolumeUUID:    "test-vol",
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket",
				EndpointUUID: "endpoint-uuid",
				SnapshotID:   "snapshot-uuid",
			},
		}

		// Mock activity responses - GenerateResourceTimestamp fails
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("", errors.New("failed to generate timestamp"))

		// Execute workflow
		env.ExecuteWorkflow(ADCWorkflow, params, backupVault, backup, account)
		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}
		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("IsServiceAccountCreatedFailure", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})

		// Set up test data
		params := &common.DeleteBackupParams{
			BackupVaultUUID: "vault-uuid",
			BackupUUID:      "backup-uuid",
			AccountName:     "test-account",
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "account-uuid"},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			Name: "test-backup-vault",
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{
					BucketName:          "test-bucket",
					ServiceAccountName:  "sa-test",
					VendorSubnetID:      "subnet-12345",
					TenantProjectNumber: "123456789",
				},
			},
			Account: account,
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			Name:          "test-backup",
			VolumeUUID:    "test-vol",
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket",
				EndpointUUID: "endpoint-uuid",
				SnapshotID:   "snapshot-uuid",
			},
		}

		// Mock activity responses - IsServiceAccountCreated fails
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("20231201120000", nil)
		env.OnActivity("CreateServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.ServiceAccount{Email: "adc-sa@test-project.iam.gserviceaccount.com"}, nil)
		env.OnActivity("IsServiceAccountCreated", mock.Anything, mock.Anything).Return(false, errors.New("failed to check service account"))
		env.OnActivity("DeleteSA", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		// Execute workflow
		env.ExecuteWorkflow(ADCWorkflow, params, backupVault, backup, account)
		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}
		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("AttachRolesToServiceAccountFailure", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})

		// Set up test data
		params := &common.DeleteBackupParams{
			BackupVaultUUID: "vault-uuid",
			BackupUUID:      "backup-uuid",
			AccountName:     "test-account",
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "account-uuid"},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			Name: "test-backup-vault",
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{
					BucketName:          "test-bucket",
					ServiceAccountName:  "sa-test",
					VendorSubnetID:      "subnet-12345",
					TenantProjectNumber: "123456789",
				},
			},
			Account: account,
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			Name:          "test-backup",
			VolumeUUID:    "test-vol",
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket",
				EndpointUUID: "endpoint-uuid",
				SnapshotID:   "snapshot-uuid",
			},
		}

		// Mock activity responses - AttachRolesToServiceAccount fails
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("20231201120000", nil)
		env.OnActivity("CreateServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.ServiceAccount{Email: "adc-sa@test-project.iam.gserviceaccount.com"}, nil)
		env.OnActivity("IsServiceAccountCreated", mock.Anything, mock.Anything).Return(true, nil)
		env.OnActivity("AttachRolesToServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to attach roles"))
		env.OnActivity("DeleteSA", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		// Execute workflow
		env.ExecuteWorkflow(ADCWorkflow, params, backupVault, backup, account)
		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}
		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("CreateHmacKeysFailure", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})

		// Set up test data
		params := &common.DeleteBackupParams{
			BackupVaultUUID: "vault-uuid",
			BackupUUID:      "backup-uuid",
			AccountName:     "test-account",
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "account-uuid"},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			Name: "test-backup-vault",
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{
					BucketName:          "test-bucket",
					ServiceAccountName:  "sa-test",
					VendorSubnetID:      "subnet-12345",
					TenantProjectNumber: "123456789",
				},
			},
			Account: account,
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			Name:          "test-backup",
			VolumeUUID:    "test-vol",
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket",
				EndpointUUID: "endpoint-uuid",
				SnapshotID:   "snapshot-uuid",
			},
		}

		// Mock activity responses - CreateHmacKeys fails
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("20231201120000", nil)
		env.OnActivity("CreateServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.ServiceAccount{Email: "adc-sa@test-project.iam.gserviceaccount.com"}, nil)
		env.OnActivity("IsServiceAccountCreated", mock.Anything, mock.Anything).Return(true, nil)
		env.OnActivity("AttachRolesToServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateHmacKeys", mock.Anything, mock.Anything).Return(nil, errors.New("failed to create HMAC keys"))
		env.OnActivity("RemoveRolesFromServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteSA", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		// Execute workflow
		env.ExecuteWorkflow(ADCWorkflow, params, backupVault, backup, account)
		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}
		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("DeployADCCloudRunServiceFailure", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})

		// Set up test data
		params := &common.DeleteBackupParams{
			BackupVaultUUID: "vault-uuid",
			BackupUUID:      "backup-uuid",
			AccountName:     "test-account",
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "account-uuid"},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			Name: "test-backup-vault",
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{
					BucketName:          "test-bucket",
					ServiceAccountName:  "sa-test",
					VendorSubnetID:      "subnet-12345",
					TenantProjectNumber: "123456789",
				},
			},
			Account: account,
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			Name:          "test-backup",
			VolumeUUID:    "test-vol",
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket",
				EndpointUUID: "endpoint-uuid",
				SnapshotID:   "snapshot-uuid",
			},
		}

		// Mock activity responses - DeployADCCloudRunService fails
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("20231201120000", nil)
		env.OnActivity("CreateServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.ServiceAccount{Email: "adc-sa@test-project.iam.gserviceaccount.com"}, nil)
		env.OnActivity("IsServiceAccountCreated", mock.Anything, mock.Anything).Return(true, nil)
		env.OnActivity("AttachRolesToServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateHmacKeys", mock.Anything, mock.Anything).Return(&common.HmacKeys{
			AccessKey: "dGVzdC1hY2Nlc3Mta2V5",
			SecretKey: "dGVzdC1zZWNyZXQta2V5",
		}, nil)
		env.OnActivity("DeployADCCloudRunService", mock.Anything, mock.Anything).Return(nil, errors.New("failed to deploy Cloud Run service"))
		env.OnActivity("RemoveRolesFromServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteSA", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		// Execute workflow
		env.ExecuteWorkflow(ADCWorkflow, params, backupVault, backup, account)
		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}
		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("CheckOperationStatusFailure", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})

		// Set up test data
		params := &common.DeleteBackupParams{
			BackupVaultUUID: "vault-uuid",
			BackupUUID:      "backup-uuid",
			AccountName:     "test-account",
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "account-uuid"},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			Name: "test-backup-vault",
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{
					BucketName:          "test-bucket",
					ServiceAccountName:  "sa-test",
					VendorSubnetID:      "subnet-12345",
					TenantProjectNumber: "123456789",
				},
			},
			Account: account,
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			Name:          "test-backup",
			VolumeUUID:    "test-vol",
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket",
				EndpointUUID: "endpoint-uuid",
				SnapshotID:   "snapshot-uuid",
			},
		}

		// Mock activity responses - CheckOperationStatus fails
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("20231201120000", nil)
		env.OnActivity("CreateServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.ServiceAccount{Email: "adc-sa@test-project.iam.gserviceaccount.com"}, nil)
		env.OnActivity("IsServiceAccountCreated", mock.Anything, mock.Anything).Return(true, nil)
		env.OnActivity("AttachRolesToServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateHmacKeys", mock.Anything, mock.Anything).Return(&common.HmacKeys{
			AccessKey: "dGVzdC1hY2Nlc3Mta2V5",
			SecretKey: "dGVzdC1zZWNyZXQta2V5",
		}, nil)
		env.OnActivity("DeployADCCloudRunService", mock.Anything, mock.Anything).Return(&hyperscaler.CloudRunOperationResponse{
			OperationName: "operations/test-operation",
			Status:        "RUNNING",
		}, nil)
		// First call to CheckOperationStatus (for deployment) fails
		env.OnActivity("CheckOperationStatus", mock.Anything, "operations/test-operation").Return(false, errors.New("failed to check operation status"))
		env.OnActivity("CleanupADCCloudRunService", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.CloudRunOperationResponse{
			OperationName: "operations/cleanup-operation-123",
			Status:        "RUNNING",
		}, nil)
		env.OnActivity("RemoveRolesFromServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteSA", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		// Execute workflow
		env.ExecuteWorkflow(ADCWorkflow, params, backupVault, backup, account)
		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}
		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("GetADCServiceURLFailure", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})

		// Set up test data
		params := &common.DeleteBackupParams{
			BackupVaultUUID: "vault-uuid",
			BackupUUID:      "backup-uuid",
			AccountName:     "test-account",
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "account-uuid"},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			Name: "test-backup-vault",
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{
					BucketName:          "test-bucket",
					ServiceAccountName:  "sa-test",
					VendorSubnetID:      "subnet-12345",
					TenantProjectNumber: "123456789",
				},
			},
			Account: account,
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			Name:          "test-backup",
			VolumeUUID:    "test-vol",
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket",
				EndpointUUID: "endpoint-uuid",
				SnapshotID:   "snapshot-uuid",
			},
		}

		// Mock activity responses - GetADCServiceURL fails
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("20231201120000", nil)
		env.OnActivity("CreateServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.ServiceAccount{Email: "adc-sa@test-project.iam.gserviceaccount.com"}, nil)
		env.OnActivity("IsServiceAccountCreated", mock.Anything, mock.Anything).Return(true, nil)
		env.OnActivity("AttachRolesToServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateHmacKeys", mock.Anything, mock.Anything).Return(&common.HmacKeys{
			AccessKey: "dGVzdC1hY2Nlc3Mta2V5",
			SecretKey: "dGVzdC1zZWNyZXQta2V5",
		}, nil)
		env.OnActivity("DeployADCCloudRunService", mock.Anything, mock.Anything).Return(&hyperscaler.CloudRunOperationResponse{
			OperationName: "operations/test-operation",
			Status:        "RUNNING",
		}, nil)
		env.OnActivity("CheckOperationStatus", mock.Anything, mock.Anything).Return(true, nil)
		env.OnActivity("GetADCServiceURL", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("", errors.New("failed to get service URL"))
		env.OnActivity("CleanupADCCloudRunService", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.CloudRunOperationResponse{
			OperationName: "operations/cleanup-operation-123",
			Status:        "RUNNING",
		}, nil)
		env.OnActivity("RemoveRolesFromServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteSA", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		// Execute workflow
		env.ExecuteWorkflow(ADCWorkflow, params, backupVault, backup, account)
		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}
		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("InitialDeleteRequestWithCloudRunFailure", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})

		// Set up test data
		params := &common.DeleteBackupParams{
			BackupVaultUUID: "vault-uuid",
			BackupUUID:      "backup-uuid",
			AccountName:     "test-account",
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "account-uuid"},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			Name: "test-backup-vault",
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{
					BucketName:          "test-bucket",
					ServiceAccountName:  "sa-test",
					VendorSubnetID:      "subnet-12345",
					TenantProjectNumber: "123456789",
				},
			},
			Account: account,
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			Name:          "test-backup",
			VolumeUUID:    "test-vol",
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket",
				EndpointUUID: "endpoint-uuid",
				SnapshotID:   "snapshot-uuid",
			},
		}

		// Mock activity responses - InitialDeleteRequestWithCloudRun fails
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("20231201120000", nil)
		env.OnActivity("CreateServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.ServiceAccount{Email: "adc-sa@test-project.iam.gserviceaccount.com"}, nil)
		env.OnActivity("IsServiceAccountCreated", mock.Anything, mock.Anything).Return(true, nil)
		env.OnActivity("AttachRolesToServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateHmacKeys", mock.Anything, mock.Anything).Return(&common.HmacKeys{
			AccessKey: "dGVzdC1hY2Nlc3Mta2V5",
			SecretKey: "dGVzdC1zZWNyZXQta2V5",
		}, nil)
		env.OnActivity("DeployADCCloudRunService", mock.Anything, mock.Anything).Return(&hyperscaler.CloudRunOperationResponse{
			OperationName: "operations/test-operation",
			Status:        "RUNNING",
		}, nil)
		env.OnActivity("CheckOperationStatus", mock.Anything, mock.Anything).Return(true, nil)
		env.OnActivity("GetADCServiceURL", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("https://adc-svc-20231201120000-abc123.run.app", nil)
		env.OnActivity("InitialDeleteRequestWithCloudRun", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to initiate delete request"))
		env.OnActivity("CleanupADCCloudRunService", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.CloudRunOperationResponse{
			OperationName: "operations/cleanup-operation-123",
			Status:        "RUNNING",
		}, nil)
		env.OnActivity("RemoveRolesFromServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteSA", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		// Execute workflow
		env.ExecuteWorkflow(ADCWorkflow, params, backupVault, backup, account)
		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}
		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("RetryPolicyFailure", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})

		// Set up test data
		params := &common.DeleteBackupParams{
			BackupVaultUUID: "vault-uuid",
			BackupUUID:      "backup-uuid",
			AccountName:     "test-account",
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "account-uuid"},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			Name: "test-backup-vault",
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{
					BucketName:          "test-bucket",
					ServiceAccountName:  "sa-test",
					VendorSubnetID:      "subnet-12345",
					TenantProjectNumber: "123456789",
				},
			},
			Account: account,
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			Name:          "test-backup",
			VolumeUUID:    "test-vol",
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket",
				EndpointUUID: "endpoint-uuid",
				SnapshotID:   "snapshot-uuid",
			},
		}

		// Mock activity responses - GenerateResourceTimestamp fails
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("", errors.New("failed to generate timestamp"))

		// Execute workflow
		env.ExecuteWorkflow(ADCWorkflow, params, backupVault, backup, account)
		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}
		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("CleanupTimeout", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})

		// Set up test data
		params := &common.DeleteBackupParams{
			BackupVaultUUID: "vault-uuid",
			BackupUUID:      "backup-uuid",
			AccountName:     "test-account",
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "account-uuid"},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			Name: "test-backup-vault",
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{
					BucketName:          "test-bucket",
					ServiceAccountName:  "sa-test",
					VendorSubnetID:      "subnet-12345",
					TenantProjectNumber: "123456789",
				},
			},
			Account: account,
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			Name:          "test-backup",
			VolumeUUID:    "test-vol",
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket",
				EndpointUUID: "endpoint-uuid",
				SnapshotID:   "snapshot-uuid",
			},
		}

		// Mock activity responses - all succeed until cleanup
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("20231201120000", nil)
		env.OnActivity("CreateServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.ServiceAccount{Email: "adc-sa@test-project.iam.gserviceaccount.com"}, nil)
		env.OnActivity("IsServiceAccountCreated", mock.Anything, mock.Anything).Return(true, nil)
		env.OnActivity("AttachRolesToServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateHmacKeys", mock.Anything, mock.Anything).Return(&common.HmacKeys{
			AccessKey: "dGVzdC1hY2Nlc3Mta2V5",
			SecretKey: "dGVzdC1zZWNyZXQta2V5",
		}, nil)
		env.OnActivity("DeployADCCloudRunService", mock.Anything, mock.Anything).Return(&hyperscaler.CloudRunOperationResponse{
			OperationName: "operations/test-operation",
			Status:        "RUNNING",
		}, nil)
		env.OnActivity("CheckOperationStatus", mock.Anything, mock.Anything).Return(true, nil)
		env.OnActivity("GetADCServiceURL", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("https://adc-svc-20231201120000-abc123.run.app", nil)
		env.OnActivity("InitialDeleteRequestWithCloudRun", mock.Anything, mock.Anything, mock.Anything).Return(&common.ADCResponse{
			StatusCode: http.StatusOK,
		}, nil)
		env.OnActivity("CleanupADCCloudRunService", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.CloudRunOperationResponse{
			OperationName: "operations/cleanup-operation-123",
			Status:        "RUNNING",
		}, nil)
		// Mock cleanup operation to timeout (never complete)
		env.OnActivity("CheckOperationStatus", mock.Anything, "operations/cleanup-operation-123").Return(false, nil)
		env.OnActivity("RemoveRolesFromServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteSA", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		// Step 9: Logical size calculation activities
		env.OnActivity("IsLatestBackupAnyStateActivity", mock.Anything, mock.Anything, mock.Anything).Return(false, nil)
		env.OnActivity("FetchLogicalSizeAndUpdateActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		// Execute workflow
		env.ExecuteWorkflow(ADCWorkflow, params, backupVault, backup, account)
		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}
		// Assert workflow execution - should complete successfully even with cleanup timeout
		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("SleepFailure", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})

		// Set up test data
		params := &common.DeleteBackupParams{
			BackupVaultUUID: "vault-uuid",
			BackupUUID:      "backup-uuid",
			AccountName:     "test-account",
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "account-uuid"},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			Name: "test-backup-vault",
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{
					BucketName:          "test-bucket",
					ServiceAccountName:  "sa-test",
					VendorSubnetID:      "subnet-12345",
					TenantProjectNumber: "123456789",
				},
			},
			Account: account,
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			Name:          "test-backup",
			VolumeUUID:    "test-vol",
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket",
				EndpointUUID: "endpoint-uuid",
				SnapshotID:   "snapshot-uuid",
			},
		}

		// Mock activity responses - all succeed until sleep
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("20231201120000", nil)
		env.OnActivity("CreateServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.ServiceAccount{Email: "adc-sa@test-project.iam.gserviceaccount.com"}, nil)
		env.OnActivity("IsServiceAccountCreated", mock.Anything, mock.Anything).Return(true, nil)
		env.OnActivity("AttachRolesToServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateHmacKeys", mock.Anything, mock.Anything).Return(&common.HmacKeys{
			AccessKey: "dGVzdC1hY2Nlc3Mta2V5",
			SecretKey: "dGVzdC1zZWNyZXQta2V5",
		}, nil)
		env.OnActivity("DeployADCCloudRunService", mock.Anything, mock.Anything).Return(&hyperscaler.CloudRunOperationResponse{
			OperationName: "operations/test-operation",
			Status:        "RUNNING",
		}, nil)
		env.OnActivity("CheckOperationStatus", mock.Anything, mock.Anything).Return(true, nil)
		env.OnActivity("GetADCServiceURL", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("https://adc-svc-20231201120000-abc123.run.app", nil)
		env.OnActivity("InitialDeleteRequestWithCloudRun", mock.Anything, mock.Anything, mock.Anything).Return(&common.ADCResponse{
			StatusCode: http.StatusOK,
		}, nil)
		env.OnActivity("CleanupADCCloudRunService", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.CloudRunOperationResponse{
			OperationName: "operations/cleanup-operation-123",
			Status:        "RUNNING",
		}, nil)
		env.OnActivity("CheckOperationStatus", mock.Anything, "operations/cleanup-operation-123").Return(true, nil)
		env.OnActivity("RemoveRolesFromServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteSA", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		// Step 9: Logical size calculation activities
		env.OnActivity("IsLatestBackupAnyStateActivity", mock.Anything, mock.Anything, mock.Anything).Return(false, nil)
		env.OnActivity("FetchLogicalSizeAndUpdateActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		// Execute workflow
		env.ExecuteWorkflow(ADCWorkflow, params, backupVault, backup, account)
		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}
		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("GetBucketDetailsFailure", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})

		// Set up test data with non-matching bucket name
		params := &common.DeleteBackupParams{
			BackupVaultUUID: "vault-uuid",
			BackupUUID:      "backup-uuid",
			AccountName:     "test-account",
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "account-uuid"},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			Name: "test-backup-vault",
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{
					BucketName:          "different-bucket",
					ServiceAccountName:  "sa-test",
					VendorSubnetID:      "subnet-12345",
					TenantProjectNumber: "123456789",
				},
			},
			Account: account,
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			Name:          "test-backup",
			VolumeUUID:    "test-vol",
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket", // This doesn't match the bucket in backupVault
				EndpointUUID: "endpoint-uuid",
				SnapshotID:   "snapshot-uuid",
			},
		}

		// Mock activity responses
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("20231201120000", nil)

		// Execute workflow
		env.ExecuteWorkflow(ADCWorkflow, params, backupVault, backup, account)
		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}
		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("CreateServiceAccountFailure", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})

		// Set up test data
		params := &common.DeleteBackupParams{
			BackupVaultUUID: "vault-uuid",
			BackupUUID:      "backup-uuid",
			AccountName:     "test-account",
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "account-uuid"},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			Name: "test-backup-vault",
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{
					BucketName:          "test-bucket",
					ServiceAccountName:  "sa-test",
					VendorSubnetID:      "subnet-12345",
					TenantProjectNumber: "123456789",
				},
			},
			Account: account,
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			Name:          "test-backup",
			VolumeUUID:    "test-vol",
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket",
				EndpointUUID: "endpoint-uuid",
				SnapshotID:   "snapshot-uuid",
			},
		}

		// Mock activity responses - CreateServiceAccount fails
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("20231201120000", nil)
		env.OnActivity("CreateServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to create service account"))

		// Execute workflow
		env.ExecuteWorkflow(ADCWorkflow, params, backupVault, backup, account)
		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}
		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("ServiceAccountNotCreated", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})

		// Set up test data
		params := &common.DeleteBackupParams{
			BackupVaultUUID: "vault-uuid",
			BackupUUID:      "backup-uuid",
			AccountName:     "test-account",
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "account-uuid"},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			Name: "test-backup-vault",
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{
					BucketName:          "test-bucket",
					ServiceAccountName:  "sa-test",
					VendorSubnetID:      "subnet-12345",
					TenantProjectNumber: "123456789",
				},
			},
			Account: account,
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			Name:          "test-backup",
			VolumeUUID:    "test-vol",
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket",
				EndpointUUID: "endpoint-uuid",
				SnapshotID:   "snapshot-uuid",
			},
		}

		// Mock activity responses - IsServiceAccountCreated returns false
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("20231201120000", nil)
		env.OnActivity("CreateServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.ServiceAccount{Email: "adc-sa@test-project.iam.gserviceaccount.com"}, nil)
		env.OnActivity("IsServiceAccountCreated", mock.Anything, mock.Anything).Return(false, nil)

		// Execute workflow
		env.ExecuteWorkflow(ADCWorkflow, params, backupVault, backup, account)
		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}
		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("IsLatestBackupAnyStateActivityFailure", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})

		// Set up test data
		params := &common.DeleteBackupParams{
			BackupVaultUUID: "vault-uuid",
			BackupUUID:      "backup-uuid",
			AccountName:     "test-account",
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "account-uuid"},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			Name: "test-backup-vault",
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{
					BucketName:          "test-bucket",
					ServiceAccountName:  "sa-test",
					VendorSubnetID:      "subnet-12345",
					TenantProjectNumber: "123456789",
				},
			},
			Account: account,
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			Name:          "test-backup",
			VolumeUUID:    "test-vol",
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket",
				EndpointUUID: "endpoint-uuid",
				SnapshotID:   "snapshot-uuid",
			},
		}

		// Mock activity responses - all succeed until logical size calculation
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("20231201120000", nil)
		env.OnActivity("CreateServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.ServiceAccount{Email: "adc-sa@test-project.iam.gserviceaccount.com"}, nil)
		env.OnActivity("IsServiceAccountCreated", mock.Anything, mock.Anything).Return(true, nil)
		env.OnActivity("AttachRolesToServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateHmacKeys", mock.Anything, mock.Anything).Return(&common.HmacKeys{
			AccessKey: "dGVzdC1hY2Nlc3Mta2V5",
			SecretKey: "dGVzdC1zZWNyZXQta2V5",
		}, nil)
		env.OnActivity("DeployADCCloudRunService", mock.Anything, mock.Anything).Return(&hyperscaler.CloudRunOperationResponse{
			OperationName: "operations/test-operation",
			Status:        "RUNNING",
		}, nil)
		env.OnActivity("CheckOperationStatus", mock.Anything, mock.Anything).Return(true, nil)
		env.OnActivity("GetADCServiceURL", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("https://adc-svc-20231201120000-abc123.run.app", nil)
		env.OnActivity("InitialDeleteRequestWithCloudRun", mock.Anything, mock.Anything, mock.Anything).Return(&common.ADCResponse{
			StatusCode: http.StatusOK,
		}, nil)
		env.OnActivity("CleanupADCCloudRunService", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.CloudRunOperationResponse{
			OperationName: "operations/cleanup-operation-123",
			Status:        "RUNNING",
		}, nil)
		env.OnActivity("CheckOperationStatus", mock.Anything, "operations/cleanup-operation-123").Return(true, nil)
		env.OnActivity("RemoveRolesFromServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteSA", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		// Step 9: Logical size calculation activities - IsLatestBackupAnyStateActivity fails
		env.OnActivity("IsLatestBackupAnyStateActivity", mock.Anything, mock.Anything, mock.Anything).Return(false, errors.New("failed to check if backup is latest"))

		// Execute workflow
		env.ExecuteWorkflow(ADCWorkflow, params, backupVault, backup, account)
		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}
		// Assert workflow execution - should complete successfully even with logical size error
		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("FetchLogicalSizeAndUpdateActivityFailure", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})

		// Set up test data
		params := &common.DeleteBackupParams{
			BackupVaultUUID: "vault-uuid",
			BackupUUID:      "backup-uuid",
			AccountName:     "test-account",
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "account-uuid"},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			Name: "test-backup-vault",
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{
					BucketName:          "test-bucket",
					ServiceAccountName:  "sa-test",
					VendorSubnetID:      "subnet-12345",
					TenantProjectNumber: "123456789",
				},
			},
			Account: account,
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			Name:          "test-backup",
			VolumeUUID:    "test-vol",
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket",
				EndpointUUID: "endpoint-uuid",
				SnapshotID:   "snapshot-uuid",
			},
		}

		// Mock activity responses - all succeed until logical size calculation
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("20231201120000", nil)
		env.OnActivity("CreateServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.ServiceAccount{Email: "adc-sa@test-project.iam.gserviceaccount.com"}, nil)
		env.OnActivity("IsServiceAccountCreated", mock.Anything, mock.Anything).Return(true, nil)
		env.OnActivity("AttachRolesToServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateHmacKeys", mock.Anything, mock.Anything).Return(&common.HmacKeys{
			AccessKey: "dGVzdC1hY2Nlc3Mta2V5",
			SecretKey: "dGVzdC1zZWNyZXQta2V5",
		}, nil)
		env.OnActivity("DeployADCCloudRunService", mock.Anything, mock.Anything).Return(&hyperscaler.CloudRunOperationResponse{
			OperationName: "operations/test-operation",
			Status:        "RUNNING",
		}, nil)
		env.OnActivity("CheckOperationStatus", mock.Anything, mock.Anything).Return(true, nil)
		env.OnActivity("GetADCServiceURL", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("https://adc-svc-20231201120000-abc123.run.app", nil)
		env.OnActivity("InitialDeleteRequestWithCloudRun", mock.Anything, mock.Anything, mock.Anything).Return(&common.ADCResponse{
			StatusCode: http.StatusOK,
		}, nil)
		env.OnActivity("CleanupADCCloudRunService", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.CloudRunOperationResponse{
			OperationName: "operations/cleanup-operation-123",
			Status:        "RUNNING",
		}, nil)
		env.OnActivity("CheckOperationStatus", mock.Anything, "operations/cleanup-operation-123").Return(true, nil)
		env.OnActivity("RemoveRolesFromServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteSA", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		// Step 9: Logical size calculation activities - IsLatestBackupAnyStateActivity succeeds but FetchLogicalSizeAndUpdateActivity fails
		env.OnActivity("IsLatestBackupAnyStateActivity", mock.Anything, mock.Anything, mock.Anything).Return(false, nil)
		env.OnActivity("FetchLogicalSizeAndUpdateActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to fetch logical size"))

		// Execute workflow
		env.ExecuteWorkflow(ADCWorkflow, params, backupVault, backup, account)
		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}
		// Assert workflow execution - should complete successfully even with logical size error
		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("IsLatestBackupTrue", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})

		// Set up test data
		params := &common.DeleteBackupParams{
			BackupVaultUUID: "vault-uuid",
			BackupUUID:      "backup-uuid",
			AccountName:     "test-account",
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "account-uuid"},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			Name: "test-backup-vault",
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{
					BucketName:          "test-bucket",
					ServiceAccountName:  "sa-test",
					VendorSubnetID:      "subnet-12345",
					TenantProjectNumber: "123456789",
				},
			},
			Account: account,
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			Name:          "test-backup",
			VolumeUUID:    "test-vol",
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket",
				EndpointUUID: "endpoint-uuid",
				SnapshotID:   "snapshot-uuid",
			},
		}

		// Mock activity responses - all succeed
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("20231201120000", nil)
		env.OnActivity("CreateServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.ServiceAccount{Email: "adc-sa@test-project.iam.gserviceaccount.com"}, nil)
		env.OnActivity("IsServiceAccountCreated", mock.Anything, mock.Anything).Return(true, nil)
		env.OnActivity("AttachRolesToServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateHmacKeys", mock.Anything, mock.Anything).Return(&common.HmacKeys{
			AccessKey: "dGVzdC1hY2Nlc3Mta2V5",
			SecretKey: "dGVzdC1zZWNyZXQta2V5",
		}, nil)
		env.OnActivity("DeployADCCloudRunService", mock.Anything, mock.Anything).Return(&hyperscaler.CloudRunOperationResponse{
			OperationName: "operations/test-operation",
			Status:        "RUNNING",
		}, nil)
		env.OnActivity("CheckOperationStatus", mock.Anything, mock.Anything).Return(true, nil)
		env.OnActivity("GetADCServiceURL", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("https://adc-svc-20231201120000-abc123.run.app", nil)
		env.OnActivity("InitialDeleteRequestWithCloudRun", mock.Anything, mock.Anything, mock.Anything).Return(&common.ADCResponse{
			StatusCode: http.StatusOK,
		}, nil)
		env.OnActivity("CleanupADCCloudRunService", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.CloudRunOperationResponse{
			OperationName: "operations/cleanup-operation-123",
			Status:        "RUNNING",
		}, nil)
		env.OnActivity("CheckOperationStatus", mock.Anything, "operations/cleanup-operation-123").Return(true, nil)
		env.OnActivity("RemoveRolesFromServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteSA", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		// Step 9: Logical size calculation activities - backup is latest, so no logical size update needed
		env.OnActivity("IsLatestBackupAnyStateActivity", mock.Anything, mock.Anything, mock.Anything).Return(true, nil)

		// Execute workflow
		env.ExecuteWorkflow(ADCWorkflow, params, backupVault, backup, account)
		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}
		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("ProgressiveSleepWithRedirect", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})

		// Set up test data
		params := &common.DeleteBackupParams{
			BackupVaultUUID: "vault-uuid",
			BackupUUID:      "backup-uuid",
			AccountName:     "test-account",
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "account-uuid"},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			Name: "test-backup-vault",
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{
					BucketName:          "test-bucket",
					ServiceAccountName:  "sa-test",
					VendorSubnetID:      "subnet-12345",
					TenantProjectNumber: "123456789",
				},
			},
			Account: account,
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			Name:          "test-backup",
			VolumeUUID:    "test-vol",
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket",
				EndpointUUID: "endpoint-uuid",
				SnapshotID:   "snapshot-uuid",
			},
		}

		// Mock activity responses - InitialDeleteRequestWithCloudRun returns redirect
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("20231201120000", nil)
		env.OnActivity("CreateServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.ServiceAccount{Email: "adc-sa@test-project.iam.gserviceaccount.com"}, nil)
		env.OnActivity("IsServiceAccountCreated", mock.Anything, mock.Anything).Return(true, nil)
		env.OnActivity("AttachRolesToServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateHmacKeys", mock.Anything, mock.Anything).Return(&common.HmacKeys{
			AccessKey: "dGVzdC1hY2Nlc3Mta2V5",
			SecretKey: "dGVzdC1zZWNyZXQta2V5",
		}, nil)
		env.OnActivity("DeployADCCloudRunService", mock.Anything, mock.Anything).Return(&hyperscaler.CloudRunOperationResponse{
			OperationName: "operations/test-operation",
			Status:        "RUNNING",
		}, nil)
		env.OnActivity("CheckOperationStatus", mock.Anything, mock.Anything).Return(true, nil)
		env.OnActivity("GetADCServiceURL", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("https://adc-svc-20231201120000-abc123.run.app", nil)
		// Initial delete request returns redirect
		env.OnActivity("InitialDeleteRequestWithCloudRun", mock.Anything, mock.Anything, mock.Anything).Return(&common.ADCResponse{
			StatusCode:  http.StatusTemporaryRedirect,
			RedirectURL: "https://adc-svc-20231201120000-abc123.run.app/status/123",
		}, nil)
		// Status check returns OK after progressive sleep
		env.OnActivity("CheckDeleteStatusWithCloudRun", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.ADCResponse{
			StatusCode: http.StatusOK,
		}, nil)
		env.OnActivity("CleanupADCCloudRunService", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.CloudRunOperationResponse{
			OperationName: "operations/cleanup-operation-123",
			Status:        "RUNNING",
		}, nil)
		env.OnActivity("CheckOperationStatus", mock.Anything, "operations/cleanup-operation-123").Return(true, nil)
		env.OnActivity("RemoveRolesFromServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteSA", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		// Step 9: Logical size calculation activities
		env.OnActivity("IsLatestBackupAnyStateActivity", mock.Anything, mock.Anything, mock.Anything).Return(false, nil)
		env.OnActivity("FetchLogicalSizeAndUpdateActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		// Execute workflow
		env.ExecuteWorkflow(ADCWorkflow, params, backupVault, backup, account)
		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}
		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("ProgressiveSleepWithMultipleRedirects", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})

		// Set up test data
		params := &common.DeleteBackupParams{
			BackupVaultUUID: "vault-uuid",
			BackupUUID:      "backup-uuid",
			AccountName:     "test-account",
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "account-uuid"},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			Name: "test-backup-vault",
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{
					BucketName:          "test-bucket",
					ServiceAccountName:  "sa-test",
					VendorSubnetID:      "subnet-12345",
					TenantProjectNumber: "123456789",
				},
			},
			Account: account,
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			Name:          "test-backup",
			VolumeUUID:    "test-vol",
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket",
				EndpointUUID: "endpoint-uuid",
				SnapshotID:   "snapshot-uuid",
			},
		}

		// Mock activity responses - InitialDeleteRequestWithCloudRun returns redirect
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("20231201120000", nil)
		env.OnActivity("CreateServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.ServiceAccount{Email: "adc-sa@test-project.iam.gserviceaccount.com"}, nil)
		env.OnActivity("IsServiceAccountCreated", mock.Anything, mock.Anything).Return(true, nil)
		env.OnActivity("AttachRolesToServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateHmacKeys", mock.Anything, mock.Anything).Return(&common.HmacKeys{
			AccessKey: "dGVzdC1hY2Nlc3Mta2V5",
			SecretKey: "dGVzdC1zZWNyZXQta2V5",
		}, nil)
		env.OnActivity("DeployADCCloudRunService", mock.Anything, mock.Anything).Return(&hyperscaler.CloudRunOperationResponse{
			OperationName: "operations/test-operation",
			Status:        "RUNNING",
		}, nil)
		env.OnActivity("CheckOperationStatus", mock.Anything, mock.Anything).Return(true, nil)
		env.OnActivity("GetADCServiceURL", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("https://adc-svc-20231201120000-abc123.run.app", nil)
		// Initial delete request returns redirect
		env.OnActivity("InitialDeleteRequestWithCloudRun", mock.Anything, mock.Anything, mock.Anything).Return(&common.ADCResponse{
			StatusCode:  http.StatusTemporaryRedirect,
			RedirectURL: "https://adc-svc-20231201120000-abc123.run.app/status/123",
		}, nil)
		// First status check returns another redirect (triggers progressive sleep)
		env.OnActivity("CheckDeleteStatusWithCloudRun", mock.Anything, mock.Anything, mock.Anything, "https://adc-svc-20231201120000-abc123.run.app/status/123").Return(&common.ADCResponse{
			StatusCode:  http.StatusTemporaryRedirect,
			RedirectURL: "https://adc-svc-20231201120000-abc123.run.app/status/456",
		}, nil)
		// Second status check returns OK
		env.OnActivity("CheckDeleteStatusWithCloudRun", mock.Anything, mock.Anything, mock.Anything, "https://adc-svc-20231201120000-abc123.run.app/status/456").Return(&common.ADCResponse{
			StatusCode: http.StatusOK,
		}, nil)
		env.OnActivity("CleanupADCCloudRunService", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.CloudRunOperationResponse{
			OperationName: "operations/cleanup-operation-123",
			Status:        "RUNNING",
		}, nil)
		env.OnActivity("CheckOperationStatus", mock.Anything, "operations/cleanup-operation-123").Return(true, nil)
		env.OnActivity("RemoveRolesFromServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteSA", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		// Step 9: Logical size calculation activities
		env.OnActivity("IsLatestBackupAnyStateActivity", mock.Anything, mock.Anything, mock.Anything).Return(false, nil)
		env.OnActivity("FetchLogicalSizeAndUpdateActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		// Execute workflow
		env.ExecuteWorkflow(ADCWorkflow, params, backupVault, backup, account)
		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}
		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("ProgressiveSleepWithNotFound", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})

		// Set up test data
		params := &common.DeleteBackupParams{
			BackupVaultUUID: "vault-uuid",
			BackupUUID:      "backup-uuid",
			AccountName:     "test-account",
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "account-uuid"},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			Name: "test-backup-vault",
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{
					BucketName:          "test-bucket",
					ServiceAccountName:  "sa-test",
					VendorSubnetID:      "subnet-12345",
					TenantProjectNumber: "123456789",
				},
			},
			Account: account,
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			Name:          "test-backup",
			VolumeUUID:    "test-vol",
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket",
				EndpointUUID: "endpoint-uuid",
				SnapshotID:   "snapshot-uuid",
			},
		}

		// Mock activity responses - InitialDeleteRequestWithCloudRun returns redirect
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("20231201120000", nil)
		env.OnActivity("CreateServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.ServiceAccount{Email: "adc-sa@test-project.iam.gserviceaccount.com"}, nil)
		env.OnActivity("IsServiceAccountCreated", mock.Anything, mock.Anything).Return(true, nil)
		env.OnActivity("AttachRolesToServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateHmacKeys", mock.Anything, mock.Anything).Return(&common.HmacKeys{
			AccessKey: "dGVzdC1hY2Nlc3Mta2V5",
			SecretKey: "dGVzdC1zZWNyZXQta2V5",
		}, nil)
		env.OnActivity("DeployADCCloudRunService", mock.Anything, mock.Anything).Return(&hyperscaler.CloudRunOperationResponse{
			OperationName: "operations/test-operation",
			Status:        "RUNNING",
		}, nil)
		env.OnActivity("CheckOperationStatus", mock.Anything, mock.Anything).Return(true, nil)
		env.OnActivity("GetADCServiceURL", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("https://adc-svc-20231201120000-abc123.run.app", nil)
		// Initial delete request returns redirect
		env.OnActivity("InitialDeleteRequestWithCloudRun", mock.Anything, mock.Anything, mock.Anything).Return(&common.ADCResponse{
			StatusCode:  http.StatusTemporaryRedirect,
			RedirectURL: "https://adc-svc-20231201120000-abc123.run.app/status/123",
		}, nil)
		// Status check returns not found (triggers progressive sleep logic)
		env.OnActivity("CheckDeleteStatusWithCloudRun", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.ADCResponse{
			StatusCode: http.StatusNotFound,
		}, nil)
		env.OnActivity("CleanupADCCloudRunService", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.CloudRunOperationResponse{
			OperationName: "operations/cleanup-operation-123",
			Status:        "RUNNING",
		}, nil)
		env.OnActivity("CheckOperationStatus", mock.Anything, "operations/cleanup-operation-123").Return(true, nil)
		env.OnActivity("RemoveRolesFromServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteSA", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		// Step 9: Logical size calculation activities
		env.OnActivity("IsLatestBackupAnyStateActivity", mock.Anything, mock.Anything, mock.Anything).Return(false, nil)
		env.OnActivity("FetchLogicalSizeAndUpdateActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		// Execute workflow
		env.ExecuteWorkflow(ADCWorkflow, params, backupVault, backup, account)
		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}
		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
}

func TestCalculateProgressiveSleepDuration(t *testing.T) {
	t.Run("FirstPhase", func(t *testing.T) {
		// Test first phase: elapsed < 5 minutes
		duration := calculateProgressiveSleepDuration(2 * time.Minute)
		assert.Equal(t, firstPhaseSleepDuration, duration)
	})

	t.Run("SecondPhase", func(t *testing.T) {
		// Test second phase: 5 minutes <= elapsed < 15 minutes
		duration := calculateProgressiveSleepDuration(10 * time.Minute)
		assert.Equal(t, secondPhaseSleepDuration, duration)
	})

	t.Run("ThirdPhase", func(t *testing.T) {
		// Test third phase: 15 minutes <= elapsed < 75 minutes
		duration := calculateProgressiveSleepDuration(30 * time.Minute)
		assert.Equal(t, thirdPhaseSleepDuration, duration)
	})

	t.Run("FourthPhase", func(t *testing.T) {
		// Test fourth phase: elapsed >= 75 minutes
		duration := calculateProgressiveSleepDuration(2 * time.Hour)
		assert.Equal(t, fourthPhaseSleepDuration, duration)
	})

	t.Run("BoundaryValues", func(t *testing.T) {
		// Test boundary values
		assert.Equal(t, firstPhaseSleepDuration, calculateProgressiveSleepDuration(4*time.Minute+59*time.Second))
		assert.Equal(t, secondPhaseSleepDuration, calculateProgressiveSleepDuration(5*time.Minute))
		assert.Equal(t, secondPhaseSleepDuration, calculateProgressiveSleepDuration(14*time.Minute+59*time.Second))
		assert.Equal(t, thirdPhaseSleepDuration, calculateProgressiveSleepDuration(15*time.Minute))
		assert.Equal(t, thirdPhaseSleepDuration, calculateProgressiveSleepDuration(74*time.Minute+59*time.Second))
		assert.Equal(t, fourthPhaseSleepDuration, calculateProgressiveSleepDuration(75*time.Minute))
	})
}
