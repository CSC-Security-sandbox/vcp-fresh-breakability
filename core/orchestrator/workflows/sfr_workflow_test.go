package workflows

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	hyperscaler "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestRestoreFilesFromBackupWorkflow(t *testing.T) {
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
		// Set default activity options to avoid "StartToCloseTimeout is not set" errors
		env.SetTestTimeout(time.Hour)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})
		env.RegisterActivity(&activities.SFRActivity{})
		env.RegisterActivity(&activities.VolumeCreateActivity{})

		// Set up test data
		params := &common.RestoreFilesFromBackupParams{
			AccountName:     "test-account",
			SourceFileList:  []string{"/backup.txt", "/restore.txt"},
			RestoreFilePath: "/restore_dir",
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid"},
			Name:      "test-backup-vault",
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
				SnapshotName: "snapshot-name",
			},
		}
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
			Name:      "test-volume",
			AccountID: 1,
			Account:   account,
			Pool: &datamodel.Pool{
				BaseModel:      datamodel.BaseModel{ID: 1},
				DeploymentName: "deployment-name",
				PoolCredentials: &datamodel.PoolCredentials{
					Password:      "password",
					SecretID:      "secret-id",
					CertificateID: "cert-id",
					AuthType:      1,
				},
			},
			PoolID: 1,
		}
		job := &datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "job-uuid", ID: 100},
		}

		// Mock activity responses
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil).Times(2) // Called for PROCESSING and DONE in success case
		env.OnActivity("UpdateBackupRestoreCount", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("20231201120000abcd", nil)
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
		env.OnActivity("GetADCServiceURL", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("https://adc-svc-20231201120000abcd-abc123.run.app", nil)
		fileInodeSizeMap := map[string]*activities.FileInodeAndSize{
			"/backup.txt": {
				Inode: "12345",
				Size:  1024,
			},
			"/restore.txt": {
				Inode: "67890",
				Size:  2048,
			},
		}
		env.OnActivity("GetFileInodeNumbers", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(fileInodeSizeMap, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{
			{
				BaseModel: datamodel.BaseModel{UUID: "node-uuid"},
				Name:      "node-1",
			},
		}, nil)
		env.OnActivity("GetObjStoreNameFromBackupActivity", mock.Anything, mock.Anything, mock.Anything).Return("obj-store-name", nil)
		env.OnActivity("GetBucketDetailsFromBackupActivity", mock.Anything, mock.Anything, mock.Anything).Return(&datamodel.BucketDetails{
			BucketName: "test-bucket",
		}, nil)
		env.OnActivity("GetSmSourcePathActivity", mock.Anything, mock.Anything).Return("/source/path", nil)
		env.OnActivity("GetSmSourcePathForRestoreActivity", mock.Anything, mock.Anything, mock.Anything).Return("/restore/source/path", nil)
		env.OnActivity("GetOrCreateObjectStore", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.CloudTarget{
			UUID: "obj-store-uuid",
		}, nil)
		env.OnActivity("SnapmirrorGetOrCreate", mock.Anything, mock.Anything, mock.Anything).Return(&common.SnapmirrorRelationship{
			UUID: "snapmirror-uuid",
		}, nil)
		env.OnActivity("SnapmirrorTransferWithFiles", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSnapmirrorTransferStatus", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(activities.SmStatusSuccess, nil)
		env.OnActivity("CleanupADCCloudRunService", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.CloudRunOperationResponse{
			OperationName: "operations/cleanup-operation-123",
			Status:        "RUNNING",
		}, nil)
		env.OnActivity("CheckOperationStatus", mock.Anything, "operations/cleanup-operation-123").Return(true, nil)
		env.OnActivity("RemoveRolesFromServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteSA", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVolumeStateInDB", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(job, nil)
		env.OnActivity("PopulateSfrMetadataActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		// Execute workflow
		env.ExecuteWorkflow(RestoreFilesFromBackupWorkflow, params, backup, volume)
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
		env.SetTestTimeout(time.Hour)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})
		env.RegisterActivity(&activities.SFRActivity{})
		env.RegisterActivity(&activities.VolumeCreateActivity{})

		// Set up test data
		params := &common.RestoreFilesFromBackupParams{
			AccountName:    "test-account",
			SourceFileList: []string{"/backup.txt"},
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "account-uuid", ID: 1},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid"},
			Name:      "test-backup-vault",
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{
					BucketName:          "test-bucket",
					TenantProjectNumber: "123456789",
				},
			},
			Account: account,
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket",
				EndpointUUID: "endpoint-uuid",
			},
		}
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
			Account:   account,
			Pool: &datamodel.Pool{
				PoolCredentials: &datamodel.PoolCredentials{},
			},
		}

		// Mock activity responses - GenerateResourceTimestamp fails
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil).Maybe() // May be called if workflow gets far enough
		env.OnActivity("UpdateBackupRestoreCount", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("", errors.New("failed to generate timestamp"))
		env.OnActivity("UpdateVolumeStateInDB", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		// Execute workflow
		env.ExecuteWorkflow(RestoreFilesFromBackupWorkflow, params, backup, volume)
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
		env.SetTestTimeout(time.Hour)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})
		env.RegisterActivity(&activities.SFRActivity{})
		env.RegisterActivity(&activities.VolumeCreateActivity{})

		// Set up test data
		params := &common.RestoreFilesFromBackupParams{
			AccountName:    "test-account",
			SourceFileList: []string{"/backup.txt"},
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "account-uuid", ID: 1},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid"},
			Name:      "test-backup-vault",
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{
					BucketName:          "test-bucket",
					TenantProjectNumber: "123456789",
				},
			},
			Account: account,
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket",
				EndpointUUID: "endpoint-uuid",
			},
		}
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
			Account:   account,
			Pool: &datamodel.Pool{
				PoolCredentials: &datamodel.PoolCredentials{},
			},
		}

		// Mock activity responses - CreateServiceAccount fails
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil).Maybe() // May be called if workflow gets far enough
		env.OnActivity("UpdateBackupRestoreCount", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("20231201120000abcd", nil)
		env.OnActivity("CreateServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to create service account"))
		env.OnActivity("UpdateVolumeStateInDB", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		// Execute workflow
		env.ExecuteWorkflow(RestoreFilesFromBackupWorkflow, params, backup, volume)
		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}
		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("GetFileInodeNumbersFailure", func(t *testing.T) {
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
		env.SetTestTimeout(time.Hour)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})
		env.RegisterActivity(&activities.SFRActivity{})
		env.RegisterActivity(&activities.VolumeCreateActivity{})

		// Set up test data
		params := &common.RestoreFilesFromBackupParams{
			AccountName:    "test-account",
			SourceFileList: []string{"/backup.txt"},
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "account-uuid", ID: 1},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid"},
			Name:      "test-backup-vault",
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{
					BucketName:          "test-bucket",
					TenantProjectNumber: "123456789",
				},
			},
			Account: account,
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			Name:          "test-backup",
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket",
				EndpointUUID: "endpoint-uuid",
				SnapshotID:   "snapshot-uuid",
			},
		}
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
			Account:   account,
			Pool: &datamodel.Pool{
				PoolCredentials: &datamodel.PoolCredentials{},
			},
		}

		// Mock activity responses - GetFileInodeNumbers fails
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil).Maybe() // May be called if workflow gets far enough
		env.OnActivity("UpdateBackupRestoreCount", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("20231201120000abcd", nil)
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
		env.OnActivity("CheckOperationStatus", mock.Anything, "operations/test-operation").Return(true, nil).Once()
		env.OnActivity("GetADCServiceURL", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("https://adc-svc-20231201120000abcd-abc123.run.app", nil)
		env.OnActivity("GetFileInodeNumbers", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to get inode numbers"))
		// Rollback manager calls CleanupADCCloudRunService but doesn't wait for operation status
		env.OnActivity("CleanupADCCloudRunService", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.CloudRunOperationResponse{
			OperationName: "operations/cleanup-operation-123",
			Status:        "RUNNING",
		}, nil)
		env.OnActivity("RemoveRolesFromServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteSA", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVolumeStateInDB", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		// Execute workflow
		env.ExecuteWorkflow(RestoreFilesFromBackupWorkflow, params, backup, volume)
		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}
		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("MissingFilesInBackup", func(t *testing.T) {
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
		env.SetTestTimeout(time.Hour)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})
		env.RegisterActivity(&activities.SFRActivity{})
		env.RegisterActivity(&activities.VolumeCreateActivity{})

		// Set up test data
		params := &common.RestoreFilesFromBackupParams{
			AccountName:    "test-account",
			SourceFileList: []string{"/missing-file.txt"},
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "account-uuid", ID: 1},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid"},
			Name:      "test-backup-vault",
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{
					BucketName:          "test-bucket",
					TenantProjectNumber: "123456789",
				},
			},
			Account: account,
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			Name:          "test-backup",
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket",
				EndpointUUID: "endpoint-uuid",
				SnapshotID:   "snapshot-uuid",
			},
		}
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
			Account:   account,
			Pool: &datamodel.Pool{
				PoolCredentials: &datamodel.PoolCredentials{},
			},
		}

		// Mock activity responses - file not found in backup
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil).Maybe() // May be called if workflow gets far enough
		env.OnActivity("UpdateBackupRestoreCount", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("20231201120000abcd", nil)
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
		env.OnActivity("CheckOperationStatus", mock.Anything, "operations/test-operation").Return(true, nil).Once()
		env.OnActivity("GetADCServiceURL", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("https://adc-svc-20231201120000abcd-abc123.run.app", nil)
		// File not found in backup
		fileInodeSizeMap := map[string]*activities.FileInodeAndSize{}
		env.OnActivity("GetFileInodeNumbers", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(fileInodeSizeMap, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{
			{
				BaseModel: datamodel.BaseModel{UUID: "node-uuid"},
				Name:      "node-1",
			},
		}, nil)
		// Rollback manager calls CleanupADCCloudRunService but doesn't wait for operation status
		env.OnActivity("CleanupADCCloudRunService", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.CloudRunOperationResponse{
			OperationName: "operations/cleanup-operation-123",
			Status:        "RUNNING",
		}, nil)
		env.OnActivity("RemoveRolesFromServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteSA", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVolumeStateInDB", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		// Execute workflow
		env.ExecuteWorkflow(RestoreFilesFromBackupWorkflow, params, backup, volume)
		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}
		// Assert workflow execution - should fail because no files found
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("SnapmirrorTransferFailure", func(t *testing.T) {
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
		env.SetTestTimeout(time.Hour)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})
		env.RegisterActivity(&activities.SFRActivity{})
		env.RegisterActivity(&activities.VolumeCreateActivity{})

		// Set up test data
		params := &common.RestoreFilesFromBackupParams{
			AccountName:    "test-account",
			SourceFileList: []string{"/backup.txt"},
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "account-uuid", ID: 1},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid"},
			Name:      "test-backup-vault",
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{
					BucketName:          "test-bucket",
					TenantProjectNumber: "123456789",
				},
			},
			Account: account,
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			Name:          "test-backup",
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket",
				EndpointUUID: "endpoint-uuid",
				SnapshotID:   "snapshot-uuid",
				SnapshotName: "snapshot-name",
			},
		}
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
			Account:   account,
			PoolID:    1,
			Pool: &datamodel.Pool{
				BaseModel:      datamodel.BaseModel{ID: 1},
				DeploymentName: "deployment-name",
				PoolCredentials: &datamodel.PoolCredentials{
					Password:      "password",
					SecretID:      "secret-id",
					CertificateID: "cert-id",
					AuthType:      1,
				},
			},
		}

		// Mock activity responses - SnapmirrorTransferWithFiles fails
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil).Maybe() // May be called if workflow gets far enough
		env.OnActivity("UpdateBackupRestoreCount", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("20231201120000abcd", nil)
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
		env.OnActivity("CheckOperationStatus", mock.Anything, "operations/test-operation").Return(true, nil).Once()
		env.OnActivity("GetADCServiceURL", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("https://adc-svc-20231201120000abcd-abc123.run.app", nil)
		fileInodeSizeMap := map[string]*activities.FileInodeAndSize{
			"/backup.txt": {
				Inode: "12345",
				Size:  1024,
			},
		}
		env.OnActivity("GetFileInodeNumbers", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(fileInodeSizeMap, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{
			{
				BaseModel: datamodel.BaseModel{UUID: "node-uuid"},
				Name:      "node-1",
			},
		}, nil)
		env.OnActivity("GetObjStoreNameFromBackupActivity", mock.Anything, mock.Anything, mock.Anything).Return("obj-store-name", nil)
		env.OnActivity("GetBucketDetailsFromBackupActivity", mock.Anything, mock.Anything, mock.Anything).Return(&datamodel.BucketDetails{
			BucketName: "test-bucket",
		}, nil)
		env.OnActivity("GetSmSourcePathActivity", mock.Anything, mock.Anything).Return("/source/path", nil)
		env.OnActivity("GetSmSourcePathForRestoreActivity", mock.Anything, mock.Anything, mock.Anything).Return("/restore/source/path", nil)
		env.OnActivity("GetOrCreateObjectStore", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.CloudTarget{
			UUID: "obj-store-uuid",
		}, nil)
		env.OnActivity("SnapmirrorGetOrCreate", mock.Anything, mock.Anything, mock.Anything).Return(&common.SnapmirrorRelationship{
			UUID: "snapmirror-uuid",
		}, nil)
		env.OnActivity("SnapmirrorTransferWithFiles", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to initiate snapmirror transfer"))
		// Rollback manager calls CleanupADCCloudRunService but doesn't wait for operation status
		env.OnActivity("CleanupADCCloudRunService", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.CloudRunOperationResponse{
			OperationName: "operations/cleanup-operation-123",
			Status:        "RUNNING",
		}, nil)
		env.OnActivity("RemoveRolesFromServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteSA", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVolumeStateInDB", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		// Execute workflow
		env.ExecuteWorkflow(RestoreFilesFromBackupWorkflow, params, backup, volume)
		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}
		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("SnapmirrorTransferStatusFailed", func(t *testing.T) {
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
		env.SetTestTimeout(time.Hour)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})
		env.RegisterActivity(&activities.SFRActivity{})
		env.RegisterActivity(&activities.VolumeCreateActivity{})

		// Set up test data
		params := &common.RestoreFilesFromBackupParams{
			AccountName:    "test-account",
			SourceFileList: []string{"/backup.txt"},
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "account-uuid", ID: 1},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid"},
			Name:      "test-backup-vault",
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{
					BucketName:          "test-bucket",
					TenantProjectNumber: "123456789",
				},
			},
			Account: account,
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			Name:          "test-backup",
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket",
				EndpointUUID: "endpoint-uuid",
				SnapshotID:   "snapshot-uuid",
				SnapshotName: "snapshot-name",
			},
		}
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
			Account:   account,
			PoolID:    1,
			Pool: &datamodel.Pool{
				BaseModel:      datamodel.BaseModel{ID: 1},
				DeploymentName: "deployment-name",
				PoolCredentials: &datamodel.PoolCredentials{
					Password:      "password",
					SecretID:      "secret-id",
					CertificateID: "cert-id",
					AuthType:      1,
				},
			},
		}

		// Mock activity responses - transfer status returns failed
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil).Maybe() // May be called if workflow gets far enough
		env.OnActivity("UpdateBackupRestoreCount", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("20231201120000abcd", nil)
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
		env.OnActivity("CheckOperationStatus", mock.Anything, "operations/test-operation").Return(true, nil).Once()
		env.OnActivity("GetADCServiceURL", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("https://adc-svc-20231201120000abcd-abc123.run.app", nil)
		fileInodeSizeMap := map[string]*activities.FileInodeAndSize{
			"/backup.txt": {
				Inode: "12345",
				Size:  1024,
			},
		}
		env.OnActivity("GetFileInodeNumbers", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(fileInodeSizeMap, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{
			{
				BaseModel: datamodel.BaseModel{UUID: "node-uuid"},
				Name:      "node-1",
			},
		}, nil)
		env.OnActivity("GetObjStoreNameFromBackupActivity", mock.Anything, mock.Anything, mock.Anything).Return("obj-store-name", nil)
		env.OnActivity("GetBucketDetailsFromBackupActivity", mock.Anything, mock.Anything, mock.Anything).Return(&datamodel.BucketDetails{
			BucketName: "test-bucket",
		}, nil)
		env.OnActivity("GetSmSourcePathActivity", mock.Anything, mock.Anything).Return("/source/path", nil)
		env.OnActivity("GetSmSourcePathForRestoreActivity", mock.Anything, mock.Anything, mock.Anything).Return("/restore/source/path", nil)
		env.OnActivity("GetOrCreateObjectStore", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.CloudTarget{
			UUID: "obj-store-uuid",
		}, nil)
		env.OnActivity("SnapmirrorGetOrCreate", mock.Anything, mock.Anything, mock.Anything).Return(&common.SnapmirrorRelationship{
			UUID: "snapmirror-uuid",
		}, nil)
		env.OnActivity("SnapmirrorTransferWithFiles", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSnapmirrorTransferStatus", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(activities.SmStatusFailed, nil)
		// Rollback manager calls CleanupADCCloudRunService but doesn't wait for operation status
		env.OnActivity("CleanupADCCloudRunService", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.CloudRunOperationResponse{
			OperationName: "operations/cleanup-operation-123",
			Status:        "RUNNING",
		}, nil)
		env.OnActivity("RemoveRolesFromServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteSA", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVolumeStateInDB", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		// Execute workflow
		env.ExecuteWorkflow(RestoreFilesFromBackupWorkflow, params, backup, volume)
		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}
		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("GetJobFailure", func(t *testing.T) {
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
		env.SetTestTimeout(time.Hour)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})
		env.RegisterActivity(&activities.SFRActivity{})
		env.RegisterActivity(&activities.VolumeCreateActivity{})

		// Set up test data
		params := &common.RestoreFilesFromBackupParams{
			AccountName:    "test-account",
			SourceFileList: []string{"/backup.txt"},
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid"},
			Name:      "test-backup-vault",
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{
					BucketName:          "test-bucket",
					TenantProjectNumber: "123456789",
				},
			},
			Account: account,
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			Name:          "test-backup",
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket",
				EndpointUUID: "endpoint-uuid",
				SnapshotID:   "snapshot-uuid",
				SnapshotName: "snapshot-name",
			},
		}
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
			Name:      "test-volume",
			AccountID: 1,
			Account:   account,
			PoolID:    1,
			Pool: &datamodel.Pool{
				BaseModel:      datamodel.BaseModel{ID: 1},
				DeploymentName: "deployment-name",
				PoolCredentials: &datamodel.PoolCredentials{
					Password:      "password",
					SecretID:      "secret-id",
					CertificateID: "cert-id",
					AuthType:      1,
				},
			},
		}

		// Mock activity responses - GetJob fails but workflow should continue
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil).Times(2) // Called for PROCESSING and DONE
		env.OnActivity("UpdateBackupRestoreCount", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("20231201120000abcd", nil)
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
		env.OnActivity("GetADCServiceURL", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("https://adc-svc-20231201120000abcd-abc123.run.app", nil)
		fileInodeSizeMap := map[string]*activities.FileInodeAndSize{
			"/backup.txt": {
				Inode: "12345",
				Size:  1024,
			},
		}
		env.OnActivity("GetFileInodeNumbers", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(fileInodeSizeMap, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{
			{
				BaseModel: datamodel.BaseModel{UUID: "node-uuid"},
				Name:      "node-1",
			},
		}, nil)
		env.OnActivity("GetObjStoreNameFromBackupActivity", mock.Anything, mock.Anything, mock.Anything).Return("obj-store-name", nil)
		env.OnActivity("GetBucketDetailsFromBackupActivity", mock.Anything, mock.Anything, mock.Anything).Return(&datamodel.BucketDetails{
			BucketName: "test-bucket",
		}, nil)
		env.OnActivity("GetSmSourcePathActivity", mock.Anything, mock.Anything).Return("/source/path", nil)
		env.OnActivity("GetSmSourcePathForRestoreActivity", mock.Anything, mock.Anything, mock.Anything).Return("/restore/source/path", nil)
		env.OnActivity("GetOrCreateObjectStore", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.CloudTarget{
			UUID: "obj-store-uuid",
		}, nil)
		env.OnActivity("SnapmirrorGetOrCreate", mock.Anything, mock.Anything, mock.Anything).Return(&common.SnapmirrorRelationship{
			UUID: "snapmirror-uuid",
		}, nil)
		env.OnActivity("SnapmirrorTransferWithFiles", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSnapmirrorTransferStatus", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(activities.SmStatusSuccess, nil)
		env.OnActivity("CleanupADCCloudRunService", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.CloudRunOperationResponse{
			OperationName: "operations/cleanup-operation-123",
			Status:        "RUNNING",
		}, nil)
		env.OnActivity("CheckOperationStatus", mock.Anything, "operations/cleanup-operation-123").Return(true, nil)
		env.OnActivity("RemoveRolesFromServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteSA", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVolumeStateInDB", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(nil, errors.New("failed to get job"))

		// Execute workflow
		env.ExecuteWorkflow(RestoreFilesFromBackupWorkflow, params, backup, volume)
		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}
		// Assert workflow execution - should succeed even if GetJob fails
		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("PopulateSfrMetadataFailure", func(t *testing.T) {
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
		env.SetTestTimeout(time.Hour)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})
		env.RegisterActivity(&activities.SFRActivity{})
		env.RegisterActivity(&activities.VolumeCreateActivity{})

		// Set up test data
		params := &common.RestoreFilesFromBackupParams{
			AccountName:    "test-account",
			SourceFileList: []string{"/backup.txt"},
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid"},
			Name:      "test-backup-vault",
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{
					BucketName:          "test-bucket",
					TenantProjectNumber: "123456789",
				},
			},
			Account: account,
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			Name:          "test-backup",
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket",
				EndpointUUID: "endpoint-uuid",
				SnapshotID:   "snapshot-uuid",
				SnapshotName: "snapshot-name",
			},
		}
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
			Name:      "test-volume",
			AccountID: 1,
			Account:   account,
			PoolID:    1,
			Pool: &datamodel.Pool{
				BaseModel:      datamodel.BaseModel{ID: 1},
				DeploymentName: "deployment-name",
				PoolCredentials: &datamodel.PoolCredentials{
					Password:      "password",
					SecretID:      "secret-id",
					CertificateID: "cert-id",
					AuthType:      1,
				},
			},
		}
		job := &datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "job-uuid", ID: 100},
		}

		// Mock activity responses - PopulateSfrMetadataActivity fails but workflow should continue
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil).Times(2) // Called for PROCESSING and DONE
		env.OnActivity("UpdateBackupRestoreCount", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("20231201120000abcd", nil)
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
		env.OnActivity("GetADCServiceURL", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("https://adc-svc-20231201120000abcd-abc123.run.app", nil)
		fileInodeSizeMap := map[string]*activities.FileInodeAndSize{
			"/backup.txt": {
				Inode: "12345",
				Size:  1024,
			},
		}
		env.OnActivity("GetFileInodeNumbers", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(fileInodeSizeMap, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{
			{
				BaseModel: datamodel.BaseModel{UUID: "node-uuid"},
				Name:      "node-1",
			},
		}, nil)
		env.OnActivity("GetObjStoreNameFromBackupActivity", mock.Anything, mock.Anything, mock.Anything).Return("obj-store-name", nil)
		env.OnActivity("GetBucketDetailsFromBackupActivity", mock.Anything, mock.Anything, mock.Anything).Return(&datamodel.BucketDetails{
			BucketName: "test-bucket",
		}, nil)
		env.OnActivity("GetSmSourcePathActivity", mock.Anything, mock.Anything).Return("/source/path", nil)
		env.OnActivity("GetSmSourcePathForRestoreActivity", mock.Anything, mock.Anything, mock.Anything).Return("/restore/source/path", nil)
		env.OnActivity("GetOrCreateObjectStore", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.CloudTarget{
			UUID: "obj-store-uuid",
		}, nil)
		env.OnActivity("SnapmirrorGetOrCreate", mock.Anything, mock.Anything, mock.Anything).Return(&common.SnapmirrorRelationship{
			UUID: "snapmirror-uuid",
		}, nil)
		env.OnActivity("SnapmirrorTransferWithFiles", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSnapmirrorTransferStatus", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(activities.SmStatusSuccess, nil)
		env.OnActivity("CleanupADCCloudRunService", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.CloudRunOperationResponse{
			OperationName: "operations/cleanup-operation-123",
			Status:        "RUNNING",
		}, nil)
		env.OnActivity("CheckOperationStatus", mock.Anything, "operations/cleanup-operation-123").Return(true, nil)
		env.OnActivity("RemoveRolesFromServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteSA", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVolumeStateInDB", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(job, nil)
		env.OnActivity("PopulateSfrMetadataActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to populate SfrMetadata"))

		// Execute workflow
		env.ExecuteWorkflow(RestoreFilesFromBackupWorkflow, params, backup, volume)
		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}
		// Assert workflow execution - should succeed even if PopulateSfrMetadataActivity fails
		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	// Additional tests for missing coverage lines
	t.Run("UpdateJobStatusProcessingFailure", func(t *testing.T) {
		// Test lines 38-39: UpdateJobStatus failure when setting to PROCESSING
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
		env.SetTestTimeout(time.Hour)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})
		env.RegisterActivity(&activities.SFRActivity{})
		env.RegisterActivity(&activities.VolumeCreateActivity{})

		params := &common.RestoreFilesFromBackupParams{
			AccountName:    "test-account",
			SourceFileList: []string{"/backup.txt"},
		}
		backup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{UUID: "backup-uuid"},
		}
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
		}

		// Mock UpdateJobStatus to fail - signature is (context.Context, *datamodel.Job)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.MatchedBy(func(job *datamodel.Job) bool {
			return job.State == string(models.JobsStatePROCESSING)
		})).Return(errors.New("failed to update job status"))

		env.ExecuteWorkflow(RestoreFilesFromBackupWorkflow, params, backup, volume)
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("UpdateJobStatusErrorFailure", func(t *testing.T) {
		// Test lines 53-54: UpdateJobStatus failure when setting to ERROR
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
		env.SetTestTimeout(time.Hour)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})
		env.RegisterActivity(&activities.SFRActivity{})
		env.RegisterActivity(&activities.VolumeCreateActivity{})

		params := &common.RestoreFilesFromBackupParams{
			AccountName:    "test-account",
			SourceFileList: []string{"/backup.txt"},
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid"},
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{
					BucketName:          "test-bucket",
					TenantProjectNumber: "123456789",
				},
			},
			Account: account,
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket",
				EndpointUUID: "endpoint-uuid",
			},
		}
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
			Account:   account,
			Pool: &datamodel.Pool{
				PoolCredentials: &datamodel.PoolCredentials{},
			},
		}

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.MatchedBy(func(job *datamodel.Job) bool {
			return job.State == string(models.JobsStatePROCESSING)
		})).Return(nil)
		env.OnActivity("UpdateBackupRestoreCount", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("", errors.New("test error"))
		env.OnActivity("UpdateVolumeStateInDB", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		// UpdateJobStatus should fail when trying to set ERROR status
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.MatchedBy(func(job *datamodel.Job) bool {
			return job.State == string(models.JobsStateERROR)
		})).Return(errors.New("failed to update job status to ERROR"))

		env.ExecuteWorkflow(RestoreFilesFromBackupWorkflow, params, backup, volume)
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("UpdateJobStatusDoneFailure", func(t *testing.T) {
		// Test lines 62-63: UpdateJobStatus failure when setting to DONE
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
		env.SetTestTimeout(time.Hour)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})
		env.RegisterActivity(&activities.SFRActivity{})
		env.RegisterActivity(&activities.VolumeCreateActivity{})

		params := &common.RestoreFilesFromBackupParams{
			AccountName:    "test-account",
			SourceFileList: []string{"/backup.txt"},
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid"},
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{
					BucketName:          "test-bucket",
					TenantProjectNumber: "123456789",
				},
			},
			Account: account,
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket",
				EndpointUUID: "endpoint-uuid",
				SnapshotID:   "snapshot-uuid",
				SnapshotName: "snapshot-name",
			},
		}
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
			Account:   account,
			PoolID:    1,
			Pool: &datamodel.Pool{
				BaseModel:      datamodel.BaseModel{ID: 1},
				DeploymentName: "deployment-name",
				PoolCredentials: &datamodel.PoolCredentials{
					Password:      "password",
					SecretID:      "secret-id",
					CertificateID: "cert-id",
					AuthType:      1,
				},
			},
		}

		// Setup all mocks for successful workflow execution
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.MatchedBy(func(job *datamodel.Job) bool {
			return job.State == string(models.JobsStatePROCESSING)
		})).Return(nil)
		env.OnActivity("UpdateBackupRestoreCount", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("20231201120000abcd", nil)
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
		env.OnActivity("GetADCServiceURL", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("https://adc-svc-20231201120000abcd-abc123.run.app", nil)
		fileInodeSizeMap := map[string]*activities.FileInodeAndSize{
			"/backup.txt": {
				Inode: "12345",
				Size:  1024,
			},
		}
		env.OnActivity("GetFileInodeNumbers", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(fileInodeSizeMap, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{
			{
				BaseModel: datamodel.BaseModel{UUID: "node-uuid"},
				Name:      "node-1",
			},
		}, nil)
		env.OnActivity("GetObjStoreNameFromBackupActivity", mock.Anything, mock.Anything, mock.Anything).Return("obj-store-name", nil)
		env.OnActivity("GetBucketDetailsFromBackupActivity", mock.Anything, mock.Anything, mock.Anything).Return(&datamodel.BucketDetails{
			BucketName: "test-bucket",
		}, nil)
		env.OnActivity("GetSmSourcePathActivity", mock.Anything, mock.Anything).Return("/source/path", nil)
		env.OnActivity("GetSmSourcePathForRestoreActivity", mock.Anything, mock.Anything, mock.Anything).Return("/restore/source/path", nil)
		env.OnActivity("GetOrCreateObjectStore", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.CloudTarget{
			UUID: "obj-store-uuid",
		}, nil)
		env.OnActivity("SnapmirrorGetOrCreate", mock.Anything, mock.Anything, mock.Anything).Return(&common.SnapmirrorRelationship{
			UUID: "snapmirror-uuid",
		}, nil)
		env.OnActivity("SnapmirrorTransferWithFiles", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSnapmirrorTransferStatus", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(activities.SmStatusSuccess, nil)
		env.OnActivity("CleanupADCCloudRunService", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.CloudRunOperationResponse{
			OperationName: "operations/cleanup-operation-123",
			Status:        "RUNNING",
		}, nil)
		env.OnActivity("CheckOperationStatus", mock.Anything, "operations/cleanup-operation-123").Return(true, nil)
		env.OnActivity("RemoveRolesFromServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteSA", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVolumeStateInDB", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(nil, nil)
		// UpdateJobStatus should fail when trying to set DONE status
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.MatchedBy(func(job *datamodel.Job) bool {
			return job.State == string(models.JobsStateDONE)
		})).Return(errors.New("failed to update job status to DONE"))

		env.ExecuteWorkflow(RestoreFilesFromBackupWorkflow, params, backup, volume)
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("BackupVaultNotFound", func(t *testing.T) {
		// Test line 109: Backup vault not found in backup
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
		env.SetTestTimeout(time.Hour)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})
		env.RegisterActivity(&activities.SFRActivity{})
		env.RegisterActivity(&activities.VolumeCreateActivity{})

		params := &common.RestoreFilesFromBackupParams{
			AccountName:    "test-account",
			SourceFileList: []string{"/backup.txt"},
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			BackupVault:   nil, // Backup vault is nil to trigger error
			BackupVaultID: 1,
		}
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
			Account:   account,
			Pool: &datamodel.Pool{
				PoolCredentials: &datamodel.PoolCredentials{},
			},
		}

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil).Maybe()
		env.OnActivity("UpdateBackupRestoreCount", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
		env.OnActivity("UpdateVolumeStateInDB", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

		env.ExecuteWorkflow(RestoreFilesFromBackupWorkflow, params, backup, volume)
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("GetBucketDetailsFailure", func(t *testing.T) {
		// Test lines 194-195: getBucketDetailsForBucket failure
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
		env.SetTestTimeout(time.Hour)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})
		env.RegisterActivity(&activities.SFRActivity{})
		env.RegisterActivity(&activities.VolumeCreateActivity{})

		params := &common.RestoreFilesFromBackupParams{
			AccountName:    "test-account",
			SourceFileList: []string{"/backup.txt"},
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid"},
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{
					BucketName:          "different-bucket",
					TenantProjectNumber: "123456789",
				},
			},
			Account: account,
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket", // Different from bucket in backupVault
				EndpointUUID: "endpoint-uuid",
			},
		}
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
			Account:   account,
			Pool: &datamodel.Pool{
				PoolCredentials: &datamodel.PoolCredentials{},
			},
		}

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil).Maybe()
		env.OnActivity("UpdateBackupRestoreCount", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("20231201120000abcd", nil)
		env.OnActivity("UpdateVolumeStateInDB", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(RestoreFilesFromBackupWorkflow, params, backup, volume)
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("EndpointUUIDOrBucketNameEmpty", func(t *testing.T) {
		// Test line 199: Endpoint UUID or bucket name is empty
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
		env.SetTestTimeout(time.Hour)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})
		env.RegisterActivity(&activities.SFRActivity{})
		env.RegisterActivity(&activities.VolumeCreateActivity{})

		params := &common.RestoreFilesFromBackupParams{
			AccountName:    "test-account",
			SourceFileList: []string{"/backup.txt"},
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid"},
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{
					BucketName:          "", // Empty bucket name
					TenantProjectNumber: "123456789",
				},
			},
			Account: account,
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket",
				EndpointUUID: "", // Empty endpoint UUID
			},
		}
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
			Account:   account,
			Pool: &datamodel.Pool{
				PoolCredentials: &datamodel.PoolCredentials{},
			},
		}

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil).Maybe()
		env.OnActivity("UpdateBackupRestoreCount", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("20231201120000abcd", nil)
		env.OnActivity("UpdateVolumeStateInDB", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(RestoreFilesFromBackupWorkflow, params, backup, volume)
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("IsServiceAccountCreatedFailure", func(t *testing.T) {
		// Test lines 219-220: IsServiceAccountCreated failure
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
		env.SetTestTimeout(time.Hour)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})
		env.RegisterActivity(&activities.SFRActivity{})
		env.RegisterActivity(&activities.VolumeCreateActivity{})

		params := &common.RestoreFilesFromBackupParams{
			AccountName:    "test-account",
			SourceFileList: []string{"/backup.txt"},
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid"},
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{
					BucketName:          "test-bucket",
					TenantProjectNumber: "123456789",
				},
			},
			Account: account,
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket",
				EndpointUUID: "endpoint-uuid",
			},
		}
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
			Account:   account,
			Pool: &datamodel.Pool{
				PoolCredentials: &datamodel.PoolCredentials{},
			},
		}

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil).Maybe()
		env.OnActivity("UpdateBackupRestoreCount", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("20231201120000abcd", nil)
		env.OnActivity("CreateServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.ServiceAccount{Email: "adc-sa@test-project.iam.gserviceaccount.com"}, nil)
		env.OnActivity("IsServiceAccountCreated", mock.Anything, mock.Anything).Return(false, errors.New("failed to check service account"))
		env.OnActivity("UpdateVolumeStateInDB", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		// RemoveRolesFromServiceAccount may be called in cleanup, but might be skipped if service account wasn't created
		env.OnActivity("RemoveRolesFromServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
		env.OnActivity("DeleteSA", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(RestoreFilesFromBackupWorkflow, params, backup, volume)
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("ServiceAccountNotCreated", func(t *testing.T) {
		// Test lines 224-225: Service account is not created
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
		env.SetTestTimeout(time.Hour)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})
		env.RegisterActivity(&activities.SFRActivity{})
		env.RegisterActivity(&activities.VolumeCreateActivity{})

		params := &common.RestoreFilesFromBackupParams{
			AccountName:    "test-account",
			SourceFileList: []string{"/backup.txt"},
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid"},
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{
					BucketName:          "test-bucket",
					TenantProjectNumber: "123456789",
				},
			},
			Account: account,
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket",
				EndpointUUID: "endpoint-uuid",
			},
		}
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
			Account:   account,
			Pool: &datamodel.Pool{
				PoolCredentials: &datamodel.PoolCredentials{},
			},
		}

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil).Maybe()
		env.OnActivity("UpdateBackupRestoreCount", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("20231201120000abcd", nil)
		env.OnActivity("CreateServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.ServiceAccount{Email: "adc-sa@test-project.iam.gserviceaccount.com"}, nil)
		env.OnActivity("IsServiceAccountCreated", mock.Anything, mock.Anything).Return(false, nil) // Service account not created
		env.OnActivity("UpdateVolumeStateInDB", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		// RemoveRolesFromServiceAccount may be called in cleanup, but might be skipped if service account wasn't created
		env.OnActivity("RemoveRolesFromServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
		env.OnActivity("DeleteSA", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(RestoreFilesFromBackupWorkflow, params, backup, volume)
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("AttachRolesToServiceAccountFailure", func(t *testing.T) {
		// Test lines 232-233: AttachRolesToServiceAccount failure
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
		env.SetTestTimeout(time.Hour)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})
		env.RegisterActivity(&activities.SFRActivity{})
		env.RegisterActivity(&activities.VolumeCreateActivity{})

		params := &common.RestoreFilesFromBackupParams{
			AccountName:    "test-account",
			SourceFileList: []string{"/backup.txt"},
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid"},
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{
					BucketName:          "test-bucket",
					TenantProjectNumber: "123456789",
				},
			},
			Account: account,
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket",
				EndpointUUID: "endpoint-uuid",
			},
		}
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
			Account:   account,
			Pool: &datamodel.Pool{
				PoolCredentials: &datamodel.PoolCredentials{},
			},
		}

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil).Maybe()
		env.OnActivity("UpdateBackupRestoreCount", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("20231201120000abcd", nil)
		env.OnActivity("CreateServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.ServiceAccount{Email: "adc-sa@test-project.iam.gserviceaccount.com"}, nil)
		env.OnActivity("IsServiceAccountCreated", mock.Anything, mock.Anything).Return(true, nil)
		env.OnActivity("AttachRolesToServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to attach roles"))
		env.OnActivity("UpdateVolumeStateInDB", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		// RemoveRolesFromServiceAccount may be called in cleanup, but might fail
		env.OnActivity("RemoveRolesFromServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
		env.OnActivity("DeleteSA", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(RestoreFilesFromBackupWorkflow, params, backup, volume)
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("CreateHmacKeysFailure", func(t *testing.T) {
		// Test lines 244-245: CreateHmacKeys failure
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
		env.SetTestTimeout(time.Hour)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})
		env.RegisterActivity(&activities.SFRActivity{})
		env.RegisterActivity(&activities.VolumeCreateActivity{})

		params := &common.RestoreFilesFromBackupParams{
			AccountName:    "test-account",
			SourceFileList: []string{"/backup.txt"},
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid"},
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{
					BucketName:          "test-bucket",
					TenantProjectNumber: "123456789",
				},
			},
			Account: account,
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket",
				EndpointUUID: "endpoint-uuid",
			},
		}
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
			Account:   account,
			Pool: &datamodel.Pool{
				PoolCredentials: &datamodel.PoolCredentials{},
			},
		}

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil).Maybe()
		env.OnActivity("UpdateBackupRestoreCount", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("20231201120000abcd", nil)
		env.OnActivity("CreateServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.ServiceAccount{Email: "adc-sa@test-project.iam.gserviceaccount.com"}, nil)
		env.OnActivity("IsServiceAccountCreated", mock.Anything, mock.Anything).Return(true, nil)
		env.OnActivity("AttachRolesToServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateHmacKeys", mock.Anything, mock.Anything).Return(nil, errors.New("failed to create HMAC keys"))
		env.OnActivity("UpdateVolumeStateInDB", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("RemoveRolesFromServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteSA", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(RestoreFilesFromBackupWorkflow, params, backup, volume)
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("DeployADCCloudRunServiceFailure", func(t *testing.T) {
		// Test lines 300-301: DeployADCCloudRunService failure
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
		env.SetTestTimeout(time.Hour)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})
		env.RegisterActivity(&activities.SFRActivity{})
		env.RegisterActivity(&activities.VolumeCreateActivity{})

		params := &common.RestoreFilesFromBackupParams{
			AccountName:    "test-account",
			SourceFileList: []string{"/backup.txt"},
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid"},
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{
					BucketName:          "test-bucket",
					TenantProjectNumber: "123456789",
				},
			},
			Account: account,
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket",
				EndpointUUID: "endpoint-uuid",
			},
		}
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
			Account:   account,
			Pool: &datamodel.Pool{
				PoolCredentials: &datamodel.PoolCredentials{},
			},
		}

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil).Maybe()
		env.OnActivity("UpdateBackupRestoreCount", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("20231201120000abcd", nil)
		env.OnActivity("CreateServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.ServiceAccount{Email: "adc-sa@test-project.iam.gserviceaccount.com"}, nil)
		env.OnActivity("IsServiceAccountCreated", mock.Anything, mock.Anything).Return(true, nil)
		env.OnActivity("AttachRolesToServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateHmacKeys", mock.Anything, mock.Anything).Return(&common.HmacKeys{
			AccessKey: "dGVzdC1hY2Nlc3Mta2V5",
			SecretKey: "dGVzdC1zZWNyZXQta2V5",
		}, nil)
		env.OnActivity("DeployADCCloudRunService", mock.Anything, mock.Anything).Return(nil, errors.New("failed to deploy Cloud Run service"))
		env.OnActivity("UpdateVolumeStateInDB", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("RemoveRolesFromServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteSA", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(RestoreFilesFromBackupWorkflow, params, backup, volume)
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("CloudRunDeploymentTimeout", func(t *testing.T) {
		// Test lines 312-313, 316-319, 325: Cloud Run deployment timeout
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
		env.SetTestTimeout(time.Hour)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})
		env.RegisterActivity(&activities.SFRActivity{})
		env.RegisterActivity(&activities.VolumeCreateActivity{})

		params := &common.RestoreFilesFromBackupParams{
			AccountName:    "test-account",
			SourceFileList: []string{"/backup.txt"},
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid"},
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{
					BucketName:          "test-bucket",
					TenantProjectNumber: "123456789",
				},
			},
			Account: account,
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket",
				EndpointUUID: "endpoint-uuid",
			},
		}
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
			Account:   account,
			Pool: &datamodel.Pool{
				PoolCredentials: &datamodel.PoolCredentials{},
			},
		}

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil).Maybe()
		env.OnActivity("UpdateBackupRestoreCount", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("20231201120000abcd", nil)
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
		// Return false 20 times to trigger timeout
		env.OnActivity("CheckOperationStatus", mock.Anything, mock.Anything).Return(false, nil).Times(20)
		env.OnActivity("UpdateVolumeStateInDB", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("RemoveRolesFromServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteSA", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(RestoreFilesFromBackupWorkflow, params, backup, volume)
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("GetADCServiceURLFailure", func(t *testing.T) {
		// Test lines 332-333: GetADCServiceURL failure
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
		env.SetTestTimeout(time.Hour)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})
		env.RegisterActivity(&activities.SFRActivity{})
		env.RegisterActivity(&activities.VolumeCreateActivity{})

		params := &common.RestoreFilesFromBackupParams{
			AccountName:    "test-account",
			SourceFileList: []string{"/backup.txt"},
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid"},
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{
					BucketName:          "test-bucket",
					TenantProjectNumber: "123456789",
				},
			},
			Account: account,
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket",
				EndpointUUID: "endpoint-uuid",
			},
		}
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
			Account:   account,
			Pool: &datamodel.Pool{
				PoolCredentials: &datamodel.PoolCredentials{},
			},
		}

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil).Maybe()
		env.OnActivity("UpdateBackupRestoreCount", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("20231201120000abcd", nil)
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
		env.OnActivity("GetADCServiceURL", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("", errors.New("failed to get ADC service URL"))
		env.OnActivity("UpdateVolumeStateInDB", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("RemoveRolesFromServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteSA", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(RestoreFilesFromBackupWorkflow, params, backup, volume)
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("GetNodeFailure", func(t *testing.T) {
		// Test lines 368-369: GetNode failure
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
		env.SetTestTimeout(time.Hour)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})
		env.RegisterActivity(&activities.SFRActivity{})
		env.RegisterActivity(&activities.VolumeCreateActivity{})

		params := &common.RestoreFilesFromBackupParams{
			AccountName:    "test-account",
			SourceFileList: []string{"/backup.txt"},
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid"},
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{
					BucketName:          "test-bucket",
					TenantProjectNumber: "123456789",
				},
			},
			Account: account,
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket",
				EndpointUUID: "endpoint-uuid",
				SnapshotID:   "snapshot-uuid",
			},
		}
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
			Account:   account,
			PoolID:    1,
			Pool: &datamodel.Pool{
				BaseModel:      datamodel.BaseModel{ID: 1},
				DeploymentName: "deployment-name",
				PoolCredentials: &datamodel.PoolCredentials{
					Password:      "password",
					SecretID:      "secret-id",
					CertificateID: "cert-id",
					AuthType:      1,
				},
			},
		}

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil).Maybe()
		env.OnActivity("UpdateBackupRestoreCount", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("20231201120000abcd", nil)
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
		env.OnActivity("GetADCServiceURL", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("https://adc-svc-20231201120000abcd-abc123.run.app", nil)
		fileInodeSizeMap := map[string]*activities.FileInodeAndSize{
			"/backup.txt": {
				Inode: "12345",
				Size:  1024,
			},
		}
		env.OnActivity("GetFileInodeNumbers", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(fileInodeSizeMap, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return(nil, errors.New("failed to get nodes"))
		env.OnActivity("UpdateVolumeStateInDB", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CleanupADCCloudRunService", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.CloudRunOperationResponse{
			OperationName: "operations/cleanup-operation-123",
			Status:        "RUNNING",
		}, nil)
		env.OnActivity("RemoveRolesFromServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteSA", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(RestoreFilesFromBackupWorkflow, params, backup, volume)
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("SnapmirrorTransferTimeout", func(t *testing.T) {
		// Test lines 508-510, 513-515: Snapmirror transfer timeout
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
		env.SetTestTimeout(time.Hour * 25) // Set timeout longer than maxWaitTime
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})
		env.RegisterActivity(&activities.SFRActivity{})
		env.RegisterActivity(&activities.VolumeCreateActivity{})

		params := &common.RestoreFilesFromBackupParams{
			AccountName:    "test-account",
			SourceFileList: []string{"/backup.txt"},
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid"},
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{
					BucketName:          "test-bucket",
					TenantProjectNumber: "123456789",
				},
			},
			Account: account,
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket",
				EndpointUUID: "endpoint-uuid",
				SnapshotID:   "snapshot-uuid",
				SnapshotName: "snapshot-name",
			},
		}
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
			Account:   account,
			PoolID:    1,
			Pool: &datamodel.Pool{
				BaseModel:      datamodel.BaseModel{ID: 1},
				DeploymentName: "deployment-name",
				PoolCredentials: &datamodel.PoolCredentials{
					Password:      "password",
					SecretID:      "secret-id",
					CertificateID: "cert-id",
					AuthType:      1,
				},
			},
		}

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil).Maybe()
		env.OnActivity("UpdateBackupRestoreCount", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("20231201120000abcd", nil)
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
		env.OnActivity("GetADCServiceURL", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("https://adc-svc-20231201120000abcd-abc123.run.app", nil)
		fileInodeSizeMap := map[string]*activities.FileInodeAndSize{
			"/backup.txt": {
				Inode: "12345",
				Size:  1024,
			},
		}
		env.OnActivity("GetFileInodeNumbers", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(fileInodeSizeMap, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{
			{
				BaseModel: datamodel.BaseModel{UUID: "node-uuid"},
				Name:      "node-1",
			},
		}, nil)
		env.OnActivity("GetObjStoreNameFromBackupActivity", mock.Anything, mock.Anything, mock.Anything).Return("obj-store-name", nil)
		env.OnActivity("GetBucketDetailsFromBackupActivity", mock.Anything, mock.Anything, mock.Anything).Return(&datamodel.BucketDetails{
			BucketName: "test-bucket",
		}, nil)
		env.OnActivity("GetSmSourcePathActivity", mock.Anything, mock.Anything).Return("/source/path", nil)
		env.OnActivity("GetSmSourcePathForRestoreActivity", mock.Anything, mock.Anything, mock.Anything).Return("/restore/source/path", nil)
		env.OnActivity("GetOrCreateObjectStore", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.CloudTarget{
			UUID: "obj-store-uuid",
		}, nil)
		env.OnActivity("SnapmirrorGetOrCreate", mock.Anything, mock.Anything, mock.Anything).Return(&common.SnapmirrorRelationship{
			UUID: "snapmirror-uuid",
		}, nil)
		env.OnActivity("SnapmirrorTransferWithFiles", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		// Return "in-progress" status many times to simulate timeout - use Maybe() to allow unlimited calls
		env.OnActivity("GetSnapmirrorTransferStatus", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("in-progress", nil).Maybe()
		env.OnActivity("UpdateVolumeStateInDB", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CleanupADCCloudRunService", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.CloudRunOperationResponse{
			OperationName: "operations/cleanup-operation-123",
			Status:        "RUNNING",
		}, nil)
		env.OnActivity("RemoveRolesFromServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteSA", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(RestoreFilesFromBackupWorkflow, params, backup, volume)
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("SomeFilesMissing", func(t *testing.T) {
		// Test lines 441-443, 477: Some files missing but continue with found files
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
		env.SetTestTimeout(time.Hour)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})
		env.RegisterActivity(&activities.SFRActivity{})
		env.RegisterActivity(&activities.VolumeCreateActivity{})

		params := &common.RestoreFilesFromBackupParams{
			AccountName:    "test-account",
			SourceFileList: []string{"/backup.txt", "/missing.txt"},
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid"},
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{
					BucketName:          "test-bucket",
					TenantProjectNumber: "123456789",
				},
			},
			Account: account,
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket",
				EndpointUUID: "endpoint-uuid",
				SnapshotID:   "snapshot-uuid",
				SnapshotName: "snapshot-name",
			},
		}
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
			Account:   account,
			PoolID:    1,
			Pool: &datamodel.Pool{
				BaseModel:      datamodel.BaseModel{ID: 1},
				DeploymentName: "deployment-name",
				PoolCredentials: &datamodel.PoolCredentials{
					Password:      "password",
					SecretID:      "secret-id",
					CertificateID: "cert-id",
					AuthType:      1,
				},
			},
		}

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil).Times(2)
		env.OnActivity("UpdateBackupRestoreCount", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("20231201120000abcd", nil)
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
		env.OnActivity("GetADCServiceURL", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("https://adc-svc-20231201120000abcd-abc123.run.app", nil)
		// Only one file found in backup
		fileInodeSizeMap := map[string]*activities.FileInodeAndSize{
			"/backup.txt": {
				Inode: "12345",
				Size:  1024,
			},
			// /missing.txt is not in the map
		}
		env.OnActivity("GetFileInodeNumbers", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(fileInodeSizeMap, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{
			{
				BaseModel: datamodel.BaseModel{UUID: "node-uuid"},
				Name:      "node-1",
			},
		}, nil)
		env.OnActivity("GetObjStoreNameFromBackupActivity", mock.Anything, mock.Anything, mock.Anything).Return("obj-store-name", nil)
		env.OnActivity("GetBucketDetailsFromBackupActivity", mock.Anything, mock.Anything, mock.Anything).Return(&datamodel.BucketDetails{
			BucketName: "test-bucket",
		}, nil)
		env.OnActivity("GetSmSourcePathActivity", mock.Anything, mock.Anything).Return("/source/path", nil)
		env.OnActivity("GetSmSourcePathForRestoreActivity", mock.Anything, mock.Anything, mock.Anything).Return("/restore/source/path", nil)
		env.OnActivity("GetOrCreateObjectStore", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.CloudTarget{
			UUID: "obj-store-uuid",
		}, nil)
		env.OnActivity("SnapmirrorGetOrCreate", mock.Anything, mock.Anything, mock.Anything).Return(&common.SnapmirrorRelationship{
			UUID: "snapmirror-uuid",
		}, nil)
		env.OnActivity("SnapmirrorTransferWithFiles", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSnapmirrorTransferStatus", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(activities.SmStatusSuccess, nil)
		env.OnActivity("CleanupADCCloudRunService", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.CloudRunOperationResponse{
			OperationName: "operations/cleanup-operation-123",
			Status:        "RUNNING",
		}, nil)
		env.OnActivity("CheckOperationStatus", mock.Anything, "operations/cleanup-operation-123").Return(true, nil)
		env.OnActivity("RemoveRolesFromServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteSA", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVolumeStateInDB", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		// GetJob may or may not be called depending on workflow path
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(nil, nil).Maybe()

		env.ExecuteWorkflow(RestoreFilesFromBackupWorkflow, params, backup, volume)
		// Should fail because some files are missing (lines 564-567)
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("GetSnapmirrorTransferStatusFailure", func(t *testing.T) {
		// Test lines 495-496: GetSnapmirrorTransferStatus failure
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
		env.SetTestTimeout(time.Hour)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})
		env.RegisterActivity(&activities.SFRActivity{})
		env.RegisterActivity(&activities.VolumeCreateActivity{})

		params := &common.RestoreFilesFromBackupParams{
			AccountName:    "test-account",
			SourceFileList: []string{"/backup.txt"},
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid"},
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{
					BucketName:          "test-bucket",
					TenantProjectNumber: "123456789",
				},
			},
			Account: account,
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket",
				EndpointUUID: "endpoint-uuid",
				SnapshotID:   "snapshot-uuid",
				SnapshotName: "snapshot-name",
			},
		}
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
			Account:   account,
			PoolID:    1,
			Pool: &datamodel.Pool{
				BaseModel:      datamodel.BaseModel{ID: 1},
				DeploymentName: "deployment-name",
				PoolCredentials: &datamodel.PoolCredentials{
					Password:      "password",
					SecretID:      "secret-id",
					CertificateID: "cert-id",
					AuthType:      1,
				},
			},
		}

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil).Maybe()
		env.OnActivity("UpdateBackupRestoreCount", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("20231201120000abcd", nil)
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
		env.OnActivity("GetADCServiceURL", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("https://adc-svc-20231201120000abcd-abc123.run.app", nil)
		fileInodeSizeMap := map[string]*activities.FileInodeAndSize{
			"/backup.txt": {
				Inode: "12345",
				Size:  1024,
			},
		}
		env.OnActivity("GetFileInodeNumbers", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(fileInodeSizeMap, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{
			{
				BaseModel: datamodel.BaseModel{UUID: "node-uuid"},
				Name:      "node-1",
			},
		}, nil)
		env.OnActivity("GetObjStoreNameFromBackupActivity", mock.Anything, mock.Anything, mock.Anything).Return("obj-store-name", nil)
		env.OnActivity("GetBucketDetailsFromBackupActivity", mock.Anything, mock.Anything, mock.Anything).Return(&datamodel.BucketDetails{
			BucketName: "test-bucket",
		}, nil)
		env.OnActivity("GetSmSourcePathActivity", mock.Anything, mock.Anything).Return("/source/path", nil)
		env.OnActivity("GetSmSourcePathForRestoreActivity", mock.Anything, mock.Anything, mock.Anything).Return("/restore/source/path", nil)
		env.OnActivity("GetOrCreateObjectStore", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.CloudTarget{
			UUID: "obj-store-uuid",
		}, nil)
		env.OnActivity("SnapmirrorGetOrCreate", mock.Anything, mock.Anything, mock.Anything).Return(&common.SnapmirrorRelationship{
			UUID: "snapmirror-uuid",
		}, nil)
		env.OnActivity("SnapmirrorTransferWithFiles", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSnapmirrorTransferStatus", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("", errors.New("failed to get transfer status"))
		env.OnActivity("UpdateVolumeStateInDB", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CleanupADCCloudRunService", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.CloudRunOperationResponse{
			OperationName: "operations/cleanup-operation-123",
			Status:        "RUNNING",
		}, nil)
		env.OnActivity("RemoveRolesFromServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteSA", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(RestoreFilesFromBackupWorkflow, params, backup, volume)
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("GetObjStoreNameFromBackupActivityFailure", func(t *testing.T) {
		// Test line 384: GetObjStoreNameFromBackupActivity failure
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
		env.SetTestTimeout(time.Hour)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})
		env.RegisterActivity(&activities.SFRActivity{})
		env.RegisterActivity(&activities.VolumeCreateActivity{})

		params := &common.RestoreFilesFromBackupParams{
			AccountName:    "test-account",
			SourceFileList: []string{"/backup.txt"},
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid"},
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{
					BucketName:          "test-bucket",
					TenantProjectNumber: "123456789",
				},
			},
			Account: account,
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket",
				EndpointUUID: "endpoint-uuid",
				SnapshotID:   "snapshot-uuid",
			},
		}
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
			Account:   account,
			PoolID:    1,
			Pool: &datamodel.Pool{
				BaseModel:      datamodel.BaseModel{ID: 1},
				DeploymentName: "deployment-name",
				PoolCredentials: &datamodel.PoolCredentials{
					Password:      "password",
					SecretID:      "secret-id",
					CertificateID: "cert-id",
					AuthType:      1,
				},
			},
		}

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil).Maybe()
		env.OnActivity("UpdateBackupRestoreCount", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("20231201120000abcd", nil)
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
		env.OnActivity("GetADCServiceURL", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("https://adc-svc-20231201120000abcd-abc123.run.app", nil)
		fileInodeSizeMap := map[string]*activities.FileInodeAndSize{
			"/backup.txt": {
				Inode: "12345",
				Size:  1024,
			},
		}
		env.OnActivity("GetFileInodeNumbers", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(fileInodeSizeMap, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{
			{
				BaseModel: datamodel.BaseModel{UUID: "node-uuid"},
				Name:      "node-1",
			},
		}, nil)
		env.OnActivity("GetObjStoreNameFromBackupActivity", mock.Anything, mock.Anything, mock.Anything).Return("", errors.New("failed to get obj store name"))
		env.OnActivity("UpdateVolumeStateInDB", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CleanupADCCloudRunService", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.CloudRunOperationResponse{
			OperationName: "operations/cleanup-operation-123",
			Status:        "RUNNING",
		}, nil)
		env.OnActivity("RemoveRolesFromServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteSA", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(RestoreFilesFromBackupWorkflow, params, backup, volume)
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("GetBucketDetailsFromBackupActivityFailure", func(t *testing.T) {
		// Test line 390: GetBucketDetailsFromBackupActivity failure
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
		env.SetTestTimeout(time.Hour)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})
		env.RegisterActivity(&activities.SFRActivity{})
		env.RegisterActivity(&activities.VolumeCreateActivity{})

		params := &common.RestoreFilesFromBackupParams{
			AccountName:    "test-account",
			SourceFileList: []string{"/backup.txt"},
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid"},
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{
					BucketName:          "test-bucket",
					TenantProjectNumber: "123456789",
				},
			},
			Account: account,
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket",
				EndpointUUID: "endpoint-uuid",
				SnapshotID:   "snapshot-uuid",
			},
		}
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
			Account:   account,
			PoolID:    1,
			Pool: &datamodel.Pool{
				BaseModel:      datamodel.BaseModel{ID: 1},
				DeploymentName: "deployment-name",
				PoolCredentials: &datamodel.PoolCredentials{
					Password:      "password",
					SecretID:      "secret-id",
					CertificateID: "cert-id",
					AuthType:      1,
				},
			},
		}

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil).Maybe()
		env.OnActivity("UpdateBackupRestoreCount", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("20231201120000abcd", nil)
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
		env.OnActivity("GetADCServiceURL", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("https://adc-svc-20231201120000abcd-abc123.run.app", nil)
		fileInodeSizeMap := map[string]*activities.FileInodeAndSize{
			"/backup.txt": {
				Inode: "12345",
				Size:  1024,
			},
		}
		env.OnActivity("GetFileInodeNumbers", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(fileInodeSizeMap, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{
			{
				BaseModel: datamodel.BaseModel{UUID: "node-uuid"},
				Name:      "node-1",
			},
		}, nil)
		env.OnActivity("GetObjStoreNameFromBackupActivity", mock.Anything, mock.Anything, mock.Anything).Return("obj-store-name", nil)
		env.OnActivity("GetBucketDetailsFromBackupActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to get bucket details"))
		env.OnActivity("UpdateVolumeStateInDB", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CleanupADCCloudRunService", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.CloudRunOperationResponse{
			OperationName: "operations/cleanup-operation-123",
			Status:        "RUNNING",
		}, nil)
		env.OnActivity("RemoveRolesFromServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteSA", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(RestoreFilesFromBackupWorkflow, params, backup, volume)
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("GetSmSourcePathActivityFailure", func(t *testing.T) {
		// Test line 404: GetSmSourcePathActivity failure
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
		env.SetTestTimeout(time.Hour)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})
		env.RegisterActivity(&activities.SFRActivity{})
		env.RegisterActivity(&activities.VolumeCreateActivity{})

		params := &common.RestoreFilesFromBackupParams{
			AccountName:    "test-account",
			SourceFileList: []string{"/backup.txt"},
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid"},
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{
					BucketName:          "test-bucket",
					TenantProjectNumber: "123456789",
				},
			},
			Account: account,
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket",
				EndpointUUID: "endpoint-uuid",
				SnapshotID:   "snapshot-uuid",
			},
		}
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
			Account:   account,
			PoolID:    1,
			Pool: &datamodel.Pool{
				BaseModel:      datamodel.BaseModel{ID: 1},
				DeploymentName: "deployment-name",
				PoolCredentials: &datamodel.PoolCredentials{
					Password:      "password",
					SecretID:      "secret-id",
					CertificateID: "cert-id",
					AuthType:      1,
				},
			},
		}

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil).Maybe()
		env.OnActivity("UpdateBackupRestoreCount", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("20231201120000abcd", nil)
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
		env.OnActivity("GetADCServiceURL", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("https://adc-svc-20231201120000abcd-abc123.run.app", nil)
		fileInodeSizeMap := map[string]*activities.FileInodeAndSize{
			"/backup.txt": {
				Inode: "12345",
				Size:  1024,
			},
		}
		env.OnActivity("GetFileInodeNumbers", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(fileInodeSizeMap, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{
			{
				BaseModel: datamodel.BaseModel{UUID: "node-uuid"},
				Name:      "node-1",
			},
		}, nil)
		env.OnActivity("GetObjStoreNameFromBackupActivity", mock.Anything, mock.Anything, mock.Anything).Return("obj-store-name", nil)
		env.OnActivity("GetBucketDetailsFromBackupActivity", mock.Anything, mock.Anything, mock.Anything).Return(&datamodel.BucketDetails{
			BucketName: "test-bucket",
		}, nil)
		env.OnActivity("GetSmSourcePathActivity", mock.Anything, mock.Anything).Return("", errors.New("failed to get SM source path"))
		env.OnActivity("UpdateVolumeStateInDB", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CleanupADCCloudRunService", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.CloudRunOperationResponse{
			OperationName: "operations/cleanup-operation-123",
			Status:        "RUNNING",
		}, nil)
		env.OnActivity("RemoveRolesFromServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteSA", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(RestoreFilesFromBackupWorkflow, params, backup, volume)
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("GetSmSourcePathForRestoreActivityFailure", func(t *testing.T) {
		// Test line 410: GetSmSourcePathForRestoreActivity failure
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
		env.SetTestTimeout(time.Hour)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})
		env.RegisterActivity(&activities.SFRActivity{})
		env.RegisterActivity(&activities.VolumeCreateActivity{})

		params := &common.RestoreFilesFromBackupParams{
			AccountName:    "test-account",
			SourceFileList: []string{"/backup.txt"},
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid"},
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{
					BucketName:          "test-bucket",
					TenantProjectNumber: "123456789",
				},
			},
			Account: account,
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket",
				EndpointUUID: "endpoint-uuid",
				SnapshotID:   "snapshot-uuid",
			},
		}
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
			Account:   account,
			PoolID:    1,
			Pool: &datamodel.Pool{
				BaseModel:      datamodel.BaseModel{ID: 1},
				DeploymentName: "deployment-name",
				PoolCredentials: &datamodel.PoolCredentials{
					Password:      "password",
					SecretID:      "secret-id",
					CertificateID: "cert-id",
					AuthType:      1,
				},
			},
		}

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil).Maybe()
		env.OnActivity("UpdateBackupRestoreCount", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("20231201120000abcd", nil)
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
		env.OnActivity("GetADCServiceURL", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("https://adc-svc-20231201120000abcd-abc123.run.app", nil)
		fileInodeSizeMap := map[string]*activities.FileInodeAndSize{
			"/backup.txt": {
				Inode: "12345",
				Size:  1024,
			},
		}
		env.OnActivity("GetFileInodeNumbers", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(fileInodeSizeMap, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{
			{
				BaseModel: datamodel.BaseModel{UUID: "node-uuid"},
				Name:      "node-1",
			},
		}, nil)
		env.OnActivity("GetObjStoreNameFromBackupActivity", mock.Anything, mock.Anything, mock.Anything).Return("obj-store-name", nil)
		env.OnActivity("GetBucketDetailsFromBackupActivity", mock.Anything, mock.Anything, mock.Anything).Return(&datamodel.BucketDetails{
			BucketName: "test-bucket",
		}, nil)
		env.OnActivity("GetSmSourcePathActivity", mock.Anything, mock.Anything).Return("/source/path", nil)
		env.OnActivity("GetSmSourcePathForRestoreActivity", mock.Anything, mock.Anything, mock.Anything).Return("", errors.New("failed to get SM source path for restore"))
		env.OnActivity("UpdateVolumeStateInDB", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CleanupADCCloudRunService", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.CloudRunOperationResponse{
			OperationName: "operations/cleanup-operation-123",
			Status:        "RUNNING",
		}, nil)
		env.OnActivity("RemoveRolesFromServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteSA", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(RestoreFilesFromBackupWorkflow, params, backup, volume)
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("GetOrCreateObjectStoreFailure", func(t *testing.T) {
		// Test line 416: GetOrCreateObjectStore failure
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
		env.SetTestTimeout(time.Hour)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})
		env.RegisterActivity(&activities.SFRActivity{})
		env.RegisterActivity(&activities.VolumeCreateActivity{})

		params := &common.RestoreFilesFromBackupParams{
			AccountName:    "test-account",
			SourceFileList: []string{"/backup.txt"},
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid"},
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{
					BucketName:          "test-bucket",
					TenantProjectNumber: "123456789",
				},
			},
			Account: account,
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket",
				EndpointUUID: "endpoint-uuid",
				SnapshotID:   "snapshot-uuid",
			},
		}
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
			Account:   account,
			PoolID:    1,
			Pool: &datamodel.Pool{
				BaseModel:      datamodel.BaseModel{ID: 1},
				DeploymentName: "deployment-name",
				PoolCredentials: &datamodel.PoolCredentials{
					Password:      "password",
					SecretID:      "secret-id",
					CertificateID: "cert-id",
					AuthType:      1,
				},
			},
		}

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil).Maybe()
		env.OnActivity("UpdateBackupRestoreCount", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("20231201120000abcd", nil)
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
		env.OnActivity("GetADCServiceURL", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("https://adc-svc-20231201120000abcd-abc123.run.app", nil)
		fileInodeSizeMap := map[string]*activities.FileInodeAndSize{
			"/backup.txt": {
				Inode: "12345",
				Size:  1024,
			},
		}
		env.OnActivity("GetFileInodeNumbers", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(fileInodeSizeMap, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{
			{
				BaseModel: datamodel.BaseModel{UUID: "node-uuid"},
				Name:      "node-1",
			},
		}, nil)
		env.OnActivity("GetObjStoreNameFromBackupActivity", mock.Anything, mock.Anything, mock.Anything).Return("obj-store-name", nil)
		env.OnActivity("GetBucketDetailsFromBackupActivity", mock.Anything, mock.Anything, mock.Anything).Return(&datamodel.BucketDetails{
			BucketName: "test-bucket",
		}, nil)
		env.OnActivity("GetSmSourcePathActivity", mock.Anything, mock.Anything).Return("/source/path", nil)
		env.OnActivity("GetSmSourcePathForRestoreActivity", mock.Anything, mock.Anything, mock.Anything).Return("/restore/source/path", nil)
		env.OnActivity("GetOrCreateObjectStore", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to get or create object store"))
		env.OnActivity("UpdateVolumeStateInDB", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CleanupADCCloudRunService", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.CloudRunOperationResponse{
			OperationName: "operations/cleanup-operation-123",
			Status:        "RUNNING",
		}, nil)
		env.OnActivity("RemoveRolesFromServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteSA", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(RestoreFilesFromBackupWorkflow, params, backup, volume)
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("SnapmirrorGetOrCreateFailure", func(t *testing.T) {
		// Test line 429: SnapmirrorGetOrCreate failure
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
		env.SetTestTimeout(time.Hour)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})
		env.RegisterActivity(&activities.SFRActivity{})
		env.RegisterActivity(&activities.VolumeCreateActivity{})

		params := &common.RestoreFilesFromBackupParams{
			AccountName:    "test-account",
			SourceFileList: []string{"/backup.txt"},
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid"},
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{
					BucketName:          "test-bucket",
					TenantProjectNumber: "123456789",
				},
			},
			Account: account,
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket",
				EndpointUUID: "endpoint-uuid",
				SnapshotID:   "snapshot-uuid",
			},
		}
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
			Account:   account,
			PoolID:    1,
			Pool: &datamodel.Pool{
				BaseModel:      datamodel.BaseModel{ID: 1},
				DeploymentName: "deployment-name",
				PoolCredentials: &datamodel.PoolCredentials{
					Password:      "password",
					SecretID:      "secret-id",
					CertificateID: "cert-id",
					AuthType:      1,
				},
			},
		}

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil).Maybe()
		env.OnActivity("UpdateBackupRestoreCount", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("20231201120000abcd", nil)
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
		env.OnActivity("GetADCServiceURL", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("https://adc-svc-20231201120000abcd-abc123.run.app", nil)
		fileInodeSizeMap := map[string]*activities.FileInodeAndSize{
			"/backup.txt": {
				Inode: "12345",
				Size:  1024,
			},
		}
		env.OnActivity("GetFileInodeNumbers", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(fileInodeSizeMap, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{
			{
				BaseModel: datamodel.BaseModel{UUID: "node-uuid"},
				Name:      "node-1",
			},
		}, nil)
		env.OnActivity("GetObjStoreNameFromBackupActivity", mock.Anything, mock.Anything, mock.Anything).Return("obj-store-name", nil)
		env.OnActivity("GetBucketDetailsFromBackupActivity", mock.Anything, mock.Anything, mock.Anything).Return(&datamodel.BucketDetails{
			BucketName: "test-bucket",
		}, nil)
		env.OnActivity("GetSmSourcePathActivity", mock.Anything, mock.Anything).Return("/source/path", nil)
		env.OnActivity("GetSmSourcePathForRestoreActivity", mock.Anything, mock.Anything, mock.Anything).Return("/restore/source/path", nil)
		env.OnActivity("GetOrCreateObjectStore", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.CloudTarget{
			UUID: "obj-store-uuid",
		}, nil)
		env.OnActivity("SnapmirrorGetOrCreate", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to get or create snapmirror"))
		env.OnActivity("UpdateVolumeStateInDB", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CleanupADCCloudRunService", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.CloudRunOperationResponse{
			OperationName: "operations/cleanup-operation-123",
			Status:        "RUNNING",
		}, nil)
		env.OnActivity("RemoveRolesFromServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteSA", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(RestoreFilesFromBackupWorkflow, params, backup, volume)
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("CloudRunCleanupCheckStatusFailure", func(t *testing.T) {
		// Test lines 533-534: Cloud Run cleanup check status failure
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
		env.SetTestTimeout(time.Hour)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})
		env.RegisterActivity(&activities.SFRActivity{})
		env.RegisterActivity(&activities.VolumeCreateActivity{})

		params := &common.RestoreFilesFromBackupParams{
			AccountName:    "test-account",
			SourceFileList: []string{"/backup.txt"},
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid"},
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{
					BucketName:          "test-bucket",
					TenantProjectNumber: "123456789",
				},
			},
			Account: account,
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket",
				EndpointUUID: "endpoint-uuid",
				SnapshotID:   "snapshot-uuid",
				SnapshotName: "snapshot-name",
			},
		}
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
			Account:   account,
			PoolID:    1,
			Pool: &datamodel.Pool{
				BaseModel:      datamodel.BaseModel{ID: 1},
				DeploymentName: "deployment-name",
				PoolCredentials: &datamodel.PoolCredentials{
					Password:      "password",
					SecretID:      "secret-id",
					CertificateID: "cert-id",
					AuthType:      1,
				},
			},
		}

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil).Times(2)
		env.OnActivity("UpdateBackupRestoreCount", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("20231201120000abcd", nil)
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
		env.OnActivity("CheckOperationStatus", mock.Anything, "operations/test-operation").Return(true, nil).Once()
		env.OnActivity("GetADCServiceURL", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("https://adc-svc-20231201120000abcd-abc123.run.app", nil)
		fileInodeSizeMap := map[string]*activities.FileInodeAndSize{
			"/backup.txt": {
				Inode: "12345",
				Size:  1024,
			},
		}
		env.OnActivity("GetFileInodeNumbers", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(fileInodeSizeMap, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{
			{
				BaseModel: datamodel.BaseModel{UUID: "node-uuid"},
				Name:      "node-1",
			},
		}, nil)
		env.OnActivity("GetObjStoreNameFromBackupActivity", mock.Anything, mock.Anything, mock.Anything).Return("obj-store-name", nil)
		env.OnActivity("GetBucketDetailsFromBackupActivity", mock.Anything, mock.Anything, mock.Anything).Return(&datamodel.BucketDetails{
			BucketName: "test-bucket",
		}, nil)
		env.OnActivity("GetSmSourcePathActivity", mock.Anything, mock.Anything).Return("/source/path", nil)
		env.OnActivity("GetSmSourcePathForRestoreActivity", mock.Anything, mock.Anything, mock.Anything).Return("/restore/source/path", nil)
		env.OnActivity("GetOrCreateObjectStore", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.CloudTarget{
			UUID: "obj-store-uuid",
		}, nil)
		env.OnActivity("SnapmirrorGetOrCreate", mock.Anything, mock.Anything, mock.Anything).Return(&common.SnapmirrorRelationship{
			UUID: "snapmirror-uuid",
		}, nil)
		env.OnActivity("SnapmirrorTransferWithFiles", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSnapmirrorTransferStatus", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(activities.SmStatusSuccess, nil)
		env.OnActivity("CleanupADCCloudRunService", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.CloudRunOperationResponse{
			OperationName: "operations/cleanup-operation-123",
			Status:        "RUNNING",
		}, nil)
		env.OnActivity("CheckOperationStatus", mock.Anything, "operations/cleanup-operation-123").Return(false, errors.New("failed to check cleanup status"))
		env.OnActivity("UpdateVolumeStateInDB", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("RemoveRolesFromServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteSA", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		// GetJob may or may not be called depending on workflow path
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(nil, nil).Maybe()

		env.ExecuteWorkflow(RestoreFilesFromBackupWorkflow, params, backup, volume)
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("RemoveRolesFromServiceAccountFailure", func(t *testing.T) {
		// Test lines 552-553: RemoveRolesFromServiceAccount failure
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
		env.SetTestTimeout(time.Hour)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})
		env.RegisterActivity(&activities.SFRActivity{})
		env.RegisterActivity(&activities.VolumeCreateActivity{})

		params := &common.RestoreFilesFromBackupParams{
			AccountName:    "test-account",
			SourceFileList: []string{"/backup.txt"},
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid"},
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{
					BucketName:          "test-bucket",
					TenantProjectNumber: "123456789",
				},
			},
			Account: account,
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket",
				EndpointUUID: "endpoint-uuid",
				SnapshotID:   "snapshot-uuid",
				SnapshotName: "snapshot-name",
			},
		}
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
			Account:   account,
			PoolID:    1,
			Pool: &datamodel.Pool{
				BaseModel:      datamodel.BaseModel{ID: 1},
				DeploymentName: "deployment-name",
				PoolCredentials: &datamodel.PoolCredentials{
					Password:      "password",
					SecretID:      "secret-id",
					CertificateID: "cert-id",
					AuthType:      1,
				},
			},
		}

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil).Times(2)
		env.OnActivity("UpdateBackupRestoreCount", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("20231201120000abcd", nil)
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
		env.OnActivity("GetADCServiceURL", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("https://adc-svc-20231201120000abcd-abc123.run.app", nil)
		fileInodeSizeMap := map[string]*activities.FileInodeAndSize{
			"/backup.txt": {
				Inode: "12345",
				Size:  1024,
			},
		}
		env.OnActivity("GetFileInodeNumbers", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(fileInodeSizeMap, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{
			{
				BaseModel: datamodel.BaseModel{UUID: "node-uuid"},
				Name:      "node-1",
			},
		}, nil)
		env.OnActivity("GetObjStoreNameFromBackupActivity", mock.Anything, mock.Anything, mock.Anything).Return("obj-store-name", nil)
		env.OnActivity("GetBucketDetailsFromBackupActivity", mock.Anything, mock.Anything, mock.Anything).Return(&datamodel.BucketDetails{
			BucketName: "test-bucket",
		}, nil)
		env.OnActivity("GetSmSourcePathActivity", mock.Anything, mock.Anything).Return("/source/path", nil)
		env.OnActivity("GetSmSourcePathForRestoreActivity", mock.Anything, mock.Anything, mock.Anything).Return("/restore/source/path", nil)
		env.OnActivity("GetOrCreateObjectStore", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.CloudTarget{
			UUID: "obj-store-uuid",
		}, nil)
		env.OnActivity("SnapmirrorGetOrCreate", mock.Anything, mock.Anything, mock.Anything).Return(&common.SnapmirrorRelationship{
			UUID: "snapmirror-uuid",
		}, nil)
		env.OnActivity("SnapmirrorTransferWithFiles", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSnapmirrorTransferStatus", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(activities.SmStatusSuccess, nil)
		env.OnActivity("CleanupADCCloudRunService", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.CloudRunOperationResponse{
			OperationName: "operations/cleanup-operation-123",
			Status:        "RUNNING",
		}, nil)
		env.OnActivity("CheckOperationStatus", mock.Anything, "operations/cleanup-operation-123").Return(true, nil)
		env.OnActivity("RemoveRolesFromServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to remove roles"))
		env.OnActivity("UpdateVolumeStateInDB", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteSA", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		// GetJob may or may not be called depending on workflow path
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(nil, nil).Maybe()

		env.ExecuteWorkflow(RestoreFilesFromBackupWorkflow, params, backup, volume)
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("DeleteSAFailure", func(t *testing.T) {
		// Test lines 559-560: DeleteSA failure
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
		env.SetTestTimeout(time.Hour)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.ADCActivity{})
		env.RegisterActivity(&activities.BackupActivity{})
		env.RegisterActivity(&activities.SFRActivity{})
		env.RegisterActivity(&activities.VolumeCreateActivity{})

		params := &common.RestoreFilesFromBackupParams{
			AccountName:    "test-account",
			SourceFileList: []string{"/backup.txt"},
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "account-uuid"},
			Name:      "test-account",
		}
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid"},
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{
					BucketName:          "test-bucket",
					TenantProjectNumber: "123456789",
				},
			},
			Account: account,
		}
		backup := &datamodel.Backup{
			BaseModel:     datamodel.BaseModel{UUID: "backup-uuid"},
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes: &datamodel.BackupAttributes{
				BucketName:   "test-bucket",
				EndpointUUID: "endpoint-uuid",
				SnapshotID:   "snapshot-uuid",
				SnapshotName: "snapshot-name",
			},
		}
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
			Account:   account,
			PoolID:    1,
			Pool: &datamodel.Pool{
				BaseModel:      datamodel.BaseModel{ID: 1},
				DeploymentName: "deployment-name",
				PoolCredentials: &datamodel.PoolCredentials{
					Password:      "password",
					SecretID:      "secret-id",
					CertificateID: "cert-id",
					AuthType:      1,
				},
			},
		}

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil).Times(2)
		env.OnActivity("UpdateBackupRestoreCount", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GenerateResourceTimestamp", mock.Anything).Return("20231201120000abcd", nil)
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
		env.OnActivity("GetADCServiceURL", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("https://adc-svc-20231201120000abcd-abc123.run.app", nil)
		fileInodeSizeMap := map[string]*activities.FileInodeAndSize{
			"/backup.txt": {
				Inode: "12345",
				Size:  1024,
			},
		}
		env.OnActivity("GetFileInodeNumbers", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(fileInodeSizeMap, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{
			{
				BaseModel: datamodel.BaseModel{UUID: "node-uuid"},
				Name:      "node-1",
			},
		}, nil)
		env.OnActivity("GetObjStoreNameFromBackupActivity", mock.Anything, mock.Anything, mock.Anything).Return("obj-store-name", nil)
		env.OnActivity("GetBucketDetailsFromBackupActivity", mock.Anything, mock.Anything, mock.Anything).Return(&datamodel.BucketDetails{
			BucketName: "test-bucket",
		}, nil)
		env.OnActivity("GetSmSourcePathActivity", mock.Anything, mock.Anything).Return("/source/path", nil)
		env.OnActivity("GetSmSourcePathForRestoreActivity", mock.Anything, mock.Anything, mock.Anything).Return("/restore/source/path", nil)
		env.OnActivity("GetOrCreateObjectStore", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.CloudTarget{
			UUID: "obj-store-uuid",
		}, nil)
		env.OnActivity("SnapmirrorGetOrCreate", mock.Anything, mock.Anything, mock.Anything).Return(&common.SnapmirrorRelationship{
			UUID: "snapmirror-uuid",
		}, nil)
		env.OnActivity("SnapmirrorTransferWithFiles", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSnapmirrorTransferStatus", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(activities.SmStatusSuccess, nil)
		env.OnActivity("CleanupADCCloudRunService", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler.CloudRunOperationResponse{
			OperationName: "operations/cleanup-operation-123",
			Status:        "RUNNING",
		}, nil)
		env.OnActivity("CheckOperationStatus", mock.Anything, "operations/cleanup-operation-123").Return(true, nil)
		env.OnActivity("RemoveRolesFromServiceAccount", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteSA", mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to delete service account"))
		env.OnActivity("UpdateVolumeStateInDB", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		// GetJob may or may not be called depending on workflow path
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(nil, nil).Maybe()

		env.ExecuteWorkflow(RestoreFilesFromBackupWorkflow, params, backup, volume)
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
}
