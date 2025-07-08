package workflows

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestBackupWorkflow(t *testing.T) {
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
	env.RegisterActivity(&activities.BackupActivity{}) // Register backup-specific activities

	// Set up test data
	params := &common.CreateBackupParams{
		VolumeUUID:  "test-vol",
		AccountName: "test-account",
		BackupName:  "test-backup",
	}
	backupVault := &datamodel.BackupVault{
		Name:          "test-backup-vault",
		BucketDetails: datamodel.BucketDetailsArray{&datamodel.BucketDetails{BucketName: "test-bucket", ServiceAccountName: "sa-test", VendorSubnetID: "subnet-12345"}},
	}
	backup := &datamodel.Backup{
		State:         "InProgress",
		Name:          "test-backup",
		VolumeUUID:    "test-vol",
		BackupVault:   backupVault,
		BackupVaultID: 1,
		Attributes:    &datamodel.BackupAttributes{},
	}

	volume := &datamodel.Volume{
		Pool:             &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, Username: "username", Password: "password"},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}, VendorSubnetID: "subnet-12345", ExternalUUID: "external-uuid"},
	}

	// Mock activity responses
	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	env.OnActivity("GetOrCreateObjectStore", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.CloudTarget{}, nil)
	env.OnActivity("SnapmirrorGetorCreate", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.SnapmirrorRelationship{UUID: "uuid", DestinationUUID: nillable.ToPointer("snapmirror-uuid")}, nil)
	env.OnActivity("SnapshotCreate", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.SnapshotProviderResponse{ProviderResponse: vsa.ProviderResponse{ExternalUUID: "uuid"}}, nil)
	env.OnActivity("SnapmirrorTransfer", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("SnapmirrorTransferPoll", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("FinishBackup", mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	env.ExecuteWorkflow(CreateBackupWorkflow, params, backup, backupVault, volume)
	_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
	if err != nil {
		t.Fatalf("Failed to query workflow: %v", err)
	}
	// Assert workflow execution
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestBackupWorkflowFail(t *testing.T) {
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
	env.RegisterActivity(&activities.BackupActivity{}) // Register backup-specific activities

	// Set up test data
	params := &common.CreateBackupParams{
		VolumeUUID:  "test-vol",
		AccountName: "test-account",
		BackupName:  "test-backup",
	}
	backupVault := &datamodel.BackupVault{
		Name:          "test-backup-vault",
		BucketDetails: datamodel.BucketDetailsArray{&datamodel.BucketDetails{BucketName: "test-bucket", ServiceAccountName: "sa-test", VendorSubnetID: "subnet-12345"}},
	}
	backup := &datamodel.Backup{
		State:         "InProgress",
		Name:          "test-backup",
		VolumeUUID:    "test-vol",
		BackupVault:   backupVault,
		BackupVaultID: 1,
		Attributes:    &datamodel.BackupAttributes{},
	}

	volume := &datamodel.Volume{
		Pool:             &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, Username: "username", Password: "password"},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}, VendorSubnetID: "subnet-12345", ExternalUUID: "external-uuid"},
	}

	// Mock activity responses
	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	env.OnActivity("GetOrCreateObjectStore", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.CloudTarget{}, errors.New("failed to get or create object store"))
	env.OnActivity("UpdateBackupError", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	env.ExecuteWorkflow(CreateBackupWorkflow, params, backup, backupVault, volume)
	_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
	if err != nil {
		t.Fatalf("Failed to query workflow: %v", err)
	}
	// Assert workflow execution
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestBackupWorkflowFailAfterSnapshot(t *testing.T) {
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
	env.RegisterActivity(&activities.BackupActivity{}) // Register backup-specific activities

	// Set up test data
	params := &common.CreateBackupParams{
		VolumeUUID:  "test-vol",
		AccountName: "test-account",
		BackupName:  "test-backup",
	}
	backupVault := &datamodel.BackupVault{
		Name:          "test-backup-vault",
		BucketDetails: datamodel.BucketDetailsArray{&datamodel.BucketDetails{BucketName: "test-bucket", ServiceAccountName: "sa-test", VendorSubnetID: "subnet-12345"}},
	}
	backup := &datamodel.Backup{
		State:         "InProgress",
		Name:          "test-backup",
		VolumeUUID:    "test-vol",
		BackupVault:   backupVault,
		BackupVaultID: 1,
		Attributes:    &datamodel.BackupAttributes{},
	}

	volume := &datamodel.Volume{
		Pool:             &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, Username: "username", Password: "password"},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}, VendorSubnetID: "subnet-12345", ExternalUUID: "external-uuid"},
	}

	// Mock activity responses
	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	env.OnActivity("GetOrCreateObjectStore", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.CloudTarget{}, nil)
	env.OnActivity("SnapmirrorGetorCreate", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.SnapmirrorRelationship{UUID: "uuid", DestinationUUID: nillable.ToPointer("snapmirror-uuid")}, nil)
	env.OnActivity("SnapshotCreate", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.SnapshotProviderResponse{ProviderResponse: vsa.ProviderResponse{ExternalUUID: "uuid"}}, nil)
	env.OnActivity("SnapmirrorTransfer", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to transfer snapmirror"))
	env.OnActivity("UpdateBackupError", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("DeleteBackupSnapshot", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	env.ExecuteWorkflow(CreateBackupWorkflow, params, backup, backupVault, volume)
	_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
	if err != nil {
		t.Fatalf("Failed to query workflow: %v", err)
	}
	// Assert workflow execution
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestGetSnapshotName(t *testing.T) {
	backup := &datamodel.Backup{Name: "test-backup"}
	expected := "adhoc-:test-backup"
	result := getSnapshotName(backup)
	assert.Equal(t, expected, result, "getSnapshotName should return the correct snapshot name")
}

func TestGetObjStoreName(t *testing.T) {
	backupVault := &datamodel.BackupVault{
		BucketDetails: datamodel.BucketDetailsArray{
			&datamodel.BucketDetails{BucketName: "test-bucket", VendorSubnetID: "subnet-12345"},
		},
	}
	volume := &datamodel.Volume{
		VolumeAttributes: &datamodel.VolumeAttributes{VendorSubnetID: "subnet-12345"},
	}
	expected := "test-bucket"
	result, err := getObjStoreName(backupVault, volume)
	assert.NoError(t, err, "getObjStoreName should not return an error")
	assert.Equal(t, expected, result, "getObjStoreName should return the correct object store name")
}

func TestGetBucketDetails(t *testing.T) {
	backupVault := &datamodel.BackupVault{
		BucketDetails: datamodel.BucketDetailsArray{
			&datamodel.BucketDetails{BucketName: "test-bucket", VendorSubnetID: "subnet-12345"},
		},
	}
	volume := &datamodel.Volume{
		VolumeAttributes: &datamodel.VolumeAttributes{VendorSubnetID: "subnet-12345"},
	}
	expected := &datamodel.BucketDetails{BucketName: "test-bucket", VendorSubnetID: "subnet-12345"}
	result, err := getBucketDetails(backupVault, volume)
	assert.NoError(t, err, "getBucketDetails should not return an error")
	assert.Equal(t, expected, result, "getBucketDetails should return the correct bucket details")
}

func TestGetBucketDetails_NoMatch(t *testing.T) {
	backupVault := &datamodel.BackupVault{
		BucketDetails: datamodel.BucketDetailsArray{
			&datamodel.BucketDetails{BucketName: "test-bucket", VendorSubnetID: "subnet-12345"},
		},
	}
	volume := &datamodel.Volume{
		VolumeAttributes: &datamodel.VolumeAttributes{VendorSubnetID: "subnet-67890"},
	}
	_, err := getBucketDetails(backupVault, volume)
	assert.Error(t, err, "getBucketDetails should return an error if no matching bucket details are found")
}

func TestGetSmSourcePath(t *testing.T) {
	volume := &datamodel.Volume{
		Svm:  &datamodel.Svm{Name: "svm_test"},
		Name: "volume_test",
	}
	expected := "svm_test:volume_test"
	result := getSmSourcePath(volume)
	assert.Equal(t, expected, result, "getSmSourcePath should return the correct source path")
}

func TestGetSmDestinationPath(t *testing.T) {
	backupVault := &datamodel.BackupVault{
		BucketDetails: datamodel.BucketDetailsArray{
			&datamodel.BucketDetails{BucketName: "test-bucket", VendorSubnetID: "subnet-12345"},
		},
	}
	volume := &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
		VolumeAttributes: &datamodel.VolumeAttributes{VendorSubnetID: "subnet-12345"},
	}
	expected := "test-bucket:/objstore/volume-uuid"
	result, err := getSmDestinationPath(backupVault, volume)
	assert.NoError(t, err, "getSmDestinationPath should not return an error")
	assert.Equal(t, expected, result, "getSmDestinationPath should return the correct destination path")
}

func TestGetSmDestinationPath_Error(t *testing.T) {
	backupVault := &datamodel.BackupVault{
		BucketDetails: datamodel.BucketDetailsArray{
			&datamodel.BucketDetails{BucketName: "test-bucket", VendorSubnetID: "subnet-12345"},
		},
	}
	volume := &datamodel.Volume{
		VolumeAttributes: &datamodel.VolumeAttributes{VendorSubnetID: "subnet-67890"},
	}
	_, err := getSmDestinationPath(backupVault, volume)
	assert.Error(t, err, "getSmDestinationPath should return an error if object store name cannot be retrieved")
}

func TestBackupWorkflowSuccess(t *testing.T) {
	t.Run("BackupWorkflowSuccess", func(t *testing.T) {
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
		env.RegisterActivity(&activities.BackupActivity{})

		// Set up test data
		params := &common.DeleteBackupParams{
			BackupVaultUUID: "vault-uuid",
			BackupUUID:      "backup-uuid",
			AccountName:     "test-account",
		}
		backupVault := &datamodel.BackupVault{
			Name: "test-backup-vault",
			BucketDetails: datamodel.BucketDetailsArray{
				&datamodel.BucketDetails{BucketName: "test-bucket", ServiceAccountName: "sa-test", VendorSubnetID: "subnet-12345"},
			},
		}
		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "test-vol"},
			Pool: &datamodel.Pool{
				BaseModel: datamodel.BaseModel{ID: int64(1)},
			},
			Svm: &datamodel.Svm{
				Name: "svm_test",
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				VendorSubnetID: "subnet-12345",
			},
		}
		backup := &datamodel.Backup{
			Name:          "test-backup",
			VolumeUUID:    "test-vol",
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes:    &datamodel.BackupAttributes{BucketName: "test-bucket"},
		}

		// Mock activity responses
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetBackupVault", mock.Anything, params.BackupVaultUUID).Return(backupVault, nil)
		env.OnActivity("GetBackup", mock.Anything, params.BackupVaultUUID, params.BackupUUID, params.AccountName).Return(backup, nil)
		env.OnActivity("IsVolumeDeleted", mock.Anything, backup.VolumeUUID).Return(false, nil)
		env.OnActivity("GetBackupCountByVolumeUUID", mock.Anything, backup.VolumeUUID).Return(int64(1), nil)
		env.OnActivity("GetVolume", mock.Anything, backup.VolumeUUID).Return(volume, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
		env.OnActivity("GetObjectStore", mock.Anything, mock.Anything, mock.Anything).Return(&common.CloudTarget{}, nil)
		env.OnActivity("GetSnapmirror", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.SnapmirrorRelationship{UUID: "snapmirror-uuid"}, nil)
		env.OnActivity("DeleteSnapmirror", mock.Anything, mock.Anything, mock.Anything).Return(&vsa.OntapAsyncResponse{JobUUID: "job-uuid"}, nil)
		env.OnActivity("GetOntapJob", mock.Anything, mock.Anything, mock.Anything).Return(&vsa.OntapJob{State: "success"}, nil)
		env.OnActivity("DeleteCloudEndpoint", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.OntapAsyncResponse{JobUUID: "job-uuid"}, nil)
		env.OnActivity("DeleteSnapshotForBackup", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteBackup", mock.Anything, params.BackupUUID).Return(nil, nil)

		// Execute workflow
		env.ExecuteWorkflow(DeleteBackupWorkflow, params)

		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("UpdateJobStatusFailed", func(t *testing.T) {
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
		env.RegisterActivity(&activities.BackupActivity{})
		// Set up test data
		params := &common.DeleteBackupParams{
			BackupVaultUUID: "vault-uuid",
			BackupUUID:      "backup-uuid",
			AccountName:     "test-account",
		}

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(errors.New("failed to update job status"))

		env.ExecuteWorkflow(DeleteBackupWorkflow, params)

		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
}

func TestObjStoreNameFromBackup_ReturnsValidName(t *testing.T) {
	backupVault := &datamodel.BackupVault{
		BucketDetails: datamodel.BucketDetailsArray{
			&datamodel.BucketDetails{BucketName: "test-bucket", VendorSubnetID: "subnet-12345"},
		},
	}
	backup := &datamodel.Backup{
		Attributes: &datamodel.BackupAttributes{BucketName: "test-bucket"},
	}
	expected := "test-bucket"
	result, err := getObjStoreNameFromBackup(backupVault, backup)
	assert.NoError(t, err)
	assert.Equal(t, expected, result)
}

func TestObjStoreNameFromBackup_BucketNotFound(t *testing.T) {
	backupVault := &datamodel.BackupVault{
		BucketDetails: datamodel.BucketDetailsArray{
			&datamodel.BucketDetails{BucketName: "test-bucket", VendorSubnetID: "subnet-12345"},
		},
	}
	backup := &datamodel.Backup{
		Attributes: &datamodel.BackupAttributes{BucketName: "non-existent-bucket"},
	}
	_, err := getObjStoreNameFromBackup(backupVault, backup)
	assert.Error(t, err)
}

func TestBucketDetailsFromBackup_ReturnsValidDetails(t *testing.T) {
	backupVault := &datamodel.BackupVault{
		BucketDetails: datamodel.BucketDetailsArray{
			&datamodel.BucketDetails{BucketName: "test-bucket", VendorSubnetID: "subnet-12345"},
		},
	}
	backup := &datamodel.Backup{
		Attributes: &datamodel.BackupAttributes{BucketName: "test-bucket"},
	}
	expected := &datamodel.BucketDetails{BucketName: "test-bucket", VendorSubnetID: "subnet-12345"}
	result, err := getBucketDetailsFromBackup(backupVault, backup)
	assert.NoError(t, err)
	assert.Equal(t, expected, result)
}

func TestBucketDetailsFromBackup_BucketNotFound(t *testing.T) {
	backupVault := &datamodel.BackupVault{
		BucketDetails: datamodel.BucketDetailsArray{
			&datamodel.BucketDetails{BucketName: "test-bucket", VendorSubnetID: "subnet-12345"},
		},
	}
	backup := &datamodel.Backup{
		Attributes: &datamodel.BackupAttributes{BucketName: "non-existent-bucket"},
	}
	_, err := getBucketDetailsFromBackup(backupVault, backup)
	assert.Error(t, err)
}

func TestSmSourcePathForRestore_ReturnsValidPath(t *testing.T) {
	backupVault := &datamodel.BackupVault{
		BucketDetails: datamodel.BucketDetailsArray{
			&datamodel.BucketDetails{BucketName: "test-bucket", VendorSubnetID: "subnet-12345"},
		},
	}
	backup := &datamodel.Backup{
		Attributes: &datamodel.BackupAttributes{BucketName: "test-bucket", SnapshotID: "snapshot-uuid"},
	}
	expected := "test-bucket:/objstore/snapshot-uuid"
	result, err := getSmSourcePathForRestore(backupVault, backup)
	assert.NoError(t, err)
	assert.Equal(t, expected, result)
}

func TestSmSourcePathForRestore_BucketNotFound(t *testing.T) {
	backupVault := &datamodel.BackupVault{
		BucketDetails: datamodel.BucketDetailsArray{
			&datamodel.BucketDetails{BucketName: "test-bucket", VendorSubnetID: "subnet-12345"},
		},
	}
	backup := &datamodel.Backup{
		Attributes: &datamodel.BackupAttributes{BucketName: "non-existent-bucket", SnapshotID: "snapshot-uuid"},
	}
	_, err := getSmSourcePathForRestore(backupVault, backup)
	assert.Error(t, err)
}
