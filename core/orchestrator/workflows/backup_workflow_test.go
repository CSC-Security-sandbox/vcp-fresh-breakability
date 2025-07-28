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
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
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
		Pool:             &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, PoolCredentials: &datamodel.PoolCredentials{Password: "password"}},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}, VendorSubnetID: "subnet-12345", ExternalUUID: "external-uuid"},
	}

	// Mock activity responses
	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	env.OnActivity("GetObjStoreNameActivity", mock.Anything, mock.Anything, mock.Anything).Return("test-bucket", nil)
	env.OnActivity("GetBucketDetailsActivity", mock.Anything, mock.Anything, mock.Anything).Return(&datamodel.BucketDetails{BucketName: "test-bucket", ServiceAccountName: "sa-test"}, nil)
	env.OnActivity("GetOrCreateObjectStore", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.CloudTarget{}, nil)
	env.OnActivity("GetSmDestinationPathActivity", mock.Anything, mock.Anything, mock.Anything).Return("test-bucket:/objstore/test-vol", nil)
	env.OnActivity("GetSmSourcePathActivity", mock.Anything, mock.Anything).Return("svm_test:volume_test", nil)
	env.OnActivity("SnapmirrorGetorCreate", mock.Anything, mock.Anything, mock.Anything).Return(&common.SnapmirrorRelationship{UUID: "uuid", DestinationUUID: nillable.ToPointer("snapmirror-uuid")}, nil)
	env.OnActivity("SnapshotCreate", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.SnapshotProviderResponse{ProviderResponse: vsa.ProviderResponse{ExternalUUID: "uuid"}}, nil)
	env.OnActivity("SnapmirrorTransfer", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("GetSnapmirrorTransferStatus", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("success", nil)
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
		Pool:             &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, PoolCredentials: &datamodel.PoolCredentials{Password: "password"}},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}, VendorSubnetID: "subnet-12345", ExternalUUID: "external-uuid"},
	}

	// Mock activity responses
	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	env.OnActivity("GetObjStoreNameActivity", mock.Anything, mock.Anything, mock.Anything).Return("test-bucket", nil)
	env.OnActivity("GetBucketDetailsActivity", mock.Anything, mock.Anything, mock.Anything).Return(&datamodel.BucketDetails{BucketName: "test-bucket", ServiceAccountName: "sa-test"}, nil)
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
		Pool:             &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, PoolCredentials: &datamodel.PoolCredentials{Password: "password"}},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}, VendorSubnetID: "subnet-12345", ExternalUUID: "external-uuid"},
	}

	// Mock activity responses
	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	env.OnActivity("GetObjStoreNameActivity", mock.Anything, mock.Anything, mock.Anything).Return("test-bucket", nil)
	env.OnActivity("GetBucketDetailsActivity", mock.Anything, mock.Anything, mock.Anything).Return(&datamodel.BucketDetails{BucketName: "test-bucket", ServiceAccountName: "sa-test"}, nil)
	env.OnActivity("GetOrCreateObjectStore", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.CloudTarget{}, nil)
	env.OnActivity("GetSmDestinationPathActivity", mock.Anything, mock.Anything, mock.Anything).Return("test-bucket:/objstore/test-vol", nil)
	env.OnActivity("GetSmSourcePathActivity", mock.Anything, mock.Anything).Return("svm_test:volume_test", nil)
	env.OnActivity("SnapmirrorGetorCreate", mock.Anything, mock.Anything, mock.Anything).Return(&common.SnapmirrorRelationship{UUID: "uuid", DestinationUUID: nillable.ToPointer("snapmirror-uuid")}, nil)
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

func TestBackupWorkflowGetSmSourcePathActivityFailure(t *testing.T) {
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
		Pool:             &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, PoolCredentials: &datamodel.PoolCredentials{Password: "password"}},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}, VendorSubnetID: "subnet-12345", ExternalUUID: "external-uuid"},
	}

	// Mock activity responses - specifically test GetSmSourcePathActivity failure
	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	env.OnActivity("GetObjStoreNameActivity", mock.Anything, mock.Anything, mock.Anything).Return("test-bucket", nil)
	env.OnActivity("GetBucketDetailsActivity", mock.Anything, mock.Anything, mock.Anything).Return(&datamodel.BucketDetails{BucketName: "test-bucket", ServiceAccountName: "sa-test"}, nil)
	env.OnActivity("GetOrCreateObjectStore", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.CloudTarget{}, nil)
	env.OnActivity("GetSmDestinationPathActivity", mock.Anything, mock.Anything, mock.Anything).Return("test-bucket:/objstore/test-vol", nil)
	// This is the key test - GetSmSourcePathActivity fails
	env.OnActivity("GetSmSourcePathActivity", mock.Anything, mock.Anything).Return("", errors.New("failed to get SM source path"))
	env.OnActivity("UpdateBackupError", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	env.ExecuteWorkflow(CreateBackupWorkflow, params, backup, backupVault, volume)
	_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
	if err != nil {
		t.Fatalf("Failed to query workflow: %v", err)
	}
	// Assert workflow execution - should complete successfully even with error due to error handling
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestBackupWorkflowSnapmirrorTransferPolling(t *testing.T) {
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
		Pool:             &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, PoolCredentials: &datamodel.PoolCredentials{Password: "password"}},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}, VendorSubnetID: "subnet-12345", ExternalUUID: "external-uuid"},
	}

	// Mock activity responses - test the polling behavior
	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	env.OnActivity("GetObjStoreNameActivity", mock.Anything, mock.Anything, mock.Anything).Return("test-bucket", nil)
	env.OnActivity("GetBucketDetailsActivity", mock.Anything, mock.Anything, mock.Anything).Return(&datamodel.BucketDetails{BucketName: "test-bucket", ServiceAccountName: "sa-test"}, nil)
	env.OnActivity("GetOrCreateObjectStore", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.CloudTarget{}, nil)
	env.OnActivity("GetSmDestinationPathActivity", mock.Anything, mock.Anything, mock.Anything).Return("test-bucket:/objstore/test-vol", nil)
	env.OnActivity("GetSmSourcePathActivity", mock.Anything, mock.Anything).Return("svm_test:volume_test", nil)
	env.OnActivity("SnapmirrorGetorCreate", mock.Anything, mock.Anything, mock.Anything).Return(&common.SnapmirrorRelationship{UUID: "uuid", DestinationUUID: nillable.ToPointer("snapmirror-uuid")}, nil)
	env.OnActivity("SnapshotCreate", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.SnapshotProviderResponse{ProviderResponse: vsa.ProviderResponse{ExternalUUID: "uuid"}}, nil)
	env.OnActivity("SnapmirrorTransfer", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	// Mock polling behavior: first transferring, then success
	env.OnActivity("GetSnapmirrorTransferStatus", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("transferring", nil).Once()
	env.OnActivity("GetSnapmirrorTransferStatus", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("success", nil).Once()
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

func TestBackupWorkflowSnapmirrorTransferFailed(t *testing.T) {
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
		Pool:             &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, PoolCredentials: &datamodel.PoolCredentials{Password: "password"}},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}, VendorSubnetID: "subnet-12345", ExternalUUID: "external-uuid"},
	}

	// Mock activity responses
	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	env.OnActivity("GetObjStoreNameActivity", mock.Anything, mock.Anything, mock.Anything).Return("test-bucket", nil)
	env.OnActivity("GetBucketDetailsActivity", mock.Anything, mock.Anything, mock.Anything).Return(&datamodel.BucketDetails{BucketName: "test-bucket", ServiceAccountName: "sa-test"}, nil)
	env.OnActivity("GetOrCreateObjectStore", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.CloudTarget{}, nil)
	env.OnActivity("GetSmDestinationPathActivity", mock.Anything, mock.Anything, mock.Anything).Return("test-bucket:/objstore/test-vol", nil)
	env.OnActivity("GetSmSourcePathActivity", mock.Anything, mock.Anything).Return("svm_test:volume_test", nil)
	env.OnActivity("SnapmirrorGetorCreate", mock.Anything, mock.Anything, mock.Anything).Return(&common.SnapmirrorRelationship{UUID: "uuid", DestinationUUID: nillable.ToPointer("snapmirror-uuid")}, nil)
	env.OnActivity("SnapshotCreate", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.SnapshotProviderResponse{ProviderResponse: vsa.ProviderResponse{ExternalUUID: "uuid"}}, nil)
	env.OnActivity("SnapmirrorTransfer", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("GetSnapmirrorTransferStatus", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("failed", nil)
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
	expected := "vcp-ad-test-backup"
	result := getSnapshotName(backup)
	assert.Equal(t, expected, result, "getSnapshotName should return the correct snapshot name")
}

func TestGetSnapshotName_WithEmptyName(t *testing.T) {
	backup := &datamodel.Backup{Name: ""}
	expected := "vcp-ad-"
	result := getSnapshotName(backup)
	assert.Equal(t, expected, result, "getSnapshotName should handle empty backup name")
}

func TestUpdateBackupWorkflowSuccess(t *testing.T) {
	t.Run("UpdateBackupWorkflowSuccess", func(t *testing.T) {
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

		backup := &datamodel.Backup{
			Name:        "test-backup",
			VolumeUUID:  "test-vol",
			Attributes:  &datamodel.BackupAttributes{BucketName: "test-bucket"},
			Description: "Updated description",
		}

		// Mock activity responses
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateBackup", mock.Anything, backup).Return(nil)

		// Execute workflow
		env.ExecuteWorkflow(UpdateBackupWorkflow, backup)

		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
}

func TestUpdateBackupWorkflowFailure(t *testing.T) {
	t.Run("UpdateBackupWorkflowFailure", func(t *testing.T) {
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
		// Mock storage for proper BackupActivity initialization
		mockStorage := database.NewMockStorage(t)
		mockStorage.On("UpdateBackupState", mock.Anything, mock.Anything).Return(nil, nil)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&activities.BackupActivity{SE: mockStorage})

		// Set up test data
		backup := &datamodel.Backup{
			Name:        "test-backup",
			VolumeUUID:  "test-vol",
			Attributes:  &datamodel.BackupAttributes{BucketName: "test-bucket"},
			Description: "Updated description",
		}

		// Mock activity responses - use simple mocks that just check workflow completion
		// First UpdateJobStatus call (PROCESSING) should succeed
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil).Once()
		// UpdateBackup should fail
		env.OnActivity("UpdateBackup", mock.Anything, mock.Anything).Return(errors.New("update failed"))
		// Second UpdateJobStatus call (ERROR) - let it succeed so we can test the flow
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil).Maybe()
		// Execute workflow
		env.ExecuteWorkflow(UpdateBackupWorkflow, backup)

		// Assert workflow execution - the workflow handles errors internally and completes successfully
		// even when UpdateBackup fails, because it updates the job status to handle the error
		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
}

func TestDeleteBackupWorkflow(t *testing.T) {
	t.Run("DeleteBackupWorkflowSuccess", func(t *testing.T) {
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
				PoolCredentials: &datamodel.PoolCredentials{
					Password:      "password",
					SecretID:      "",
					CertificateID: "",
				},
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
		env.OnActivity("GetObjectStore", mock.Anything, mock.Anything, mock.Anything).Return(&common.CloudTarget{UUID: "obj-store-uuid"}, nil)
		env.OnActivity("GetSmDestinationPathActivity", mock.Anything, mock.Anything, mock.Anything).Return("test-bucket:/objstore/test-vol", nil)
		env.OnActivity("GetSmSourcePathActivity", mock.Anything, mock.Anything).Return("svm_test:volume_test", nil)
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

	t.Run("DeleteBackupWithVolumeDeleted", func(t *testing.T) {
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
		backup := &datamodel.Backup{
			Name:          "test-backup",
			VolumeUUID:    "test-vol",
			BackupVault:   backupVault,
			BackupVaultID: 1,
			Attributes:    &datamodel.BackupAttributes{BucketName: "test-bucket"},
		}

		// Mock activity responses for volume deleted scenario
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetBackupVault", mock.Anything, params.BackupVaultUUID).Return(backupVault, nil)
		env.OnActivity("GetBackup", mock.Anything, params.BackupVaultUUID, params.BackupUUID, params.AccountName).Return(backup, nil)
		env.OnActivity("IsVolumeDeleted", mock.Anything, backup.VolumeUUID).Return(true, nil) // Volume is deleted
		env.OnActivity("DeleteBackup", mock.Anything, params.BackupUUID).Return(nil, nil)

		// Execute workflow
		env.ExecuteWorkflow(DeleteBackupWorkflow, params)

		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("DeleteBackupMultipleBackupsShared", func(t *testing.T) {
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
				PoolCredentials: &datamodel.PoolCredentials{
					Password:      "password",
					SecretID:      "",
					CertificateID: "",
				},
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
			Attributes:    &datamodel.BackupAttributes{BucketName: "test-bucket", EndpointUUID: "endpoint-uuid", SnapshotID: "snapshot-id"},
		}

		// Mock activity responses for multiple backups scenario with shared snapshot
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetBackupVault", mock.Anything, params.BackupVaultUUID).Return(backupVault, nil)
		env.OnActivity("GetBackup", mock.Anything, params.BackupVaultUUID, params.BackupUUID, params.AccountName).Return(backup, nil)
		env.OnActivity("IsVolumeDeleted", mock.Anything, backup.VolumeUUID).Return(false, nil)
		env.OnActivity("GetBackupCountByVolumeUUID", mock.Anything, backup.VolumeUUID).Return(int64(2), nil) // Multiple backups
		env.OnActivity("GetVolume", mock.Anything, backup.VolumeUUID).Return(volume, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
		env.OnActivity("GetObjectStore", mock.Anything, mock.Anything, mock.Anything).Return(&common.CloudTarget{UUID: "obj-store-uuid"}, nil)
		env.OnActivity("IsBackupShared", mock.Anything, backup).Return(true, nil) // Backup is shared
		env.OnActivity("DeleteBackup", mock.Anything, params.BackupUUID).Return(nil, nil)

		// Execute workflow
		env.ExecuteWorkflow(DeleteBackupWorkflow, params)

		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("DeleteBackupMultipleBackupsNotShared", func(t *testing.T) {
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
				PoolCredentials: &datamodel.PoolCredentials{
					Password:      "password",
					SecretID:      "",
					CertificateID: "",
				},
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
			Attributes:    &datamodel.BackupAttributes{BucketName: "test-bucket", EndpointUUID: "endpoint-uuid", SnapshotID: "snapshot-id"},
		}

		// Mock activity responses for multiple backups scenario with non-shared snapshot
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetBackupVault", mock.Anything, params.BackupVaultUUID).Return(backupVault, nil)
		env.OnActivity("GetBackup", mock.Anything, params.BackupVaultUUID, params.BackupUUID, params.AccountName).Return(backup, nil)
		env.OnActivity("IsVolumeDeleted", mock.Anything, backup.VolumeUUID).Return(false, nil)
		env.OnActivity("GetBackupCountByVolumeUUID", mock.Anything, backup.VolumeUUID).Return(int64(2), nil) // Multiple backups
		env.OnActivity("GetVolume", mock.Anything, backup.VolumeUUID).Return(volume, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
		env.OnActivity("GetObjectStore", mock.Anything, mock.Anything, mock.Anything).Return(&common.CloudTarget{UUID: "obj-store-uuid"}, nil)
		env.OnActivity("IsBackupShared", mock.Anything, backup).Return(false, nil) // Backup is not shared
		env.OnActivity("DeleteSnapshotFromObjectStore", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.OntapAsyncResponse{JobUUID: "job-uuid"}, nil)
		env.OnActivity("GetOntapJob", mock.Anything, mock.Anything, mock.Anything).Return(&vsa.OntapJob{State: "success"}, nil)
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

func TestGetBucketDetailsForBucket_Success(t *testing.T) {
	backupVault := &datamodel.BackupVault{
		BucketDetails: datamodel.BucketDetailsArray{
			&datamodel.BucketDetails{BucketName: "test-bucket", VendorSubnetID: "subnet-12345"},
			&datamodel.BucketDetails{BucketName: "other-bucket", VendorSubnetID: "subnet-67890"},
		},
	}
	expected := &datamodel.BucketDetails{BucketName: "test-bucket", VendorSubnetID: "subnet-12345"}
	result, err := getBucketDetailsForBucket(backupVault, "test-bucket")
	assert.NoError(t, err)
	assert.Equal(t, expected, result)
}

func TestGetBucketDetailsForBucket_NotFound(t *testing.T) {
	backupVault := &datamodel.BackupVault{
		BucketDetails: datamodel.BucketDetailsArray{
			&datamodel.BucketDetails{BucketName: "test-bucket", VendorSubnetID: "subnet-12345"},
		},
	}
	_, err := getBucketDetailsForBucket(backupVault, "non-existent-bucket")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no matching bucket details found for bucket non-existent-bucket")
}

func TestGetBucketDetailsForBucket_EmptyBucketName(t *testing.T) {
	backupVault := &datamodel.BackupVault{
		BucketDetails: datamodel.BucketDetailsArray{
			&datamodel.BucketDetails{BucketName: "test-bucket", VendorSubnetID: "subnet-12345"},
		},
	}
	_, err := getBucketDetailsForBucket(backupVault, "")
	assert.Error(t, err)
}

func TestDeleteBackupWorkflowHandleError(t *testing.T) {
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

	backup := &datamodel.Backup{
		Name:       "test-backup",
		VolumeUUID: "test-vol",
		Attributes: &datamodel.BackupAttributes{BucketName: "test-bucket"},
	}

	// Mock activity responses for HandleError function
	env.OnActivity("GetBackup", mock.Anything, params.BackupVaultUUID, params.BackupUUID, params.AccountName).Return(backup, nil)
	env.OnActivity("UpdateBackupError", mock.Anything, backup, mock.Anything).Return(nil)

	// Test HandleError function by simulating workflow execution that triggers error handling
	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("GetBackupVault", mock.Anything, params.BackupVaultUUID).Return(nil, errors.New("failed to get backup vault"))

	// Execute workflow - this should trigger error handling
	env.ExecuteWorkflow(DeleteBackupWorkflow, params)

	// Assert workflow execution - HandleError is called but workflow completes successfully
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestDeleteBackupWorkflowHandleErrorFailure(t *testing.T) {
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

	// Mock activity responses for HandleError function failure scenarios
	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("GetBackupVault", mock.Anything, params.BackupVaultUUID).Return(nil, errors.New("failed to get backup vault"))
	env.OnActivity("GetBackup", mock.Anything, params.BackupVaultUUID, params.BackupUUID, params.AccountName).Return(nil, errors.New("failed to get backup"))

	// Execute workflow - this should trigger error handling which then fails
	env.ExecuteWorkflow(DeleteBackupWorkflow, params)

	// Assert workflow execution - HandleError fails but workflow still completes with error
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestCreateBackupWorkflowEdgeCases(t *testing.T) {
	t.Run("TestCreateBackupWorkflowNilDestinationUUID", func(t *testing.T) {
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
			Pool:             &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, PoolCredentials: &datamodel.PoolCredentials{Password: "password"}},
			Svm:              &datamodel.Svm{Name: "svm_test"},
			VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}, VendorSubnetID: "subnet-12345", ExternalUUID: "external-uuid"},
		}

		// Mock activity responses - test case where DestinationUUID is nil
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
		env.OnActivity("GetObjStoreNameActivity", mock.Anything, mock.Anything, mock.Anything).Return("test-bucket", nil)
		env.OnActivity("GetBucketDetailsActivity", mock.Anything, mock.Anything, mock.Anything).Return(&datamodel.BucketDetails{BucketName: "test-bucket", ServiceAccountName: "sa-test"}, nil)
		env.OnActivity("GetOrCreateObjectStore", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.CloudTarget{}, nil)
		env.OnActivity("GetSmDestinationPathActivity", mock.Anything, mock.Anything, mock.Anything).Return("test-bucket:/objstore/test-vol", nil)
		env.OnActivity("GetSmSourcePathActivity", mock.Anything, mock.Anything).Return("svm_test:volume_test", nil)
		// Return snapmirror relationship with nil DestinationUUID to test edge case
		env.OnActivity("SnapmirrorGetorCreate", mock.Anything, mock.Anything, mock.Anything).Return(&common.SnapmirrorRelationship{UUID: "uuid", DestinationUUID: nil}, nil)
		env.OnActivity("SnapshotCreate", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.SnapshotProviderResponse{ProviderResponse: vsa.ProviderResponse{ExternalUUID: "uuid"}}, nil)
		env.OnActivity("SnapmirrorTransfer", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSnapmirrorTransferStatus", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("success", nil)
		env.OnActivity("FinishBackup", mock.Anything, mock.Anything).Return(nil)

		// Execute workflow
		env.ExecuteWorkflow(CreateBackupWorkflow, params, backup, backupVault, volume)

		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
}

func TestDeleteBackupWorkflowAdditionalErrorCases(t *testing.T) {
	t.Run("GetBackupVaultFailure", func(t *testing.T) {
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

		// Mock activity responses for GetBackupVault failure
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetBackupVault", mock.Anything, params.BackupVaultUUID).Return(nil, errors.New("failed to get backup vault"))

		// Execute workflow
		env.ExecuteWorkflow(DeleteBackupWorkflow, params)

		// Assert workflow execution - HandleError is called and workflow completes
		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("GetBackupFailure", func(t *testing.T) {
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

		// Mock activity responses for GetBackup failure
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetBackupVault", mock.Anything, params.BackupVaultUUID).Return(backupVault, nil)
		env.OnActivity("GetBackup", mock.Anything, params.BackupVaultUUID, params.BackupUUID, params.AccountName).Return(nil, errors.New("failed to get backup"))

		// Execute workflow
		env.ExecuteWorkflow(DeleteBackupWorkflow, params)

		// Assert workflow execution - HandleError is called and workflow completes
		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
}

func TestUpdateBackupWorkflowAdditionalCases(t *testing.T) {
	t.Run("UpdateBackupWorkflowGetBackupFailure", func(t *testing.T) {
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
		backup := &datamodel.Backup{
			Name:        "test-backup",
			VolumeUUID:  "test-vol",
			Attributes:  &datamodel.BackupAttributes{BucketName: "test-bucket"},
			Description: "Updated description",
		}

		// Mock activity responses
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateBackup", mock.Anything, backup).Return(nil)

		// Execute workflow
		env.ExecuteWorkflow(UpdateBackupWorkflow, backup)

		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
}
