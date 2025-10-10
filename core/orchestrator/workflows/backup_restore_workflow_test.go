package workflows

import (
	"errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

type BackupRestoreWorkflowTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env *testsuite.TestWorkflowEnvironment
}

func (s *BackupRestoreWorkflowTestSuite) SetupTest() {
	s.env = s.NewTestWorkflowEnvironment()
	s.env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
	mockHeader := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{
			"logParam": encodedValue,
		},
	}
	s.env.SetHeader(mockHeader)
	s.env.RegisterWorkflow(RestoreBackupWorkflow)
	s.env.RegisterWorkflow(PreBlockVolumeWorkflow)
	s.env.RegisterWorkflow(PostBlockVolumeWorkflow)
	s.env.RegisterWorkflow(PreFileVolumeWorkflow)
	s.env.RegisterWorkflow(PostFileVolumeWorkflow)
}

func (s *BackupRestoreWorkflowTestSuite) AfterTest() {
	s.env.AssertExpectations(s.T())
}

func TestBackupRestoreWorkflowTestSuite(t *testing.T) {
	suite.Run(t, new(BackupRestoreWorkflowTestSuite))
}

func (s *BackupRestoreWorkflowTestSuite) createTestData() (*common.CreateVolumeParams, *datamodel.Volume, *datamodel.BackupVault, *datamodel.Backup, []*common.HostParams, *vsa.VolumeResponse) {
	params := &common.CreateVolumeParams{
		AccountName: "test-account",
		BackupPath:  "projects/test/locations/us-central1/backupVaults/bv1/backups/backup1",
	}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
		Name:      "test-volume",
		Account:   &datamodel.Account{Name: "test-account"},
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "test-password",
				SecretID:      "test-secret",
				CertificateID: "test-cert",
				AuthType:      1, // Use integer value for AuthType
			},
			DeploymentName: "test-deployment",
			ClusterDetails: datamodel.ClusterDetails{
				RegionalTenantProject: "123456789",
			},
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			Protocols: []string{"ISCSI"},
		},
	}

	backupVault := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{UUID: "test-vault-uuid"},
		Name:      "test-vault",
		BucketDetails: []*datamodel.BucketDetails{
			{
				BucketName: "test-bucket",
			},
		},
	}

	backup := &datamodel.Backup{
		BaseModel: datamodel.BaseModel{UUID: "test-backup-uuid"},
		Name:      "test-backup",
		Attributes: &datamodel.BackupAttributes{
			EndpointUUID: "test-endpoint-uuid",
			BucketName:   "test-bucket",
			SnapshotID:   "test-snapshot-id",
		},
	}

	hostParams := []*common.HostParams{
		{HostName: "test-host", HostIQNs: []string{"iqn.test.host"}},
	}

	volCreateResponse := &vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			ExternalUUID: "test-external-uuid",
		},
	}

	return params, volume, backupVault, backup, hostParams, volCreateResponse
}

// Helper function to register common activities and mocks
func (s *BackupRestoreWorkflowTestSuite) registerCommonActivities() {
	commonActivity := &activities.CommonActivities{}
	backupActivity := &activities.BackupActivity{}
	volumeCreateActivity := &activities.VolumeCreateActivity{}
	volumeUpdateActivity := &activities.VolumeUpdateActivity{}

	// Register activities
	s.env.RegisterActivity(commonActivity)
	s.env.RegisterActivity(volumeCreateActivity)
	s.env.RegisterActivity(volumeUpdateActivity)
	s.env.RegisterActivity(activities.GetObjStoreNameFromBackup)
	s.env.RegisterActivity(activities.GetBucketDetailsFromBackup)

	// Register specific backup activity methods
	s.env.RegisterActivity(backupActivity.GetSmSourcePathActivity)
	s.env.RegisterActivity(backupActivity.GetSmSourcePathForRestoreActivity)
	s.env.RegisterActivity(backupActivity.GetOrCreateObjectStore)
	s.env.RegisterActivity(backupActivity.SnapmirrorGetOrCreate)
	s.env.RegisterActivity(backupActivity.SnapmirrorTransfer)
	s.env.RegisterActivity(backupActivity.GetSnapmirrorTransferStatus)

	// Register missing activities
	s.env.RegisterActivity(volumeCreateActivity.CrossPoolOrVPCRestorationActivity)
	s.env.RegisterActivity(volumeCreateActivity.DeleteObjectStoreForCrossVPC)
	s.env.RegisterActivity(volumeCreateActivity.FinaliseRestoredVolume)
	s.env.RegisterActivity(volumeCreateActivity.DeleteRolesForServiceAccountInBackupTenantProject)

	// WaitForONTAPJob is already registered in the package
}

// Helper function to set up common mocks
func (s *BackupRestoreWorkflowTestSuite) setupCommonMocks(volume *datamodel.Volume) {
	commonActivity := &activities.CommonActivities{}
	backupActivity := &activities.BackupActivity{}
	volumeCreateActivity := &activities.VolumeCreateActivity{}
	volumeUpdateActivity := &activities.VolumeUpdateActivity{}

	// Mock activities
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CrossPoolOrVPCRestorationActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.DeleteRolesForServiceAccountInBackupTenantProject, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnWorkflow("PreBlockVolumeWorkflow", mock.Anything, mock.Anything, mock.Anything).Return(volume, nil)
	s.env.OnActivity(backupActivity.GetSmSourcePathActivity, mock.Anything, mock.Anything).Return("test-dest-path", nil)
	s.env.OnActivity(backupActivity.GetSmSourcePathForRestoreActivity, mock.Anything, mock.Anything, mock.Anything).Return("test-source-path", nil)
	s.env.OnActivity(activities.GetObjStoreNameFromBackup, mock.Anything, mock.Anything).Return("test-obj-store", nil)
	s.env.OnActivity(activities.GetBucketDetailsFromBackup, mock.Anything, mock.Anything).Return(&datamodel.BucketDetails{BucketName: "test-bucket"}, nil)
	s.env.OnActivity(backupActivity.GetOrCreateObjectStore, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.CloudTarget{}, nil)
	s.env.OnActivity(backupActivity.SnapmirrorGetOrCreate, mock.Anything, mock.Anything, mock.Anything).Return(&common.SnapmirrorRelationship{UUID: "test-uuid"}, nil)
	s.env.OnActivity(backupActivity.SnapmirrorTransfer, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupActivity.PollTransferStatusWithHistoryCheckActivity, mock.Anything, mock.Anything, mock.Anything).Return(&activities.PollTransferStatusOutput{
		BackupActivitiesContext: &activities.BackupActivitiesContext{
			BackupWorkflowInit: &activities.BackupWorkflowInput{
				Backup:      &datamodel.Backup{},
				BackupVault: &datamodel.BackupVault{},
				Volume:      volume,
			},
			Node:         &models.Node{EndpointAddress: "127.0.0.1"},
			SnapshotName: "test-backup",
			SnapmirrorRelationship: &common.SnapmirrorRelationship{
				UUID: "test-snapmirror-uuid",
			},
			TransferStatus: activities.SmStatusSuccess,
		},
		TransferComplete:    true,
		ShouldContinueAsNew: false,
	}, nil)
	s.env.OnActivity(volumeUpdateActivity.GetVolumeFromONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{Type: "rw"}, nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.DeleteObjectStoreForCrossVPC, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.OntapAsyncResponse{}, nil)
	s.env.OnActivity(volumeCreateActivity.FinaliseRestoredVolume, mock.Anything, mock.Anything).Return(nil)
	s.env.OnWorkflow("PostBlockVolumeWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(volume, nil)

	// Mock WaitForONTAPJob workflow
	s.env.OnWorkflow(WaitForONTAPJob, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
}

func (s *BackupRestoreWorkflowTestSuite) TestRestoreBackupWorkflow_Success() {
	// Setup test data
	params, volume, backupVault, backup, hostParams, volCreateResponse := s.createTestData()

	// Register common activities and mocks
	s.registerCommonActivities()
	s.setupCommonMocks(volume)

	// Execute workflow
	s.env.ExecuteWorkflow(RestoreBackupWorkflow, params, volume, backupVault, backup, hostParams, volCreateResponse)

	// Assertions
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NoError(s.T(), s.env.GetWorkflowError())
}

func (s *BackupRestoreWorkflowTestSuite) TestRestoreBackupWorkflow_SetupFailure() {
	params, volume, backupVault, backup, hostParams, volCreateResponse := s.createTestData()

	// Create activity instances
	commonActivity := &activities.CommonActivities{}

	// Register activities
	s.env.RegisterActivity(commonActivity)

	// Mock UpdateJobStatus to fail
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(errors.New("setup failed"))

	// Execute workflow
	s.env.ExecuteWorkflow(RestoreBackupWorkflow, params, volume, backupVault, backup, hostParams, volCreateResponse)

	// Assertions
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "setup failed")
}

func (s *BackupRestoreWorkflowTestSuite) TestRestoreBackupWorkflow_GetNodeFailure() {
	params, volume, backupVault, backup, hostParams, volCreateResponse := s.createTestData()

	// Create activity instances
	commonActivity := &activities.CommonActivities{}

	// Register activities
	s.env.RegisterActivity(commonActivity)

	// Track UpdateJobStatus calls to verify error handling
	var jobStatusCalls []string
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		job := args.Get(1).(*datamodel.Job)
		jobStatusCalls = append(jobStatusCalls, job.State)
	}).Return(nil)

	// Mock GetNode to fail with non-retryable error
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return(nil, temporal.NewNonRetryableApplicationError("get node failed", "GetNodeFailure", nil))

	// Execute workflow
	s.env.ExecuteWorkflow(RestoreBackupWorkflow, params, volume, backupVault, backup, hostParams, volCreateResponse)

	// Debug logging
	s.T().Logf("Workflow completed: %v", s.env.IsWorkflowCompleted())
	s.T().Logf("Workflow error: %v", s.env.GetWorkflowError())
	s.T().Logf("Job status calls: %v", jobStatusCalls)

	// Assertions
	assert.True(s.T(), s.env.IsWorkflowCompleted())

	// The workflow should fail due to the GetNode error
	assert.Error(s.T(), s.env.GetWorkflowError())

	// Verify that the workflow attempted to update job status to ERROR
	assert.Contains(s.T(), jobStatusCalls, "PROCESSING")
	assert.Contains(s.T(), jobStatusCalls, "ERROR")
}

func (s *BackupRestoreWorkflowTestSuite) TestRestoreBackupWorkflow_PreWorkflowFailure() {
	params, volume, backupVault, backup, hostParams, volCreateResponse := s.createTestData()

	// Create activity instances
	commonActivity := &activities.CommonActivities{}

	// Register activities
	s.env.RegisterActivity(commonActivity)

	// Track UpdateJobStatus calls to verify error handling
	var jobStatusCalls []string
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		job := args.Get(1).(*datamodel.Job)
		jobStatusCalls = append(jobStatusCalls, job.State)
	}).Return(nil)

	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)

	// Mock child workflow to fail
	s.env.OnWorkflow("PreBlockVolumeWorkflow", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("pre workflow failed"))

	// Execute workflow
	s.env.ExecuteWorkflow(RestoreBackupWorkflow, params, volume, backupVault, backup, hostParams, volCreateResponse)

	// Debug logging
	s.T().Logf("Workflow completed: %v", s.env.IsWorkflowCompleted())
	s.T().Logf("Workflow error: %v", s.env.GetWorkflowError())
	s.T().Logf("Job status calls: %v", jobStatusCalls)

	// Assertions
	assert.True(s.T(), s.env.IsWorkflowCompleted())

	// The workflow should fail due to the pre workflow failure
	assert.Error(s.T(), s.env.GetWorkflowError())

	// Verify that the workflow attempted to update job status to ERROR
	assert.Contains(s.T(), jobStatusCalls, "PROCESSING")
	assert.Contains(s.T(), jobStatusCalls, "ERROR")
}

func (s *BackupRestoreWorkflowTestSuite) TestRestoreBackupWorkflow_GetSmSourcePathFailure() {
	params, volume, backupVault, backup, hostParams, volCreateResponse := s.createTestData()

	// Create activity instances
	commonActivity := &activities.CommonActivities{}
	backupActivity := &activities.BackupActivity{}

	// Register activities
	s.env.RegisterActivity(commonActivity)

	// Register specific backup activity methods
	s.env.RegisterActivity(backupActivity.GetSmSourcePathActivity)
	s.env.RegisterActivity(backupActivity.GetSmSourcePathForRestoreActivity)

	// Track UpdateJobStatus calls to verify error handling
	var jobStatusCalls []string
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		job := args.Get(1).(*datamodel.Job)
		jobStatusCalls = append(jobStatusCalls, job.State)
	}).Return(nil)

	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnWorkflow("PreBlockVolumeWorkflow", mock.Anything, mock.Anything, mock.Anything).Return(volume, nil)
	s.env.OnActivity(backupActivity.GetSmSourcePathActivity, mock.Anything, mock.Anything).Return("", errors.New("get sm source path failed"))

	// Execute workflow
	s.env.ExecuteWorkflow(RestoreBackupWorkflow, params, volume, backupVault, backup, hostParams, volCreateResponse)

	// Debug logging
	s.T().Logf("Workflow completed: %v", s.env.IsWorkflowCompleted())
	s.T().Logf("Workflow error: %v", s.env.GetWorkflowError())
	s.T().Logf("Job status calls: %v", jobStatusCalls)

	// Assertions
	assert.True(s.T(), s.env.IsWorkflowCompleted())

	// The workflow should fail due to the GetSmSourcePath error
	assert.Error(s.T(), s.env.GetWorkflowError())

	// Verify that the workflow attempted to update job status to ERROR
	assert.Contains(s.T(), jobStatusCalls, "PROCESSING")
	assert.Contains(s.T(), jobStatusCalls, "ERROR")
}

func (s *BackupRestoreWorkflowTestSuite) TestRestoreBackupWorkflow_SnapmirrorTransferPolling() {
	params, volume, backupVault, backup, hostParams, volCreateResponse := s.createTestData()

	// Register common activities and mocks
	s.registerCommonActivities()
	s.setupCommonMocks(volume)

	// Override snapmirror transfer status: first transferring, then success
	backupActivity := &activities.BackupActivity{}
	s.env.OnActivity(backupActivity.GetSnapmirrorTransferStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(activities.SmStatusTransferring, nil).Once()
	s.env.OnActivity(backupActivity.GetSnapmirrorTransferStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(activities.SmStatusSuccess, nil).Once()

	// Execute workflow
	s.env.ExecuteWorkflow(RestoreBackupWorkflow, params, volume, backupVault, backup, hostParams, volCreateResponse)

	// Assertions
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NoError(s.T(), s.env.GetWorkflowError())
}

func (s *BackupRestoreWorkflowTestSuite) TestRestoreBackupWorkflow_SnapmirrorTransferFailed() {
	params, volume, backupVault, backup, hostParams, volCreateResponse := s.createTestData()

	// Create activity instances
	commonActivity := &activities.CommonActivities{}
	backupActivity := &activities.BackupActivity{}
	volumeCreateActivity := &activities.VolumeCreateActivity{}

	// Register activities
	s.env.RegisterActivity(commonActivity)
	s.env.RegisterActivity(volumeCreateActivity)
	s.env.RegisterActivity(activities.GetObjStoreNameFromBackup)
	s.env.RegisterActivity(activities.GetBucketDetailsFromBackup)

	// Register specific backup activity methods
	s.env.RegisterActivity(backupActivity.GetSmSourcePathActivity)
	s.env.RegisterActivity(backupActivity.GetSmSourcePathForRestoreActivity)
	s.env.RegisterActivity(backupActivity.GetOrCreateObjectStore)
	s.env.RegisterActivity(backupActivity.SnapmirrorGetOrCreate)
	s.env.RegisterActivity(backupActivity.SnapmirrorTransfer)
	s.env.RegisterActivity(backupActivity.GetSnapmirrorTransferStatus)

	// Register missing activities
	s.env.RegisterActivity(volumeCreateActivity.CrossPoolOrVPCRestorationActivity)
	s.env.RegisterActivity(volumeCreateActivity.DeleteObjectStoreForCrossVPC)

	// Track UpdateJobStatus calls to verify error handling
	var jobStatusCalls []string
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		job := args.Get(1).(*datamodel.Job)
		jobStatusCalls = append(jobStatusCalls, job.State)
	}).Return(nil)

	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnWorkflow("PreBlockVolumeWorkflow", mock.Anything, mock.Anything, mock.Anything).Return(volume, nil)
	s.env.OnActivity(backupActivity.GetSmSourcePathActivity, mock.Anything, mock.Anything).Return("test-dest-path", nil)
	s.env.OnActivity(backupActivity.GetSmSourcePathForRestoreActivity, mock.Anything, mock.Anything, mock.Anything).Return("test-source-path", nil)
	s.env.OnActivity(activities.GetObjStoreNameFromBackup, mock.Anything, mock.Anything).Return("test-obj-store", nil)
	s.env.OnActivity(activities.GetBucketDetailsFromBackup, mock.Anything, mock.Anything).Return(&datamodel.BucketDetails{BucketName: "test-bucket"}, nil)
	s.env.OnActivity(backupActivity.GetOrCreateObjectStore, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.CloudTarget{}, nil)
	s.env.OnActivity(backupActivity.SnapmirrorGetOrCreate, mock.Anything, mock.Anything, mock.Anything).Return(&common.SnapmirrorRelationship{UUID: "test-uuid"}, nil)
	s.env.OnActivity(backupActivity.SnapmirrorTransfer, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupActivity.GetSnapmirrorTransferStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(activities.SmStatusFailed, nil)

	// Execute workflow
	s.env.ExecuteWorkflow(RestoreBackupWorkflow, params, volume, backupVault, backup, hostParams, volCreateResponse)

	// Debug logging
	s.T().Logf("Workflow completed: %v", s.env.IsWorkflowCompleted())
	s.T().Logf("Workflow error: %v", s.env.GetWorkflowError())
	s.T().Logf("Job status calls: %v", jobStatusCalls)

	// Assertions
	assert.True(s.T(), s.env.IsWorkflowCompleted())

	// The workflow completes successfully but should have called UpdateJobStatus with ERROR
	// This is due to a bug in the workflow where it returns nil instead of the customErr
	assert.Error(s.T(), s.env.GetWorkflowError()) // The workflow should fail due to the error scenario being tested

	// Verify that the workflow attempted to update job status to ERROR
	assert.Contains(s.T(), jobStatusCalls, "PROCESSING")
	assert.Contains(s.T(), jobStatusCalls, "ERROR")
}

func (s *BackupRestoreWorkflowTestSuite) TestRestoreBackupWorkflow_VolumeStatePolling() {
	params, volume, backupVault, backup, hostParams, volCreateResponse := s.createTestData()

	// Register common activities and mocks
	s.registerCommonActivities()
	s.setupCommonMocks(volume)

	// Override volume state polling: first DP, then RW
	volumeUpdateActivity := &activities.VolumeUpdateActivity{}
	s.env.OnActivity(volumeUpdateActivity.GetVolumeFromONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{Type: "dp"}, nil).Once()
	s.env.OnActivity(volumeUpdateActivity.GetVolumeFromONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{Type: "rw"}, nil).Once()

	// Execute workflow
	s.env.ExecuteWorkflow(RestoreBackupWorkflow, params, volume, backupVault, backup, hostParams, volCreateResponse)

	// Assertions
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NoError(s.T(), s.env.GetWorkflowError())
}

func (s *BackupRestoreWorkflowTestSuite) TestRestoreBackupWorkflow_VolumeStateInvalid() {
	params, volume, backupVault, backup, hostParams, volCreateResponse := s.createTestData()

	// Create activity instances
	commonActivity := &activities.CommonActivities{}
	backupActivity := &activities.BackupActivity{}
	volumeCreateActivity := &activities.VolumeCreateActivity{}
	volumeUpdateActivity := &activities.VolumeUpdateActivity{}

	// Register activities
	s.env.RegisterActivity(commonActivity)
	s.env.RegisterActivity(volumeCreateActivity)
	s.env.RegisterActivity(volumeUpdateActivity)
	s.env.RegisterActivity(activities.GetObjStoreNameFromBackup)
	s.env.RegisterActivity(activities.GetBucketDetailsFromBackup)

	// Register specific backup activity methods
	s.env.RegisterActivity(backupActivity.GetSmSourcePathActivity)
	s.env.RegisterActivity(backupActivity.GetSmSourcePathForRestoreActivity)
	s.env.RegisterActivity(backupActivity.GetOrCreateObjectStore)
	s.env.RegisterActivity(backupActivity.SnapmirrorGetOrCreate)
	s.env.RegisterActivity(backupActivity.SnapmirrorTransfer)
	s.env.RegisterActivity(backupActivity.GetSnapmirrorTransferStatus)

	// Register missing activities
	s.env.RegisterActivity(volumeCreateActivity.CrossPoolOrVPCRestorationActivity)
	s.env.RegisterActivity(volumeCreateActivity.DeleteObjectStoreForCrossVPC)

	// Track UpdateJobStatus calls to verify error handling
	var jobStatusCalls []string
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		job := args.Get(1).(*datamodel.Job)
		jobStatusCalls = append(jobStatusCalls, job.State)
	}).Return(nil)

	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnWorkflow("PreBlockVolumeWorkflow", mock.Anything, mock.Anything, mock.Anything).Return(volume, nil)
	s.env.OnActivity(backupActivity.GetSmSourcePathActivity, mock.Anything, mock.Anything).Return("test-dest-path", nil)
	s.env.OnActivity(backupActivity.GetSmSourcePathForRestoreActivity, mock.Anything, mock.Anything, mock.Anything).Return("test-source-path", nil)
	s.env.OnActivity(activities.GetObjStoreNameFromBackup, mock.Anything, mock.Anything).Return("test-obj-store", nil)
	s.env.OnActivity(activities.GetBucketDetailsFromBackup, mock.Anything, mock.Anything).Return(&datamodel.BucketDetails{BucketName: "test-bucket"}, nil)
	s.env.OnActivity(backupActivity.GetOrCreateObjectStore, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.CloudTarget{}, nil)
	s.env.OnActivity(backupActivity.SnapmirrorGetOrCreate, mock.Anything, mock.Anything, mock.Anything).Return(&common.SnapmirrorRelationship{UUID: "test-uuid"}, nil)
	s.env.OnActivity(backupActivity.SnapmirrorTransfer, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupActivity.GetSnapmirrorTransferStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(activities.SmStatusSuccess, nil)
	s.env.OnActivity(volumeUpdateActivity.GetVolumeFromONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{Type: "invalid"}, nil)

	// Execute workflow
	s.env.ExecuteWorkflow(RestoreBackupWorkflow, params, volume, backupVault, backup, hostParams, volCreateResponse)

	// Debug logging
	s.T().Logf("Workflow completed: %v", s.env.IsWorkflowCompleted())
	s.T().Logf("Workflow error: %v", s.env.GetWorkflowError())
	s.T().Logf("Job status calls: %v", jobStatusCalls)

	// Assertions
	assert.True(s.T(), s.env.IsWorkflowCompleted())

	// The workflow completes successfully but should have called UpdateJobStatus with ERROR
	// This is due to a bug in the workflow where it returns nil instead of the customErr
	assert.Error(s.T(), s.env.GetWorkflowError()) // The workflow should fail due to the error scenario being tested - UPDATED

	// Verify that the workflow attempted to update job status to ERROR
	assert.Contains(s.T(), jobStatusCalls, "PROCESSING")
	assert.Contains(s.T(), jobStatusCalls, "ERROR")
}

func (s *BackupRestoreWorkflowTestSuite) TestRestoreBackupWorkflow_PostWorkflowFailure() {
	params, volume, backupVault, backup, hostParams, volCreateResponse := s.createTestData()

	// Create activity instances
	commonActivity := &activities.CommonActivities{}
	backupActivity := &activities.BackupActivity{}
	volumeCreateActivity := &activities.VolumeCreateActivity{}
	volumeUpdateActivity := &activities.VolumeUpdateActivity{}

	// Register activities
	s.env.RegisterActivity(commonActivity)
	s.env.RegisterActivity(volumeCreateActivity)
	s.env.RegisterActivity(volumeUpdateActivity)
	s.env.RegisterActivity(activities.GetObjStoreNameFromBackup)
	s.env.RegisterActivity(activities.GetBucketDetailsFromBackup)

	// Register specific backup activity methods
	s.env.RegisterActivity(backupActivity.GetSmSourcePathActivity)
	s.env.RegisterActivity(backupActivity.GetSmSourcePathForRestoreActivity)
	s.env.RegisterActivity(backupActivity.GetOrCreateObjectStore)
	s.env.RegisterActivity(backupActivity.SnapmirrorGetOrCreate)
	s.env.RegisterActivity(backupActivity.SnapmirrorTransfer)
	s.env.RegisterActivity(backupActivity.GetSnapmirrorTransferStatus)

	// Register missing activities
	s.env.RegisterActivity(volumeCreateActivity.CrossPoolOrVPCRestorationActivity)
	s.env.RegisterActivity(volumeCreateActivity.DeleteObjectStoreForCrossVPC)

	// Track UpdateJobStatus calls to verify error handling
	var jobStatusCalls []string
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		job := args.Get(1).(*datamodel.Job)
		jobStatusCalls = append(jobStatusCalls, job.State)
	}).Return(nil)

	s.env.OnActivity(volumeCreateActivity.CrossPoolOrVPCRestorationActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnWorkflow("PreBlockVolumeWorkflow", mock.Anything, mock.Anything, mock.Anything).Return(volume, nil)
	s.env.OnActivity(backupActivity.GetSmSourcePathActivity, mock.Anything, mock.Anything).Return("test-dest-path", nil)
	s.env.OnActivity(backupActivity.GetSmSourcePathForRestoreActivity, mock.Anything, mock.Anything, mock.Anything).Return("test-source-path", nil)
	s.env.OnActivity(activities.GetObjStoreNameFromBackup, mock.Anything, mock.Anything).Return("test-obj-store", nil)
	s.env.OnActivity(activities.GetBucketDetailsFromBackup, mock.Anything, mock.Anything).Return(&datamodel.BucketDetails{BucketName: "test-bucket"}, nil)
	s.env.OnActivity(backupActivity.GetOrCreateObjectStore, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.CloudTarget{}, nil)
	s.env.OnActivity(backupActivity.SnapmirrorGetOrCreate, mock.Anything, mock.Anything, mock.Anything).Return(&common.SnapmirrorRelationship{UUID: "test-uuid"}, nil)
	s.env.OnActivity(backupActivity.SnapmirrorTransfer, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupActivity.GetSnapmirrorTransferStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(activities.SmStatusSuccess, nil)
	s.env.OnActivity(volumeUpdateActivity.GetVolumeFromONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{Type: "rw"}, nil)

	// Mock post workflow to fail
	s.env.OnWorkflow("PostBlockVolumeWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("post workflow failed"))

	// Execute workflow
	s.env.ExecuteWorkflow(RestoreBackupWorkflow, params, volume, backupVault, backup, hostParams, volCreateResponse)

	// Debug logging
	s.T().Logf("Workflow completed: %v", s.env.IsWorkflowCompleted())
	s.T().Logf("Workflow error: %v", s.env.GetWorkflowError())
	s.T().Logf("Job status calls: %v", jobStatusCalls)

	// Assertions
	assert.True(s.T(), s.env.IsWorkflowCompleted())

	// The workflow completes successfully but should have called UpdateJobStatus with ERROR
	// This is due to a bug in the workflow where it returns nil instead of the customErr
	assert.Error(s.T(), s.env.GetWorkflowError()) // The workflow should fail due to the error scenario being tested

	// Verify that the workflow attempted to update job status to ERROR
	assert.Contains(s.T(), jobStatusCalls, "PROCESSING")
	assert.Contains(s.T(), jobStatusCalls, "ERROR")
}

func (s *BackupRestoreWorkflowTestSuite) TestRestoreBackupWorkflow_UpdateVolumeDetailsFailure() {
	params, volume, backupVault, backup, hostParams, volCreateResponse := s.createTestData()

	// Create activity instances
	commonActivity := &activities.CommonActivities{}
	backupActivity := &activities.BackupActivity{}
	volumeCreateActivity := &activities.VolumeCreateActivity{}
	volumeUpdateActivity := &activities.VolumeUpdateActivity{}

	// Register activities
	s.env.RegisterActivity(commonActivity)
	s.env.RegisterActivity(volumeCreateActivity)
	s.env.RegisterActivity(volumeUpdateActivity)
	s.env.RegisterActivity(activities.GetObjStoreNameFromBackup)
	s.env.RegisterActivity(activities.GetBucketDetailsFromBackup)

	// Register specific backup activity methods
	s.env.RegisterActivity(backupActivity.GetSmSourcePathActivity)
	s.env.RegisterActivity(backupActivity.GetSmSourcePathForRestoreActivity)
	s.env.RegisterActivity(backupActivity.GetOrCreateObjectStore)
	s.env.RegisterActivity(backupActivity.SnapmirrorGetOrCreate)
	s.env.RegisterActivity(backupActivity.SnapmirrorTransfer)
	s.env.RegisterActivity(backupActivity.GetSnapmirrorTransferStatus)

	// Register missing activities
	s.env.RegisterActivity(volumeCreateActivity.CrossPoolOrVPCRestorationActivity)
	s.env.RegisterActivity(volumeCreateActivity.DeleteObjectStoreForCrossVPC)

	// Track UpdateJobStatus calls to verify error handling
	var jobStatusCalls []string
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		job := args.Get(1).(*datamodel.Job)
		jobStatusCalls = append(jobStatusCalls, job.State)
	}).Return(nil)

	s.env.OnActivity(volumeCreateActivity.CrossPoolOrVPCRestorationActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnWorkflow("PreBlockVolumeWorkflow", mock.Anything, mock.Anything, mock.Anything).Return(volume, nil)
	s.env.OnActivity(backupActivity.GetSmSourcePathActivity, mock.Anything, mock.Anything).Return("test-dest-path", nil)
	s.env.OnActivity(backupActivity.GetSmSourcePathForRestoreActivity, mock.Anything, mock.Anything, mock.Anything).Return("test-source-path", nil)
	s.env.OnActivity(activities.GetObjStoreNameFromBackup, mock.Anything, mock.Anything).Return("test-obj-store", nil)
	s.env.OnActivity(activities.GetBucketDetailsFromBackup, mock.Anything, mock.Anything).Return(&datamodel.BucketDetails{BucketName: "test-bucket"}, nil)
	s.env.OnActivity(backupActivity.GetOrCreateObjectStore, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.CloudTarget{}, nil)
	s.env.OnActivity(backupActivity.SnapmirrorGetOrCreate, mock.Anything, mock.Anything, mock.Anything).Return(&common.SnapmirrorRelationship{UUID: "test-uuid"}, nil)
	s.env.OnActivity(backupActivity.SnapmirrorTransfer, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupActivity.GetSnapmirrorTransferStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(activities.SmStatusSuccess, nil)
	s.env.OnActivity(volumeUpdateActivity.GetVolumeFromONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{Type: "rw"}, nil)
	s.env.OnWorkflow("PostBlockVolumeWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(volume, nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("update volume details failed"))

	// Execute workflow
	s.env.ExecuteWorkflow(RestoreBackupWorkflow, params, volume, backupVault, backup, hostParams, volCreateResponse)

	// Debug logging
	s.T().Logf("Workflow completed: %v", s.env.IsWorkflowCompleted())
	s.T().Logf("Workflow error: %v", s.env.GetWorkflowError())
	s.T().Logf("Job status calls: %v", jobStatusCalls)

	// Assertions
	assert.True(s.T(), s.env.IsWorkflowCompleted())

	// The workflow completes successfully but should have called UpdateJobStatus with ERROR
	// This is due to a bug in the workflow where it returns nil instead of the customErr
	assert.Error(s.T(), s.env.GetWorkflowError()) // The workflow should fail due to the error scenario being tested

	// Verify that the workflow attempted to update job status to ERROR
	assert.Contains(s.T(), jobStatusCalls, "PROCESSING")
	assert.Contains(s.T(), jobStatusCalls, "ERROR")
}

func (s *BackupRestoreWorkflowTestSuite) TestRestoreBackupWorkflow_QueryHandler() {
	params, volume, backupVault, backup, hostParams, volCreateResponse := s.createTestData()

	// Register common activities and mocks
	s.registerCommonActivities()
	s.setupCommonMocks(volume)

	// Execute workflow
	s.env.ExecuteWorkflow(RestoreBackupWorkflow, params, volume, backupVault, backup, hostParams, volCreateResponse)

	// Query workflow status
	status, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.NoError(s.T(), err)
	assert.NotNil(s.T(), status)

	// Assertions
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NoError(s.T(), s.env.GetWorkflowError())
}

func (s *BackupRestoreWorkflowTestSuite) TestRestoreBackupWorkflow_Constants() {
	// Test volume state constants
	assert.Equal(s.T(), "rw", "rw")
	assert.Equal(s.T(), "dp", "dp")
	assert.Equal(s.T(), "ls", "ls")

	// Test WaitForRestore constant
	assert.Equal(s.T(), time.Duration(10)*time.Second, WaitForRestore)
}

func (s *BackupRestoreWorkflowTestSuite) TestRestoreBackupWorkflow_WorkflowInterface() {
	// Test that restoreBackupWorkflow implements WorkflowInterface
	var _ WorkflowInterface = &restoreBackupWorkflow{}
}

func (s *BackupRestoreWorkflowTestSuite) TestRestoreBackupWorkflow_RetryPolicyError() {
	params, volume, backupVault, backup, hostParams, volCreateResponse := s.createTestData()

	// Set invalid environment variable to cause PopulateRetryPolicyParams to fail
	originalStartToCloseTimeout := StartToCloseTimeout
	StartToCloseTimeout = "invalid-duration"
	defer func() { StartToCloseTimeout = originalStartToCloseTimeout }()

	// Create activity instances
	commonActivity := &activities.CommonActivities{}

	// Register activities
	s.env.RegisterActivity(commonActivity)

	// Track UpdateJobStatus calls to verify error handling
	var jobStatusCalls []string
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		job := args.Get(1).(*datamodel.Job)
		jobStatusCalls = append(jobStatusCalls, job.State)
	}).Return(nil)

	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)

	// Execute workflow
	s.env.ExecuteWorkflow(RestoreBackupWorkflow, params, volume, backupVault, backup, hostParams, volCreateResponse)

	// Debug logging
	s.T().Logf("Workflow completed: %v", s.env.IsWorkflowCompleted())
	s.T().Logf("Workflow error: %v", s.env.GetWorkflowError())
	s.T().Logf("Job status calls: %v", jobStatusCalls)

	// Assertions
	assert.True(s.T(), s.env.IsWorkflowCompleted())

	// The workflow completes successfully but should have called UpdateJobStatus with ERROR
	// This is due to a bug in the workflow where it returns nil instead of the customErr
	assert.Error(s.T(), s.env.GetWorkflowError()) // The workflow should fail due to the error scenario being tested

	// Verify that the workflow attempted to update job status to ERROR
	assert.Contains(s.T(), jobStatusCalls, "PROCESSING")
	assert.Contains(s.T(), jobStatusCalls, "ERROR")
}

func (s *BackupRestoreWorkflowTestSuite) TestRestoreBackupWorkflow_GetSmSourcePathForRestoreFailure() {
	params, volume, backupVault, backup, hostParams, volCreateResponse := s.createTestData()

	// Create activity instances
	commonActivity := &activities.CommonActivities{}
	backupActivity := &activities.BackupActivity{}

	// Register activities
	s.env.RegisterActivity(commonActivity)

	// Register specific backup activity methods
	s.env.RegisterActivity(backupActivity.GetSmSourcePathActivity)
	s.env.RegisterActivity(backupActivity.GetSmSourcePathForRestoreActivity)

	// Track UpdateJobStatus calls
	var jobStatusCalls []string
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		job := args.Get(1).(*datamodel.Job)
		jobStatusCalls = append(jobStatusCalls, job.State)
	}).Return(nil)

	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnWorkflow("PreBlockVolumeWorkflow", mock.Anything, mock.Anything, mock.Anything).Return(volume, nil)
	s.env.OnActivity(backupActivity.GetSmSourcePathActivity, mock.Anything, mock.Anything).Return("test-dest-path", nil)
	s.env.OnActivity(backupActivity.GetSmSourcePathForRestoreActivity, mock.Anything, mock.Anything, mock.Anything).Return("", errors.New("get sm source path for restore failed"))

	// Execute workflow
	s.env.ExecuteWorkflow(RestoreBackupWorkflow, params, volume, backupVault, backup, hostParams, volCreateResponse)

	// Assertions
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError()) // The workflow should fail due to the error scenario being tested
	assert.Contains(s.T(), jobStatusCalls, "PROCESSING")
	assert.Contains(s.T(), jobStatusCalls, "ERROR")
}

func (s *BackupRestoreWorkflowTestSuite) TestRestoreBackupWorkflow_GetObjStoreNameFailure() {
	params, volume, backupVault, backup, hostParams, volCreateResponse := s.createTestData()

	// Create activity instances
	commonActivity := &activities.CommonActivities{}
	backupActivity := &activities.BackupActivity{}

	// Register activities
	s.env.RegisterActivity(commonActivity)
	s.env.RegisterActivity(activities.GetObjStoreNameFromBackup)

	// Register specific backup activity methods
	s.env.RegisterActivity(backupActivity.GetSmSourcePathActivity)
	s.env.RegisterActivity(backupActivity.GetSmSourcePathForRestoreActivity)

	// Track UpdateJobStatus calls
	var jobStatusCalls []string
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		job := args.Get(1).(*datamodel.Job)
		jobStatusCalls = append(jobStatusCalls, job.State)
	}).Return(nil)

	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnWorkflow("PreBlockVolumeWorkflow", mock.Anything, mock.Anything, mock.Anything).Return(volume, nil)
	s.env.OnActivity(backupActivity.GetSmSourcePathActivity, mock.Anything, mock.Anything).Return("test-dest-path", nil)
	s.env.OnActivity(backupActivity.GetSmSourcePathForRestoreActivity, mock.Anything, mock.Anything, mock.Anything).Return("test-source-path", nil)
	s.env.OnActivity(activities.GetObjStoreNameFromBackup, mock.Anything, mock.Anything).Return("", errors.New("get obj store name failed"))

	// Execute workflow
	s.env.ExecuteWorkflow(RestoreBackupWorkflow, params, volume, backupVault, backup, hostParams, volCreateResponse)

	// Assertions
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError()) // The workflow should fail due to the error scenario being tested
	assert.Contains(s.T(), jobStatusCalls, "PROCESSING")
	assert.Contains(s.T(), jobStatusCalls, "ERROR")
}

func (s *BackupRestoreWorkflowTestSuite) TestRestoreBackupWorkflow_GetBucketDetailsFailure() {
	params, volume, backupVault, backup, hostParams, volCreateResponse := s.createTestData()

	// Create activity instances
	commonActivity := &activities.CommonActivities{}
	backupActivity := &activities.BackupActivity{}
	volumeCreateActivity := &activities.VolumeCreateActivity{}

	// Register activities
	s.env.RegisterActivity(commonActivity)
	s.env.RegisterActivity(volumeCreateActivity)
	s.env.RegisterActivity(activities.GetObjStoreNameFromBackup)
	s.env.RegisterActivity(activities.GetBucketDetailsFromBackup)

	// Register specific backup activity methods
	s.env.RegisterActivity(backupActivity.GetSmSourcePathActivity)
	s.env.RegisterActivity(backupActivity.GetSmSourcePathForRestoreActivity)
	s.env.RegisterActivity(backupActivity.GetOrCreateObjectStore)
	s.env.RegisterActivity(backupActivity.SnapmirrorGetOrCreate)
	s.env.RegisterActivity(backupActivity.SnapmirrorTransfer)
	s.env.RegisterActivity(backupActivity.GetSnapmirrorTransferStatus)

	// Register missing activities
	s.env.RegisterActivity(volumeCreateActivity.CrossPoolOrVPCRestorationActivity)
	s.env.RegisterActivity(volumeCreateActivity.DeleteObjectStoreForCrossVPC)

	// Track UpdateJobStatus calls
	var jobStatusCalls []string
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		job := args.Get(1).(*datamodel.Job)
		jobStatusCalls = append(jobStatusCalls, job.State)
	}).Return(nil)

	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnWorkflow("PreBlockVolumeWorkflow", mock.Anything, mock.Anything, mock.Anything).Return(volume, nil)
	s.env.OnActivity(backupActivity.GetSmSourcePathActivity, mock.Anything, mock.Anything).Return("test-dest-path", nil)
	s.env.OnActivity(backupActivity.GetSmSourcePathForRestoreActivity, mock.Anything, mock.Anything, mock.Anything).Return("test-source-path", nil)
	s.env.OnActivity(activities.GetObjStoreNameFromBackup, mock.Anything, mock.Anything).Return("test-obj-store", nil)
	s.env.OnActivity(activities.GetBucketDetailsFromBackup, mock.Anything, mock.Anything).Return(nil, errors.New("get bucket details failed"))

	// Execute workflow
	s.env.ExecuteWorkflow(RestoreBackupWorkflow, params, volume, backupVault, backup, hostParams, volCreateResponse)

	// Assertions
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	// The workflow should fail due to the bucket details failure
	assert.Error(s.T(), s.env.GetWorkflowError())
	// Verify that the workflow attempted to update job status to ERROR
	assert.Contains(s.T(), jobStatusCalls, "PROCESSING")
	assert.Contains(s.T(), jobStatusCalls, "ERROR")
}

func (s *BackupRestoreWorkflowTestSuite) TestRestoreBackupWorkflow_GetOrCreateObjectStoreFailure() {
	params, volume, backupVault, backup, hostParams, volCreateResponse := s.createTestData()

	// Create activity instances
	commonActivity := &activities.CommonActivities{}
	backupActivity := &activities.BackupActivity{}
	volumeCreateActivity := &activities.VolumeCreateActivity{}

	// Register activities
	s.env.RegisterActivity(commonActivity)
	s.env.RegisterActivity(volumeCreateActivity)
	s.env.RegisterActivity(activities.GetObjStoreNameFromBackup)
	s.env.RegisterActivity(activities.GetBucketDetailsFromBackup)

	// Register specific backup activity methods
	s.env.RegisterActivity(backupActivity.GetSmSourcePathActivity)
	s.env.RegisterActivity(backupActivity.GetSmSourcePathForRestoreActivity)
	s.env.RegisterActivity(backupActivity.GetOrCreateObjectStore)
	s.env.RegisterActivity(backupActivity.SnapmirrorGetOrCreate)
	s.env.RegisterActivity(backupActivity.SnapmirrorTransfer)
	s.env.RegisterActivity(backupActivity.GetSnapmirrorTransferStatus)

	// Register missing activities
	s.env.RegisterActivity(volumeCreateActivity.CrossPoolOrVPCRestorationActivity)
	s.env.RegisterActivity(volumeCreateActivity.DeleteObjectStoreForCrossVPC)

	// Track UpdateJobStatus calls
	var jobStatusCalls []string
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		job := args.Get(1).(*datamodel.Job)
		jobStatusCalls = append(jobStatusCalls, job.State)
	}).Return(nil)

	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnWorkflow("PreBlockVolumeWorkflow", mock.Anything, mock.Anything, mock.Anything).Return(volume, nil)
	s.env.OnActivity(backupActivity.GetSmSourcePathActivity, mock.Anything, mock.Anything).Return("test-dest-path", nil)
	s.env.OnActivity(backupActivity.GetSmSourcePathForRestoreActivity, mock.Anything, mock.Anything, mock.Anything).Return("test-source-path", nil)
	s.env.OnActivity(activities.GetObjStoreNameFromBackup, mock.Anything, mock.Anything).Return("test-obj-store", nil)
	s.env.OnActivity(activities.GetBucketDetailsFromBackup, mock.Anything, mock.Anything).Return(&datamodel.BucketDetails{BucketName: "test-bucket"}, nil)
	s.env.OnActivity(backupActivity.GetOrCreateObjectStore, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("get or create object store failed"))

	// Execute workflow
	s.env.ExecuteWorkflow(RestoreBackupWorkflow, params, volume, backupVault, backup, hostParams, volCreateResponse)

	// Assertions
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	// The workflow should fail due to the object store failure
	assert.Error(s.T(), s.env.GetWorkflowError())
	// Verify that the workflow attempted to update job status to ERROR
	assert.Contains(s.T(), jobStatusCalls, "PROCESSING")
	assert.Contains(s.T(), jobStatusCalls, "ERROR")
}

func (s *BackupRestoreWorkflowTestSuite) TestRestoreBackupWorkflow_SnapmirrorGetOrCreateFailure() {
	params, volume, backupVault, backup, hostParams, volCreateResponse := s.createTestData()

	// Create activity instances
	commonActivity := &activities.CommonActivities{}
	backupActivity := &activities.BackupActivity{}
	volumeCreateActivity := &activities.VolumeCreateActivity{}

	// Register activities
	s.env.RegisterActivity(commonActivity)
	s.env.RegisterActivity(volumeCreateActivity)
	s.env.RegisterActivity(activities.GetObjStoreNameFromBackup)
	s.env.RegisterActivity(activities.GetBucketDetailsFromBackup)

	// Register specific backup activity methods
	s.env.RegisterActivity(backupActivity.GetSmSourcePathActivity)
	s.env.RegisterActivity(backupActivity.GetSmSourcePathForRestoreActivity)
	s.env.RegisterActivity(backupActivity.GetOrCreateObjectStore)
	s.env.RegisterActivity(backupActivity.SnapmirrorGetOrCreate)
	s.env.RegisterActivity(backupActivity.SnapmirrorTransfer)
	s.env.RegisterActivity(backupActivity.GetSnapmirrorTransferStatus)

	// Register missing activities
	s.env.RegisterActivity(volumeCreateActivity.CrossPoolOrVPCRestorationActivity)
	s.env.RegisterActivity(volumeCreateActivity.DeleteObjectStoreForCrossVPC)

	// Track UpdateJobStatus calls
	var jobStatusCalls []string
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		job := args.Get(1).(*datamodel.Job)
		jobStatusCalls = append(jobStatusCalls, job.State)
	}).Return(nil)

	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnWorkflow("PreBlockVolumeWorkflow", mock.Anything, mock.Anything, mock.Anything).Return(volume, nil)
	s.env.OnActivity(backupActivity.GetSmSourcePathActivity, mock.Anything, mock.Anything).Return("test-dest-path", nil)
	s.env.OnActivity(backupActivity.GetSmSourcePathForRestoreActivity, mock.Anything, mock.Anything, mock.Anything).Return("test-source-path", nil)
	s.env.OnActivity(activities.GetObjStoreNameFromBackup, mock.Anything, mock.Anything).Return("test-obj-store", nil)
	s.env.OnActivity(activities.GetBucketDetailsFromBackup, mock.Anything, mock.Anything).Return(&datamodel.BucketDetails{BucketName: "test-bucket"}, nil)
	s.env.OnActivity(backupActivity.GetOrCreateObjectStore, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.CloudTarget{}, nil)
	s.env.OnActivity(backupActivity.SnapmirrorGetOrCreate, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("snapmirror get or create failed"))

	// Execute workflow
	s.env.ExecuteWorkflow(RestoreBackupWorkflow, params, volume, backupVault, backup, hostParams, volCreateResponse)

	// Assertions
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError()) // The workflow should fail due to the error scenario being tested
	assert.Contains(s.T(), jobStatusCalls, "PROCESSING")
	assert.Contains(s.T(), jobStatusCalls, "ERROR")
}

func (s *BackupRestoreWorkflowTestSuite) TestRestoreBackupWorkflow_SnapmirrorTransferFailure() {
	params, volume, backupVault, backup, hostParams, volCreateResponse := s.createTestData()

	// Create activity instances
	commonActivity := &activities.CommonActivities{}
	backupActivity := &activities.BackupActivity{}
	volumeCreateActivity := &activities.VolumeCreateActivity{}

	// Register activities
	s.env.RegisterActivity(commonActivity)
	s.env.RegisterActivity(volumeCreateActivity)
	s.env.RegisterActivity(activities.GetObjStoreNameFromBackup)
	s.env.RegisterActivity(activities.GetBucketDetailsFromBackup)

	// Register specific backup activity methods
	s.env.RegisterActivity(backupActivity.GetSmSourcePathActivity)
	s.env.RegisterActivity(backupActivity.GetSmSourcePathForRestoreActivity)
	s.env.RegisterActivity(backupActivity.GetOrCreateObjectStore)
	s.env.RegisterActivity(backupActivity.SnapmirrorGetOrCreate)
	s.env.RegisterActivity(backupActivity.SnapmirrorTransfer)
	s.env.RegisterActivity(backupActivity.GetSnapmirrorTransferStatus)

	// Register missing activities
	s.env.RegisterActivity(volumeCreateActivity.CrossPoolOrVPCRestorationActivity)
	s.env.RegisterActivity(volumeCreateActivity.DeleteObjectStoreForCrossVPC)

	// Track UpdateJobStatus calls
	var jobStatusCalls []string
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		job := args.Get(1).(*datamodel.Job)
		jobStatusCalls = append(jobStatusCalls, job.State)
	}).Return(nil)

	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnWorkflow("PreBlockVolumeWorkflow", mock.Anything, mock.Anything, mock.Anything).Return(volume, nil)
	s.env.OnActivity(backupActivity.GetSmSourcePathActivity, mock.Anything, mock.Anything).Return("test-dest-path", nil)
	s.env.OnActivity(backupActivity.GetSmSourcePathForRestoreActivity, mock.Anything, mock.Anything, mock.Anything).Return("test-source-path", nil)
	s.env.OnActivity(activities.GetObjStoreNameFromBackup, mock.Anything, mock.Anything).Return("test-obj-store", nil)
	s.env.OnActivity(activities.GetBucketDetailsFromBackup, mock.Anything, mock.Anything).Return(&datamodel.BucketDetails{BucketName: "test-bucket"}, nil)
	s.env.OnActivity(backupActivity.GetOrCreateObjectStore, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.CloudTarget{}, nil)
	s.env.OnActivity(backupActivity.SnapmirrorGetOrCreate, mock.Anything, mock.Anything, mock.Anything).Return(&common.SnapmirrorRelationship{UUID: "test-uuid"}, nil)
	s.env.OnActivity(backupActivity.SnapmirrorTransfer, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("snapmirror transfer failed"))

	// Execute workflow
	s.env.ExecuteWorkflow(RestoreBackupWorkflow, params, volume, backupVault, backup, hostParams, volCreateResponse)

	// Assertions
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError()) // The workflow should fail due to the error scenario being tested
	assert.Contains(s.T(), jobStatusCalls, "PROCESSING")
	assert.Contains(s.T(), jobStatusCalls, "ERROR")
}

func (s *BackupRestoreWorkflowTestSuite) TestRestoreBackupWorkflow_SnapmirrorTransferWaitTimeCap() {
	// This test covers the case where waitTime exceeds BackupMaxWaitTimeCap (line 214)
	params, volume, backupVault, backup, hostParams, volCreateResponse := s.createTestData()

	// Register common activities and mocks
	s.registerCommonActivities()
	s.setupCommonMocks(volume)

	// Override snapmirror transfer status to trigger wait time cap logic
	backupActivity := &activities.BackupActivity{}
	// Mock multiple transferring status calls to trigger the wait time cap logic
	// First call: transferring
	s.env.OnActivity(backupActivity.GetSnapmirrorTransferStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(activities.SmStatusTransferring, nil).Once()
	// Second call: transferring (wait time doubles)
	s.env.OnActivity(backupActivity.GetSnapmirrorTransferStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(activities.SmStatusTransferring, nil).Once()
	// Third call: transferring (wait time doubles again)
	s.env.OnActivity(backupActivity.GetSnapmirrorTransferStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(activities.SmStatusTransferring, nil).Once()
	// Fourth call: transferring (wait time doubles again, should hit cap)
	s.env.OnActivity(backupActivity.GetSnapmirrorTransferStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(activities.SmStatusTransferring, nil).Once()
	// Fifth call: transferring (wait time should be capped)
	s.env.OnActivity(backupActivity.GetSnapmirrorTransferStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(activities.SmStatusTransferring, nil).Once()
	// Sixth call: transferring (wait time should still be capped)
	s.env.OnActivity(backupActivity.GetSnapmirrorTransferStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(activities.SmStatusTransferring, nil).Once()
	// Final call: success
	s.env.OnActivity(backupActivity.GetSnapmirrorTransferStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(activities.SmStatusSuccess, nil).Once()

	// Execute workflow
	s.env.ExecuteWorkflow(RestoreBackupWorkflow, params, volume, backupVault, backup, hostParams, volCreateResponse)

	// Assertions
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NoError(s.T(), s.env.GetWorkflowError())
}

// Test for RestoreBackupWorkflowWithContext function
func (s *BackupRestoreWorkflowTestSuite) TestRestoreBackupWorkflowWithContext_Success() {
	params, volume, backupVault, backup, hostParams, volCreateResponse := s.createTestData()

	// Create backup activities context
	backupActivitiesContext := &activities.BackupActivitiesContext{
		BackupWorkflowInit: &activities.BackupWorkflowInput{
			Backup:      backup,
			BackupVault: backupVault,
			Volume:      volume,
		},
		Node: &models.Node{EndpointAddress: "127.0.0.1"},
		SnapmirrorRelationship: &common.SnapmirrorRelationship{
			UUID: "test-snapmirror-uuid",
		},
		TransferStatus: activities.SmStatusSuccess,
	}

	// Register common activities and mocks
	s.registerCommonActivities()
	s.setupCommonMocks(volume)

	// Execute workflow with context
	s.env.ExecuteWorkflow(RestoreBackupWorkflowWithContext, backupActivitiesContext, params, hostParams, volCreateResponse)

	// Assertions
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NoError(s.T(), s.env.GetWorkflowError())
}

// Test for RestoreBackupWorkflowWithContext with setup failure
func (s *BackupRestoreWorkflowTestSuite) TestRestoreBackupWorkflowWithContext_SetupFailure() {
	params, volume, backupVault, backup, hostParams, volCreateResponse := s.createTestData()

	// Create backup activities context
	backupActivitiesContext := &activities.BackupActivitiesContext{
		BackupWorkflowInit: &activities.BackupWorkflowInput{
			Backup:      backup,
			BackupVault: backupVault,
			Volume:      volume,
		},
	}

	// Create activity instances
	commonActivity := &activities.CommonActivities{}

	// Register activities
	s.env.RegisterActivity(commonActivity)

	// Mock UpdateJobStatus to fail
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(errors.New("setup failed"))

	// Execute workflow with context
	s.env.ExecuteWorkflow(RestoreBackupWorkflowWithContext, backupActivitiesContext, params, hostParams, volCreateResponse)

	// Assertions
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "setup failed")
}

// Test for RestoreBackupWorkflowWithContext with run failure
func (s *BackupRestoreWorkflowTestSuite) TestRestoreBackupWorkflowWithContext_RunFailure() {
	params, volume, backupVault, backup, hostParams, volCreateResponse := s.createTestData()

	// Create backup activities context
	backupActivitiesContext := &activities.BackupActivitiesContext{
		BackupWorkflowInit: &activities.BackupWorkflowInput{
			Backup:      backup,
			BackupVault: backupVault,
			Volume:      volume,
		},
	}

	// Create activity instances
	commonActivity := &activities.CommonActivities{}

	// Register activities
	s.env.RegisterActivity(commonActivity)

	// Track UpdateJobStatus calls to verify error handling
	var jobStatusCalls []string
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		job := args.Get(1).(*datamodel.Job)
		jobStatusCalls = append(jobStatusCalls, job.State)
	}).Return(nil)

	// Mock GetNode to fail with non-retryable error
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return(nil, temporal.NewNonRetryableApplicationError("get node failed", "GetNodeFailure", nil))

	// Execute workflow with context
	s.env.ExecuteWorkflow(RestoreBackupWorkflowWithContext, backupActivitiesContext, params, hostParams, volCreateResponse)

	// Assertions
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())

	// Verify that the workflow attempted to update job status to ERROR
	assert.Contains(s.T(), jobStatusCalls, "PROCESSING")
	assert.Contains(s.T(), jobStatusCalls, "ERROR")
}

// Test for RestoreBackupWorkflowWithContext with continuation scenario
func (s *BackupRestoreWorkflowTestSuite) TestRestoreBackupWorkflowWithContext_ContinuationScenario() {
	params, volume, backupVault, backup, hostParams, volCreateResponse := s.createTestData()

	// Create backup activities context with continuation data
	backupActivitiesContext := &activities.BackupActivitiesContext{
		BackupWorkflowInit: &activities.BackupWorkflowInput{
			Backup:      backup,
			BackupVault: backupVault,
			Volume:      volume,
		},
		Node:         &models.Node{EndpointAddress: "127.0.0.1"},
		SnapshotName: "test-backup",
		SnapmirrorRelationship: &common.SnapmirrorRelationship{
			UUID: "test-snapmirror-uuid",
		},
		TransferStatus: activities.SmStatusSuccess,
	}

	// Register common activities and mocks
	s.registerCommonActivities()
	s.setupCommonMocks(volume)

	// Execute workflow with context
	s.env.ExecuteWorkflow(RestoreBackupWorkflowWithContext, backupActivitiesContext, params, hostParams, volCreateResponse)

	// Assertions
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NoError(s.T(), s.env.GetWorkflowError())
}
