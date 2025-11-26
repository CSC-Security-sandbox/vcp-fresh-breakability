package replicationWorkflows

import (
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/replicationActivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/replication"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

type HybridCreateWorkflowTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env *testsuite.TestWorkflowEnvironment
}

func (s *HybridCreateWorkflowTestSuite) SetupTest() {
	// Set environment to local to avoid Google Cloud credentials issues in tests
	err := os.Setenv("ENV", "local")
	if err != nil {
		s.T().Fatalf("Failed to set ENV variable: %v", err)
	}

	s.env = s.NewTestWorkflowEnvironment()
	s.env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	s.env.RegisterWorkflow(CreateHybridReplicationWorkflow)
	s.env.RegisterWorkflow(EstablishPeeringWorkflow)
	s.env.RegisterWorkflow(InternalEstablishWorkflow)
	s.env.RegisterWorkflow(workflows.CreateVolumeWorkflow)
}

func (s *HybridCreateWorkflowTestSuite) AfterTest() {
	s.env.AssertExpectations(s.T())
}

func TestHybridCreateWorkflowTestSuite(t *testing.T) {
	suite.Run(t, new(HybridCreateWorkflowTestSuite))
}

// Helper function to register all activities needed for CreateHybridReplicationWorkflow tests
func (s *HybridCreateWorkflowTestSuite) registerHybridReplicationActivities(commonActivity *activities.CommonActivities, hybridReplicationActivity *replicationActivities.HybridReplicationActivity) {
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(commonActivity.CreateJob)
	s.env.RegisterActivity(commonActivity.GetNode)
	s.env.RegisterActivity(commonActivity.GetAuthJWTToken)
	s.env.RegisterActivity(hybridReplicationActivity.GetDstBasePathForHybridReplication)
	s.env.RegisterActivity(hybridReplicationActivity.GetDstSignedTokenForHybridReplication)
	s.env.RegisterActivity(hybridReplicationActivity.GetOrCreateClusterPeerForHybridReplication)
	s.env.RegisterActivity(hybridReplicationActivity.CreateLocalHybridReplicationRow)
	s.env.RegisterActivity(hybridReplicationActivity.HydrateReplicationSateForHybridReplication)
	s.env.RegisterActivity(hybridReplicationActivity.UpdateClusterPeerDetailsOnErrorActivity)
	s.env.RegisterActivity(hybridReplicationActivity.UpdateReplicationRowDetailsOnErrorActivity)
	s.env.RegisterActivity(hybridReplicationActivity.CreateJobForHybridReplication)
	s.env.RegisterActivity(hybridReplicationActivity.WaitForClusterPeerActivityForHybridReplication)
	s.env.RegisterActivity(hybridReplicationActivity.SetClusterPeeringStatusToPeeredForHybridReplication)
	s.env.RegisterActivity(hybridReplicationActivity.SetVolumeReplicationPeeringStatusToPendingSVMPeering)
	s.env.RegisterActivity(hybridReplicationActivity.CreateSVMPeerInOntapForHybridReplication)
	s.env.RegisterActivity(hybridReplicationActivity.SetVolumeReplicationSVMPeeringDetails)
	s.env.RegisterActivity(hybridReplicationActivity.WaitForSVMPeerForHybridReplication)
	s.env.RegisterActivity(hybridReplicationActivity.GetReplicationForHybridReplication)
	s.env.RegisterActivity(hybridReplicationActivity.CleanupReplicationIfNeeded)
	s.env.RegisterActivity(hybridReplicationActivity.CreateHybridVolumeReplicationInternal)
	s.env.RegisterActivity(hybridReplicationActivity.UpdateHybridVolumeReplicationDetailsAndSetPeeringStatusToPeered)
	s.env.RegisterActivity(hybridReplicationActivity.HydrateReplicationSateForHybridReplication)
	s.env.RegisterActivity(hybridReplicationActivity.UpdateClusterPeeringInReplication)
	s.env.RegisterActivity(hybridReplicationActivity.HydrateVolumeReplicationForHybridReplication)
	s.env.RegisterActivity(hybridReplicationActivity.GetOrCreateClusterPeerInOntapForHybridReplication)
	s.env.RegisterActivity(hybridReplicationActivity.UpdateClusterPeerDetailsOnErrorActivity)
	s.env.RegisterActivity(hybridReplicationActivity.UpdateReplicationRowDetailsOnErrorActivity)
	s.env.RegisterActivity(hybridReplicationActivity.GetNodeForHybridReplication)
	s.env.RegisterActivity(hybridReplicationActivity.CreateNodesForHybridReplication)
}

// Helper function to create test data
func (s *HybridCreateWorkflowTestSuite) createTestData() (*common.CreateVolumeParams, *datamodel.Volume, *datamodel.BackupVault, *datamodel.Backup) {
	// Create test account
	account := &datamodel.Account{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-account-uuid"},
		Name:      "test_account",
	}

	// Create test pool
	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"},
		Name:      "test_pool",
		AccountID: account.ID,
		Account:   account,
		ClusterDetails: datamodel.ClusterDetails{
			ExternalName: "test-cluster",
		},
		DeploymentName: "test-deployment",
		PoolCredentials: &datamodel.PoolCredentials{
			Password:      "test-password",
			SecretID:      "test-secret-id",
			CertificateID: "test-cert-id",
			AuthType:      0, // USERNAME_PWD
		},
	}

	// Create test SVM
	svm := &datamodel.Svm{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-svm-uuid"},
		Name:      "test_svm",
		AccountID: account.ID,
		Account:   account,
	}

	// Create test volume
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-volume-uuid"},
		Name:      "test_volume",
		AccountID: account.ID,
		Account:   account,
		PoolID:    pool.ID,
		Pool:      pool,
		SvmID:     svm.ID,
		Svm:       svm,
	}

	// Create test backup vault
	backupVault := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-backup-vault-uuid"},
		Name:      "test_backup_vault",
		AccountID: account.ID,
		Account:   account,
	}

	// Create test backup
	backup := &datamodel.Backup{
		BaseModel:     datamodel.BaseModel{ID: 1, UUID: "test-backup-uuid"},
		Name:          "test_backup",
		BackupVaultID: backupVault.ID,
		BackupVault:   backupVault,
	}

	// Create test parameters
	params := &common.CreateVolumeParams{
		AccountName: "test_account",
		Region:      "us-central1",
		HybridReplicationParameters: &models.HybridReplicationParameters{
			ResourceID:          "test-replication-id",
			ReplicationType:     models.HybridReplicationParametersReplicationTypeONPREM,
			PeerVolumeName:      "peer-volume",
			PeerClusterName:     "peer-cluster",
			PeerSvmName:         "peer-svm",
			PeerIPAddresses:     []string{"192.168.1.1", "192.168.1.2"},
			Labels:              map[string]string{"env": "test"},
			Description:         "Test hybrid replication",
			ClusterLocation:     "us-central1",
			ReplicationSchedule: "hourly",
		},
	}

	return params, volume, backupVault, backup
}

func (s *HybridCreateWorkflowTestSuite) TestCreateHybridReplicationWorkflow_Success() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := &activities.CommonActivities{SE: mockStorage}
	hybridReplicationActivity := &replicationActivities.HybridReplicationActivity{SE: mockStorage}

	s.registerHybridReplicationActivities(commonActivity, hybridReplicationActivity)
	params, volume, backupVault, backup := s.createTestData()

	// Mock successful activity responses
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.CreateJob, mock.Anything, mock.Anything).Return(&datamodel.Job{BaseModel: datamodel.BaseModel{UUID: "test-job-uuid"}}, nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{BaseModel: datamodel.BaseModel{UUID: "test-node-uuid"}}}, nil)
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("test-jwt-token", nil)
	s.env.OnActivity(hybridReplicationActivity.GetDstBasePathForHybridReplication, mock.Anything, mock.Anything).Return(&replication.CreateHybridReplicationResult{
		DestinationVolume:        volume,
		DestinationRegion:        params.Region,
		DestinationProjectNumber: params.AccountName,
	}, nil)
	s.env.OnActivity(hybridReplicationActivity.GetDstSignedTokenForHybridReplication, mock.Anything, mock.Anything).Return(&replication.CreateHybridReplicationResult{
		DestinationVolume:        volume,
		DestinationRegion:        params.Region,
		DestinationProjectNumber: params.AccountName,
	}, nil)
	s.env.OnActivity(hybridReplicationActivity.GetOrCreateClusterPeerForHybridReplication, mock.Anything, mock.Anything).Return(&replication.CreateHybridReplicationResult{
		DestinationVolume:        volume,
		DestinationRegion:        params.Region,
		DestinationProjectNumber: params.AccountName,
	}, nil)
	s.env.OnActivity(hybridReplicationActivity.CreateLocalHybridReplicationRow, mock.Anything, mock.Anything).Return(&replication.CreateHybridReplicationResult{
		DestinationVolume:        volume,
		DestinationRegion:        params.Region,
		DestinationProjectNumber: params.AccountName,
	}, nil)
	s.env.OnActivity(hybridReplicationActivity.HydrateReplicationSateForHybridReplication, mock.Anything, mock.Anything).Return(&replication.CreateHybridReplicationResult{
		DestinationVolume:        volume,
		DestinationRegion:        params.Region,
		DestinationProjectNumber: params.AccountName,
	}, nil)
	s.env.OnActivity(hybridReplicationActivity.CreateJobForHybridReplication, mock.Anything, mock.Anything, mock.Anything).Return(&datamodel.Job{BaseModel: datamodel.BaseModel{UUID: "test-job-uuid"}}, nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateHybridReplicationWorkflow, params, volume, backupVault, backup)

	// Verify workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())

	// Verify status query works
	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)
}

func (s *HybridCreateWorkflowTestSuite) TestCreateHybridReplicationWorkflow_GetLocalBasePathError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := &activities.CommonActivities{SE: mockStorage}
	hybridReplicationActivity := &replicationActivities.HybridReplicationActivity{SE: mockStorage}

	s.registerHybridReplicationActivities(commonActivity, hybridReplicationActivity)
	params, volume, backupVault, backup := s.createTestData()

	// Mock activity responses with error
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(hybridReplicationActivity.GetDstBasePathForHybridReplication, mock.Anything, mock.Anything).Return(nil, errors.New("failed to get local base path"))
	s.env.OnActivity(hybridReplicationActivity.UpdateClusterPeerDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(hybridReplicationActivity.UpdateReplicationRowDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateHybridReplicationWorkflow, params, volume, backupVault, backup)

	// Verify workflow completed successfully (child workflow error is ignored)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())

	// Verify status query works
	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)
}

func (s *HybridCreateWorkflowTestSuite) TestCreateHybridReplicationWorkflow_GetSignedLocalTokenError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := &activities.CommonActivities{SE: mockStorage}
	hybridReplicationActivity := &replicationActivities.HybridReplicationActivity{SE: mockStorage}

	s.registerHybridReplicationActivities(commonActivity, hybridReplicationActivity)
	params, volume, backupVault, backup := s.createTestData()

	// Mock activity responses with error
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(hybridReplicationActivity.GetDstBasePathForHybridReplication, mock.Anything, mock.Anything).Return(&replication.CreateHybridReplicationResult{
		DestinationVolume:        volume,
		DestinationRegion:        params.Region,
		DestinationProjectNumber: params.AccountName,
	}, nil)
	s.env.OnActivity(hybridReplicationActivity.GetDstSignedTokenForHybridReplication, mock.Anything, mock.Anything).Return(nil, errors.New("failed to get signed local token"))
	s.env.OnActivity(hybridReplicationActivity.UpdateClusterPeerDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(hybridReplicationActivity.UpdateReplicationRowDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateHybridReplicationWorkflow, params, volume, backupVault, backup)

	// Verify workflow completed successfully (child workflow error is ignored)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())

	// Verify status query works
	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)
}

func (s *HybridCreateWorkflowTestSuite) TestCreateHybridReplicationWorkflow_CreateClusterPeerError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := &activities.CommonActivities{SE: mockStorage}
	hybridReplicationActivity := &replicationActivities.HybridReplicationActivity{SE: mockStorage}

	s.registerHybridReplicationActivities(commonActivity, hybridReplicationActivity)
	params, volume, backupVault, backup := s.createTestData()

	// Create replication result with proper DestinationVolume
	replicationResult := &replication.CreateHybridReplicationResult{
		DestinationVolume:           volume,
		DestinationRegion:           params.Region,
		DestinationProjectNumber:    params.AccountName,
		HybridReplicationParameters: params.HybridReplicationParameters,
	}

	// Mock activity responses with error
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(hybridReplicationActivity.GetDstBasePathForHybridReplication, mock.Anything, mock.Anything).Return(replicationResult, nil)
	s.env.OnActivity(hybridReplicationActivity.GetDstSignedTokenForHybridReplication, mock.Anything, mock.Anything).Return(replicationResult, nil)
	s.env.OnActivity(hybridReplicationActivity.CreateJobForHybridReplication, mock.Anything, mock.Anything, mock.Anything).Return(&datamodel.Job{BaseModel: datamodel.BaseModel{UUID: "test-job-uuid"}}, nil)
	s.env.OnActivity(hybridReplicationActivity.DescribeJobForHybridReplicationWorkflow, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{BaseModel: datamodel.BaseModel{UUID: "test-node-uuid"}}}, nil)
	s.env.OnActivity(hybridReplicationActivity.CreateLocalHybridReplicationRow, mock.Anything, mock.Anything).Return(replicationResult, nil)
	s.env.OnActivity(hybridReplicationActivity.HydrateVolumeReplicationForHybridReplication, mock.Anything, mock.Anything).Return(replicationResult, nil)

	// Mock GetNodesByPoolID for GetNodeForHybridReplication activity (called from both main workflow and child workflow)
	// Use Maybe() to allow multiple calls with different contexts
	mockStorage.On("GetNodesByPoolID", mock.Anything, mock.Anything).Maybe().Return([]*datamodel.Node{{BaseModel: datamodel.BaseModel{UUID: "test-node-uuid"}}}, nil)
	s.env.OnActivity(hybridReplicationActivity.GetNodeForHybridReplication, mock.Anything, mock.Anything).Return(replicationResult, nil)
	s.env.OnActivity(hybridReplicationActivity.CreateNodesForHybridReplication, mock.Anything, mock.Anything).Return(replicationResult, nil)

	s.env.OnActivity(hybridReplicationActivity.GetOrCreateClusterPeerForHybridReplication, mock.Anything, mock.Anything).Return(nil, errors.New("failed to create cluster peer"))
	s.env.OnActivity(hybridReplicationActivity.UpdateClusterPeerDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(hybridReplicationActivity.UpdateReplicationRowDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil)

	// Add mocks for CreateEstablishPeeringWorkflow child workflow activities
	s.env.OnActivity(hybridReplicationActivity.UpdateClusterPeeringInReplication, mock.Anything, mock.Anything).Return(replicationResult, nil)
	s.env.OnActivity(hybridReplicationActivity.GetOrCreateClusterPeerInOntapForHybridReplication, mock.Anything, mock.Anything).Return(replicationResult, nil)
	s.env.OnActivity(hybridReplicationActivity.HydrateReplicationSateForHybridReplication, mock.Anything, mock.Anything).Return(replicationResult, nil)
	s.env.OnActivity(hybridReplicationActivity.WaitForClusterPeerActivityForHybridReplication, mock.Anything, mock.Anything).Return(replicationResult, nil)
	s.env.OnActivity(hybridReplicationActivity.SetClusterPeeringStatusToPeeredForHybridReplication, mock.Anything, mock.Anything).Return(replicationResult, nil)
	s.env.OnActivity(hybridReplicationActivity.SetVolumeReplicationPeeringStatusToPendingSVMPeering, mock.Anything, mock.Anything).Return(replicationResult, nil)
	s.env.OnActivity(hybridReplicationActivity.CreateSVMPeerInOntapForHybridReplication, mock.Anything, mock.Anything).Return(replicationResult, nil)
	s.env.OnActivity(hybridReplicationActivity.SetVolumeReplicationSVMPeeringDetails, mock.Anything, mock.Anything).Return(replicationResult, nil)
	s.env.OnActivity(hybridReplicationActivity.WaitForSVMPeerForHybridReplication, mock.Anything, mock.Anything).Return(replicationResult, nil)
	s.env.OnActivity(hybridReplicationActivity.GetReplicationForHybridReplication, mock.Anything, mock.Anything).Return(replicationResult, nil)
	s.env.OnActivity(hybridReplicationActivity.CleanupReplicationIfNeeded, mock.Anything, mock.Anything).Return(replicationResult, nil)
	s.env.OnActivity(hybridReplicationActivity.CreateHybridVolumeReplicationInternal, mock.Anything, mock.Anything).Return(replicationResult, nil)
	s.env.OnActivity(hybridReplicationActivity.UpdateHybridVolumeReplicationDetailsAndSetPeeringStatusToPeered, mock.Anything, mock.Anything).Return(replicationResult, nil)

	// Mock CreateVolumeWorkflow child workflow
	s.env.OnWorkflow(workflows.CreateVolumeWorkflow, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateHybridReplicationWorkflow, params, volume, backupVault, backup)

	// Verify workflow completed successfully (child workflow error is ignored)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())

	// Verify status query works
	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)
}

func (s *HybridCreateWorkflowTestSuite) TestCreateHybridReplicationWorkflow_CreateLocalVolumeReplicationRowError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := &activities.CommonActivities{SE: mockStorage}
	hybridReplicationActivity := &replicationActivities.HybridReplicationActivity{SE: mockStorage}

	s.registerHybridReplicationActivities(commonActivity, hybridReplicationActivity)
	params, volume, backupVault, backup := s.createTestData()

	// Mock activity responses with error
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)
	mockStorage.On("CreateJob", mock.Anything, mock.Anything).Return(&datamodel.Job{BaseModel: datamodel.BaseModel{UUID: "test-job-uuid"}}, nil)
	mockStorage.On("GetNodesByPoolID", mock.Anything, mock.Anything).Return([]*datamodel.Node{{BaseModel: datamodel.BaseModel{UUID: "test-node-uuid"}}}, nil)
	s.env.OnActivity(hybridReplicationActivity.GetDstBasePathForHybridReplication, mock.Anything, mock.Anything).Return(&replication.CreateHybridReplicationResult{
		DestinationVolume:        volume,
		DestinationRegion:        params.Region,
		DestinationProjectNumber: params.AccountName,
	}, nil)
	s.env.OnActivity(hybridReplicationActivity.GetDstSignedTokenForHybridReplication, mock.Anything, mock.Anything).Return(&replication.CreateHybridReplicationResult{
		DestinationVolume:        volume,
		DestinationRegion:        params.Region,
		DestinationProjectNumber: params.AccountName,
	}, nil)
	s.env.OnActivity(hybridReplicationActivity.GetOrCreateClusterPeerForHybridReplication, mock.Anything, mock.Anything).Return(&replication.CreateHybridReplicationResult{
		DestinationVolume:        volume,
		DestinationRegion:        params.Region,
		DestinationProjectNumber: params.AccountName,
	}, nil)
	s.env.OnActivity(hybridReplicationActivity.CreateLocalHybridReplicationRow, mock.Anything, mock.Anything).Return(nil, errors.New("failed to create local volume replication row"))
	s.env.OnActivity(hybridReplicationActivity.UpdateClusterPeerDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(hybridReplicationActivity.UpdateReplicationRowDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateHybridReplicationWorkflow, params, volume, backupVault, backup)

	// Verify workflow completed successfully (child workflow error is ignored)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())

	// Verify status query works
	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)
}

func (s *HybridCreateWorkflowTestSuite) TestCreateHybridReplicationWorkflow_HydrateReplicationStateError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := &activities.CommonActivities{SE: mockStorage}
	hybridReplicationActivity := &replicationActivities.HybridReplicationActivity{SE: mockStorage}

	s.registerHybridReplicationActivities(commonActivity, hybridReplicationActivity)
	params, volume, backupVault, backup := s.createTestData()

	// Mock activity responses with error
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)
	mockStorage.On("CreateJob", mock.Anything, mock.Anything).Return(&datamodel.Job{BaseModel: datamodel.BaseModel{UUID: "test-job-uuid"}}, nil)
	mockStorage.On("GetNodesByPoolID", mock.Anything, mock.Anything).Return([]*datamodel.Node{{BaseModel: datamodel.BaseModel{UUID: "test-node-uuid"}}}, nil)
	s.env.OnActivity(hybridReplicationActivity.GetDstBasePathForHybridReplication, mock.Anything, mock.Anything).Return(&replication.CreateHybridReplicationResult{
		DestinationVolume:        volume,
		DestinationRegion:        params.Region,
		DestinationProjectNumber: params.AccountName,
	}, nil)
	s.env.OnActivity(hybridReplicationActivity.GetDstSignedTokenForHybridReplication, mock.Anything, mock.Anything).Return(&replication.CreateHybridReplicationResult{
		DestinationVolume:        volume,
		DestinationRegion:        params.Region,
		DestinationProjectNumber: params.AccountName,
	}, nil)
	s.env.OnActivity(hybridReplicationActivity.GetOrCreateClusterPeerForHybridReplication, mock.Anything, mock.Anything).Return(&replication.CreateHybridReplicationResult{
		DestinationVolume:        volume,
		DestinationRegion:        params.Region,
		DestinationProjectNumber: params.AccountName,
	}, nil)
	s.env.OnActivity(hybridReplicationActivity.CreateLocalHybridReplicationRow, mock.Anything, mock.Anything).Return(&replication.CreateHybridReplicationResult{
		DestinationVolume:        volume,
		DestinationRegion:        params.Region,
		DestinationProjectNumber: params.AccountName,
	}, nil)
	s.env.OnActivity(hybridReplicationActivity.HydrateReplicationSateForHybridReplication, mock.Anything, mock.Anything).Return(nil, errors.New("failed to hydrate replication state"))
	s.env.OnActivity(hybridReplicationActivity.UpdateClusterPeerDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(hybridReplicationActivity.UpdateReplicationRowDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateHybridReplicationWorkflow, params, volume, backupVault, backup)

	// Verify workflow completed successfully (child workflow error is ignored)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())

	// Verify status query works
	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)
}

func (s *HybridCreateWorkflowTestSuite) TestCreateHybridReplicationWorkflow_UpdateJobStatusError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := &activities.CommonActivities{SE: mockStorage}
	hybridReplicationActivity := &replicationActivities.HybridReplicationActivity{SE: mockStorage}

	s.registerHybridReplicationActivities(commonActivity, hybridReplicationActivity)
	params, volume, backupVault, backup := s.createTestData()

	// Mock activity responses with error
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(errors.New("failed to update job status"))

	// Execute workflow
	s.env.ExecuteWorkflow(CreateHybridReplicationWorkflow, params, volume, backupVault, backup)

	// Verify workflow failed due to UpdateJobStatus error
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())

	// Verify status query works
	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)
}

func (s *HybridCreateWorkflowTestSuite) TestCreateEstablishPeeringWorkflow_Success() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := &activities.CommonActivities{SE: mockStorage}
	hybridReplicationActivity := &replicationActivities.HybridReplicationActivity{SE: mockStorage}

	s.registerHybridReplicationActivities(commonActivity, hybridReplicationActivity)
	params, volume, _, _ := s.createTestData()

	// Create replication result
	replicationResult := &replication.CreateHybridReplicationResult{
		DestinationVolume:           volume,
		DestinationRegion:           params.Region,
		DestinationProjectNumber:    params.AccountName,
		HybridReplicationParameters: params.HybridReplicationParameters,
	}

	// Mock successful activity responses
	s.env.OnActivity(commonActivity.CreateJob, mock.Anything, mock.Anything).Return(&datamodel.Job{BaseModel: datamodel.BaseModel{UUID: "test-job-uuid"}}, nil)
	s.env.OnActivity(hybridReplicationActivity.CreateJobForHybridReplication, mock.Anything, mock.Anything, mock.Anything).Return(&datamodel.Job{BaseModel: datamodel.BaseModel{UUID: "test-job-uuid"}}, nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{BaseModel: datamodel.BaseModel{UUID: "test-node-uuid"}}}, nil)
	s.env.OnActivity(hybridReplicationActivity.GetDstBasePathForHybridReplication, mock.Anything, mock.Anything).Return(replicationResult, nil)
	s.env.OnActivity(hybridReplicationActivity.GetDstSignedTokenForHybridReplication, mock.Anything, mock.Anything).Return(replicationResult, nil)
	s.env.OnActivity(hybridReplicationActivity.GetNodeForHybridReplication, mock.Anything, mock.Anything).Return(replicationResult, nil)
	s.env.OnActivity(hybridReplicationActivity.CreateNodesForHybridReplication, mock.Anything, mock.Anything).Return(replicationResult, nil)
	s.env.OnActivity(hybridReplicationActivity.GetOrCreateClusterPeerForHybridReplication, mock.Anything, mock.Anything).Return(replicationResult, nil)
	s.env.OnActivity(hybridReplicationActivity.UpdateClusterPeeringInReplication, mock.Anything, mock.Anything).Return(replicationResult, nil)
	s.env.OnActivity(hybridReplicationActivity.GetOrCreateClusterPeerInOntapForHybridReplication, mock.Anything, mock.Anything).Return(replicationResult, nil)

	// Add mocks for CreateInternalEstablishWorkflow child workflow
	s.env.OnActivity(hybridReplicationActivity.HydrateReplicationSateForHybridReplication, mock.Anything, mock.Anything).Return(replicationResult, nil)
	s.env.OnActivity(hybridReplicationActivity.WaitForClusterPeerActivityForHybridReplication, mock.Anything, mock.Anything).Return(replicationResult, nil)
	s.env.OnActivity(hybridReplicationActivity.SetClusterPeeringStatusToPeeredForHybridReplication, mock.Anything, mock.Anything).Return(replicationResult, nil)
	s.env.OnActivity(hybridReplicationActivity.SetVolumeReplicationPeeringStatusToPendingSVMPeering, mock.Anything, mock.Anything).Return(replicationResult, nil)
	s.env.OnActivity(hybridReplicationActivity.CreateSVMPeerInOntapForHybridReplication, mock.Anything, mock.Anything).Return(replicationResult, nil)
	s.env.OnActivity(hybridReplicationActivity.SetVolumeReplicationSVMPeeringDetails, mock.Anything, mock.Anything).Return(replicationResult, nil)
	s.env.OnActivity(hybridReplicationActivity.WaitForSVMPeerForHybridReplication, mock.Anything, mock.Anything).Return(replicationResult, nil)
	s.env.OnActivity(hybridReplicationActivity.GetReplicationForHybridReplication, mock.Anything, mock.Anything).Return(replicationResult, nil)
	s.env.OnActivity(hybridReplicationActivity.CleanupReplicationIfNeeded, mock.Anything, mock.Anything).Return(replicationResult, nil)
	s.env.OnActivity(hybridReplicationActivity.CreateHybridVolumeReplicationInternal, mock.Anything, mock.Anything).Return(replicationResult, nil)
	s.env.OnActivity(hybridReplicationActivity.UpdateHybridVolumeReplicationDetailsAndSetPeeringStatusToPeered, mock.Anything, mock.Anything).Return(replicationResult, nil)
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(hybridReplicationActivity.UpdateClusterPeerDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(hybridReplicationActivity.UpdateReplicationRowDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(EstablishPeeringWorkflow, *replicationResult, volume)

	// Verify workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
}

func (s *HybridCreateWorkflowTestSuite) TestCreateInternalEstablishWorkflow_Success() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := &activities.CommonActivities{SE: mockStorage}
	hybridReplicationActivity := &replicationActivities.HybridReplicationActivity{SE: mockStorage}

	s.registerHybridReplicationActivities(commonActivity, hybridReplicationActivity)
	params, volume, _, _ := s.createTestData()

	// Create replication result
	replicationResult := &replication.CreateHybridReplicationResult{
		DestinationVolume:           volume,
		DestinationRegion:           params.Region,
		DestinationProjectNumber:    params.AccountName,
		HybridReplicationParameters: params.HybridReplicationParameters,
		ClusterPeeringRow: &datamodel.ClusterPeerings{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-cluster-peering-uuid"},
			ClusterPeeringAttributes: &datamodel.ClusterPeeringAttributes{
				ExpiryTime: nil, // Will use default timeout
			},
		},
	}

	// Mock successful activity responses
	s.env.OnActivity(commonActivity.CreateJob, mock.Anything, mock.Anything).Return(&datamodel.Job{BaseModel: datamodel.BaseModel{UUID: "test-job-uuid"}}, nil)
	s.env.OnActivity(hybridReplicationActivity.CreateJobForHybridReplication, mock.Anything, mock.Anything, mock.Anything).Return(&datamodel.Job{BaseModel: datamodel.BaseModel{UUID: "test-job-uuid"}}, nil)
	s.env.OnActivity(hybridReplicationActivity.HydrateReplicationSateForHybridReplication, mock.Anything, mock.Anything).Return(replicationResult, nil)
	s.env.OnActivity(hybridReplicationActivity.WaitForClusterPeerActivityForHybridReplication, mock.Anything, mock.Anything).Return(replicationResult, nil)
	s.env.OnActivity(hybridReplicationActivity.SetClusterPeeringStatusToPeeredForHybridReplication, mock.Anything, mock.Anything).Return(replicationResult, nil)
	s.env.OnActivity(hybridReplicationActivity.SetVolumeReplicationPeeringStatusToPendingSVMPeering, mock.Anything, mock.Anything).Return(replicationResult, nil)
	s.env.OnActivity(hybridReplicationActivity.CreateSVMPeerInOntapForHybridReplication, mock.Anything, mock.Anything).Return(replicationResult, nil)
	s.env.OnActivity(hybridReplicationActivity.SetVolumeReplicationSVMPeeringDetails, mock.Anything, mock.Anything).Return(replicationResult, nil)
	s.env.OnActivity(hybridReplicationActivity.WaitForSVMPeerForHybridReplication, mock.Anything, mock.Anything).Return(replicationResult, nil)
	s.env.OnActivity(hybridReplicationActivity.SetSVMPeeringToPeered, mock.Anything, mock.Anything).Return(replicationResult, nil)
	s.env.OnActivity(hybridReplicationActivity.GetReplicationForHybridReplication, mock.Anything, mock.Anything).Return(replicationResult, nil)
	s.env.OnActivity(hybridReplicationActivity.CleanupReplicationIfNeeded, mock.Anything, mock.Anything).Return(replicationResult, nil)
	s.env.OnActivity(hybridReplicationActivity.CreateHybridVolumeReplicationInternal, mock.Anything, mock.Anything).Return(replicationResult, nil)
	s.env.OnActivity(hybridReplicationActivity.UpdateHybridVolumeReplicationDetailsAndSetPeeringStatusToPeered, mock.Anything, mock.Anything).Return(replicationResult, nil)
	s.env.OnActivity(hybridReplicationActivity.DescribeJobForHybridReplicationWorkflow, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(InternalEstablishWorkflow, *replicationResult, volume)

	// Verify workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
}
