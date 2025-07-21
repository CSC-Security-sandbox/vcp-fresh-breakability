package backgroundworkflows

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

type ScheduledBackupsTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env *testsuite.TestWorkflowEnvironment
}

func (s *ScheduledBackupsTestSuite) SetupTest() {
	s.env = s.NewTestWorkflowEnvironment()
	s.env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	s.env.RegisterWorkflow(CreateScheduledBackupInitWorkflow)
	s.env.RegisterWorkflow(CreateScheduledBackupWorkflow)
	s.env.RegisterWorkflow(DeleteScheduledBackupWorkflow)
}

func (s *ScheduledBackupsTestSuite) AfterTest() {
	s.env.AssertExpectations(s.T())
}

func TestScheduledBackupsTestSuite(t *testing.T) {
	suite.Run(t, new(ScheduledBackupsTestSuite))
}

func (s *ScheduledBackupsTestSuite) TestCreateScheduledBackupInitWorkflow_Success() {
	mockStorage := database.NewMockStorage(s.T())
	scheduledBackupActivity := &backgroundactivities.ScheduledBackupActivity{SE: mockStorage}

	s.env.RegisterActivity(scheduledBackupActivity.GetVolumesByBackupPolicyUUID)

	volumes := []*datamodel.Volume{
		{
			BaseModel: datamodel.BaseModel{UUID: "volume-uuid-1"},
			Name:      "test-volume-1",
		},
		{
			BaseModel: datamodel.BaseModel{UUID: "volume-uuid-2"},
			Name:      "test-volume-2",
		},
	}
	s.env.OnActivity(scheduledBackupActivity.GetVolumesByBackupPolicyUUID, mock.Anything, mock.Anything, mock.Anything).
		Return(volumes, nil)
	s.env.OnWorkflow(CreateScheduledBackupWorkflow, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	backupPolicy := &datamodel.BackupPolicy{
		BaseModel: datamodel.BaseModel{UUID: "backup-policy-uuid"},
		AccountID: 1,
	}
	s.env.ExecuteWorkflow(CreateScheduledBackupInitWorkflow, backupPolicy)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NoError(s.T(), s.env.GetWorkflowError())
}

func (s *ScheduledBackupsTestSuite) TestCreateScheduledBackupInitWorkflow_GetVolumesByBackupPolicyUUIDFailure() {
	mockStorage := database.NewMockStorage(s.T())
	scheduledBackupActivity := &backgroundactivities.ScheduledBackupActivity{SE: mockStorage}

	s.env.RegisterActivity(scheduledBackupActivity.GetVolumesByBackupPolicyUUID)
	s.env.OnActivity(scheduledBackupActivity.GetVolumesByBackupPolicyUUID, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, errors.New("could not fetch volumes attached to the backup policy"))

	backupPolicy := &datamodel.BackupPolicy{
		BaseModel: datamodel.BaseModel{UUID: "backup-policy-uuid"},
		AccountID: 1,
	}
	s.env.ExecuteWorkflow(CreateScheduledBackupInitWorkflow, backupPolicy)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())

	var activityError *temporal.ActivityError
	if errors.As(s.env.GetWorkflowError(), &activityError) {
		assert.Equal(s.T(), "could not fetch volumes attached to the backup policy", activityError.Unwrap().Error())
	} else {
		assert.Fail(s.T(), "Expected ActivityError but got: %v", s.env.GetWorkflowError())
	}
}

func (s *ScheduledBackupsTestSuite) TestCreateScheduledBackupWorkflow_Success() {
	scheduledWeeklyBackupDay = int(time.Now().Weekday())
	scheduledMonthlyBackupDay = time.Now().Day()

	mockStorage := database.NewMockStorage(s.T())
	commonActivity := &activities.CommonActivities{SE: mockStorage}
	backupActivity := &activities.BackupActivity{SE: mockStorage}
	scheduledBackupActivity := &backgroundactivities.ScheduledBackupActivity{SE: mockStorage}

	s.env.RegisterActivity(commonActivity.GetNode)
	s.env.RegisterActivity(backupActivity.GetBackupVault)
	s.env.RegisterActivity(backupActivity.GetOrCreateObjectStore)
	s.env.RegisterActivity(backupActivity.SnapmirrorGetorCreate)
	s.env.RegisterActivity(backupActivity.SnapshotCreate)
	s.env.RegisterActivity(backupActivity.SnapmirrorTransfer)
	s.env.RegisterActivity(backupActivity.GetSnapmirrorTransferStatus)
	s.env.RegisterActivity(backupActivity.FinishBackup)
	s.env.RegisterActivity(scheduledBackupActivity.CreateScheduledBackup)
	s.env.RegisterActivity(scheduledBackupActivity.GenerateScheduledSnapshotName)
	s.env.RegisterActivity(scheduledBackupActivity.HydrateCreatedBackupsToCCFE)

	s.env.OnActivity(backupActivity.GetBackupVault, mock.Anything, mock.Anything).
		Return(&datamodel.BackupVault{
			Name: "test-backup-vault",
			BucketDetails: []*datamodel.BucketDetails{
				{
					BucketName:         "vsa-backup-bucket",
					VendorSubnetID:     "test-vendor-subnet-id",
					ServiceAccountName: "test-service-account",
				},
			},
		}, nil)
	s.env.OnActivity(scheduledBackupActivity.CreateScheduledBackup, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&datamodel.Backup{Attributes: &datamodel.BackupAttributes{}}, nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).
		Return([]*datamodel.Node{
			{
				EndpointAddress: "0.0.0.0",
			},
		}, nil)
	s.env.OnActivity(scheduledBackupActivity.GenerateScheduledSnapshotName, mock.Anything, mock.Anything).
		Return("scheduled-snapshot-name", nil)
	s.env.OnActivity(backupActivity.GetOrCreateObjectStore, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&common.CloudTarget{
			Name: "vsa-backup-bucket",
		}, nil)

	destinationUUID := "test-destination-uuid-1"
	s.env.OnActivity(backupActivity.SnapmirrorGetorCreate, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&common.SnapmirrorRelationship{
			UUID:            "test-uuid-1",
			DestinationUUID: &destinationUUID,
		}, nil)
	s.env.OnActivity(backupActivity.SnapshotCreate, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&vsa.SnapshotProviderResponse{
			ProviderResponse: vsa.ProviderResponse{
				ExternalUUID: "test-uuid-1",
			},
		}, nil)
	s.env.OnActivity(backupActivity.SnapmirrorTransfer, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupActivity.GetSnapmirrorTransferStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(activities.SmStatusTransferring, nil).Once()
	s.env.OnActivity(backupActivity.GetSnapmirrorTransferStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(activities.SmStatusSuccess, nil).Once()
	s.env.OnActivity(backupActivity.FinishBackup, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(scheduledBackupActivity.HydrateCreatedBackupsToCCFE, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnWorkflow(DeleteScheduledBackupWorkflow, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "volume-uuid-1",
		},
		Name: "test-volume-1",
		Svm: &datamodel.Svm{
			Name: "test-svm-1",
		},
		PoolID: 1,
		Pool: &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				Password: "pool-password",
				SecretID: "pool-credential-secret-id",
			},
			DeploymentName: "test-pool-deployment",
		},
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "backup-vault-uuid-1",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID:   "external-uuid-1",
			VendorSubnetID: "test-vendor-subnet-id",
		},
	}
	backupPolicy := &datamodel.BackupPolicy{
		DailyBackupsToKeep:   3,
		WeeklyBackupsToKeep:  1,
		MonthlyBackupsToKeep: 1,
	}
	s.env.ExecuteWorkflow(CreateScheduledBackupWorkflow, volume, backupPolicy)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NoError(s.T(), s.env.GetWorkflowError())
}

func (s *ScheduledBackupsTestSuite) TestCreateScheduledBackupWorkflow_GetBackupVaultFailure() {
	scheduledWeeklyBackupDay = int(time.Now().Weekday())
	scheduledMonthlyBackupDay = time.Now().Day()

	mockStorage := database.NewMockStorage(s.T())
	backupActivity := &activities.BackupActivity{SE: mockStorage}

	s.env.RegisterActivity(backupActivity.GetBackupVault)

	s.env.OnActivity(backupActivity.GetBackupVault, mock.Anything, mock.Anything).
		Return(nil, errors.New("could not fetch backup vault"))

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "volume-uuid-1",
		},
		Name: "test-volume-1",
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "backup-vault-uuid-1",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID:   "external-uuid-1",
			VendorSubnetID: "test-vendor-subnet-id",
		},
	}
	backupPolicy := &datamodel.BackupPolicy{
		DailyBackupsToKeep:   3,
		WeeklyBackupsToKeep:  1,
		MonthlyBackupsToKeep: 1,
	}
	s.env.ExecuteWorkflow(CreateScheduledBackupWorkflow, volume, backupPolicy)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())

	var activityError *temporal.ActivityError
	if errors.As(s.env.GetWorkflowError(), &activityError) {
		assert.Equal(s.T(), "could not fetch backup vault", activityError.Unwrap().Error())
	} else {
		assert.Fail(s.T(), "Expected ActivityError but got: %v", s.env.GetWorkflowError())
	}
}

func (s *ScheduledBackupsTestSuite) TestCreateScheduledBackupWorkflow_DailyScheduledBackupFailure() {
	scheduledWeeklyBackupDay = int(time.Now().Weekday())
	scheduledMonthlyBackupDay = time.Now().Day()

	mockStorage := database.NewMockStorage(s.T())
	backupActivity := &activities.BackupActivity{SE: mockStorage}
	scheduledBackupActivity := &backgroundactivities.ScheduledBackupActivity{SE: mockStorage}

	s.env.RegisterActivity(backupActivity.GetBackupVault)
	s.env.RegisterActivity(scheduledBackupActivity.CreateScheduledBackup)

	s.env.OnActivity(backupActivity.GetBackupVault, mock.Anything, mock.Anything).
		Return(&datamodel.BackupVault{
			Name: "test-backup-vault",
			BucketDetails: []*datamodel.BucketDetails{
				{
					BucketName:         "vsa-backup-bucket",
					VendorSubnetID:     "test-vendor-subnet-id",
					ServiceAccountName: "test-service-account",
				},
			},
		}, nil)
	s.env.OnActivity(scheduledBackupActivity.CreateScheduledBackup, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, errors.New("could not create daily scheduled backup"))

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "volume-uuid-1",
		},
		Name: "test-volume-1",
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "backup-vault-uuid-1",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID:   "external-uuid-1",
			VendorSubnetID: "test-vendor-subnet-id",
		},
	}
	backupPolicy := &datamodel.BackupPolicy{
		DailyBackupsToKeep:   3,
		WeeklyBackupsToKeep:  1,
		MonthlyBackupsToKeep: 1,
	}
	s.env.ExecuteWorkflow(CreateScheduledBackupWorkflow, volume, backupPolicy)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())

	var activityError *temporal.ActivityError
	if errors.As(s.env.GetWorkflowError(), &activityError) {
		assert.Equal(s.T(), "could not create daily scheduled backup", activityError.Unwrap().Error())
	} else {
		assert.Fail(s.T(), "Expected ActivityError but got: %v", s.env.GetWorkflowError())
	}
}

func (s *ScheduledBackupsTestSuite) TestCreateScheduledBackupWorkflow_WeeklyScheduledBackupFailure() {
	scheduledWeeklyBackupDay = int(time.Now().Weekday())
	scheduledMonthlyBackupDay = time.Now().Day()

	mockStorage := database.NewMockStorage(s.T())
	backupActivity := &activities.BackupActivity{SE: mockStorage}
	scheduledBackupActivity := &backgroundactivities.ScheduledBackupActivity{SE: mockStorage}

	s.env.RegisterActivity(backupActivity.GetBackupVault)
	s.env.RegisterActivity(scheduledBackupActivity.CreateScheduledBackup)

	s.env.OnActivity(backupActivity.GetBackupVault, mock.Anything, mock.Anything).
		Return(&datamodel.BackupVault{
			Name: "test-backup-vault",
			BucketDetails: []*datamodel.BucketDetails{
				{
					BucketName:         "vsa-backup-bucket",
					VendorSubnetID:     "test-vendor-subnet-id",
					ServiceAccountName: "test-service-account",
				},
			},
		}, nil)
	s.env.OnActivity(scheduledBackupActivity.CreateScheduledBackup, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&datamodel.Backup{Attributes: &datamodel.BackupAttributes{}}, nil).Once()
	s.env.OnActivity(scheduledBackupActivity.CreateScheduledBackup, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, errors.New("could not create weekly scheduled backup")).Times(3)

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "volume-uuid-1",
		},
		Name: "test-volume-1",
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "backup-vault-uuid-1",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID:   "external-uuid-1",
			VendorSubnetID: "test-vendor-subnet-id",
		},
	}
	backupPolicy := &datamodel.BackupPolicy{
		DailyBackupsToKeep:   3,
		WeeklyBackupsToKeep:  1,
		MonthlyBackupsToKeep: 1,
	}
	s.env.ExecuteWorkflow(CreateScheduledBackupWorkflow, volume, backupPolicy)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())

	var activityError *temporal.ActivityError
	if errors.As(s.env.GetWorkflowError(), &activityError) {
		assert.Equal(s.T(), "could not create weekly scheduled backup", activityError.Unwrap().Error())
	} else {
		assert.Fail(s.T(), "Expected ActivityError but got: %v", s.env.GetWorkflowError())
	}
}

func (s *ScheduledBackupsTestSuite) TestCreateScheduledBackupWorkflow_MonthlyScheduledBackupFailure() {
	originalScheduledWeeklyBackupDay := scheduledWeeklyBackupDay
	originalScheduledMonthlyBackupDay := scheduledMonthlyBackupDay
	defer func() {
		scheduledWeeklyBackupDay = originalScheduledWeeklyBackupDay
		scheduledMonthlyBackupDay = originalScheduledMonthlyBackupDay
	}()

	scheduledWeeklyBackupDay = int(time.Now().Weekday())
	scheduledMonthlyBackupDay = time.Now().Day()

	mockStorage := database.NewMockStorage(s.T())
	backupActivity := &activities.BackupActivity{SE: mockStorage}
	scheduledBackupActivity := &backgroundactivities.ScheduledBackupActivity{SE: mockStorage}

	s.env.RegisterActivity(backupActivity.GetBackupVault)
	s.env.RegisterActivity(scheduledBackupActivity.CreateScheduledBackup)

	s.env.OnActivity(backupActivity.GetBackupVault, mock.Anything, mock.Anything).
		Return(&datamodel.BackupVault{
			Name: "test-backup-vault",
			BucketDetails: []*datamodel.BucketDetails{
				{
					BucketName:         "vsa-backup-bucket",
					VendorSubnetID:     "test-vendor-subnet-id",
					ServiceAccountName: "test-service-account",
				},
			},
		}, nil)
	s.env.OnActivity(scheduledBackupActivity.CreateScheduledBackup, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&datamodel.Backup{Attributes: &datamodel.BackupAttributes{}}, nil).Twice()
	s.env.OnActivity(scheduledBackupActivity.CreateScheduledBackup, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, errors.New("could not create monthly scheduled backup")).Times(3)

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "volume-uuid-1",
		},
		Name: "test-volume-1",
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "backup-vault-uuid-1",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID:   "external-uuid-1",
			VendorSubnetID: "test-vendor-subnet-id",
		},
	}
	backupPolicy := &datamodel.BackupPolicy{
		DailyBackupsToKeep:   3,
		WeeklyBackupsToKeep:  1,
		MonthlyBackupsToKeep: 1,
	}
	s.env.ExecuteWorkflow(CreateScheduledBackupWorkflow, volume, backupPolicy)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())

	var activityError *temporal.ActivityError
	if errors.As(s.env.GetWorkflowError(), &activityError) {
		assert.Equal(s.T(), "could not create monthly scheduled backup", activityError.Unwrap().Error())
	} else {
		assert.Fail(s.T(), "Expected ActivityError but got: %v", s.env.GetWorkflowError())
	}
}

func (s *ScheduledBackupsTestSuite) TestCreateScheduledBackupWorkflow_NoBackupsToBeCreated() {
	originalScheduledWeeklyBackupDay := scheduledWeeklyBackupDay
	originalScheduledMonthlyBackupDay := scheduledMonthlyBackupDay
	defer func() {
		scheduledWeeklyBackupDay = originalScheduledWeeklyBackupDay
		scheduledMonthlyBackupDay = originalScheduledMonthlyBackupDay
	}()

	scheduledWeeklyBackupDay = int(time.Now().Weekday()) + 1 // Set to a different day to ensure no weekly backup is not created
	scheduledMonthlyBackupDay = time.Now().Day() + 1         // Set to a different day to ensure no monthly backup is created

	mockStorage := database.NewMockStorage(s.T())
	backupActivity := &activities.BackupActivity{SE: mockStorage}
	scheduledBackupActivity := &backgroundactivities.ScheduledBackupActivity{SE: mockStorage}

	s.env.RegisterActivity(backupActivity.GetBackupVault)
	s.env.RegisterActivity(scheduledBackupActivity.CreateScheduledBackup)

	s.env.OnActivity(backupActivity.GetBackupVault, mock.Anything, mock.Anything).
		Return(&datamodel.BackupVault{
			Name: "test-backup-vault",
			BucketDetails: []*datamodel.BucketDetails{
				{
					BucketName:         "vsa-backup-bucket",
					VendorSubnetID:     "test-vendor-subnet-id",
					ServiceAccountName: "test-service-account",
				},
			},
		}, nil)
	s.env.OnActivity(scheduledBackupActivity.CreateScheduledBackup, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&datamodel.Backup{Attributes: &datamodel.BackupAttributes{}}, nil)

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "volume-uuid-1",
		},
		Name: "test-volume-1",
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "backup-vault-uuid-1",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID:   "external-uuid-1",
			VendorSubnetID: "test-vendor-subnet-id",
		},
	}
	backupPolicy := &datamodel.BackupPolicy{
		DailyBackupsToKeep:   0,
		WeeklyBackupsToKeep:  1,
		MonthlyBackupsToKeep: 1,
	}
	s.env.ExecuteWorkflow(CreateScheduledBackupWorkflow, volume, backupPolicy)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NoError(s.T(), s.env.GetWorkflowError())
}

func (s *ScheduledBackupsTestSuite) TestCreateScheduledBackupWorkflow_GetNodeFailure() {
	scheduledWeeklyBackupDay = int(time.Now().Weekday())
	scheduledMonthlyBackupDay = time.Now().Day()

	mockStorage := database.NewMockStorage(s.T())
	commonActivity := &activities.CommonActivities{SE: mockStorage}
	backupActivity := &activities.BackupActivity{SE: mockStorage}
	scheduledBackupActivity := &backgroundactivities.ScheduledBackupActivity{SE: mockStorage}

	s.env.RegisterActivity(commonActivity.GetNode)
	s.env.RegisterActivity(backupActivity.GetBackupVault)
	s.env.RegisterActivity(scheduledBackupActivity.CreateScheduledBackup)

	s.env.OnActivity(backupActivity.GetBackupVault, mock.Anything, mock.Anything).
		Return(&datamodel.BackupVault{
			Name: "test-backup-vault",
			BucketDetails: []*datamodel.BucketDetails{
				{
					BucketName:         "vsa-backup-bucket",
					VendorSubnetID:     "test-vendor-subnet-id",
					ServiceAccountName: "test-service-account",
				},
			},
		}, nil)
	s.env.OnActivity(scheduledBackupActivity.CreateScheduledBackup, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&datamodel.Backup{Attributes: &datamodel.BackupAttributes{}}, nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).
		Return(nil, errors.New("could not fetch nodes of the cluster"))

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "volume-uuid-1",
		},
		Name: "test-volume-1",
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "backup-vault-uuid-1",
		},
		PoolID: 1,
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID:   "external-uuid-1",
			VendorSubnetID: "test-vendor-subnet-id",
		},
	}
	backupPolicy := &datamodel.BackupPolicy{
		DailyBackupsToKeep:   3,
		WeeklyBackupsToKeep:  1,
		MonthlyBackupsToKeep: 1,
	}
	s.env.ExecuteWorkflow(CreateScheduledBackupWorkflow, volume, backupPolicy)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())

	var activityError *temporal.ActivityError
	if errors.As(s.env.GetWorkflowError(), &activityError) {
		assert.Equal(s.T(), "could not fetch nodes of the cluster", activityError.Unwrap().Error())
	} else {
		assert.Fail(s.T(), "Expected ActivityError but got: %v", s.env.GetWorkflowError())
	}
}

func (s *ScheduledBackupsTestSuite) TestCreateScheduledBackupWorkflow_GenerateSnapshotNameFailure() {
	scheduledWeeklyBackupDay = int(time.Now().Weekday())
	scheduledMonthlyBackupDay = time.Now().Day()

	mockStorage := database.NewMockStorage(s.T())
	commonActivity := &activities.CommonActivities{SE: mockStorage}
	backupActivity := &activities.BackupActivity{SE: mockStorage}
	scheduledBackupActivity := &backgroundactivities.ScheduledBackupActivity{SE: mockStorage}

	s.env.RegisterActivity(commonActivity.GetNode)
	s.env.RegisterActivity(backupActivity.GetBackupVault)
	s.env.RegisterActivity(scheduledBackupActivity.CreateScheduledBackup)
	s.env.RegisterActivity(scheduledBackupActivity.GenerateScheduledSnapshotName)

	s.env.OnActivity(backupActivity.GetBackupVault, mock.Anything, mock.Anything).
		Return(&datamodel.BackupVault{
			Name: "test-backup-vault",
			BucketDetails: []*datamodel.BucketDetails{
				{
					BucketName:         "vsa-backup-bucket",
					VendorSubnetID:     "test-vendor-subnet-id",
					ServiceAccountName: "test-service-account",
				},
			},
		}, nil)
	s.env.OnActivity(scheduledBackupActivity.CreateScheduledBackup, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&datamodel.Backup{Attributes: &datamodel.BackupAttributes{}}, nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).
		Return([]*datamodel.Node{
			{
				EndpointAddress: "0.0.0.0",
			},
		}, nil)
	s.env.OnActivity(scheduledBackupActivity.GenerateScheduledSnapshotName, mock.Anything, mock.Anything).
		Return("", errors.New("could not generate snapshot name"))

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "volume-uuid-1",
		},
		Name: "test-volume-1",
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "backup-vault-uuid-1",
		},
		PoolID: 1,
		Pool: &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				Password: "pool-password",
				SecretID: "pool-credential-secret-id",
			},
			DeploymentName: "test-pool-deployment",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID:   "external-uuid-1",
			VendorSubnetID: "test-vendor-subnet-id",
		},
	}
	backupPolicy := &datamodel.BackupPolicy{
		DailyBackupsToKeep:   3,
		WeeklyBackupsToKeep:  1,
		MonthlyBackupsToKeep: 1,
	}
	s.env.ExecuteWorkflow(CreateScheduledBackupWorkflow, volume, backupPolicy)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())

	var activityError *temporal.ActivityError
	if errors.As(s.env.GetWorkflowError(), &activityError) {
		assert.Equal(s.T(), "could not generate snapshot name", activityError.Unwrap().Error())
	} else {
		assert.Fail(s.T(), "Expected ActivityError but got: %v", s.env.GetWorkflowError())
	}
}

func (s *ScheduledBackupsTestSuite) TestCreateScheduledBackupWorkflow_ObjectStoreBucketDetailsNotFound() {
	scheduledWeeklyBackupDay = int(time.Now().Weekday())
	scheduledMonthlyBackupDay = time.Now().Day()

	mockStorage := database.NewMockStorage(s.T())
	commonActivity := &activities.CommonActivities{SE: mockStorage}
	backupActivity := &activities.BackupActivity{SE: mockStorage}
	scheduledBackupActivity := &backgroundactivities.ScheduledBackupActivity{SE: mockStorage}

	s.env.RegisterActivity(commonActivity.GetNode)
	s.env.RegisterActivity(backupActivity.GetBackupVault)
	s.env.RegisterActivity(scheduledBackupActivity.CreateScheduledBackup)
	s.env.RegisterActivity(scheduledBackupActivity.GenerateScheduledSnapshotName)

	s.env.OnActivity(backupActivity.GetBackupVault, mock.Anything, mock.Anything).
		Return(&datamodel.BackupVault{
			Name: "test-backup-vault",
			BucketDetails: []*datamodel.BucketDetails{
				{
					BucketName:         "vsa-backup-bucket",
					VendorSubnetID:     "test-vendor-subnet-id-1",
					ServiceAccountName: "test-service-account",
				},
			},
		}, nil)
	s.env.OnActivity(scheduledBackupActivity.CreateScheduledBackup, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&datamodel.Backup{Attributes: &datamodel.BackupAttributes{}}, nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).
		Return([]*datamodel.Node{
			{
				EndpointAddress: "0.0.0.0",
			},
		}, nil)
	s.env.OnActivity(scheduledBackupActivity.GenerateScheduledSnapshotName, mock.Anything, mock.Anything).
		Return("scheduled-snapshot-name", nil)

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "volume-uuid-1",
		},
		Name:   "test-volume-1",
		PoolID: 1,
		Pool: &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				Password: "pool-password",
				SecretID: "pool-credential-secret-id",
			},
			DeploymentName: "test-pool-deployment",
		},
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "backup-vault-uuid-1",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID:   "external-uuid-1",
			VendorSubnetID: "test-vendor-subnet-id-2",
		},
	}
	backupPolicy := &datamodel.BackupPolicy{
		DailyBackupsToKeep:   3,
		WeeklyBackupsToKeep:  1,
		MonthlyBackupsToKeep: 1,
	}
	s.env.ExecuteWorkflow(CreateScheduledBackupWorkflow, volume, backupPolicy)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
}

func (s *ScheduledBackupsTestSuite) TestCreateScheduledBackupWorkflow_GetOrCreateObjectStoreFailure() {
	scheduledWeeklyBackupDay = int(time.Now().Weekday())
	scheduledMonthlyBackupDay = time.Now().Day()

	mockStorage := database.NewMockStorage(s.T())
	commonActivity := &activities.CommonActivities{SE: mockStorage}
	backupActivity := &activities.BackupActivity{SE: mockStorage}
	scheduledBackupActivity := &backgroundactivities.ScheduledBackupActivity{SE: mockStorage}

	s.env.RegisterActivity(commonActivity.GetNode)
	s.env.RegisterActivity(backupActivity.GetBackupVault)
	s.env.RegisterActivity(backupActivity.GetOrCreateObjectStore)
	s.env.RegisterActivity(scheduledBackupActivity.CreateScheduledBackup)
	s.env.RegisterActivity(scheduledBackupActivity.GenerateScheduledSnapshotName)

	s.env.OnActivity(backupActivity.GetBackupVault, mock.Anything, mock.Anything).
		Return(&datamodel.BackupVault{
			Name: "test-backup-vault",
			BucketDetails: []*datamodel.BucketDetails{
				{
					BucketName:         "vsa-backup-bucket",
					VendorSubnetID:     "test-vendor-subnet-id",
					ServiceAccountName: "test-service-account",
				},
			},
		}, nil)
	s.env.OnActivity(scheduledBackupActivity.CreateScheduledBackup, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&datamodel.Backup{Attributes: &datamodel.BackupAttributes{}}, nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).
		Return([]*datamodel.Node{
			{
				EndpointAddress: "0.0.0.0",
			},
		}, nil)
	s.env.OnActivity(scheduledBackupActivity.GenerateScheduledSnapshotName, mock.Anything, mock.Anything).
		Return("scheduled-snapshot-name", nil)
	s.env.OnActivity(backupActivity.GetOrCreateObjectStore, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, errors.New("could not get or create cloud target"))

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "volume-uuid-1",
		},
		Name: "test-volume-1",
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "backup-vault-uuid-1",
		},
		PoolID: 1,
		Pool: &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				Password: "pool-password",
				SecretID: "pool-credential-secret-id",
			},
			DeploymentName: "test-pool-deployment",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID:   "external-uuid-1",
			VendorSubnetID: "test-vendor-subnet-id",
		},
	}
	backupPolicy := &datamodel.BackupPolicy{
		DailyBackupsToKeep:   3,
		WeeklyBackupsToKeep:  1,
		MonthlyBackupsToKeep: 1,
	}
	s.env.ExecuteWorkflow(CreateScheduledBackupWorkflow, volume, backupPolicy)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())

	var activityError *temporal.ActivityError
	if errors.As(s.env.GetWorkflowError(), &activityError) {
		assert.Equal(s.T(), "could not get or create cloud target", activityError.Unwrap().Error())
	} else {
		assert.Fail(s.T(), "Expected ActivityError but got: %v", s.env.GetWorkflowError())
	}
}

func (s *ScheduledBackupsTestSuite) TestCreateScheduledBackupWorkflow_SnapmirrorGetOrCreateFailure() {
	scheduledWeeklyBackupDay = int(time.Now().Weekday())
	scheduledMonthlyBackupDay = time.Now().Day()

	mockStorage := database.NewMockStorage(s.T())
	commonActivity := &activities.CommonActivities{SE: mockStorage}
	backupActivity := &activities.BackupActivity{SE: mockStorage}
	scheduledBackupActivity := &backgroundactivities.ScheduledBackupActivity{SE: mockStorage}

	s.env.RegisterActivity(commonActivity.GetNode)
	s.env.RegisterActivity(backupActivity.GetBackupVault)
	s.env.RegisterActivity(backupActivity.GetOrCreateObjectStore)
	s.env.RegisterActivity(backupActivity.SnapmirrorGetorCreate)
	s.env.RegisterActivity(scheduledBackupActivity.CreateScheduledBackup)
	s.env.RegisterActivity(scheduledBackupActivity.GenerateScheduledSnapshotName)

	s.env.OnActivity(backupActivity.GetBackupVault, mock.Anything, mock.Anything).
		Return(&datamodel.BackupVault{
			Name: "test-backup-vault",
			BucketDetails: []*datamodel.BucketDetails{
				{
					BucketName:         "vsa-backup-bucket",
					VendorSubnetID:     "test-vendor-subnet-id",
					ServiceAccountName: "test-service-account",
				},
			},
		}, nil)
	s.env.OnActivity(scheduledBackupActivity.CreateScheduledBackup, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&datamodel.Backup{Attributes: &datamodel.BackupAttributes{}}, nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).
		Return([]*datamodel.Node{
			{
				EndpointAddress: "0.0.0.0",
			},
		}, nil)
	s.env.OnActivity(scheduledBackupActivity.GenerateScheduledSnapshotName, mock.Anything, mock.Anything).
		Return("scheduled-snapshot-name", nil)
	s.env.OnActivity(backupActivity.GetOrCreateObjectStore, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&common.CloudTarget{
			Name: "vsa-backup-bucket",
		}, nil)

	s.env.OnActivity(backupActivity.SnapmirrorGetorCreate, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, errors.New("could not get or create snapmirror relationship"))

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "volume-uuid-1",
		},
		Name:   "test-volume-1",
		PoolID: 1,
		Pool: &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				Password: "pool-password",
				SecretID: "pool-credential-secret-id",
			},
			DeploymentName: "test-pool-deployment",
		},
		Svm: &datamodel.Svm{
			Name: "test-svm-1",
		},
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "backup-vault-uuid-1",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID:   "external-uuid-1",
			VendorSubnetID: "test-vendor-subnet-id",
		},
	}
	backupPolicy := &datamodel.BackupPolicy{
		DailyBackupsToKeep:   3,
		WeeklyBackupsToKeep:  1,
		MonthlyBackupsToKeep: 1,
	}
	s.env.ExecuteWorkflow(CreateScheduledBackupWorkflow, volume, backupPolicy)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())

	var activityError *temporal.ActivityError
	if errors.As(s.env.GetWorkflowError(), &activityError) {
		assert.Equal(s.T(), "could not get or create snapmirror relationship", activityError.Unwrap().Error())
	} else {
		assert.Fail(s.T(), "Expected ActivityError but got: %v", s.env.GetWorkflowError())
	}
}

func (s *ScheduledBackupsTestSuite) TestCreateScheduledBackupWorkflow_SnapshotCreateFailure() {
	scheduledWeeklyBackupDay = int(time.Now().Weekday())
	scheduledMonthlyBackupDay = time.Now().Day()

	mockStorage := database.NewMockStorage(s.T())
	commonActivity := &activities.CommonActivities{SE: mockStorage}
	backupActivity := &activities.BackupActivity{SE: mockStorage}
	scheduledBackupActivity := &backgroundactivities.ScheduledBackupActivity{SE: mockStorage}

	s.env.RegisterActivity(commonActivity.GetNode)
	s.env.RegisterActivity(backupActivity.GetBackupVault)
	s.env.RegisterActivity(backupActivity.GetOrCreateObjectStore)
	s.env.RegisterActivity(backupActivity.SnapmirrorGetorCreate)
	s.env.RegisterActivity(backupActivity.SnapshotCreate)
	s.env.RegisterActivity(scheduledBackupActivity.CreateScheduledBackup)
	s.env.RegisterActivity(scheduledBackupActivity.GenerateScheduledSnapshotName)

	s.env.OnActivity(backupActivity.GetBackupVault, mock.Anything, mock.Anything).
		Return(&datamodel.BackupVault{
			Name: "test-backup-vault",
			BucketDetails: []*datamodel.BucketDetails{
				{
					BucketName:         "vsa-backup-bucket",
					VendorSubnetID:     "test-vendor-subnet-id",
					ServiceAccountName: "test-service-account",
				},
			},
		}, nil)
	s.env.OnActivity(scheduledBackupActivity.CreateScheduledBackup, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&datamodel.Backup{Attributes: &datamodel.BackupAttributes{}}, nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).
		Return([]*datamodel.Node{
			{
				EndpointAddress: "0.0.0.0",
			},
		}, nil)
	s.env.OnActivity(scheduledBackupActivity.GenerateScheduledSnapshotName, mock.Anything, mock.Anything).
		Return("scheduled-snapshot-name", nil)
	s.env.OnActivity(backupActivity.GetOrCreateObjectStore, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&common.CloudTarget{
			Name: "vsa-backup-bucket",
		}, nil)

	destinationUUID := "test-destination-uuid-1"
	s.env.OnActivity(backupActivity.SnapmirrorGetorCreate, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&common.SnapmirrorRelationship{
			UUID:            "test-uuid-1",
			DestinationUUID: &destinationUUID,
		}, nil)
	s.env.OnActivity(backupActivity.SnapshotCreate, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, errors.New("could not create snapshot"))

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "volume-uuid-1",
		},
		Name: "test-volume-1",
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "backup-vault-uuid-1",
		},
		PoolID: 1,
		Pool: &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				Password: "pool-password",
				SecretID: "pool-credential-secret-id",
			},
			DeploymentName: "test-pool-deployment",
		},
		Svm: &datamodel.Svm{
			Name: "test-svm-1",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID:   "external-uuid-1",
			VendorSubnetID: "test-vendor-subnet-id",
		},
	}
	backupPolicy := &datamodel.BackupPolicy{
		DailyBackupsToKeep:   3,
		WeeklyBackupsToKeep:  1,
		MonthlyBackupsToKeep: 1,
	}
	s.env.ExecuteWorkflow(CreateScheduledBackupWorkflow, volume, backupPolicy)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())

	var activityError *temporal.ActivityError
	if errors.As(s.env.GetWorkflowError(), &activityError) {
		assert.Equal(s.T(), "could not create snapshot", activityError.Unwrap().Error())
	} else {
		assert.Fail(s.T(), "Expected ActivityError but got: %v", s.env.GetWorkflowError())
	}
}

func (s *ScheduledBackupsTestSuite) TestCreateScheduledBackupWorkflow_SnapmirrorTransferFailure() {
	scheduledWeeklyBackupDay = int(time.Now().Weekday())
	scheduledMonthlyBackupDay = time.Now().Day()

	mockStorage := database.NewMockStorage(s.T())
	commonActivity := &activities.CommonActivities{SE: mockStorage}
	backupActivity := &activities.BackupActivity{SE: mockStorage}
	scheduledBackupActivity := &backgroundactivities.ScheduledBackupActivity{SE: mockStorage}

	s.env.RegisterActivity(commonActivity.GetNode)
	s.env.RegisterActivity(backupActivity.GetBackupVault)
	s.env.RegisterActivity(backupActivity.GetOrCreateObjectStore)
	s.env.RegisterActivity(backupActivity.SnapmirrorGetorCreate)
	s.env.RegisterActivity(backupActivity.SnapshotCreate)
	s.env.RegisterActivity(backupActivity.SnapmirrorTransfer)
	s.env.RegisterActivity(scheduledBackupActivity.CreateScheduledBackup)
	s.env.RegisterActivity(scheduledBackupActivity.GenerateScheduledSnapshotName)

	s.env.OnActivity(backupActivity.GetBackupVault, mock.Anything, mock.Anything).
		Return(&datamodel.BackupVault{
			Name: "test-backup-vault",
			BucketDetails: []*datamodel.BucketDetails{
				{
					BucketName:         "vsa-backup-bucket",
					VendorSubnetID:     "test-vendor-subnet-id",
					ServiceAccountName: "test-service-account",
				},
			},
		}, nil)
	s.env.OnActivity(scheduledBackupActivity.CreateScheduledBackup, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&datamodel.Backup{Attributes: &datamodel.BackupAttributes{}}, nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).
		Return([]*datamodel.Node{
			{
				EndpointAddress: "0.0.0.0",
			},
		}, nil)
	s.env.OnActivity(scheduledBackupActivity.GenerateScheduledSnapshotName, mock.Anything, mock.Anything).
		Return("scheduled-snapshot-name", nil)
	s.env.OnActivity(backupActivity.GetOrCreateObjectStore, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&common.CloudTarget{
			Name: "vsa-backup-bucket",
		}, nil)

	destinationUUID := "test-destination-uuid-1"
	s.env.OnActivity(backupActivity.SnapmirrorGetorCreate, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&common.SnapmirrorRelationship{
			UUID:            "test-uuid-1",
			DestinationUUID: &destinationUUID,
		}, nil)
	s.env.OnActivity(backupActivity.SnapshotCreate, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&vsa.SnapshotProviderResponse{
			ProviderResponse: vsa.ProviderResponse{
				ExternalUUID: "test-uuid-1",
			},
		}, nil)
	s.env.OnActivity(backupActivity.SnapmirrorTransfer, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("could not transfer snapshot to object store"))

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "volume-uuid-1",
		},
		Name:   "test-volume-1",
		PoolID: 1,
		Pool: &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				Password: "pool-password",
				SecretID: "pool-credential-secret-id",
			},
			DeploymentName: "test-pool-deployment",
		},
		Svm: &datamodel.Svm{
			Name: "test-svm-1",
		},
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "backup-vault-uuid-1",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID:   "external-uuid-1",
			VendorSubnetID: "test-vendor-subnet-id",
		},
	}
	backupPolicy := &datamodel.BackupPolicy{
		DailyBackupsToKeep:   3,
		WeeklyBackupsToKeep:  1,
		MonthlyBackupsToKeep: 1,
	}
	s.env.ExecuteWorkflow(CreateScheduledBackupWorkflow, volume, backupPolicy)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())

	var activityError *temporal.ActivityError
	if errors.As(s.env.GetWorkflowError(), &activityError) {
		assert.Equal(s.T(), "could not transfer snapshot to object store", activityError.Unwrap().Error())
	} else {
		assert.Fail(s.T(), "Expected ActivityError but got: %v", s.env.GetWorkflowError())
	}
}

func (s *ScheduledBackupsTestSuite) TestCreateScheduledBackupWorkflow_GetSnapmirrorTransferStatusFailure() {
	scheduledWeeklyBackupDay = int(time.Now().Weekday())
	scheduledMonthlyBackupDay = time.Now().Day()

	mockStorage := database.NewMockStorage(s.T())
	commonActivity := &activities.CommonActivities{SE: mockStorage}
	backupActivity := &activities.BackupActivity{SE: mockStorage}
	scheduledBackupActivity := &backgroundactivities.ScheduledBackupActivity{SE: mockStorage}

	s.env.RegisterActivity(commonActivity.GetNode)
	s.env.RegisterActivity(backupActivity.GetBackupVault)
	s.env.RegisterActivity(backupActivity.GetOrCreateObjectStore)
	s.env.RegisterActivity(backupActivity.SnapmirrorGetorCreate)
	s.env.RegisterActivity(backupActivity.SnapshotCreate)
	s.env.RegisterActivity(backupActivity.SnapmirrorTransfer)
	s.env.RegisterActivity(backupActivity.GetSnapmirrorTransferStatus)
	s.env.RegisterActivity(scheduledBackupActivity.CreateScheduledBackup)
	s.env.RegisterActivity(scheduledBackupActivity.GenerateScheduledSnapshotName)

	s.env.OnActivity(backupActivity.GetBackupVault, mock.Anything, mock.Anything).
		Return(&datamodel.BackupVault{
			Name: "test-backup-vault",
			BucketDetails: []*datamodel.BucketDetails{
				{
					BucketName:         "vsa-backup-bucket",
					VendorSubnetID:     "test-vendor-subnet-id",
					ServiceAccountName: "test-service-account",
				},
			},
		}, nil)
	s.env.OnActivity(scheduledBackupActivity.CreateScheduledBackup, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&datamodel.Backup{Attributes: &datamodel.BackupAttributes{}}, nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).
		Return([]*datamodel.Node{
			{
				EndpointAddress: "0.0.0.0",
			},
		}, nil)
	s.env.OnActivity(scheduledBackupActivity.GenerateScheduledSnapshotName, mock.Anything, mock.Anything).
		Return("scheduled-snapshot-name", nil)
	s.env.OnActivity(backupActivity.GetOrCreateObjectStore, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&common.CloudTarget{
			Name: "vsa-backup-bucket",
		}, nil)

	destinationUUID := "test-destination-uuid-1"
	s.env.OnActivity(backupActivity.SnapmirrorGetorCreate, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&common.SnapmirrorRelationship{
			UUID:            "test-uuid-1",
			DestinationUUID: &destinationUUID,
		}, nil)
	s.env.OnActivity(backupActivity.SnapshotCreate, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&vsa.SnapshotProviderResponse{
			ProviderResponse: vsa.ProviderResponse{
				ExternalUUID: "test-uuid-1",
			},
		}, nil)
	s.env.OnActivity(backupActivity.SnapmirrorTransfer, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupActivity.GetSnapmirrorTransferStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(activities.SmStatusFailed, errors.New("could not get the status of snapmirror transfer"))

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "volume-uuid-1",
		},
		Name:   "test-volume-1",
		PoolID: 1,
		Svm: &datamodel.Svm{
			Name: "test-svm-1",
		},
		Pool: &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				Password: "pool-password",
				SecretID: "pool-credential-secret-id",
			},
			DeploymentName: "test-pool-deployment",
		},
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "backup-vault-uuid-1",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID:   "external-uuid-1",
			VendorSubnetID: "test-vendor-subnet-id",
		},
	}
	backupPolicy := &datamodel.BackupPolicy{
		DailyBackupsToKeep:   3,
		WeeklyBackupsToKeep:  1,
		MonthlyBackupsToKeep: 1,
	}
	s.env.ExecuteWorkflow(CreateScheduledBackupWorkflow, volume, backupPolicy)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())

	var activityError *temporal.ActivityError
	if errors.As(s.env.GetWorkflowError(), &activityError) {
		assert.Equal(s.T(), "could not get the status of snapmirror transfer", activityError.Unwrap().Error())
	} else {
		assert.Fail(s.T(), "Expected ActivityError but got: %v", s.env.GetWorkflowError())
	}
}

func (s *ScheduledBackupsTestSuite) TestCreateScheduledBackupWorkflow_FinishBackupFailure() {
	scheduledWeeklyBackupDay = int(time.Now().Weekday())
	scheduledMonthlyBackupDay = time.Now().Day()

	mockStorage := database.NewMockStorage(s.T())
	commonActivity := &activities.CommonActivities{SE: mockStorage}
	backupActivity := &activities.BackupActivity{SE: mockStorage}
	scheduledBackupActivity := &backgroundactivities.ScheduledBackupActivity{SE: mockStorage}

	s.env.RegisterActivity(commonActivity.GetNode)
	s.env.RegisterActivity(backupActivity.GetBackupVault)
	s.env.RegisterActivity(backupActivity.GetOrCreateObjectStore)
	s.env.RegisterActivity(backupActivity.SnapmirrorGetorCreate)
	s.env.RegisterActivity(backupActivity.SnapshotCreate)
	s.env.RegisterActivity(backupActivity.SnapmirrorTransfer)
	s.env.RegisterActivity(backupActivity.GetSnapmirrorTransferStatus)
	s.env.RegisterActivity(backupActivity.FinishBackup)
	s.env.RegisterActivity(scheduledBackupActivity.CreateScheduledBackup)
	s.env.RegisterActivity(scheduledBackupActivity.GenerateScheduledSnapshotName)

	s.env.OnActivity(backupActivity.GetBackupVault, mock.Anything, mock.Anything).
		Return(&datamodel.BackupVault{
			Name: "test-backup-vault",
			BucketDetails: []*datamodel.BucketDetails{
				{
					BucketName:         "vsa-backup-bucket",
					VendorSubnetID:     "test-vendor-subnet-id",
					ServiceAccountName: "test-service-account",
				},
			},
		}, nil)
	s.env.OnActivity(scheduledBackupActivity.CreateScheduledBackup, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&datamodel.Backup{Attributes: &datamodel.BackupAttributes{}}, nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).
		Return([]*datamodel.Node{
			{
				EndpointAddress: "0.0.0.0",
			},
		}, nil)
	s.env.OnActivity(scheduledBackupActivity.GenerateScheduledSnapshotName, mock.Anything, mock.Anything).
		Return("scheduled-snapshot-name", nil)
	s.env.OnActivity(backupActivity.GetOrCreateObjectStore, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&common.CloudTarget{
			Name: "vsa-backup-bucket",
		}, nil)

	destinationUUID := "test-destination-uuid-1"
	s.env.OnActivity(backupActivity.SnapmirrorGetorCreate, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&common.SnapmirrorRelationship{
			UUID:            "test-uuid-1",
			DestinationUUID: &destinationUUID,
		}, nil)
	s.env.OnActivity(backupActivity.SnapshotCreate, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&vsa.SnapshotProviderResponse{
			ProviderResponse: vsa.ProviderResponse{
				ExternalUUID: "test-uuid-1",
			},
		}, nil)
	s.env.OnActivity(backupActivity.SnapmirrorTransfer, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupActivity.GetSnapmirrorTransferStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(activities.SmStatusSuccess, nil)
	s.env.OnActivity(backupActivity.FinishBackup, mock.Anything, mock.Anything).Return(errors.New("could not update backup status"))

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "volume-uuid-1",
		},
		Name: "test-volume-1",
		Svm: &datamodel.Svm{
			Name: "test-svm-1",
		},
		PoolID: 1,
		Pool: &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				Password: "pool-password",
				SecretID: "pool-credential-secret-id",
			},
			DeploymentName: "test-pool-deployment",
		},
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "backup-vault-uuid-1",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID:   "external-uuid-1",
			VendorSubnetID: "test-vendor-subnet-id",
		},
	}
	backupPolicy := &datamodel.BackupPolicy{
		DailyBackupsToKeep:   3,
		WeeklyBackupsToKeep:  1,
		MonthlyBackupsToKeep: 1,
	}
	s.env.ExecuteWorkflow(CreateScheduledBackupWorkflow, volume, backupPolicy)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())

	var activityError *temporal.ActivityError
	if errors.As(s.env.GetWorkflowError(), &activityError) {
		assert.Equal(s.T(), "could not update backup status", activityError.Unwrap().Error())
	} else {
		assert.Fail(s.T(), "Expected ActivityError but got: %v", s.env.GetWorkflowError())
	}
}

func (s *ScheduledBackupsTestSuite) TestCreateScheduledBackupWorkflow_HydrateBackupsToCCFEFailure() {
	scheduledWeeklyBackupDay = int(time.Now().Weekday())
	scheduledMonthlyBackupDay = time.Now().Day()

	mockStorage := database.NewMockStorage(s.T())
	commonActivity := &activities.CommonActivities{SE: mockStorage}
	backupActivity := &activities.BackupActivity{SE: mockStorage}
	scheduledBackupActivity := &backgroundactivities.ScheduledBackupActivity{SE: mockStorage}

	s.env.RegisterActivity(commonActivity.GetNode)
	s.env.RegisterActivity(backupActivity.GetBackupVault)
	s.env.RegisterActivity(backupActivity.GetOrCreateObjectStore)
	s.env.RegisterActivity(backupActivity.SnapmirrorGetorCreate)
	s.env.RegisterActivity(backupActivity.SnapshotCreate)
	s.env.RegisterActivity(backupActivity.SnapmirrorTransfer)
	s.env.RegisterActivity(backupActivity.GetSnapmirrorTransferStatus)
	s.env.RegisterActivity(backupActivity.FinishBackup)
	s.env.RegisterActivity(scheduledBackupActivity.CreateScheduledBackup)
	s.env.RegisterActivity(scheduledBackupActivity.GenerateScheduledSnapshotName)
	s.env.RegisterActivity(scheduledBackupActivity.HydrateCreatedBackupsToCCFE)

	s.env.OnActivity(backupActivity.GetBackupVault, mock.Anything, mock.Anything).
		Return(&datamodel.BackupVault{
			Name: "test-backup-vault",
			BucketDetails: []*datamodel.BucketDetails{
				{
					BucketName:         "vsa-backup-bucket",
					VendorSubnetID:     "test-vendor-subnet-id",
					ServiceAccountName: "test-service-account",
				},
			},
		}, nil)
	s.env.OnActivity(scheduledBackupActivity.CreateScheduledBackup, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&datamodel.Backup{Attributes: &datamodel.BackupAttributes{}}, nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).
		Return([]*datamodel.Node{
			{
				EndpointAddress: "0.0.0.0",
			},
		}, nil)
	s.env.OnActivity(scheduledBackupActivity.GenerateScheduledSnapshotName, mock.Anything, mock.Anything).
		Return("scheduled-snapshot-name", nil)
	s.env.OnActivity(backupActivity.GetOrCreateObjectStore, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&common.CloudTarget{
			Name: "vsa-backup-bucket",
		}, nil)

	destinationUUID := "test-destination-uuid-1"
	s.env.OnActivity(backupActivity.SnapmirrorGetorCreate, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&common.SnapmirrorRelationship{
			UUID:            "test-uuid-1",
			DestinationUUID: &destinationUUID,
		}, nil)
	s.env.OnActivity(backupActivity.SnapshotCreate, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&vsa.SnapshotProviderResponse{
			ProviderResponse: vsa.ProviderResponse{
				ExternalUUID: "test-uuid-1",
			},
		}, nil)
	s.env.OnActivity(backupActivity.SnapmirrorTransfer, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupActivity.GetSnapmirrorTransferStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(activities.SmStatusSuccess, nil)
	s.env.OnActivity(backupActivity.FinishBackup, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(scheduledBackupActivity.HydrateCreatedBackupsToCCFE, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("could not hydrate backups to CCFE"))

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "volume-uuid-1",
		},
		Name: "test-volume-1",
		Svm: &datamodel.Svm{
			Name: "test-svm-1",
		},
		PoolID: 1,
		Pool: &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				Password: "pool-password",
				SecretID: "pool-credential-secret-id",
			},
			DeploymentName: "test-pool-deployment",
		},
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "backup-vault-uuid-1",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID:   "external-uuid-1",
			VendorSubnetID: "test-vendor-subnet-id",
		},
	}
	backupPolicy := &datamodel.BackupPolicy{
		DailyBackupsToKeep:   3,
		WeeklyBackupsToKeep:  1,
		MonthlyBackupsToKeep: 1,
	}
	s.env.ExecuteWorkflow(CreateScheduledBackupWorkflow, volume, backupPolicy)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())

	var activityError *temporal.ActivityError
	if errors.As(s.env.GetWorkflowError(), &activityError) {
		assert.Equal(s.T(), "could not hydrate backups to CCFE", activityError.Unwrap().Error())
	} else {
		assert.Fail(s.T(), "Expected ActivityError but got: %v", s.env.GetWorkflowError())
	}
}

func (s *ScheduledBackupsTestSuite) TestCreateScheduledBackupWorkflow_ErrorLaunchingChildWorkflow() {
	scheduledWeeklyBackupDay = int(time.Now().Weekday())
	scheduledMonthlyBackupDay = time.Now().Day()

	mockStorage := database.NewMockStorage(s.T())
	commonActivity := &activities.CommonActivities{SE: mockStorage}
	backupActivity := &activities.BackupActivity{SE: mockStorage}
	scheduledBackupActivity := &backgroundactivities.ScheduledBackupActivity{SE: mockStorage}

	s.env.RegisterActivity(commonActivity.GetNode)
	s.env.RegisterActivity(backupActivity.GetBackupVault)
	s.env.RegisterActivity(backupActivity.GetOrCreateObjectStore)
	s.env.RegisterActivity(backupActivity.SnapmirrorGetorCreate)
	s.env.RegisterActivity(backupActivity.SnapshotCreate)
	s.env.RegisterActivity(backupActivity.SnapmirrorTransfer)
	s.env.RegisterActivity(backupActivity.GetSnapmirrorTransferStatus)
	s.env.RegisterActivity(backupActivity.FinishBackup)
	s.env.RegisterActivity(scheduledBackupActivity.CreateScheduledBackup)
	s.env.RegisterActivity(scheduledBackupActivity.GenerateScheduledSnapshotName)
	s.env.RegisterActivity(scheduledBackupActivity.HydrateCreatedBackupsToCCFE)

	s.env.OnActivity(backupActivity.GetBackupVault, mock.Anything, mock.Anything).
		Return(&datamodel.BackupVault{
			Name: "test-backup-vault",
			BucketDetails: []*datamodel.BucketDetails{
				{
					BucketName:         "vsa-backup-bucket",
					VendorSubnetID:     "test-vendor-subnet-id",
					ServiceAccountName: "test-service-account",
				},
			},
		}, nil)
	s.env.OnActivity(scheduledBackupActivity.CreateScheduledBackup, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&datamodel.Backup{Attributes: &datamodel.BackupAttributes{}}, nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).
		Return([]*datamodel.Node{
			{
				EndpointAddress: "0.0.0.0",
			},
		}, nil)
	s.env.OnActivity(scheduledBackupActivity.GenerateScheduledSnapshotName, mock.Anything, mock.Anything).
		Return("scheduled-snapshot-name", nil)
	s.env.OnActivity(backupActivity.GetOrCreateObjectStore, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&common.CloudTarget{
			Name: "vsa-backup-bucket",
		}, nil)

	destinationUUID := "test-destination-uuid-1"
	s.env.OnActivity(backupActivity.SnapmirrorGetorCreate, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&common.SnapmirrorRelationship{
			UUID:            "test-uuid-1",
			DestinationUUID: &destinationUUID,
		}, nil)
	s.env.OnActivity(backupActivity.SnapshotCreate, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&vsa.SnapshotProviderResponse{
			ProviderResponse: vsa.ProviderResponse{
				ExternalUUID: "test-uuid-1",
			},
		}, nil)
	s.env.OnActivity(backupActivity.SnapmirrorTransfer, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupActivity.GetSnapmirrorTransferStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(activities.SmStatusSuccess, nil)
	s.env.OnActivity(backupActivity.FinishBackup, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(scheduledBackupActivity.HydrateCreatedBackupsToCCFE, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnWorkflow(DeleteScheduledBackupWorkflow, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("could not launch child workflow"))

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "volume-uuid-1",
		},
		Name: "test-volume-1",
		Svm: &datamodel.Svm{
			Name: "test-svm-1",
		},
		PoolID: 1,
		Pool: &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				Password: "pool-password",
				SecretID: "pool-credential-secret-id",
			},
			DeploymentName: "test-pool-deployment",
		},
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "backup-vault-uuid-1",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID:   "external-uuid-1",
			VendorSubnetID: "test-vendor-subnet-id",
		},
	}
	backupPolicy := &datamodel.BackupPolicy{
		DailyBackupsToKeep:   3,
		WeeklyBackupsToKeep:  1,
		MonthlyBackupsToKeep: 1,
	}
	s.env.ExecuteWorkflow(CreateScheduledBackupWorkflow, volume, backupPolicy)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())

	var workflowExecutionError *temporal.WorkflowExecutionError
	if errors.As(s.env.GetWorkflowError(), &workflowExecutionError) {
		var applicationError *temporal.ApplicationError
		if errors.As(workflowExecutionError.Unwrap(), &applicationError) {
			assert.Equal(s.T(), "could not launch child workflow", applicationError.Error())
		}
	} else {
		assert.Fail(s.T(), "Expected WorkflowExecutionError but got: %v", s.env.GetWorkflowError())
	}
}

func (s *ScheduledBackupsTestSuite) TestDeleteScheduledBackupWorkflowSuccess() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := &activities.CommonActivities{SE: mockStorage}
	backupActivity := &activities.BackupActivity{SE: mockStorage}
	scheduledBackupActivity := &backgroundactivities.ScheduledBackupActivity{SE: mockStorage}

	s.env.RegisterActivity(backupActivity.GetBackupVault)
	s.env.RegisterActivity(scheduledBackupActivity.FetchScheduledBackupForDeletion)
	s.env.RegisterActivity(commonActivity.GetNode)
	s.env.RegisterActivity(backupActivity.GetObjectStore)
	s.env.RegisterActivity(backupActivity.IsBackupShared)
	s.env.RegisterActivity(backupActivity.DeleteSnapshotFromObjectStore)
	s.env.RegisterActivity(commonActivity.GetOntapJob)
	s.env.RegisterActivity(backupActivity.DeleteBackup)
	s.env.RegisterActivity(scheduledBackupActivity.HydrateDeletedBackupsToCCFE)

	s.env.OnActivity(backupActivity.GetBackupVault, mock.Anything, mock.Anything).
		Return(&datamodel.BackupVault{
			Name: "test-backup-vault",
			BucketDetails: []*datamodel.BucketDetails{
				{
					BucketName:     "vsa-backup-bucket",
					VendorSubnetID: "test-vendor-subnet-id",
				},
			},
		}, nil)
	s.env.OnActivity(scheduledBackupActivity.FetchScheduledBackupForDeletion, mock.Anything, mock.Anything, mock.Anything).
		Return([]*datamodel.Backup{{
			Attributes: &datamodel.BackupAttributes{
				SnapshotID: "test-snapshot-id-1",
			},
			Name:        "Weekly-backup1",
			ScheduleTag: scheduleTagWeekly,
		},
			{
				Attributes: &datamodel.BackupAttributes{
					SnapshotID: "test-snapshot-id-2",
				},
				Name:        "Monthly-backup1",
				ScheduleTag: scheduleTagMonthly,
			},
			{
				Attributes: &datamodel.BackupAttributes{
					SnapshotID: "test-snapshot-id-3",
				},
				Name:        "Daily-backup1",
				ScheduleTag: scheduleTagDaily,
			}}, nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).
		Return([]*datamodel.Node{
			{
				EndpointAddress: "0.0.0.0",
			},
		}, nil)
	s.env.OnActivity(backupActivity.GetObjectStore, mock.Anything, mock.Anything, mock.Anything).
		Return(&common.CloudTarget{
			Name: "vsa-backup-bucket",
			UUID: "test-cloud-target-uuid",
		}, nil)
	s.env.OnActivity(backupActivity.IsBackupShared, mock.Anything, mock.Anything).
		Return(false, nil)
	s.env.OnActivity(backupActivity.DeleteSnapshotFromObjectStore, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&vsa.OntapAsyncResponse{
			JobUUID: "test-job-uuid-1",
		}, nil)
	s.env.OnActivity(commonActivity.GetOntapJob, mock.Anything, mock.Anything, mock.Anything).
		Return(&vsa.OntapJob{State: "success"}, nil)
	s.env.OnActivity(backupActivity.DeleteBackup, mock.Anything, mock.Anything).
		Return(nil, nil)
	s.env.OnActivity(scheduledBackupActivity.HydrateDeletedBackupsToCCFE, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil)

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "volume-uuid-1",
		},
		Name: "test-volume-1",
		Svm: &datamodel.Svm{
			Name: "test-svm-1",
		},
		Pool: &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				Password: "pool-password",
				SecretID: "pool-credential-secret-id",
			},
			DeploymentName: "test-pool-deployment",
		},
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "backup-vault-uuid-1",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID:   "external-uuid-1",
			VendorSubnetID: "test-vendor-subnet-id",
		},
	}
	backupPolicy := &datamodel.BackupPolicy{
		DailyBackupsToKeep:   3,
		WeeklyBackupsToKeep:  1,
		MonthlyBackupsToKeep: 1,
	}
	s.env.ExecuteWorkflow(DeleteScheduledBackupWorkflow, volume, backupPolicy)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NoError(s.T(), s.env.GetWorkflowError())
}

func (s *ScheduledBackupsTestSuite) TestDeleteScheduledBackupWorkflow_GetBackupVaultFailure() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := &activities.CommonActivities{SE: mockStorage}
	backupActivity := &activities.BackupActivity{SE: mockStorage}
	scheduledBackupActivity := &backgroundactivities.ScheduledBackupActivity{SE: mockStorage}

	s.env.RegisterActivity(backupActivity.GetBackupVault)
	s.env.RegisterActivity(scheduledBackupActivity.FetchScheduledBackupForDeletion)
	s.env.RegisterActivity(commonActivity.GetNode)
	s.env.RegisterActivity(backupActivity.GetObjectStore)
	s.env.RegisterActivity(backupActivity.IsBackupShared)
	s.env.RegisterActivity(backupActivity.DeleteSnapshotFromObjectStore)
	s.env.RegisterActivity(commonActivity.GetOntapJob)
	s.env.RegisterActivity(backupActivity.DeleteBackup)
	s.env.RegisterActivity(scheduledBackupActivity.HydrateDeletedBackupsToCCFE)

	s.env.OnActivity(backupActivity.GetBackupVault, mock.Anything, mock.Anything).
		Return(nil, errors.New("could not get backup vault"))

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "volume-uuid-1",
		},
		Name: "test-volume-1",
		Svm: &datamodel.Svm{
			Name: "test-svm-1",
		},
		Pool: &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				Password: "pool-password",
				SecretID: "pool-credential-secret-id",
			},
			DeploymentName: "test-pool-deployment",
		},
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "backup-vault-uuid-1",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID:   "external-uuid-1",
			VendorSubnetID: "test-vendor-subnet-id",
		},
	}
	backupPolicy := &datamodel.BackupPolicy{
		DailyBackupsToKeep:   3,
		WeeklyBackupsToKeep:  1,
		MonthlyBackupsToKeep: 1,
	}
	s.env.ExecuteWorkflow(DeleteScheduledBackupWorkflow, volume, backupPolicy)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
}

func (s *ScheduledBackupsTestSuite) TestDeleteScheduledBackupWorkflow_FetchScheduledBackupForDeletionFailure() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := &activities.CommonActivities{SE: mockStorage}
	backupActivity := &activities.BackupActivity{SE: mockStorage}
	scheduledBackupActivity := &backgroundactivities.ScheduledBackupActivity{SE: mockStorage}

	s.env.RegisterActivity(backupActivity.GetBackupVault)
	s.env.RegisterActivity(scheduledBackupActivity.FetchScheduledBackupForDeletion)
	s.env.RegisterActivity(commonActivity.GetNode)
	s.env.RegisterActivity(backupActivity.GetObjectStore)
	s.env.RegisterActivity(backupActivity.IsBackupShared)
	s.env.RegisterActivity(backupActivity.DeleteSnapshotFromObjectStore)
	s.env.RegisterActivity(commonActivity.GetOntapJob)
	s.env.RegisterActivity(backupActivity.DeleteBackup)
	s.env.RegisterActivity(scheduledBackupActivity.HydrateDeletedBackupsToCCFE)

	s.env.OnActivity(backupActivity.GetBackupVault, mock.Anything, mock.Anything).
		Return(&datamodel.BackupVault{
			Name: "test-backup-vault",
			BucketDetails: []*datamodel.BucketDetails{
				{
					BucketName:     "vsa-backup-bucket",
					VendorSubnetID: "test-vendor-subnet-id",
				},
			},
		}, nil)
	s.env.OnActivity(scheduledBackupActivity.FetchScheduledBackupForDeletion, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, errors.New("could not fetch scheduled backups for deletion"))

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "volume-uuid-1",
		},
		Name: "test-volume-1",
		Svm: &datamodel.Svm{
			Name: "test-svm-1",
		},
		Pool: &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				Password: "pool-password",
				SecretID: "pool-credential-secret-id",
			},
			DeploymentName: "test-pool-deployment",
		},
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "backup-vault-uuid-1",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID:   "external-uuid-1",
			VendorSubnetID: "test-vendor-subnet-id",
		},
	}
	backupPolicy := &datamodel.BackupPolicy{
		DailyBackupsToKeep:   3,
		WeeklyBackupsToKeep:  1,
		MonthlyBackupsToKeep: 1,
	}

	s.env.ExecuteWorkflow(DeleteScheduledBackupWorkflow, volume, backupPolicy)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
}

func (s *ScheduledBackupsTestSuite) TestDeleteScheduledBackupWorkflow_NoBackupsToBeDeleted() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := &activities.CommonActivities{SE: mockStorage}
	backupActivity := &activities.BackupActivity{SE: mockStorage}
	scheduledBackupActivity := &backgroundactivities.ScheduledBackupActivity{SE: mockStorage}

	s.env.RegisterActivity(backupActivity.GetBackupVault)
	s.env.RegisterActivity(scheduledBackupActivity.FetchScheduledBackupForDeletion)
	s.env.RegisterActivity(commonActivity.GetNode)
	s.env.RegisterActivity(backupActivity.GetObjectStore)
	s.env.RegisterActivity(backupActivity.IsBackupShared)
	s.env.RegisterActivity(backupActivity.DeleteSnapshotFromObjectStore)
	s.env.RegisterActivity(commonActivity.GetOntapJob)
	s.env.RegisterActivity(backupActivity.DeleteBackup)
	s.env.RegisterActivity(scheduledBackupActivity.HydrateDeletedBackupsToCCFE)

	s.env.OnActivity(backupActivity.GetBackupVault, mock.Anything, mock.Anything).
		Return(&datamodel.BackupVault{
			Name: "test-backup-vault",
			BucketDetails: []*datamodel.BucketDetails{
				{
					BucketName:     "vsa-backup-bucket",
					VendorSubnetID: "test-vendor-subnet-id",
				},
			},
		}, nil)
	s.env.OnActivity(scheduledBackupActivity.FetchScheduledBackupForDeletion, mock.Anything, mock.Anything, mock.Anything).
		Return([]*datamodel.Backup{}, nil)

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "volume-uuid-1",
		},
		Name: "test-volume-1",
		Svm: &datamodel.Svm{
			Name: "test-svm-1",
		},
		Pool: &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				Password: "pool-password",
				SecretID: "pool-credential-secret-id",
			},
			DeploymentName: "test-pool-deployment",
		},
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "backup-vault-uuid-1",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID:   "external-uuid-1",
			VendorSubnetID: "test-vendor-subnet-id",
		},
	}
	backupPolicy := &datamodel.BackupPolicy{
		DailyBackupsToKeep:   3,
		WeeklyBackupsToKeep:  1,
		MonthlyBackupsToKeep: 1,
	}
	s.env.ExecuteWorkflow(DeleteScheduledBackupWorkflow, volume, backupPolicy)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NoError(s.T(), s.env.GetWorkflowError())
}

func (s *ScheduledBackupsTestSuite) TestDeleteScheduledBackupWorkflow_GetNodeFailure() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := &activities.CommonActivities{SE: mockStorage}
	backupActivity := &activities.BackupActivity{SE: mockStorage}
	scheduledBackupActivity := &backgroundactivities.ScheduledBackupActivity{SE: mockStorage}

	s.env.RegisterActivity(backupActivity.GetBackupVault)
	s.env.RegisterActivity(scheduledBackupActivity.FetchScheduledBackupForDeletion)
	s.env.RegisterActivity(commonActivity.GetNode)
	s.env.RegisterActivity(backupActivity.GetObjectStore)
	s.env.RegisterActivity(backupActivity.IsBackupShared)
	s.env.RegisterActivity(backupActivity.DeleteSnapshotFromObjectStore)
	s.env.RegisterActivity(commonActivity.GetOntapJob)
	s.env.RegisterActivity(backupActivity.DeleteBackup)
	s.env.RegisterActivity(scheduledBackupActivity.HydrateDeletedBackupsToCCFE)

	s.env.OnActivity(backupActivity.GetBackupVault, mock.Anything, mock.Anything).
		Return(&datamodel.BackupVault{
			Name: "test-backup-vault",
			BucketDetails: []*datamodel.BucketDetails{
				{
					BucketName:     "vsa-backup-bucket",
					VendorSubnetID: "test-vendor-subnet-id",
				},
			},
		}, nil)
	s.env.OnActivity(scheduledBackupActivity.FetchScheduledBackupForDeletion, mock.Anything, mock.Anything, mock.Anything).
		Return([]*datamodel.Backup{
			{
				Attributes: &datamodel.BackupAttributes{
					SnapshotID: "test-snapshot-id-1",
				},
				Name:        "Weekly-backup1",
				ScheduleTag: scheduleTagWeekly,
			},
		}, nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).
		Return(nil, errors.New("could not get node details"))

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "volume-uuid-1",
		},
		Name: "test-volume-1",
		Svm: &datamodel.Svm{
			Name: "test-svm-1",
		},
		Pool: &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				Password: "pool-password",
				SecretID: "pool-credential-secret-id",
			},
			DeploymentName: "test-pool-deployment",
		},
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "backup-vault-uuid-1",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID:   "external-uuid-1",
			VendorSubnetID: "test-vendor-subnet-id",
		},
	}
	backupPolicy := &datamodel.BackupPolicy{
		DailyBackupsToKeep:   3,
		WeeklyBackupsToKeep:  1,
		MonthlyBackupsToKeep: 1,
	}
	s.env.ExecuteWorkflow(DeleteScheduledBackupWorkflow, volume, backupPolicy)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
}

func (s *ScheduledBackupsTestSuite) TestDeleteScheduledBackupWorkflow_GetObjectStoreFailure() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := &activities.CommonActivities{SE: mockStorage}
	backupActivity := &activities.BackupActivity{SE: mockStorage}
	scheduledBackupActivity := &backgroundactivities.ScheduledBackupActivity{SE: mockStorage}

	s.env.RegisterActivity(backupActivity.GetBackupVault)
	s.env.RegisterActivity(scheduledBackupActivity.FetchScheduledBackupForDeletion)
	s.env.RegisterActivity(commonActivity.GetNode)
	s.env.RegisterActivity(backupActivity.GetObjectStore)
	s.env.RegisterActivity(backupActivity.IsBackupShared)
	s.env.RegisterActivity(backupActivity.DeleteSnapshotFromObjectStore)
	s.env.RegisterActivity(commonActivity.GetOntapJob)
	s.env.RegisterActivity(backupActivity.DeleteBackup)
	s.env.RegisterActivity(scheduledBackupActivity.HydrateDeletedBackupsToCCFE)

	s.env.OnActivity(backupActivity.GetBackupVault, mock.Anything, mock.Anything).
		Return(&datamodel.BackupVault{
			Name: "test-backup-vault",
			BucketDetails: []*datamodel.BucketDetails{
				{
					BucketName:     "vsa-backup-bucket",
					VendorSubnetID: "test-vendor-subnet-id",
				},
			},
		}, nil)
	s.env.OnActivity(scheduledBackupActivity.FetchScheduledBackupForDeletion, mock.Anything, mock.Anything, mock.Anything).
		Return([]*datamodel.Backup{
			{
				Attributes: &datamodel.BackupAttributes{
					SnapshotID: "test-snapshot-id-1",
				},
				Name:        "Weekly-backup1",
				ScheduleTag: scheduleTagWeekly,
			},
		}, nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).
		Return([]*datamodel.Node{
			{
				EndpointAddress: "0.0.0.0",
			},
		}, nil)
	s.env.OnActivity(backupActivity.GetObjectStore, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, errors.New("could not get object store details"))

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "volume-uuid-1",
		},
		Name: "test-volume-1",
		Svm: &datamodel.Svm{
			Name: "test-svm-1",
		},
		Pool: &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				Password: "pool-password",
				SecretID: "pool-credential-secret-id",
			},
			DeploymentName: "test-pool-deployment",
		},
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "backup-vault-uuid-1",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID:   "external-uuid-1",
			VendorSubnetID: "test-vendor-subnet-id",
		},
	}
	backupPolicy := &datamodel.BackupPolicy{
		DailyBackupsToKeep:   3,
		WeeklyBackupsToKeep:  1,
		MonthlyBackupsToKeep: 1,
	}

	s.env.ExecuteWorkflow(DeleteScheduledBackupWorkflow, volume, backupPolicy)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
}

func (s *ScheduledBackupsTestSuite) TestDeleteScheduledBackupWorkflow_IsBackupSharedFailure() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := &activities.CommonActivities{SE: mockStorage}
	backupActivity := &activities.BackupActivity{SE: mockStorage}
	scheduledBackupActivity := &backgroundactivities.ScheduledBackupActivity{SE: mockStorage}

	s.env.RegisterActivity(backupActivity.GetBackupVault)
	s.env.RegisterActivity(scheduledBackupActivity.FetchScheduledBackupForDeletion)
	s.env.RegisterActivity(commonActivity.GetNode)
	s.env.RegisterActivity(backupActivity.GetObjectStore)
	s.env.RegisterActivity(backupActivity.IsBackupShared)
	s.env.RegisterActivity(backupActivity.DeleteSnapshotFromObjectStore)
	s.env.RegisterActivity(commonActivity.GetOntapJob)
	s.env.RegisterActivity(backupActivity.DeleteBackup)
	s.env.RegisterActivity(scheduledBackupActivity.HydrateDeletedBackupsToCCFE)

	s.env.OnActivity(backupActivity.GetBackupVault, mock.Anything, mock.Anything).
		Return(&datamodel.BackupVault{
			Name: "test-backup-vault",
			BucketDetails: []*datamodel.BucketDetails{
				{
					BucketName:     "vsa-backup-bucket",
					VendorSubnetID: "test-vendor-subnet-id",
				},
			},
		}, nil)
	s.env.OnActivity(scheduledBackupActivity.FetchScheduledBackupForDeletion, mock.Anything, mock.Anything, mock.Anything).
		Return([]*datamodel.Backup{{
			Attributes: &datamodel.BackupAttributes{
				SnapshotID: "test-snapshot-id-1",
			},
			Name:        "Weekly-backup1",
			ScheduleTag: scheduleTagWeekly,
		},
			{
				Attributes: &datamodel.BackupAttributes{
					SnapshotID: "test-snapshot-id-2",
				},
				Name:        "Monthly-backup1",
				ScheduleTag: scheduleTagMonthly,
			},
			{
				Attributes: &datamodel.BackupAttributes{
					SnapshotID: "test-snapshot-id-3",
				},
				Name:        "Daily-backup1",
				ScheduleTag: scheduleTagDaily,
			}}, nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).
		Return([]*datamodel.Node{
			{
				EndpointAddress: "0.0.0.0",
			},
		}, nil)
	s.env.OnActivity(backupActivity.GetObjectStore, mock.Anything, mock.Anything, mock.Anything).
		Return(&common.CloudTarget{
			Name: "vsa-backup-bucket",
			UUID: "test-cloud-target-uuid",
		}, nil)
	s.env.OnActivity(backupActivity.IsBackupShared, mock.Anything, mock.Anything).
		Return(false, errors.New("could not determine if backup is shared"))

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "volume-uuid-1",
		},
		Name: "test-volume-1",
		Svm: &datamodel.Svm{
			Name: "test-svm-1",
		},
		Pool: &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				Password: "pool-password",
				SecretID: "pool-credential-secret-id",
			},
			DeploymentName: "test-pool-deployment",
		},
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "backup-vault-uuid-1",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID:   "external-uuid-1",
			VendorSubnetID: "test-vendor-subnet-id",
		},
	}
	backupPolicy := &datamodel.BackupPolicy{
		DailyBackupsToKeep:   3,
		WeeklyBackupsToKeep:  1,
		MonthlyBackupsToKeep: 1,
	}
	s.env.ExecuteWorkflow(DeleteScheduledBackupWorkflow, volume, backupPolicy)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
}

func (s *ScheduledBackupsTestSuite) TestDeleteScheduledBackupWorkflow_DeleteSnapshotFromObjectStoreFailure() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := &activities.CommonActivities{SE: mockStorage}
	backupActivity := &activities.BackupActivity{SE: mockStorage}
	scheduledBackupActivity := &backgroundactivities.ScheduledBackupActivity{SE: mockStorage}

	s.env.RegisterActivity(backupActivity.GetBackupVault)
	s.env.RegisterActivity(scheduledBackupActivity.FetchScheduledBackupForDeletion)
	s.env.RegisterActivity(commonActivity.GetNode)
	s.env.RegisterActivity(backupActivity.GetObjectStore)
	s.env.RegisterActivity(backupActivity.IsBackupShared)
	s.env.RegisterActivity(backupActivity.DeleteSnapshotFromObjectStore)
	s.env.RegisterActivity(commonActivity.GetOntapJob)
	s.env.RegisterActivity(backupActivity.DeleteBackup)
	s.env.RegisterActivity(scheduledBackupActivity.HydrateDeletedBackupsToCCFE)

	s.env.OnActivity(backupActivity.GetBackupVault, mock.Anything, mock.Anything).
		Return(&datamodel.BackupVault{
			Name: "test-backup-vault",
			BucketDetails: []*datamodel.BucketDetails{
				{
					BucketName:     "vsa-backup-bucket",
					VendorSubnetID: "test-vendor-subnet-id",
				},
			},
		}, nil)
	s.env.OnActivity(scheduledBackupActivity.FetchScheduledBackupForDeletion, mock.Anything, mock.Anything, mock.Anything).
		Return([]*datamodel.Backup{{
			Attributes: &datamodel.BackupAttributes{
				SnapshotID: "test-snapshot-id-1",
			},
			Name:        "Weekly-backup1",
			ScheduleTag: scheduleTagWeekly,
		},
			{
				Attributes: &datamodel.BackupAttributes{
					SnapshotID: "test-snapshot-id-2",
				},
				Name:        "Monthly-backup1",
				ScheduleTag: scheduleTagMonthly,
			},
			{
				Attributes: &datamodel.BackupAttributes{
					SnapshotID: "test-snapshot-id-3",
				},
				Name:        "Daily-backup1",
				ScheduleTag: scheduleTagDaily,
			}}, nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).
		Return([]*datamodel.Node{
			{
				EndpointAddress: "0.0.0.0",
			},
		}, nil)
	s.env.OnActivity(backupActivity.GetObjectStore, mock.Anything, mock.Anything, mock.Anything).
		Return(&common.CloudTarget{
			Name: "vsa-backup-bucket",
			UUID: "test-cloud-target-uuid",
		}, nil)
	s.env.OnActivity(backupActivity.IsBackupShared, mock.Anything, mock.Anything).
		Return(false, nil)
	s.env.OnActivity(backupActivity.DeleteSnapshotFromObjectStore, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, errors.New("could not delete snapshot from object store"))

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "volume-uuid-1",
		},
		Name: "test-volume-1",
		Svm: &datamodel.Svm{
			Name: "test-svm-1",
		},
		Pool: &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				Password: "pool-password",
				SecretID: "pool-credential-secret-id",
			},
			DeploymentName: "test-pool-deployment",
		},
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "backup-vault-uuid-1",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID:   "external-uuid-1",
			VendorSubnetID: "test-vendor-subnet-id",
		},
	}
	backupPolicy := &datamodel.BackupPolicy{
		DailyBackupsToKeep:   3,
		WeeklyBackupsToKeep:  1,
		MonthlyBackupsToKeep: 1,
	}
	s.env.ExecuteWorkflow(DeleteScheduledBackupWorkflow, volume, backupPolicy)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
}

func (s *ScheduledBackupsTestSuite) TestDeleteScheduledBackupWorkflow_GetOntapJobFailure() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := &activities.CommonActivities{SE: mockStorage}
	backupActivity := &activities.BackupActivity{SE: mockStorage}
	scheduledBackupActivity := &backgroundactivities.ScheduledBackupActivity{SE: mockStorage}

	s.env.RegisterActivity(backupActivity.GetBackupVault)
	s.env.RegisterActivity(scheduledBackupActivity.FetchScheduledBackupForDeletion)
	s.env.RegisterActivity(commonActivity.GetNode)
	s.env.RegisterActivity(backupActivity.GetObjectStore)
	s.env.RegisterActivity(backupActivity.IsBackupShared)
	s.env.RegisterActivity(backupActivity.DeleteSnapshotFromObjectStore)
	s.env.RegisterActivity(commonActivity.GetOntapJob)
	s.env.RegisterActivity(backupActivity.DeleteBackup)
	s.env.RegisterActivity(scheduledBackupActivity.HydrateDeletedBackupsToCCFE)

	s.env.OnActivity(backupActivity.GetBackupVault, mock.Anything, mock.Anything).
		Return(&datamodel.BackupVault{
			Name: "test-backup-vault",
			BucketDetails: []*datamodel.BucketDetails{
				{
					BucketName:     "vsa-backup-bucket",
					VendorSubnetID: "test-vendor-subnet-id",
				},
			},
		}, nil)
	s.env.OnActivity(scheduledBackupActivity.FetchScheduledBackupForDeletion, mock.Anything, mock.Anything, mock.Anything).
		Return([]*datamodel.Backup{{
			Attributes: &datamodel.BackupAttributes{
				SnapshotID: "test-snapshot-id-1",
			},
			Name:        "Weekly-backup1",
			ScheduleTag: scheduleTagWeekly,
		},
			{
				Attributes: &datamodel.BackupAttributes{
					SnapshotID: "test-snapshot-id-2",
				},
				Name:        "Monthly-backup1",
				ScheduleTag: scheduleTagMonthly,
			},
			{
				Attributes: &datamodel.BackupAttributes{
					SnapshotID: "test-snapshot-id-3",
				},
				Name:        "Daily-backup1",
				ScheduleTag: scheduleTagDaily,
			}}, nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).
		Return([]*datamodel.Node{
			{
				EndpointAddress: "0.0.0.0",
			},
		}, nil)
	s.env.OnActivity(backupActivity.GetObjectStore, mock.Anything, mock.Anything, mock.Anything).
		Return(&common.CloudTarget{
			Name: "vsa-backup-bucket",
			UUID: "test-cloud-target-uuid",
		}, nil)
	s.env.OnActivity(backupActivity.IsBackupShared, mock.Anything, mock.Anything).
		Return(false, nil)
	s.env.OnActivity(backupActivity.DeleteSnapshotFromObjectStore, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&vsa.OntapAsyncResponse{
			JobUUID: "test-job-uuid-1",
		}, nil)
	s.env.OnActivity(commonActivity.GetOntapJob, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, errors.New("could not get ONTAP job details"))

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "volume-uuid-1",
		},
		Name: "test-volume-1",
		Svm: &datamodel.Svm{
			Name: "test-svm-1",
		},
		Pool: &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				Password: "pool-password",
				SecretID: "pool-credential-secret-id",
			},
			DeploymentName: "test-pool-deployment",
		},
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "backup-vault-uuid-1",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID:   "external-uuid-1",
			VendorSubnetID: "test-vendor-subnet-id",
		},
	}
	backupPolicy := &datamodel.BackupPolicy{
		DailyBackupsToKeep:   3,
		WeeklyBackupsToKeep:  1,
		MonthlyBackupsToKeep: 1,
	}
	s.env.ExecuteWorkflow(DeleteScheduledBackupWorkflow, volume, backupPolicy)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
}

func (s *ScheduledBackupsTestSuite) TestDeleteScheduledBackupWorkflow_DeleteBackupFailure() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := &activities.CommonActivities{SE: mockStorage}
	backupActivity := &activities.BackupActivity{SE: mockStorage}
	scheduledBackupActivity := &backgroundactivities.ScheduledBackupActivity{SE: mockStorage}

	s.env.RegisterActivity(backupActivity.GetBackupVault)
	s.env.RegisterActivity(scheduledBackupActivity.FetchScheduledBackupForDeletion)
	s.env.RegisterActivity(commonActivity.GetNode)
	s.env.RegisterActivity(backupActivity.GetObjectStore)
	s.env.RegisterActivity(backupActivity.IsBackupShared)
	s.env.RegisterActivity(backupActivity.DeleteSnapshotFromObjectStore)
	s.env.RegisterActivity(commonActivity.GetOntapJob)
	s.env.RegisterActivity(backupActivity.DeleteBackup)

	s.env.OnActivity(backupActivity.GetBackupVault, mock.Anything, mock.Anything).
		Return(&datamodel.BackupVault{
			Name: "test-backup-vault",
			BucketDetails: []*datamodel.BucketDetails{
				{
					BucketName:     "vsa-backup-bucket",
					VendorSubnetID: "test-vendor-subnet-id",
				},
			},
		}, nil)
	s.env.OnActivity(scheduledBackupActivity.FetchScheduledBackupForDeletion, mock.Anything, mock.Anything, mock.Anything).
		Return([]*datamodel.Backup{{
			Attributes: &datamodel.BackupAttributes{
				SnapshotID: "test-snapshot-id-1",
			},
			Name:        "Weekly-backup1",
			ScheduleTag: scheduleTagWeekly,
		},
			{
				Attributes: &datamodel.BackupAttributes{
					SnapshotID: "test-snapshot-id-2",
				},
				Name:        "Monthly-backup1",
				ScheduleTag: scheduleTagMonthly,
			},
			{
				Attributes: &datamodel.BackupAttributes{
					SnapshotID: "test-snapshot-id-3",
				},
				Name:        "Daily-backup1",
				ScheduleTag: scheduleTagDaily,
			}}, nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).
		Return([]*datamodel.Node{
			{
				EndpointAddress: "0.0.0.0",
			},
		}, nil)
	s.env.OnActivity(backupActivity.GetObjectStore, mock.Anything, mock.Anything, mock.Anything).
		Return(&common.CloudTarget{
			Name: "vsa-backup-bucket",
			UUID: "test-cloud-target-uuid",
		}, nil)
	s.env.OnActivity(backupActivity.IsBackupShared, mock.Anything, mock.Anything).
		Return(false, nil)
	s.env.OnActivity(backupActivity.DeleteSnapshotFromObjectStore, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&vsa.OntapAsyncResponse{
			JobUUID: "test-job-uuid-1",
		}, nil)
	s.env.OnActivity(commonActivity.GetOntapJob, mock.Anything, mock.Anything, mock.Anything).
		Return(&vsa.OntapJob{State: "success"}, nil)
	s.env.OnActivity(backupActivity.DeleteBackup, mock.Anything, mock.Anything).
		Return(nil, nil)

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "volume-uuid-1",
		},
		Name: "test-volume-1",
		Svm: &datamodel.Svm{
			Name: "test-svm-1",
		},
		Pool: &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				Password: "pool-password",
				SecretID: "pool-credential-secret-id",
			},
			DeploymentName: "test-pool-deployment",
		},
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "backup-vault-uuid-1",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID:   "external-uuid-1",
			VendorSubnetID: "test-vendor-subnet-id",
		},
	}
	backupPolicy := &datamodel.BackupPolicy{
		DailyBackupsToKeep:   3,
		WeeklyBackupsToKeep:  1,
		MonthlyBackupsToKeep: 1,
	}
	s.env.ExecuteWorkflow(DeleteScheduledBackupWorkflow, volume, backupPolicy)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
}

func (s *ScheduledBackupsTestSuite) TestDeleteScheduledBackupWorkflow_HydrateDeletedBackupsToCCFE() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := &activities.CommonActivities{SE: mockStorage}
	backupActivity := &activities.BackupActivity{SE: mockStorage}
	scheduledBackupActivity := &backgroundactivities.ScheduledBackupActivity{SE: mockStorage}

	s.env.RegisterActivity(backupActivity.GetBackupVault)
	s.env.RegisterActivity(scheduledBackupActivity.FetchScheduledBackupForDeletion)
	s.env.RegisterActivity(commonActivity.GetNode)
	s.env.RegisterActivity(backupActivity.GetObjectStore)
	s.env.RegisterActivity(backupActivity.IsBackupShared)
	s.env.RegisterActivity(backupActivity.DeleteSnapshotFromObjectStore)
	s.env.RegisterActivity(commonActivity.GetOntapJob)
	s.env.RegisterActivity(backupActivity.DeleteBackup)
	s.env.RegisterActivity(scheduledBackupActivity.HydrateDeletedBackupsToCCFE)

	s.env.OnActivity(backupActivity.GetBackupVault, mock.Anything, mock.Anything).
		Return(&datamodel.BackupVault{
			Name: "test-backup-vault",
			BucketDetails: []*datamodel.BucketDetails{
				{
					BucketName:     "vsa-backup-bucket",
					VendorSubnetID: "test-vendor-subnet-id",
				},
			},
		}, nil)
	s.env.OnActivity(scheduledBackupActivity.FetchScheduledBackupForDeletion, mock.Anything, mock.Anything, mock.Anything).
		Return([]*datamodel.Backup{{
			Attributes: &datamodel.BackupAttributes{
				SnapshotID: "test-snapshot-id-1",
			},
			Name:        "Weekly-backup1",
			ScheduleTag: scheduleTagWeekly,
		},
			{
				Attributes: &datamodel.BackupAttributes{
					SnapshotID: "test-snapshot-id-2",
				},
				Name:        "Monthly-backup1",
				ScheduleTag: scheduleTagMonthly,
			},
			{
				Attributes: &datamodel.BackupAttributes{
					SnapshotID: "test-snapshot-id-3",
				},
				Name:        "Daily-backup1",
				ScheduleTag: scheduleTagDaily,
			}}, nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).
		Return([]*datamodel.Node{
			{
				EndpointAddress: "0.0.0.0",
			},
		}, nil)
	s.env.OnActivity(backupActivity.GetObjectStore, mock.Anything, mock.Anything, mock.Anything).
		Return(&common.CloudTarget{
			Name: "vsa-backup-bucket",
			UUID: "test-cloud-target-uuid",
		}, nil)
	s.env.OnActivity(backupActivity.IsBackupShared, mock.Anything, mock.Anything).
		Return(false, nil)
	s.env.OnActivity(backupActivity.DeleteSnapshotFromObjectStore, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&vsa.OntapAsyncResponse{
			JobUUID: "test-job-uuid-1",
		}, nil)
	s.env.OnActivity(commonActivity.GetOntapJob, mock.Anything, mock.Anything, mock.Anything).
		Return(&vsa.OntapJob{State: "success"}, nil)
	s.env.OnActivity(backupActivity.DeleteBackup, mock.Anything, mock.Anything).
		Return(nil, nil)
	s.env.OnActivity(scheduledBackupActivity.HydrateDeletedBackupsToCCFE, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(errors.New("could not hydrate deleted backups to CCFE"))

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "volume-uuid-1",
		},
		Name: "test-volume-1",
		Svm: &datamodel.Svm{
			Name: "test-svm-1",
		},
		Pool: &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				Password: "pool-password",
				SecretID: "pool-credential-secret-id",
			},
			DeploymentName: "test-pool-deployment",
		},
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "backup-vault-uuid-1",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID:   "external-uuid-1",
			VendorSubnetID: "test-vendor-subnet-id",
		},
	}
	backupPolicy := &datamodel.BackupPolicy{
		DailyBackupsToKeep:   3,
		WeeklyBackupsToKeep:  1,
		MonthlyBackupsToKeep: 1,
	}
	s.env.ExecuteWorkflow(DeleteScheduledBackupWorkflow, volume, backupPolicy)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
}
