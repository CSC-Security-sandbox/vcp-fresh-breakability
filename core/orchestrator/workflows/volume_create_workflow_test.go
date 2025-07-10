package workflows

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

type UnitTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env *testsuite.TestWorkflowEnvironment
}

func (s *UnitTestSuite) SetupTest() {
	s.env = s.NewTestWorkflowEnvironment()
	s.env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
	mockHeader := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{
			"logParam": encodedValue,
		},
	}
	s.env.SetHeader(mockHeader)

	// Register workflow
	s.env.RegisterWorkflow(CreateVolumeWorkflow)
	s.env.RegisterWorkflow(CreateBackupWorkflow)
}

func (s *UnitTestSuite) AfterTest() {
	s.env.AssertExpectations(s.T())
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_Success() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	volume := &datamodel.Volume{
		Pool:             &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, Username: "username", SecretID: "secretid"},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}},
	}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)
	s.env.RegisterActivity(volumeCreateActivity.InitiateSplitForVolume)

	// Mock activities
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{}}, nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name:         "lun_test",
			ExternalUUID: "lun-uuid",
		},
		SerialNumber: "6c5738423724595454686164",
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateLunMap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.InitiateSplitForVolume, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume, nil, nil)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *UnitTestSuite) Test_RestoreVolumeWorkflow_Failure() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	backupCreateActivity := activities.BackupActivity{SE: mockStorage}
	volume := &datamodel.Volume{
		Pool:             &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, Username: "username", SecretID: "secretid"},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}, IsDataProtection: true},
	}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	minEnforcedRetentionDuration := int64(30)
	bv := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "backup-vault-uuid"},
		Name:      "bv1",
		ImmutableAttributes: &datamodel.ImmutableAttributes{
			BackupMinimumEnforcedRetentionDuration: &minEnforcedRetentionDuration,
		},
	}
	backup := &datamodel.Backup{VolumeUUID: "463811e7-9760-acf5-9bdb-020073ca3335", State: "creating"}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)
	s.env.RegisterActivity(volumeCreateActivity.InitiateSplitForVolume)

	// Mock activities
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{}}, nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name:         "lun_test",
			ExternalUUID: "lun-uuid",
		},
		SerialNumber: "6c5738423724595454686164",
	}, nil)
	s.env.OnActivity(getObjStoreNameFromBackup, mock.Anything, mock.Anything).Return(bv, nil)
	s.env.OnActivity(getBucketDetailsFromBackup, mock.Anything, mock.Anything).Return(&datamodel.BucketDetails{}, nil)

	s.env.OnActivity(volumeCreateActivity.CreateLunMap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupCreateActivity.GetOrCreateObjectStore, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.CloudTarget{}, nil)
	s.env.OnActivity(backupCreateActivity.SnapmirrorGetorCreate, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.SnapmirrorRelationship{UUID: "uuid", DestinationUUID: nillable.ToPointer("snapmirror-uuid")}, nil)
	s.env.OnActivity(backupCreateActivity.SnapmirrorTransfer, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupCreateActivity.SnapmirrorTransferPoll, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.InitiateSplitForVolume, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{BackupPath: "projects/123456789/locations/us-e4/backupVaults/bv1/backups/backupName"}, volume, bv, backup)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_Failure_UpdateVolumeDetails() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	volume := &datamodel.Volume{
		Pool:             &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, Username: "username", SecretID: "secretid"},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}},
	}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Times(2)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed updating job"))

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)

	// Mock activities
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{
		Name: "host_group_test", Hosts: datamodel.Hosts{Hosts: []string{"iqn.1993-08.org.debian:01:f2c983feb27", "iqn.1994-05.com.redhat:19ee49a2145f"}}},
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name:         "lun_test",
			ExternalUUID: "lun-uuid",
		},
		SerialNumber: "6c5738423724595454686164",
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateLunMap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(
		errors.New("failed to update volume details"))

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume, nil, nil)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "workflow execution error")
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func TestUnitTestSuite(t *testing.T) {
	suite.Run(t, new(UnitTestSuite))
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_FindTenancyError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	volume := &datamodel.Volume{
		Pool:             &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, Username: "username", Password: "password"},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}},
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "backup-vault-id",
		},
	}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)

	// Mock activities
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{
		Name: "host_group_test", Hosts: datamodel.Hosts{Hosts: []string{"iqn.1993-08.org.debian:01:f2c983feb27", "iqn.1994-05.com.redhat:19ee49a2145f"}}},
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name:         "lun_test",
			ExternalUUID: "lun-uuid",
		},
		SerialNumber: "6c5738423724595454686164",
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateLunMap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("test-token", nil)
	s.env.OnActivity(volumeCreateActivity.FindTenancy, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to find tenancy"))

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume, nil, nil)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_TokenError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	volume := &datamodel.Volume{
		Pool:             &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, Username: "username", Password: "password"},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}},
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "backup-vault-id",
		},
	}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)

	// Mock activities
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{
		Name: "host_group_test", Hosts: datamodel.Hosts{Hosts: []string{"iqn.1993-08.org.debian:01:f2c983feb27", "iqn.1994-05.com.redhat:19ee49a2145f"}}},
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name:         "lun_test",
			ExternalUUID: "lun-uuid",
		},
		SerialNumber: "6c5738423724595454686164",
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateLunMap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return(nil, errors.New("failed to get auth JWT token"))

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume, nil, nil)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_CheckBackupVaultExistsInVCPError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	volume := &datamodel.Volume{
		Pool:             &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, Username: "username", Password: "password"},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}},
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "backup-vault-id",
		},
		Account: &datamodel.Account{Name: "account-1"},
	}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)

	// Mock activities
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{
		Name: "host_group_test", Hosts: datamodel.Hosts{Hosts: []string{"iqn.1993-08.org.debian:01:f2c983feb27", "iqn.1994-05.com.redhat:19ee49a2145f"}}},
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name:         "lun_test",
			ExternalUUID: "lun-uuid",
		},
		SerialNumber: "6c5738423724595454686164",
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateLunMap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("test-token", nil)
	s.env.OnActivity(volumeCreateActivity.FindTenancy, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.TenancyInfo{RegionalTenantProject: "tenant-project"}, nil)
	s.env.OnActivity(volumeCreateActivity.CheckBackupVaultExistsInVCP, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to check backup vault exists in VCP"))

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume, nil, nil)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_CheckBackupPolicyExistsInVCPError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	volume := &datamodel.Volume{
		Pool:             &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, Username: "username", Password: "password"},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}},
		DataProtection: &datamodel.DataProtection{
			BackupPolicyID: "backup-policy-id",
		},
		Account: &datamodel.Account{Name: "account-1"},
	}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)

	// Mock activities
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return(&datamodel.Node{EndpointAddress: "127.0.0.1"}, nil)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{
		Name: "host_group_test", Hosts: datamodel.Hosts{Hosts: []string{"iqn.1993-08.org.debian:01:f2c983feb27", "iqn.1994-05.com.redhat:19ee49a2145f"}}},
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name:         "lun_test",
			ExternalUUID: "lun-uuid",
		},
		SerialNumber: "6c5738423724595454686164",
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateLunMap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateBackupPolicyWhenVolumeAttachedInVCP, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to check backup policy exists in VCP"))

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume, nil, nil)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_CheckForBucketResourceNameError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	volume := &datamodel.Volume{
		Pool:             &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, Username: "username", Password: "password"},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}},
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "backup-vault-id",
		},
		Account: &datamodel.Account{Name: "account-1"},
	}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)

	// Mock activities
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{
		Name: "host_group_test", Hosts: datamodel.Hosts{Hosts: []string{"iqn.1993-08.org.debian:01:f2c983feb27", "iqn.1994-05.com.redhat:19ee49a2145f"}}},
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name:         "lun_test",
			ExternalUUID: "lun-uuid",
		},
		SerialNumber: "6c5738423724595454686164",
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateLunMap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("test-token", nil)
	s.env.OnActivity(volumeCreateActivity.FindTenancy, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.TenancyInfo{RegionalTenantProject: "tenant-project"}, nil)
	s.env.OnActivity(volumeCreateActivity.CheckBackupVaultExistsInVCP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CheckForBucketResourceName, mock.Anything, mock.Anything).Return(nil, errors.New("failed to check for bucket resource name"))

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume, nil, nil)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_GenerateResourceNamesError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	volume := &datamodel.Volume{
		Pool:             &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, Username: "username", Password: "password"},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}},
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "backup-vault-id",
		},
		Account: &datamodel.Account{Name: "account-1"},
	}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)

	// Mock activities
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{
		Name: "host_group_test", Hosts: datamodel.Hosts{Hosts: []string{"iqn.1993-08.org.debian:01:f2c983feb27", "iqn.1994-05.com.redhat:19ee49a2145f"}}},
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name:         "lun_test",
			ExternalUUID: "lun-uuid",
		},
		SerialNumber: "6c5738423724595454686164",
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateLunMap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("test-token", nil)
	s.env.OnActivity(volumeCreateActivity.FindTenancy, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.TenancyInfo{RegionalTenantProject: "tenant-project"}, nil)
	s.env.OnActivity(volumeCreateActivity.CheckBackupVaultExistsInVCP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CheckForBucketResourceName, mock.Anything, mock.Anything).Return(&common.BucketDetails{}, nil)
	s.env.OnActivity(volumeCreateActivity.GenerateResourceNames, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to generate resource names"))

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume, nil, nil)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_CreateBucketError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	volume := &datamodel.Volume{
		Pool:             &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, Username: "username", Password: "password"},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}},
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "backup-vault-id",
		},
		Account: &datamodel.Account{Name: "account-1"},
	}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)

	// Mock activities
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{
		Name: "host_group_test", Hosts: datamodel.Hosts{Hosts: []string{"iqn.1993-08.org.debian:01:f2c983feb27", "iqn.1994-05.com.redhat:19ee49a2145f"}}},
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name:         "lun_test",
			ExternalUUID: "lun-uuid",
		},
		SerialNumber: "6c5738423724595454686164",
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateLunMap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("test-token", nil)
	s.env.OnActivity(volumeCreateActivity.FindTenancy, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.TenancyInfo{RegionalTenantProject: "tenant-project"}, nil)
	s.env.OnActivity(volumeCreateActivity.CheckBackupVaultExistsInVCP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CheckForBucketResourceName, mock.Anything, mock.Anything).Return(&common.BucketDetails{}, nil)
	s.env.OnActivity(volumeCreateActivity.GenerateResourceNames, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.ResourceNames{BucketName: "bucket-1"}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateBucket, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to create bucket"))

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume, nil, nil)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_UpdateBackupVaultWithBucketDetailsError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	volume := &datamodel.Volume{
		Pool:             &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, Username: "username", Password: "password"},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}},
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "backup-vault-id",
		},
		Account: &datamodel.Account{Name: "account-1"},
	}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)

	// Mock activities
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{
		Name: "host_group_test", Hosts: datamodel.Hosts{Hosts: []string{"iqn.1993-08.org.debian:01:f2c983feb27", "iqn.1994-05.com.redhat:19ee49a2145f"}}},
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name:         "lun_test",
			ExternalUUID: "lun-uuid",
		},
		SerialNumber: "6c5738423724595454686164",
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateLunMap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("test-token", nil)
	s.env.OnActivity(volumeCreateActivity.FindTenancy, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.TenancyInfo{RegionalTenantProject: "tenant-project"}, nil)
	s.env.OnActivity(volumeCreateActivity.CheckBackupVaultExistsInVCP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CheckForBucketResourceName, mock.Anything, mock.Anything).Return(&common.BucketDetails{}, nil)
	s.env.OnActivity(volumeCreateActivity.GenerateResourceNames, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.ResourceNames{BucketName: "bucket-1"}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateBucket, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.BucketDetails{BucketName: "bucket-1", ServiceAccountName: "sa-1", TenantProjectNumber: "tp-1", VendorSubnetID: "vendor1"}, nil)
	s.env.OnActivity(volumeCreateActivity.UpdateBackupVaultWithBucketDetails, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to update backup vault with bucket details"))

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume, nil, nil)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_InitiateSplitForVolumeError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	volume := &datamodel.Volume{
		Pool:             &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, Username: "username", Password: "password"},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}},
		Account:          &datamodel.Account{Name: "account-1"},
	}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)

	// Mock activities
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{
		Name: "host_group_test", Hosts: datamodel.Hosts{Hosts: []string{"iqn.1993-08.org.debian:01:f2c983feb27", "iqn.1994-05.com.redhat:19ee49a2145f"}}},
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name:         "lun_test",
			ExternalUUID: "lun-uuid",
		},
		SerialNumber: "6c5738423724595454686164",
	}, nil)

	s.env.OnActivity(volumeCreateActivity.InitiateSplitForVolume, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to initiate split for volume"))

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume, nil, nil)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_CreateSnapshotPolicyInONTAP() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	volume := &datamodel.Volume{
		Pool:             &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, Username: "username", Password: "password"},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}},
		SnapshotPolicy: &datamodel.SnapshotPolicy{
			Name:      "policy1",
			IsEnabled: true,
			Schedules: []*datamodel.SnapshotPolicySchedule{
				{
					DaysOfMonth:     []int{1, 15},
					DaysOfWeek:      []int{2},
					Hours:           []int{3},
					Minutes:         []int{0},
					SnapmirrorLabel: "label1",
					Count:           5,
				},
			},
		},
	}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)
	s.env.RegisterActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP)
	s.env.RegisterActivity(volumeCreateActivity.InitiateSplitForVolume)

	// Mock activities
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{
		Name: "host_group_test", Hosts: datamodel.Hosts{Hosts: []string{"iqn.1993-08.org.debian:01:f2c983feb27"}},
	}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name:         "lun_test",
			ExternalUUID: "lun-uuid",
		},
		SerialNumber: "serial-1",
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateLunMap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow (success path)
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume, nil, nil)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)

	// Now test error path for CreateSnapshotPolicyInONTAP
	s.env = s.NewTestWorkflowEnvironment()
	s.env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
	mockHeader := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{
			"logParam": encodedValue,
		},
	}
	s.env.SetHeader(mockHeader)
	s.env.RegisterWorkflow(CreateVolumeWorkflow)
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)
	s.env.RegisterActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{
		Name: "host_group_test", Hosts: datamodel.Hosts{Hosts: []string{"iqn.1993-08.org.debian:01:f2c983feb27"}},
	}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name:         "lun_test",
			ExternalUUID: "lun-uuid",
		},
		SerialNumber: "serial-1",
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateLunMap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("snapshot policy error"))
	s.env.OnActivity(volumeCreateActivity.InitiateSplitForVolume, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume, nil, nil)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

func (s *UnitTestSuite) Test_RestoreVolumeWorkflow_Success() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	backupActivity := activities.BackupActivity{SE: mockStorage}
	volume := &datamodel.Volume{
		Pool:             &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, Username: "username", SecretID: "secretid"},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}, IsDataProtection: true},
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
		Attributes:    &datamodel.BackupAttributes{BucketName: "test-bucket"},
	}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)
	s.env.RegisterActivity(volumeCreateActivity.InitiateSplitForVolume)

	// Mock activities
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{}}, nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupActivity.GetOrCreateObjectStore, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.CloudTarget{}, nil)
	s.env.OnActivity(backupActivity.SnapmirrorGetorCreate, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.SnapmirrorRelationship{UUID: "uuid", DestinationUUID: nillable.ToPointer("snapmirror-uuid")}, nil)
	s.env.OnActivity(backupActivity.SnapmirrorTransfer, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupActivity.SnapmirrorTransferPoll, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateLunName, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name:         "lun_test",
			ExternalUUID: "lun-uuid",
		},
		SerialNumber: "6c5738423724595454686164",
	}, nil)

	s.env.OnActivity(volumeCreateActivity.CreateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name:         "lun_test",
			ExternalUUID: "lun-uuid",
		},
		SerialNumber: "6c5738423724595454686164",
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateLunMap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.InitiateSplitForVolume, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{BackupPath: "projects/123456789/locations/us-e4/backupVaults/bv1/backups/backupName"}, volume, backupVault, backup)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *UnitTestSuite) Test_RestoreVolumeWorkflow_GetOrCreateObjectStoreError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	backupActivity := activities.BackupActivity{SE: mockStorage}
	volume := &datamodel.Volume{
		Pool:             &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, Username: "username", SecretID: "secretid"},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}, IsDataProtection: true},
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
		Attributes:    &datamodel.BackupAttributes{BucketName: "test-bucket"},
	}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)
	s.env.RegisterActivity(volumeCreateActivity.InitiateSplitForVolume)

	// Mock activities
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{}}, nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupActivity.GetOrCreateObjectStore, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.CloudTarget{}, errors.New("failed to get or create object store"))
	s.env.OnActivity(backupActivity.SnapmirrorGetorCreate, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.SnapmirrorRelationship{UUID: "uuid", DestinationUUID: nillable.ToPointer("snapmirror-uuid")}, nil)
	s.env.OnActivity(backupActivity.SnapmirrorTransfer, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupActivity.SnapmirrorTransferPoll, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateLunName, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name:         "lun_test",
			ExternalUUID: "lun-uuid",
		},
		SerialNumber: "6c5738423724595454686164",
	}, nil)

	s.env.OnActivity(volumeCreateActivity.CreateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name:         "lun_test",
			ExternalUUID: "lun-uuid",
		},
		SerialNumber: "6c5738423724595454686164",
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateLunMap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.InitiateSplitForVolume, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{BackupPath: "projects/123456789/locations/us-e4/backupVaults/bv1/backups/backupName"}, volume, backupVault, backup)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "workflow execution error")
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *UnitTestSuite) Test_RestoreVolumeWorkflow_GetOrCreateSnapmirrorError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	backupActivity := activities.BackupActivity{SE: mockStorage}
	volume := &datamodel.Volume{
		Pool:             &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, Username: "username", SecretID: "secretid"},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}, IsDataProtection: true},
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
		Attributes:    &datamodel.BackupAttributes{BucketName: "test-bucket"},
	}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)
	s.env.RegisterActivity(volumeCreateActivity.InitiateSplitForVolume)

	// Mock activities
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{}}, nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupActivity.GetOrCreateObjectStore, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.CloudTarget{}, nil)
	s.env.OnActivity(backupActivity.SnapmirrorGetorCreate, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.SnapmirrorRelationship{UUID: "uuid", DestinationUUID: nillable.ToPointer("snapmirror-uuid")}, errors.New("failed to get or snapmirror"))
	s.env.OnActivity(backupActivity.SnapmirrorTransfer, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupActivity.SnapmirrorTransferPoll, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateLunName, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name:         "lun_test",
			ExternalUUID: "lun-uuid",
		},
		SerialNumber: "6c5738423724595454686164",
	}, nil)

	s.env.OnActivity(volumeCreateActivity.CreateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name:         "lun_test",
			ExternalUUID: "lun-uuid",
		},
		SerialNumber: "6c5738423724595454686164",
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateLunMap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.InitiateSplitForVolume, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{BackupPath: "projects/123456789/locations/us-e4/backupVaults/bv1/backups/backupName"}, volume, backupVault, backup)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "workflow execution error")
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *UnitTestSuite) Test_RestoreVolumeWorkflow_TransferSnapmirrorError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	backupActivity := activities.BackupActivity{SE: mockStorage}
	volume := &datamodel.Volume{
		Pool:             &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, Username: "username", SecretID: "secretid"},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}, IsDataProtection: true},
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
		Attributes:    &datamodel.BackupAttributes{BucketName: "test-bucket"},
	}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)
	s.env.RegisterActivity(volumeCreateActivity.InitiateSplitForVolume)

	// Mock activities
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{}}, nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupActivity.GetOrCreateObjectStore, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.CloudTarget{}, nil)
	s.env.OnActivity(backupActivity.SnapmirrorGetorCreate, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.SnapmirrorRelationship{UUID: "uuid", DestinationUUID: nillable.ToPointer("snapmirror-uuid")}, nil)
	s.env.OnActivity(backupActivity.SnapmirrorTransfer, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed in snapmirror transfer"))
	s.env.OnActivity(backupActivity.SnapmirrorTransferPoll, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateLunName, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name:         "lun_test",
			ExternalUUID: "lun-uuid",
		},
		SerialNumber: "6c5738423724595454686164",
	}, nil)

	s.env.OnActivity(volumeCreateActivity.CreateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name:         "lun_test",
			ExternalUUID: "lun-uuid",
		},
		SerialNumber: "6c5738423724595454686164",
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateLunMap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.InitiateSplitForVolume, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{BackupPath: "projects/123456789/locations/us-e4/backupVaults/bv1/backups/backupName"}, volume, backupVault, backup)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "workflow execution error")
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *UnitTestSuite) Test_RestoreVolumeWorkflow_TransferPollSnapmirrorError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	backupActivity := activities.BackupActivity{SE: mockStorage}
	volume := &datamodel.Volume{
		Pool:             &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, Username: "username", SecretID: "secretid"},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}, IsDataProtection: true},
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
		Attributes:    &datamodel.BackupAttributes{BucketName: "test-bucket"},
	}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)
	s.env.RegisterActivity(volumeCreateActivity.InitiateSplitForVolume)

	// Mock activities
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{}}, nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupActivity.GetOrCreateObjectStore, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.CloudTarget{}, nil)
	s.env.OnActivity(backupActivity.SnapmirrorGetorCreate, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.SnapmirrorRelationship{UUID: "uuid", DestinationUUID: nillable.ToPointer("snapmirror-uuid")}, nil)
	s.env.OnActivity(backupActivity.SnapmirrorTransfer, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupActivity.SnapmirrorTransferPoll, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed in snapmirror transfer poll"))
	s.env.OnActivity(volumeCreateActivity.UpdateLunName, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name:         "lun_test",
			ExternalUUID: "lun-uuid",
		},
		SerialNumber: "6c5738423724595454686164",
	}, nil)

	s.env.OnActivity(volumeCreateActivity.CreateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name:         "lun_test",
			ExternalUUID: "lun-uuid",
		},
		SerialNumber: "6c5738423724595454686164",
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateLunMap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.InitiateSplitForVolume, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{BackupPath: "projects/123456789/locations/us-e4/backupVaults/bv1/backups/backupName"}, volume, backupVault, backup)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "workflow execution error")
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *UnitTestSuite) Test_RestoreVolumeWorkflow_UpdateLunError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	backupActivity := activities.BackupActivity{SE: mockStorage}
	volume := &datamodel.Volume{
		Pool:             &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, Username: "username", SecretID: "secretid"},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}, IsDataProtection: true},
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
		Attributes:    &datamodel.BackupAttributes{BucketName: "test-bucket"},
	}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)
	s.env.RegisterActivity(volumeCreateActivity.InitiateSplitForVolume)

	// Mock activities
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{}}, nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupActivity.GetOrCreateObjectStore, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.CloudTarget{}, nil)
	s.env.OnActivity(backupActivity.SnapmirrorGetorCreate, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.SnapmirrorRelationship{UUID: "uuid", DestinationUUID: nillable.ToPointer("snapmirror-uuid")}, nil)
	s.env.OnActivity(backupActivity.SnapmirrorTransfer, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupActivity.SnapmirrorTransferPoll, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateLunName, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name:         "lun_test",
			ExternalUUID: "lun-uuid",
		},
		SerialNumber: "6c5738423724595454686164",
	}, errors.New("failed while updating Lun"))

	s.env.OnActivity(volumeCreateActivity.CreateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name:         "lun_test",
			ExternalUUID: "lun-uuid",
		},
		SerialNumber: "6c5738423724595454686164",
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateLunMap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.InitiateSplitForVolume, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{BackupPath: "projects/123456789/locations/us-e4/backupVaults/bv1/backups/backupName"}, volume, backupVault, backup)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "workflow execution error")
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}
