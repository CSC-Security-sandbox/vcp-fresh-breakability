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
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("test-token", nil)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{}}, nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
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
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
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
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("test-token", nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{
		Name: "host_group_test", Hosts: datamodel.Hosts{Hosts: []string{"iqn.1993-08.org.debian:01:f2c983feb27", "iqn.1994-05.com.redhat:19ee49a2145f"}}},
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
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
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume)

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
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("test-token", nil)
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
	s.env.OnActivity(volumeCreateActivity.FindTenancy, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to find tenancy"))

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume)

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
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("test-token", nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{
		Name: "host_group_test", Hosts: datamodel.Hosts{Hosts: []string{"iqn.1993-08.org.debian:01:f2c983feb27", "iqn.1994-05.com.redhat:19ee49a2145f"}}},
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name:         "lun_test",
			ExternalUUID: "lun-uuid",
		},
		SerialNumber: "6c5738423724595454686164",
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateLunMap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.FindTenancy, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.TenancyInfo{RegionalTenantProject: "tenant-project"}, nil)
	s.env.OnActivity(volumeCreateActivity.CheckBackupVaultExistsInVCP, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to check backup vault exists in VCP"))

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume)

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
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("test-token", nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return(&datamodel.Node{EndpointAddress: "127.0.0.1"}, nil)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{
		Name: "host_group_test", Hosts: datamodel.Hosts{Hosts: []string{"iqn.1993-08.org.debian:01:f2c983feb27", "iqn.1994-05.com.redhat:19ee49a2145f"}}},
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
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
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume)

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
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("test-token", nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{
		Name: "host_group_test", Hosts: datamodel.Hosts{Hosts: []string{"iqn.1993-08.org.debian:01:f2c983feb27", "iqn.1994-05.com.redhat:19ee49a2145f"}}},
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name:         "lun_test",
			ExternalUUID: "lun-uuid",
		},
		SerialNumber: "6c5738423724595454686164",
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateLunMap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.FindTenancy, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.TenancyInfo{RegionalTenantProject: "tenant-project"}, nil)
	s.env.OnActivity(volumeCreateActivity.CheckBackupVaultExistsInVCP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CheckForBucketResourceName, mock.Anything, mock.Anything).Return(nil, errors.New("failed to check for bucket resource name"))

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume)

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
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("test-token", nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{
		Name: "host_group_test", Hosts: datamodel.Hosts{Hosts: []string{"iqn.1993-08.org.debian:01:f2c983feb27", "iqn.1994-05.com.redhat:19ee49a2145f"}}},
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name:         "lun_test",
			ExternalUUID: "lun-uuid",
		},
		SerialNumber: "6c5738423724595454686164",
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateLunMap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.FindTenancy, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.TenancyInfo{RegionalTenantProject: "tenant-project"}, nil)
	s.env.OnActivity(volumeCreateActivity.CheckBackupVaultExistsInVCP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CheckForBucketResourceName, mock.Anything, mock.Anything).Return(&common.BucketDetails{}, nil)
	s.env.OnActivity(volumeCreateActivity.GenerateResourceNames, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to generate resource names"))

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume)

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
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("test-token", nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{
		Name: "host_group_test", Hosts: datamodel.Hosts{Hosts: []string{"iqn.1993-08.org.debian:01:f2c983feb27", "iqn.1994-05.com.redhat:19ee49a2145f"}}},
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name:         "lun_test",
			ExternalUUID: "lun-uuid",
		},
		SerialNumber: "6c5738423724595454686164",
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateLunMap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.FindTenancy, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.TenancyInfo{RegionalTenantProject: "tenant-project"}, nil)
	s.env.OnActivity(volumeCreateActivity.CheckBackupVaultExistsInVCP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CheckForBucketResourceName, mock.Anything, mock.Anything).Return(&common.BucketDetails{}, nil)
	s.env.OnActivity(volumeCreateActivity.GenerateResourceNames, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.ResourceNames{BucketName: "bucket-1"}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateBucket, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to create bucket"))

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume)

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
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("test-token", nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{
		Name: "host_group_test", Hosts: datamodel.Hosts{Hosts: []string{"iqn.1993-08.org.debian:01:f2c983feb27", "iqn.1994-05.com.redhat:19ee49a2145f"}}},
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name:         "lun_test",
			ExternalUUID: "lun-uuid",
		},
		SerialNumber: "6c5738423724595454686164",
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateLunMap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.FindTenancy, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.TenancyInfo{RegionalTenantProject: "tenant-project"}, nil)
	s.env.OnActivity(volumeCreateActivity.CheckBackupVaultExistsInVCP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CheckForBucketResourceName, mock.Anything, mock.Anything).Return(&common.BucketDetails{}, nil)
	s.env.OnActivity(volumeCreateActivity.GenerateResourceNames, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.ResourceNames{BucketName: "bucket-1"}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateBucket, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.BucketDetails{BucketName: "bucket-1", ServiceAccountName: "sa-1", TenantProjectNumber: "tp-1", VendorSubnetID: "vendor1"}, nil)
	s.env.OnActivity(volumeCreateActivity.UpdateBackupVaultWithBucketDetails, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to update backup vault with bucket details"))

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume)

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
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("test-token", nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{
		Name: "host_group_test", Hosts: datamodel.Hosts{Hosts: []string{"iqn.1993-08.org.debian:01:f2c983feb27", "iqn.1994-05.com.redhat:19ee49a2145f"}}},
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
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
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume)

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
	s.env.RegisterActivity(commonActivity.GetAuthJWTToken)
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)
	s.env.RegisterActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP)
	s.env.RegisterActivity(volumeCreateActivity.InitiateSplitForVolume)

	// Mock activities
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("test-token", nil)
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
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow (success path)
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume)
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
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("snapshot policy error"))
	s.env.OnActivity(volumeCreateActivity.InitiateSplitForVolume, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}
