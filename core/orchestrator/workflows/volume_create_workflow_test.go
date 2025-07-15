package workflows

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
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
	s.env.RegisterWorkflow(PreBlockVolumeWorkflow)
	s.env.RegisterWorkflow(PostBlockVolumeWorkflow)
	s.env.RegisterWorkflow(PreFileVolumeWorkflow)
	s.env.RegisterWorkflow(PostFileVolumeWorkflow)
}

func (s *UnitTestSuite) AfterTest() {
	s.env.AssertExpectations(s.T())
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_Success() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	volumeDeleteActivity := activities.VolumeDeleteActivity{SE: mockStorage}
	volume := &datamodel.Volume{
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
		},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}, Protocols: []string{utils.ProtocolISCSI}},
	}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)
	s.env.RegisterActivity(volumeCreateActivity.InitiateSplitForVolume)
	s.env.RegisterActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP)
	s.env.RegisterActivity(volumeCreateActivity.CreateVolumeInONTAP)
	s.env.RegisterActivity(volumeCreateActivity.GetHosts)
	s.env.RegisterActivity(volumeCreateActivity.CreateIgroup)
	s.env.RegisterActivity(volumeCreateActivity.CreateLun)
	s.env.RegisterActivity(volumeCreateActivity.CreateLunMap)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeStateInDB)
	s.env.RegisterActivity(volumeDeleteActivity.DeleteVolumeInONTAP)
	s.env.RegisterActivity(volumeDeleteActivity.DeleteSnapshotPolicyInONTAP)

	// Mock activities
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{}}, nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeDeleteActivity.DeleteSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeDeleteActivity.DeleteVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
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

	// Assert workflow completed successfully
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
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			}},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}, IsDataProtection: true, Protocols: []string{utils.ProtocolISCSI}},
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
	s.env.OnActivity(backupCreateActivity.GetSnapmirrorTransferStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("success", nil)
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
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			}},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}, Protocols: []string{utils.ProtocolISCSI}},
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
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			}},
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
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, PoolCredentials: &datamodel.PoolCredentials{
			Password: "password",
		}},
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
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			}},
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

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_GetBackupPolicyError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	volume := &datamodel.Volume{
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, PoolCredentials: &datamodel.PoolCredentials{
			SecretID: "",
			Password: "password",
		}},
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
	s.env.RegisterActivity(volumeCreateActivity.InitiateSplitForVolume)

	// Mock activities
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("test-token", nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{
		Name: "host_group_test", Hosts: datamodel.Hosts{Hosts: []string{"iqn.1993-08.org.debian:01:f2c983feb27", "iqn.1994-05.com.redhat:19ee49a2145f"}}},
	}, nil)
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
	s.env.OnActivity(volumeCreateActivity.CheckIfBackupPolicyExistsInVCP, mock.Anything, mock.Anything, mock.Anything).Return(false, errors.New("failed to check backup policy exists in VCP"))

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume, nil, nil)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_CreateBackupPolicyFetchedFromSDEError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	volume := &datamodel.Volume{
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, PoolCredentials: &datamodel.PoolCredentials{
			SecretID: "",
			Password: "password",
		}},
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
	s.env.RegisterActivity(volumeCreateActivity.InitiateSplitForVolume)

	// Mock activities
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("test-token", nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return(&datamodel.Node{EndpointAddress: "127.0.0.1"}, nil)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{
		Name: "host_group_test", Hosts: datamodel.Hosts{Hosts: []string{"iqn.1993-08.org.debian:01:f2c983feb27", "iqn.1994-05.com.redhat:19ee49a2145f"}}},
	}, nil)
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
	s.env.OnActivity(volumeCreateActivity.CheckIfBackupPolicyExistsInVCP, mock.Anything, mock.Anything, mock.Anything).Return(false, nil)
	s.env.OnActivity(volumeCreateActivity.CreateBackupPolicyFetchedFromSDE, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to check backup policy in SDE"))

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume, nil, nil)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_CreateBackupPolicyInVCPSucceeds() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	volume := &datamodel.Volume{
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, PoolCredentials: &datamodel.PoolCredentials{
			SecretID: "",
			Password: "password",
		}},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}, Protocols: []string{utils.ProtocolISCSI}},
		DataProtection: &datamodel.DataProtection{
			BackupPolicyID: "backup-policy-id",
		},
		Account: &datamodel.Account{Name: "account-1"},
	}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)
	s.env.RegisterActivity(volumeCreateActivity.InitiateSplitForVolume)

	// Mock activities
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("test-token", nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{
		Name: "host_group_test", Hosts: datamodel.Hosts{Hosts: []string{"iqn.1993-08.org.debian:01:f2c983feb27", "iqn.1994-05.com.redhat:19ee49a2145f"}}},
	}, nil)
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
	s.env.OnActivity(volumeCreateActivity.CheckIfBackupPolicyExistsInVCP, mock.Anything, mock.Anything, mock.Anything).Return(false, nil)
	s.env.OnActivity(volumeCreateActivity.CreateBackupPolicyFetchedFromSDE, mock.Anything, mock.Anything, mock.Anything).Return(&datamodel.BackupPolicy{
		BaseModel: datamodel.BaseModel{
			UUID: "backup-policy-uuid",
		},
		Name: "backup-policy-name",
	}, nil)
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

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_CheckForBucketResourceNameError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	volume := &datamodel.Volume{
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, PoolCredentials: &datamodel.PoolCredentials{
			SecretID: "",
			Password: "password",
		}},
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
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, PoolCredentials: &datamodel.PoolCredentials{
			SecretID: "",
			Password: "password",
		}},
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
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, PoolCredentials: &datamodel.PoolCredentials{
			Password:      "password",
			SecretID:      "",
			CertificateID: "",
		},
		},
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
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, PoolCredentials: &datamodel.PoolCredentials{
			Password:      "password",
			SecretID:      "",
			CertificateID: "",
		}},
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
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, PoolCredentials: &datamodel.PoolCredentials{
			Password:      "password",
			SecretID:      "",
			CertificateID: "",
		}},
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
	volumeDeleteActivity := activities.VolumeDeleteActivity{SE: mockStorage}
	volume := &datamodel.Volume{
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			}},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}, Protocols: []string{utils.ProtocolISCSI}},
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
	s.env.RegisterActivity(commonActivity.GetNode)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)
	s.env.RegisterActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP)
	s.env.RegisterActivity(volumeCreateActivity.CreateVolumeInONTAP)
	s.env.RegisterActivity(volumeCreateActivity.GetHosts)
	s.env.RegisterActivity(volumeCreateActivity.CreateIgroup)
	s.env.RegisterActivity(volumeCreateActivity.CreateLun)
	s.env.RegisterActivity(volumeCreateActivity.CreateLunMap)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeStateInDB)
	s.env.RegisterActivity(volumeCreateActivity.InitiateSplitForVolume)
	s.env.RegisterActivity(volumeDeleteActivity.DeleteVolumeInONTAP)
	s.env.RegisterActivity(volumeDeleteActivity.DeleteSnapshotPolicyInONTAP)

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
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeDeleteActivity.DeleteVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeDeleteActivity.DeleteSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

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
	s.env.RegisterWorkflow(PreBlockVolumeWorkflow)
	s.env.RegisterWorkflow(PostBlockVolumeWorkflow)
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(commonActivity.GetNode)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)
	s.env.RegisterActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP)
	s.env.RegisterActivity(volumeCreateActivity.CreateVolumeInONTAP)
	s.env.RegisterActivity(volumeCreateActivity.GetHosts)
	s.env.RegisterActivity(volumeCreateActivity.CreateIgroup)
	s.env.RegisterActivity(volumeCreateActivity.CreateLun)
	s.env.RegisterActivity(volumeCreateActivity.CreateLunMap)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeStateInDB)
	s.env.RegisterActivity(volumeCreateActivity.InitiateSplitForVolume)
	s.env.RegisterActivity(volumeDeleteActivity.DeleteVolumeInONTAP)
	s.env.RegisterActivity(volumeDeleteActivity.DeleteSnapshotPolicyInONTAP)
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
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeDeleteActivity.DeleteVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeDeleteActivity.DeleteSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
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
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			}},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}, IsDataProtection: true, Protocols: []string{utils.ProtocolISCSI}},
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
	s.env.OnActivity(backupActivity.GetSnapmirrorTransferStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("success", nil)
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
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, PoolCredentials: &datamodel.PoolCredentials{
			Password:      "password",
			SecretID:      "",
			CertificateID: "",
		}},
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
	s.env.OnActivity(backupActivity.GetSnapmirrorTransferStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("success", nil)
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
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, PoolCredentials: &datamodel.PoolCredentials{
			Password:      "password",
			SecretID:      "",
			CertificateID: "",
		}},
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
	s.env.OnActivity(backupActivity.GetSnapmirrorTransferStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("success", nil)
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
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, PoolCredentials: &datamodel.PoolCredentials{
			Password:      "password",
			SecretID:      "",
			CertificateID: "",
		}},
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
	s.env.OnActivity(backupActivity.SnapmirrorTransfer, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("failed", errors.New("failed in snapmirror transfer"))
	s.env.OnActivity(backupActivity.GetSnapmirrorTransferStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
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
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, PoolCredentials: &datamodel.PoolCredentials{
			Password:      "password",
			SecretID:      "",
			CertificateID: "",
		}},
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
	s.env.OnActivity(backupActivity.GetSnapmirrorTransferStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("failed", errors.New("failed in snapmirror transfer poll"))
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
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, PoolCredentials: &datamodel.PoolCredentials{
			Password:      "password",
			SecretID:      "",
			CertificateID: "",
		}},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}, IsDataProtection: true, Protocols: []string{utils.ProtocolISCSI}},
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
	s.env.OnActivity(backupActivity.GetSnapmirrorTransferStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("success", nil)
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

func (s *UnitTestSuite) Test_PreFileVolumeWorkflow_Success() {
	// Test PreFileVolumeWorkflow with file protocol
	volume := &datamodel.Volume{
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			}},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{Protocols: []string{utils.ProtocolNFSv3}},
	}
	node := &models.Node{EndpointAddress: "127.0.0.1"}

	// Enable file protocols for testing
	originalValue := utils.FileProtocolSupported
	utils.FileProtocolSupported = true
	defer func() { utils.FileProtocolSupported = originalValue }()

	// Execute the workflow
	s.env.ExecuteWorkflow(PreFileVolumeWorkflow, volume, node)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
}

func (s *UnitTestSuite) Test_PreFileVolumeWorkflow_FileProtocolsDisabled() {
	// Test PreFileVolumeWorkflow when file protocols are disabled
	volume := &datamodel.Volume{
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			}},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{Protocols: []string{utils.ProtocolNFSv3}},
	}
	node := &models.Node{EndpointAddress: "127.0.0.1"}

	// Disable file protocols for testing
	originalValue := utils.FileProtocolSupported
	utils.FileProtocolSupported = false
	defer func() { utils.FileProtocolSupported = originalValue }()

	// Execute the workflow
	s.env.ExecuteWorkflow(PreFileVolumeWorkflow, volume, node)

	// Assert workflow completed successfully (should handle disabled protocols gracefully)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
}

func (s *UnitTestSuite) Test_PostFileVolumeWorkflow_Success() {
	// Test PostFileVolumeWorkflow with file protocol
	volume := &datamodel.Volume{
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			}},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{Protocols: []string{utils.ProtocolNFSv3}},
	}
	node := &models.Node{EndpointAddress: "127.0.0.1"}
	volCreateResponse := &vsa.VolumeResponse{ProviderResponse: vsa.ProviderResponse{ExternalUUID: "test-uuid"}}
	isRestoreFromBackup := false

	// Enable file protocols for testing
	originalValue := utils.FileProtocolSupported
	utils.FileProtocolSupported = true
	defer func() { utils.FileProtocolSupported = originalValue }()

	// Execute the workflow
	s.env.ExecuteWorkflow(PostFileVolumeWorkflow, volume, node, volCreateResponse, isRestoreFromBackup)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
}

func (s *UnitTestSuite) Test_PostFileVolumeWorkflow_FileProtocolsDisabled() {
	// Test PostFileVolumeWorkflow when file protocols are disabled
	volume := &datamodel.Volume{
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			}},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{Protocols: []string{utils.ProtocolNFSv3}},
	}
	node := &models.Node{EndpointAddress: "127.0.0.1"}
	volCreateResponse := &vsa.VolumeResponse{ProviderResponse: vsa.ProviderResponse{ExternalUUID: "test-uuid"}}
	isRestoreFromBackup := false

	// Disable file protocols for testing
	originalValue := utils.FileProtocolSupported
	utils.FileProtocolSupported = false
	defer func() { utils.FileProtocolSupported = originalValue }()

	// Execute the workflow
	s.env.ExecuteWorkflow(PostFileVolumeWorkflow, volume, node, volCreateResponse, isRestoreFromBackup)

	// Assert workflow completed successfully (should handle disabled protocols gracefully)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
}

func (s *UnitTestSuite) Test_SelectVolumeChildWorkflow_ISCSI() {
	// Test selectVolumeChildWorkflow with ISCSI protocol
	protocols := []string{utils.ProtocolISCSI}

	// Test pre phase
	preWorkflow, err := selectVolumeChildWorkflow(protocols, PhasePre)
	assert.Nil(s.T(), err)
	assert.NotNil(s.T(), preWorkflow)
	// Verify it returns a function that can be called
	assert.IsType(s.T(), PreBlockVolumeWorkflow, preWorkflow)

	// Test post phase
	postWorkflow, err := selectVolumeChildWorkflow(protocols, PhasePost)
	assert.Nil(s.T(), err)
	assert.NotNil(s.T(), postWorkflow)
	// Verify it returns a function that can be called
	assert.IsType(s.T(), PostBlockVolumeWorkflow, postWorkflow)

	// Test invalid phase
	invalidWorkflow, err := selectVolumeChildWorkflow(protocols, "invalid")
	assert.NotNil(s.T(), err)
	assert.Nil(s.T(), invalidWorkflow)
	assert.Contains(s.T(), err.Error(), "invalid phase")
}

func (s *UnitTestSuite) Test_SelectVolumeChildWorkflow_NFSv3() {
	// Test selectVolumeChildWorkflow with NFSv3 protocol
	protocols := []string{utils.ProtocolNFSv3}

	// Enable file protocols for testing
	originalValue := utils.FileProtocolSupported
	utils.FileProtocolSupported = true
	defer func() { utils.FileProtocolSupported = originalValue }()

	// Test pre phase
	preWorkflow, err := selectVolumeChildWorkflow(protocols, PhasePre)
	assert.Nil(s.T(), err)
	assert.NotNil(s.T(), preWorkflow)
	// Verify it returns a function that can be called
	assert.IsType(s.T(), PreFileVolumeWorkflow, preWorkflow)

	// Test post phase
	postWorkflow, err := selectVolumeChildWorkflow(protocols, PhasePost)
	assert.Nil(s.T(), err)
	assert.NotNil(s.T(), postWorkflow)
	// Verify it returns a function that can be called
	assert.IsType(s.T(), PostFileVolumeWorkflow, postWorkflow)
}

func (s *UnitTestSuite) Test_SelectVolumeChildWorkflow_NFSv4() {
	// Test selectVolumeChildWorkflow with NFSv4 protocol
	protocols := []string{utils.ProtocolNFSv4}

	// Enable file protocols for testing
	originalValue := utils.FileProtocolSupported
	utils.FileProtocolSupported = true
	defer func() { utils.FileProtocolSupported = originalValue }()

	// Test pre phase
	preWorkflow, err := selectVolumeChildWorkflow(protocols, PhasePre)
	assert.Nil(s.T(), err)
	assert.NotNil(s.T(), preWorkflow)
	// Verify it returns a function that can be called
	assert.IsType(s.T(), PreFileVolumeWorkflow, preWorkflow)

	// Test post phase
	postWorkflow, err := selectVolumeChildWorkflow(protocols, PhasePost)
	assert.Nil(s.T(), err)
	assert.NotNil(s.T(), postWorkflow)
	// Verify it returns a function that can be called
	assert.IsType(s.T(), PostFileVolumeWorkflow, postWorkflow)
}

func (s *UnitTestSuite) Test_SelectVolumeChildWorkflow_SMB() {
	// Test selectVolumeChildWorkflow with SMB protocol
	protocols := []string{utils.ProtocolSMB}

	// Enable file protocols for testing
	originalValue := utils.FileProtocolSupported
	utils.FileProtocolSupported = true
	defer func() { utils.FileProtocolSupported = originalValue }()

	// Test pre phase
	preWorkflow, err := selectVolumeChildWorkflow(protocols, PhasePre)
	assert.Nil(s.T(), err)
	assert.NotNil(s.T(), preWorkflow)
	// Verify it returns a function that can be called
	assert.IsType(s.T(), PreFileVolumeWorkflow, preWorkflow)
	// Test post phase
	postWorkflow, err := selectVolumeChildWorkflow(protocols, PhasePost)
	assert.Nil(s.T(), err)
	assert.NotNil(s.T(), postWorkflow)
	// Verify it returns a function that can be called
	assert.IsType(s.T(), PostFileVolumeWorkflow, postWorkflow)
}

func (s *UnitTestSuite) Test_SelectVolumeChildWorkflow_FileProtocolsDisabled() {
	// Test selectVolumeChildWorkflow when file protocols are disabled
	protocols := []string{utils.ProtocolNFSv3}

	// Disable file protocols for testing
	originalValue := utils.FileProtocolSupported
	utils.FileProtocolSupported = false
	defer func() { utils.FileProtocolSupported = originalValue }()

	// Test pre phase
	preWorkflow, err := selectVolumeChildWorkflow(protocols, PhasePre)
	assert.NotNil(s.T(), err)
	assert.Nil(s.T(), preWorkflow)
	assert.Contains(s.T(), err.Error(), "file protocols are not enabled")

	// Test post phase
	postWorkflow, err := selectVolumeChildWorkflow(protocols, PhasePost)
	assert.NotNil(s.T(), err)
	assert.Nil(s.T(), postWorkflow)
	assert.Contains(s.T(), err.Error(), "file protocols are not enabled")
}

func (s *UnitTestSuite) Test_SelectVolumeChildWorkflow_UnsupportedProtocol() {
	// Test selectVolumeChildWorkflow with unsupported protocol
	protocols := []string{"UNSUPPORTED_PROTOCOL"}

	// Test pre phase
	preWorkflow, err := selectVolumeChildWorkflow(protocols, PhasePre)
	assert.NotNil(s.T(), err)
	assert.Nil(s.T(), preWorkflow)
	assert.Contains(s.T(), err.Error(), "unsupported or unspecified protocol")

	// Test post phase
	postWorkflow, err := selectVolumeChildWorkflow(protocols, PhasePost)
	assert.NotNil(s.T(), err)
	assert.Nil(s.T(), postWorkflow)
	assert.Contains(s.T(), err.Error(), "unsupported or unspecified protocol")
}

func (s *UnitTestSuite) Test_SelectVolumeChildWorkflow_EmptyProtocols() {
	// Test selectVolumeChildWorkflow with empty protocols
	protocols := []string{}

	// Test pre phase
	preWorkflow, err := selectVolumeChildWorkflow(protocols, PhasePre)
	assert.NotNil(s.T(), err)
	assert.Nil(s.T(), preWorkflow)
	assert.Contains(s.T(), err.Error(), "unsupported or unspecified protocol")

	// Test post phase
	postWorkflow, err := selectVolumeChildWorkflow(protocols, PhasePost)
	assert.NotNil(s.T(), err)
	assert.Nil(s.T(), postWorkflow)
	assert.Contains(s.T(), err.Error(), "unsupported or unspecified protocol")
}

func (s *UnitTestSuite) Test_SelectVolumeChildWorkflow_MixedProtocols() {
	// Test selectVolumeChildWorkflow with mixed protocols (should prioritize ISCSI)
	protocols := []string{utils.ProtocolISCSI, utils.ProtocolNFSv3}

	// Test pre phase
	preWorkflow, err := selectVolumeChildWorkflow(protocols, PhasePre)
	assert.Nil(s.T(), err)
	assert.NotNil(s.T(), preWorkflow)
	// Verify it returns a function that can be called
	assert.IsType(s.T(), PreBlockVolumeWorkflow, preWorkflow)

	// Test post phase
	postWorkflow, err := selectVolumeChildWorkflow(protocols, PhasePost)
	assert.Nil(s.T(), err)
	assert.NotNil(s.T(), postWorkflow)
	// Verify it returns a function that can be called
	assert.IsType(s.T(), PostBlockVolumeWorkflow, postWorkflow)
}

func (s *UnitTestSuite) Test_PostBlockVolumeWorkflow_PopulateRetryPolicyError() {
	// Test PostBlockVolumeWorkflow when PopulateRetryPolicyParams fails
	volume := &datamodel.Volume{
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			}},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}, Protocols: []string{utils.ProtocolISCSI}},
	}
	node := &models.Node{EndpointAddress: "127.0.0.1"}
	volCreateResponse := &vsa.VolumeResponse{ProviderResponse: vsa.ProviderResponse{ExternalUUID: "test-uuid"}}
	isRestoreFromBackup := false

	// Set invalid environment variable to cause PopulateRetryPolicyParams to fail
	originalStartToCloseTimeout := StartToCloseTimeout
	StartToCloseTimeout = "invalid-duration"
	defer func() { StartToCloseTimeout = originalStartToCloseTimeout }()

	// Execute the workflow
	s.env.ExecuteWorkflow(PostBlockVolumeWorkflow, volume, node, volCreateResponse, isRestoreFromBackup)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "invalid duration")
}

func (s *UnitTestSuite) Test_PostBlockVolumeWorkflow_GetHostsError() {
	// Test PostBlockVolumeWorkflow when GetHosts fails
	mockStorage := database.NewMockStorage(s.T())
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}

	volume := &datamodel.Volume{
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			}},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}, Protocols: []string{utils.ProtocolISCSI}},
	}
	node := &models.Node{EndpointAddress: "127.0.0.1"}
	volCreateResponse := &vsa.VolumeResponse{ProviderResponse: vsa.ProviderResponse{ExternalUUID: "test-uuid"}}
	isRestoreFromBackup := false

	// Register activities
	s.env.RegisterActivity(volumeCreateActivity.GetHosts)

	// Mock GetHosts to return error
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return(nil, errors.New("get hosts error"))

	// Execute the workflow
	s.env.ExecuteWorkflow(PostBlockVolumeWorkflow, volume, node, volCreateResponse, isRestoreFromBackup)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "get hosts error")
}

func (s *UnitTestSuite) Test_PostBlockVolumeWorkflow_CreateIgroupError() {
	// Test PostBlockVolumeWorkflow when CreateIgroup fails
	mockStorage := database.NewMockStorage(s.T())
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}

	volume := &datamodel.Volume{
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			}},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}, Protocols: []string{utils.ProtocolISCSI}},
	}
	node := &models.Node{EndpointAddress: "127.0.0.1"}
	volCreateResponse := &vsa.VolumeResponse{ProviderResponse: vsa.ProviderResponse{ExternalUUID: "test-uuid"}}
	isRestoreFromBackup := false

	// Register activities
	s.env.RegisterActivity(volumeCreateActivity.GetHosts)
	s.env.RegisterActivity(volumeCreateActivity.CreateIgroup)

	// Mock activities
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("create igroup error"))

	// Execute the workflow
	s.env.ExecuteWorkflow(PostBlockVolumeWorkflow, volume, node, volCreateResponse, isRestoreFromBackup)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "create igroup error")
}

func (s *UnitTestSuite) Test_PostBlockVolumeWorkflow_UpdateLunNameError() {
	// Test PostBlockVolumeWorkflow when UpdateLunName fails (restore from backup)
	mockStorage := database.NewMockStorage(s.T())
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}

	volume := &datamodel.Volume{
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			}},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}, Protocols: []string{utils.ProtocolISCSI}},
	}
	node := &models.Node{EndpointAddress: "127.0.0.1"}
	volCreateResponse := &vsa.VolumeResponse{ProviderResponse: vsa.ProviderResponse{ExternalUUID: "test-uuid"}}
	isRestoreFromBackup := true

	// Register activities
	s.env.RegisterActivity(volumeCreateActivity.GetHosts)
	s.env.RegisterActivity(volumeCreateActivity.CreateIgroup)
	s.env.RegisterActivity(volumeCreateActivity.UpdateLunName)

	// Mock activities
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateLunName, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("update lun name error"))

	// Execute the workflow
	s.env.ExecuteWorkflow(PostBlockVolumeWorkflow, volume, node, volCreateResponse, isRestoreFromBackup)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "update lun name error")
}

func (s *UnitTestSuite) Test_PostBlockVolumeWorkflow_CreateLunError() {
	// Test PostBlockVolumeWorkflow when CreateLun fails (not restore from backup)
	mockStorage := database.NewMockStorage(s.T())
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}

	volume := &datamodel.Volume{
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			}},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}, Protocols: []string{utils.ProtocolISCSI}},
	}
	node := &models.Node{EndpointAddress: "127.0.0.1"}
	volCreateResponse := &vsa.VolumeResponse{ProviderResponse: vsa.ProviderResponse{ExternalUUID: "test-uuid"}}
	isRestoreFromBackup := false

	// Register activities
	s.env.RegisterActivity(volumeCreateActivity.GetHosts)
	s.env.RegisterActivity(volumeCreateActivity.CreateIgroup)
	s.env.RegisterActivity(volumeCreateActivity.CreateLun)

	// Mock activities
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("create lun error"))

	// Execute the workflow
	s.env.ExecuteWorkflow(PostBlockVolumeWorkflow, volume, node, volCreateResponse, isRestoreFromBackup)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "create lun error")
}

func (s *UnitTestSuite) Test_PostBlockVolumeWorkflow_CreateLunMapError() {
	// Test PostBlockVolumeWorkflow when CreateLunMap fails
	mockStorage := database.NewMockStorage(s.T())
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}

	volume := &datamodel.Volume{
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			}},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}, Protocols: []string{utils.ProtocolISCSI}},
	}
	node := &models.Node{EndpointAddress: "127.0.0.1"}
	volCreateResponse := &vsa.VolumeResponse{ProviderResponse: vsa.ProviderResponse{ExternalUUID: "test-uuid"}}
	isRestoreFromBackup := false

	// Register activities
	s.env.RegisterActivity(volumeCreateActivity.GetHosts)
	s.env.RegisterActivity(volumeCreateActivity.CreateIgroup)
	s.env.RegisterActivity(volumeCreateActivity.CreateLun)
	s.env.RegisterActivity(volumeCreateActivity.CreateLunMap)

	// Mock activities
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name:         "lun_test",
			ExternalUUID: "lun-uuid",
		},
		SerialNumber: "6c5738423724595454686164",
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateLunMap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("create lun map error"))

	// Execute the workflow
	s.env.ExecuteWorkflow(PostBlockVolumeWorkflow, volume, node, volCreateResponse, isRestoreFromBackup)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "create lun map error")
}

func (s *UnitTestSuite) Test_PreFileVolumeWorkflow_PopulateRetryPolicyError() {
	// Test PreFileVolumeWorkflow when PopulateRetryPolicyParams fails
	volume := &datamodel.Volume{
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			}},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{Protocols: []string{utils.ProtocolNFSv3}},
	}
	node := &models.Node{EndpointAddress: "127.0.0.1"}

	// Set invalid environment variable to cause PopulateRetryPolicyParams to fail
	originalStartToCloseTimeout := StartToCloseTimeout
	StartToCloseTimeout = "invalid-duration"
	defer func() { StartToCloseTimeout = originalStartToCloseTimeout }()

	// Execute the workflow
	s.env.ExecuteWorkflow(PreFileVolumeWorkflow, volume, node)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "invalid duration")
}
