package workflows

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
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

	// Register all activities that might be used across tests
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	volumeDeleteActivity := activities.VolumeDeleteActivity{SE: mockStorage}
	backupActivity := activities.BackupActivity{SE: mockStorage}

	// Register common activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(commonActivity.GetNode)
	s.env.RegisterActivity(commonActivity.GetAuthJWTToken)

	// Register volume create activities
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)
	s.env.RegisterActivity(volumeCreateActivity.InitiateSplitForVolume)
	s.env.RegisterActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP)
	s.env.RegisterActivity(volumeCreateActivity.CreateVolumeInONTAP)
	s.env.RegisterActivity(volumeCreateActivity.GetHosts)
	s.env.RegisterActivity(volumeCreateActivity.CreateIgroup)
	s.env.RegisterActivity(volumeCreateActivity.CreateLun)
	s.env.RegisterActivity(volumeCreateActivity.CreateLunMap)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeStateInDB)
	s.env.RegisterActivity(volumeCreateActivity.CreateExportPolicyInOntap)
	s.env.RegisterActivity(volumeCreateActivity.FindTenancy)
	s.env.RegisterActivity(volumeCreateActivity.CheckBackupVaultExistsInVCP)
	s.env.RegisterActivity(volumeCreateActivity.CheckForBucketResourceName)
	s.env.RegisterActivity(volumeCreateActivity.GenerateResourceNames)
	s.env.RegisterActivity(volumeCreateActivity.CreateBucket)
	s.env.RegisterActivity(volumeCreateActivity.UpdateBackupVaultWithBucketDetails)
	s.env.RegisterActivity(volumeCreateActivity.CheckIfBackupPolicyExistsInVCP)
	s.env.RegisterActivity(volumeCreateActivity.CreateBackupPolicyFetchedFromSDE)
	s.env.RegisterActivity(volumeCreateActivity.UpdateLunName)

	// Register volume delete activities
	s.env.RegisterActivity(volumeDeleteActivity.DeleteVolumeInONTAP)
	s.env.RegisterActivity(volumeDeleteActivity.DeleteSnapshotPolicyInONTAP)

	// Register backup activities
	s.env.RegisterActivity(backupActivity.GetOrCreateObjectStore)
	s.env.RegisterActivity(backupActivity.SnapmirrorGetOrCreate)
	s.env.RegisterActivity(backupActivity.SnapmirrorTransfer)
	s.env.RegisterActivity(backupActivity.GetSnapmirrorTransferStatus)

	// Set default mock responses for commonly used activities
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeDeleteActivity.DeleteSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeDeleteActivity.DeleteVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Enable file protocols for testing
	utils.SetFileProtocolSupportedForTesting(true)
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
		Account: &datamodel.Account{Name: "account-1"},
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
		},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}, Protocols: []string{utils.ProtocolISCSI}},
	}

	// Mock activities
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
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
}

func (s *UnitTestSuite) Test_RestoreVolumeWorkflow_Failure() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	backupCreateActivity := activities.BackupActivity{SE: mockStorage}
	volume := &datamodel.Volume{
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
		},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}, IsDataProtection: true, Protocols: []string{utils.ProtocolISCSI}},
	}

	minEnforcedRetentionDuration := int64(30)
	bv := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "backup-vault-uuid"},
		Name:      "bv1",
		ImmutableAttributes: &datamodel.ImmutableAttributes{
			BackupMinimumEnforcedRetentionDuration: &minEnforcedRetentionDuration,
		},
	}
	backup := &datamodel.Backup{VolumeUUID: "463811e7-9760-acf5-9bdb-020073ca3335", State: "creating"}

	// Mock activities
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
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
	s.env.OnActivity(activities.GetObjStoreNameFromBackup, mock.Anything, mock.Anything).Return(bv, nil)
	s.env.OnActivity(activities.GetBucketDetailsFromBackup, mock.Anything, mock.Anything).Return(&datamodel.BucketDetails{}, nil)

	s.env.OnActivity(volumeCreateActivity.CreateLunMap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupCreateActivity.GetOrCreateObjectStore, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.CloudTarget{}, nil)
	s.env.OnActivity(backupCreateActivity.SnapmirrorGetOrCreate, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.SnapmirrorRelationship{UUID: "uuid", DestinationUUID: nillable.ToPointer("snapmirror-uuid")}, nil)
	s.env.OnActivity(backupCreateActivity.SnapmirrorTransfer, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	// Mock workflow.Sleep to return error
	s.env.OnWorkflow(workflow.Sleep, mock.Anything, mock.Anything).Return(nil, errors.New("failed to sleep during workflow execution"))
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
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_Failure_UpdateVolumeDetails() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	volume := &datamodel.Volume{
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
		},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}, Protocols: []string{utils.ProtocolISCSI}},
	}

	// Mock activities
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
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

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)

	// Mock activities
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
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
	// UpdateJob is called through UpdateJobStatus activity, not directly on mock
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

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)

	// Mock activities
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
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
	// UpdateJob is called through UpdateJobStatus activity, not directly on mock
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

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)

	// Mock activities
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
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
	s.env.OnActivity(volumeCreateActivity.CheckBackupVaultExistsInVCP, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to check backup vault exists in VCP"))

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume, nil, nil)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	// UpdateJob is called through UpdateJobStatus activity, not directly on mock
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
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}, Protocols: []string{utils.ProtocolISCSI}},
		DataProtection: &datamodel.DataProtection{
			BackupPolicyID: "backup-policy-id",
		},
		Account: &datamodel.Account{Name: "account-1"},
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)
	s.env.RegisterActivity(volumeCreateActivity.InitiateSplitForVolume)

	// Mock activities
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
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
	// UpdateJob is called through UpdateJobStatus activity, not directly on mock
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
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}, Protocols: []string{utils.ProtocolISCSI}},
		DataProtection: &datamodel.DataProtection{
			BackupPolicyID: "backup-policy-id",
		},
		Account: &datamodel.Account{Name: "account-1"},
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)
	s.env.RegisterActivity(volumeCreateActivity.InitiateSplitForVolume)

	// Mock activities
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
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
	// UpdateJob is called through UpdateJobStatus activity, not directly on mock
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_CreateBackupPolicyScheduleError() {
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

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)
	s.env.RegisterActivity(volumeCreateActivity.InitiateSplitForVolume)

	// Mock activities
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
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
	s.env.OnActivity(volumeCreateActivity.CreateBackupPolicySchedule, mock.Anything, mock.Anything).Return(errors.New("failed to create backup policy schedule"))

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume, nil, nil)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	// UpdateJob is called through UpdateJobStatus activity, not directly on mock
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

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)
	s.env.RegisterActivity(volumeCreateActivity.InitiateSplitForVolume)

	// Mock activities
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
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
	s.env.OnActivity(volumeCreateActivity.CreateBackupPolicySchedule, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume, nil, nil)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	// UpdateJob is called through UpdateJobStatus activity, not directly on mock
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

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)

	// Mock activities
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
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
	// UpdateJob is called through UpdateJobStatus activity, not directly on mock
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

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)

	// Mock activities
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
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
	// UpdateJob is called through UpdateJobStatus activity, not directly on mock
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

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)

	// Mock activities
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
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
	// UpdateJob is called through UpdateJobStatus activity, not directly on mock
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

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)

	// Mock activities
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
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
	// UpdateJob is called through UpdateJobStatus activity, not directly on mock
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

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)

	// Mock activities
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
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
	// UpdateJob is called through UpdateJobStatus activity, not directly on mock
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_CreateSnapshotPolicyInONTAP() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	volumeDeleteActivity := activities.VolumeDeleteActivity{SE: mockStorage}
	volume := &datamodel.Volume{
		Account: &datamodel.Account{Name: "account-1"},
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
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
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
	// UpdateJob is called through UpdateJobStatus activity, not directly on mock

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
		Account: &datamodel.Account{Name: "account-1"},
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
		Attributes:    &datamodel.BackupAttributes{BucketName: "test-bucket", EndpointUUID: "test-endpoint-uuid"},
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)
	s.env.RegisterActivity(volumeCreateActivity.InitiateSplitForVolume)
	s.env.RegisterActivity(backupActivity.GetSmSourcePathActivity)
	s.env.RegisterActivity(backupActivity.GetSmSourcePathForRestoreActivity)
	s.env.RegisterActivity(activities.GetObjStoreNameFromBackup)
	s.env.RegisterActivity(activities.GetBucketDetailsFromBackup)

	// Mock activities
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{}}, nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupActivity.GetSmSourcePathActivity, mock.Anything, mock.Anything).Return("test-destination-path", nil)
	s.env.OnActivity(backupActivity.GetSmSourcePathForRestoreActivity, mock.Anything, mock.Anything, mock.Anything).Return("test-source-path", nil)
	s.env.OnActivity(activities.GetObjStoreNameFromBackup, mock.Anything, mock.Anything).Return("test-obj-store", nil)
	s.env.OnActivity(activities.GetBucketDetailsFromBackup, mock.Anything, mock.Anything).Return(&datamodel.BucketDetails{BucketName: "test-bucket"}, nil)
	s.env.OnActivity(backupActivity.GetOrCreateObjectStore, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.CloudTarget{}, nil)
	s.env.OnActivity(backupActivity.SnapmirrorGetOrCreate, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.SnapmirrorRelationship{UUID: "uuid", DestinationUUID: nillable.ToPointer("snapmirror-uuid")}, nil)
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

	// Assert workflow succeeded
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
}

func (s *UnitTestSuite) Test_RestoreVolumeWorkflow_WaitError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	backupActivity := activities.BackupActivity{SE: mockStorage}
	volume := &datamodel.Volume{
		Account: &datamodel.Account{Name: "account-1"},
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
		Attributes:    &datamodel.BackupAttributes{BucketName: "test-bucket", EndpointUUID: "test-endpoint-uuid"},
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)
	s.env.RegisterActivity(volumeCreateActivity.InitiateSplitForVolume)
	s.env.RegisterActivity(backupActivity.GetSmSourcePathActivity)
	s.env.RegisterActivity(backupActivity.GetSmSourcePathForRestoreActivity)
	s.env.RegisterActivity(activities.GetObjStoreNameFromBackup)
	s.env.RegisterActivity(activities.GetBucketDetailsFromBackup)

	// Mock activities
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{}}, nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupActivity.GetSmSourcePathActivity, mock.Anything, mock.Anything).Return("test-destination-path", nil)
	s.env.OnActivity(backupActivity.GetSmSourcePathForRestoreActivity, mock.Anything, mock.Anything, mock.Anything).Return("test-source-path", nil)
	s.env.OnActivity(activities.GetObjStoreNameFromBackup, mock.Anything, mock.Anything).Return("test-obj-store", nil)
	s.env.OnActivity(activities.GetBucketDetailsFromBackup, mock.Anything, mock.Anything).Return(&datamodel.BucketDetails{BucketName: "test-bucket"}, nil)
	s.env.OnActivity(backupActivity.GetOrCreateObjectStore, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.CloudTarget{}, nil)
	s.env.OnActivity(backupActivity.SnapmirrorGetOrCreate, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.SnapmirrorRelationship{UUID: "uuid", DestinationUUID: nillable.ToPointer("snapmirror-uuid")}, nil)
	s.env.OnActivity(backupActivity.SnapmirrorTransfer, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Mock workflow.Sleep to return error
	orgWorkflowSleep := workflowSleep
	defer func() { workflowSleep = orgWorkflowSleep }()
	workflowSleep = func(ctx workflow.Context, duration time.Duration) error {
		return errors.New("workflow sleep error")
	}

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

	// Assert workflow succeeded
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "failed to sleep during snapmirror transfer polling")
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

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)
	s.env.RegisterActivity(volumeCreateActivity.InitiateSplitForVolume)

	// Mock activities
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{}}, nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupActivity.GetOrCreateObjectStore, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.CloudTarget{}, errors.New("failed to get or create object store"))
	s.env.OnActivity(backupActivity.SnapmirrorGetOrCreate, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.SnapmirrorRelationship{UUID: "uuid", DestinationUUID: nillable.ToPointer("snapmirror-uuid")}, nil)
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
	// UpdateJob is called through UpdateJobStatus activity, not directly on mock
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

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)
	s.env.RegisterActivity(volumeCreateActivity.InitiateSplitForVolume)

	// Mock activities
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{}}, nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupActivity.GetOrCreateObjectStore, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.CloudTarget{}, nil)
	s.env.OnActivity(backupActivity.SnapmirrorGetOrCreate, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.SnapmirrorRelationship{UUID: "uuid", DestinationUUID: nillable.ToPointer("snapmirror-uuid")}, errors.New("failed to get or snapmirror"))
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
	// UpdateJob is called through UpdateJobStatus activity, not directly on mock
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

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)
	s.env.RegisterActivity(volumeCreateActivity.InitiateSplitForVolume)

	// Mock activities
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{}}, nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupActivity.GetOrCreateObjectStore, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.CloudTarget{}, nil)
	s.env.OnActivity(backupActivity.SnapmirrorGetOrCreate, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.SnapmirrorRelationship{UUID: "uuid", DestinationUUID: nillable.ToPointer("snapmirror-uuid")}, nil)
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
	// UpdateJob is called through UpdateJobStatus activity, not directly on mock
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

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)
	s.env.RegisterActivity(volumeCreateActivity.InitiateSplitForVolume)

	// Mock activities
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{}}, nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupActivity.GetOrCreateObjectStore, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.CloudTarget{}, nil)
	s.env.OnActivity(backupActivity.SnapmirrorGetOrCreate, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.SnapmirrorRelationship{UUID: "uuid", DestinationUUID: nillable.ToPointer("snapmirror-uuid")}, nil)
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
	// UpdateJob is called through UpdateJobStatus activity, not directly on mock
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

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)
	s.env.RegisterActivity(volumeCreateActivity.InitiateSplitForVolume)

	// Mock activities
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{}}, nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupActivity.GetOrCreateObjectStore, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.CloudTarget{}, nil)
	s.env.OnActivity(backupActivity.SnapmirrorGetOrCreate, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.SnapmirrorRelationship{UUID: "uuid", DestinationUUID: nillable.ToPointer("snapmirror-uuid")}, nil)
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
	// UpdateJob is called through UpdateJobStatus activity, not directly on mock
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

	// Enable file protocols for testing with allowlisted accounts
	utils.SetFileProtocolSupportedForTesting(true)
	utils.SetFileProtocolAllowlistedAccountsForTesting("test_account")
	defer func() {
		utils.SetFileProtocolSupportedForTesting(false)
		utils.SetFileProtocolAllowlistedAccountsForTesting("")
	}()

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
	utils.SetFileProtocolSupportedForTesting(false)
	utils.SetFileProtocolAllowlistedAccountsForTesting("")
	defer func() {
		utils.SetFileProtocolSupportedForTesting(false)
		utils.SetFileProtocolAllowlistedAccountsForTesting("")
	}()

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

	// Enable file protocols for testing with allowlisted accounts
	utils.SetFileProtocolSupportedForTesting(true)
	utils.SetFileProtocolAllowlistedAccountsForTesting("test_account")
	defer func() {
		utils.SetFileProtocolSupportedForTesting(false)
		utils.SetFileProtocolAllowlistedAccountsForTesting("")
	}()

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
	utils.SetFileProtocolSupportedForTesting(false)
	utils.SetFileProtocolAllowlistedAccountsForTesting("")
	defer func() {
		utils.SetFileProtocolSupportedForTesting(false)
		utils.SetFileProtocolAllowlistedAccountsForTesting("")
	}()

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
	preWorkflow, err := selectVolumeChildWorkflow(protocols, PhasePre, "test_account")
	assert.Nil(s.T(), err)
	assert.NotNil(s.T(), preWorkflow)
	// Verify it returns a function that can be called
	assert.IsType(s.T(), PreBlockVolumeWorkflow, preWorkflow)

	// Test post phase
	postWorkflow, err := selectVolumeChildWorkflow(protocols, PhasePost, "test_account")
	assert.Nil(s.T(), err)
	assert.NotNil(s.T(), postWorkflow)
	// Verify it returns a function that can be called
	assert.IsType(s.T(), PostBlockVolumeWorkflow, postWorkflow)

	// Test invalid phase
	invalidWorkflow, err := selectVolumeChildWorkflow(protocols, "invalid", "test_account")
	assert.NotNil(s.T(), err)
	assert.Nil(s.T(), invalidWorkflow)
	assert.Contains(s.T(), err.Error(), "An internal error occurred.")
	assert.Contains(s.T(), err.(*vsaerrors.CustomError).OriginalErr.Error(), "invalid phase")
}

func (s *UnitTestSuite) Test_SelectVolumeChildWorkflow_NFSv3() {
	// Test selectVolumeChildWorkflow with NFSv3 protocol
	protocols := []string{utils.ProtocolNFSv3}

	// Enable file protocols for testing with allowlisted accounts
	utils.SetFileProtocolSupportedForTesting(true)
	utils.SetFileProtocolAllowlistedAccountsForTesting("test_account")
	defer func() {
		utils.SetFileProtocolSupportedForTesting(false)
		utils.SetFileProtocolAllowlistedAccountsForTesting("")
	}()

	// Test pre phase
	preWorkflow, err := selectVolumeChildWorkflow(protocols, PhasePre, "test_account")
	assert.Nil(s.T(), err)
	assert.NotNil(s.T(), preWorkflow)
	// Verify it returns a function that can be called
	assert.IsType(s.T(), PreFileVolumeWorkflow, preWorkflow)

	// Test post phase
	postWorkflow, err := selectVolumeChildWorkflow(protocols, PhasePost, "test_account")
	assert.Nil(s.T(), err)
	assert.NotNil(s.T(), postWorkflow)
	// Verify it returns a function that can be called
	assert.IsType(s.T(), PostFileVolumeWorkflow, postWorkflow)
}

func (s *UnitTestSuite) Test_SelectVolumeChildWorkflow_NFSv4() {
	// Test selectVolumeChildWorkflow with NFSv4 protocol
	protocols := []string{utils.ProtocolNFSv4}

	// Enable file protocols for testing with allowlisted accounts
	utils.SetFileProtocolSupportedForTesting(true)
	utils.SetFileProtocolAllowlistedAccountsForTesting("test_account")
	defer func() {
		utils.SetFileProtocolSupportedForTesting(false)
		utils.SetFileProtocolAllowlistedAccountsForTesting("")
	}()

	// Test pre phase
	preWorkflow, err := selectVolumeChildWorkflow(protocols, PhasePre, "test_account")
	assert.Nil(s.T(), err)
	assert.NotNil(s.T(), preWorkflow)
	// Verify it returns a function that can be called
	assert.IsType(s.T(), PreFileVolumeWorkflow, preWorkflow)

	// Test post phase
	postWorkflow, err := selectVolumeChildWorkflow(protocols, PhasePost, "test_account")
	assert.Nil(s.T(), err)
	assert.NotNil(s.T(), postWorkflow)
	// Verify it returns a function that can be called
	assert.IsType(s.T(), PostFileVolumeWorkflow, postWorkflow)
}

func (s *UnitTestSuite) Test_SelectVolumeChildWorkflow_SMB() {
	// Test selectVolumeChildWorkflow with SMB protocol
	protocols := []string{utils.ProtocolSMB}

	// Enable file protocols for testing with allowlisted accounts
	utils.SetFileProtocolSupportedForTesting(true)
	utils.SetFileProtocolAllowlistedAccountsForTesting("test_account")
	defer func() {
		utils.SetFileProtocolSupportedForTesting(false)
		utils.SetFileProtocolAllowlistedAccountsForTesting("")
	}()

	// Test pre phase
	preWorkflow, err := selectVolumeChildWorkflow(protocols, PhasePre, "test_account")
	assert.Nil(s.T(), err)
	assert.NotNil(s.T(), preWorkflow)
	// Verify it returns a function that can be called
	assert.IsType(s.T(), PreFileVolumeWorkflow, preWorkflow)
	// Test post phase
	postWorkflow, err := selectVolumeChildWorkflow(protocols, PhasePost, "test_account")
	assert.Nil(s.T(), err)
	assert.NotNil(s.T(), postWorkflow)
	// Verify it returns a function that can be called
	assert.IsType(s.T(), PostFileVolumeWorkflow, postWorkflow)
}

func (s *UnitTestSuite) Test_SelectVolumeChildWorkflow_FileProtocolsDisabled() {
	// Test selectVolumeChildWorkflow when file protocols are disabled
	protocols := []string{utils.ProtocolNFSv3}

	// Disable file protocols for testing
	utils.SetFileProtocolSupportedForTesting(false)
	utils.SetFileProtocolAllowlistedAccountsForTesting("")
	defer func() {
		utils.SetFileProtocolSupportedForTesting(false)
		utils.SetFileProtocolAllowlistedAccountsForTesting("")
	}()

	// Test pre phase
	preWorkflow, err := selectVolumeChildWorkflow(protocols, PhasePre, "test_account")
	assert.NotNil(s.T(), err)
	assert.Nil(s.T(), preWorkflow)
	assert.Contains(s.T(), err.(*vsaerrors.CustomError).OriginalErr.Error(), "file protocols are not enabled")

	// Test post phase
	postWorkflow, err := selectVolumeChildWorkflow(protocols, PhasePost, "test_account")
	assert.NotNil(s.T(), err)
	assert.Nil(s.T(), postWorkflow)
	assert.Contains(s.T(), err.(*vsaerrors.CustomError).OriginalErr.Error(), "file protocols are not enabled")
}

func (s *UnitTestSuite) Test_SelectVolumeChildWorkflow_UnsupportedProtocol() {
	// Test selectVolumeChildWorkflow with unsupported protocol
	protocols := []string{"UNSUPPORTED_PROTOCOL"}

	// Test pre phase
	preWorkflow, err := selectVolumeChildWorkflow(protocols, PhasePre, "test_account")
	assert.NotNil(s.T(), err)
	assert.Nil(s.T(), preWorkflow)
	assert.Contains(s.T(), err.(*vsaerrors.CustomError).OriginalErr.Error(), "unsupported or unspecified protocol")

	// Test post phase
	postWorkflow, err := selectVolumeChildWorkflow(protocols, PhasePost, "test_account")
	assert.NotNil(s.T(), err)
	assert.Nil(s.T(), postWorkflow)
	assert.Contains(s.T(), err.(*vsaerrors.CustomError).OriginalErr.Error(), "unsupported or unspecified protocol")
}

func (s *UnitTestSuite) Test_SelectVolumeChildWorkflow_EmptyProtocols() {
	// Test selectVolumeChildWorkflow with empty protocols
	protocols := []string{}

	// Test pre phase
	preWorkflow, err := selectVolumeChildWorkflow(protocols, PhasePre, "test_account")
	assert.NotNil(s.T(), err)
	assert.Nil(s.T(), preWorkflow)
	assert.Contains(s.T(), err.(*vsaerrors.CustomError).OriginalErr.Error(), "unsupported or unspecified protocol")

	// Test post phase
	postWorkflow, err := selectVolumeChildWorkflow(protocols, PhasePost, "test_account")
	assert.NotNil(s.T(), err)
	assert.Nil(s.T(), postWorkflow)
	assert.Contains(s.T(), err.(*vsaerrors.CustomError).OriginalErr.Error(), "unsupported or unspecified protocol")
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

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_NFS_FileVolume_Success() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	utils.SetFileProtocolSupportedForTesting(true)
	utils.SetFileProtocolAllowlistedAccountsForTesting("account-1")
	defer func() {
		utils.SetFileProtocolSupportedForTesting(false)
		utils.SetFileProtocolAllowlistedAccountsForTesting("")
	}()
	// NFS file volume with file properties and export policy
	volume := &datamodel.Volume{
		Account: &datamodel.Account{Name: "account-1"},
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
		},
		Svm: &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			Protocols: []string{"NFSV3"}, // NFS volume
			FileProperties: &datamodel.FileProperties{
				ExportPolicy: &datamodel.ExportPolicy{
					ExportPolicyName: "test-export-policy",
					ExportRules: []*datamodel.ExportRule{
						{
							AllowedClients: "10.0.0.0/8",
							AccessType:     "ReadWrite",
							NFSv3:          true,
							NFSv4:          false,
						},
					},
				},
			},
		},
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)

	// Mock activities for NFS flow
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateExportPolicyInOntap, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name:         "nfs_volume_test",
			ExternalUUID: "volume-uuid",
		},
		AvailableSpace: 1000000,
	}, nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume, nil, nil)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	// UpdateJob is called through UpdateJobStatus activity, not directly on mock
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_NFS_FileVolume_CreateExportPolicyError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}

	utils.SetFileProtocolSupportedForTesting(true)
	utils.SetFileProtocolAllowlistedAccountsForTesting("test-account")
	defer func() {
		utils.SetFileProtocolSupportedForTesting(false)
		utils.SetFileProtocolAllowlistedAccountsForTesting("")
	}()
	// NFS file volume with file properties
	volume := &datamodel.Volume{
		Account: &datamodel.Account{Name: "test-account"},
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
		},
		Svm: &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			Protocols: []string{"NFSV3"}, // NFS volume
			FileProperties: &datamodel.FileProperties{
				ExportPolicy: &datamodel.ExportPolicy{
					ExportPolicyName: "test-export-policy",
					ExportRules: []*datamodel.ExportRule{
						{
							AllowedClients: "10.0.0.0/8",
							AccessType:     "ReadWrite",
							NFSv3:          true,
						},
					},
				},
			},
		},
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)

	// Mock activities - export policy creation fails
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateExportPolicyInOntap, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to create export policy"))

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume, nil, nil)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "failed to create export policy")
	// UpdateJob is called through UpdateJobStatus activity, not directly on mock
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_NFS_FileVolume_WithBackupVault_Success() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	utils.SetFileProtocolSupportedForTesting(true)
	utils.SetFileProtocolAllowlistedAccountsForTesting("account-1")
	defer func() {
		utils.SetFileProtocolSupportedForTesting(false)
		utils.SetFileProtocolAllowlistedAccountsForTesting("")
	}()
	// NFS file volume with backup vault configuration
	volume := &datamodel.Volume{
		Account: &datamodel.Account{Name: "account-1"},
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
		},
		Svm: &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			Protocols:      []string{"NFSV3"}, // NFS volume
			VendorSubnetID: "subnet-12345",
			FileProperties: &datamodel.FileProperties{
				ExportPolicy: &datamodel.ExportPolicy{
					ExportPolicyName: "backup-export-policy",
					ExportRules: []*datamodel.ExportRule{
						{
							AllowedClients: "192.168.1.0/24",
							AccessType:     "ReadOnly",
							NFSv3:          true,
							NFSv4:          true,
						},
					},
				},
			},
		},
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "backup-vault-id",
		},
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)
	s.env.RegisterActivity(commonActivity.GetAuthJWTToken)

	// Mock activities for NFS flow with backup vault
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("dummy-token", nil)

	// Mock activities for NFS flow with backup vault
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateExportPolicyInOntap, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name:         "nfs_backup_volume_test",
			ExternalUUID: "volume-uuid",
		},
		AvailableSpace: 2000000,
	}, nil)

	// Backup vault related activities
	s.env.OnActivity(volumeCreateActivity.FindTenancy, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.TenancyInfo{RegionalTenantProject: "tenant-project"}, nil)
	s.env.OnActivity(volumeCreateActivity.CheckBackupVaultExistsInVCP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CheckForBucketResourceName, mock.Anything, mock.Anything).Return(&common.BucketDetails{
		BucketName:          "existing-bucket",
		ServiceAccountName:  "existing-sa",
		TenantProjectNumber: "existing-project",
		VendorSubnetID:      "subnet-12345",
	}, nil)
	s.env.OnActivity(volumeCreateActivity.UpdateBackupVaultWithBucketDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume, nil, nil)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	// UpdateJob is called through UpdateJobStatus activity, not directly on mock
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_NFS_FileVolume_MultipleExportRules_Success() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	utils.SetFileProtocolSupportedForTesting(true)
	utils.SetFileProtocolAllowlistedAccountsForTesting("account-1")
	defer func() {
		utils.SetFileProtocolSupportedForTesting(false)
		utils.SetFileProtocolAllowlistedAccountsForTesting("")
	}()
	// NFS file volume with multiple export rules
	volume := &datamodel.Volume{
		Account: &datamodel.Account{Name: "account-1"},
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
		},
		Svm: &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			Protocols: []string{"NFSV3"}, // NFS volume
			FileProperties: &datamodel.FileProperties{
				ExportPolicy: &datamodel.ExportPolicy{
					ExportPolicyName: "multi-rule-export-policy",
					ExportRules: []*datamodel.ExportRule{
						{
							AllowedClients: "10.0.0.0/8",
							AccessType:     "ReadWrite",
							NFSv3:          true,
							NFSv4:          false,
							Index:          1,
						},
						{
							AllowedClients: "192.168.1.0/24",
							AccessType:     "ReadOnly",
							NFSv3:          false,
							NFSv4:          true,
							Index:          2,
						},
						{
							AllowedClients: "172.16.0.0/12",
							AccessType:     "ReadWrite",
							NFSv3:          true,
							NFSv4:          true,
							Index:          3,
						},
					},
				},
			},
		},
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)

	// Mock activities for NFS flow with multiple export rules
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateExportPolicyInOntap, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name:         "nfs_multi_rule_volume_test",
			ExternalUUID: "volume-uuid",
		},
		AvailableSpace: 3000000,
	}, nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume, nil, nil)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	// UpdateJob is called through UpdateJobStatus activity, not directly on mock
}
func (s *UnitTestSuite) Test_CreateVolumeWorkflow_NFS_FileVolume_CreateSnapshotPolicyError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	utils.SetFileProtocolSupportedForTesting(true)
	utils.SetFileProtocolAllowlistedAccountsForTesting("account-1")
	defer func() {
		utils.SetFileProtocolSupportedForTesting(false)
		utils.SetFileProtocolAllowlistedAccountsForTesting("")
	}()
	// NFS file volume
	volume := &datamodel.Volume{
		Account: &datamodel.Account{Name: "account-1"},
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
		},
		Svm: &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			Protocols: []string{"NFSV3"}, // NFS volume
			FileProperties: &datamodel.FileProperties{
				ExportPolicy: &datamodel.ExportPolicy{
					ExportPolicyName: "test-export-policy",
					ExportRules: []*datamodel.ExportRule{
						{
							AllowedClients: "10.0.0.0/8",
							AccessType:     "ReadWrite",
							NFSv3:          true,
						},
					},
				},
			},
		},
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)

	// Mock activities - snapshot policy creation fails after export policy success
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateExportPolicyInOntap, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to create snapshot policy"))

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume, nil, nil)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "failed to create snapshot policy")
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_NFS_FileVolume_CreateVolumeInONTAPError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	utils.SetFileProtocolSupportedForTesting(true)
	utils.SetFileProtocolAllowlistedAccountsForTesting("account-1")
	defer func() {
		utils.SetFileProtocolSupportedForTesting(false)
		utils.SetFileProtocolAllowlistedAccountsForTesting("")
	}()
	// NFS file volume
	volume := &datamodel.Volume{
		Account: &datamodel.Account{Name: "account-1"},
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
		},
		Svm: &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			Protocols: []string{"NFSV3"}, // NFS volume
			FileProperties: &datamodel.FileProperties{
				ExportPolicy: &datamodel.ExportPolicy{
					ExportPolicyName: "test-export-policy",
					ExportRules: []*datamodel.ExportRule{
						{
							AllowedClients: "10.0.0.0/8",
							AccessType:     "ReadWrite",
							NFSv3:          true,
						},
					},
				},
			},
		},
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)

	// Mock activities - volume creation fails after export policy and snapshot policy success
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateExportPolicyInOntap, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to create volume in ONTAP"))

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume, nil, nil)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "failed to create volume in ONTAP")
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_NFS_FileVolume_WithBucketCreation_Success() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	utils.SetFileProtocolSupportedForTesting(true)
	utils.SetFileProtocolAllowlistedAccountsForTesting("account-1")
	defer func() {
		utils.SetFileProtocolSupportedForTesting(false)
		utils.SetFileProtocolAllowlistedAccountsForTesting("")
	}()
	// NFS file volume with backup vault that requires new bucket creation
	volume := &datamodel.Volume{
		Account: &datamodel.Account{Name: "account-1"},
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
		},
		Svm: &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			Protocols:      []string{"NFSV3"}, // NFS volume
			VendorSubnetID: "subnet-67890",
			FileProperties: &datamodel.FileProperties{
				ExportPolicy: &datamodel.ExportPolicy{
					ExportPolicyName: "new-bucket-export-policy",
					ExportRules: []*datamodel.ExportRule{
						{
							AllowedClients: "172.16.0.0/12",
							AccessType:     "ReadWrite",
							NFSv3:          true,
							NFSv4:          true,
						},
					},
				},
			},
		},
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "new-backup-vault-id",
		},
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)
	s.env.RegisterActivity(commonActivity.GetAuthJWTToken)

	// Mock activities for NFS flow with new bucket creation
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("dummy-token", nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateExportPolicyInOntap, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name:         "nfs_new_bucket_volume_test",
			ExternalUUID: "volume-uuid",
		},
		AvailableSpace: 4000000,
	}, nil)

	// Backup vault related activities - no existing bucket
	s.env.OnActivity(volumeCreateActivity.FindTenancy, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.TenancyInfo{RegionalTenantProject: "new-tenant-project"}, nil)
	s.env.OnActivity(volumeCreateActivity.CheckBackupVaultExistsInVCP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CheckForBucketResourceName, mock.Anything, mock.Anything).Return(&common.BucketDetails{}, nil) // Empty bucket details

	// New bucket creation flow
	s.env.OnActivity(volumeCreateActivity.GenerateResourceNames, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.ResourceNames{
		BucketName:       "new-bucket-name",
		ServiceAccountId: "new-sa-id",
		Email:            "new-sa@project.iam.gserviceaccount.com",
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateBucket, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.BucketDetails{
		BucketName:          "new-bucket-name",
		ServiceAccountName:  "new-sa-name",
		TenantProjectNumber: "new-project-123",
		VendorSubnetID:      "subnet-67890",
	}, nil)
	s.env.OnActivity(volumeCreateActivity.UpdateBackupVaultWithBucketDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume, nil, nil)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	// UpdateJob is called through UpdateJobStatus activity, not directly on mock
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_Setup_QueryHandlerError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	volume := &datamodel.Volume{
		Account: &datamodel.Account{Name: "account-1"},
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, PoolCredentials: &datamodel.PoolCredentials{
			Password:      "password",
			SecretID:      "",
			CertificateID: "",
		}},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}, Protocols: []string{"ISCSI"}},
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(commonActivity.GetNode)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)
	s.env.RegisterActivity(volumeCreateActivity.InitiateSplitForVolume)
	s.env.RegisterActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP)
	s.env.RegisterActivity(volumeCreateActivity.CreateVolumeInONTAP)
	s.env.RegisterActivity(volumeCreateActivity.GetHosts)
	s.env.RegisterActivity(volumeCreateActivity.CreateIgroup)
	s.env.RegisterActivity(volumeCreateActivity.CreateLun)
	s.env.RegisterActivity(volumeCreateActivity.CreateLunMap)

	// Mock child workflows
	s.env.OnWorkflow("PreBlockVolumeWorkflow", mock.Anything, mock.Anything, mock.Anything).Return(volume, nil)
	s.env.OnWorkflow("PostBlockVolumeWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(volume, nil)

	// Mock activities for minimal workflow
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.InitiateSplitForVolume, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{Name: "host_group_test", Hosts: datamodel.Hosts{Hosts: []string{"iqn.1993-08.org.debian:01:f2c983feb27"}}}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{ProviderResponse: vsa.ProviderResponse{Name: "lun_test", ExternalUUID: "lun-uuid"}, SerialNumber: "6c5738423724595454686164"}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateLunMap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume, nil, nil)

	// Query workflow status to test query handler
	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow completed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	// UpdateJob is called through UpdateJobStatus activity, not directly on mock
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_SetupWorkflowSuccess() {
	// Test to cover Setup function more thoroughly
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	volume := &datamodel.Volume{
		Account: &datamodel.Account{Name: "account-1"},
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
		},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}, Protocols: []string{"ISCSI"}},
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(commonActivity.GetNode)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)
	s.env.RegisterActivity(volumeCreateActivity.InitiateSplitForVolume)
	s.env.RegisterActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP)
	s.env.RegisterActivity(volumeCreateActivity.CreateVolumeInONTAP)
	s.env.RegisterActivity(volumeCreateActivity.GetHosts)
	s.env.RegisterActivity(volumeCreateActivity.CreateIgroup)
	s.env.RegisterActivity(volumeCreateActivity.CreateLun)
	s.env.RegisterActivity(volumeCreateActivity.CreateLunMap)

	// Mock child workflows
	s.env.OnWorkflow("PreBlockVolumeWorkflow", mock.Anything, mock.Anything, mock.Anything).Return(volume, nil)
	s.env.OnWorkflow("PostBlockVolumeWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(volume, nil)

	// Mock activities for minimal workflow
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.InitiateSplitForVolume, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{Name: "host_group_test", Hosts: datamodel.Hosts{Hosts: []string{"iqn.1993-08.org.debian:01:f2c983feb27"}}}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{ProviderResponse: vsa.ProviderResponse{Name: "lun_test", ExternalUUID: "lun-uuid"}, SerialNumber: "6c5738423724595454686164"}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateLunMap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume, nil, nil)

	// Test query handler
	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_NoDataProtectionSuccess() {
	// Test volume creation without data protection to cover that path
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}

	volume := &datamodel.Volume{
		Account: &datamodel.Account{Name: "account-1"},
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
		},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{Protocols: []string{"ISCSI"}, BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}},
		DataProtection:   nil, // No data protection
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)

	// Mock activities
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{
		Name: "host_group_test", Hosts: datamodel.Hosts{Hosts: []string{"iqn.1993-08.org.debian:01:f2c983feb27"}},
	}}, nil)
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
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume, nil, nil)

	// Assert workflow completed successfully (backup vault flow should be skipped)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	// UpdateJob is called through UpdateJobStatus activity, not directly on mock
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_ComplexErrorScenarios() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}

	// Test with complex volume configuration
	volume := &datamodel.Volume{
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
		},
		Svm: &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			Protocols:       []string{"iscsi", "NFSV3"}, // Mixed protocols
			BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"},
			FileProperties: &datamodel.FileProperties{
				ExportPolicy: &datamodel.ExportPolicy{
					ExportPolicyName: "test-policy",
					ExportRules: []*datamodel.ExportRule{
						{AllowedClients: "10.0.0.0/8", AccessType: "ReadWrite", NFSv3: true},
					},
				},
			},
		},
		DataProtection: &datamodel.DataProtection{BackupVaultID: "vault-123"},
		Account:        &datamodel.Account{Name: "test-account"},
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)

	// Mock GetNode to fail after multiple retries
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return(nil, errors.New("node failure after retries"))

	// Execute workflow
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume, nil, nil)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	// UpdateJob is called through UpdateJobStatus activity, not directly on mock // Once for processing, once for error
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_ComplexErrorScenarios_Full() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}

	// Test with complex volume configuration
	volume := &datamodel.Volume{
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
		},
		Svm: &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			Protocols:       []string{"iscsi", "NFSV3"}, // Mixed protocols
			BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"},
			FileProperties: &datamodel.FileProperties{
				ExportPolicy: &datamodel.ExportPolicy{
					ExportPolicyName: "test-policy",
					ExportRules: []*datamodel.ExportRule{
						{AllowedClients: "10.0.0.0/8", AccessType: "ReadWrite", NFSv3: true},
					},
				},
			},
		},
		DataProtection: &datamodel.DataProtection{BackupVaultID: "vault-123"},
		Account:        &datamodel.Account{Name: "test-account"},
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)

	// Mock GetNode to fail after multiple retries
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return(nil, errors.New("node failure after retries"))

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume, nil, nil)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	// UpdateJob is called through UpdateJobStatus activity, not directly on mock // Once for processing, once for error
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_MinimalCoverage() {
	// Minimal test to improve coverage by testing some edge conditions
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(commonActivity.GetNode)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)

	// Mock activities
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)

	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, &datamodel.Volume{
		Name: "test-volume",
		Pool: &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			Protocols: []string{"other"}, // A protocol that's neither iscsi nor NFSV3
			BlockProperties: &datamodel.BlockProperties{
				OSType: "LINUX",
			},
		},
	}, nil, nil)

	// This should cover the path where isBlock=false and isFiles=false
	s.True(s.env.IsWorkflowCompleted())
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_UpdateJobStatusProcessingError() {
	// Test error during job status update to PROCESSING
	mockStorage := database.NewMockStorage(s.T())

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "test-volume-uuid",
		},
		Name: "test-volume",
		Account: &datamodel.Account{
			Name: "test-account",
		},
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
		},
		Svm: &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "external-uuid",
			Protocols:    []string{"ISCSI"},
		},
	}

	// Mock activities
	commonActivity := activities.CommonActivities{SE: mockStorage}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)

	// Mock UpdateJobStatus to return error for PROCESSING state
	expectedError := errors.New("failed to update job status to PROCESSING")
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.MatchedBy(func(job *datamodel.Job) bool {
		return job.State == string(models.JobsStatePROCESSING)
	})).Return(expectedError)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{AccountName: "test-account"}, volume, nil, nil)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), expectedError.Error())
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_UpdateJobStatusDoneError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	volumeDeleteActivity := activities.VolumeDeleteActivity{SE: mockStorage}
	volume := &datamodel.Volume{
		Account: &datamodel.Account{Name: "account-1"},
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
		},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}, Protocols: []string{utils.ProtocolISCSI}},
	}

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

	// Mock UpdateJobStatus to succeed for PROCESSING state
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.MatchedBy(func(job *datamodel.Job) bool {
		return job.State == string(models.JobsStatePROCESSING)
	})).Return(nil)

	// Mock UpdateJobStatus to fail for DONE state (successful completion)
	expectedError := errors.New("failed to update job status to DONE")
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.MatchedBy(func(job *datamodel.Job) bool {
		return job.State == string(models.JobsStateDONE) && job.ErrorDetails == ""
	})).Return(expectedError)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume, nil, nil)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), expectedError.Error())
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_UpdateJobStatusErrorDetailsError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	volume := &datamodel.Volume{
		Account: &datamodel.Account{Name: "account-1"},
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
		},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}, Protocols: []string{utils.ProtocolISCSI}},
	}

	// Mock UpdateJobStatus to succeed for PROCESSING state
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.MatchedBy(func(job *datamodel.Job) bool {
		return job.State == string(models.JobsStatePROCESSING)
	})).Return(nil)

	// Mock UpdateJobStatus to fail for DONE state with error details
	errorDetailsUpdateError := errors.New("failed to update job status with error details")
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.MatchedBy(func(job *datamodel.Job) bool {
		return job.State == string(models.JobsStateERROR) && job.ErrorDetails != ""
	})).Return(errorDetailsUpdateError)

	// Mock GetVolumeFromONTAPAgain to return error
	nodeErr := errors.New("failed to get hosts from ONTAP")
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{}}, nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return(nil, nodeErr)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume, nil, nil)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed with the error details update error
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), errorDetailsUpdateError.Error())
}

func (s *UnitTestSuite) Test_SelectVolumeChildWorkflow_InvalidPhase() {
	// Test selectVolumeChildWorkflow with invalid phase for block protocols
	workflow, err := selectVolumeChildWorkflow([]string{utils.ProtocolISCSI}, "invalid", "test-account")
	assert.Nil(s.T(), workflow)
	assert.Error(s.T(), err)
	assert.Contains(s.T(), err.(*vsaerrors.CustomError).OriginalErr.Error(), "invalid phase: invalid")
}

func (s *UnitTestSuite) Test_SelectVolumeChildWorkflow_InvalidPhaseFile() {
	// Test selectVolumeChildWorkflow with invalid phase for file protocols
	utils.SetFileProtocolSupportedForTesting(true)
	defer utils.SetFileProtocolSupportedForTesting(false)

	workflow, err := selectVolumeChildWorkflow([]string{utils.ProtocolNFSv3}, "invalid", "test-account")
	assert.Nil(s.T(), workflow)
	assert.Error(s.T(), err)
	// Check if the error is about file protocols not being enabled first, then invalid phase
	if !assert.Contains(s.T(), err.(*vsaerrors.CustomError).OriginalErr.Error(), "file protocols are not enabled") {
		assert.Contains(s.T(), err.(*vsaerrors.CustomError).OriginalErr.Error(), "invalid phase: invalid")
	}
}

func (s *UnitTestSuite) Test_PostBlockVolumeWorkflow_WithBlockDevices_Success() {
	mockStorage := database.NewMockStorage(s.T())
	volumeActivity := activities.VolumeCreateActivity{SE: mockStorage}

	// Register activities
	s.env.RegisterActivity(volumeActivity.GetHosts)
	s.env.RegisterActivity(volumeActivity.CreateIgroup)
	s.env.RegisterActivity(volumeActivity.UpdateLunName)
	s.env.RegisterActivity(volumeActivity.CreateLun)
	s.env.RegisterActivity(volumeActivity.CreateLunMap)

	// Mock activities
	s.env.OnActivity(volumeActivity.GetHosts, mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{
		{
			Name: "host-group-1",
			Hosts: datamodel.Hosts{
				Hosts: []string{"iqn.1998-01.com.vmware:host1"},
			},
		},
	}, nil)
	s.env.OnActivity(volumeActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeActivity.UpdateLunName, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeActivity.CreateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name:         "/vol/test_volume/lun_test-lun",
			ExternalUUID: "uuid-123",
		},
		SerialNumber: "serial-123",
		Size:         1000,
	}, nil)
	s.env.OnActivity(volumeActivity.CreateLunMap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	dbVolume := &datamodel.Volume{
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
		},
		Svm: &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockDevices: &[]datamodel.BlockDevice{
				{
					Name: "test-lun",
					HostGroupDetails: []datamodel.HostGroupDetail{
						{
							HostGroupUUID: "hg-uuid-1",
							HostQNs:       []string{"iqn.1998-01.com.vmware:host1"},
						},
					},
					OSType: "Linux",
				},
			},
			Protocols: []string{utils.ProtocolISCSI},
		},
	}
	node := &models.Node{
		EndpointAddress: "127.0.0.1",
	}
	volCreateResponse := &vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name: "test_volume",
		},
	}

	s.env.ExecuteWorkflow(PostBlockVolumeWorkflow, dbVolume, node, volCreateResponse, false)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())

	// Verify that BlockDevices were updated with LUN information
	var result datamodel.Volume
	err := s.env.GetWorkflowResult(&result)
	assert.NoError(s.T(), err)

	volume := &result
	assert.NotNil(s.T(), volume.VolumeAttributes.BlockDevices)
	assert.Len(s.T(), *volume.VolumeAttributes.BlockDevices, 1)

	blockDevice := (*volume.VolumeAttributes.BlockDevices)[0]
	assert.Equal(s.T(), "lun_test-lun", blockDevice.Name)
	assert.Equal(s.T(), "serial-123", blockDevice.Identifier)
	assert.Equal(s.T(), int64(1000), blockDevice.Size)
}

func (s *UnitTestSuite) Test_PostBlockVolumeWorkflow_WithBlockProperties_Success() {
	mockStorage := database.NewMockStorage(s.T())
	volumeActivity := activities.VolumeCreateActivity{SE: mockStorage}

	// Register activities
	s.env.RegisterActivity(volumeActivity.GetHosts)
	s.env.RegisterActivity(volumeActivity.CreateIgroup)
	s.env.RegisterActivity(volumeActivity.UpdateLunName)
	s.env.RegisterActivity(volumeActivity.CreateLun)
	s.env.RegisterActivity(volumeActivity.CreateLunMap)

	// Mock activities
	s.env.OnActivity(volumeActivity.GetHosts, mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{
		{
			Name: "host-group-1",
			Hosts: datamodel.Hosts{
				Hosts: []string{"iqn.1998-01.com.vmware:host1"},
			},
		},
	}, nil)
	s.env.OnActivity(volumeActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeActivity.UpdateLunName, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeActivity.CreateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name:         "/vol/test_volume/lun_test-lun",
			ExternalUUID: "uuid-123",
		},
		SerialNumber: "serial-123",
		Size:         1000,
	}, nil)
	s.env.OnActivity(volumeActivity.CreateLunMap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	dbVolume := &datamodel.Volume{
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
		},
		Svm: &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockDevices: nil, // No BlockDevices
			BlockProperties: &datamodel.BlockProperties{
				HostGroupDetails: []datamodel.HostGroupDetail{
					{
						HostGroupUUID: "hg-uuid-1",
						HostQNs:       []string{"iqn.1998-01.com.vmware:host1"},
					},
				},
				OSType: "Linux",
			},
			Protocols: []string{utils.ProtocolISCSI},
		},
	}
	node := &models.Node{
		EndpointAddress: "127.0.0.1",
	}
	volCreateResponse := &vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name: "test_volume",
		},
	}

	s.env.ExecuteWorkflow(PostBlockVolumeWorkflow, dbVolume, node, volCreateResponse, false)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())

	// Verify that BlockProperties were updated with LUN information
	var result datamodel.Volume
	err := s.env.GetWorkflowResult(&result)
	assert.NoError(s.T(), err)

	volume := &result
	assert.NotNil(s.T(), volume.VolumeAttributes.BlockProperties)
	assert.Equal(s.T(), "lun_test-lun", volume.VolumeAttributes.BlockProperties.LunName)
	assert.Equal(s.T(), "serial-123", volume.VolumeAttributes.BlockProperties.LunSerialNumber)
	assert.Equal(s.T(), "uuid-123", volume.VolumeAttributes.BlockProperties.LunUUID)
}

func (s *UnitTestSuite) Test_CreateHostParamsFromHostGroups_WithBlockDevices() {
	// Test with BlockDevices present
	volume := &datamodel.Volume{
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockDevices: &[]datamodel.BlockDevice{
				{
					OSType: "Linux",
				},
			},
		},
	}

	hostGroups := []*datamodel.HostGroup{
		{
			Name: "host-group-1",
			Hosts: datamodel.Hosts{
				Hosts: []string{"iqn.1998-01.com.vmware:host1"},
			},
		},
		{
			Name: "host-group-2",
			Hosts: datamodel.Hosts{
				Hosts: []string{"iqn.1998-01.com.vmware:host2"},
			},
		},
	}

	result := createHostParamsFromHostGroups(hostGroups, volume)

	assert.Len(s.T(), result, 2)
	assert.Equal(s.T(), "host-group-1", result[0].HostName)
	assert.Equal(s.T(), []string{"iqn.1998-01.com.vmware:host1"}, result[0].HostIQNs)
	assert.Equal(s.T(), "Linux", result[0].OsType)
	assert.Equal(s.T(), "host-group-2", result[1].HostName)
	assert.Equal(s.T(), []string{"iqn.1998-01.com.vmware:host2"}, result[1].HostIQNs)
	assert.Equal(s.T(), "Linux", result[1].OsType)
}

func (s *UnitTestSuite) Test_CreateHostParamsFromHostGroups_WithBlockProperties() {
	// Test with BlockDevices not present, fallback to BlockProperties
	volume := &datamodel.Volume{
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockDevices: nil, // No BlockDevices
			BlockProperties: &datamodel.BlockProperties{
				OSType: "Windows",
			},
		},
	}

	hostGroups := []*datamodel.HostGroup{
		{
			Name: "host-group-1",
			Hosts: datamodel.Hosts{
				Hosts: []string{"iqn.1998-01.com.vmware:host1"},
			},
		},
	}

	result := createHostParamsFromHostGroups(hostGroups, volume)

	assert.Len(s.T(), result, 1)
	assert.Equal(s.T(), "host-group-1", result[0].HostName)
	assert.Equal(s.T(), []string{"iqn.1998-01.com.vmware:host1"}, result[0].HostIQNs)
	assert.Equal(s.T(), "Windows", result[0].OsType)
}

func (s *UnitTestSuite) Test_CreateHostParamsFromHostGroups() {
	// Test createHostParamsFromHostGroups function
	hostGroups := []*datamodel.HostGroup{
		{
			Name:  "host-group-1",
			Hosts: datamodel.Hosts{Hosts: []string{"iqn.host1", "iqn.host2"}},
		},
		{
			Name:  "host-group-2",
			Hosts: datamodel.Hosts{Hosts: []string{"iqn.host3"}},
		},
	}

	volume := &datamodel.Volume{
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"},
		},
	}

	result := createHostParamsFromHostGroups(hostGroups, volume)

	assert.Len(s.T(), result, 2)
	assert.Equal(s.T(), "host-group-1", result[0].HostName)
	assert.Equal(s.T(), []string{"iqn.host1", "iqn.host2"}, result[0].HostIQNs)
	assert.Equal(s.T(), "LINUX", result[0].OsType)
	assert.Equal(s.T(), "host-group-2", result[1].HostName)
	assert.Equal(s.T(), []string{"iqn.host3"}, result[1].HostIQNs)
}

func (s *UnitTestSuite) Test_CreateLunMapParams() {
	// Test createLunMapParams function
	hostParams := []*common.HostParams{
		{HostName: "host1"},
		{HostName: "host2"},
		{HostName: "host3"},
	}

	result := createLunMapParams("test-lun", "test-svm", hostParams)

	assert.Equal(s.T(), "test-lun", result.LunName)
	assert.Equal(s.T(), "test-svm", result.SvmName)
	assert.Equal(s.T(), []string{"host1", "host2", "host3"}, result.HostNames)
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_PreChildWorkflowError() {
	// Test error in pre-child workflow selection
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}

	volume := &datamodel.Volume{
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			}},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{Protocols: []string{"invalid"}}, // Invalid protocol
		Account:          &datamodel.Account{Name: "account-1"},
	}

	// Mock activities
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume, nil, nil)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "unsupported or unspecified protocol")
}

func (s *UnitTestSuite) Test_RestoreVolumeWorkflow_SnapmirrorTransferFailedStatus() {
	// Test restore workflow when snapmirror transfer fails with "failed" status
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
		Account:          &datamodel.Account{Name: "account-1"},
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
		Attributes:    &datamodel.BackupAttributes{BucketName: "test-bucket", EndpointUUID: "test-uuid"},
	}

	// Register activities
	s.env.RegisterActivity(backupActivity.GetSmSourcePathActivity)
	s.env.RegisterActivity(backupActivity.GetSmSourcePathForRestoreActivity)
	s.env.RegisterActivity(activities.GetObjStoreNameFromBackup)
	s.env.RegisterActivity(activities.GetBucketDetailsFromBackup)

	// Mock activities
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
	s.env.OnActivity(backupActivity.GetSmSourcePathActivity, mock.Anything, mock.Anything).Return("test-dest", nil)
	s.env.OnActivity(backupActivity.GetSmSourcePathForRestoreActivity, mock.Anything, mock.Anything, mock.Anything).Return("test-source", nil)
	s.env.OnActivity(activities.GetObjStoreNameFromBackup, mock.Anything, mock.Anything).Return("test-obj-store", nil)
	s.env.OnActivity(activities.GetBucketDetailsFromBackup, mock.Anything, mock.Anything).Return(&datamodel.BucketDetails{BucketName: "test-bucket"}, nil)
	s.env.OnActivity(backupActivity.GetOrCreateObjectStore, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.CloudTarget{}, nil)
	s.env.OnActivity(backupActivity.SnapmirrorGetOrCreate, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.SnapmirrorRelationship{UUID: "uuid"}, nil)
	s.env.OnActivity(backupActivity.SnapmirrorTransfer, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupActivity.GetSnapmirrorTransferStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("failed", nil) // Return failed status
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{BackupPath: "test-backup-path"}, volume, backupVault, backup)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "snapmirror transfer failed for restore with status: failed")
}

func (s *UnitTestSuite) Test_RestoreVolumeWorkflow_SnapmirrorTransferContinuesPolling() {
	// Test restore workflow continues polling while transfer is in progress
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
		Account:          &datamodel.Account{Name: "account-1"},
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
		Attributes:    &datamodel.BackupAttributes{BucketName: "test-bucket", EndpointUUID: "test-uuid"},
	}

	// Register activities
	s.env.RegisterActivity(backupActivity.GetSmSourcePathActivity)
	s.env.RegisterActivity(backupActivity.GetSmSourcePathForRestoreActivity)
	s.env.RegisterActivity(activities.GetObjStoreNameFromBackup)
	s.env.RegisterActivity(activities.GetBucketDetailsFromBackup)

	// Mock activities
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
	s.env.OnActivity(backupActivity.GetSmSourcePathActivity, mock.Anything, mock.Anything).Return("test-dest", nil)
	s.env.OnActivity(backupActivity.GetSmSourcePathForRestoreActivity, mock.Anything, mock.Anything, mock.Anything).Return("test-source", nil)
	s.env.OnActivity(activities.GetObjStoreNameFromBackup, mock.Anything, mock.Anything).Return("test-obj-store", nil)
	s.env.OnActivity(activities.GetBucketDetailsFromBackup, mock.Anything, mock.Anything).Return(&datamodel.BucketDetails{BucketName: "test-bucket"}, nil)
	s.env.OnActivity(backupActivity.GetOrCreateObjectStore, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.CloudTarget{}, nil)
	s.env.OnActivity(backupActivity.SnapmirrorGetOrCreate, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.SnapmirrorRelationship{UUID: "uuid"}, nil)
	s.env.OnActivity(backupActivity.SnapmirrorTransfer, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Set up polling: first "transferring", then "success"
	call1 := s.env.OnActivity(backupActivity.GetSnapmirrorTransferStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	call1.Return("transferring", nil).Once()
	call2 := s.env.OnActivity(backupActivity.GetSnapmirrorTransferStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	call2.Return("success", nil).Once()

	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateLunName, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{
		ProviderResponse: vsa.ProviderResponse{Name: "test-lun", ExternalUUID: "lun-uuid"},
		SerialNumber:     "test-serial",
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateLunMap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.InitiateSplitForVolume, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{BackupPath: "test-backup-path"}, volume, backupVault, backup)

	// Assert workflow succeeded after polling
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_CreateVolumeInONTAPError() {
	// Test error in CreateVolumeInONTAP activity
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
		Account:          &datamodel.Account{Name: "account-1"},
	}

	// Mock activities - CreateVolumeInONTAP fails
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to create volume in ONTAP"))
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume, nil, nil)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "failed to create volume in ONTAP")
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_NoSnapshotPolicy() {
	// Test workflow when volume has no snapshot policy
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
		Account:          &datamodel.Account{Name: "account-1"},
		// No SnapshotPolicy set
	}

	// Mock activities
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{
		ProviderResponse: vsa.ProviderResponse{Name: "test-lun", ExternalUUID: "lun-uuid"},
		SerialNumber:     "test-serial",
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateLunMap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.InitiateSplitForVolume, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume, nil, nil)

	// Assert workflow succeeded
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_GetNodeError() {
	// Test error in GetNode activity
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}

	volume := &datamodel.Volume{
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			}},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}, Protocols: []string{utils.ProtocolISCSI}},
		Account:          &datamodel.Account{Name: "account-1"},
	}

	// Mock activities - GetNode fails
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return(nil, errors.New("failed to get node"))

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume, nil, nil)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "failed to get node")
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_PostChildWorkflowError() {
	// Test error in post child workflow
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
		Account:          &datamodel.Account{Name: "account-1"},
	}

	// Mock activities
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return(nil, errors.New("failed to get hosts"))
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume, nil, nil)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "failed to get hosts")
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_GetSmSourcePathActivityError() {
	// Test error in backup restore workflow - GetSmSourcePathActivity
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
		Account:          &datamodel.Account{Name: "account-1"},
	}

	backupVault := &datamodel.BackupVault{
		Name:          "test-backup-vault",
		BucketDetails: datamodel.BucketDetailsArray{&datamodel.BucketDetails{BucketName: "test-bucket"}},
	}
	backup := &datamodel.Backup{
		State:         "InProgress",
		Name:          "test-backup",
		VolumeUUID:    "test-vol",
		BackupVault:   backupVault,
		BackupVaultID: 1,
		Attributes:    &datamodel.BackupAttributes{BucketName: "test-bucket"},
	}

	// Mock activities
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
	s.env.OnActivity(backupActivity.GetSmSourcePathActivity, mock.Anything, mock.Anything).Return("", errors.New("failed to get SM source path"))
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{BackupPath: "test-backup-path"}, volume, backupVault, backup)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "failed to get SM source path")
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_GetSmSourcePathForRestoreActivityError() {
	// Test error in backup restore workflow - GetSmSourcePathForRestoreActivity
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
		Account:          &datamodel.Account{Name: "account-1"},
	}

	backupVault := &datamodel.BackupVault{
		Name:          "test-backup-vault",
		BucketDetails: datamodel.BucketDetailsArray{&datamodel.BucketDetails{BucketName: "test-bucket"}},
	}
	backup := &datamodel.Backup{
		State:         "InProgress",
		Name:          "test-backup",
		VolumeUUID:    "test-vol",
		BackupVault:   backupVault,
		BackupVaultID: 1,
		Attributes:    &datamodel.BackupAttributes{BucketName: "test-bucket"},
	}

	// Mock activities
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
	s.env.OnActivity(backupActivity.GetSmSourcePathActivity, mock.Anything, mock.Anything).Return("test-destination-path", nil)
	s.env.OnActivity(backupActivity.GetSmSourcePathForRestoreActivity, mock.Anything, mock.Anything, mock.Anything).Return("", errors.New("failed to get SM source path for restore"))
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{BackupPath: "test-backup-path"}, volume, backupVault, backup)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "failed to get SM source path for restore")
}
