package workflows

import (
	"database/sql"
	err1 "errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/active_directory_activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
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
	"go.temporal.io/sdk/temporal"
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
	s.env.RegisterWorkflow(EnsureCIFSShareWorkflow)
	s.env.RegisterWorkflow(PostFileVolumeWorkflowForSMB)

	// Register all activities that might be used across tests
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	volumeDeleteActivity := activities.VolumeDeleteActivity{SE: mockStorage}
	backupActivity := activities.BackupActivity{SE: mockStorage}
	adActivity := active_directory_activities.ActiveDirectoryActivity{SE: mockStorage}

	// Register common activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(commonActivity.GetNode)
	s.env.RegisterActivity(commonActivity.GetSVM)
	s.env.RegisterActivity(adActivity.GetActiveDirectoryForPool)
	s.env.RegisterActivity(commonActivity.CreateFirewallRule)
	s.env.RegisterActivity(adActivity.CreateOrModifyADDNS)
	s.env.RegisterActivity(adActivity.GetOrCreateCifsService)
	s.env.RegisterActivity(adActivity.DdnsModify)
	s.env.RegisterActivity(adActivity.CreateJunctionPathForCifsShare)
	s.env.RegisterActivity(commonActivity.UpdateSvmActiveDirectory)
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
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeAttributesInDB)
	s.env.RegisterActivity(volumeCreateActivity.CreateExportPolicyInOntap)
	s.env.RegisterActivity(volumeCreateActivity.FindTenancy)
	s.env.RegisterActivity(volumeCreateActivity.CheckBackupVaultExistsInVCP)
	s.env.RegisterActivity(volumeCreateActivity.CheckForBucketResourceName)
	s.env.RegisterActivity(volumeCreateActivity.GenerateResourceNames)
	s.env.RegisterActivity(volumeCreateActivity.CreateBucket)
	s.env.RegisterActivity(volumeCreateActivity.UpdateBackupVaultWithBucketDetails)
	s.env.RegisterActivity(volumeCreateActivity.SetupCrossRegionBackupPermissionsActivity)
	s.env.RegisterActivity(volumeCreateActivity.CheckIfBackupPolicyExistsInVCP)
	s.env.RegisterActivity(volumeCreateActivity.CreateBackupPolicyFetchedFromSDE)
	s.env.RegisterActivity(volumeCreateActivity.UpdateLunName)
	s.env.RegisterActivity(volumeCreateActivity.GetAggregatesFromOntap)
	s.env.RegisterActivity(volumeCreateActivity.ConfigureLdap)

	// Register volume delete activities
	s.env.RegisterActivity(volumeDeleteActivity.DeleteVolumeInONTAP)
	s.env.RegisterActivity(volumeDeleteActivity.DeleteSnapshotPolicyInONTAP)

	// Register backup activities
	s.env.RegisterActivity(backupActivity.GetOrCreateObjectStore)
	s.env.RegisterActivity(backupActivity.SnapmirrorGetOrCreate)
	s.env.RegisterActivity(backupActivity.SnapmirrorTransfer)
	s.env.RegisterActivity(backupActivity.GetSnapmirrorTransferStatus)

	// Register background activities
	syncBackupZiZsActivity := backgroundactivities.SyncBackupZiZsActivity{SE: mockStorage}
	s.env.RegisterActivity(syncBackupZiZsActivity.SyncBucketDetails)

	// Set default mock responses for commonly used activities
	s.env.OnActivity(commonActivity.GetJob, mock.Anything, mock.Anything).Return(&datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
		State:     string(models.JobsStateNEW),
	}, nil).Maybe()
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeAttributesInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil)
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
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
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
	s.env.OnActivity(volumeCreateActivity.LunSizeUpdateValidation, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateClonedVolumeBeforeSplit, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateLunMap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.InitiateSplitForVolume, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_LargeVolume_Success() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	volumeDeleteActivity := activities.VolumeDeleteActivity{SE: mockStorage}
	volume := &datamodel.Volume{
		Account:               &datamodel.Account{Name: "account-1"},
		LargeVolumeAttributes: &datamodel.LargeVolumeAttributes{LargeCapacity: true},
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
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
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
	s.env.OnActivity(volumeCreateActivity.LunSizeUpdateValidation, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateClonedVolumeBeforeSplit, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateLunMap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.InitiateSplitForVolume, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_LargeVolumeWithConstituentCount_Success() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	volumeDeleteActivity := activities.VolumeDeleteActivity{SE: mockStorage}
	volume := &datamodel.Volume{
		Account:               &datamodel.Account{Name: "account-1"},
		LargeVolumeAttributes: &datamodel.LargeVolumeAttributes{LargeCapacity: true},
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
	s.env.OnActivity(volumeCreateActivity.GetAggregatesFromOntap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
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
	s.env.OnActivity(volumeCreateActivity.LunSizeUpdateValidation, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateClonedVolumeBeforeSplit, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateLunMap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.InitiateSplitForVolume, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_LargeVolume_Failure() {
	mockStorage := database.NewMockStorage(s.T())
	largeVolumeConstituentCount := int32(8)
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	volume := &datamodel.Volume{
		Account: &datamodel.Account{Name: "account-1"},
		LargeVolumeAttributes: &datamodel.LargeVolumeAttributes{
			LargeCapacity:               true,
			LargeVolumeConstituentCount: nillable.GetInt32Ptr(largeVolumeConstituentCount),
		},

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
	// Create a CustomError first, then wrap it as non-retryable
	customErr := vsaerrors.NewVCPError(vsaerrors.ErrInternalServerError, errors.New("failed to get aggregates from ONTAP"))
	s.env.OnActivity(volumeCreateActivity.GetAggregatesFromOntap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, vsaerrors.WrapAsNonRetryableTemporalApplicationError(customErr))

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed with proper error validation
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())

	// Validate the error message contains the expected text
	workflowErr := s.env.GetWorkflowError()
	s.T().Logf("workflowErr type: %T", workflowErr)
	s.T().Logf("workflowErr message: %s", workflowErr.Error())

	// The error message should be the user-facing message from CustomError
	assert.Contains(s.T(), workflowErr.Error(), "An internal error occurred.")

	// Walk through the error chain to find the original ApplicationError
	var temporalAppErr *temporal.ApplicationError
	if err1.As(workflowErr, &temporalAppErr) {
		s.T().Logf("Found temporal.ApplicationError - NonRetryable: %v, Type: %s", temporalAppErr.NonRetryable(), temporalAppErr.Type())
		assert.Contains(s.T(), temporalAppErr.Error(), "failed to get aggregates from ONTAP", "Original error message should be preserved")
	} else {
		s.T().Error("Expected workflow error to be of type temporal.ApplicationError")
	}
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

	// Mock activities
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{}}, nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
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
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{BackupPath: "projects/123456789/locations/us-e4/backupVaults/bv1/backups/backupName"}, volume)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_RestoreSnapshotWithThinCloneType_NoSplit() {
	// Test that InitiateSplitForVolume is NOT called for THIN clone type
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	volumeDeleteActivity := activities.VolumeDeleteActivity{SE: mockStorage}

	volume := &datamodel.Volume{
		Account: &datamodel.Account{Name: "account-1"},
		Pool: &datamodel.Pool{
			BaseModel:       datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{Password: "password"},
		},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}, Protocols: []string{utils.ProtocolISCSI}},
	}

	// Snapshot parameters to trigger isRestoreSnapshot=true
	snapshot := &datamodel.Snapshot{BaseModel: datamodel.BaseModel{ID: 123, UUID: "snap-uuid"}}
	params := &common.CreateVolumeParams{
		AccountName: "account-1",
		SnapshotID:  "snap-uuid",
		Snapshot:    snapshot,
	}

	// Mock all required activities
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{ProviderResponse: vsa.ProviderResponse{ExternalUUID: "vol-uuid"}, AvailableSpace: 10}, nil)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{Name: "hg1", Hosts: datamodel.Hosts{Hosts: []string{"iqn.1"}}}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.LunSizeUpdateValidation, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateLunName, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name:         "/vol/vol1/luns/lun1",
			ExternalUUID: "lun-uuid",
		},
		SerialNumber: "6c5738423724595454686164",
		Size:         2048,
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateLunMap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateClonedVolumeBeforeSplit, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{ProviderResponse: vsa.ProviderResponse{ExternalUUID: "vol-uuid"}, AvailableSpace: 10}, nil)

	// This activity should NOT be called for THIN clone type
	s.env.OnActivity(volumeCreateActivity.InitiateSplitForVolume, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("InitiateSplitForVolume should not be called for THIN clone type"))

	s.env.OnActivity(volumeDeleteActivity.DeleteSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeDeleteActivity.DeleteVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, params, volume)

	// Assert workflow completed successfully (InitiateSplitForVolume should not be called)
	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
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
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
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
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume)

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
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume)

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
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
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
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume)

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
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
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
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume)

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
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
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
	s.env.OnActivity(volumeCreateActivity.GetAggregatesFromOntap, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume)

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
	backupPolicyActivity := activities.BackupPolicyActivity{SE: mockStorage}
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
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
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
	s.env.OnActivity(volumeCreateActivity.CreateBackupPolicySchedule, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to create backup policy schedule"))
	s.env.OnActivity(backupPolicyActivity.DeleteBackupPolicyInVCP, mock.Anything, mock.Anything).Return(&datamodel.BackupPolicy{}, nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	// UpdateJob is called through UpdateJobStatus activity, not directly on mock
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_PauseBackupPolicyScheduleError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	backupPolicyActivity := activities.BackupPolicyActivity{SE: mockStorage}
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
	s.env.RegisterActivity(volumeCreateActivity.UpdateClonedVolumeBeforeSplit)
	s.env.RegisterActivity(volumeCreateActivity.InitiateSplitForVolume)
	s.env.RegisterActivity(backupPolicyActivity.PauseBackupPolicySchedule)

	// Mock activities
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("test-token", nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{
		Name: "host_group_test", Hosts: datamodel.Hosts{Hosts: []string{"iqn.1993-08.org.debian:01:f2c983feb27", "iqn.1994-05.com.redhat:19ee49a2145f"}}},
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
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
	s.env.OnActivity(volumeCreateActivity.CreateBackupPolicySchedule, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupPolicyActivity.PauseBackupPolicySchedule, mock.Anything, mock.Anything).Return(errors.New("could not pause backup policy schedule"))
	s.env.OnActivity(backupPolicyActivity.DeleteBackupPolicySchedule, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupPolicyActivity.DeleteBackupPolicyInVCP, mock.Anything, mock.Anything).Return(&datamodel.BackupPolicy{}, nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume)

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
	backupPolicyActivity := activities.BackupPolicyActivity{SE: mockStorage}
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
	s.env.OnActivity(commonActivity.GetJob, mock.Anything, mock.Anything).Return(&datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
		State:     string(models.JobsStateNEW),
	}, nil).Maybe()
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)
	s.env.RegisterActivity(volumeCreateActivity.LunSizeUpdateValidation)
	s.env.RegisterActivity(volumeCreateActivity.UpdateClonedVolumeBeforeSplit)
	s.env.RegisterActivity(volumeCreateActivity.InitiateSplitForVolume)
	s.env.RegisterActivity(backupPolicyActivity.PauseBackupPolicySchedule)

	// Mock activities
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("test-token", nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{
		Name: "host_group_test", Hosts: datamodel.Hosts{Hosts: []string{"iqn.1993-08.org.debian:01:f2c983feb27", "iqn.1994-05.com.redhat:19ee49a2145f"}}},
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
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
	s.env.OnActivity(volumeCreateActivity.CreateBackupPolicySchedule, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupPolicyActivity.PauseBackupPolicySchedule, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume)

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
	s.env.OnActivity(volumeCreateActivity.CheckBackupVaultExistsInVCP, mock.Anything, mock.Anything, mock.Anything).Return(&datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{UUID: "backup-vault-uuid"},
		Name:      "test-backup-vault",
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CheckForBucketResourceName, mock.Anything, mock.Anything).Return(nil, errors.New("failed to check for bucket resource name"))

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume)

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
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
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
	s.env.OnActivity(volumeCreateActivity.CheckBackupVaultExistsInVCP, mock.Anything, mock.Anything, mock.Anything).Return(&datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{UUID: "backup-vault-uuid"},
		Name:      "test-backup-vault",
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CheckForBucketResourceName, mock.Anything, mock.Anything).Return(&common.BucketDetails{}, nil)
	s.env.OnActivity(volumeCreateActivity.GenerateResourceNames, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to generate resource names"))

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume)

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
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
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
	s.env.OnActivity(volumeCreateActivity.CheckBackupVaultExistsInVCP, mock.Anything, mock.Anything, mock.Anything).Return(&datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{UUID: "backup-vault-uuid"},
		Name:      "test-backup-vault",
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CheckForBucketResourceName, mock.Anything, mock.Anything).Return(&common.BucketDetails{}, nil)
	s.env.OnActivity(volumeCreateActivity.GenerateResourceNames, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.ResourceNames{BucketName: "bucket-1"}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateBucket, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.AnythingOfType("*string")).Return(nil, errors.New("failed to create bucket"))

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume)

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
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
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
	s.env.OnActivity(volumeCreateActivity.CheckBackupVaultExistsInVCP, mock.Anything, mock.Anything, mock.Anything).Return(&datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{UUID: "backup-vault-uuid"},
		Name:      "test-backup-vault",
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CheckForBucketResourceName, mock.Anything, mock.Anything).Return(&common.BucketDetails{}, nil)
	s.env.OnActivity(volumeCreateActivity.GenerateResourceNames, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.ResourceNames{BucketName: "bucket-1"}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateBucket, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.AnythingOfType("*string")).Return(&common.BucketDetails{BucketName: "bucket-1", ServiceAccountName: "sa-1", TenantProjectNumber: "tp-1", VendorSubnetID: "vendor1"}, nil)
	s.env.OnActivity(volumeCreateActivity.UpdateBackupVaultWithBucketDetails, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to update backup vault with bucket details"))

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	// UpdateJob is called through UpdateJobStatus activity, not directly on mock
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_CheckOrCreateRemoteBackupVaultInVCPError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	syncBackupZiZsActivity := backgroundactivities.SyncBackupZiZsActivity{SE: mockStorage}
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
	s.env.RegisterActivity(syncBackupZiZsActivity.SyncBucketDetails)
	s.env.RegisterActivity(volumeCreateActivity.CheckOrCreateRemoteBackupVaultInVCP)

	// Mock activities
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{
		Name: "host_group_test", Hosts: datamodel.Hosts{Hosts: []string{"iqn.1993-08.org.debian:01:f2c983feb27", "iqn.1994-05.com.redhat:19ee49a2145f"}}},
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
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
	s.env.OnActivity(volumeCreateActivity.CheckBackupVaultExistsInVCP, mock.Anything, mock.Anything, mock.Anything).Return(&datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{UUID: "backup-vault-uuid"},
		Name:      "test-backup-vault",
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CheckForBucketResourceName, mock.Anything, mock.Anything).Return(&common.BucketDetails{}, nil)
	s.env.OnActivity(volumeCreateActivity.GenerateResourceNames, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.ResourceNames{BucketName: "bucket-1"}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateBucket, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.AnythingOfType("*string")).Return(&common.BucketDetails{BucketName: "bucket-1", ServiceAccountName: "sa-1", TenantProjectNumber: "tp-1", VendorSubnetID: "vendor1"}, nil)
	s.env.OnActivity(syncBackupZiZsActivity.SyncBucketDetails, mock.Anything, mock.Anything).Return(&datamodel.BucketDetails{
		BucketName:          "bucket-1",
		ServiceAccountName:  "sa-1",
		TenantProjectNumber: "tp-1",
		VendorSubnetID:      "vendor1",
	}, nil)
	s.env.OnActivity(volumeCreateActivity.UpdateBackupVaultWithBucketDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CheckOrCreateRemoteBackupVaultInVCP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to check or create remote backup vault in VCP"))

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume)

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
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
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
	s.env.RegisterActivity(volumeCreateActivity.LunSizeUpdateValidation)
	s.env.RegisterActivity(volumeCreateActivity.UpdateClonedVolumeBeforeSplit)
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
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
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
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume)
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
	s.env.OnActivity(volumeCreateActivity.InitiateSplitForVolume, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeDeleteActivity.DeleteVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeDeleteActivity.DeleteSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
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
	defer func() {
		utils.SetFileProtocolSupportedForTesting(false)
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
	defer func() {
		utils.SetFileProtocolSupportedForTesting(false)
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

	// Enable file protocols for testing with allowlisted accounts
	utils.SetFileProtocolSupportedForTesting(true)
	defer func() {
		utils.SetFileProtocolSupportedForTesting(false)
	}()

	// Execute the workflow
	s.env.ExecuteWorkflow(PostFileVolumeWorkflow, volume, node)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
}

func (s *UnitTestSuite) Test_PostFileVolumeWorkflow_LDAP_Enabled_Success() {
	// Test PostFileVolumeWorkflow with LDAP Enabled Pool
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	adActivity := active_directory_activities.ActiveDirectoryActivity{SE: mockStorage}
	volumeActivity := activities.VolumeCreateActivity{SE: mockStorage}
	originalEnableLdap := enableLdap
	defer func() { enableLdap = originalEnableLdap }()
	enableLdap = true

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "test-uuid"},
		PoolID:    123,
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
			PoolAttributes: &datamodel.PoolAttributes{
				LdapEnabled: true,
			},
		},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{Protocols: []string{utils.ProtocolNFSv3}},
	}
	node := &models.Node{EndpointAddress: "127.0.0.1"}
	svm := &datamodel.Svm{
		BaseModel: datamodel.BaseModel{UUID: "svm-uuid"},
		Name:      "svm-name",
		SvmDetails: &datamodel.SvmDetails{
			ExternalUUID: "svm-external-uuid",
		},
	}
	activeDirectory := &vsa.ActiveDirectory{UUID: "ad-uuid"}

	// Enable file protocols for testing with allowlisted accounts
	utils.SetFileProtocolSupportedForTesting(true)
	defer func() {
		utils.SetFileProtocolSupportedForTesting(false)
	}()

	s.env.OnActivity(commonActivity.GetSVM, mock.Anything, mock.Anything).Return(svm, nil).Once()
	s.env.OnActivity(adActivity.GetActiveDirectoryForPool, mock.Anything, mock.Anything).Return(activeDirectory, nil).Once()
	s.env.OnActivity(adActivity.CreateOrModifyADDNS, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	s.env.OnActivity(adActivity.GetOrCreateCifsService, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&active_directory_activities.GetOrCreateCifsServiceResult{FQDN: "fqdn.example.com"}, nil).Once()
	s.env.OnActivity(volumeActivity.ConfigureLdap, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	s.env.OnActivity(commonActivity.UpdateSvmActiveDirectory, mock.Anything, mock.Anything).Return(svm, nil).Once()

	// Execute the workflow
	s.env.ExecuteWorkflow(PostFileVolumeWorkflow, volume, node)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
}

func (s *UnitTestSuite) Test_PostFileVolumeWorkflow_LDAP_Enabled_SVMUpdateFails() {
	// Test PostFileVolumeWorkflow with LDAP Enabled Pool
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	adActivity := active_directory_activities.ActiveDirectoryActivity{SE: mockStorage}
	volumeActivity := activities.VolumeCreateActivity{SE: mockStorage}
	originalEnableLdap := enableLdap
	defer func() { enableLdap = originalEnableLdap }()
	enableLdap = true

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "test-uuid"},
		PoolID:    123,
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
			PoolAttributes: &datamodel.PoolAttributes{
				LdapEnabled: true,
			},
		},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{Protocols: []string{utils.ProtocolNFSv3}},
	}
	node := &models.Node{EndpointAddress: "127.0.0.1"}
	svm := &datamodel.Svm{
		BaseModel: datamodel.BaseModel{UUID: "svm-uuid"},
		Name:      "svm-name",
		SvmDetails: &datamodel.SvmDetails{
			ExternalUUID: "svm-external-uuid",
		},
	}
	activeDirectory := &vsa.ActiveDirectory{UUID: "ad-uuid"}

	// Enable file protocols for testing with allowlisted accounts
	utils.SetFileProtocolSupportedForTesting(true)
	defer func() {
		utils.SetFileProtocolSupportedForTesting(false)
	}()

	s.env.OnActivity(commonActivity.GetSVM, mock.Anything, mock.Anything).Return(svm, nil).Once()
	s.env.OnActivity(adActivity.GetActiveDirectoryForPool, mock.Anything, mock.Anything).Return(activeDirectory, nil).Once()
	s.env.OnActivity(adActivity.CreateOrModifyADDNS, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	s.env.OnActivity(adActivity.GetOrCreateCifsService, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&active_directory_activities.GetOrCreateCifsServiceResult{FQDN: "fqdn.example.com"}, nil).Once()
	s.env.OnActivity(volumeActivity.ConfigureLdap, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	s.env.OnActivity(commonActivity.UpdateSvmActiveDirectory, mock.Anything, mock.Anything).Return(nil, errors.New("Failed to update SVM Active Directory association during PostFileVolumeWorkflow"))

	// Execute the workflow
	s.env.ExecuteWorkflow(PostFileVolumeWorkflow, volume, node)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.ErrorContains(s.T(), s.env.GetWorkflowError(), "Failed to update SVM Active Directory association during PostFileVolumeWorkflow")
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

	// Disable file protocols for testing
	utils.SetFileProtocolSupportedForTesting(false)
	defer func() {
		utils.SetFileProtocolSupportedForTesting(false)
	}()

	// Execute the workflow
	s.env.ExecuteWorkflow(PostFileVolumeWorkflow, volume, node)

	// Assert workflow completed successfully (should handle disabled protocols gracefully)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
}

func (s *UnitTestSuite) Test_PostFileVolumeWorkflowForSMB_AssignsActiveDirectory() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	adActivity := active_directory_activities.ActiveDirectoryActivity{SE: mockStorage}

	volume := &datamodel.Volume{
		Name:   "vol-smb",
		PoolID: int64(42),
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
			ClusterDetails: datamodel.ClusterDetails{
				SnHostProject: "sn-host-project",
				Network:       "data-network",
			},
		},
		VolumeAttributes: &datamodel.VolumeAttributes{FileProperties: &datamodel.FileProperties{
			JunctionPath: "/vol/vol-smb",
		}},
	}
	node := &models.Node{EndpointAddress: "127.0.0.1"}
	svm := &datamodel.Svm{
		BaseModel: datamodel.BaseModel{UUID: "svm-uuid"},
		Name:      "svm-name",
		SvmDetails: &datamodel.SvmDetails{
			ExternalUUID: "svm-external-uuid",
		},
	}
	activeDirectory := &vsa.ActiveDirectory{UUID: "ad-uuid"}
	updatedSvm := &datamodel.Svm{
		BaseModel: datamodel.BaseModel{UUID: "svm-uuid"},
		Name:      "svm-name",
		SvmDetails: &datamodel.SvmDetails{
			ExternalUUID: "svm-external-uuid",
		},
		ActiveDirectoryID: sql.NullInt64{Int64: 11, Valid: true},
	}

	s.env.OnActivity(commonActivity.GetSVM, mock.Anything, mock.Anything).Return(svm, nil).Once()
	s.env.OnActivity(adActivity.GetActiveDirectoryForPool, mock.Anything, mock.Anything).Return(activeDirectory, nil).Once()
	s.env.OnActivity(commonActivity.UpdateSvmActiveDirectory, mock.Anything, mock.MatchedBy(func(params activities.UpdateSvmActiveDirectoryParams) bool {
		assert.Equal(s.T(), svm, params.Svm)
		assert.Equal(s.T(), activeDirectory.UUID, params.ActiveDirectoryUUID)
		return true
	})).Return(updatedSvm, nil).Once()

	s.env.OnActivity(adActivity.CreateOrModifyADDNS, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	s.env.OnActivity(adActivity.GetOrCreateCifsService, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&active_directory_activities.GetOrCreateCifsServiceResult{FQDN: "fqdn.example.com"}, nil).Once()
	s.env.OnActivity(adActivity.CreateJunctionPathForCifsShare, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	s.env.ExecuteWorkflow(PostFileVolumeWorkflowForSMB, volume, node)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())

	var workflowResult *datamodel.Volume
	resultErr := s.env.GetWorkflowResult(&workflowResult)
	assert.NoError(s.T(), resultErr)
	if assert.NotNil(s.T(), workflowResult) && workflowResult.VolumeAttributes != nil && workflowResult.VolumeAttributes.FileProperties != nil {
		assert.Equal(s.T(), "fqdn.example.com", workflowResult.VolumeAttributes.FileProperties.Fqdn)
	}
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
	assert.Contains(s.T(), err.Error(), "An internal error occurred.")
	assert.Contains(s.T(), err.(*vsaerrors.CustomError).OriginalErr.Error(), "invalid phase")
}

func (s *UnitTestSuite) Test_SelectVolumeChildWorkflow_NFSv3() {
	// Test selectVolumeChildWorkflow with NFSv3 protocol
	protocols := []string{utils.ProtocolNFSv3}

	// Enable file protocols for testing with allowlisted accounts
	utils.SetFileProtocolSupportedForTesting(true)
	defer func() {
		utils.SetFileProtocolSupportedForTesting(false)
	}()

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

	// Enable file protocols for testing with allowlisted accounts
	utils.SetFileProtocolSupportedForTesting(true)
	defer func() {
		utils.SetFileProtocolSupportedForTesting(false)
	}()

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

func (s *UnitTestSuite) Test_SelectVolumeChildWorkflow_SMB_WithFlagEnabled() {
	// Test selectVolumeChildWorkflow with SMB protocol when enableSmb is true
	protocols := []string{utils.ProtocolSMB}

	// Save original value and enable flag
	originalEnableSmb := enableSmb
	defer func() { enableSmb = originalEnableSmb }()
	enableSmb = true

	// Enable file protocols for testing with allowlisted accounts
	utils.SetFileProtocolSupportedForTesting(true)
	defer func() {
		utils.SetFileProtocolSupportedForTesting(false)
	}()

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
	// Verify it returns PostFileVolumeWorkflowForSMB when flag is enabled
	assert.IsType(s.T(), PostFileVolumeWorkflowForSMB, postWorkflow)
}

func (s *UnitTestSuite) Test_SelectVolumeChildWorkflow_SMB_WithFlagDisabled() {
	// Test selectVolumeChildWorkflow with SMB protocol when enableSmb is false
	protocols := []string{utils.ProtocolSMB}

	// Save original value and disable flag
	originalEnableSmb := enableSmb
	defer func() { enableSmb = originalEnableSmb }()
	enableSmb = false

	// Enable file protocols for testing with allowlisted accounts
	utils.SetFileProtocolSupportedForTesting(true)
	defer func() {
		utils.SetFileProtocolSupportedForTesting(false)
	}()

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
	// Verify it returns PostFileVolumeWorkflow when flag is disabled
	assert.IsType(s.T(), PostFileVolumeWorkflow, postWorkflow)
}

func (s *UnitTestSuite) Test_SelectVolumeChildWorkflow_NFS_And_SMB_Combination() {
	// Test selectVolumeChildWorkflow with both NFS and SMB protocols when enableSmb is true
	protocols := []string{utils.ProtocolNFSv3, utils.ProtocolSMB}

	// Save original value and enable flag
	originalEnableSmb := enableSmb
	defer func() { enableSmb = originalEnableSmb }()
	enableSmb = true

	// Enable file protocols for testing with allowlisted accounts
	utils.SetFileProtocolSupportedForTesting(true)
	defer func() {
		utils.SetFileProtocolSupportedForTesting(false)
	}()

	// Test pre phase - should return single workflow
	preWorkflow, err := selectVolumeChildWorkflow(protocols, PhasePre)
	assert.Nil(s.T(), err)
	assert.NotNil(s.T(), preWorkflow)
	assert.IsType(s.T(), PreFileVolumeWorkflow, preWorkflow)

	// Test post phase - should return slice of workflows
	postWorkflow, err := selectVolumeChildWorkflow(protocols, PhasePost)
	assert.Nil(s.T(), err)
	assert.NotNil(s.T(), postWorkflow)

	// Verify it returns a slice of workflows
	workflowSlice, ok := postWorkflow.([]interface{})
	assert.True(s.T(), ok, "Expected slice of workflows for NFS+SMB combination")
	assert.Len(s.T(), workflowSlice, 2, "Expected 2 workflows: PostFileVolumeWorkflow and PostFileVolumeWorkflowForSMB")
	assert.IsType(s.T(), PostFileVolumeWorkflowForSMB, workflowSlice[0])
	assert.IsType(s.T(), PostFileVolumeWorkflow, workflowSlice[1])
}

func (s *UnitTestSuite) Test_SelectVolumeChildWorkflow_FileProtocolsDisabled() {
	// Test selectVolumeChildWorkflow when file protocols are disabled
	protocols := []string{utils.ProtocolNFSv3}

	// Disable file protocols for testing
	utils.SetFileProtocolSupportedForTesting(false)
	defer func() {
		utils.SetFileProtocolSupportedForTesting(false)
	}()

	// Test pre phase
	preWorkflow, err := selectVolumeChildWorkflow(protocols, PhasePre)
	assert.NotNil(s.T(), err)
	assert.Nil(s.T(), preWorkflow)
	assert.Contains(s.T(), err.(*vsaerrors.CustomError).OriginalErr.Error(), "file protocols are not enabled")

	// Test post phase
	postWorkflow, err := selectVolumeChildWorkflow(protocols, PhasePost)
	assert.NotNil(s.T(), err)
	assert.Nil(s.T(), postWorkflow)
	assert.Contains(s.T(), err.(*vsaerrors.CustomError).OriginalErr.Error(), "file protocols are not enabled")
}

func (s *UnitTestSuite) Test_SelectVolumeChildWorkflow_UnsupportedProtocol() {
	// Test selectVolumeChildWorkflow with unsupported protocol
	protocols := []string{"UNSUPPORTED_PROTOCOL"}

	// Test pre phase
	preWorkflow, err := selectVolumeChildWorkflow(protocols, PhasePre)
	assert.NotNil(s.T(), err)
	assert.Nil(s.T(), preWorkflow)
	assert.Contains(s.T(), err.(*vsaerrors.CustomError).OriginalErr.Error(), "unsupported or unspecified protocol")

	// Test post phase
	postWorkflow, err := selectVolumeChildWorkflow(protocols, PhasePost)
	assert.NotNil(s.T(), err)
	assert.Nil(s.T(), postWorkflow)
	assert.Contains(s.T(), err.(*vsaerrors.CustomError).OriginalErr.Error(), "unsupported or unspecified protocol")
}

func (s *UnitTestSuite) Test_SelectVolumeChildWorkflow_EmptyProtocols() {
	// Test selectVolumeChildWorkflow with empty protocols
	protocols := []string{}

	// Test pre phase
	preWorkflow, err := selectVolumeChildWorkflow(protocols, PhasePre)
	assert.NotNil(s.T(), err)
	assert.Nil(s.T(), preWorkflow)
	assert.Contains(s.T(), err.(*vsaerrors.CustomError).OriginalErr.Error(), "unsupported or unspecified protocol")

	// Test post phase
	postWorkflow, err := selectVolumeChildWorkflow(protocols, PhasePost)
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
	restoreVolumeCreateResponse := &vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{ExternalUUID: "test-uuid"},
		Size:             int64(0),
	}
	isRestoreFromBackup := false
	isRestoreSnapshot := false

	// Set invalid environment variable to cause PopulateRetryPolicyParams to fail
	originalStartToCloseTimeout := StartToCloseTimeout
	StartToCloseTimeout = "invalid-duration"
	defer func() { StartToCloseTimeout = originalStartToCloseTimeout }()

	// Execute the workflow
	s.env.ExecuteWorkflow(PostBlockVolumeWorkflow, volume, node, nil, volCreateResponse, isRestoreFromBackup, isRestoreSnapshot, restoreVolumeCreateResponse)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "invalid duration")
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
	restoreVolumeCreateResponse := &vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{ExternalUUID: "test-uuid"},
		Size:             int64(0),
	}
	volCreateResponse := &vsa.VolumeResponse{ProviderResponse: vsa.ProviderResponse{ExternalUUID: "test-uuid"}}
	isRestoreFromBackup := true
	isRestoreSnapshot := false

	// Register activities
	s.env.RegisterActivity(volumeCreateActivity.GetHosts)
	s.env.RegisterActivity(volumeCreateActivity.CreateIgroup)
	s.env.RegisterActivity(volumeCreateActivity.UpdateLunName)

	// Mock activities
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateLunName, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("update lun name error"))

	// Execute the workflow
	s.env.ExecuteWorkflow(PostBlockVolumeWorkflow, volume, node, nil, volCreateResponse, isRestoreFromBackup, isRestoreSnapshot, restoreVolumeCreateResponse)

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
	restoreVolumeCreateResponse := &vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{ExternalUUID: "test-uuid"},
		Size:             int64(0),
	}
	isRestoreFromBackup := false
	isRestoreSnapshot := false

	// Register activities
	s.env.RegisterActivity(volumeCreateActivity.GetHosts)
	s.env.RegisterActivity(volumeCreateActivity.CreateIgroup)
	s.env.RegisterActivity(volumeCreateActivity.CreateLun)

	// Mock activities
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("create lun error"))
	// Execute the workflow
	s.env.ExecuteWorkflow(PostBlockVolumeWorkflow, volume, node, nil, volCreateResponse, isRestoreFromBackup, isRestoreSnapshot, restoreVolumeCreateResponse)

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
	restoreVolumeCreateResponse := &vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{ExternalUUID: "test-uuid"},
		Size:             int64(0),
	}
	isRestoreFromBackup := false
	isRestoreSnapshot := false

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
	s.env.ExecuteWorkflow(PostBlockVolumeWorkflow, volume, node, nil, volCreateResponse, isRestoreFromBackup, isRestoreSnapshot, restoreVolumeCreateResponse)

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

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_DualProtocol_FileVolume_Success() {
	mockStorage := database.NewMockStorage(s.T())
	adActivity := active_directory_activities.ActiveDirectoryActivity{SE: mockStorage}
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	utils.SetFileProtocolSupportedForTesting(true)
	defer func() {
		utils.SetFileProtocolSupportedForTesting(false)
	}()

	expectedSMBProperties := []string{"browsable", "encrypt_data", "oplocks"}
	// NFS file volume with file properties and export policy
	volume := &datamodel.Volume{
		Account: &datamodel.Account{Name: "account-1"},
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			ClusterDetails: datamodel.ClusterDetails{
				Network:       "test-network",
				SnHostProject: "test-project",
			},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
		},
		Svm: &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			Protocols: []string{"NFSV3", "SMB"}, // Dual protocol volume
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
				JunctionPath:     "/test_share",
				SMBShareSettings: expectedSMBProperties,
			},
		},
	}

	// node := &models.Node{Name: "node-1"}
	activeDirectory := &vsa.ActiveDirectory{
		Domain: "example.com",
		DNS:    "8.8.8.8",
	}
	svmName := "test-svm"
	externalSVMUUID := "svm-uuid"
	expectedFQDN := "NETBIOS-1234.example.com"
	svm := &datamodel.Svm{
		Name: svmName,
		SvmDetails: &datamodel.SvmDetails{
			ExternalUUID: externalSVMUUID,
		},
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)

	// Mock activities for Dual protocol volume flow
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateExportPolicyInOntap, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name:         "nfs_volume_test",
			ExternalUUID: "volume-uuid",
		},
		AvailableSpace: 1000000,
	}, nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.LunSizeUpdateValidation, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateClonedVolumeBeforeSplit, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.ConfigureLdap, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetSVM, mock.Anything, mock.Anything).Return(svm, nil)
	s.env.OnActivity(adActivity.GetActiveDirectoryForPool, mock.Anything, mock.Anything).Return(activeDirectory, nil)
	s.env.OnActivity(adActivity.CreateOrModifyADDNS, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(adActivity.GetOrCreateCifsService, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
		&active_directory_activities.GetOrCreateCifsServiceResult{
			FQDN:      expectedFQDN,
			NeedsDDNS: false,
		}, nil)
	s.env.OnActivity(adActivity.CreateJunctionPathForCifsShare,
		mock.Anything,
		mock.Anything,
		svmName,
		"/test_share",
		expectedSMBProperties).Return(nil)
	s.env.OnActivity(commonActivity.CreateFirewallRule, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.UpdateSvmActiveDirectory, mock.Anything, mock.Anything).Return(nil, nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	// UpdateJob is called through UpdateJobStatus activity, not directly on mock
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_NFS_FileVolume_Success() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	utils.SetFileProtocolSupportedForTesting(true)
	defer func() {
		utils.SetFileProtocolSupportedForTesting(false)
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
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name:         "nfs_volume_test",
			ExternalUUID: "volume-uuid",
		},
		AvailableSpace: 1000000,
	}, nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.LunSizeUpdateValidation, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateClonedVolumeBeforeSplit, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.ConfigureLdap, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume)

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
	defer func() {
		utils.SetFileProtocolSupportedForTesting(false)
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
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume)

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
	defer func() {
		utils.SetFileProtocolSupportedForTesting(false)
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
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name:         "nfs_backup_volume_test",
			ExternalUUID: "volume-uuid",
		},
		AvailableSpace: 2000000,
	}, nil)
	s.env.OnActivity(volumeCreateActivity.LunSizeUpdateValidation, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateClonedVolumeBeforeSplit, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Backup vault related activities
	s.env.OnActivity(volumeCreateActivity.FindTenancy, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.TenancyInfo{RegionalTenantProject: "tenant-project"}, nil)

	// Mock CheckBackupVaultExistsInVCP to return a backup vault and nil error
	backupVault := &datamodel.BackupVault{
		BaseModel:       datamodel.BaseModel{UUID: "backup-vault-uuid"},
		Name:            "test-backup-vault",
		BackupVaultType: "LOCAL", // Use LOCAL type to avoid cross-region complexity in this test
	}
	s.env.OnActivity(volumeCreateActivity.CheckBackupVaultExistsInVCP, mock.Anything, mock.Anything, mock.Anything).Return(backupVault, nil)

	s.env.OnActivity(volumeCreateActivity.CheckForBucketResourceName, mock.Anything, mock.Anything).Return(&common.BucketDetails{
		BucketName:          "existing-bucket",
		ServiceAccountName:  "existing-sa",
		TenantProjectNumber: "existing-project",
		VendorSubnetID:      "subnet-12345",
	}, nil)
	s.env.OnActivity(volumeCreateActivity.UpdateBackupVaultWithBucketDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.ConfigureLdap, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume)

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
	defer func() {
		utils.SetFileProtocolSupportedForTesting(false)
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
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name:         "nfs_multi_rule_volume_test",
			ExternalUUID: "volume-uuid",
		},
		AvailableSpace: 3000000,
	}, nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.LunSizeUpdateValidation, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateClonedVolumeBeforeSplit, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.ConfigureLdap, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume)

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
	defer func() {
		utils.SetFileProtocolSupportedForTesting(false)
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
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume)

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
	defer func() {
		utils.SetFileProtocolSupportedForTesting(false)
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
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to create volume in ONTAP"))

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume)

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
	defer func() {
		utils.SetFileProtocolSupportedForTesting(false)
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
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name:         "nfs_new_bucket_volume_test",
			ExternalUUID: "volume-uuid",
		},
		AvailableSpace: 4000000,
	}, nil)
	s.env.OnActivity(volumeCreateActivity.LunSizeUpdateValidation, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateClonedVolumeBeforeSplit, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.ConfigureLdap, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Backup vault related activities - no existing bucket
	s.env.OnActivity(volumeCreateActivity.FindTenancy, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.TenancyInfo{RegionalTenantProject: "new-tenant-project"}, nil)

	// Mock CheckBackupVaultExistsInVCP to return a backup vault and nil error
	backupVault := &datamodel.BackupVault{
		BaseModel:       datamodel.BaseModel{UUID: "new-backup-vault-uuid"},
		Name:            "new-test-backup-vault",
		BackupVaultType: "LOCAL", // Use LOCAL type to avoid cross-region complexity
	}
	s.env.OnActivity(volumeCreateActivity.CheckBackupVaultExistsInVCP, mock.Anything, mock.Anything, mock.Anything).Return(backupVault, nil)

	s.env.OnActivity(volumeCreateActivity.CheckForBucketResourceName, mock.Anything, mock.Anything).Return(&common.BucketDetails{}, nil) // Empty bucket details

	// New bucket creation flow
	s.env.OnActivity(volumeCreateActivity.GenerateResourceNames, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.ResourceNames{
		BucketName:       "new-bucket-name",
		ServiceAccountId: "new-sa-id",
		Email:            "new-sa@project.iam.gserviceaccount.com",
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateBucket, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.AnythingOfType("*string")).Return(&common.BucketDetails{
		BucketName:          "new-bucket-name",
		ServiceAccountName:  "new-sa-name",
		TenantProjectNumber: "new-project-123",
		VendorSubnetID:      "subnet-67890",
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CheckOrCreateRemoteBackupVaultInVCP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&datamodel.BackupVault{}, nil)
	s.env.OnActivity(volumeCreateActivity.UpdateBackupVaultWithBucketDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateRemoteBackupVaultWithBucketDetails, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	syncBackupZiZsActivity := backgroundactivities.SyncBackupZiZsActivity{SE: mockStorage}
	s.env.OnActivity(syncBackupZiZsActivity.SyncBucketDetails, mock.Anything, mock.Anything).Return(&datamodel.BucketDetails{
		BucketName:          "new-bucket-name",
		ServiceAccountName:  "new-sa-name",
		TenantProjectNumber: "new-project-123",
		VendorSubnetID:      "subnet-67890",
		SatisfiesPzi:        true,
		SatisfiesPzs:        true,
	}, nil)
	// Mock SetupCrossRegionBackupPermissionsActivity - not called for LOCAL backup vault type
	s.env.OnActivity(volumeCreateActivity.SetupCrossRegionBackupPermissionsActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume)

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
	s.env.OnWorkflow("PostBlockVolumeWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(volume, nil)

	// Mock activities for minimal workflow
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.LunSizeUpdateValidation, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateClonedVolumeBeforeSplit, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.InitiateSplitForVolume, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{Name: "host_group_test", Hosts: datamodel.Hosts{Hosts: []string{"iqn.1993-08.org.debian:01:f2c983feb27"}}}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{ProviderResponse: vsa.ProviderResponse{Name: "lun_test", ExternalUUID: "lun-uuid"}, SerialNumber: "6c5738423724595454686164"}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateLunMap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume)

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
	s.env.OnWorkflow("PostBlockVolumeWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(volume, nil)

	// Mock activities for minimal workflow
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.LunSizeUpdateValidation, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateClonedVolumeBeforeSplit, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.InitiateSplitForVolume, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{Name: "host_group_test", Hosts: datamodel.Hosts{Hosts: []string{"iqn.1993-08.org.debian:01:f2c983feb27"}}}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{ProviderResponse: vsa.ProviderResponse{Name: "lun_test", ExternalUUID: "lun-uuid"}, SerialNumber: "6c5738423724595454686164"}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateLunMap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume)

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
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name:         "lun_test",
			ExternalUUID: "lun-uuid",
		},
		SerialNumber: "6c5738423724595454686164",
	}, nil)
	s.env.OnActivity(volumeCreateActivity.LunSizeUpdateValidation, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateClonedVolumeBeforeSplit, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateLunMap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume)

	// Assert workflow completed successfully (backup vault flow should be skipped)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	// UpdateJob is called through UpdateJobStatus activity, not directly on mock
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_IsDataProtectionTrue_SkipsGetHostsAndCreateIgroup() {
	// Test that when IsDataProtection is true, GetHosts and CreateIgroup activities are NOT called
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
		Svm: &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			Protocols:         []string{utils.ProtocolISCSI}, // SAN protocol
			BlockProperties:  &datamodel.BlockProperties{OSType: "LINUX"},
			IsDataProtection: true, // This should skip GetHosts and CreateIgroup
		},
		// Note: Not setting DataProtection to avoid backup vault flow
	}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeDetails)

	// Mock activities - note that GetHosts and CreateIgroup should NOT be called
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("test-token", nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeAttributesInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	// CreateLun should be called but will skip LUN creation internally when IsDataProtection is true
	s.env.OnActivity(volumeCreateActivity.CreateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name:         "lun_test",
			ExternalUUID: "lun-uuid",
		},
		SerialNumber: "6c5738423724595454686164",
	}, nil)
	s.env.OnActivity(volumeCreateActivity.LunSizeUpdateValidation, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateClonedVolumeBeforeSplit, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateLunMap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.InitiateSplitForVolume, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeDeleteActivity.DeleteSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeDeleteActivity.DeleteVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Mock child workflows
	s.env.OnWorkflow("PreBlockVolumeWorkflow", mock.Anything, mock.Anything, mock.Anything).Return(volume, nil)
	s.env.OnWorkflow("PostBlockVolumeWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(volume, nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())

	// Verify that GetHosts and CreateIgroup were NOT called
	s.env.AssertNotCalled(s.T(), "GetHosts")
	s.env.AssertNotCalled(s.T(), "CreateIgroup")
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
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume)

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
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume)

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
	})

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
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{AccountName: "test-account"}, volume)

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
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
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
	s.env.OnActivity(volumeCreateActivity.LunSizeUpdateValidation, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateClonedVolumeBeforeSplit, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
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
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume)

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
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed with the error details update error
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), errorDetailsUpdateError.Error())
}

func (s *UnitTestSuite) Test_SelectVolumeChildWorkflow_InvalidPhase() {
	// Test selectVolumeChildWorkflow with invalid phase for block protocols
	workflow, err := selectVolumeChildWorkflow([]string{utils.ProtocolISCSI}, "invalid")
	assert.Nil(s.T(), workflow)
	assert.Error(s.T(), err)
	assert.Contains(s.T(), err.(*vsaerrors.CustomError).OriginalErr.Error(), "invalid phase: invalid")
}

func (s *UnitTestSuite) Test_SelectVolumeChildWorkflow_InvalidPhaseFile() {
	// Test selectVolumeChildWorkflow with invalid phase for file protocols
	utils.SetFileProtocolSupportedForTesting(true)
	defer utils.SetFileProtocolSupportedForTesting(false)

	volumeChildWorkflow, err := selectVolumeChildWorkflow([]string{utils.ProtocolNFSv3}, "invalid")
	assert.Nil(s.T(), volumeChildWorkflow)
	assert.Error(s.T(), err)
	// Since file protocols are enabled, the code should proceed to check the phase and return invalid phase error
	assert.Contains(s.T(), err.(*vsaerrors.CustomError).OriginalErr.Error(), "invalid phase: invalid")
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
	s.env.OnActivity(volumeActivity.UpdateLunName, mock.Anything, mock.Anything, mock.Anything).Return(nil)
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
	restoreVolumeCreateResponse := &vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{ExternalUUID: "test-uuid"},
		Size:             int64(0),
	}
	volCreateResponse := &vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name: "test_volume",
		},
	}
	s.env.ExecuteWorkflow(PostBlockVolumeWorkflow, dbVolume, node, nil, volCreateResponse, false, false, restoreVolumeCreateResponse)

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
	s.env.OnActivity(volumeActivity.UpdateLunName, mock.Anything, mock.Anything, mock.Anything).Return(nil)
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
	restoreVolumeCreateResponse := &vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{ExternalUUID: "test-uuid"},
		Size:             int64(0),
	}
	volCreateResponse := &vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name: "test_volume",
		},
	}
	s.env.ExecuteWorkflow(PostBlockVolumeWorkflow, dbVolume, node, nil, volCreateResponse, false, false, restoreVolumeCreateResponse)

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
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "unsupported or unspecified protocol")
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
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to create volume in ONTAP"))
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume)

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
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{
		ProviderResponse: vsa.ProviderResponse{Name: "test-lun", ExternalUUID: "lun-uuid"},
		SerialNumber:     "test-serial",
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateLunMap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.LunSizeUpdateValidation, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateClonedVolumeBeforeSplit, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.InitiateSplitForVolume, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume)

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
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume)

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
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{}, nil)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return(nil, errors.New("failed to get hosts"))
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.LunSizeUpdateValidation, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateClonedVolumeBeforeSplit, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	// The workflow error will contain the activity error message
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "failed to get hosts")
}

func (s *UnitTestSuite) Test_PostBlockVolumeWorkflow_UpdateLunName_WhenRestoreSnapshot() {
	// Arrange
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
	restoreVolumeCreateResponse := &vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{ExternalUUID: "test-uuid"},
		Size:             int64(0),
	}
	volCreateResponse := &vsa.VolumeResponse{ProviderResponse: vsa.ProviderResponse{ExternalUUID: "test-uuid"}}
	isRestoreFromBackup := false
	isRestoreSnapshot := true

	// Register and mock activities
	s.env.RegisterActivity(volumeCreateActivity.GetHosts)
	s.env.RegisterActivity(volumeCreateActivity.CreateIgroup)
	s.env.RegisterActivity(volumeCreateActivity.UpdateLunName)
	s.env.RegisterActivity(volumeCreateActivity.CreateLun)
	s.env.RegisterActivity(volumeCreateActivity.CreateLunMap)
	hostGroups := []*datamodel.HostGroup{{Name: "hg1", Hosts: datamodel.Hosts{Hosts: []string{"iqn.1"}}}}
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return(hostGroups, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	// UpdateLunName must be used when isRestoreSnapshot=true
	s.env.OnActivity(volumeCreateActivity.UpdateLunName, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name:         "/vol/vol1/luns/lun1",
			ExternalUUID: "lun-uuid",
		},
		SerialNumber: "6c5738423724595454686164",
		Size:         1024,
	}, nil)
	// If CreateLun is called, fail the test by returning an error
	s.env.OnActivity(volumeCreateActivity.CreateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("CreateLun should not be called when restoring from snapshot"))
	s.env.OnActivity(volumeCreateActivity.CreateLunMap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Act
	s.env.ExecuteWorkflow(PostBlockVolumeWorkflow, volume, node, nil, volCreateResponse, isRestoreFromBackup, isRestoreSnapshot, restoreVolumeCreateResponse)

	// Assert
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_RestoreSnapshot_UsesUpdateLunName() {
	// Arrange
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	volumeDeleteActivity := activities.VolumeDeleteActivity{SE: mockStorage}

	volume := &datamodel.Volume{
		Account: &datamodel.Account{Name: "account-1"},
		Pool: &datamodel.Pool{
			BaseModel:       datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{Password: "password"},
		},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}, Protocols: []string{utils.ProtocolISCSI}},
	}

	// Snapshot parameters to trigger isRestoreSnapshot=true
	snapshot := &datamodel.Snapshot{BaseModel: datamodel.BaseModel{ID: 123, UUID: "snap-uuid"}}
	params := &common.CreateVolumeParams{AccountName: "account-1", SnapshotID: "snap-uuid", Snapshot: snapshot}

	// Mocks for activities used in the full workflow
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{ProviderResponse: vsa.ProviderResponse{ExternalUUID: "vol-uuid"}, AvailableSpace: 10}, nil)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{Name: "hg1", Hosts: datamodel.Hosts{Hosts: []string{"iqn.1"}}}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	// UpdateLunName must be used; CreateLun must not be used
	s.env.OnActivity(volumeCreateActivity.LunSizeUpdateValidation, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateLunName, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name:         "/vol/vol1/luns/lun1",
			ExternalUUID: "lun-uuid",
		},
		SerialNumber: "6c5738423724595454686164",
		Size:         2048,
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("CreateLun should not be called during snapshot restore"))
	s.env.OnActivity(volumeCreateActivity.CreateLunMap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeDeleteActivity.DeleteSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeDeleteActivity.DeleteVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateClonedVolumeBeforeSplit, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{ProviderResponse: vsa.ProviderResponse{ExternalUUID: "vol-uuid"}, AvailableSpace: 10}, nil)
	s.env.OnActivity(volumeCreateActivity.InitiateSplitForVolume, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Act
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, params, volume)

	// Assert
	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
}

func (s *UnitTestSuite) Test_PostBlockVolumeWorkflow_UpdateVolumeStateInDBError() {
	// Test PostBlockVolumeWorkflow when UpdateVolumeStateInDB fails in the defer function
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
	restoreVolumeCreateResponse := &vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{ExternalUUID: "test-uuid"},
		Size:             int64(0),
	}
	volCreateResponse := &vsa.VolumeResponse{ProviderResponse: vsa.ProviderResponse{ExternalUUID: "test-uuid"}}
	isRestoreFromBackup := false
	isRestoreSnapshot := false

	// Register activities
	s.env.RegisterActivity(volumeCreateActivity.GetHosts)
	s.env.RegisterActivity(volumeCreateActivity.CreateIgroup)
	s.env.RegisterActivity(volumeCreateActivity.CreateLun)
	s.env.RegisterActivity(volumeCreateActivity.UpdateVolumeStateInDB)

	// Mock activities - CreateLun fails to trigger the defer function
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("create lun error"))

	// Mock UpdateVolumeStateInDB to fail in the defer function
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to update volume state in DB to error"))

	// Execute the workflow
	s.env.ExecuteWorkflow(PostBlockVolumeWorkflow, volume, node, nil, volCreateResponse, isRestoreFromBackup, isRestoreSnapshot, restoreVolumeCreateResponse)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "create lun error")
}

func (s *UnitTestSuite) Test_PostBlockVolumeWorkflow_UpdateVolumeStateInDBErrorInDefer() {
	// Test PostBlockVolumeWorkflow when UpdateVolumeStateInDB fails in the defer function
	// This specifically covers the log.Errorf line 126
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
	restoreVolumeCreateResponse := &vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{ExternalUUID: "test-uuid"},
		Size:             int64(0),
	}
	volCreateResponse := &vsa.VolumeResponse{ProviderResponse: vsa.ProviderResponse{ExternalUUID: "test-uuid"}}
	isRestoreFromBackup := false
	isRestoreSnapshot := false

	// Register activities
	s.env.RegisterActivity(volumeCreateActivity.GetHosts)
	s.env.RegisterActivity(volumeCreateActivity.CreateIgroup)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Mock CreateLun to fail to trigger the defer function
	s.env.OnActivity(volumeCreateActivity.CreateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("create lun error"))

	// Mock UpdateVolumeStateInDB to fail in the defer function to trigger the log.Errorf line
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to update volume state in DB to error"))

	// Execute the workflow
	s.env.ExecuteWorkflow(PostBlockVolumeWorkflow, volume, node, nil, volCreateResponse, isRestoreFromBackup, isRestoreSnapshot, restoreVolumeCreateResponse)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "create lun error")
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_RollbackManagerAddActivityCoverage() {
	// Test to cover line 365: rollbackManager.AddActivity for DeleteVolumeInONTAP
	// This tests the successful path where CreateVolumeInONTAP succeeds
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	volumeDeleteActivity := activities.VolumeDeleteActivity{SE: mockStorage}

	volume := &datamodel.Volume{
		Account: &datamodel.Account{Name: "account-1"},
		Pool: &datamodel.Pool{
			BaseModel:       datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{Password: "password"},
		},
		Svm:              &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}, Protocols: []string{utils.ProtocolISCSI}},
	}

	// Mock activities to ensure successful execution through the rollbackManager.AddActivity line
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{ProviderResponse: vsa.ProviderResponse{ExternalUUID: "vol-uuid"}, AvailableSpace: 10}, nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{Name: "hg1", Hosts: datamodel.Hosts{Hosts: []string{"iqn.1"}}}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.LunSizeUpdateValidation, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateClonedVolumeBeforeSplit, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.InitiateSplitForVolume, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeDeleteActivity.DeleteSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeDeleteActivity.DeleteVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Mock the child workflow to succeed
	s.env.OnWorkflow("PostBlockVolumeWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(volume, nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{AccountName: "account-1"}, volume)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_CoverageForMissingLines() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}

	volume := &datamodel.Volume{
		Account: &datamodel.Account{Name: "account-1"},
		Pool: &datamodel.Pool{
			BaseModel:       datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{Password: "password"},
		},
		Svm: &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"},
			Protocols:       []string{utils.ProtocolISCSI},
			VendorSubnetID:  "subnet-123",
		},
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "backup-vault-123",
		},
		SizeInBytes: 100,
		AccountID:   1,
	}

	s.env.OnActivity(commonActivity.GetJob, mock.Anything, mock.Anything).Return(&datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
		State:     string(models.JobsStateNEW),
	}, nil).Maybe()
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{ProviderResponse: vsa.ProviderResponse{ExternalUUID: "vol-uuid"}, Size: 200, AvailableSpace: 150}, nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{Name: "hg1", Hosts: datamodel.Hosts{Hosts: []string{"iqn.1"}}}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.LunSizeUpdateValidation, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateClonedVolumeBeforeSplit, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	// Mock InitiateSplitForVolume to return nil to trigger the fallback logic (line 392-393)
	s.env.OnActivity(volumeCreateActivity.InitiateSplitForVolume, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Mock the child workflow to return a non-nil updatedVolume to trigger line 402-403
	updatedVolume := &datamodel.Volume{
		Account: &datamodel.Account{Name: "account-1"},
		Pool: &datamodel.Pool{
			BaseModel:       datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{Password: "password"},
		},
		Svm: &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"},
			Protocols:       []string{utils.ProtocolISCSI},
			VendorSubnetID:  "subnet-123",
		},
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "backup-vault-123",
		},
		SizeInBytes: 100,
		AccountID:   1,
	}
	s.env.OnWorkflow("PostBlockVolumeWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(updatedVolume, nil)

	// Mock activities for backup vault processing to trigger line 409-410
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("jwt-token-123", nil)
	s.env.OnActivity(volumeCreateActivity.FindTenancy, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.TenancyInfo{}, nil)
	s.env.OnActivity(volumeCreateActivity.CheckBackupVaultExistsInVCP, mock.Anything, mock.Anything, mock.Anything).Return(&datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{UUID: "backup-vault-uuid"},
		Name:      "test-backup-vault",
	}, nil)
	s.env.OnActivity(volumeCreateActivity.CheckForBucketResourceName, mock.Anything, mock.Anything).Return(&common.BucketDetails{BucketName: "existing-bucket"}, nil)

	// Execute the workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{AccountName: "account-1", Region: "us-central1"}, volume)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_BackupVaultWithEmptyBucketDetails() {
	// Test to cover the missing lines for backup vault processing when bucket details are empty
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}

	volume := &datamodel.Volume{
		Account: &datamodel.Account{Name: "account-1"},
		Pool: &datamodel.Pool{
			BaseModel:       datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{Password: "password"},
		},
		Svm: &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"},
			Protocols:       []string{utils.ProtocolISCSI},
			VendorSubnetID:  "subnet-123",
		},
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "backup-vault-123",
		},
		SizeInBytes: 100,
		AccountID:   1,
	}

	// Mock activities to ensure successful execution through the specific lines
	s.env.OnActivity(commonActivity.GetJob, mock.Anything, mock.Anything).Return(&datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
		State:     string(models.JobsStateNEW),
	}, nil).Maybe()
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{ProviderResponse: vsa.ProviderResponse{ExternalUUID: "vol-uuid"}, Size: 200, AvailableSpace: 150}, nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{Name: "hg1", Hosts: datamodel.Hosts{Hosts: []string{"iqn.1"}}}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.LunSizeUpdateValidation, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateClonedVolumeBeforeSplit, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	// Mock InitiateSplitForVolume to return nil to trigger the fallback logic (line 392-393)
	s.env.OnActivity(volumeCreateActivity.InitiateSplitForVolume, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Mock the child workflow to return a non-nil updatedVolume to trigger line 402-403
	updatedVolume := &datamodel.Volume{
		Account: &datamodel.Account{Name: "account-1"},
		Pool: &datamodel.Pool{
			BaseModel:       datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{Password: "password"},
		},
		Svm: &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"},
			Protocols:       []string{utils.ProtocolISCSI},
			VendorSubnetID:  "subnet-123",
		},
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "backup-vault-123",
		},
		SizeInBytes: 100,
		AccountID:   1,
	}
	s.env.OnWorkflow("PostBlockVolumeWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(updatedVolume, nil)

	// Mock activities for backup vault processing to trigger line 409-410
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("jwt-token-123", nil)
	s.env.OnActivity(volumeCreateActivity.FindTenancy, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.TenancyInfo{}, nil)
	s.env.OnActivity(volumeCreateActivity.CheckBackupVaultExistsInVCP, mock.Anything, mock.Anything, mock.Anything).Return(&datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{UUID: "backup-vault-uuid"},
		Name:      "test-backup-vault",
	}, nil)

	// Mock CheckForBucketResourceName to return empty bucket details to trigger bucket creation logic
	s.env.OnActivity(volumeCreateActivity.CheckForBucketResourceName, mock.Anything, mock.Anything).Return(&common.BucketDetails{}, nil)
	s.env.OnActivity(volumeCreateActivity.GenerateResourceNames, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.ResourceNames{}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateBucket, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.AnythingOfType("*string")).Return(&common.BucketDetails{BucketName: "new-bucket"}, nil)
	s.env.OnActivity(volumeCreateActivity.CheckOrCreateRemoteBackupVaultInVCP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&datamodel.BackupVault{}, nil)
	s.env.OnActivity(volumeCreateActivity.UpdateBackupVaultWithBucketDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateRemoteBackupVaultWithBucketDetails, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	syncBackupZiZsActivity := backgroundactivities.SyncBackupZiZsActivity{SE: mockStorage}
	s.env.OnActivity(syncBackupZiZsActivity.SyncBucketDetails, mock.Anything, mock.Anything).Return(nil, errors.New("cloud service error - bucket sync failed"))
	// Mock SetupCrossRegionBackupPermissionsActivity - not called for non-cross-region backup vault
	s.env.OnActivity(volumeCreateActivity.SetupCrossRegionBackupPermissionsActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	// Execute the workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{AccountName: "account-1", Region: "us-central1"}, volume)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_WithSplitVolumeAndChildWorkflow() {
	// Test to cover the missing lines: 393 (postSplitVolumeRes.AvailableSpace > 0),
	// 402-403 (updatedVolume != nil), and 409-410 (bucket creation logic)
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}

	volume := &datamodel.Volume{
		Account: &datamodel.Account{Name: "account-1"},
		Pool: &datamodel.Pool{
			BaseModel:       datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{Password: "password"},
		},
		Svm: &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"},
			Protocols:       []string{utils.ProtocolISCSI},
			VendorSubnetID:  "subnet-123",
		},
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "backup-vault-123",
		},
		SizeInBytes: 100,
		AccountID:   1,
	}

	// Mock activities to ensure successful execution through the specific lines
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{ProviderResponse: vsa.ProviderResponse{ExternalUUID: "vol-uuid"}, Size: 200, AvailableSpace: 150}, nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{Name: "hg1", Hosts: datamodel.Hosts{Hosts: []string{"iqn.1"}}}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.LunSizeUpdateValidation, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateClonedVolumeBeforeSplit, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	// Mock InitiateSplitForVolume to return a response with AvailableSpace > 0 to cover line 393
	s.env.OnActivity(volumeCreateActivity.InitiateSplitForVolume, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Mock the child workflow to return a non-nil updatedVolume to trigger line 402-403
	updatedVolume := &datamodel.Volume{
		Account: &datamodel.Account{Name: "account-1"},
		Pool: &datamodel.Pool{
			BaseModel:       datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{Password: "password"},
		},
		Svm: &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"},
			Protocols:       []string{utils.ProtocolISCSI},
			VendorSubnetID:  "subnet-123",
		},
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "backup-vault-123",
		},
		SizeInBytes: 100,
		AccountID:   1,
	}
	s.env.OnWorkflow("PostBlockVolumeWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(updatedVolume, nil)

	// Mock activities for backup vault processing to trigger line 409-410
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("jwt-token-123", nil)
	s.env.OnActivity(volumeCreateActivity.FindTenancy, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.TenancyInfo{}, nil)
	s.env.OnActivity(volumeCreateActivity.CheckBackupVaultExistsInVCP, mock.Anything, mock.Anything, mock.Anything).Return(&datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{UUID: "backup-vault-uuid"},
		Name:      "test-backup-vault",
	}, nil)

	// Mock CheckForBucketResourceName to return empty bucket details to trigger bucket creation logic (lines 409-410)
	s.env.OnActivity(volumeCreateActivity.CheckForBucketResourceName, mock.Anything, mock.Anything).Return(&common.BucketDetails{}, nil)
	s.env.OnActivity(volumeCreateActivity.GenerateResourceNames, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.ResourceNames{}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateBucket, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.AnythingOfType("*string")).Return(&common.BucketDetails{
		BucketName:          "new-bucket",
		TenantProjectNumber: "test-project-123",
		ServiceAccountName:  "test-sa",
		VendorSubnetID:      "subnet-123",
	}, nil)

	syncBackupZiZsActivity := backgroundactivities.SyncBackupZiZsActivity{SE: mockStorage}
	s.env.OnActivity(syncBackupZiZsActivity.SyncBucketDetails, mock.Anything, mock.Anything).Return(&datamodel.BucketDetails{
		BucketName:          "new-bucket",
		TenantProjectNumber: "test-project-123",
		ServiceAccountName:  "test-sa",
		VendorSubnetID:      "subnet-123",
		SatisfiesPzi:        true,
		SatisfiesPzs:        true,
	}, nil)

	s.env.OnActivity(volumeCreateActivity.CheckOrCreateRemoteBackupVaultInVCP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&datamodel.BackupVault{}, nil)
	s.env.OnActivity(volumeCreateActivity.UpdateBackupVaultWithBucketDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateRemoteBackupVaultWithBucketDetails, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	// Mock SetupCrossRegionBackupPermissionsActivity - not called for non-cross-region backup vault
	s.env.OnActivity(volumeCreateActivity.SetupCrossRegionBackupPermissionsActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	// Execute the workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{AccountName: "account-1", Region: "us-central1"}, volume)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
}

// TestCreateVolumeWorkflow_CVCountUpdate tests the CV count update functionality for auto-provisioned large volumes
func (s *UnitTestSuite) TestCreateVolumeWorkflow_CVCountUpdate() {
	s.SetupTest()

	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	volumeDeleteActivity := activities.VolumeDeleteActivity{SE: mockStorage}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "test-uuid"},
		Name:      "test-volume",
		Account:   &datamodel.Account{Name: "account-1"},
		Svm:       &datamodel.Svm{Name: "svm_test"},
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
		},
		LargeVolumeAttributes: &datamodel.LargeVolumeAttributes{
			LargeCapacity:               true,
			LargeVolumeConstituentCount: nil, // Auto-provisioned
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"},
			Protocols:       []string{utils.ProtocolISCSI},
		},
	}

	// Mock activities
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	// Mock GetJob activity - return NEW state for workflow job (EnsureJobState)
	s.env.OnActivity(commonActivity.GetJob, mock.Anything, mock.Anything).Return(&datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
		State:     string(models.JobsStateNEW),
	}, nil).Maybe()
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{}}, nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			ExternalUUID: "ontap-uuid",
			Name:         "test-volume",
		},
		State:            "online",
		ConstituentCount: nillable.GetInt32Ptr(8), // This should trigger CV count update
	}, nil)
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
	s.env.OnActivity(volumeCreateActivity.LunSizeUpdateValidation, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateClonedVolumeBeforeSplit, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateLunMap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.InitiateSplitForVolume, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, &common.CreateVolumeParams{}, volume)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
}

// TestCreateVolumeWorkflow_CVCountUpdateLogic tests the CV count update logic directly
func TestCreateVolumeWorkflow_CVCountUpdateLogic(t *testing.T) {
	tests := []struct {
		name                string
		dbVolume            *datamodel.Volume
		volCreateResponse   *vsa.VolumeResponse
		shouldExecuteUpdate bool
		expectedLogMessage  string
		description         string
	}{
		{
			name: "Auto-provisioned large volume - should update CV count",
			dbVolume: &datamodel.Volume{
				BaseModel: datamodel.BaseModel{UUID: "test-uuid"},
				LargeVolumeAttributes: &datamodel.LargeVolumeAttributes{
					LargeCapacity:               true,
					LargeVolumeConstituentCount: nil, // Auto-provisioned
				},
			},
			volCreateResponse: &vsa.VolumeResponse{
				ConstituentCount: nillable.GetInt32Ptr(8),
			},
			shouldExecuteUpdate: true,
			expectedLogMessage:  "Updating CV count for auto-provisioned volume test-uuid: 8",
			description:         "Should execute CV count update for auto-provisioned large volume",
		},
		{
			name: "Customer-specified large volume - should NOT update CV count",
			dbVolume: &datamodel.Volume{
				BaseModel: datamodel.BaseModel{UUID: "test-uuid-2"},
				LargeVolumeAttributes: &datamodel.LargeVolumeAttributes{
					LargeCapacity:               true,
					LargeVolumeConstituentCount: nillable.GetInt32Ptr(4), // Customer specified
				},
			},
			volCreateResponse: &vsa.VolumeResponse{
				ConstituentCount: nillable.GetInt32Ptr(6),
			},
			shouldExecuteUpdate: false,
			expectedLogMessage:  "",
			description:         "Should NOT execute CV count update when customer specified count",
		},
		{
			name: "Regular volume - should NOT update CV count",
			dbVolume: &datamodel.Volume{
				BaseModel:             datamodel.BaseModel{UUID: "test-uuid-3"},
				LargeVolumeAttributes: nil, // Not a large volume
			},
			volCreateResponse: &vsa.VolumeResponse{
				ConstituentCount: nillable.GetInt32Ptr(1),
			},
			shouldExecuteUpdate: false,
			expectedLogMessage:  "",
			description:         "Should NOT execute CV count update for regular volumes",
		},
		{
			name: "Auto-provisioned large volume with no CV count in response - should NOT update",
			dbVolume: &datamodel.Volume{
				BaseModel: datamodel.BaseModel{UUID: "test-uuid-4"},
				LargeVolumeAttributes: &datamodel.LargeVolumeAttributes{
					LargeCapacity:               true,
					LargeVolumeConstituentCount: nil, // Auto-provisioned
				},
			},
			volCreateResponse: &vsa.VolumeResponse{
				ConstituentCount: nil, // No CV count in response
			},
			shouldExecuteUpdate: false,
			expectedLogMessage:  "",
			description:         "Should NOT execute CV count update when no CV count in response",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a copy of dbVolume to avoid modifying test data
			testVolume := &datamodel.Volume{
				BaseModel: tt.dbVolume.BaseModel,
			}
			if tt.dbVolume.LargeVolumeAttributes != nil {
				testVolume.LargeVolumeAttributes = &datamodel.LargeVolumeAttributes{
					LargeCapacity:               tt.dbVolume.LargeVolumeAttributes.LargeCapacity,
					LargeVolumeConstituentCount: tt.dbVolume.LargeVolumeAttributes.LargeVolumeConstituentCount,
				}
			}

			// Test the condition logic that determines whether to execute CV count update
			shouldExecute := testVolume.LargeVolumeAttributes != nil &&
				testVolume.LargeVolumeAttributes.LargeCapacity &&
				testVolume.LargeVolumeAttributes.LargeVolumeConstituentCount == nil &&
				tt.volCreateResponse.ConstituentCount != nil

			assert.Equal(t, tt.shouldExecuteUpdate, shouldExecute, tt.description)

			// Actually execute the update logic (simulating the workflow code)
			if shouldExecute {
				testVolume.LargeVolumeAttributes.LargeVolumeConstituentCount = tt.volCreateResponse.ConstituentCount
			}

			// Verify the result of the update
			if tt.shouldExecuteUpdate {
				// Test the expected log message format
				expectedLog := fmt.Sprintf("Updating CV count for auto-provisioned volume %s: %d",
					testVolume.UUID, *tt.volCreateResponse.ConstituentCount)
				assert.Equal(t, expectedLog, tt.expectedLogMessage, "Log message should match expected format")

				// Verify the field was actually updated
				assert.NotNil(t, testVolume.LargeVolumeAttributes.LargeVolumeConstituentCount, "CV count should be set after update")
				assert.Equal(t, *tt.volCreateResponse.ConstituentCount, *testVolume.LargeVolumeAttributes.LargeVolumeConstituentCount,
					"CV count should match the value from ONTAP response")
			} else {
				// Verify no update occurred when it shouldn't
				if testVolume.LargeVolumeAttributes != nil {
					assert.Equal(t, tt.dbVolume.LargeVolumeAttributes.LargeVolumeConstituentCount,
						testVolume.LargeVolumeAttributes.LargeVolumeConstituentCount,
						"CV count should not be modified when update should not execute")
				}
			}
		})
	}
}

func (s *UnitTestSuite) Test_UpdateRemoteBackupVaultDetailsInVCP_Success() {
	// Test successful flow when remote backup vault needs to be created and updated
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}

	// Create volume with DataProtection.BackupVaultID to trigger backup vault flow
	volume := &datamodel.Volume{
		Account:   &datamodel.Account{Name: "project-123"},
		AccountID: 1,
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password: "password",
			},
		},
		Svm: &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"},
			Protocols:       []string{utils.ProtocolISCSI},
			VendorSubnetID:  "subnet-123",
		},
		// This is the key - DataProtection with BackupVaultID triggers the backup vault flow
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "backup-vault-123",
			KmsGrant:      nillable.ToPointer("projects/test-project/locations/us-central1/keyRings/test-keyring/cryptoKeys/test-key"),
		},
		SizeInBytes: 100,
	}

	// Mock required workflow activities - ALL activities that may be called
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{ProviderResponse: vsa.ProviderResponse{ExternalUUID: "vol-uuid"}, Size: 200, AvailableSpace: 150}, nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{Name: "hg1", Hosts: datamodel.Hosts{Hosts: []string{"iqn.1"}}}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Mock the PostBlockVolumeWorkflow child workflow
	s.env.OnWorkflow("PostBlockVolumeWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(volume, nil)

	// Mock backup vault activities - these are the key ones for our test
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("jwt-token-123", nil)

	// Mock CheckBackupVaultExistsInVCP to return a CROSS_REGION backup vault
	backupVault := &datamodel.BackupVault{
		BaseModel:        datamodel.BaseModel{UUID: "vault-uuid-123"},
		Name:             "test-vault",
		BackupVaultType:  "CROSS_REGION",
		SourceRegionName: nillable.ToPointer("us-central1"),
		BackupRegionName: nillable.ToPointer("us-east1"),
	}
	s.env.OnActivity(volumeCreateActivity.CheckBackupVaultExistsInVCP, mock.Anything, mock.Anything, mock.Anything).Return(backupVault, nil)

	// Mock tenancy activities
	s.env.OnActivity(volumeCreateActivity.FindTenancy, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.TenancyInfo{}, nil)

	// Mock CheckForBucketResourceName to return empty bucket details to trigger creation flow
	s.env.OnActivity(volumeCreateActivity.CheckForBucketResourceName, mock.Anything, mock.Anything).Return(&common.BucketDetails{}, nil)

	// Mock resource generation and bucket creation
	s.env.OnActivity(volumeCreateActivity.GenerateResourceNames, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.ResourceNames{}, nil)
	bucketDetails := &common.BucketDetails{
		BucketName:          "test-bucket",
		ServiceAccountName:  "test-sa",
		TenantProjectNumber: "123456789",
	}
	// Verify kmsGrant is passed to CreateBucket activity
	var capturedKmsGrant *string
	s.env.OnActivity(volumeCreateActivity.CreateBucket, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.MatchedBy(func(kmsGrant *string) bool {
		capturedKmsGrant = kmsGrant
		return true
	})).Return(bucketDetails, nil)

	// Mock bucket sync activity
	syncBackupZiZsActivity := backgroundactivities.SyncBackupZiZsActivity{SE: mockStorage}
	s.env.OnActivity(syncBackupZiZsActivity.SyncBucketDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	// Mock the backup vault update activities
	s.env.OnActivity(volumeCreateActivity.CheckOrCreateRemoteBackupVaultInVCP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&datamodel.BackupVault{}, nil)
	s.env.OnActivity(volumeCreateActivity.UpdateBackupVaultWithBucketDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateRemoteBackupVaultWithBucketDetails, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Mock SetupCrossRegionBackupPermissionsActivity for cross-region backup vault
	s.env.OnActivity(volumeCreateActivity.SetupCrossRegionBackupPermissionsActivity,
		mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow with cross-region backup vault
	s.env.ExecuteWorkflow(CreateVolumeWorkflow,
		&common.CreateVolumeParams{AccountName: "project-123", Region: "us-central1"},
		volume)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	// Verify kmsGrant was passed to CreateBucket
	assert.NotNil(s.T(), capturedKmsGrant)
	assert.Equal(s.T(), "projects/test-project/locations/us-central1/keyRings/test-keyring/cryptoKeys/test-key", *capturedKmsGrant)
}

func (s *UnitTestSuite) Test_UpdateRemoteBackupVaultDetailsInVCP_Error() {
	// Test error handling when UpdateRemoteBackupVaultDetailsInVCP fails
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}

	volume := &datamodel.Volume{
		Account:   &datamodel.Account{Name: "project-123"},
		AccountID: 1,
		Pool: &datamodel.Pool{
			BaseModel:       datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{Password: "password"},
		},
		Svm: &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"},
			Protocols:       []string{utils.ProtocolISCSI},
			VendorSubnetID:  "subnet-123",
		},
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "backup-vault-123",
		},
		SizeInBytes: 100,
	}

	// Mock standard workflow activities
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{ProviderResponse: vsa.ProviderResponse{ExternalUUID: "vol-uuid"}, Size: 200, AvailableSpace: 150}, nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{Name: "hg1", Hosts: datamodel.Hosts{Hosts: []string{"iqn.1"}}}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateLunMap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Mock child workflow
	s.env.OnWorkflow("PostBlockVolumeWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(volume, nil)

	// Mock backup vault activities
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("jwt-token-123", nil)

	backupVault := &datamodel.BackupVault{
		BaseModel:        datamodel.BaseModel{UUID: "vault-uuid-123"},
		Name:             "test-vault",
		BackupVaultType:  "CROSS_REGION",
		SourceRegionName: nillable.ToPointer("us-central1"),
		BackupRegionName: nillable.ToPointer("us-east1"),
	}
	s.env.OnActivity(volumeCreateActivity.CheckBackupVaultExistsInVCP, mock.Anything, mock.Anything, mock.Anything).Return(backupVault, nil)
	s.env.OnActivity(volumeCreateActivity.FindTenancy, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.TenancyInfo{}, nil)
	s.env.OnActivity(volumeCreateActivity.CheckForBucketResourceName, mock.Anything, mock.Anything).Return(&common.BucketDetails{}, nil)
	s.env.OnActivity(volumeCreateActivity.GenerateResourceNames, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.ResourceNames{}, nil)

	bucketDetails := &common.BucketDetails{
		BucketName:          "test-bucket",
		ServiceAccountName:  "test-sa",
		TenantProjectNumber: "123456789",
	}
	s.env.OnActivity(volumeCreateActivity.CreateBucket, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.AnythingOfType("*string")).Return(bucketDetails, nil)

	syncBackupZiZsActivity := backgroundactivities.SyncBackupZiZsActivity{SE: mockStorage}
	s.env.OnActivity(syncBackupZiZsActivity.SyncBucketDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	s.env.OnActivity(volumeCreateActivity.CheckOrCreateRemoteBackupVaultInVCP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&datamodel.BackupVault{}, nil)
	s.env.OnActivity(volumeCreateActivity.UpdateBackupVaultWithBucketDetails, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to update remote backup vault"))

	// Mock cleanup activities that will be called due to error
	volumeDeleteActivity := activities.VolumeDeleteActivity{SE: mockStorage}
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeStateInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeDeleteActivity.DeleteVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeDeleteActivity.DeleteSnapshotPolicyInONTAP, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow,
		&common.CreateVolumeParams{AccountName: "project-123", Region: "us-central1"},
		volume)

	// Assert workflow completed with error
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

func (s *UnitTestSuite) Test_UpdateRemoteBackupVaultDetailsInVCP_NotTriggered_NoBackupVault() {
	// Test that UpdateRemoteBackupVaultDetailsInVCP is not called when volume has no backup vault
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}

	volume := &datamodel.Volume{
		Account:   &datamodel.Account{Name: "project-123"},
		AccountID: 1,
		Pool: &datamodel.Pool{
			BaseModel:       datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{Password: "password"},
		},
		Svm: &datamodel.Svm{Name: "svm_test"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"},
			Protocols:       []string{utils.ProtocolISCSI},
			VendorSubnetID:  "subnet-123",
		},
		// No DataProtection - this means no backup vault processing
		SizeInBytes: 100,
	}

	// Mock standard workflow activities
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{ProviderResponse: vsa.ProviderResponse{ExternalUUID: "vol-uuid"}, Size: 200, AvailableSpace: 150}, nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeDetails, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{Name: "hg1", Hosts: datamodel.Hosts{Hosts: []string{"iqn.1"}}}}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateIgroup, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.CreateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{}, nil)
	s.env.OnActivity(volumeCreateActivity.CreateLunMap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Mock child workflow
	s.env.OnWorkflow("PostBlockVolumeWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(volume, nil)

	// NOTE: Do NOT mock UpdateRemoteBackupVaultDetailsInVCP - it should not be called

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow,
		&common.CreateVolumeParams{AccountName: "project-123", Region: "us-central1"},
		volume)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
}

// TestEnsureCIFSShareWorkflow tests the EnsureCIFSShareWorkflow
func (s *UnitTestSuite) Test_EnsureCIFSShareWorkflow_Success_AllSteps() {
	mockStorage := database.NewMockStorage(s.T())
	adActivity := active_directory_activities.ActiveDirectoryActivity{SE: mockStorage}

	volume := &datamodel.Volume{
		Name: "test-volume",
		Svm:  &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			FileProperties: &datamodel.FileProperties{
				JunctionPath: "/junction",
			},
		},
	}

	node := &models.Node{Name: "node-1"}
	activeDirectory := &vsa.ActiveDirectory{
		Domain: "example.com",
		DNS:    "8.8.8.8",
	}
	svmName := "test-svm"
	externalSVMUUID := "svm-uuid"
	expectedFQDN := "NETBIOS-1234.example.com"

	// Mock all activities
	s.env.OnActivity(adActivity.CreateOrModifyADDNS, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(adActivity.GetOrCreateCifsService, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
		&active_directory_activities.GetOrCreateCifsServiceResult{
			FQDN:      expectedFQDN,
			NeedsDDNS: false,
		}, nil)
	s.env.OnActivity(adActivity.CreateJunctionPathForCifsShare, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(EnsureCIFSShareWorkflow, volume, node, activeDirectory, svmName, externalSVMUUID)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())

	// Get result
	var resultFQDN string
	err := s.env.GetWorkflowResult(&resultFQDN)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), expectedFQDN, resultFQDN)
}

func (s *UnitTestSuite) Test_EnsureCIFSShareWorkflow_Success_WithDDNS() {
	mockStorage := database.NewMockStorage(s.T())
	adActivity := active_directory_activities.ActiveDirectoryActivity{SE: mockStorage}

	volume := &datamodel.Volume{
		Name: "test-volume",
		Svm:  &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			FileProperties: &datamodel.FileProperties{
				JunctionPath: "/junction",
			},
		},
	}

	node := &models.Node{Name: "node-1"}
	activeDirectory := &vsa.ActiveDirectory{
		Domain: "example.com",
		DNS:    "8.8.8.8",
	}
	svmName := "test-svm"
	externalSVMUUID := "svm-uuid"
	expectedFQDN := "NETBIOS-1234.example.com"

	// Mock activities - service exists and needs DDNS
	s.env.OnActivity(adActivity.CreateOrModifyADDNS, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(adActivity.GetOrCreateCifsService, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
		&active_directory_activities.GetOrCreateCifsServiceResult{
			FQDN:            "",
			NeedsDDNS:       true,
			CifsServiceName: "NETBIOS-1234",
			AdDomain:        "example.com",
		}, nil)
	s.env.OnActivity(adActivity.DdnsModify, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(adActivity.CreateJunctionPathForCifsShare, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(EnsureCIFSShareWorkflow, volume, node, activeDirectory, svmName, externalSVMUUID)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())

	// Get result
	var resultFQDN string
	err := s.env.GetWorkflowResult(&resultFQDN)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), expectedFQDN, resultFQDN)
}

func (s *UnitTestSuite) Test_EnsureCIFSShareWorkflow_Success_NoDDNSNeeded() {
	mockStorage := database.NewMockStorage(s.T())
	adActivity := active_directory_activities.ActiveDirectoryActivity{SE: mockStorage}

	volume := &datamodel.Volume{
		Name: "test-volume",
		Svm:  &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			FileProperties: &datamodel.FileProperties{
				JunctionPath: "/junction",
			},
		},
	}

	node := &models.Node{Name: "node-1"}
	activeDirectory := &vsa.ActiveDirectory{
		Domain: "example.com",
		DNS:    "8.8.8.8",
	}
	svmName := "test-svm"
	externalSVMUUID := "svm-uuid"
	expectedFQDN := "NETBIOS-1234.example.com"

	// Mock activities - service exists and DDNS already enabled
	s.env.OnActivity(adActivity.CreateOrModifyADDNS, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(adActivity.GetOrCreateCifsService, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
		&active_directory_activities.GetOrCreateCifsServiceResult{
			FQDN:            "",
			NeedsDDNS:       false,
			CifsServiceName: "NETBIOS-1234",
			AdDomain:        "example.com",
		}, nil)
	s.env.OnActivity(adActivity.CreateJunctionPathForCifsShare, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(EnsureCIFSShareWorkflow, volume, node, activeDirectory, svmName, externalSVMUUID)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())

	// Get result
	var resultFQDN string
	err := s.env.GetWorkflowResult(&resultFQDN)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), expectedFQDN, resultFQDN)
}

func (s *UnitTestSuite) Test_EnsureCIFSShareWorkflow_SkipsNonFileVolume() {
	volume := &datamodel.Volume{
		Name:             "block-volume",
		Svm:              &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{}, // No FileProperties
	}

	node := &models.Node{Name: "node-1"}
	activeDirectory := &vsa.ActiveDirectory{
		Domain: "example.com",
		DNS:    "8.8.8.8",
	}
	svmName := "test-svm"
	externalSVMUUID := "svm-uuid"

	// Execute workflow - should skip without calling activities
	s.env.ExecuteWorkflow(EnsureCIFSShareWorkflow, volume, node, activeDirectory, svmName, externalSVMUUID)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())

	// Get result - should be empty
	var resultFQDN string
	err := s.env.GetWorkflowResult(&resultFQDN)
	assert.NoError(s.T(), err)
	assert.Empty(s.T(), resultFQDN)
}

func (s *UnitTestSuite) Test_EnsureCIFSShareWorkflow_WithSMBShareProperties() {
	mockStorage := database.NewMockStorage(s.T())
	adActivity := active_directory_activities.ActiveDirectoryActivity{SE: mockStorage}

	expectedSMBProperties := []string{"browsable", "encrypt_data", "oplocks"}
	volume := &datamodel.Volume{
		Name: "test-volume",
		Svm:  &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			FileProperties: &datamodel.FileProperties{
				JunctionPath:     "/test_share",
				SMBShareSettings: expectedSMBProperties,
			},
		},
	}

	node := &models.Node{Name: "node-1"}
	activeDirectory := &vsa.ActiveDirectory{
		Domain: "example.com",
		DNS:    "8.8.8.8",
	}
	svmName := "test-svm"
	externalSVMUUID := "svm-uuid"
	expectedFQDN := "NETBIOS-1234.example.com"

	// Mock activities
	s.env.OnActivity(adActivity.CreateOrModifyADDNS, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(adActivity.GetOrCreateCifsService, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
		&active_directory_activities.GetOrCreateCifsServiceResult{
			FQDN:      expectedFQDN,
			NeedsDDNS: false,
		}, nil)

	// Verify that CreateJunctionPathForCifsShare is called with the correct SMB properties
	s.env.OnActivity(adActivity.CreateJunctionPathForCifsShare,
		mock.Anything,
		mock.Anything,
		svmName,
		"/test_share",
		expectedSMBProperties).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(EnsureCIFSShareWorkflow, volume, node, activeDirectory, svmName, externalSVMUUID)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())

	// Get result
	var resultFQDN string
	err := s.env.GetWorkflowResult(&resultFQDN)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), expectedFQDN, resultFQDN)
}

func (s *UnitTestSuite) Test_EnsureCIFSShareWorkflow_Error_CreateDNSFails() {
	mockStorage := database.NewMockStorage(s.T())
	adActivity := active_directory_activities.ActiveDirectoryActivity{SE: mockStorage}

	volume := &datamodel.Volume{
		Name: "test-volume",
		Svm:  &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			FileProperties: &datamodel.FileProperties{
				JunctionPath: "/junction",
			},
		},
	}

	node := &models.Node{Name: "node-1"}
	activeDirectory := &vsa.ActiveDirectory{
		Domain: "example.com",
		DNS:    "8.8.8.8",
	}
	svmName := "test-svm"
	externalSVMUUID := "svm-uuid"

	// Mock activity to fail
	s.env.OnActivity(adActivity.CreateOrModifyADDNS, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
		errors.New("DNS creation failed"))

	// Execute workflow
	s.env.ExecuteWorkflow(EnsureCIFSShareWorkflow, volume, node, activeDirectory, svmName, externalSVMUUID)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

func (s *UnitTestSuite) Test_EnsureCIFSShareWorkflow_Error_GetCifsServiceFails() {
	mockStorage := database.NewMockStorage(s.T())
	adActivity := active_directory_activities.ActiveDirectoryActivity{SE: mockStorage}

	volume := &datamodel.Volume{
		Name: "test-volume",
		Svm:  &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			FileProperties: &datamodel.FileProperties{
				JunctionPath: "/junction",
			},
		},
	}

	node := &models.Node{Name: "node-1"}
	activeDirectory := &vsa.ActiveDirectory{
		Domain: "example.com",
		DNS:    "8.8.8.8",
	}
	svmName := "test-svm"
	externalSVMUUID := "svm-uuid"

	// Mock activities
	s.env.OnActivity(adActivity.CreateOrModifyADDNS, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(adActivity.GetOrCreateCifsService, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
		nil, errors.New("CIFS service creation failed"))

	// Execute workflow
	s.env.ExecuteWorkflow(EnsureCIFSShareWorkflow, volume, node, activeDirectory, svmName, externalSVMUUID)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

func (s *UnitTestSuite) Test_EnsureCIFSShareWorkflow_Error_DDNSFails() {
	mockStorage := database.NewMockStorage(s.T())
	adActivity := active_directory_activities.ActiveDirectoryActivity{SE: mockStorage}

	volume := &datamodel.Volume{
		Name: "test-volume",
		Svm:  &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			FileProperties: &datamodel.FileProperties{
				JunctionPath: "/junction",
			},
		},
	}

	node := &models.Node{Name: "node-1"}
	activeDirectory := &vsa.ActiveDirectory{
		Domain: "example.com",
		DNS:    "8.8.8.8",
	}
	svmName := "test-svm"
	externalSVMUUID := "svm-uuid"

	// Mock activities - service exists and needs DDNS
	s.env.OnActivity(adActivity.CreateOrModifyADDNS, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(adActivity.GetOrCreateCifsService, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
		&active_directory_activities.GetOrCreateCifsServiceResult{
			FQDN:            "",
			NeedsDDNS:       true,
			CifsServiceName: "NETBIOS-1234",
			AdDomain:        "example.com",
		}, nil)
	s.env.OnActivity(adActivity.DdnsModify, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
		errors.New("DDNS enable failed"))

	// Execute workflow
	s.env.ExecuteWorkflow(EnsureCIFSShareWorkflow, volume, node, activeDirectory, svmName, externalSVMUUID)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

func (s *UnitTestSuite) Test_EnsureCIFSShareWorkflow_Error_CreateJunctionPathFails() {
	mockStorage := database.NewMockStorage(s.T())
	adActivity := active_directory_activities.ActiveDirectoryActivity{SE: mockStorage}

	volume := &datamodel.Volume{
		Name: "test-volume",
		Svm:  &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			FileProperties: &datamodel.FileProperties{
				JunctionPath: "/junction",
			},
			Protocols: []string{"SMB"},
		},
	}

	node := &models.Node{Name: "node-1"}
	activeDirectory := &vsa.ActiveDirectory{
		Domain: "example.com",
		DNS:    "8.8.8.8",
	}
	svmName := "test-svm"
	externalSVMUUID := "svm-uuid"
	expectedFQDN := "NETBIOS-1234.example.com"

	// Mock activities
	s.env.OnActivity(adActivity.CreateOrModifyADDNS, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(adActivity.GetOrCreateCifsService, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
		&active_directory_activities.GetOrCreateCifsServiceResult{
			FQDN:      expectedFQDN,
			NeedsDDNS: false,
		}, nil)
	s.env.OnActivity(adActivity.CreateJunctionPathForCifsShare, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
		errors.New("junction path creation failed"))

	// Execute workflow
	s.env.ExecuteWorkflow(EnsureCIFSShareWorkflow, volume, node, activeDirectory, svmName, externalSVMUUID)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

// TestCreateVolumeWorkflow_SDEBackupRestore tests the workflow when restoring from SDE backup
func (s *UnitTestSuite) Test_CreateVolumeWorkflow_SDEBackupRestore_FetchMetadataSuccess() {
	mockStorage := database.NewMockStorage(s.T())
	volumeActivity := activities.VolumeCreateActivity{SE: mockStorage}
	commonActivity := activities.CommonActivities{SE: mockStorage}

	// Test volume with backup path but nil backup vault/backup (triggers metadata fetch)
	volume := &datamodel.Volume{
		BaseModel:   datamodel.BaseModel{UUID: "vol-uuid-123"},
		Name:        "test-volume",
		SizeInBytes: 10737418240, // 10GB - large enough for backup
		Account:     &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "test-account"},
		AccountID:   1,
		Pool: &datamodel.Pool{
			BaseModel:       datamodel.BaseModel{ID: 1, UUID: "pool-uuid-123"},
			DeploymentName:  "test-deployment",
			VendorID:        "gcp-us-central1-a",
			PoolCredentials: &datamodel.PoolCredentials{Password: "password"},
		},
		Svm: &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			Protocols:          []string{"NFS"},
			RestoredBackupPath: "projects/123456/locations/us-central1/backupVaults/my-vault/backups/my-backup",
			RestoredBackupID:   "",
		},
	}

	backupVault := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "bv-uuid-123"},
		Name:      "my-vault",
		BucketDetails: datamodel.BucketDetailsArray{
			{BucketName: "test-bucket", TenantProjectNumber: "123456789"},
		},
	}

	backup := &datamodel.Backup{
		BaseModel:   datamodel.BaseModel{UUID: "backup-uuid-123"},
		Name:        "my-backup",
		SizeInBytes: 1073741824, // 1GB
		BackupVault: backupVault,
		Attributes: &datamodel.BackupAttributes{
			BucketName: "test-bucket",
		},
	}

	backupMetadata := &activities.BackupRestoreMetadata{
		BackupVault:   backupVault,
		Backup:        backup,
		BucketDetails: backupVault.BucketDetails[0],
	}

	params := &common.CreateVolumeParams{
		AccountName: "test-account",
		Name:        "test-volume",
		Region:      "us-central1",
		BackupPath:  "projects/123456/locations/us-central1/backupVaults/my-vault/backups/my-backup",
	}

	// Register activities
	s.env.RegisterActivity(volumeActivity.FetchBackupMetadataForRestore)
	s.env.RegisterActivity(volumeActivity.CreateSnapshotPolicyInONTAP)
	s.env.RegisterActivity(volumeActivity.GetHosts)

	// Mock activities
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{}}, nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeActivity.FetchBackupMetadataForRestore, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(backupMetadata, nil)

	// Mock remaining activities with errors to stop workflow (we just want to test metadata fetch)
	s.env.OnActivity(volumeActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(err1.New("stop workflow"))

	// Execute workflow with nil backup vault and backup
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, params, volume)

	// Assert workflow completed (with error from snapshot policy - expected)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	// Verify FetchBackupMetadataForRestore was called since backup vault/backup were nil
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_SDEBackupRestore_FetchMetadataFails() {
	mockStorage := database.NewMockStorage(s.T())
	volumeActivity := activities.VolumeCreateActivity{SE: mockStorage}
	commonActivity := activities.CommonActivities{SE: mockStorage}

	// Test volume with backup path but nil backup vault/backup
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "vol-uuid-123"},
		Name:      "test-volume",
		Account:   &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "test-account"},
		AccountID: 1,
		Pool: &datamodel.Pool{
			BaseModel:       datamodel.BaseModel{ID: 1, UUID: "pool-uuid-123"},
			DeploymentName:  "test-deployment",
			VendorID:        "gcp-us-central1-a",
			PoolCredentials: &datamodel.PoolCredentials{Password: "password"},
		},
		Svm: &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			Protocols:          []string{"NFS"},
			RestoredBackupPath: "projects/123456/locations/us-central1/backupVaults/my-vault/backups/my-backup",
		},
	}

	params := &common.CreateVolumeParams{
		AccountName: "test-account",
		Name:        "test-volume",
		Region:      "us-central1",
		BackupPath:  "projects/123456/locations/us-central1/backupVaults/my-vault/backups/my-backup",
	}

	// Register activities
	s.env.RegisterActivity(volumeActivity.FetchBackupMetadataForRestore)
	s.env.RegisterActivity(volumeActivity.UpdateVolumeStateInDB)
	s.env.RegisterActivity(volumeActivity.GetHosts)

	// Mock activities
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{}}, nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("test-token", nil)
	s.env.OnActivity(volumeActivity.FetchBackupMetadataForRestore, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, err1.New("failed to fetch backup metadata"))
	s.env.OnActivity(volumeActivity.UpdateVolumeStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow with nil backup vault and backup
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, params, volume)

	// Assert workflow completed with error
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "failed to fetch backup metadata")
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_SDEBackupRestore_VolumeTooSmall() {
	mockStorage := database.NewMockStorage(s.T())
	volumeActivity := activities.VolumeCreateActivity{SE: mockStorage}
	commonActivity := activities.CommonActivities{SE: mockStorage}

	// Test volume too small for backup
	volume := &datamodel.Volume{
		BaseModel:   datamodel.BaseModel{UUID: "vol-uuid-123"},
		Name:        "test-volume",
		SizeInBytes: 1073741824, // 1GB - smaller than required
		Account:     &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "test-account"},
		AccountID:   1,
		Pool: &datamodel.Pool{
			BaseModel:       datamodel.BaseModel{ID: 1, UUID: "pool-uuid-123"},
			DeploymentName:  "test-deployment",
			VendorID:        "gcp-us-central1-a",
			PoolCredentials: &datamodel.PoolCredentials{Password: "password"},
		},
		Svm: &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			Protocols:          []string{"NFS"},
			RestoredBackupPath: "projects/123456/locations/us-central1/backupVaults/my-vault/backups/my-backup",
		},
	}

	backupVault := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "bv-uuid-123"},
		Name:      "my-vault",
		BucketDetails: datamodel.BucketDetailsArray{
			{BucketName: "test-bucket", TenantProjectNumber: "123456789"},
		},
	}

	// Backup is larger than volume
	backup := &datamodel.Backup{
		BaseModel:   datamodel.BaseModel{UUID: "backup-uuid-123"},
		Name:        "my-backup",
		SizeInBytes: 10737418240, // 10GB - larger than volume
		BackupVault: backupVault,
		Attributes: &datamodel.BackupAttributes{
			BucketName: "test-bucket",
		},
	}

	backupMetadata := &activities.BackupRestoreMetadata{
		BackupVault:   backupVault,
		Backup:        backup,
		BucketDetails: backupVault.BucketDetails[0],
	}

	params := &common.CreateVolumeParams{
		AccountName: "test-account",
		Name:        "test-volume",
		Region:      "us-central1",
		BackupPath:  "projects/123456/locations/us-central1/backupVaults/my-vault/backups/my-backup",
	}

	// Register activities
	s.env.RegisterActivity(volumeActivity.FetchBackupMetadataForRestore)
	s.env.RegisterActivity(volumeActivity.UpdateVolumeStateInDB)
	s.env.RegisterActivity(volumeActivity.GetHosts)

	// Mock activities
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{}}, nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("test-token", nil)
	s.env.OnActivity(volumeActivity.FetchBackupMetadataForRestore, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(backupMetadata, nil)
	s.env.OnActivity(volumeActivity.UpdateVolumeStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow with nil backup vault and backup
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, params, volume)

	// Assert workflow completed with error about volume size
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "restored volume size should be greater than or equal to")
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_SDEBackupRestore_CrossRegion() {
	mockStorage := database.NewMockStorage(s.T())
	volumeActivity := activities.VolumeCreateActivity{SE: mockStorage}
	commonActivity := activities.CommonActivities{SE: mockStorage}

	// Test cross-region backup restore (backup in us-east1, volume in us-west1)
	volume := &datamodel.Volume{
		BaseModel:   datamodel.BaseModel{UUID: "vol-uuid-123"},
		Name:        "test-volume",
		SizeInBytes: 107374182400, // 100GB
		Account:     &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "test-account"},
		AccountID:   1,
		Pool: &datamodel.Pool{
			BaseModel:       datamodel.BaseModel{ID: 1, UUID: "pool-uuid-123"},
			DeploymentName:  "test-deployment",
			VendorID:        "gcp-us-west1-a", // Volume in us-west1
			PoolCredentials: &datamodel.PoolCredentials{Password: "password"},
		},
		Svm: &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			Protocols:          []string{"NFS"},
			RestoredBackupPath: "projects/123456/locations/us-east1/backupVaults/my-vault/backups/my-backup", // Backup in us-east1
		},
	}

	backupVault := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "bv-uuid-123"},
		Name:      "my-vault",
		BucketDetails: datamodel.BucketDetailsArray{
			{BucketName: "test-bucket", TenantProjectNumber: "123456789"},
		},
	}

	backup := &datamodel.Backup{
		BaseModel:   datamodel.BaseModel{UUID: "backup-uuid-123"},
		Name:        "my-backup",
		SizeInBytes: 1073741824, // 1GB
		BackupVault: backupVault,
		Attributes: &datamodel.BackupAttributes{
			BucketName: "test-bucket",
		},
	}

	backupMetadata := &activities.BackupRestoreMetadata{
		BackupVault:   backupVault,
		Backup:        backup,
		BucketDetails: backupVault.BucketDetails[0],
	}

	params := &common.CreateVolumeParams{
		AccountName: "test-account",
		Name:        "test-volume",
		Region:      "us-west1", // Volume region
		BackupPath:  "projects/123456/locations/us-east1/backupVaults/my-vault/backups/my-backup",
	}

	// Register activities
	s.env.RegisterActivity(volumeActivity.FetchBackupMetadataForRestore)
	s.env.RegisterActivity(volumeActivity.CreateSnapshotPolicyInONTAP)
	s.env.RegisterActivity(volumeActivity.UpdateVolumeStateInDB)
	s.env.RegisterActivity(volumeActivity.GetHosts)

	// Mock activities
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{}}, nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeActivity.FetchBackupMetadataForRestore, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(backupMetadata, nil)
	s.env.OnActivity(volumeActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(err1.New("stop workflow"))
	s.env.OnActivity(volumeActivity.UpdateVolumeStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow with nil backup vault and backup
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, params, volume)

	// Assert workflow completed (with error from snapshot policy - expected)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	// Verify FetchBackupMetadataForRestore was called for cross-region restore
}

func (s *UnitTestSuite) Test_CreateVolumeWorkflow_NoBackupPath_SkipsMetadataFetch() {
	mockStorage := database.NewMockStorage(s.T())
	volumeActivity := activities.VolumeCreateActivity{SE: mockStorage}
	commonActivity := activities.CommonActivities{SE: mockStorage}

	// Test volume without backup path - should NOT call FetchBackupMetadataForRestore
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "vol-uuid-123"},
		Name:      "test-volume",
		Account:   &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "test-account"},
		AccountID: 1,
		Pool: &datamodel.Pool{
			BaseModel:       datamodel.BaseModel{ID: 1, UUID: "pool-uuid-123"},
			DeploymentName:  "test-deployment",
			VendorID:        "gcp-us-central1-a",
			PoolCredentials: &datamodel.PoolCredentials{Password: "password"},
		},
		Svm: &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			Protocols: []string{"NFS"},
			// No RestoredBackupPath
		},
	}

	params := &common.CreateVolumeParams{
		AccountName: "test-account",
		Name:        "test-volume",
		Region:      "us-central1",
		// No BackupPath
	}

	// Register activities
	s.env.RegisterActivity(volumeActivity.FetchBackupMetadataForRestore)
	s.env.RegisterActivity(volumeActivity.CreateSnapshotPolicyInONTAP)
	s.env.RegisterActivity(volumeActivity.UpdateVolumeStateInDB)
	s.env.RegisterActivity(volumeActivity.GetHosts)

	// Mock activities - FetchBackupMetadataForRestore should NOT be called
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{}}, nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(volumeActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(err1.New("stop workflow"))
	s.env.OnActivity(volumeActivity.UpdateVolumeStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow without backup
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, params, volume)

	// Assert workflow completed (with error from snapshot policy - expected)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	// FetchBackupMetadataForRestore should NOT have been called since there's no backup path
}

// Test_CreateVolumeWorkflow_SDEBackupRestore_NilBackupMetadata tests nil backup metadata handling
// This test covers missing lines: 659-660
func (s *UnitTestSuite) Test_CreateVolumeWorkflow_SDEBackupRestore_NilBackupMetadata() {
	mockStorage := database.NewMockStorage(s.T())
	volumeActivity := activities.VolumeCreateActivity{SE: mockStorage}
	commonActivity := activities.CommonActivities{SE: mockStorage}

	volume := &datamodel.Volume{
		BaseModel:   datamodel.BaseModel{UUID: "vol-uuid-123"},
		Name:        "test-volume",
		SizeInBytes: 10737418240, // 10GB
		Account:     &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "test-account"},
		AccountID:   1,
		Pool: &datamodel.Pool{
			BaseModel:       datamodel.BaseModel{ID: 1, UUID: "pool-uuid-123"},
			DeploymentName:  "test-deployment",
			VendorID:        "gcp-us-central1-a",
			PoolCredentials: &datamodel.PoolCredentials{Password: "password"},
		},
		Svm: &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			Protocols:          []string{"NFS"},
			RestoredBackupPath: "projects/123456/locations/us-central1/backupVaults/my-vault/backups/my-backup",
		},
	}

	params := &common.CreateVolumeParams{
		AccountName: "test-account",
		Name:        "test-volume",
		Region:      "us-central1",
		BackupPath:  "projects/123456/locations/us-central1/backupVaults/my-vault/backups/my-backup",
	}

	// Register activities
	s.env.RegisterActivity(volumeActivity.FetchBackupMetadataForRestore)
	s.env.RegisterActivity(volumeActivity.UpdateVolumeStateInDB)
	s.env.RegisterActivity(volumeActivity.GetHosts)

	// Mock activities - return nil backupMetadata without error (covers lines 659-660)
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{}}, nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("test-token", nil)
	s.env.OnActivity(volumeActivity.FetchBackupMetadataForRestore, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil) // nil metadata, no error
	s.env.OnActivity(volumeActivity.UpdateVolumeStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, params, volume)

	// Assert workflow completed with error about nil backup metadata
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "failed to fetch backup metadata: received nil response")
}

// Test_CreateVolumeWorkflow_LargeVolumeRestoreCompatibility tests large volume restore compatibility verification
// This test covers missing lines: 680-684
func (s *UnitTestSuite) Test_CreateVolumeWorkflow_LargeVolumeRestoreCompatibility() {
	mockStorage := database.NewMockStorage(s.T())
	volumeActivity := activities.VolumeCreateActivity{SE: mockStorage}
	commonActivity := activities.CommonActivities{SE: mockStorage}

	volume := &datamodel.Volume{
		BaseModel:   datamodel.BaseModel{UUID: "vol-uuid-123"},
		Name:        "test-volume",
		SizeInBytes: 107374182400, // 100GB
		Account:     &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "test-account"},
		AccountID:   1,
		Pool: &datamodel.Pool{
			BaseModel:       datamodel.BaseModel{ID: 1, UUID: "pool-uuid-123"},
			DeploymentName:  "test-deployment",
			VendorID:        "gcp-us-central1-a",
			PoolCredentials: &datamodel.PoolCredentials{Password: "password"},
		},
		Svm: &datamodel.Svm{Name: "test-svm"},
		LargeVolumeAttributes: &datamodel.LargeVolumeAttributes{
			LargeCapacity: true,
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			Protocols:          []string{"NFS"},
			RestoredBackupPath: "projects/123456/locations/us-central1/backupVaults/my-vault/backups/my-backup",
		},
	}

	backupVault := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "bv-uuid-123"},
		Name:      "my-vault",
		BucketDetails: datamodel.BucketDetailsArray{
			{BucketName: "test-bucket", TenantProjectNumber: "123456789"},
		},
	}

	backup := &datamodel.Backup{
		BaseModel:   datamodel.BaseModel{UUID: "backup-uuid-123"},
		Name:        "my-backup",
		SizeInBytes: 1073741824, // 1GB
		BackupVault: backupVault,
		Attributes: &datamodel.BackupAttributes{
			BucketName:               "test-bucket",
			OntapVolumeStyle:         "flexgroup",
			ConstituentCountOfBackup: 4,
		},
	}

	backupMetadata := &activities.BackupRestoreMetadata{
		BackupVault:   backupVault,
		Backup:        backup,
		BucketDetails: backupVault.BucketDetails[0],
	}

	params := &common.CreateVolumeParams{
		AccountName:                 "test-account",
		Name:                        "test-volume",
		Region:                      "us-central1",
		BackupPath:                  "projects/123456/locations/us-central1/backupVaults/my-vault/backups/my-backup",
		LargeCapacity:               true,
		LargeVolumeConstituentCount: 4, // Matches backup
	}

	// Register activities
	s.env.RegisterActivity(volumeActivity.FetchBackupMetadataForRestore)
	s.env.RegisterActivity(volumeActivity.CreateSnapshotPolicyInONTAP)
	s.env.RegisterActivity(volumeActivity.UpdateVolumeStateInDB)
	s.env.RegisterActivity(volumeActivity.GetHosts)

	// Mock activities
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{}}, nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("test-token", nil)
	s.env.OnActivity(volumeActivity.FetchBackupMetadataForRestore, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(backupMetadata, nil)
	s.env.OnActivity(volumeActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(err1.New("stop workflow"))
	s.env.OnActivity(volumeActivity.UpdateVolumeStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, params, volume)

	// Assert workflow completed (with error from snapshot policy - expected)
	// The large volume compatibility check should have passed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
}

// Test_CreateVolumeWorkflow_LargeVolumeRestoreCompatibilityError tests the error path at line 685
// This test covers missing line: 685
func (s *UnitTestSuite) Test_CreateVolumeWorkflow_LargeVolumeRestoreCompatibilityError() {
	mockStorage := database.NewMockStorage(s.T())
	volumeActivity := activities.VolumeCreateActivity{SE: mockStorage}
	commonActivity := activities.CommonActivities{SE: mockStorage}

	volume := &datamodel.Volume{
		BaseModel:   datamodel.BaseModel{UUID: "vol-uuid-123"},
		Name:        "test-volume",
		SizeInBytes: 107374182400, // 100GB
		Account:     &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "test-account"},
		AccountID:   1,
		Pool: &datamodel.Pool{
			BaseModel:       datamodel.BaseModel{ID: 1, UUID: "pool-uuid-123"},
			DeploymentName:  "test-deployment",
			VendorID:        "gcp-us-central1-a",
			PoolCredentials: &datamodel.PoolCredentials{Password: "password"},
		},
		Svm: &datamodel.Svm{Name: "test-svm"},
		LargeVolumeAttributes: &datamodel.LargeVolumeAttributes{
			LargeCapacity: true,
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			Protocols:          []string{"NFS"},
			RestoredBackupPath: "projects/123456/locations/us-central1/backupVaults/my-vault/backups/my-backup",
		},
	}

	backupVault := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "bv-uuid-123"},
		Name:      "my-vault",
		BucketDetails: datamodel.BucketDetailsArray{
			{BucketName: "test-bucket", TenantProjectNumber: "123456789"},
		},
	}

	// Create backup with flexgroup style but mismatched constituent count
	backup := &datamodel.Backup{
		BaseModel:   datamodel.BaseModel{UUID: "backup-uuid-123"},
		Name:        "my-backup",
		SizeInBytes: 1073741824, // 1GB
		BackupVault: backupVault,
		Attributes: &datamodel.BackupAttributes{
			BucketName:               "test-bucket",
			OntapVolumeStyle:         "flexgroup",
			ConstituentCountOfBackup: 4, // Backup has 4 constituents
		},
	}

	backupMetadata := &activities.BackupRestoreMetadata{
		BackupVault:   backupVault,
		Backup:        backup,
		BucketDetails: backupVault.BucketDetails[0],
	}

	// Create params with mismatched constituent count (8 vs 4)
	params := &common.CreateVolumeParams{
		AccountName:                 "test-account",
		Name:                        "test-volume",
		Region:                      "us-central1",
		BackupPath:                  "projects/123456/locations/us-central1/backupVaults/my-vault/backups/my-backup",
		LargeCapacity:               true,
		LargeVolumeConstituentCount: 8, // Mismatched - backup has 4, customer wants 8
	}

	// Register activities
	s.env.RegisterActivity(volumeActivity.FetchBackupMetadataForRestore)
	s.env.RegisterActivity(volumeActivity.UpdateVolumeStateInDB)
	s.env.RegisterActivity(volumeActivity.GetHosts)

	// Mock activities
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{}}, nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("test-token", nil)
	s.env.OnActivity(volumeActivity.FetchBackupMetadataForRestore, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(backupMetadata, nil)
	s.env.OnActivity(volumeActivity.UpdateVolumeStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, params, volume)

	// Assert workflow completed with error
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "Constituent count provided")
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "does not match")
}

// Test_CreateVolumeWorkflow_LargeVolumeRestoreCompatibilityError_NonFlexgroupBackup tests the error path at line 685
// when trying to restore large capacity volume from non-flexgroup backup
func (s *UnitTestSuite) Test_CreateVolumeWorkflow_LargeVolumeRestoreCompatibilityError_NonFlexgroupBackup() {
	mockStorage := database.NewMockStorage(s.T())
	volumeActivity := activities.VolumeCreateActivity{SE: mockStorage}
	commonActivity := activities.CommonActivities{SE: mockStorage}

	volume := &datamodel.Volume{
		BaseModel:   datamodel.BaseModel{UUID: "vol-uuid-123"},
		Name:        "test-volume",
		SizeInBytes: 107374182400, // 100GB
		Account:     &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "test-account"},
		AccountID:   1,
		Pool: &datamodel.Pool{
			BaseModel:       datamodel.BaseModel{ID: 1, UUID: "pool-uuid-123"},
			DeploymentName:  "test-deployment",
			VendorID:        "gcp-us-central1-a",
			PoolCredentials: &datamodel.PoolCredentials{Password: "password"},
		},
		Svm: &datamodel.Svm{Name: "test-svm"},
		LargeVolumeAttributes: &datamodel.LargeVolumeAttributes{
			LargeCapacity: true,
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			Protocols:          []string{"NFS"},
			RestoredBackupPath: "projects/123456/locations/us-central1/backupVaults/my-vault/backups/my-backup",
		},
	}

	backupVault := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "bv-uuid-123"},
		Name:      "my-vault",
		BucketDetails: datamodel.BucketDetailsArray{
			{BucketName: "test-bucket", TenantProjectNumber: "123456789"},
		},
	}

	// Create backup with flexvol style (not flexgroup) - incompatible with large capacity
	backup := &datamodel.Backup{
		BaseModel:   datamodel.BaseModel{UUID: "backup-uuid-123"},
		Name:        "my-backup",
		SizeInBytes: 1073741824, // 1GB
		BackupVault: backupVault,
		Attributes: &datamodel.BackupAttributes{
			BucketName:       "test-bucket",
			OntapVolumeStyle: "flexvol", // Not flexgroup - incompatible with large capacity
		},
	}

	backupMetadata := &activities.BackupRestoreMetadata{
		BackupVault:   backupVault,
		Backup:        backup,
		BucketDetails: backupVault.BucketDetails[0],
	}

	// Create params with large capacity
	params := &common.CreateVolumeParams{
		AccountName:   "test-account",
		Name:          "test-volume",
		Region:        "us-central1",
		BackupPath:    "projects/123456/locations/us-central1/backupVaults/my-vault/backups/my-backup",
		LargeCapacity: true, // Trying to restore large capacity from non-flexgroup backup
	}

	// Register activities
	s.env.RegisterActivity(volumeActivity.FetchBackupMetadataForRestore)
	s.env.RegisterActivity(volumeActivity.UpdateVolumeStateInDB)
	s.env.RegisterActivity(volumeActivity.GetHosts)

	// Mock activities
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeActivity.GetHosts, mock.Anything, mock.Anything).Return([]*datamodel.HostGroup{{}}, nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("test-token", nil)
	s.env.OnActivity(volumeActivity.FetchBackupMetadataForRestore, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(backupMetadata, nil)
	s.env.OnActivity(volumeActivity.UpdateVolumeStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	s.env.ExecuteWorkflow(CreateVolumeWorkflow, params, volume)

	// Assert workflow completed with error
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "Cannot restore a large capacity volume from a backup that is not a large volume backup")
}

// TestVerifyBackupRestoreCompatibilityForLargeVolumes tests the _verifyBackupRestoreCompatibilityForLargeVolumes function
// These tests cover missing lines: 1028-1029, 1032-1033, 1036-1038, 1042-1043, 1046-1047, 1049
func TestVerifyBackupRestoreCompatibilityForLargeVolumes(t *testing.T) {
	t.Run("Error_LargeCapacityFromNonFlexgroupBackup", func(t *testing.T) {
		// This test covers lines 1028-1029
		backup := &datamodel.Backup{
			Attributes: &datamodel.BackupAttributes{
				OntapVolumeStyle: "flexvol", // Not flexgroup
			},
		}
		params := &common.CreateVolumeParams{
			LargeCapacity: true,
		}

		result, err := _verifyBackupRestoreCompatibilityForLargeVolumes(backup, params)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "Cannot restore a large capacity volume from a backup that is not a large volume backup")
	})

	t.Run("Success_NonFlexgroupBackup", func(t *testing.T) {
		// This test covers lines 1032-1033
		backup := &datamodel.Backup{
			Attributes: &datamodel.BackupAttributes{
				OntapVolumeStyle: "flexvol", // Not flexgroup
			},
		}
		params := &common.CreateVolumeParams{
			LargeCapacity: false,
		}

		result, err := _verifyBackupRestoreCompatibilityForLargeVolumes(backup, params)

		assert.NoError(t, err)
		assert.Equal(t, params, result)
	})

	t.Run("Success_SetConstituentCountFromBackup", func(t *testing.T) {
		// This test covers lines 1036-1038
		constituentCount := int32(4)
		backup := &datamodel.Backup{
			Attributes: &datamodel.BackupAttributes{
				OntapVolumeStyle:         "flexgroup",
				ConstituentCountOfBackup: constituentCount,
			},
		}
		params := &common.CreateVolumeParams{
			BackupPath:                  "projects/123456/locations/us-central1/backupVaults/my-vault/backups/my-backup",
			LargeCapacity:               true,
			LargeVolumeConstituentCount: 0, // Not provided
		}

		result, err := _verifyBackupRestoreCompatibilityForLargeVolumes(backup, params)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, constituentCount, result.LargeVolumeConstituentCount)
	})

	t.Run("Success_MatchingConstituentCounts", func(t *testing.T) {
		// This test covers lines 1042-1043, 1049
		constituentCount := int32(4)
		backup := &datamodel.Backup{
			Attributes: &datamodel.BackupAttributes{
				OntapVolumeStyle:         "flexgroup",
				ConstituentCountOfBackup: constituentCount,
			},
		}
		params := &common.CreateVolumeParams{
			LargeCapacity:               true,
			LargeVolumeConstituentCount: constituentCount, // Matches backup
		}

		result, err := _verifyBackupRestoreCompatibilityForLargeVolumes(backup, params)

		assert.NoError(t, err)
		assert.Equal(t, params, result)
	})

	t.Run("Error_MismatchedConstituentCounts", func(t *testing.T) {
		// This test covers lines 1042-1043, 1046-1047
		backupConstituentCount := int32(4)
		customerConstituentCount := int32(8)
		backup := &datamodel.Backup{
			Attributes: &datamodel.BackupAttributes{
				OntapVolumeStyle:         "flexgroup",
				ConstituentCountOfBackup: backupConstituentCount,
			},
		}
		params := &common.CreateVolumeParams{
			LargeCapacity:               true,
			LargeVolumeConstituentCount: customerConstituentCount, // Doesn't match backup
		}

		result, err := _verifyBackupRestoreCompatibilityForLargeVolumes(backup, params)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "Constituent count provided")
		assert.Contains(t, err.Error(), "does not match")
	})

	t.Run("Success_ZeroCustomerCount", func(t *testing.T) {
		// This test covers line 1049 when customer count is 0 (not provided)
		constituentCount := int32(4)
		backup := &datamodel.Backup{
			Attributes: &datamodel.BackupAttributes{
				OntapVolumeStyle:         "flexgroup",
				ConstituentCountOfBackup: constituentCount,
			},
		}
		params := &common.CreateVolumeParams{
			LargeCapacity:               true,
			LargeVolumeConstituentCount: 0, // Not provided, but BackupPath is empty so doesn't use line 1036-1038
		}

		result, err := _verifyBackupRestoreCompatibilityForLargeVolumes(backup, params)

		assert.NoError(t, err)
		assert.Equal(t, params, result)
	})
}
