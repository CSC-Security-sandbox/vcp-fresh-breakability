package workflows

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

type VolumeUpdateTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env *testsuite.TestWorkflowEnvironment
}

func (s *VolumeUpdateTestSuite) SetupTest() {
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
	s.env.RegisterWorkflow(UpdateVolumeWorkflow)
}

func (s *VolumeUpdateTestSuite) AfterTest() {
	s.env.AssertExpectations(s.T())
}

func (s *VolumeUpdateTestSuite) Test_UpdateVolumeWorkflow_Success() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	updateActivity := activities.VolumeUpdateActivity{SE: mockStorage}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInONTAP)
	s.env.RegisterActivity(updateActivity.UpdateLun)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInDB)

	// Mock activities
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(updateActivity.GetVolumeFromONTAP, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			ExternalUUID: "test-external-uuid",
			Name:         "test_volume",
		},
		AvailableSpace: 1000,
		Size:           1000,
		State:          "online",
	}, nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(updateActivity.UpdateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{}, nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	volume := &datamodel.Volume{
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, PoolCredentials: &datamodel.PoolCredentials{
			Password:      "password",
			SecretID:      "",
			CertificateID: "",
		}},
		Account: &datamodel.Account{
			Name: "test_account",
		},
		SizeInBytes: 1000,
		VolumeAttributes: &datamodel.VolumeAttributes{
			IsDataProtection: false,
		},
	}
	params := &common.UpdateVolumeParams{
		QuotaInBytes: 2000,
	}
	s.env.ExecuteWorkflow(UpdateVolumeWorkflow, params, volume)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *VolumeUpdateTestSuite) Test_UpdateVolumeWorkflow_Success_WithDataProtectionTrue() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	updateActivity := activities.VolumeUpdateActivity{SE: mockStorage}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInONTAP)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInDB)

	// Mock activities
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(updateActivity.GetVolumeFromONTAP, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			ExternalUUID: "test-external-uuid",
			Name:         "test_volume",
		},
		AvailableSpace: 1000,
		Size:           1000,
		State:          "online",
	}, nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	volume := &datamodel.Volume{
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, PoolCredentials: &datamodel.PoolCredentials{
			Password:      "password",
			SecretID:      "",
			CertificateID: "",
		}},
		Account: &datamodel.Account{
			Name: "test_account",
		},
		SizeInBytes: 1000,
		VolumeAttributes: &datamodel.VolumeAttributes{
			IsDataProtection: true,
		},
	}
	params := &common.UpdateVolumeParams{
		QuotaInBytes: 2000,
	}
	s.env.ExecuteWorkflow(UpdateVolumeWorkflow, params, volume)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *VolumeUpdateTestSuite) Test_UpdateVolumeWorkflow_Success_WithSnapshotPolicy_NoExistingWithNew() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	updateActivity := activities.VolumeUpdateActivity{SE: mockStorage}
	createActivity := activities.VolumeCreateActivity{SE: mockStorage}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInONTAP)
	s.env.RegisterActivity(updateActivity.UpdateLun)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInDB)
	s.env.RegisterActivity(createActivity.CreateSnapshotPolicyInONTAP)
	s.env.RegisterActivity(updateActivity.UpdateSnapshotPolicyInOntap)

	// Mock activities
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(updateActivity.GetVolumeFromONTAP, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			ExternalUUID: "test-external-uuid",
			Name:         "test_volume",
		},
		AvailableSpace: 1000,
		Size:           1000,
		State:          "online",
	}, nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(updateActivity.UpdateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{}, nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(createActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(updateActivity.UpdateSnapshotPolicyInOntap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Case : No existing snapshot policy, new policy provided
	volume1 := &datamodel.Volume{
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			}},
		Account:        &datamodel.Account{Name: "test_account"},
		SizeInBytes:    100,
		SnapshotPolicy: nil,
		VolumeAttributes: &datamodel.VolumeAttributes{
			IsDataProtection: false,
		},
	}
	params1 := &common.UpdateVolumeParams{
		QuotaInBytes: 200,
		SnapshotPolicy: &models.SnapshotPolicy{
			Name:      "policy1",
			IsEnabled: true,
			Schedules: []*models.SnapshotPolicySchedule{
				{
					Schedule: &models.Schedule{
						DaysOfMonth: []int{1, 2},
						DaysOfWeek:  []int{1},
						Hours:       []int{0},
						Minutes:     []int{0},
					},
					Count:           3,
					SnapmirrorLabel: "label1",
				},
			},
		},
		AutoTieringPolicy: &common.AutoTieringPolicy{
			AutoTieringEnabled: false,
		},
	}
	s.env.ExecuteWorkflow(UpdateVolumeWorkflow, params1, volume1)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
}

func (s *VolumeUpdateTestSuite) Test_UpdateVolumeWorkflow_Success_WithSnapshotPolicy_NoExistingWithNew_Error() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	updateActivity := activities.VolumeUpdateActivity{SE: mockStorage}
	createActivity := activities.VolumeCreateActivity{SE: mockStorage}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInONTAP)
	s.env.RegisterActivity(updateActivity.UpdateLun)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInDB)
	s.env.RegisterActivity(createActivity.CreateSnapshotPolicyInONTAP)
	s.env.RegisterActivity(updateActivity.UpdateSnapshotPolicyInOntap)

	// Mock activities
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return(&datamodel.Node{EndpointAddress: "127.0.0.1"}, nil)
	s.env.OnActivity(updateActivity.GetVolumeFromONTAP, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			ExternalUUID: "test-external-uuid",
			Name:         "test_volume",
		},
		AvailableSpace: 1000,
		Size:           1000,
		State:          "online",
	}, nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(updateActivity.UpdateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{}, nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(createActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("error"))
	s.env.OnActivity(updateActivity.UpdateSnapshotPolicyInOntap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Case : No existing snapshot policy, new policy provided
	volume1 := &datamodel.Volume{
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			}},
		Account:        &datamodel.Account{Name: "test_account"},
		SizeInBytes:    100,
		SnapshotPolicy: nil,
		VolumeAttributes: &datamodel.VolumeAttributes{
			IsDataProtection: false,
		},
	}
	params1 := &common.UpdateVolumeParams{
		QuotaInBytes: 200,
		SnapshotPolicy: &models.SnapshotPolicy{
			Name:      "policy1",
			IsEnabled: true,
			Schedules: []*models.SnapshotPolicySchedule{
				{
					Schedule: &models.Schedule{
						DaysOfMonth: []int{1, 2},
						DaysOfWeek:  []int{1},
						Hours:       []int{0},
						Minutes:     []int{0},
					},
					Count:           3,
					SnapmirrorLabel: "label1",
				},
			},
		},
	}
	s.env.ExecuteWorkflow(UpdateVolumeWorkflow, params1, volume1)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
}

func (s *VolumeUpdateTestSuite) Test_UpdateVolumeWorkflow_Success_WithSnapshotPolicy_ExistingWithNew() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	updateActivity := activities.VolumeUpdateActivity{SE: mockStorage}
	createActivity := activities.VolumeCreateActivity{SE: mockStorage}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInONTAP)
	s.env.RegisterActivity(updateActivity.UpdateLun)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInDB)
	s.env.RegisterActivity(createActivity.CreateSnapshotPolicyInONTAP)
	s.env.RegisterActivity(updateActivity.UpdateSnapshotPolicyInOntap)

	// Mock activities
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(updateActivity.GetVolumeFromONTAP, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			ExternalUUID: "test-external-uuid",
			Name:         "test_volume",
		},
		AvailableSpace: 1000,
		Size:           1000,
		State:          "online",
	}, nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(updateActivity.UpdateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{}, nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(createActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(updateActivity.UpdateSnapshotPolicyInOntap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Case : Existing snapshot policy, new policy provided (update)
	volume2 := &datamodel.Volume{
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, PoolCredentials: &datamodel.PoolCredentials{
			Password:      "password",
			SecretID:      "",
			CertificateID: "",
		}},
		Account:     &datamodel.Account{Name: "test_account"},
		SizeInBytes: 100,
		SnapshotPolicy: &datamodel.SnapshotPolicy{
			Name:      "policy1",
			IsEnabled: true,
			Schedules: []*datamodel.SnapshotPolicySchedule{},
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			IsDataProtection: false,
		},
	}
	params2 := &common.UpdateVolumeParams{
		QuotaInBytes: 200,
		SnapshotPolicy: &models.SnapshotPolicy{
			Name:      "policy2",
			IsEnabled: false,
			Schedules: []*models.SnapshotPolicySchedule{
				{
					Schedule: &models.Schedule{
						DaysOfMonth: []int{3, 4},
						DaysOfWeek:  []int{2},
						Hours:       []int{1},
						Minutes:     []int{30},
					},
					Count:           2,
					SnapmirrorLabel: "label2",
				},
			},
		},
		AutoTieringPolicy: &common.AutoTieringPolicy{
			AutoTieringEnabled: false,
		},
	}
	s.env.ExecuteWorkflow(UpdateVolumeWorkflow, params2, volume2)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
}

func (s *VolumeUpdateTestSuite) Test_UpdateVolumeWorkflow_Success_WithSnapshotPolicy_ExistingWithNew_Error() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	updateActivity := activities.VolumeUpdateActivity{SE: mockStorage}
	createActivity := activities.VolumeCreateActivity{SE: mockStorage}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInONTAP)
	s.env.RegisterActivity(updateActivity.UpdateLun)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInDB)
	s.env.RegisterActivity(createActivity.CreateSnapshotPolicyInONTAP)
	s.env.RegisterActivity(updateActivity.UpdateSnapshotPolicyInOntap)

	// Mock activities
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return(&datamodel.Node{EndpointAddress: "127.0.0.1"}, nil)
	s.env.OnActivity(updateActivity.GetVolumeFromONTAP, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			ExternalUUID: "test-external-uuid",
			Name:         "test_volume",
		},
		AvailableSpace: 1000,
		Size:           1000,
		State:          "online",
	}, nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(updateActivity.UpdateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{}, nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(createActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(updateActivity.UpdateSnapshotPolicyInOntap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("error"))

	// Case : Existing snapshot policy, new policy provided (update)
	volume2 := &datamodel.Volume{
		Pool:        &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}},
		Account:     &datamodel.Account{Name: "test_account"},
		SizeInBytes: 100,
		SnapshotPolicy: &datamodel.SnapshotPolicy{
			Name:      "policy1",
			IsEnabled: true,
			Schedules: []*datamodel.SnapshotPolicySchedule{},
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			IsDataProtection: false,
		},
	}
	params2 := &common.UpdateVolumeParams{
		QuotaInBytes: 200,
		SnapshotPolicy: &models.SnapshotPolicy{
			Name:      "policy2",
			IsEnabled: false,
			Schedules: []*models.SnapshotPolicySchedule{
				{
					Schedule: &models.Schedule{
						DaysOfMonth: []int{3, 4},
						DaysOfWeek:  []int{2},
						Hours:       []int{1},
						Minutes:     []int{30},
					},
					Count:           2,
					SnapmirrorLabel: "label2",
				},
			},
		},
	}
	s.env.ExecuteWorkflow(UpdateVolumeWorkflow, params2, volume2)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
}

func (s *VolumeUpdateTestSuite) Test_UpdateVolumeWorkflow_Success_WithSnapshotPolicy_NoExisting() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	updateActivity := activities.VolumeUpdateActivity{SE: mockStorage}
	createActivity := activities.VolumeCreateActivity{SE: mockStorage}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInONTAP)
	s.env.RegisterActivity(updateActivity.UpdateLun)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInDB)
	s.env.RegisterActivity(createActivity.CreateSnapshotPolicyInONTAP)
	s.env.RegisterActivity(updateActivity.UpdateSnapshotPolicyInOntap)

	// Mock activities
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(updateActivity.GetVolumeFromONTAP, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			ExternalUUID: "test-external-uuid",
			Name:         "test_volume",
		},
		AvailableSpace: 1000,
		Size:           1000,
		State:          "online",
	}, nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(updateActivity.UpdateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{}, nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(createActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(updateActivity.UpdateSnapshotPolicyInOntap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Case : No snapshot policy provided in params (should skip snapshot policy logic)
	volume3 := &datamodel.Volume{
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			}},
		Account:     &datamodel.Account{Name: "test_account"},
		SizeInBytes: 100,
		VolumeAttributes: &datamodel.VolumeAttributes{
			IsDataProtection: false,
		},
	}
	params3 := &common.UpdateVolumeParams{
		QuotaInBytes:   200,
		SnapshotPolicy: nil,
		AutoTieringPolicy: &common.AutoTieringPolicy{
			AutoTieringEnabled: false,
		},
	}
	s.env.ExecuteWorkflow(UpdateVolumeWorkflow, params3, volume3)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
}

func (s *VolumeUpdateTestSuite) Test_UpdateVolumeWorkflow_SnapshotPolicy_OnlyEnableDisable() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	updateActivity := activities.VolumeUpdateActivity{SE: mockStorage}
	createActivity := activities.VolumeCreateActivity{SE: mockStorage}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInONTAP)
	s.env.RegisterActivity(updateActivity.UpdateLun)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInDB)
	s.env.RegisterActivity(createActivity.CreateSnapshotPolicyInONTAP)
	s.env.RegisterActivity(updateActivity.UpdateSnapshotPolicyInOntap)

	// Mock activities
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(updateActivity.GetVolumeFromONTAP, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			ExternalUUID: "test-external-uuid",
			Name:         "test_volume",
		},
		AvailableSpace: 1000,
		Size:           1000,
		State:          "online",
	}, nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(updateActivity.UpdateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{}, nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(createActivity.CreateSnapshotPolicyInONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(updateActivity.UpdateSnapshotPolicyInOntap, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	volume := &datamodel.Volume{
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			}},
		Account:     &datamodel.Account{Name: "test_account"},
		SizeInBytes: 100,
		SnapshotPolicy: &datamodel.SnapshotPolicy{
			Name:      "policy1",
			IsEnabled: true,
			Schedules: []*datamodel.SnapshotPolicySchedule{
				// Existing schedules
			},
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			IsDataProtection: false,
		},
	}
	params := &common.UpdateVolumeParams{
		QuotaInBytes: 200,
		SnapshotPolicy: &models.SnapshotPolicy{
			Name:      "policy1",
			IsEnabled: true,
			Schedules: []*models.SnapshotPolicySchedule{}, // Empty schedules
		},
		AutoTieringPolicy: &common.AutoTieringPolicy{
			AutoTieringEnabled: false,
		},
	}
	s.env.ExecuteWorkflow(UpdateVolumeWorkflow, params, volume)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
}

func (s *VolumeUpdateTestSuite) Test_UpdateVolumeWorkflow_DataProtectionVolume_SnapshotPolicy_Skip() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	updateActivity := activities.VolumeUpdateActivity{SE: mockStorage}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInONTAP)
	s.env.RegisterActivity(updateActivity.UpdateLun)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInDB)

	// Mock activities
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(updateActivity.GetVolumeFromONTAP, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			ExternalUUID: "test-external-uuid",
			Name:         "test_volume",
		},
		AvailableSpace: 1000,
		Size:           1000,
		State:          "online",
	}, nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(updateActivity.UpdateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{}, nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Case : No existing snapshot policy, new policy provided
	volume1 := &datamodel.Volume{
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			}},
		Account:        &datamodel.Account{Name: "test_account"},
		SizeInBytes:    100,
		SnapshotPolicy: nil,
		VolumeAttributes: &datamodel.VolumeAttributes{
			IsDataProtection: true,
		},
	}
	params1 := &common.UpdateVolumeParams{
		QuotaInBytes: 200,
		SnapshotPolicy: &models.SnapshotPolicy{
			Name:      "policy1",
			IsEnabled: true,
			Schedules: []*models.SnapshotPolicySchedule{
				{
					Schedule: &models.Schedule{
						DaysOfMonth: []int{1, 2},
						DaysOfWeek:  []int{1},
						Hours:       []int{0},
						Minutes:     []int{0},
					},
					Count:           3,
					SnapmirrorLabel: "label1",
				},
			},
		},
		AutoTieringPolicy: &common.AutoTieringPolicy{
			AutoTieringEnabled: false,
		},
	}
	s.env.ExecuteWorkflow(UpdateVolumeWorkflow, params1, volume1)
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
}

func (s *VolumeUpdateTestSuite) Test_UpdateVolumeWorkflow_NoSizeChange() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	updateActivity := activities.VolumeUpdateActivity{SE: mockStorage}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "PROCESSING", mock.Anything, mock.Anything).Return(nil)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "DONE", mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInONTAP)
	s.env.RegisterActivity(updateActivity.UpdateLun)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInDB)

	// Mock activities
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(updateActivity.GetVolumeFromONTAP, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			ExternalUUID: "test-external-uuid",
			Name:         "test_volume",
		},
		AvailableSpace: 100,
		Size:           200,
		State:          "online",
	}, nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow with no size change
	volume := &datamodel.Volume{
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			}},
		Account: &datamodel.Account{
			Name: "test_account",
		},
		SizeInBytes: 100,
	}
	params := &common.UpdateVolumeParams{
		QuotaInBytes: 100,
		AutoTieringPolicy: &common.AutoTieringPolicy{
			AutoTieringEnabled: false,
		},
	}
	s.env.ExecuteWorkflow(UpdateVolumeWorkflow, params, volume)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *VolumeUpdateTestSuite) Test_UpdateVolumeWorkflow_Failure() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	updateActivity := activities.VolumeUpdateActivity{SE: mockStorage}
	createActivity := activities.VolumeCreateActivity{SE: mockStorage}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "PROCESSING", mock.Anything, mock.Anything).Return(nil)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, "ERROR", mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(commonActivity.GetNode)
	s.env.RegisterActivity(updateActivity.GetVolumeFromONTAP)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInONTAP)
	s.env.RegisterActivity(updateActivity.UpdateLun)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInDB)
	s.env.RegisterActivity(createActivity.UpdateVolumeStateInDB)

	// Mock activities
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(updateActivity.GetVolumeFromONTAP, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("ONTAP error"))
	s.env.OnActivity(updateActivity.UpdateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return()
	s.env.OnActivity(updateActivity.UpdateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{}, nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(createActivity.UpdateVolumeStateInDB, mock.Anything, "test-volume-uuid", models.LifeCycleStateREADY, models.LifeCycleStateAvailableDetails).Return(nil)

	// Execute workflow
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{ID: int64(2), UUID: "test-volume-uuid"},
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: int64(1), UUID: "test-pool-uuid"},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
		},
		Account: &datamodel.Account{
			Name: "test_account",
		},
		SizeInBytes: 100,
	}
	params := &common.UpdateVolumeParams{
		QuotaInBytes: 200,
	}
	s.env.ExecuteWorkflow(UpdateVolumeWorkflow, params, volume)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "workflow execution error")
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *VolumeUpdateTestSuite) Test_UpdateVolumeWorkflow_JobUpdateFailure() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	updateActivity := activities.VolumeUpdateActivity{SE: mockStorage}
	createActivity := activities.VolumeCreateActivity{SE: mockStorage}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Times(2)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed updating job"))

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(commonActivity.GetNode)
	s.env.RegisterActivity(updateActivity.GetVolumeFromONTAP)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInONTAP)
	s.env.RegisterActivity(updateActivity.UpdateLun)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInDB)
	s.env.RegisterActivity(createActivity.UpdateVolumeStateInDB)

	// Mock activities
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("test-token", nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(updateActivity.GetVolumeFromONTAP, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			ExternalUUID: "test-external-uuid",
			Name:         "test_volume",
		},
		AvailableSpace: 100,
		Size:           100,
		State:          "online",
	}, nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("ONTAP error"))
	s.env.OnActivity(updateActivity.UpdateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{}, nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(createActivity.UpdateVolumeStateInDB, mock.Anything, "test-volume-uuid", models.LifeCycleStateREADY, models.LifeCycleStateAvailableDetails).Return(nil)

	// Execute workflow
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{ID: int64(2), UUID: "test-volume-uuid"},
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: int64(1), UUID: "test-pool-uuid"},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
		},
		Account: &datamodel.Account{
			Name: "test_account",
		},
		SizeInBytes: 100,
	}
	params := &common.UpdateVolumeParams{
		QuotaInBytes: 200,
	}
	s.env.ExecuteWorkflow(UpdateVolumeWorkflow, params, volume)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "workflow execution error")
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *VolumeUpdateTestSuite) Test_UpdateVolumeWorkflow_SizeChanged() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	updateActivity := activities.VolumeUpdateActivity{SE: mockStorage}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInONTAP)
	s.env.RegisterActivity(updateActivity.UpdateLun)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInDB)

	// Mock activities
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(updateActivity.GetVolumeFromONTAP, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			ExternalUUID: "test-external-uuid",
			Name:         "test_volume",
		},
		AvailableSpace: 1000,
		Size:           1000,
		State:          "online",
	}, nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(updateActivity.UpdateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{
		ProviderResponse: vsa.ProviderResponse{
			ExternalUUID: "test-external-uuid",
			Name:         "lun_test_volume",
		},
		SerialNumber: "6c573830325d596f4f373437",
		Size:         88424124416,
		OSType:       "LINUX",
	}, nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	volume := &datamodel.Volume{
		Name: "test_volume",
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, PoolCredentials: &datamodel.PoolCredentials{
			Password:      "password",
			SecretID:      "",
			CertificateID: "",
		}},
		Account: &datamodel.Account{
			Name: "test_account",
		},
		SizeInBytes: 1000,
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockDevices: &[]datamodel.BlockDevice{
				{
					Name: "lun_test_volume",
					HostGroupDetails: []datamodel.HostGroupDetail{
						{
							HostGroupUUID: "hg-uuid-1",
							HostQNs:       []string{"iqn.1998-01.com.vmware:host1"},
						},
					},
					OSType: "LINUX",
				},
			},
		},
	}
	params := &common.UpdateVolumeParams{
		QuotaInBytes: 2000,
	}
	s.env.ExecuteWorkflow(UpdateVolumeWorkflow, params, volume)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *VolumeUpdateTestSuite) Test_UpdateVolumeWorkflow_WithBlockDevices_HostGroupsChanged() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	updateActivity := activities.VolumeUpdateActivity{SE: mockStorage}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInONTAP)
	s.env.RegisterActivity(updateActivity.UpdateLun)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInDB)
	s.env.RegisterActivity(updateActivity.EnsureHostGroupsExistsAndMapDisk)
	s.env.RegisterActivity(updateActivity.UnmapHostGroupFromDisk)

	// Mock activities
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(updateActivity.GetVolumeFromONTAP, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			ExternalUUID: "test-external-uuid",
			Name:         "test_volume",
		},
		AvailableSpace: 1000,
		Size:           1000,
		State:          "online",
	}, nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(updateActivity.UpdateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{}, nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(updateActivity.EnsureHostGroupsExistsAndMapDisk, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(updateActivity.UnmapHostGroupFromDisk, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	volume := &datamodel.Volume{
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, PoolCredentials: &datamodel.PoolCredentials{
			Password:      "password",
			SecretID:      "",
			CertificateID: "",
		}},
		Account: &datamodel.Account{
			Name: "test_account",
		},
		SizeInBytes: 1000,
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
				},
			},
		},
	}
	params := &common.UpdateVolumeParams{
		QuotaInBytes: 2000,
		BlockDevices: []*common.BlockDevice{
			{
				Name:       "test-lun",
				HostGroups: []string{"hg-uuid-2", "hg-uuid-3"}, // Different host groups
			},
		},
	}
	s.env.ExecuteWorkflow(UpdateVolumeWorkflow, params, volume)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *VolumeUpdateTestSuite) Test_UpdateVolumeWorkflow_WithBlockDevices_HostGroupsUnchanged() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	updateActivity := activities.VolumeUpdateActivity{SE: mockStorage}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInONTAP)
	s.env.RegisterActivity(updateActivity.UpdateLun)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInDB)

	// Mock activities
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(updateActivity.GetVolumeFromONTAP, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			ExternalUUID: "test-external-uuid",
			Name:         "test_volume",
		},
		AvailableSpace: 1000,
		Size:           1000,
		State:          "online",
	}, nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(updateActivity.UpdateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{}, nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	volume := &datamodel.Volume{
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, PoolCredentials: &datamodel.PoolCredentials{
			Password:      "password",
			SecretID:      "",
			CertificateID: "",
		}},
		Account: &datamodel.Account{
			Name: "test_account",
		},
		SizeInBytes: 1000,
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
				},
			},
		},
	}
	params := &common.UpdateVolumeParams{
		QuotaInBytes: 2000,
		BlockDevices: []*common.BlockDevice{
			{
				Name:       "test-lun",
				HostGroups: []string{"hg-uuid-1"}, // Same host groups
			},
		},
	}
	s.env.ExecuteWorkflow(UpdateVolumeWorkflow, params, volume)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *VolumeUpdateTestSuite) Test_UpdateVolumeWorkflow_WithBlockDevices_NoExistingBlockDevices() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	updateActivity := activities.VolumeUpdateActivity{SE: mockStorage}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInONTAP)
	s.env.RegisterActivity(updateActivity.UpdateLun)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInDB)
	s.env.RegisterActivity(updateActivity.EnsureHostGroupsExistsAndMapDisk)

	// Mock activities
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(updateActivity.GetVolumeFromONTAP, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			ExternalUUID: "test-external-uuid",
			Name:         "test_volume",
		},
		AvailableSpace: 1000,
		Size:           1000,
		State:          "online",
	}, nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(updateActivity.UpdateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{}, nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(updateActivity.EnsureHostGroupsExistsAndMapDisk, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	volume := &datamodel.Volume{
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, PoolCredentials: &datamodel.PoolCredentials{
			Password:      "password",
			SecretID:      "",
			CertificateID: "",
		}},
		Account: &datamodel.Account{
			Name: "test_account",
		},
		SizeInBytes: 1000,
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockDevices: nil, // No existing BlockDevices
		},
	}
	params := &common.UpdateVolumeParams{
		QuotaInBytes: 2000,
		BlockDevices: []*common.BlockDevice{
			{
				Name:       "test-lun",
				HostGroups: []string{"hg-uuid-1", "hg-uuid-2"},
			},
		},
	}
	s.env.ExecuteWorkflow(UpdateVolumeWorkflow, params, volume)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *VolumeUpdateTestSuite) Test_UpdateVolumeWorkflow_WithBlockProperties_Fallback() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	updateActivity := activities.VolumeUpdateActivity{SE: mockStorage}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInONTAP)
	s.env.RegisterActivity(updateActivity.UpdateLun)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInDB)
	s.env.RegisterActivity(updateActivity.EnsureHostGroupsExistsAndMapDisk)
	s.env.RegisterActivity(updateActivity.UnmapHostGroupFromDisk)

	// Mock activities
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(updateActivity.GetVolumeFromONTAP, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			ExternalUUID: "test-external-uuid",
			Name:         "test_volume",
		},
		AvailableSpace: 1000,
		Size:           1000,
		State:          "online",
	}, nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(updateActivity.UpdateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{}, nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(updateActivity.EnsureHostGroupsExistsAndMapDisk, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(updateActivity.UnmapHostGroupFromDisk, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	volume := &datamodel.Volume{
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, PoolCredentials: &datamodel.PoolCredentials{
			Password:      "password",
			SecretID:      "",
			CertificateID: "",
		}},
		Account: &datamodel.Account{
			Name: "test_account",
		},
		SizeInBytes: 1000,
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockDevices: &[]datamodel.BlockDevice{}, // Empty BlockDevices
			BlockProperties: &datamodel.BlockProperties{
				HostGroupDetails: []datamodel.HostGroupDetail{
					{
						HostGroupUUID: "hg-uuid-1",
						HostQNs:       []string{"iqn.1998-01.com.vmware:host1"},
					},
				},
			},
		},
	}
	params := &common.UpdateVolumeParams{
		QuotaInBytes: 2000,
		BlockDevices: []*common.BlockDevice{}, // Empty BlockDevices
		BlockProperties: &common.BlockPropertiesRequest{
			HostGroupUUIDs: []string{"hg-uuid-2", "hg-uuid-3"}, // Different host groups
		},
	}
	s.env.ExecuteWorkflow(UpdateVolumeWorkflow, params, volume)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *VolumeUpdateTestSuite) Test_UpdateVolumeWorkflow_BPSuccess() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	updateActivity := activities.VolumeUpdateActivity{SE: mockStorage}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInONTAP)
	s.env.RegisterActivity(updateActivity.UpdateLun)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInDB)

	// Mock activities
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(updateActivity.GetVolumeFromONTAP, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			ExternalUUID: "test-external-uuid",
			Name:         "test_volume",
		},
		AvailableSpace: 100,
		Size:           200,
		State:          "online",
	}, nil)
	s.env.OnActivity(updateActivity.EnsureHostGroupsExistsAndMapDisk, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(updateActivity.UnmapHostGroupFromDisk, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow with no size change
	volume := &datamodel.Volume{
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
		},
		Account: &datamodel.Account{
			Name: "test_account",
		},
		SizeInBytes: 100,
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockProperties: &datamodel.BlockProperties{
				HostGroupDetails: []datamodel.HostGroupDetail{
					{
						HostGroupUUID: "hg-uuid-1",
					},
					{
						HostGroupUUID: "hg-uuid-2",
					},
				},
			},
		},
	}
	params := &common.UpdateVolumeParams{
		QuotaInBytes: 100,
		BlockProperties: &common.BlockPropertiesRequest{
			HostGroupUUIDs: []string{"hg-uuid-3"},
		},
		AutoTieringPolicy: &common.AutoTieringPolicy{
			AutoTieringEnabled: false,
		},
	}
	s.env.ExecuteWorkflow(UpdateVolumeWorkflow, params, volume)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func TestVolumeUpdateTestSuite(t *testing.T) {
	suite.Run(t, new(VolumeUpdateTestSuite))
}

func (s *VolumeUpdateTestSuite) Test_UpdateVolumeWorkflow_FindTenancyDetailsFailure() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	updateActivity := activities.VolumeUpdateActivity{SE: mockStorage}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInONTAP)
	s.env.RegisterActivity(updateActivity.UpdateLun)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInDB)

	// Mock activities
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(updateActivity.GetVolumeFromONTAP, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			ExternalUUID: "test-external-uuid",
			Name:         "test_volume",
		},
		AvailableSpace: 1000,
		Size:           1000,
		State:          "online",
	}, nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(updateActivity.UpdateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{}, nil)
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("test-token", nil)
	s.env.OnActivity(updateActivity.FindTenancyDetails, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to find tenancy details"))
	// s.env.OnActivity(updateActivity.UpdateVolumeInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	volume := &datamodel.Volume{
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			}},
		Account: &datamodel.Account{
			Name: "test_account",
		},
		SizeInBytes: 100,
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "test-backup-vault",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			VendorSubnetID: "test-subnet-id",
		},
	}
	params := &common.UpdateVolumeParams{
		QuotaInBytes: 200,
		Region:       "us-west-1",
	}
	s.env.ExecuteWorkflow(UpdateVolumeWorkflow, params, volume)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *VolumeUpdateTestSuite) Test_UpdateVolumeWorkflow_TokenError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	updateActivity := activities.VolumeUpdateActivity{SE: mockStorage}
	createActivity := activities.VolumeCreateActivity{SE: mockStorage}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(commonActivity.GetNode)
	s.env.RegisterActivity(updateActivity.GetVolumeFromONTAP)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInONTAP)
	s.env.RegisterActivity(updateActivity.UpdateLun)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInDB)
	s.env.RegisterActivity(commonActivity.GetAuthJWTToken)
	s.env.RegisterActivity(createActivity.UpdateVolumeStateInDB)

	// Mock activities
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(updateActivity.GetVolumeFromONTAP, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			ExternalUUID: "test-external-uuid",
			Name:         "test_volume",
		},
		AvailableSpace: 1000,
		Size:           1000,
		State:          "online",
	}, nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(updateActivity.UpdateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{}, nil)
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return(nil, errors.New("failed to get auth JWT token"))
	s.env.OnActivity(createActivity.UpdateVolumeStateInDB, mock.Anything, "test-volume-uuid", models.LifeCycleStateREADY, models.LifeCycleStateAvailableDetails).Return(nil)

	// Execute workflow
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
		},
		Account: &datamodel.Account{
			Name: "test_account",
		},
		SizeInBytes: 100,
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "test-backup-vault",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			VendorSubnetID: "test-subnet-id",
		},
	}
	params := &common.UpdateVolumeParams{
		QuotaInBytes: 200,
		Region:       "us-west-1",
		AccountName:  "test_account",
	}
	s.env.ExecuteWorkflow(UpdateVolumeWorkflow, params, volume)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *VolumeUpdateTestSuite) Test_UpdateVolumeWorkflow_CheckBackupVaultExistInVCPFailure() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	updateActivity := activities.VolumeUpdateActivity{SE: mockStorage}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInONTAP)
	s.env.RegisterActivity(updateActivity.UpdateLun)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInDB)

	// Mock activities
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(updateActivity.GetVolumeFromONTAP, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			ExternalUUID: "test-external-uuid",
			Name:         "test_volume",
		},
		AvailableSpace: 1000,
		Size:           1000,
		State:          "online",
	}, nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(updateActivity.UpdateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{}, nil)
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("test-token", nil)
	s.env.OnActivity(updateActivity.FindTenancyDetails, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.TenancyInfo{RegionalTenantProject: "tenant-project"}, nil)
	s.env.OnActivity(updateActivity.CheckBackupVaultExistInVCP, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to check backup vault exists in VCP"))

	// Execute workflow
	volume := &datamodel.Volume{
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			}},
		Account: &datamodel.Account{
			Name: "test_account",
		},
		SizeInBytes: 100,
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "test-backup-vault",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			VendorSubnetID: "test-subnet-id",
		},
	}
	params := &common.UpdateVolumeParams{
		QuotaInBytes: 200,
		Region:       "us-west-1",
	}
	s.env.ExecuteWorkflow(UpdateVolumeWorkflow, params, volume)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *VolumeUpdateTestSuite) Test_UpdateVolumeWorkflow_CheckBucketResourceNameFailure() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	updateActivity := activities.VolumeUpdateActivity{SE: mockStorage}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInONTAP)
	s.env.RegisterActivity(updateActivity.UpdateLun)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInDB)

	// Mock activities
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(updateActivity.GetVolumeFromONTAP, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			ExternalUUID: "test-external-uuid",
			Name:         "test_volume",
		},
		AvailableSpace: 1000,
		Size:           1000,
		State:          "online",
	}, nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(updateActivity.UpdateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{}, nil)
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("test-token", nil)
	s.env.OnActivity(updateActivity.FindTenancyDetails, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.TenancyInfo{RegionalTenantProject: "tenant-project"}, nil)
	s.env.OnActivity(updateActivity.CheckBackupVaultExistInVCP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(updateActivity.CheckBucketResourceName, mock.Anything, mock.Anything).Return(nil, errors.New("failed to check for bucket resource name"))
	// s.env.OnActivity(updateActivity.UpdateVolumeInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	volume := &datamodel.Volume{
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			}},
		Account: &datamodel.Account{
			Name: "test_account",
		},
		SizeInBytes: 100,
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "test-backup-vault",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			VendorSubnetID: "test-subnet-id",
		},
	}
	params := &common.UpdateVolumeParams{
		QuotaInBytes: 200,
		Region:       "us-west-1",
	}
	s.env.ExecuteWorkflow(UpdateVolumeWorkflow, params, volume)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *VolumeUpdateTestSuite) Test_UpdateVolumeWorkflow_GenerateResourceNamesForBackupVaultFailure() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	updateActivity := activities.VolumeUpdateActivity{SE: mockStorage}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInONTAP)
	s.env.RegisterActivity(updateActivity.UpdateLun)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInDB)

	// Mock activities
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(updateActivity.GetVolumeFromONTAP, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			ExternalUUID: "test-external-uuid",
			Name:         "test_volume",
		},
		AvailableSpace: 1000,
		Size:           1000,
		State:          "online",
	}, nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(updateActivity.UpdateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{}, nil)
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("test-token", nil)
	s.env.OnActivity(updateActivity.FindTenancyDetails, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.TenancyInfo{RegionalTenantProject: "tenant-project"}, nil)
	s.env.OnActivity(updateActivity.CheckBackupVaultExistInVCP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(updateActivity.CheckBucketResourceName, mock.Anything, mock.Anything).Return(&common.BucketDetails{}, nil)
	s.env.OnActivity(updateActivity.GenerateResourceNamesForBackupVault, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to generate resource names"))

	// s.env.OnActivity(updateActivity.UpdateVolumeInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	volume := &datamodel.Volume{
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			}},
		Account: &datamodel.Account{
			Name: "test_account",
		},
		SizeInBytes: 100,
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "test-backup-vault",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			VendorSubnetID: "test-subnet-id",
		},
	}
	params := &common.UpdateVolumeParams{
		QuotaInBytes: 200,
		Region:       "us-west-1",
	}
	s.env.ExecuteWorkflow(UpdateVolumeWorkflow, params, volume)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *VolumeUpdateTestSuite) Test_UpdateVolumeWorkflow_CreateBucketForBackupVaultFailure() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	updateActivity := activities.VolumeUpdateActivity{SE: mockStorage}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInONTAP)
	s.env.RegisterActivity(updateActivity.UpdateLun)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInDB)

	// Mock activities
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(updateActivity.GetVolumeFromONTAP, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			ExternalUUID: "test-external-uuid",
			Name:         "test_volume",
		},
		AvailableSpace: 1000,
		Size:           1000,
		State:          "online",
	}, nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(updateActivity.UpdateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{}, nil)
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("test-token", nil)
	s.env.OnActivity(updateActivity.FindTenancyDetails, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.TenancyInfo{RegionalTenantProject: "tenant-project"}, nil)
	s.env.OnActivity(updateActivity.CheckBackupVaultExistInVCP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(updateActivity.CheckBucketResourceName, mock.Anything, mock.Anything).Return(&common.BucketDetails{}, nil)
	s.env.OnActivity(updateActivity.GenerateResourceNamesForBackupVault, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.ResourceNames{}, nil)
	s.env.OnActivity(updateActivity.CreateBucketForBackupVault, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to create bucket for backup vault"))

	// s.env.OnActivity(updateActivity.UpdateVolumeInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	volume := &datamodel.Volume{
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			}},
		Account: &datamodel.Account{
			Name: "test_account",
		},
		SizeInBytes: 100,
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "test-backup-vault",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			VendorSubnetID: "test-subnet-id",
		},
	}
	params := &common.UpdateVolumeParams{
		QuotaInBytes: 200,
		Region:       "us-west-1",
	}
	s.env.ExecuteWorkflow(UpdateVolumeWorkflow, params, volume)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *VolumeUpdateTestSuite) Test_UpdateVolumeWorkflow_UpdateBucketDetailsOfBackupVaultFailure() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	updateActivity := activities.VolumeUpdateActivity{SE: mockStorage}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInONTAP)
	s.env.RegisterActivity(updateActivity.UpdateLun)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInDB)

	// Mock activities
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(updateActivity.GetVolumeFromONTAP, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			ExternalUUID: "test-external-uuid",
			Name:         "test_volume",
		},
		AvailableSpace: 1000,
		Size:           1000,
		State:          "online",
	}, nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(updateActivity.UpdateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{}, nil)
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("test-token", nil)
	s.env.OnActivity(updateActivity.FindTenancyDetails, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.TenancyInfo{RegionalTenantProject: "tenant-project"}, nil)
	s.env.OnActivity(updateActivity.CheckBackupVaultExistInVCP, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(updateActivity.CheckBucketResourceName, mock.Anything, mock.Anything).Return(&common.BucketDetails{}, nil)
	s.env.OnActivity(updateActivity.GenerateResourceNamesForBackupVault, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.ResourceNames{}, nil)
	s.env.OnActivity(updateActivity.CreateBucketForBackupVault, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.BucketDetails{
		BucketName:          "test-bucket",
		ServiceAccountName:  "test-service-account",
		VendorSubnetID:      "test-subnet-id",
		TenantProjectNumber: "test-project-number",
	}, nil)
	s.env.OnActivity(updateActivity.UpdateBucketDetailsOfBackupVault, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to update bucket details of backup vault"))

	// Execute workflow
	volume := &datamodel.Volume{
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			}},
		Account: &datamodel.Account{
			Name: "test_account",
		},
		SizeInBytes: 100,
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "test-backup-vault",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			VendorSubnetID: "test-subnet-id",
		},
	}
	params := &common.UpdateVolumeParams{
		QuotaInBytes: 200,
		Region:       "us-west-1",
	}
	s.env.ExecuteWorkflow(UpdateVolumeWorkflow, params, volume)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *VolumeUpdateTestSuite) Test_UpdateVolumeWorkflow_AttachBackupPolicyTokenError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	updateActivity := activities.VolumeUpdateActivity{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInONTAP)
	s.env.RegisterActivity(updateActivity.UpdateLun)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInDB)

	// Mock activities
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(updateActivity.GetVolumeFromONTAP, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			ExternalUUID: "test-external-uuid",
			Name:         "test_volume",
		},
		AvailableSpace: 1000,
		Size:           1000,
		State:          "online",
	}, nil)
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("", errors.New("failed to get auth JWT token"))
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	volume := &datamodel.Volume{
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, PoolCredentials: &datamodel.PoolCredentials{
			Password:      "password",
			SecretID:      "",
			CertificateID: "",
		}},
		Account: &datamodel.Account{
			Name: "test_account",
		},
		DataProtection: &datamodel.DataProtection{},
		VolumeAttributes: &datamodel.VolumeAttributes{
			VendorSubnetID: "test-subnet-id",
		},
	}
	backupPolicyId := "test-backup-policy-id"
	params := &common.UpdateVolumeParams{
		Region: "us-west-1",
		DataProtection: &models.UpdateDataProtection{
			BackupPolicyId: &backupPolicyId,
		},
	}
	s.env.ExecuteWorkflow(UpdateVolumeWorkflow, params, volume)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *VolumeUpdateTestSuite) Test_UpdateVolumeWorkflow_VerifyIfBackupPolicyExistsInVCPError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	updateActivity := activities.VolumeUpdateActivity{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInONTAP)
	s.env.RegisterActivity(updateActivity.UpdateLun)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInDB)

	// Mock activities
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(updateActivity.GetVolumeFromONTAP, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			ExternalUUID: "test-external-uuid",
			Name:         "test_volume",
		},
		AvailableSpace: 1000,
		Size:           1000,
		State:          "online",
	}, nil)
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("", nil)
	s.env.OnActivity(updateActivity.VerifyIfBackupPolicyExistsInVCP, mock.Anything, mock.Anything, mock.Anything).Return(false, errors.New("failed to verify if backup policy exists in VCP"))
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	volume := &datamodel.Volume{
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, PoolCredentials: &datamodel.PoolCredentials{
			Password:      "password",
			SecretID:      "",
			CertificateID: "",
		}},
		Account: &datamodel.Account{
			Name: "test_account",
		},
		DataProtection: &datamodel.DataProtection{},
		VolumeAttributes: &datamodel.VolumeAttributes{
			VendorSubnetID: "test-subnet-id",
		},
	}
	backupPolicyId := "test-backup-policy-id"
	params := &common.UpdateVolumeParams{
		Region: "us-west-1",
		DataProtection: &models.UpdateDataProtection{
			BackupPolicyId: &backupPolicyId,
		},
	}
	s.env.ExecuteWorkflow(UpdateVolumeWorkflow, params, volume)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *VolumeUpdateTestSuite) Test_UpdateVolumeWorkflow_FetchAndCreateBackupPolicyFromSDEError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	updateActivity := activities.VolumeUpdateActivity{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInONTAP)
	s.env.RegisterActivity(updateActivity.UpdateLun)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInDB)

	// Mock activities
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(updateActivity.GetVolumeFromONTAP, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			ExternalUUID: "test-external-uuid",
			Name:         "test_volume",
		},
		AvailableSpace: 1000,
		Size:           1000,
		State:          "online",
	}, nil)
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("", nil)
	s.env.OnActivity(updateActivity.VerifyIfBackupPolicyExistsInVCP, mock.Anything, mock.Anything, mock.Anything).Return(false, nil)
	s.env.OnActivity(updateActivity.FetchAndCreateBackupPolicyFromSDE, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to fetch and create backup policy from SDE"))
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	volume := &datamodel.Volume{
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, PoolCredentials: &datamodel.PoolCredentials{
			Password:      "password",
			SecretID:      "",
			CertificateID: "",
		}},
		Account: &datamodel.Account{
			Name: "test_account",
		},
		DataProtection: &datamodel.DataProtection{},
		VolumeAttributes: &datamodel.VolumeAttributes{
			VendorSubnetID: "test-subnet-id",
		},
	}
	backupPolicyId := "test-backup-policy-id"
	params := &common.UpdateVolumeParams{
		Region: "us-west-1",
		DataProtection: &models.UpdateDataProtection{
			BackupPolicyId: &backupPolicyId,
		},
	}
	s.env.ExecuteWorkflow(UpdateVolumeWorkflow, params, volume)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *VolumeUpdateTestSuite) Test_UpdateVolumeWorkflow_CreateScheduleForBackupPolicyError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	updateActivity := activities.VolumeUpdateActivity{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	backupPolicyActivity := activities.BackupPolicyActivity{SE: mockStorage}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInONTAP)
	s.env.RegisterActivity(updateActivity.UpdateLun)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInDB)

	// Mock activities
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(updateActivity.GetVolumeFromONTAP, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			ExternalUUID: "test-external-uuid",
			Name:         "test_volume",
		},
		AvailableSpace: 1000,
		Size:           1000,
		State:          "online",
	}, nil)
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("", nil)
	s.env.OnActivity(updateActivity.VerifyIfBackupPolicyExistsInVCP, mock.Anything, mock.Anything, mock.Anything).Return(false, nil)
	s.env.OnActivity(updateActivity.FetchAndCreateBackupPolicyFromSDE, mock.Anything, mock.Anything, mock.Anything).
		Return(&datamodel.BackupPolicy{BaseModel: datamodel.BaseModel{UUID: "test-backup-policy-uuid"}}, nil)
	s.env.OnActivity(updateActivity.CreateScheduleForBackupPolicy, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to create schedule"))
	s.env.OnActivity(backupPolicyActivity.DeleteBackupPolicyInVCP, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateVolumeStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	volume := &datamodel.Volume{
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, PoolCredentials: &datamodel.PoolCredentials{
			Password:      "password",
			SecretID:      "",
			CertificateID: "",
		}},
		Account: &datamodel.Account{
			Name: "test_account",
		},
		DataProtection: &datamodel.DataProtection{},
		VolumeAttributes: &datamodel.VolumeAttributes{
			VendorSubnetID: "test-subnet-id",
		},
	}
	backupPolicyId := "test-backup-policy-id"
	params := &common.UpdateVolumeParams{
		Region: "us-west-1",
		DataProtection: &models.UpdateDataProtection{
			BackupPolicyId: &backupPolicyId,
		},
	}
	s.env.ExecuteWorkflow(UpdateVolumeWorkflow, params, volume)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *VolumeUpdateTestSuite) Test_UpdateVolumeWorkflow_AttachBackupPolicySuccess() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	updateActivity := activities.VolumeUpdateActivity{SE: mockStorage}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInONTAP)
	s.env.RegisterActivity(updateActivity.UpdateLun)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInDB)

	// Mock activities
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(updateActivity.GetVolumeFromONTAP, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			ExternalUUID: "test-external-uuid",
			Name:         "test_volume",
		},
		AvailableSpace: 1000,
		Size:           1000,
		State:          "online",
	}, nil)
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("", nil)
	s.env.OnActivity(updateActivity.VerifyIfBackupPolicyExistsInVCP, mock.Anything, mock.Anything, mock.Anything).Return(false, nil)
	s.env.OnActivity(updateActivity.FetchAndCreateBackupPolicyFromSDE, mock.Anything, mock.Anything, mock.Anything).Return(&datamodel.BackupPolicy{PolicyEnabled: true}, nil)
	s.env.OnActivity(updateActivity.CreateScheduleForBackupPolicy, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	volume := &datamodel.Volume{
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, PoolCredentials: &datamodel.PoolCredentials{
			Password:      "password",
			SecretID:      "",
			CertificateID: "",
		}},
		Account: &datamodel.Account{
			Name: "test_account",
		},
		DataProtection: &datamodel.DataProtection{},
		VolumeAttributes: &datamodel.VolumeAttributes{
			VendorSubnetID: "test-subnet-id",
		},
	}
	backupPolicyId := "test-backup-policy-id"
	params := &common.UpdateVolumeParams{
		Region: "us-west-1",
		DataProtection: &models.UpdateDataProtection{
			BackupPolicyId: &backupPolicyId,
		},
	}
	s.env.ExecuteWorkflow(UpdateVolumeWorkflow, params, volume)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *VolumeUpdateTestSuite) Test_UpdateVolumeWorkflow_PauseBackupPolicyWhenDisabled() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	updateActivity := activities.VolumeUpdateActivity{SE: mockStorage}
	backupPolicyActivity := activities.BackupPolicyActivity{SE: mockStorage}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInONTAP)
	s.env.RegisterActivity(updateActivity.UpdateLun)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInDB)
	s.env.RegisterActivity(backupPolicyActivity.PauseBackupPolicySchedule)

	// Mock activities
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(updateActivity.GetVolumeFromONTAP, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			ExternalUUID: "test-external-uuid",
			Name:         "test_volume",
		},
		AvailableSpace: 1000,
		Size:           1000,
		State:          "online",
	}, nil)
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("", nil)
	s.env.OnActivity(updateActivity.VerifyIfBackupPolicyExistsInVCP, mock.Anything, mock.Anything, mock.Anything).Return(false, nil)

	// Mock a backup policy with PolicyEnabled = false
	disabledBackupPolicy := &datamodel.BackupPolicy{
		BaseModel: datamodel.BaseModel{
			UUID: "test-backup-policy-uuid",
		},
		Name:          "test-backup-policy",
		PolicyEnabled: false, // This is the key condition for the test
	}

	s.env.OnActivity(updateActivity.FetchAndCreateBackupPolicyFromSDE, mock.Anything, mock.Anything, mock.Anything).Return(disabledBackupPolicy, nil)
	s.env.OnActivity(updateActivity.CreateScheduleForBackupPolicy, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupPolicyActivity.PauseBackupPolicySchedule, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	volume := &datamodel.Volume{
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, PoolCredentials: &datamodel.PoolCredentials{
			Password:      "password",
			SecretID:      "",
			CertificateID: "",
		}},
		Account: &datamodel.Account{
			Name: "test_account",
		},
		DataProtection: &datamodel.DataProtection{},
		VolumeAttributes: &datamodel.VolumeAttributes{
			VendorSubnetID: "test-subnet-id",
		},
	}
	backupPolicyId := "test-backup-policy-id"
	params := &common.UpdateVolumeParams{
		Region: "us-west-1",
		DataProtection: &models.UpdateDataProtection{
			BackupPolicyId: &backupPolicyId,
		},
	}
	s.env.ExecuteWorkflow(UpdateVolumeWorkflow, params, volume)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)

	// Verify that PauseBackupPolicySchedule was called with the disabled backup policy
	s.env.AssertExpectations(s.T())
}

func (s *VolumeUpdateTestSuite) Test_UpdateVolumeWorkflow_PauseBackupPolicyScheduleError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	updateActivity := activities.VolumeUpdateActivity{SE: mockStorage}
	backupPolicyActivity := activities.BackupPolicyActivity{SE: mockStorage}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInONTAP)
	s.env.RegisterActivity(updateActivity.UpdateLun)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInDB)
	s.env.RegisterActivity(backupPolicyActivity.PauseBackupPolicySchedule)

	// Mock activities
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(updateActivity.GetVolumeFromONTAP, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			ExternalUUID: "test-external-uuid",
			Name:         "test_volume",
		},
		AvailableSpace: 1000,
		Size:           1000,
		State:          "online",
	}, nil)
	s.env.OnActivity(commonActivity.GetAuthJWTToken, mock.Anything, mock.Anything).Return("", nil)
	s.env.OnActivity(updateActivity.VerifyIfBackupPolicyExistsInVCP, mock.Anything, mock.Anything, mock.Anything).Return(false, nil)

	// Mock a backup policy with PolicyEnabled = false
	disabledBackupPolicy := &datamodel.BackupPolicy{
		BaseModel: datamodel.BaseModel{
			UUID: "test-backup-policy-uuid",
		},
		Name:          "test-backup-policy",
		PolicyEnabled: false, // This is the key condition for the test
	}

	s.env.OnActivity(updateActivity.FetchAndCreateBackupPolicyFromSDE, mock.Anything, mock.Anything, mock.Anything).Return(disabledBackupPolicy, nil)
	s.env.OnActivity(updateActivity.CreateScheduleForBackupPolicy, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupPolicyActivity.PauseBackupPolicySchedule, mock.Anything, mock.Anything).Return(errors.New("failed to pause backup policy schedule"))
	s.env.OnActivity(backupPolicyActivity.DeleteBackupPolicySchedule, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(backupPolicyActivity.DeleteBackupPolicyInVCP, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	volume := &datamodel.Volume{
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, PoolCredentials: &datamodel.PoolCredentials{
			Password:      "password",
			SecretID:      "",
			CertificateID: "",
		}},
		Account: &datamodel.Account{
			Name: "test_account",
		},
		DataProtection: &datamodel.DataProtection{},
		VolumeAttributes: &datamodel.VolumeAttributes{
			VendorSubnetID: "test-subnet-id",
		},
	}
	backupPolicyId := "test-backup-policy-id"
	params := &common.UpdateVolumeParams{
		Region: "us-west-1",
		DataProtection: &models.UpdateDataProtection{
			BackupPolicyId: &backupPolicyId,
		},
	}
	s.env.ExecuteWorkflow(UpdateVolumeWorkflow, params, volume)

	// Assert workflow failed due to PauseBackupPolicySchedule error
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "failed to pause backup policy schedule")

	// Verify that PauseBackupPolicySchedule was called with the disabled backup policy
	s.env.AssertExpectations(s.T())
}

func (s *VolumeUpdateTestSuite) Test_UpdateVolumeWorkflow_AutoTier() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	updateActivity := activities.VolumeUpdateActivity{SE: mockStorage}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInONTAP)
	s.env.RegisterActivity(updateActivity.UpdateLun)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInDB)

	// Mock activities
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(updateActivity.GetVolumeFromONTAP, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			ExternalUUID: "test-external-uuid",
			Name:         "test_volume",
		},
		AvailableSpace: 100,
		Size:           200,
		State:          "online",
	}, nil)
	s.env.OnActivity(updateActivity.EnsureHostGroupsExistsAndMapDisk, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(updateActivity.UnmapHostGroupFromDisk, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow with no size change
	volume := &datamodel.Volume{
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			}},
		Account: &datamodel.Account{
			Name: "test_account",
		},
		SizeInBytes: 100,
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockProperties: &datamodel.BlockProperties{
				HostGroupDetails: []datamodel.HostGroupDetail{
					{
						HostGroupUUID: "hg-uuid-1",
					},
					{
						HostGroupUUID: "hg-uuid-2",
					},
				},
			},
		},
		AutoTieringEnabled: true,
		AutoTieringPolicy: &datamodel.AutoTieringPolicy{
			CoolingThresholdDays: 5,
		},
	}
	params := &common.UpdateVolumeParams{
		QuotaInBytes: 100,
		BlockProperties: &common.BlockPropertiesRequest{
			HostGroupUUIDs: []string{"hg-uuid-3"},
		},
		AutoTieringPolicy: &common.AutoTieringPolicy{
			AutoTieringEnabled:   true,
			CoolingThresholdDays: 10,
		},
	}
	s.env.ExecuteWorkflow(UpdateVolumeWorkflow, params, volume)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())

	// Run the workflow again with different params
	s.env = s.NewTestWorkflowEnvironment()
	s.env.RegisterWorkflow(UpdateVolumeWorkflow)
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInONTAP)
	s.env.OnActivity(updateActivity.UpdateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(updateActivity.GetVolumeFromONTAP, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			ExternalUUID: "test-external-uuid",
			Name:         "test_volume",
		},
		AvailableSpace: 100,
		Size:           200,
		State:          "online",
	}, nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// New params for second run
	params2 := &common.UpdateVolumeParams{
		QuotaInBytes: 100,
		AutoTieringPolicy: &common.AutoTieringPolicy{
			AutoTieringEnabled: false,
		},
	}
	s.env.ExecuteWorkflow(UpdateVolumeWorkflow, params2, volume)
	assert.True(s.T(), s.env.IsWorkflowCompleted())

	assert.Nil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 4)
}

func TestIsUpdateRequired(t *testing.T) {
	tests := []struct {
		name           string
		response       *vsa.VolumeResponse
		params         *common.UpdateVolumeParams
		existingVolume *datamodel.Volume
		want           bool
	}{
		{
			name: "Size increased - update required",
			response: &vsa.VolumeResponse{
				Size: 100,
			},
			params: &common.UpdateVolumeParams{
				QuotaInBytes: 200,
				AutoTieringPolicy: &common.AutoTieringPolicy{
					AutoTieringEnabled: false,
				},
			},
			existingVolume: &datamodel.Volume{
				AutoTieringEnabled: false,
			},
			want: true,
		},
		{
			name: "Size same - no update required",
			response: &vsa.VolumeResponse{
				Size: 200,
			},
			params: &common.UpdateVolumeParams{
				QuotaInBytes: 200,
				AutoTieringPolicy: &common.AutoTieringPolicy{
					AutoTieringEnabled: false,
				},
			},
			existingVolume: &datamodel.Volume{
				AutoTieringEnabled: false,
			},
			want: false,
		},
		{
			name: "SnapReserve changed - update required",
			response: &vsa.VolumeResponse{
				Size:        150,
				SnapReserve: 10,
			},
			params: &common.UpdateVolumeParams{
				QuotaInBytes: 150,
				SnapReserve:  &[]int64{20}[0],
				AutoTieringPolicy: &common.AutoTieringPolicy{
					AutoTieringEnabled: false,
				},
			},
			existingVolume: &datamodel.Volume{
				AutoTieringEnabled: false,
			},
			want: true,
		},
		{
			name: "SnapReserve same - no update required",
			response: &vsa.VolumeResponse{
				Size:        150,
				SnapReserve: 10,
			},
			params: &common.UpdateVolumeParams{
				QuotaInBytes: 150,
				SnapReserve:  &[]int64{10}[0],
				AutoTieringPolicy: &common.AutoTieringPolicy{
					AutoTieringEnabled: false,
				},
			},
			existingVolume: &datamodel.Volume{
				AutoTieringEnabled: false,
			},
			want: false,
		},
		{
			name: "SnapshotPolicy name changed - update required",
			response: &vsa.VolumeResponse{
				Size:               150,
				SnapshotPolicyName: "old-policy",
			},
			params: &common.UpdateVolumeParams{
				QuotaInBytes:   150,
				SnapshotPolicy: &models.SnapshotPolicy{Name: "new-policy"},
				AutoTieringPolicy: &common.AutoTieringPolicy{
					AutoTieringEnabled: false,
				},
			},
			existingVolume: &datamodel.Volume{
				AutoTieringEnabled: false,
			},
			want: true,
		},
		{
			name: "SnapshotPolicy name same - no update required",
			response: &vsa.VolumeResponse{
				Size:               150,
				SnapshotPolicyName: "policy1",
			},
			params: &common.UpdateVolumeParams{
				QuotaInBytes:   150,
				SnapshotPolicy: &models.SnapshotPolicy{Name: "policy1"},
				AutoTieringPolicy: &common.AutoTieringPolicy{
					AutoTieringEnabled: false,
				},
			},
			existingVolume: &datamodel.Volume{
				AutoTieringEnabled: false,
			},
			want: false,
		},
		{
			name: "AutoTieringEnabled changed from false to true - update required",
			response: &vsa.VolumeResponse{
				Size: 150,
			},
			params: &common.UpdateVolumeParams{
				QuotaInBytes: 150,
				AutoTieringPolicy: &common.AutoTieringPolicy{
					AutoTieringEnabled:   true,
					CoolingThresholdDays: 10,
				},
			},
			existingVolume: &datamodel.Volume{
				AutoTieringEnabled: false,
				AutoTieringPolicy: &datamodel.AutoTieringPolicy{
					CoolingThresholdDays: 5,
				},
			},
			want: true,
		},
		{
			name: "AutoTieringEnabled changed from true to false - update required",
			response: &vsa.VolumeResponse{
				Size: 150,
			},
			params: &common.UpdateVolumeParams{
				QuotaInBytes: 150,
				AutoTieringPolicy: &common.AutoTieringPolicy{
					AutoTieringEnabled: false,
				},
			},
			existingVolume: &datamodel.Volume{
				AutoTieringEnabled: true,
				AutoTieringPolicy: &datamodel.AutoTieringPolicy{
					CoolingThresholdDays: 10,
				},
			},
			want: true,
		},
		{
			name: "CoolnessPeriod changed when AutoTieringEnabled is true - update required",
			response: &vsa.VolumeResponse{
				Size: 150,
			},
			params: &common.UpdateVolumeParams{
				QuotaInBytes: 150,
				AutoTieringPolicy: &common.AutoTieringPolicy{
					AutoTieringEnabled:   true,
					CoolingThresholdDays: 15,
				},
			},
			existingVolume: &datamodel.Volume{
				AutoTieringEnabled: true,
				AutoTieringPolicy:  &datamodel.AutoTieringPolicy{CoolingThresholdDays: 10},
			},
			want: true,
		},
		{
			name: "CoolnessPeriod changed when AutoTieringEnabled is false - no update required",
			response: &vsa.VolumeResponse{
				Size: 150,
			},
			params: &common.UpdateVolumeParams{
				QuotaInBytes: 150,
				AutoTieringPolicy: &common.AutoTieringPolicy{
					AutoTieringEnabled:   false,
					CoolingThresholdDays: 15,
				},
			},
			existingVolume: &datamodel.Volume{
				AutoTieringEnabled: false,
				AutoTieringPolicy:  &datamodel.AutoTieringPolicy{CoolingThresholdDays: 10},
			},
			want: false,
		},
		{
			name: "No changes - no update required",
			response: &vsa.VolumeResponse{
				Size:               150,
				SnapReserve:        10,
				SnapshotPolicyName: "policy1",
			},
			params: &common.UpdateVolumeParams{
				QuotaInBytes:   150,
				SnapReserve:    &[]int64{10}[0],
				SnapshotPolicy: &models.SnapshotPolicy{Name: "policy1"},
				AutoTieringPolicy: &common.AutoTieringPolicy{
					AutoTieringEnabled:   true,
					CoolingThresholdDays: 10,
				},
			},
			existingVolume: &datamodel.Volume{
				AutoTieringEnabled: true,
				AutoTieringPolicy:  &datamodel.AutoTieringPolicy{CoolingThresholdDays: 10},
			},
			want: false,
		},
		{
			name: "Multiple changes - update required",
			response: &vsa.VolumeResponse{
				Size:               100,
				SnapReserve:        5,
				SnapshotPolicyName: "old-policy",
			},
			params: &common.UpdateVolumeParams{
				QuotaInBytes:   200,
				SnapReserve:    &[]int64{15}[0],
				SnapshotPolicy: &models.SnapshotPolicy{Name: "new-policy"},
				AutoTieringPolicy: &common.AutoTieringPolicy{
					AutoTieringEnabled:   true,
					CoolingThresholdDays: 20,
				},
			},
			existingVolume: &datamodel.Volume{
				AutoTieringEnabled: false,
				AutoTieringPolicy:  &datamodel.AutoTieringPolicy{CoolingThresholdDays: 10},
			},
			want: true,
		},
		{
			name: "No SnapReserve or SnapshotPolicy in params - no update required",
			response: &vsa.VolumeResponse{
				Size:               150,
				SnapReserve:        5,
				SnapshotPolicyName: "policy1",
			},
			params: &common.UpdateVolumeParams{
				QuotaInBytes: 150,
				AutoTieringPolicy: &common.AutoTieringPolicy{
					AutoTieringEnabled: false,
				},
			},
			existingVolume: &datamodel.Volume{
				AutoTieringEnabled: false,
			},
			want: false,
		},
		{
			name: "Size decreased but params has changes - no update required for size",
			response: &vsa.VolumeResponse{
				Size: 200,
			},
			params: &common.UpdateVolumeParams{
				QuotaInBytes: 100,
				AutoTieringPolicy: &common.AutoTieringPolicy{
					AutoTieringEnabled: false,
				},
			},
			existingVolume: &datamodel.Volume{
				AutoTieringEnabled: false,
			},
			want: false,
		},
		{
			name: "Size is same and no tiering policy passed - no update required",
			response: &vsa.VolumeResponse{
				Size: 200,
			},
			params: &common.UpdateVolumeParams{
				QuotaInBytes: 200,
			},
			existingVolume: &datamodel.Volume{},
			want:           false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isUpdateRequired(tt.response, tt.params, tt.existingVolume)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestGetUpdateParamsForRollback(t *testing.T) {
	tests := []struct {
		name           string
		volResponse    *vsa.VolumeResponse
		existingVolume *datamodel.Volume
		expected       *common.UpdateVolumeParams
	}{
		{
			name: "Volume with AutoTieringPolicy - all fields populated",
			volResponse: &vsa.VolumeResponse{
				Size: 1024 * 1024 * 1024, // 1GB
			},
			existingVolume: &datamodel.Volume{
				AutoTieringEnabled: true,
				AutoTieringPolicy: &datamodel.AutoTieringPolicy{
					CoolingThresholdDays: 30,
					TieringPolicy:        "auto",
					RetrievalPolicy:      "default",
				},
			},
			expected: &common.UpdateVolumeParams{
				QuotaInBytes: 1024 * 1024 * 1024,
				AutoTieringPolicy: &common.AutoTieringPolicy{
					AutoTieringEnabled:   true,
					CoolingThresholdDays: 30,
					TieringPolicy:        "auto",
					RetrievalPolicy:      "default",
				},
			},
		},
		{
			name: "Volume with AutoTieringPolicy - AutoTieringEnabled false",
			volResponse: &vsa.VolumeResponse{
				Size: 2048 * 1024 * 1024, // 2GB
			},
			existingVolume: &datamodel.Volume{
				AutoTieringEnabled: false,
				AutoTieringPolicy: &datamodel.AutoTieringPolicy{
					CoolingThresholdDays: 15,
					TieringPolicy:        "none",
					RetrievalPolicy:      "never",
				},
			},
			expected: &common.UpdateVolumeParams{
				QuotaInBytes: 2048 * 1024 * 1024,
				AutoTieringPolicy: &common.AutoTieringPolicy{
					AutoTieringEnabled:   false,
					CoolingThresholdDays: 15,
					TieringPolicy:        "none",
					RetrievalPolicy:      "never",
				},
			},
		},
		{
			name: "Volume with AutoTieringPolicy - zero cooling threshold",
			volResponse: &vsa.VolumeResponse{
				Size: 512 * 1024 * 1024, // 512MB
			},
			existingVolume: &datamodel.Volume{
				AutoTieringEnabled: true,
				AutoTieringPolicy: &datamodel.AutoTieringPolicy{
					CoolingThresholdDays: 0,
					TieringPolicy:        "snapshot-only",
					RetrievalPolicy:      "on-read",
				},
			},
			expected: &common.UpdateVolumeParams{
				QuotaInBytes: 512 * 1024 * 1024,
				AutoTieringPolicy: &common.AutoTieringPolicy{
					AutoTieringEnabled:   true,
					CoolingThresholdDays: 0,
					TieringPolicy:        "snapshot-only",
					RetrievalPolicy:      "on-read",
				},
			},
		},
		{
			name: "Volume with AutoTieringPolicy - empty string policies",
			volResponse: &vsa.VolumeResponse{
				Size: 4096 * 1024 * 1024, // 4GB
			},
			existingVolume: &datamodel.Volume{
				AutoTieringEnabled: true,
				AutoTieringPolicy: &datamodel.AutoTieringPolicy{
					CoolingThresholdDays: 90,
					TieringPolicy:        "",
					RetrievalPolicy:      "",
				},
			},
			expected: &common.UpdateVolumeParams{
				QuotaInBytes: 4096 * 1024 * 1024,
				AutoTieringPolicy: &common.AutoTieringPolicy{
					AutoTieringEnabled:   true,
					CoolingThresholdDays: 90,
					TieringPolicy:        "",
					RetrievalPolicy:      "",
				},
			},
		},
		{
			name: "Volume without AutoTieringPolicy - nil policy",
			volResponse: &vsa.VolumeResponse{
				Size: 1024 * 1024 * 1024, // 1GB
			},
			existingVolume: &datamodel.Volume{
				AutoTieringEnabled: false,
				AutoTieringPolicy:  nil,
			},
			expected: &common.UpdateVolumeParams{
				QuotaInBytes: 1024 * 1024 * 1024,
				// AutoTieringPolicy should be nil
			},
		},
		{
			name: "Volume with AutoTieringEnabled but nil AutoTieringPolicy",
			volResponse: &vsa.VolumeResponse{
				Size: 2048 * 1024 * 1024, // 2GB
			},
			existingVolume: &datamodel.Volume{
				AutoTieringEnabled: true,
				AutoTieringPolicy:  nil,
			},
			expected: &common.UpdateVolumeParams{
				QuotaInBytes: 2048 * 1024 * 1024,
				// AutoTieringPolicy should be nil even though AutoTieringEnabled is true
			},
		},
		{
			name: "Volume with cooling threshold days",
			volResponse: &vsa.VolumeResponse{
				Size: 8192 * 1024 * 1024, // 8GB
			},
			existingVolume: &datamodel.Volume{
				AutoTieringEnabled: true,
				AutoTieringPolicy: &datamodel.AutoTieringPolicy{
					CoolingThresholdDays: 183, // Maximum allowed days
					TieringPolicy:        "auto",
					RetrievalPolicy:      "default",
				},
			},
			expected: &common.UpdateVolumeParams{
				QuotaInBytes: 8192 * 1024 * 1024,
				AutoTieringPolicy: &common.AutoTieringPolicy{
					AutoTieringEnabled:   true,
					CoolingThresholdDays: 183,
					TieringPolicy:        "auto",
					RetrievalPolicy:      "default",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getUpdateParamsForRollback(tt.volResponse, tt.existingVolume)

			// Assert basic fields
			assert.Equal(t, tt.expected.QuotaInBytes, result.QuotaInBytes)

			// Assert AutoTieringPolicy
			if tt.expected.AutoTieringPolicy == nil {
				assert.Nil(t, result.AutoTieringPolicy)
			} else {
				assert.NotNil(t, result.AutoTieringPolicy)
				assert.Equal(t, tt.expected.AutoTieringPolicy.AutoTieringEnabled, result.AutoTieringPolicy.AutoTieringEnabled)
				assert.Equal(t, tt.expected.AutoTieringPolicy.CoolingThresholdDays, result.AutoTieringPolicy.CoolingThresholdDays)
				assert.Equal(t, tt.expected.AutoTieringPolicy.TieringPolicy, result.AutoTieringPolicy.TieringPolicy)
				assert.Equal(t, tt.expected.AutoTieringPolicy.RetrievalPolicy, result.AutoTieringPolicy.RetrievalPolicy)
			}
		})
	}
}

func (s *VolumeUpdateTestSuite) Test_UpdateVolumeWorkflow_UpdateJobStatusProcessingError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)

	// Mock UpdateJobStatus to return error for PROCESSING state
	expectedError := errors.New("failed to update job status to PROCESSING")
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.MatchedBy(func(job *datamodel.Job) bool {
		return job.State == string(models.JobsStatePROCESSING)
	})).Return(expectedError)

	// Execute workflow
	volume := &datamodel.Volume{
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, PoolCredentials: &datamodel.PoolCredentials{
			Password:      "password",
			SecretID:      "",
			CertificateID: "",
		}},
		Account: &datamodel.Account{
			Name: "test_account",
		},
		SizeInBytes: 1000,
	}
	params := &common.UpdateVolumeParams{
		QuotaInBytes: 2000,
	}
	s.env.ExecuteWorkflow(UpdateVolumeWorkflow, params, volume)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), expectedError.Error())
}

func (s *VolumeUpdateTestSuite) Test_UpdateVolumeWorkflow_UpdateJobStatusDoneError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	updateActivity := activities.VolumeUpdateActivity{SE: mockStorage}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInONTAP)
	s.env.RegisterActivity(updateActivity.UpdateLun)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInDB)

	// Mock activities
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(updateActivity.GetVolumeFromONTAP, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			ExternalUUID: "test-external-uuid",
			Name:         "test_volume",
		},
		AvailableSpace: 1000,
		Size:           1000,
		State:          "online",
	}, nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(updateActivity.UpdateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.LunResponse{}, nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil)

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
	volume := &datamodel.Volume{
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, PoolCredentials: &datamodel.PoolCredentials{
			Password:      "password",
			SecretID:      "",
			CertificateID: "",
		}},
		Account: &datamodel.Account{
			Name: "test_account",
		},
		SizeInBytes: 1000,
		VolumeAttributes: &datamodel.VolumeAttributes{
			IsDataProtection: false,
		},
	}
	params := &common.UpdateVolumeParams{
		QuotaInBytes: 2000,
	}
	s.env.ExecuteWorkflow(UpdateVolumeWorkflow, params, volume)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), expectedError.Error())
}

func (s *VolumeUpdateTestSuite) Test_UpdateVolumeWorkflow_UpdateJobStatusErrorDetailsError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	updateActivity := activities.VolumeUpdateActivity{SE: mockStorage}

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInONTAP)

	// Mock activities
	updateVolError := errors.New("failed to get hosts from ONTAP")
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(updateActivity.GetVolumeFromONTAP, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			ExternalUUID: "test-external-uuid",
			Name:         "test_volume",
		},
		AvailableSpace: 1000,
		Size:           1000,
		State:          "online",
	}, nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(updateVolError)

	// Mock UpdateJobStatus to succeed for PROCESSING state
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.MatchedBy(func(job *datamodel.Job) bool {
		return job.State == string(models.JobsStatePROCESSING)
	})).Return(nil)

	// Mock UpdateJobStatus to fail for DONE state with error details
	errorDetailsUpdateError := errors.New("failed to update job status with error details")
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.MatchedBy(func(job *datamodel.Job) bool {
		return job.State == string(models.JobsStateERROR) && job.ErrorDetails != ""
	})).Return(errorDetailsUpdateError)

	// Execute workflow
	volume := &datamodel.Volume{
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, PoolCredentials: &datamodel.PoolCredentials{
			Password:      "password",
			SecretID:      "",
			CertificateID: "",
		}},
		Account: &datamodel.Account{
			Name: "test_account",
		},
		SizeInBytes: 1000,
	}
	params := &common.UpdateVolumeParams{
		QuotaInBytes: 2000,
	}
	s.env.ExecuteWorkflow(UpdateVolumeWorkflow, params, volume)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), errorDetailsUpdateError.Error())
}
