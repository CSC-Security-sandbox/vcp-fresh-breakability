package activities

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/backup_policy"
	cvpmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/scheduler"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"go.temporal.io/sdk/temporal"
)

func TestDeleteBackupPolicyInSDE(t *testing.T) {
	t.Run("DeleteBackupPolicyInSDE", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		mockScheduler := scheduler.NewMockScheduler(tt)
		mockClient := backup_policy.NewMockClientService(tt)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}

		originalCreateClient := cvpCreateClient
		defer func() { cvpCreateClient = originalCreateClient }()
		cvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		mockClient.On("V1betaDeleteBackupPolicy", mock.Anything).Return(
			&backup_policy.V1betaDeleteBackupPolicyAccepted{
				Payload: &cvpmodels.OperationV1beta{
					Done: nillable.ToPointer(true),
				},
			}, nil, nil)

		params := &common.DeleteBackupPolicyParams{
			Name:           "test-backup-policy",
			OwnerID:        "test-owner-id",
			BackupPolicyID: "test-backup-policy-uuid",
			LocationID:     "test-location",
		}

		backupPolicyActivity := BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}
		err := backupPolicyActivity.DeleteBackupPolicyInSDE(context.Background(), params)
		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
		mockScheduler.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})
	t.Run("DeleteBackupPolicyInSDEFailsWhenSDEThrowsError", func(tt *testing.T) {
		params := &common.DeleteBackupPolicyParams{
			Name:           "test-backup-policy",
			OwnerID:        "test-owner-id",
			BackupPolicyID: "test-backup-policy-uuid",
			LocationID:     "test-location",
		}

		errorCases := []struct {
			name     string
			err      error
			wantType string
			wantMsg  string
		}{
			{
				name:     "BadRequest",
				err:      &backup_policy.V1betaDeleteBackupPolicyBadRequest{},
				wantType: "V1betaDeleteBackupPolicyBadRequest",
				wantMsg:  "Bad request deleting backup policy",
			},
			{
				name:     "Unauthorized",
				err:      &backup_policy.V1betaDeleteBackupPolicyUnauthorized{},
				wantType: "V1betaDeleteBackupPolicyUnauthorized",
				wantMsg:  "Unauthorized to delete backup policy",
			},
			{
				name:     "Forbidden",
				err:      &backup_policy.V1betaDeleteBackupPolicyForbidden{},
				wantType: "V1betaDeleteBackupPolicyForbidden",
				wantMsg:  "Forbidden to delete backup policy",
			},
			{
				name:     "NotFound",
				err:      &backup_policy.V1betaDeleteBackupPolicyNotFound{},
				wantType: "V1betaDeleteBackupPolicyNotFound",
				wantMsg:  "Backup policy test-backup-policy-uuid not found",
			},
			{
				name:     "Conflict",
				err:      &backup_policy.V1betaDeleteBackupPolicyConflict{},
				wantType: "V1betaDeleteBackupPolicyConflict",
				wantMsg:  "Conflict deleting backup policy",
			},
			{
				name:     "InternalServerError",
				err:      &backup_policy.V1betaDeleteBackupPolicyInternalServerError{},
				wantType: "V1betaDeleteBackupPolicyInternalServerError",
				wantMsg:  "Internal server error deleting backup policy",
			},
			{
				name:     "NotImplemented",
				err:      &backup_policy.V1betaDeleteBackupPolicyNotImplemented{},
				wantType: "V1betaDeleteBackupPolicyNotImplemented",
				wantMsg:  "Not implemented delete backup policy",
			},
			{
				name:     "UnknownError",
				err:      errors.New("some unknown error"),
				wantType: "",
				wantMsg:  "some unknown error",
			},
		}

		for _, tc := range errorCases {
			t.Run(tc.name, func(t *testing.T) {
				mockStorage := database.NewMockStorage(t)
				mockScheduler := scheduler.NewMockScheduler(t)
				mockClient := backup_policy.NewMockClientService(t)
				cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
				originalCreateClient := cvpCreateClient
				defer func() { cvpCreateClient = originalCreateClient }()
				cvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
					return *cvpClient
				}
				mockClient.On("V1betaDeleteBackupPolicy", mock.Anything).Return(nil, nil, tc.err)

				backupPolicyActivity := BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}
				err := backupPolicyActivity.DeleteBackupPolicyInSDE(context.Background(), params)
				if tc.wantType == "" {
					assert.Error(t, err)
					assert.Equal(t, tc.wantMsg, err.Error())
				} else {
					appErr, ok := err.(*temporal.ApplicationError)
					assert.True(t, ok)
					assert.Contains(t, appErr.Error(), tc.wantMsg)
					assert.Equal(t, tc.wantType, appErr.Type())
				}
				mockClient.AssertExpectations(t)
			})
		}
	})
	t.Run("DeleteBackupPolicyInSDEFailsOnInvalidResponse", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		mockScheduler := scheduler.NewMockScheduler(tt)
		mockClient := backup_policy.NewMockClientService(tt)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}

		originalCreateClient := cvpCreateClient
		defer func() { cvpCreateClient = originalCreateClient }()
		cvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		mockClient.On("V1betaDeleteBackupPolicy", mock.Anything).Return(
			&backup_policy.V1betaDeleteBackupPolicyAccepted{
				Payload: &cvpmodels.OperationV1beta{},
			}, nil, nil)

		params := &common.DeleteBackupPolicyParams{
			Name:           "test-backup-policy",
			OwnerID:        "test-owner-id",
			BackupPolicyID: "test-backup-policy-uuid",
			LocationID:     "test-location",
		}

		backupPolicyActivity := BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}
		err := backupPolicyActivity.DeleteBackupPolicyInSDE(context.Background(), params)
		assert.Error(tt, err)
		assert.Equal(tt, "unknown error during delete backup policy in SDE", err.Error())
		mockStorage.AssertExpectations(tt)
		mockScheduler.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})
}

func TestDeleteBackupPolicySchedule(t *testing.T) {
	t.Run("DeleteBackupPolicySchedule", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		mockScheduler := scheduler.NewMockScheduler(tt)

		backupPolicyActivity := BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}
		mockScheduler.On("Delete", mock.Anything, mock.Anything).Return(&scheduler.ScheduleResponse{}, nil)

		err := backupPolicyActivity.DeleteBackupPolicySchedule(context.Background(), "test-backup-policy-uuid")
		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
		mockScheduler.AssertExpectations(tt)
	})
	t.Run("DeleteBackupPolicyScheduleFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		mockScheduler := scheduler.NewMockScheduler(tt)

		backupPolicyActivity := BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}
		mockScheduler.On("Delete", mock.Anything, mock.Anything).Return(nil, errors.New("scheduler error"))

		err := backupPolicyActivity.DeleteBackupPolicySchedule(context.Background(), "test-backup-policy-uuid")
		assert.Error(tt, err)
		assert.Equal(tt, "scheduler error", err.Error())
		mockStorage.AssertExpectations(tt)
		mockScheduler.AssertExpectations(tt)
	})
}

func TestDeleteBackupPolicyInVCP(t *testing.T) {
	t.Run("DeleteBackupPolicyInVCP", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)

		backupPolicyActivity := BackupPolicyActivity{SE: mockStorage}
		mockStorage.On("DeleteBackupPolicy", mock.Anything, "test-backup-policy-uuid").Return(&datamodel.BackupPolicy{}, nil)

		res, err := backupPolicyActivity.DeleteBackupPolicyInVCP(context.Background(), "test-backup-policy-uuid")
		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		mockStorage.AssertExpectations(tt)
	})
	t.Run("DeleteBackupPolicyInVCPFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)

		backupPolicyActivity := BackupPolicyActivity{SE: mockStorage}
		mockStorage.On("DeleteBackupPolicy", mock.Anything, "test-backup-policy-uuid").Return(nil, errors.New("internal server error"))

		res, err := backupPolicyActivity.DeleteBackupPolicyInVCP(context.Background(), "test-backup-policy-uuid")
		assert.Error(tt, err)
		assert.Nil(tt, res)
		assert.Equal(tt, "internal server error", err.Error())
		mockStorage.AssertExpectations(tt)
	})
}

func TestConvertsValidBackupPolicyV1betaToDataModel(tt *testing.T) {
	tt.Run("ConvertsValidBackupPolicyV1betaToDataModel", func(t *testing.T) {
		createdAt := strfmt.DateTime(time.Now())
		name := "test-backup-policy"
		dailyLimit := int64(5)
		weeklyLimit := int64(3)
		monthlyLimit := int64(2)
		description := "This is a test backup policy"
		policyEnabled := true
		backupPolicyUUID := "uuid-123"
		volumeCount := int64(0)
		backupPolicy := &cvpmodels.BackupPolicyDetailsV1beta{
			BackupPolicyV1beta: cvpmodels.BackupPolicyV1beta{
				ResourceID:  &name,
				CreatedAt:   &createdAt,
				Description: &description,
				BackupPolicyScheduleV1beta: cvpmodels.BackupPolicyScheduleV1beta{
					DailyBackupLimit:   &dailyLimit,
					WeeklyBackupLimit:  &weeklyLimit,
					MonthlyBackupLimit: &monthlyLimit,
				},
				Enabled:        &policyEnabled,
				BackupPolicyID: backupPolicyUUID,
				State:          "READY",
				VolumeCount:    &volumeCount,
			},
		}
		expected := &datamodel.BackupPolicy{
			BaseModel: datamodel.BaseModel{
				UUID:      backupPolicyUUID,
				CreatedAt: time.Time(createdAt),
			},
			Name:                  name,
			Description:           &description,
			DailyBackupsToKeep:    dailyLimit,
			WeeklyBackupsToKeep:   weeklyLimit,
			MonthlyBackupsToKeep:  monthlyLimit,
			PolicyEnabled:         policyEnabled,
			LifeCycleState:        "READY",
			LifeCycleStateDetails: models.LifeCycleStateAvailableDetails,
		}
		res := convertToBackupPolicyDataModel(backupPolicy)
		assert.Equal(t, res, expected)
	})
	tt.Run("ConvertBackupPolicyWithNilFieldsToDataModel", func(t *testing.T) {
		backupPolicy := &cvpmodels.BackupPolicyDetailsV1beta{}
		expected := &datamodel.BackupPolicy{
			BaseModel: datamodel.BaseModel{
				UUID:      "",
				CreatedAt: time.Time{},
				UpdatedAt: time.Time{},
				DeletedAt: nil,
			},
			Name:                  "",
			Description:           nil,
			DailyBackupsToKeep:    0,
			WeeklyBackupsToKeep:   0,
			MonthlyBackupsToKeep:  0,
			PolicyEnabled:         false,
			LifeCycleState:        "",
			LifeCycleStateDetails: "",
		}
		res := convertToBackupPolicyDataModel(backupPolicy)
		assert.Equal(t, res, expected)
	})
}

func TestUpdateBackupPolicyInSDE(t *testing.T) {
	t.Run("UpdateBackupPolicyInSDESucceeds", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(t)
		mockScheduler := scheduler.NewMockScheduler(t)
		mockClient := backup_policy.NewMockClientService(t)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}

		originalCreateClient := cvpCreateClient
		defer func() { cvpCreateClient = originalCreateClient }()
		cvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		expected := &cvpmodels.BackupPolicyV1beta{
			BackupPolicyScheduleV1beta: cvpmodels.BackupPolicyScheduleV1beta{
				DailyBackupLimit:   nillable.ToPointer(int64(5)),
				WeeklyBackupLimit:  nillable.ToPointer(int64(3)),
				MonthlyBackupLimit: nillable.ToPointer(int64(2)),
			},
			BackupPolicyID: "test-backup-policy-uuid",
			CreatedAt:      nillable.ToPointer(strfmt.DateTime(time.Now())),
			Description:    nillable.ToPointer("This is a test backup policy"),
			Enabled:        nillable.ToPointer(true),
			ResourceID:     nillable.ToPointer("test-backup-policy"),
			State:          models.LifeCycleStateREADY,
			VolumeCount:    nillable.ToPointer(int64(2)),
		}
		mockClient.On("V1betaUpdateBackupPolicy", mock.Anything).Return(
			&backup_policy.V1betaUpdateBackupPolicyAccepted{
				Payload: &cvpmodels.OperationV1beta{
					Done:     nillable.ToPointer(true),
					Response: expected,
				},
			}, nil, nil)

		params := &common.UpdateBackupPolicyParams{
			Name:               "test-backup-policy",
			AccountName:        "test-account",
			BackupPolicyID:     "test-backup-policy-uuid",
			LocationID:         "test-location",
			Description:        nillable.ToPointer("This is a test backup policy"),
			PolicyEnabled:      nillable.ToPointer(true),
			DailyBackupLimit:   nillable.ToPointer(int64(5)),
			WeeklyBackupLimit:  nillable.ToPointer(int64(3)),
			MonthlyBackupLimit: nillable.ToPointer(int64(2)),
		}

		backupPolicyActivity := BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}
		updatedBackupPolicy, err := backupPolicyActivity.UpdateBackupPolicyInSDE(context.Background(), params)
		assert.NoError(t, err)
		assert.NotNil(t, updatedBackupPolicy)
		assert.Equal(t, *expected.Description, *updatedBackupPolicy.Description)
		assert.Equal(t, *expected.Enabled, *updatedBackupPolicy.Enabled)
		assert.Equal(t, *expected.DailyBackupLimit, *updatedBackupPolicy.DailyBackupLimit)
		assert.Equal(t, *expected.WeeklyBackupLimit, *updatedBackupPolicy.WeeklyBackupLimit)
		assert.Equal(t, *expected.MonthlyBackupLimit, *updatedBackupPolicy.MonthlyBackupLimit)
		mockStorage.AssertExpectations(t)
		mockScheduler.AssertExpectations(t)
		mockClient.AssertExpectations(t)
	})

	t.Run("UpdateBackupPolicyInSDEFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(t)
		mockScheduler := scheduler.NewMockScheduler(t)
		mockClient := backup_policy.NewMockClientService(t)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}

		originalCreateClient := cvpCreateClient
		defer func() { cvpCreateClient = originalCreateClient }()
		cvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		mockClient.On("V1betaUpdateBackupPolicy", mock.Anything).Return(
			nil, nil, errors.New("could not update backup policy in SDE"))

		params := &common.UpdateBackupPolicyParams{
			Name:               "test-backup-policy",
			AccountName:        "test-account",
			BackupPolicyID:     "test-backup-policy-uuid",
			LocationID:         "test-location",
			Description:        nillable.ToPointer("This is a test backup policy"),
			PolicyEnabled:      nillable.ToPointer(true),
			DailyBackupLimit:   nillable.ToPointer(int64(5)),
			WeeklyBackupLimit:  nillable.ToPointer(int64(3)),
			MonthlyBackupLimit: nillable.ToPointer(int64(2)),
		}

		backupPolicyActivity := BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}
		updatedBackupPolicy, err := backupPolicyActivity.UpdateBackupPolicyInSDE(context.Background(), params)
		assert.Error(t, err)
		assert.Nil(t, updatedBackupPolicy)
		assert.Equal(tt, "could not update backup policy in SDE", err.Error())
		mockStorage.AssertExpectations(t)
		mockScheduler.AssertExpectations(t)
		mockClient.AssertExpectations(t)
	})
}

func TestRevertBackupPolicyUpdateInSDE(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	mockScheduler := scheduler.NewMockScheduler(t)
	mockClient := backup_policy.NewMockClientService(t)
	cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}

	originalCreateClient := cvpCreateClient
	defer func() { cvpCreateClient = originalCreateClient }()
	cvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
		return *cvpClient
	}

	expected := &cvpmodels.BackupPolicyV1beta{
		BackupPolicyScheduleV1beta: cvpmodels.BackupPolicyScheduleV1beta{
			DailyBackupLimit:   nillable.ToPointer(int64(2)),
			WeeklyBackupLimit:  nillable.ToPointer(int64(2)),
			MonthlyBackupLimit: nillable.ToPointer(int64(2)),
		},
		BackupPolicyID: "test-backup-policy-uuid",
		CreatedAt:      nillable.ToPointer(strfmt.DateTime(time.Now())),
		Description:    nil,
		Enabled:        nillable.ToPointer(false),
		ResourceID:     nillable.ToPointer("test-backup-policy"),
		State:          models.LifeCycleStateREADY,
		VolumeCount:    nillable.ToPointer(int64(2)),
	}
	mockClient.On("V1betaUpdateBackupPolicy", mock.Anything).Return(
		&backup_policy.V1betaUpdateBackupPolicyAccepted{
			Payload: &cvpmodels.OperationV1beta{
				Done:     nillable.ToPointer(true),
				Response: expected,
			},
		}, nil, nil)

	params := &common.UpdateBackupPolicyParams{
		Name:               "test-backup-policy",
		AccountName:        "test-account",
		BackupPolicyID:     "test-backup-policy-uuid",
		LocationID:         "test-location",
		Description:        nillable.ToPointer("This is a test backup policy"),
		PolicyEnabled:      nillable.ToPointer(true),
		DailyBackupLimit:   nillable.ToPointer(int64(5)),
		WeeklyBackupLimit:  nillable.ToPointer(int64(3)),
		MonthlyBackupLimit: nillable.ToPointer(int64(2)),
	}

	dbBackupPolicy := &datamodel.BackupPolicy{
		BaseModel: datamodel.BaseModel{
			ID:   int64(1),
			UUID: "test-backup-policy-uuid",
		},
		Name: "test-backup-policy",
		Account: &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID: int64(1),
			},
		},
		AccountID:             1,
		Description:           nil,
		DailyBackupsToKeep:    2,
		WeeklyBackupsToKeep:   2,
		MonthlyBackupsToKeep:  2,
		PolicyEnabled:         false,
		LifeCycleState:        models.LifeCycleStateREADY,
		LifeCycleStateDetails: models.LifeCycleStateReadyDetails,
	}

	backupPolicyActivity := BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}
	revertedBackupPolicy, err := backupPolicyActivity.RevertBackupPolicyUpdateInSDE(context.Background(), params, dbBackupPolicy)
	assert.NoError(t, err)
	assert.NotNil(t, revertedBackupPolicy)
	assert.Nil(t, revertedBackupPolicy.Description)
	assert.Equal(t, *expected.Enabled, *revertedBackupPolicy.Enabled)
	assert.Equal(t, *expected.DailyBackupLimit, *revertedBackupPolicy.DailyBackupLimit)
	assert.Equal(t, *expected.WeeklyBackupLimit, *revertedBackupPolicy.WeeklyBackupLimit)
	assert.Equal(t, *expected.MonthlyBackupLimit, *revertedBackupPolicy.MonthlyBackupLimit)
	mockStorage.AssertExpectations(t)
	mockScheduler.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestUpdateBackupPolicyInVCP(t *testing.T) {
	t.Run("UpdateBackupPolicyInVCPSucceeds", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		mockScheduler := scheduler.NewMockScheduler(tt)

		expected := &datamodel.BackupPolicy{
			BaseModel: datamodel.BaseModel{
				ID:   int64(1),
				UUID: "test-backup-policy-uuid",
			},
			Name:                  "test-backup-policy",
			Account:               &datamodel.Account{BaseModel: datamodel.BaseModel{ID: int64(1)}},
			AccountID:             1,
			Description:           nillable.ToPointer("This is a test backup policy"),
			DailyBackupsToKeep:    5,
			WeeklyBackupsToKeep:   3,
			MonthlyBackupsToKeep:  2,
			PolicyEnabled:         true,
			LifeCycleState:        models.LifeCycleStateREADY,
			LifeCycleStateDetails: models.LifeCycleStateReadyDetails,
		}
		mockStorage.On("UpdateBackupPolicy", mock.Anything, mock.Anything, mock.Anything).Return(expected, nil)

		params := &common.UpdateBackupPolicyParams{
			Name:               "test-backup-policy",
			AccountName:        "test-account",
			BackupPolicyID:     "test-backup-policy-uuid",
			LocationID:         "test-location",
			Description:        nillable.ToPointer("This is a test backup policy"),
			PolicyEnabled:      nillable.ToPointer(true),
			DailyBackupLimit:   nillable.ToPointer(int64(5)),
			WeeklyBackupLimit:  nillable.ToPointer(int64(3)),
			MonthlyBackupLimit: nillable.ToPointer(int64(2)),
		}

		backupPolicy := &datamodel.BackupPolicy{
			BaseModel: datamodel.BaseModel{
				ID:   int64(1),
				UUID: "test-backup-policy-uuid",
			},
		}

		backupPolicyActivity := BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}
		updated, err := backupPolicyActivity.UpdateBackupPolicyInVCP(context.Background(), params, backupPolicy)
		assert.NoError(t, err)
		assert.NotNil(t, updated)
		assert.Equal(tt, expected.Description, updated.Description)
		assert.Equal(tt, expected.PolicyEnabled, updated.PolicyEnabled)
		assert.Equal(tt, expected.DailyBackupsToKeep, updated.DailyBackupsToKeep)
		assert.Equal(tt, expected.WeeklyBackupsToKeep, updated.WeeklyBackupsToKeep)
		assert.Equal(tt, expected.MonthlyBackupsToKeep, updated.MonthlyBackupsToKeep)
	})

	t.Run("UpdateBackupPolicyInVCPFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		mockScheduler := scheduler.NewMockScheduler(tt)

		mockStorage.On("UpdateBackupPolicy", mock.Anything, mock.Anything, mock.Anything).Return(
			nil, errors.New("could not update backup policy in VCP"))

		params := &common.UpdateBackupPolicyParams{
			Name:               "test-backup-policy",
			AccountName:        "test-account",
			BackupPolicyID:     "test-backup-policy-uuid",
			LocationID:         "test-location",
			Description:        nillable.ToPointer("This is a test backup policy"),
			PolicyEnabled:      nillable.ToPointer(true),
			DailyBackupLimit:   nillable.ToPointer(int64(5)),
			WeeklyBackupLimit:  nillable.ToPointer(int64(3)),
			MonthlyBackupLimit: nillable.ToPointer(int64(2)),
		}

		backupPolicy := &datamodel.BackupPolicy{
			BaseModel: datamodel.BaseModel{
				ID:   int64(1),
				UUID: "test-backup-policy-uuid",
			},
		}

		backupPolicyActivity := BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}
		updated, err := backupPolicyActivity.UpdateBackupPolicyInVCP(context.Background(), params, backupPolicy)
		assert.Error(t, err)
		assert.Nil(t, updated)
		assert.Equal(t, "could not update backup policy in VCP", err.Error())
	})
}

func TestRevertBackupPolicyUpdateInVCP(t *testing.T) {
	t.Run("RevertBackupPolicyInVCPSucceeds", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		mockScheduler := scheduler.NewMockScheduler(tt)

		expected := &datamodel.BackupPolicy{
			BaseModel: datamodel.BaseModel{
				ID:   int64(1),
				UUID: "test-backup-policy-uuid",
			},
			Name:                  "test-backup-policy",
			Account:               &datamodel.Account{BaseModel: datamodel.BaseModel{ID: int64(1)}},
			AccountID:             1,
			Description:           nil,
			DailyBackupsToKeep:    2,
			WeeklyBackupsToKeep:   2,
			MonthlyBackupsToKeep:  2,
			PolicyEnabled:         false,
			LifeCycleState:        models.LifeCycleStateREADY,
			LifeCycleStateDetails: models.LifeCycleStateReadyDetails,
		}
		mockStorage.On("UpdateBackupPolicy", mock.Anything, mock.Anything, mock.Anything).Return(expected, nil)

		backupPolicy := &datamodel.BackupPolicy{
			BaseModel: datamodel.BaseModel{
				ID:   int64(1),
				UUID: "test-backup-policy-uuid",
			},
			Description:           nil,
			PolicyEnabled:         false,
			DailyBackupsToKeep:    2,
			WeeklyBackupsToKeep:   2,
			MonthlyBackupsToKeep:  2,
			LifeCycleState:        models.LifeCycleStateREADY,
			LifeCycleStateDetails: models.LifeCycleStateReadyDetails,
		}

		backupPolicyActivity := BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}
		updated, err := backupPolicyActivity.RevertBackupPolicyUpdateInVCP(context.Background(), backupPolicy)
		assert.NoError(t, err)
		assert.NotNil(t, updated)
		assert.Equal(tt, expected.Description, updated.Description)
		assert.Equal(tt, expected.PolicyEnabled, updated.PolicyEnabled)
		assert.Equal(tt, expected.DailyBackupsToKeep, updated.DailyBackupsToKeep)
		assert.Equal(tt, expected.WeeklyBackupsToKeep, updated.WeeklyBackupsToKeep)
		assert.Equal(tt, expected.MonthlyBackupsToKeep, updated.MonthlyBackupsToKeep)
	})

	t.Run("RevertBackupPolicyInVCPFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		mockScheduler := scheduler.NewMockScheduler(tt)

		mockStorage.On("UpdateBackupPolicy", mock.Anything, mock.Anything, mock.Anything).Return(
			nil, errors.New("could not update backup policy in VCP"))

		backupPolicy := &datamodel.BackupPolicy{
			BaseModel: datamodel.BaseModel{
				ID:   int64(1),
				UUID: "test-backup-policy-uuid",
			},
			Description:           nil,
			PolicyEnabled:         false,
			DailyBackupsToKeep:    2,
			WeeklyBackupsToKeep:   2,
			MonthlyBackupsToKeep:  2,
			LifeCycleState:        models.LifeCycleStateREADY,
			LifeCycleStateDetails: models.LifeCycleStateReadyDetails,
		}

		backupPolicyActivity := BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}
		updated, err := backupPolicyActivity.RevertBackupPolicyUpdateInVCP(context.Background(), backupPolicy)
		assert.Error(t, err)
		assert.Nil(t, updated)
	})
}

func TestPauseBackupPolicySchedule(t *testing.T) {
	t.Run("PauseBackupPolicyScheduleSucceeds", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		mockScheduler := scheduler.NewMockScheduler(tt)

		mockScheduler.On("Pause", mock.Anything, mock.Anything).Return(&scheduler.ScheduleResponse{}, nil)

		backupPolicyActivity := BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}
		err := backupPolicyActivity.PauseBackupPolicySchedule(context.Background(),
			&datamodel.BackupPolicy{BaseModel: datamodel.BaseModel{UUID: "test-backup-policy-uuid"}})
		assert.NoError(t, err)
	})

	t.Run("PauseBackupPolicyScheduleFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		mockScheduler := scheduler.NewMockScheduler(tt)

		mockScheduler.On("Pause", mock.Anything, mock.Anything).Return(nil, errors.New("could not pause backup policy schedule"))

		backupPolicyActivity := BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}
		err := backupPolicyActivity.PauseBackupPolicySchedule(context.Background(),
			&datamodel.BackupPolicy{BaseModel: datamodel.BaseModel{UUID: "test-backup-policy-uuid"}})
		assert.Error(t, err)
		assert.Equal(t, "could not pause backup policy schedule", err.Error())
	})
}

func TestUnpauseBackupPolicySchedule(t *testing.T) {
	t.Run("UnpauseBackupPolicyScheduleSucceeds", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		mockScheduler := scheduler.NewMockScheduler(tt)

		mockScheduler.On("Unpause", mock.Anything, mock.Anything).Return(&scheduler.ScheduleResponse{}, nil)

		backupPolicyActivity := BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}
		err := backupPolicyActivity.UnpauseBackupPolicySchedule(context.Background(),
			&datamodel.BackupPolicy{BaseModel: datamodel.BaseModel{UUID: "test-backup-policy-uuid"}})
		assert.NoError(t, err)
	})

	t.Run("PauseBackupPolicyScheduleFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		mockScheduler := scheduler.NewMockScheduler(tt)

		mockScheduler.On("Unpause", mock.Anything, mock.Anything).Return(nil, errors.New("could not unpause backup policy schedule"))

		backupPolicyActivity := BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}
		err := backupPolicyActivity.UnpauseBackupPolicySchedule(context.Background(),
			&datamodel.BackupPolicy{BaseModel: datamodel.BaseModel{UUID: "test-backup-policy-uuid"}})
		assert.Error(t, err)
		assert.Equal(t, "could not unpause backup policy schedule", err.Error())
	})
}
