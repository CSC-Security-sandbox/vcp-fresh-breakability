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
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/scheduler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
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

		originalCreateClient := CvpCreateClient
		defer func() { CvpCreateClient = originalCreateClient }()
		CvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
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
				originalCreateClient := CvpCreateClient
				defer func() { CvpCreateClient = originalCreateClient }()
				CvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
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

		originalCreateClient := CvpCreateClient
		defer func() { CvpCreateClient = originalCreateClient }()
		CvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
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
		res := ConvertToBackupPolicyDataModel(backupPolicy)
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
		res := ConvertToBackupPolicyDataModel(backupPolicy)
		assert.Equal(t, res, expected)
	})
}

func TestUpdateBackupPolicyInSDE(t *testing.T) {
	t.Run("UpdateBackupPolicyInSDESucceeds", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(t)
		mockScheduler := scheduler.NewMockScheduler(t)
		mockClient := backup_policy.NewMockClientService(t)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}

		originalCreateClient := CvpCreateClient
		defer func() { CvpCreateClient = originalCreateClient }()
		CvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
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

		originalCreateClient := CvpCreateClient
		defer func() { CvpCreateClient = originalCreateClient }()
		CvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
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

	originalCreateClient := CvpCreateClient
	defer func() { CvpCreateClient = originalCreateClient }()
	CvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
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
				ID: int64(1), UUID: "test-backup-policy-uuid",
			},
		}

		backupPolicyActivity := BackupPolicyActivity{SE: mockStorage}
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

		mockScheduler.On("Describe", mock.Anything, mock.Anything).Return(&scheduler.ScheduleDescription{Paused: false}, nil)
		mockScheduler.On("Pause", mock.Anything, mock.Anything).Return(&scheduler.ScheduleResponse{}, nil)

		backupPolicyActivity := BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}
		err := backupPolicyActivity.PauseBackupPolicySchedule(context.Background(),
			&datamodel.BackupPolicy{BaseModel: datamodel.BaseModel{UUID: "test-backup-policy-uuid"}})
		assert.NoError(t, err)
	})

	t.Run("PauseBackupPolicyScheduleFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		mockScheduler := scheduler.NewMockScheduler(tt)

		mockScheduler.On("Describe", mock.Anything, mock.Anything).Return(&scheduler.ScheduleDescription{Paused: false}, nil)
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

		mockScheduler.On("Describe", mock.Anything, mock.Anything).Return(&scheduler.ScheduleDescription{Paused: true}, nil)
		mockScheduler.On("Unpause", mock.Anything, mock.Anything).Return(&scheduler.ScheduleResponse{}, nil)

		backupPolicyActivity := BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}
		err := backupPolicyActivity.UnpauseBackupPolicySchedule(context.Background(),
			&datamodel.BackupPolicy{BaseModel: datamodel.BaseModel{UUID: "test-backup-policy-uuid"}})
		assert.NoError(t, err)
	})

	t.Run("PauseBackupPolicyScheduleFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		mockScheduler := scheduler.NewMockScheduler(tt)

		mockScheduler.On("Describe", mock.Anything, mock.Anything).Return(&scheduler.ScheduleDescription{Paused: true}, nil)
		mockScheduler.On("Unpause", mock.Anything, mock.Anything).Return(nil, errors.New("could not unpause backup policy schedule"))

		backupPolicyActivity := BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}
		err := backupPolicyActivity.UnpauseBackupPolicySchedule(context.Background(),
			&datamodel.BackupPolicy{BaseModel: datamodel.BaseModel{UUID: "test-backup-policy-uuid"}})
		assert.Error(t, err)
		assert.Equal(t, "could not unpause backup policy schedule", err.Error())
	})

	t.Run("PauseBackupPolicySchedule_SkipsWhenAlreadyPaused", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		mockScheduler := scheduler.NewMockScheduler(tt)

		mockScheduler.On("Describe", mock.Anything, mock.Anything).Return(&scheduler.ScheduleDescription{Paused: true}, nil)
		// No Pause call should be made

		backupPolicyActivity := BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}
		err := backupPolicyActivity.PauseBackupPolicySchedule(context.Background(),
			&datamodel.BackupPolicy{BaseModel: datamodel.BaseModel{UUID: "test-backup-policy-uuid"}})
		assert.NoError(t, err)
		mockScheduler.AssertExpectations(tt)
	})

	t.Run("UnpauseBackupPolicySchedule_SkipsWhenAlreadyActive", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		mockScheduler := scheduler.NewMockScheduler(tt)

		mockScheduler.On("Describe", mock.Anything, mock.Anything).Return(&scheduler.ScheduleDescription{Paused: false}, nil)
		// No Unpause call should be made

		backupPolicyActivity := BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}
		err := backupPolicyActivity.UnpauseBackupPolicySchedule(context.Background(),
			&datamodel.BackupPolicy{BaseModel: datamodel.BaseModel{UUID: "test-backup-policy-uuid"}})
		assert.NoError(t, err)
		mockScheduler.AssertExpectations(tt)
	})
}

func TestUpdateBackupPolicyStateInCaseOfError(t *testing.T) {
	t.Run("UpdateBackupPolicyStateInCaseOfError_Success", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		ctx := context.Background()

		backupPolicy := &datamodel.BackupPolicy{
			BaseModel: datamodel.BaseModel{
				UUID: "test-backup-policy-uuid",
			},
			Name: "test-backup-policy",
		}
		state := models.LifeCycleStateREADY
		stateDetails := models.LifeCycleStateAvailableDetails

		expectedUpdates := map[string]interface{}{
			"life_cycle_state":         state,
			"life_cycle_state_details": stateDetails,
		}

		mockStorage.On("UpdateBackupPolicy", ctx, backupPolicy.UUID, expectedUpdates).Return(backupPolicy, nil).Once()

		activity := BackupPolicyActivity{
			SE: mockStorage,
		}

		err := activity.UpdateBackupPolicyStateInCaseOfError(ctx, backupPolicy, state, stateDetails)

		assert.NoError(tt, err)
		mockStorage.AssertCalled(tt, "UpdateBackupPolicy", ctx, backupPolicy.UUID, expectedUpdates)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("UpdateBackupPolicyStateInCaseOfError_DatabaseError", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		ctx := context.Background()

		backupPolicy := &datamodel.BackupPolicy{
			BaseModel: datamodel.BaseModel{
				UUID: "test-backup-policy-uuid",
			},
			Name: "test-backup-policy",
		}
		state := models.LifeCycleStateREADY
		stateDetails := models.LifeCycleStateAvailableDetails

		expectedUpdates := map[string]interface{}{
			"life_cycle_state":         state,
			"life_cycle_state_details": stateDetails,
		}

		mockStorage.On("UpdateBackupPolicy", ctx, backupPolicy.UUID, expectedUpdates).Return(nil, errors.New("database update failed")).Once()

		activity := BackupPolicyActivity{
			SE: mockStorage,
		}

		err := activity.UpdateBackupPolicyStateInCaseOfError(ctx, backupPolicy, state, stateDetails)

		assert.Error(tt, err)
		assert.Equal(tt, "database update failed", err.Error())
		mockStorage.AssertCalled(tt, "UpdateBackupPolicy", ctx, backupPolicy.UUID, expectedUpdates)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("UpdateBackupPolicyStateInCaseOfError_ErrorState", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		ctx := context.Background()

		backupPolicy := &datamodel.BackupPolicy{
			BaseModel: datamodel.BaseModel{
				UUID: "test-backup-policy-uuid",
			},
			Name:           "test-backup-policy",
			LifeCycleState: models.LifeCycleStateDeleting,
		}
		state := models.LifeCycleStateError
		stateDetails := "Deletion failed due to external service error"

		expectedUpdates := map[string]interface{}{
			"life_cycle_state":         state,
			"life_cycle_state_details": stateDetails,
		}

		mockStorage.On("UpdateBackupPolicy", ctx, backupPolicy.UUID, expectedUpdates).Return(backupPolicy, nil).Once()

		activity := BackupPolicyActivity{
			SE: mockStorage,
		}

		err := activity.UpdateBackupPolicyStateInCaseOfError(ctx, backupPolicy, state, stateDetails)

		assert.NoError(tt, err)
		mockStorage.AssertCalled(tt, "UpdateBackupPolicy", ctx, backupPolicy.UUID, expectedUpdates)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("UpdateBackupPolicyStateInCaseOfError_DifferentStates", func(tt *testing.T) {
		testCases := []struct {
			name         string
			state        string
			stateDetails string
		}{
			{
				name:         "ReadyState",
				state:        models.LifeCycleStateREADY,
				stateDetails: models.LifeCycleStateAvailableDetails,
			},
			{
				name:         "ErrorState",
				state:        models.LifeCycleStateError,
				stateDetails: models.LifeCycleStateDeletionErrorDetails,
			},
			{
				name:         "AvailableState",
				state:        models.LifeCycleStateAvailable,
				stateDetails: models.LifeCycleStateAvailableDetails,
			},
		}

		for _, tc := range testCases {
			tt.Run(tc.name, func(t *testing.T) {
				mockStorage := database.NewMockStorage(t)
				ctx := context.Background()

				backupPolicy := &datamodel.BackupPolicy{
					BaseModel: datamodel.BaseModel{
						UUID: "test-backup-policy-uuid",
					},
					Name: "test-backup-policy",
				}

				expectedUpdates := map[string]interface{}{
					"life_cycle_state":         tc.state,
					"life_cycle_state_details": tc.stateDetails,
				}

				mockStorage.On("UpdateBackupPolicy", ctx, backupPolicy.UUID, expectedUpdates).Return(backupPolicy, nil).Once()

				activity := BackupPolicyActivity{
					SE: mockStorage,
				}

				err := activity.UpdateBackupPolicyStateInCaseOfError(ctx, backupPolicy, tc.state, tc.stateDetails)

				assert.NoError(t, err)
				mockStorage.AssertCalled(t, "UpdateBackupPolicy", ctx, backupPolicy.UUID, expectedUpdates)
				mockStorage.AssertExpectations(t)
			})
		}
	})
}

func TestUpdateBackupPolicyStateInCaseOfError_Integration(t *testing.T) {
	t.Run("Integration_StateUpdateWithProperParameters", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		ctx := context.Background()

		// Test with actual backup policy object that matches what would be used in workflow
		backupPolicy := &datamodel.BackupPolicy{
			BaseModel: datamodel.BaseModel{
				ID:   int64(1),
				UUID: "backup-policy-uuid-1",
			},
			Name:                  "test-backup-policy",
			AccountID:             int64(1),
			Description:           nillable.ToPointer("Test backup policy"),
			DailyBackupsToKeep:    3,
			WeeklyBackupsToKeep:   2,
			MonthlyBackupsToKeep:  1,
			PolicyEnabled:         true,
			LifeCycleState:        models.LifeCycleStateDeleting,
			LifeCycleStateDetails: models.LifeCycleStateDeletingDetails,
		}

		// Test restoring to READY state
		expectedUpdates := map[string]interface{}{
			"life_cycle_state":         models.LifeCycleStateREADY,
			"life_cycle_state_details": models.LifeCycleStateAvailableDetails,
		}

		updatedBackupPolicy := &datamodel.BackupPolicy{
			BaseModel:             backupPolicy.BaseModel,
			Name:                  backupPolicy.Name,
			AccountID:             backupPolicy.AccountID,
			Description:           backupPolicy.Description,
			DailyBackupsToKeep:    backupPolicy.DailyBackupsToKeep,
			WeeklyBackupsToKeep:   backupPolicy.WeeklyBackupsToKeep,
			MonthlyBackupsToKeep:  backupPolicy.MonthlyBackupsToKeep,
			PolicyEnabled:         backupPolicy.PolicyEnabled,
			LifeCycleState:        models.LifeCycleStateREADY,
			LifeCycleStateDetails: models.LifeCycleStateAvailableDetails,
		}

		mockStorage.On("UpdateBackupPolicy", ctx, backupPolicy.UUID, expectedUpdates).Return(updatedBackupPolicy, nil).Once()

		activity := BackupPolicyActivity{
			SE: mockStorage,
		}

		err := activity.UpdateBackupPolicyStateInCaseOfError(ctx, backupPolicy, models.LifeCycleStateREADY, models.LifeCycleStateAvailableDetails)

		assert.NoError(tt, err)
		mockStorage.AssertCalled(tt, "UpdateBackupPolicy", ctx, backupPolicy.UUID, expectedUpdates)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("Integration_MultipleStateTransitions", func(tt *testing.T) {
		testCases := []struct {
			name                string
			initialState        string
			initialStateDetails string
			targetState         string
			targetStateDetails  string
			shouldSucceed       bool
			expectedError       string
		}{
			{
				name:                "DeletingToReady",
				initialState:        models.LifeCycleStateDeleting,
				initialStateDetails: models.LifeCycleStateDeletingDetails,
				targetState:         models.LifeCycleStateREADY,
				targetStateDetails:  models.LifeCycleStateAvailableDetails,
				shouldSucceed:       true,
			},
			{
				name:                "UpdatingToReady",
				initialState:        models.LifeCycleStateUpdating,
				initialStateDetails: models.LifeCycleStateUpdatingDetails,
				targetState:         models.LifeCycleStateREADY,
				targetStateDetails:  models.LifeCycleStateAvailableDetails,
				shouldSucceed:       true,
			},
			{
				name:                "DeletingToError",
				initialState:        models.LifeCycleStateDeleting,
				initialStateDetails: models.LifeCycleStateDeletingDetails,
				targetState:         models.LifeCycleStateError,
				targetStateDetails:  models.LifeCycleStateDeletionErrorDetails,
				shouldSucceed:       true,
			},
			{
				name:                "DatabaseFailure",
				initialState:        models.LifeCycleStateDeleting,
				initialStateDetails: models.LifeCycleStateDeletingDetails,
				targetState:         models.LifeCycleStateREADY,
				targetStateDetails:  models.LifeCycleStateAvailableDetails,
				shouldSucceed:       false,
				expectedError:       "database connection failed",
			},
		}

		for _, tc := range testCases {
			tt.Run(tc.name, func(t *testing.T) {
				mockStorage := database.NewMockStorage(t)
				ctx := context.Background()

				backupPolicy := &datamodel.BackupPolicy{
					BaseModel: datamodel.BaseModel{
						ID:   int64(1),
						UUID: "backup-policy-uuid-1",
					},
					Name:                  "test-backup-policy",
					LifeCycleState:        tc.initialState,
					LifeCycleStateDetails: tc.initialStateDetails,
				}

				expectedUpdates := map[string]interface{}{
					"life_cycle_state":         tc.targetState,
					"life_cycle_state_details": tc.targetStateDetails,
				}

				if tc.shouldSucceed {
					mockStorage.On("UpdateBackupPolicy", ctx, backupPolicy.UUID, expectedUpdates).Return(backupPolicy, nil).Once()
				} else {
					mockStorage.On("UpdateBackupPolicy", ctx, backupPolicy.UUID, expectedUpdates).Return(nil, errors.New(tc.expectedError)).Once()
				}

				activity := BackupPolicyActivity{
					SE: mockStorage,
				}

				err := activity.UpdateBackupPolicyStateInCaseOfError(ctx, backupPolicy, tc.targetState, tc.targetStateDetails)

				if tc.shouldSucceed {
					assert.NoError(t, err)
				} else {
					assert.Error(t, err)
					assert.Equal(t, tc.expectedError, err.Error())
				}

				mockStorage.AssertCalled(t, "UpdateBackupPolicy", ctx, backupPolicy.UUID, expectedUpdates)
				mockStorage.AssertExpectations(t)
			})
		}
	})
}

func TestCleanupBackupPoliciesForAccount(t *testing.T) {
	t.Run("CleanupBackupPoliciesForAccount_Success", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		mockScheduler := scheduler.NewMockScheduler(tt)

		// Mock account lookup
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			Name:      "test-project-123",
		}
		mockStorage.On("GetAccount", mock.Anything, "test-project-123").Return(account, nil)

		// Mock backup policies list
		backupPolicies := []*datamodel.BackupPolicy{
			{
				BaseModel: datamodel.BaseModel{UUID: "policy-uuid-1"},
				Name:      "policy-1",
				AccountID: 1,
			},
			{
				BaseModel: datamodel.BaseModel{UUID: "policy-uuid-2"},
				Name:      "policy-2",
				AccountID: 1,
			},
		}
		conditions := [][]interface{}{
			{"account_id = ?", account.ID},
		}
		mockStorage.On("ListBackupPolicies", mock.Anything, conditions).Return(backupPolicies, nil)

		// Mock scheduler delete calls
		mockScheduler.On("Delete", mock.Anything, scheduler.DeleteScheduleParams{
			ScheduleParams: scheduler.ScheduleParams{ScheduleID: "policy-uuid-1"},
		}).Return(&scheduler.ScheduleResponse{}, nil)
		mockScheduler.On("Delete", mock.Anything, scheduler.DeleteScheduleParams{
			ScheduleParams: scheduler.ScheduleParams{ScheduleID: "policy-uuid-2"},
		}).Return(&scheduler.ScheduleResponse{}, nil)

		// Mock database delete calls
		mockStorage.On("DeleteBackupPolicy", mock.Anything, "policy-uuid-1").Return(&datamodel.BackupPolicy{}, nil)
		mockStorage.On("DeleteBackupPolicy", mock.Anything, "policy-uuid-2").Return(&datamodel.BackupPolicy{}, nil)

		activity := BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}
		err := activity.CleanupBackupPoliciesForAccount(context.Background(), "test-project-123")

		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
		mockScheduler.AssertExpectations(tt)
	})

	t.Run("CleanupBackupPoliciesForAccount_NoPolicies", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		mockScheduler := scheduler.NewMockScheduler(tt)

		// Mock account lookup
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			Name:      "test-project-123",
		}
		mockStorage.On("GetAccount", mock.Anything, "test-project-123").Return(account, nil)

		// Mock empty backup policies list
		conditions := [][]interface{}{
			{"account_id = ?", account.ID},
		}
		mockStorage.On("ListBackupPolicies", mock.Anything, conditions).Return([]*datamodel.BackupPolicy{}, nil)

		activity := BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}
		err := activity.CleanupBackupPoliciesForAccount(context.Background(), "test-project-123")

		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
		mockScheduler.AssertExpectations(tt)
	})

	t.Run("CleanupBackupPoliciesForAccount_GetAccountFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		mockScheduler := scheduler.NewMockScheduler(tt)

		mockStorage.On("GetAccount", mock.Anything, "test-project-123").Return(nil, errors.New("account not found"))

		activity := BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}
		err := activity.CleanupBackupPoliciesForAccount(context.Background(), "test-project-123")

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "account not found")
		mockStorage.AssertExpectations(tt)
		mockScheduler.AssertExpectations(tt)
	})

	t.Run("CleanupBackupPoliciesForAccount_ListBackupPoliciesFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		mockScheduler := scheduler.NewMockScheduler(tt)

		// Mock account lookup
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			Name:      "test-project-123",
		}
		mockStorage.On("GetAccount", mock.Anything, "test-project-123").Return(account, nil)

		// Mock backup policies list failure
		conditions := [][]interface{}{
			{"account_id = ?", account.ID},
		}
		mockStorage.On("ListBackupPolicies", mock.Anything, conditions).Return(nil, errors.New("database error"))

		activity := BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}
		err := activity.CleanupBackupPoliciesForAccount(context.Background(), "test-project-123")

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "database error")
		mockStorage.AssertExpectations(tt)
		mockScheduler.AssertExpectations(tt)
	})

	t.Run("CleanupBackupPoliciesForAccount_CleanupPolicyFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		mockScheduler := scheduler.NewMockScheduler(tt)

		// Mock account lookup
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			Name:      "test-project-123",
		}
		mockStorage.On("GetAccount", mock.Anything, "test-project-123").Return(account, nil)

		// Mock backup policies list
		backupPolicies := []*datamodel.BackupPolicy{
			{
				BaseModel: datamodel.BaseModel{UUID: "policy-uuid-1"},
				Name:      "policy-1",
				AccountID: 1,
			},
		}
		conditions := [][]interface{}{
			{"account_id = ?", account.ID},
		}
		mockStorage.On("ListBackupPolicies", mock.Anything, conditions).Return(backupPolicies, nil)

		// Mock scheduler delete failure
		mockScheduler.On("Delete", mock.Anything, scheduler.DeleteScheduleParams{
			ScheduleParams: scheduler.ScheduleParams{ScheduleID: "policy-uuid-1"},
		}).Return(nil, errors.New("scheduler error"))

		// Mock database delete failure
		mockStorage.On("DeleteBackupPolicy", mock.Anything, "policy-uuid-1").Return(nil, errors.New("database delete error"))

		activity := BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}
		err := activity.CleanupBackupPoliciesForAccount(context.Background(), "test-project-123")

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "database delete error")
		mockStorage.AssertExpectations(tt)
		mockScheduler.AssertExpectations(tt)
	})
}

func TestCleanupBackupPolicy(t *testing.T) {
	t.Run("CleanupBackupPolicy_Success", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		mockScheduler := scheduler.NewMockScheduler(tt)

		policy := &datamodel.BackupPolicy{
			BaseModel: datamodel.BaseModel{UUID: "policy-uuid-1"},
			Name:      "policy-1",
		}

		// Mock scheduler delete
		mockScheduler.On("Delete", mock.Anything, scheduler.DeleteScheduleParams{
			ScheduleParams: scheduler.ScheduleParams{ScheduleID: "policy-uuid-1"},
		}).Return(&scheduler.ScheduleResponse{}, nil)

		// Mock database delete
		mockStorage.On("DeleteBackupPolicy", mock.Anything, "policy-uuid-1").Return(&datamodel.BackupPolicy{}, nil)

		activity := BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}
		err := activity.cleanupBackupPolicy(context.Background(), policy)

		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
		mockScheduler.AssertExpectations(tt)
	})

	t.Run("CleanupBackupPolicy_SchedulerDeleteFails_Continues", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		mockScheduler := scheduler.NewMockScheduler(tt)

		policy := &datamodel.BackupPolicy{
			BaseModel: datamodel.BaseModel{UUID: "policy-uuid-1"},
			Name:      "policy-1",
		}

		// Mock scheduler delete failure - should continue with database deletion
		mockScheduler.On("Delete", mock.Anything, scheduler.DeleteScheduleParams{
			ScheduleParams: scheduler.ScheduleParams{ScheduleID: "policy-uuid-1"},
		}).Return(nil, errors.New("scheduler error"))

		// Mock database delete success
		mockStorage.On("DeleteBackupPolicy", mock.Anything, "policy-uuid-1").Return(&datamodel.BackupPolicy{}, nil)

		activity := BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}
		err := activity.cleanupBackupPolicy(context.Background(), policy)

		assert.NoError(tt, err) // Should succeed despite scheduler failure
		mockStorage.AssertExpectations(tt)
		mockScheduler.AssertExpectations(tt)
	})

	t.Run("CleanupBackupPolicy_DatabaseDeleteFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		mockScheduler := scheduler.NewMockScheduler(tt)

		policy := &datamodel.BackupPolicy{
			BaseModel: datamodel.BaseModel{UUID: "policy-uuid-1"},
			Name:      "policy-1",
		}

		// Mock scheduler delete success
		mockScheduler.On("Delete", mock.Anything, scheduler.DeleteScheduleParams{
			ScheduleParams: scheduler.ScheduleParams{ScheduleID: "policy-uuid-1"},
		}).Return(&scheduler.ScheduleResponse{}, nil)

		// Mock database delete failure
		mockStorage.On("DeleteBackupPolicy", mock.Anything, "policy-uuid-1").Return(nil, errors.New("database error"))

		activity := BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}
		err := activity.cleanupBackupPolicy(context.Background(), policy)

		assert.Error(tt, err)
		// Should return a non-retryable application error
		appErr, ok := err.(*temporal.ApplicationError)
		assert.True(tt, ok)
		assert.Contains(tt, appErr.Error(), "Failed to soft delete backup policy")
		assert.Equal(tt, "DeleteBackupPolicyError", appErr.Type())
		mockStorage.AssertExpectations(tt)
		mockScheduler.AssertExpectations(tt)
	})

	t.Run("CleanupBackupPolicy_BothSchedulerAndDatabaseFail", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		mockScheduler := scheduler.NewMockScheduler(tt)

		policy := &datamodel.BackupPolicy{
			BaseModel: datamodel.BaseModel{UUID: "policy-uuid-1"},
			Name:      "policy-1",
		}

		// Mock scheduler delete failure
		mockScheduler.On("Delete", mock.Anything, scheduler.DeleteScheduleParams{
			ScheduleParams: scheduler.ScheduleParams{ScheduleID: "policy-uuid-1"},
		}).Return(nil, errors.New("scheduler error"))

		// Mock database delete failure
		mockStorage.On("DeleteBackupPolicy", mock.Anything, "policy-uuid-1").Return(nil, errors.New("database error"))

		activity := BackupPolicyActivity{SE: mockStorage, Scheduler: mockScheduler}
		err := activity.cleanupBackupPolicy(context.Background(), policy)

		assert.Error(tt, err)
		// Should return a non-retryable application error for database failure
		appErr, ok := err.(*temporal.ApplicationError)
		assert.True(tt, ok)
		assert.Contains(tt, appErr.Error(), "Failed to soft delete backup policy")
		assert.Equal(tt, "DeleteBackupPolicyError", appErr.Type())
		mockStorage.AssertExpectations(tt)
		mockScheduler.AssertExpectations(tt)
	})
}
