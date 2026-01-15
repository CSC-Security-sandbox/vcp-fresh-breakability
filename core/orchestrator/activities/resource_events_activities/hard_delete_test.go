package resource_events_activities

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
)

func TestFinishProjectEventActivity_HardDeleteResourcesInOrder(t *testing.T) {
	// Create a context with logger to ensure logging lines are covered
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)

	t.Run("GetSoftDeleteAccountReturnsError", func(t *testing.T) {
		activity := &FinishProjectEventActivity{
			SE: mockStorage,
		}

		projectNumber := "test-project-404"
		expectedErr := errors.New("account not found in database")

		// Mock GetSoftDeleteAccount to return an error
		mockStorage.On("GetSoftDeleteAccount", ctx, projectNumber).Return(nil, expectedErr)

		// Execute the actual method
		err := activity.HardDeleteResourcesInOrder(ctx, projectNumber)

		// Assert that the error is properly returned
		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)

		// Verify expectations
		mockStorage.AssertExpectations(t)

		// Ensure HardDeleteResourceByTable was never called since we returned early
		mockStorage.AssertNotCalled(t, "HardDeleteResourceByTable", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("SuccessfulDeletionOfAllResources", func(t *testing.T) {
		// This test covers line 41 (success path) and lines 47-52, 54
		mockStorage := database.NewMockStorage(t)
		activity := &FinishProjectEventActivity{
			SE: mockStorage,
		}

		projectNumber := "test-project-123"
		accountID := int64(1)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID: accountID,
			},
		}

		// Mock successful GetSoftDeleteAccount
		mockStorage.On("GetSoftDeleteAccount", ctx, projectNumber).Return(account, nil)

		// Mock successful HardDeleteResourceByTable for all resources
		for _, resource := range resourcesToHardDelete {
			mockStorage.On("HardDeleteResourceByTable", ctx, resource.tableName, resource.queryFilter, accountID).Return(nil)
		}

		// Execute
		err := activity.HardDeleteResourcesInOrder(ctx, projectNumber)

		// Assert success
		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)

		// Verify all resources were deleted
		mockStorage.AssertNumberOfCalls(t, "HardDeleteResourceByTable", len(resourcesToHardDelete))
	})

	t.Run("HardDeleteResourceFailsOnFirstResource", func(t *testing.T) {
		// This test covers lines 48-52 (error in loop)
		mockStorage := database.NewMockStorage(t)
		activity := &FinishProjectEventActivity{
			SE: mockStorage,
		}

		projectNumber := "test-project-123"
		accountID := int64(1)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID: accountID,
			},
		}

		deleteErr := errors.New("database connection error")

		// Mock successful GetSoftDeleteAccount
		mockStorage.On("GetSoftDeleteAccount", ctx, projectNumber).Return(account, nil)

		// First resource deletion fails
		mockStorage.On("HardDeleteResourceByTable", ctx, resourcesToHardDelete[0].tableName, resourcesToHardDelete[0].queryFilter, accountID).Return(deleteErr)

		// Execute
		err := activity.HardDeleteResourcesInOrder(ctx, projectNumber)

		// Assert error
		assert.Error(t, err)
		assert.Equal(t, deleteErr, err)
		mockStorage.AssertExpectations(t)

		// Verify only the first delete was attempted
		mockStorage.AssertNumberOfCalls(t, "HardDeleteResourceByTable", 1)
	})

	t.Run("HardDeleteResourceFailsOnMiddleResource", func(t *testing.T) {
		// This test ensures the loop executes multiple times before failing
		mockStorage := database.NewMockStorage(t)
		activity := &FinishProjectEventActivity{
			SE: mockStorage,
		}

		projectNumber := "test-project-123"
		accountID := int64(1)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID: accountID,
			},
		}

		deleteErr := errors.New("permission denied")
		failureIndex := 5 // Fail on backup_vaults

		// Mock successful GetSoftDeleteAccount
		mockStorage.On("GetSoftDeleteAccount", ctx, projectNumber).Return(account, nil)

		// Mock successful deletes up to failure point
		for i, resource := range resourcesToHardDelete {
			if i < failureIndex {
				mockStorage.On("HardDeleteResourceByTable", ctx, resource.tableName, resource.queryFilter, accountID).Return(nil)
			} else if i == failureIndex {
				mockStorage.On("HardDeleteResourceByTable", ctx, resource.tableName, resource.queryFilter, accountID).Return(deleteErr)
				break
			}
		}

		// Execute
		err := activity.HardDeleteResourcesInOrder(ctx, projectNumber)

		// Assert error
		assert.Error(t, err)
		assert.Equal(t, deleteErr, err)
		mockStorage.AssertExpectations(t)

		// Verify deletes were attempted up to and including the failure
		mockStorage.AssertNumberOfCalls(t, "HardDeleteResourceByTable", failureIndex+1)
	})

	t.Run("HardDeleteResourceFailsOnLastResource", func(t *testing.T) {
		// This test ensures all resources are processed except the last one fails
		mockStorage := database.NewMockStorage(t)
		activity := &FinishProjectEventActivity{
			SE: mockStorage,
		}

		projectNumber := "test-project-123"
		accountID := int64(1)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID: accountID,
			},
		}

		deleteErr := errors.New("constraint violation")

		// Mock successful GetSoftDeleteAccount
		mockStorage.On("GetSoftDeleteAccount", ctx, projectNumber).Return(account, nil)

		// Mock successful deletes for all except the last resource
		for i, resource := range resourcesToHardDelete {
			if i < len(resourcesToHardDelete)-1 {
				mockStorage.On("HardDeleteResourceByTable", ctx, resource.tableName, resource.queryFilter, accountID).Return(nil)
			} else {
				mockStorage.On("HardDeleteResourceByTable", ctx, resource.tableName, resource.queryFilter, accountID).Return(deleteErr)
			}
		}

		// Execute
		err := activity.HardDeleteResourcesInOrder(ctx, projectNumber)

		// Assert error
		assert.Error(t, err)
		assert.Equal(t, deleteErr, err)
		mockStorage.AssertExpectations(t)

		// Verify all deletes were attempted
		mockStorage.AssertNumberOfCalls(t, "HardDeleteResourceByTable", len(resourcesToHardDelete))
	})

	t.Run("VerifyDeletionOrderIsRespected", func(t *testing.T) {
		// This test verifies the correct order of deletion
		mockStorage := database.NewMockStorage(t)
		activity := &FinishProjectEventActivity{
			SE: mockStorage,
		}

		projectNumber := "test-project-123"
		accountID := int64(1)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID: accountID,
			},
		}

		// Mock successful GetSoftDeleteAccount
		mockStorage.On("GetSoftDeleteAccount", ctx, projectNumber).Return(account, nil)

		// Track the order of calls
		var callOrder []string

		for _, resource := range resourcesToHardDelete {
			tableName := resource.tableName
			queryFilter := resource.queryFilter

			mockStorage.On("HardDeleteResourceByTable", ctx, tableName, queryFilter, accountID).Return(nil).Run(func(args mock.Arguments) {
				callOrder = append(callOrder, args.String(1))
			})
		}

		// Execute
		err := activity.HardDeleteResourcesInOrder(ctx, projectNumber)

		// Assert success
		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)

		// Verify the order matches expected
		expectedOrder := []string{
			"backups", "backup_policies", "snapshots", "volume_replications",
			"volumes", "backup_vaults", "svms", "pools", "host_groups",
			"kms_configs", "nodes", "service_accounts", "lifs", "accounts", "quota_rules",
		}
		assert.Equal(t, expectedOrder, callOrder)
	})

	t.Run("EmptyProjectNumber", func(t *testing.T) {
		// Test with empty project number
		mockStorage := database.NewMockStorage(t)
		activity := &FinishProjectEventActivity{
			SE: mockStorage,
		}

		projectNumber := ""
		expectedErr := errors.New("project number cannot be empty")

		// Mock GetSoftDeleteAccount to return an error for empty project
		mockStorage.On("GetSoftDeleteAccount", ctx, projectNumber).Return(nil, expectedErr)

		// Execute
		err := activity.HardDeleteResourcesInOrder(ctx, projectNumber)

		// Assert error
		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
		mockStorage.AssertExpectations(t)
	})

	t.Run("AccountWithZeroID", func(t *testing.T) {
		// Test with account ID of 0
		mockStorage := database.NewMockStorage(t)
		activity := &FinishProjectEventActivity{
			SE: mockStorage,
		}

		projectNumber := "test-project-zero"
		accountID := int64(0)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID: accountID,
			},
		}

		// Mock successful GetSoftDeleteAccount
		mockStorage.On("GetSoftDeleteAccount", ctx, projectNumber).Return(account, nil)

		// All deletes should still be attempted with accountID 0
		for _, resource := range resourcesToHardDelete {
			mockStorage.On("HardDeleteResourceByTable", ctx, resource.tableName, resource.queryFilter, accountID).Return(nil)
		}

		// Execute
		err := activity.HardDeleteResourcesInOrder(ctx, projectNumber)

		// Assert success
		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
	})

	t.Run("VerifyQueryFiltersAreCorrect", func(t *testing.T) {
		// This test verifies the correct query filters are used
		mockStorage := database.NewMockStorage(t)
		activity := &FinishProjectEventActivity{
			SE: mockStorage,
		}

		projectNumber := "test-project-123"
		accountID := int64(1)

		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID: accountID,
			},
		}

		// Mock successful GetSoftDeleteAccount
		mockStorage.On("GetSoftDeleteAccount", ctx, projectNumber).Return(account, nil)

		// Track query filters used
		var queryFilters []string

		for _, resource := range resourcesToHardDelete {
			tableName := resource.tableName
			queryFilter := resource.queryFilter

			mockStorage.On("HardDeleteResourceByTable", ctx, tableName, queryFilter, accountID).Return(nil).Run(func(args mock.Arguments) {
				queryFilters = append(queryFilters, args.String(2))
			})
		}

		// Execute
		err := activity.HardDeleteResourcesInOrder(ctx, projectNumber)

		// Assert success
		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)

		// Verify specific query filters
		assert.Equal(t, dbQueryBackupID, queryFilters[0])     // backups
		assert.Equal(t, dbQueryAccountName, queryFilters[13]) // accounts (index 13, uses dbQueryAccountName)

		// Count dbQueryAccountID usage
		accountIDQueryCount := 0
		for _, qf := range queryFilters {
			if qf == dbQueryAccountID {
				accountIDQueryCount++
			}
		}
		assert.Equal(t, 13, accountIDQueryCount) // All except backups and accounts (12 resources + quota_rules)
	})
}

func TestResourceConfiguration(t *testing.T) {
	t.Run("VerifyResourceCount", func(t *testing.T) {
		assert.Equal(t, 15, len(resourcesToHardDelete), "Should have 15 resources to delete")
	})

	t.Run("VerifyConstantValues", func(t *testing.T) {
		assert.Equal(t, "account_id = ? and deleted_at is not null", dbQueryAccountID)
		assert.Equal(t, "backup_vault_id in (select id from backup_vaults where account_id = ? and deleted_at is not null) and deleted_at is not null", dbQueryBackupID)
		assert.Equal(t, "id = ?", dbQueryAccountName)
	})

	t.Run("VerifyResourceStructure", func(t *testing.T) {
		for i, resource := range resourcesToHardDelete {
			assert.NotEmpty(t, resource.tableName, "Resource at index %d should have a table name", i)
			assert.NotEmpty(t, resource.queryFilter, "Resource at index %d should have a query filter", i)
		}
	})

	t.Run("VerifyResourceDeletionOrder", func(t *testing.T) {
		// Verify the specific order of resources
		expectedOrder := []string{
			"backups", "backup_policies", "snapshots", "volume_replications",
			"volumes", "backup_vaults", "svms", "pools", "host_groups",
			"kms_configs", "nodes", "service_accounts", "lifs", "accounts", "quota_rules",
		}

		for i, resource := range resourcesToHardDelete {
			assert.Equal(t, expectedOrder[i], resource.tableName, "Resource at index %d should be %s", i, expectedOrder[i])
		}
	})
}
