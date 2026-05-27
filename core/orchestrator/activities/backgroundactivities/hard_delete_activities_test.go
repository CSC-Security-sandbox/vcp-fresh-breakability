package backgroundactivities

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

// MockStorage is a mock for database.Storage
type MockStorage struct {
	mock.Mock
}

func (m *MockStorage) GetDeletedAccounts(ctx context.Context) ([]*datamodel.Account, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*datamodel.Account), args.Error(1)
}

func TestHardDeleteResourcesAndAccountActivity_AccountAudit(t *testing.T) {
	t.Run("Success_ReturnsDeletedAccounts", func(t *testing.T) {
		// Setup context with logger
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)

		// Create mock storage
		mockStorage := database.NewMockStorage(t)

		// Expected accounts
		expectedAccounts := []*datamodel.Account{
			{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      "account1",
			},
			{
				BaseModel: datamodel.BaseModel{ID: 2},
				Name:      "account2",
			},
		}

		// Setup mock expectation
		mockStorage.On("GetDeletedAccounts", ctx).Return(expectedAccounts, nil)

		// Create activity
		activity := &HardDeleteResourcesAndAccountActivity{
			SE: mockStorage,
		}

		// Execute
		result, err := activity.AccountAudit(ctx)

		// Assert
		assert.NoError(t, err)
		assert.Equal(t, expectedAccounts, result)
		mockStorage.AssertExpectations(t)
	})

	t.Run("Success_ReturnsEmptyList", func(t *testing.T) {
		// Setup context with logger
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)

		// Create mock storage
		mockStorage := database.NewMockStorage(t)

		// Setup mock expectation for empty list
		mockStorage.On("GetDeletedAccounts", ctx).Return([]*datamodel.Account{}, nil)

		// Create activity
		activity := &HardDeleteResourcesAndAccountActivity{
			SE: mockStorage,
		}

		// Execute
		result, err := activity.AccountAudit(ctx)

		// Assert
		assert.NoError(t, err)
		assert.Empty(t, result)
		mockStorage.AssertExpectations(t)
	})

	t.Run("Error_GetDeletedAccountsFails", func(t *testing.T) {
		// Setup context with logger
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)

		// Create mock storage
		mockStorage := database.NewMockStorage(t)

		// Setup mock expectation for error
		expectedError := errors.New("database error")
		mockStorage.On("GetDeletedAccounts", ctx).Return(nil, expectedError)

		// Create activity
		activity := &HardDeleteResourcesAndAccountActivity{
			SE: mockStorage,
		}

		// Execute
		result, err := activity.AccountAudit(ctx)

		// Assert
		assert.Error(t, err)
		assert.Equal(t, expectedError, err)
		assert.Nil(t, result)
		mockStorage.AssertExpectations(t)
	})

	t.Run("Error_ConnectionFailed", func(t *testing.T) {
		// Setup context with logger
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)

		// Create mock storage
		mockStorage := database.NewMockStorage(t)

		// Setup mock expectation for connection error
		expectedError := errors.New("connection failed")
		mockStorage.On("GetDeletedAccounts", ctx).Return(nil, expectedError)

		// Create activity
		activity := &HardDeleteResourcesAndAccountActivity{
			SE: mockStorage,
		}

		// Execute
		result, err := activity.AccountAudit(ctx)

		// Assert
		assert.Error(t, err)
		assert.Equal(t, expectedError, err)
		assert.Nil(t, result)
		mockStorage.AssertExpectations(t)
	})

	t.Run("WithContextCancelled", func(t *testing.T) {
		// Setup cancelled context with logger
		ctx, cancel := context.WithCancel(context.Background())
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		cancel() // Cancel the context

		// Create mock storage
		mockStorage := database.NewMockStorage(t)

		// Setup mock expectation for cancelled context
		mockStorage.On("GetDeletedAccounts", ctx).Return(nil, context.Canceled)

		// Create activity
		activity := &HardDeleteResourcesAndAccountActivity{
			SE: mockStorage,
		}

		// Execute
		result, err := activity.AccountAudit(ctx)

		// Assert
		assert.Error(t, err)
		assert.Equal(t, context.Canceled, err)
		assert.Nil(t, result)
		mockStorage.AssertExpectations(t)
	})
}
