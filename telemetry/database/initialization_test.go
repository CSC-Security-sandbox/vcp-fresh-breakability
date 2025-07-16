package database

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

type mockLogger struct {
	log.Logger
}

type mockDB struct {
	mock.Mock
}

func (m *mockDB) GetConnection(ctx context.Context, logger log.Logger) (database.Storage, error) {
	args := m.Called(ctx, logger)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(database.Storage), args.Error(1)
}

func Test_InitializeDatabase_Success(t *testing.T) {
	ctx := context.Background()
	logger := &mockLogger{}
	mockStorage := new(database.MockStorage)
	mdb := new(mockDB)
	mdb.On("GetConnection", ctx, logger).Return(mockStorage, nil)

	storage, err := InitializeDatabase(ctx, mdb, logger)
	assert.NoError(t, err)
	assert.Equal(t, mockStorage, storage)
	mdb.AssertExpectations(t)
}

func Test_InitializeDatabase_Error(t *testing.T) {
	ctx := context.Background()
	logger := &mockLogger{}
	errExpected := assert.AnError
	mdb := new(mockDB)
	mdb.On("GetConnection", ctx, logger).Return(nil, errExpected)

	storage, err := InitializeDatabase(ctx, mdb, logger)
	assert.Error(t, err)
	assert.Nil(t, storage)
	mdb.AssertExpectations(t)
}

func Test_InitializeDatabase_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	logger := &mockLogger{}
	mdb := new(mockDB)
	cancel() // cancel context before call
	mdb.On("GetConnection", ctx, logger).Return(nil, context.Canceled)

	storage, err := InitializeDatabase(ctx, mdb, logger)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Nil(t, storage)
	mdb.AssertExpectations(t)
}

func Test_InitializeDatabase_NilLogger(t *testing.T) {
	ctx := context.Background()
	mdb := new(mockDB)
	mdb.On("GetConnection", ctx, nil).Return(nil, assert.AnError)

	storage, err := InitializeDatabase(ctx, mdb, nil)
	assert.Error(t, err)
	assert.Nil(t, storage)
	mdb.AssertExpectations(t)
}

func Test_InitializeDatabase_NilDB(t *testing.T) {
	ctx := context.Background()
	logger := &mockLogger{}
	storage, err := InitializeDatabase(ctx, nil, logger)
	assert.Error(t, err)
	assert.Nil(t, storage)
}

func Test_InitializeDatabase_GetConnectionPanics(t *testing.T) {
	ctx := context.Background()
	logger := &mockLogger{}
	mdb := new(mockDB)
	mdb.On("GetConnection", ctx, logger).Run(func(args mock.Arguments) {
		panic("panic in GetConnection")
	}).Return(nil, nil)

	defer func() {
		r := recover()
		assert.NotNil(t, r)
	}()
	_, _ = InitializeDatabase(ctx, mdb, logger)
}

func Test_InitializeDatabase_StorageAndError(t *testing.T) {
	ctx := context.Background()
	logger := &mockLogger{}
	mockStorage := new(database.MockStorage)
	mdb := new(mockDB)
	errExpected := assert.AnError
	mdb.On("GetConnection", ctx, logger).Return(mockStorage, errExpected)

	storage, err := InitializeDatabase(ctx, mdb, logger)
	assert.Error(t, err)
	assert.Nil(t, storage) // Should not return storage if error
	mdb.AssertExpectations(t)
}

func Test_InitializeDatabase_StorageAndErrorReturned(t *testing.T) {
	ctx := context.Background()
	logger := &mockLogger{}
	mockStorage := new(database.MockStorage)
	mdb := new(mockDB)
	mdb.On("GetConnection", ctx, logger).Return(mockStorage, assert.AnError)

	storage, err := InitializeDatabase(ctx, mdb, logger)
	assert.Error(t, err)
	assert.Nil(t, storage)
	mdb.AssertExpectations(t)
}

func Test_InitializeDatabase_NilStorageErrorReturned(t *testing.T) {
	ctx := context.Background()
	logger := &mockLogger{}
	mdb := new(mockDB)
	mdb.On("GetConnection", ctx, logger).Return(nil, assert.AnError)

	storage, err := InitializeDatabase(ctx, mdb, logger)
	assert.Error(t, err)
	assert.Nil(t, storage)
	mdb.AssertExpectations(t)
}

func Test_InitializeDatabase_NilStorageNilErrorReturned(t *testing.T) {
	ctx := context.Background()
	logger := &mockLogger{}
	mdb := new(mockDB)
	mdb.On("GetConnection", ctx, logger).Return(nil, nil)

	storage, err := InitializeDatabase(ctx, mdb, logger)
	assert.Error(t, err)
	assert.Nil(t, storage)
	mdb.AssertExpectations(t)
}

func Test_InitializeDatabase_ContextDeadlineExceeded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 0)
	defer cancel()
	logger := &mockLogger{}
	mdb := new(mockDB)
	mdb.On("GetConnection", ctx, logger).Return(nil, context.DeadlineExceeded)

	storage, err := InitializeDatabase(ctx, mdb, logger)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Nil(t, storage)
	mdb.AssertExpectations(t)
}

func Test_InitializeDatabase_NilInterfaceLogger(t *testing.T) {
	ctx := context.Background()
	var logger log.Logger = (*mockLogger)(nil) // nil interface, not nil pointer
	mdb := new(mockDB)
	mdb.On("GetConnection", ctx, logger).Return(nil, assert.AnError)

	storage, err := InitializeDatabase(ctx, mdb, logger)
	assert.Error(t, err)
	assert.Nil(t, storage)
	mdb.AssertExpectations(t)
}

func Test_InitializeDatabase_ValidStorageNilError(t *testing.T) {
	ctx := context.Background()
	logger := &mockLogger{}
	mockStorage := new(database.MockStorage)
	mdb := new(mockDB)
	mdb.On("GetConnection", ctx, logger).Return(mockStorage, nil)

	storage, err := InitializeDatabase(ctx, mdb, logger)
	assert.NoError(t, err)
	assert.Equal(t, mockStorage, storage)
	mdb.AssertExpectations(t)
}

func Test_InitializeDatabase_StorageWrongType(t *testing.T) {
	ctx := context.Background()
	logger := &mockLogger{}
	mdb := new(mockDB)
	// Return a type that does not implement database.Storage
	mdb.On("GetConnection", ctx, logger).Return(123, nil)
	defer func() {
		r := recover()
		assert.NotNil(t, r)
	}()
	_, _ = InitializeDatabase(ctx, mdb, logger)
}

func Test_VCPDatabaseImpl_ImplementsInterface(t *testing.T) {
	var _ VCPDatabase = &vcpDataRepository{}
	var _ Database = &vcpDataRepository{}
}

func Test_TelemetryDatabaseImpl_ImplementsInterface(t *testing.T) {
	var _ TelemetryDatabase = &telemetryDatabaseImpl{}
	var _ Database = &telemetryDatabaseImpl{}
}

func Test_ExportedVars_AreConcreteTypes(t *testing.T) {
	assert.IsType(t, vcpDataRepository{}, VCPDatabaseImpl)
	assert.IsType(t, telemetryDatabaseImpl{}, TelemetryDatabaseImpl)
}

func Test_ReturnsErrorWhenDatabaseTypeIsNil(t *testing.T) {
	ctx := context.Background()
	logger := &mockLogger{}

	storage, err := InitializeDatabase(ctx, nil, logger)
	assert.Error(t, err)
	assert.Nil(t, storage)
	assert.EqualError(t, err, "database type is nil")
}

func Test_ReturnsErrorWhenStorageIsNil(t *testing.T) {
	ctx := context.Background()
	logger := &mockLogger{}
	mdb := new(mockDB)
	mdb.On("GetConnection", ctx, logger).Return(nil, nil)

	storage, err := InitializeDatabase(ctx, mdb, logger)
	assert.Error(t, err)
	assert.Nil(t, storage)
	assert.EqualError(t, err, "database storage is nil")
}

func Test_ReturnsErrorWhenGetConnectionFails(t *testing.T) {
	ctx := context.Background()
	logger := &mockLogger{}
	mdb := new(mockDB)
	expectedError := errors.New("connection failed")
	mdb.On("GetConnection", ctx, logger).Return(nil, expectedError)

	storage, err := InitializeDatabase(ctx, mdb, logger)
	assert.Error(t, err)
	assert.Nil(t, storage)
	assert.Equal(t, expectedError, err)
}

func Test_ReturnsStorageWhenDatabaseTypeAndConnectionAreValid(t *testing.T) {
	ctx := context.Background()
	logger := &mockLogger{}
	mockStorage := new(database.MockStorage)
	mdb := new(mockDB)
	mdb.On("GetConnection", ctx, logger).Return(mockStorage, nil)

	storage, err := InitializeDatabase(ctx, mdb, logger)
	assert.NoError(t, err)
	assert.Equal(t, mockStorage, storage)
	mdb.AssertExpectations(t)
}
