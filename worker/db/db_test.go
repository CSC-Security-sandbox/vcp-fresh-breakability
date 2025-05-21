package db_test

import (
	"context"
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/worker/db"
	"testing"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/common"
)

func TestGetDBConfig(t *testing.T) {
	cfg := &common.Config{
		DBType:            "postgres",
		DBHost:            "localhost",
		DBUser:            "testuser",
		DBPassword:        "testpass",
		DBName:            "testdb",
		DBSSLMode:         "disable",
		DBMaxOpenConns:    10,
		DBMaxIdleConns:    5,
		DBConnMaxLifetime: 30 * time.Minute,
		MigrationPath:     "./migrations",
		DBAdminUser:       "admin",
		DBAdminPassword:   "adminpass",
		MSIEnabled:        false,
		MSIDBUser:         "msiuser",
	}

	// without MSI enabled
	dbConfig := db.GetDBConfig(cfg)
	if dbConfig.User != cfg.DBUser || dbConfig.AdminUser != cfg.DBAdminUser {
		t.Errorf("expected normal user/admin, got %v / %v", dbConfig.User, dbConfig.AdminUser)
	}

	// with MSI enabled
	cfg.MSIEnabled = true
	dbConfig = db.GetDBConfig(cfg)
	if dbConfig.User != cfg.MSIDBUser || dbConfig.AdminUser != cfg.MSIDBUser {
		t.Errorf("expected MSI user for both user and admin, got %v / %v", dbConfig.User, dbConfig.AdminUser)
	}
}

func TestGetDbConnection(t *testing.T) {
	// Mock context and logger
	ctx := context.Background()
	logger := log.NewLogger()

	origInitializeDatabase := db.InitializeDatabase
	// Mock the InitializeDatabase function to return a mock database connection
	db.InitializeDatabase = func(ctx context.Context, cfg *common.Config, logger log.Logger) (database.Storage, error) {
		mockDB := &database.MockStorage{} // Assuming you have a mock implementation
		return mockDB, nil
	}
	defer func() {
		db.InitializeDatabase = origInitializeDatabase
	}()
	// Call the function to get the database connection
	dbCon, err := db.GetDbConnection(ctx, logger)
	if err != nil {
		t.Fatalf("Failed to get database connection: %v", err)
	}

	// Check if the connection is not nil
	if dbCon == nil {
		t.Fatal("Expected non-nil database connection, got nil")
	}

	// Check if the connection is of the expected type
	db.InitializeDatabase = func(ctx context.Context, cfg *common.Config, logger log.Logger) (database.Storage, error) {
		mockDB := &database.MockStorage{} // Assuming you have a mock implementation
		return mockDB, errors.New("Failed to initialize database")
	}
	_, err = db.GetDbConnection(ctx, logger)
	assert.NotNil(t, err)
}

func TestCloseDatabase(t *testing.T) {
	// Mock context and logger
	logger := log.NewLogger()

	// Mock database connection
	mockDB := &database.MockStorage{}
	mockDB.On("Close").Return(nil)
	// Call the function to close the database connection
	db.CloseDatabase(mockDB, logger)
	assert.True(t, mockDB.AssertCalled(t, "Close"))

	// Test error case
	mockDB.On("Close").Return(errors.New("failed to close database"))
	db.CloseDatabase(mockDB, logger)
	assert.True(t, mockDB.AssertCalled(t, "Close"))
}

func TestInitializeDatabase(t *testing.T) {
	// Mock context and logger
	ctx := context.Background()
	logger := log.NewLogger()

	// Mock configuration
	cfg := &common.Config{
		DBType:            "postgres",
		DBHost:            "localhost",
		DBUser:            "testuser",
		DBPassword:        "testpass",
		DBName:            "testdb",
		DBSSLMode:         "disable",
		DBMaxOpenConns:    10,
		DBMaxIdleConns:    5,
		DBConnMaxLifetime: 30 * time.Minute,
		MigrationPath:     "./migrations",
	}

	originalDoConnect := db.DoConnect
	// Mock the doConnect function to simulate a successful connection
	db.DoConnect = func(logger log.Logger, db database.Storage) error {
		return nil
	}
	defer func() {
		db.DoConnect = originalDoConnect // Restore original function
	}()

	dbCon, err := db.InitializeDatabase(ctx, cfg, logger)
	assert.NoError(t, err)
	assert.NotNil(t, dbCon)

	// Test error case
	db.DoConnect = func(logger log.Logger, db database.Storage) error {
		return errors.New("failed to initialize database")
	}
	dbCon, err = db.InitializeDatabase(ctx, cfg, logger)
	assert.Error(t, err)
	assert.Nil(t, dbCon)
}

type MockStorage struct {
	mock.Mock
}

func (m *MockStorage) Connect(isAdmin bool) error {
	args := m.Called(isAdmin)
	return args.Error(0)
}

func TestDoConnect(t *testing.T) {
	logger := log.NewLogger()

	mockDB := database.MockStorage{}
	t.Run("Successful connection on first attempt", func(t *testing.T) {
		mockDB.On("Connect", false).Return(nil).Once()
		err := db.DoConnect(logger, &mockDB)
		assert.NoError(t, err)
	})

	t.Run("Successful connection after retries", func(t *testing.T) {
		mockDB.On("Connect", false).Return(errors.New("connection failed")).Once()
		mockDB.On("Connect", false).Return(nil).Once()

		start := time.Now()
		err := db.DoConnect(logger, &mockDB)
		duration := time.Since(start)

		assert.NoError(t, err)
		assert.GreaterOrEqual(t, duration, 2*time.Second) // Ensure retry delay
		mockDB.AssertExpectations(t)
	})
}
