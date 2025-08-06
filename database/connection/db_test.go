package connection_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/common"
	conn "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/connection"
	metricsdb "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/metrics"
	vcpdb "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
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
		DBAdminUser:       "admin",
		DBAdminPassword:   "adminpass",
		MSIEnabled:        false,
		MSIDBUser:         "msiuser",
	}

	// without MSI enabled
	dbConfig := conn.GetDBConfig(cfg)
	if dbConfig.User != cfg.DBUser || dbConfig.AdminUser != cfg.DBAdminUser {
		t.Errorf("expected normal user/admin, got %v / %v", dbConfig.User, dbConfig.AdminUser)
	}

	// with MSI enabled
	cfg.MSIEnabled = true
	dbConfig = conn.GetDBConfig(cfg)
	if dbConfig.User != cfg.MSIDBUser || dbConfig.AdminUser != cfg.MSIDBUser {
		t.Errorf("expected MSI user for both user and admin, got %v / %v", dbConfig.User, dbConfig.AdminUser)
	}
}

func TestGetDbConnection(t *testing.T) {
	// Mock context and logger
	ctx := context.Background()
	logger := log.NewLogger()

	origInitializeDatabase := conn.InitializeVcpDatabase
	// Mock the InitializeDatabase function to return a mock database connection
	conn.InitializeVcpDatabase = func(ctx context.Context, cfg *common.Config, logger log.Logger) (vcpdb.Storage, error) {
		mockDB := &vcpdb.MockStorage{} // Assuming you have a mock implementation
		return mockDB, nil
	}
	defer func() {
		conn.InitializeVcpDatabase = origInitializeDatabase
	}()
	// Call the function to get the database connection
	dbCon, err := conn.GetVcpDbConnection(ctx, logger)
	if err != nil {
		t.Fatalf("Failed to get database connection: %v", err)
	}

	// Check if the connection is not nil
	if dbCon == nil {
		t.Fatal("Expected non-nil database connection, got nil")
	}

	// Check if the connection is of the expected type
	conn.InitializeVcpDatabase = func(ctx context.Context, cfg *common.Config, logger log.Logger) (vcpdb.Storage, error) {
		mockDB := &vcpdb.MockStorage{} // Assuming you have a mock implementation
		return mockDB, errors.New("failed to initialize database")
	}
	_, err = conn.GetVcpDbConnection(ctx, logger)
	assert.NotNil(t, err)
}

func TestGetTelemetryDbConnection(t *testing.T) {
	ctx := context.Background()
	logger := log.NewLogger()

	origInitializeDatabase := conn.InitializeMetricsDatabase
	defer func() { conn.InitializeMetricsDatabase = origInitializeDatabase }()

	t.Run("Successful connection and migration", func(t *testing.T) {
		conn.InitializeMetricsDatabase = func(ctx context.Context, cfg *common.Config, logger log.Logger) (metricsdb.Storage, error) {
			mockDB := &metricsdb.MockStorage{}
			mockDB.On("Migrate", ctx).Return(nil)
			return mockDB, nil
		}

		dbCon, err := conn.GetTelemetryDbConnection(ctx, logger)
		assert.NoError(t, err)
		assert.NotNil(t, dbCon)
	})

	t.Run("InitializeMetricsDatabase fails", func(t *testing.T) {
		conn.InitializeMetricsDatabase = func(ctx context.Context, cfg *common.Config, logger log.Logger) (metricsdb.Storage, error) {
			return nil, errors.New("failed to initialize telemetry database")
		}

		dbCon, err := conn.GetTelemetryDbConnection(ctx, logger)
		assert.Error(t, err)
		assert.Nil(t, dbCon)
		assert.Contains(t, err.Error(), "failed to initialize telemetry database")
	})

	t.Run("Migration fails", func(t *testing.T) {
		conn.InitializeMetricsDatabase = func(ctx context.Context, cfg *common.Config, logger log.Logger) (metricsdb.Storage, error) {
			mockDB := &metricsdb.MockStorage{}
			mockDB.On("Migrate", ctx).Return(errors.New("migration failed"))
			return mockDB, nil
		}

		dbCon, err := conn.GetTelemetryDbConnection(ctx, logger)
		assert.Error(t, err)
		assert.Nil(t, dbCon)
		assert.Contains(t, err.Error(), "migration failed")
	})
}

func TestCloseDatabase(t *testing.T) {
	// Mock context and logger
	logger := log.NewLogger()

	// Mock database connection
	mockDB := &vcpdb.MockStorage{}
	mockDB.On("Close").Return(nil)
	// Call the function to close the database connection
	conn.CloseDatabase(mockDB, logger)
	assert.True(t, mockDB.AssertCalled(t, "Close"))

	// Test error case
	mockDB.On("Close").Return(errors.New("failed to close database"))
	conn.CloseDatabase(mockDB, logger)
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
	}

	originalDoConnect := conn.DoConnect
	// Mock the doConnect function to simulate a successful connection
	conn.DoConnect = func(logger log.Logger, db vcpdb.Storage) error {
		return nil
	}
	defer func() {
		conn.DoConnect = originalDoConnect // Restore original function
	}()

	dbCon, err := conn.InitializeVcpDatabase(ctx, cfg, logger)
	assert.NoError(t, err)
	assert.NotNil(t, dbCon)

	// Test error case
	conn.DoConnect = func(logger log.Logger, db vcpdb.Storage) error {
		return errors.New("failed to initialize database")
	}
	dbCon, err = conn.InitializeVcpDatabase(ctx, cfg, logger)
	assert.Error(t, err)
	assert.Nil(t, dbCon)
}

func TestDoConnect(t *testing.T) {
	logger := log.NewLogger()

	mockDB := vcpdb.MockStorage{}
	t.Run("Successful connection on first attempt", func(t *testing.T) {
		mockDB.On("Connect", false).Return(nil).Once()
		err := conn.DoConnect(logger, &mockDB)
		assert.NoError(t, err)
	})

	t.Run("Successful connection after retries", func(t *testing.T) {
		mockDB.On("Connect", false).Return(errors.New("connection failed")).Once()
		mockDB.On("Connect", false).Return(nil).Once()

		start := time.Now()
		err := conn.DoConnect(logger, &mockDB)
		duration := time.Since(start)

		assert.NoError(t, err)
		assert.GreaterOrEqual(t, duration, 2*time.Second) // Ensure retry delay
		mockDB.AssertExpectations(t)
	})
}

func TestDoConnectMetrics(t *testing.T) {
	logger := log.NewLogger()

	t.Run("Successful connection on first attempt", func(t *testing.T) {
		mockDB := &metricsdb.MockStorage{}
		mockDB.On("Connect", false).Return(nil).Once()
		err := conn.DoConnectMetrics(logger, mockDB)
		assert.NoError(t, err)
		mockDB.AssertExpectations(t)
	})

	t.Run("Successful connection after retries", func(t *testing.T) {
		mockDB := &metricsdb.MockStorage{}
		mockDB.On("Connect", false).Return(errors.New("connection failed")).Once()
		mockDB.On("Connect", false).Return(nil).Once()

		start := time.Now()
		err := conn.DoConnectMetrics(logger, mockDB)
		duration := time.Since(start)

		assert.NoError(t, err)
		assert.GreaterOrEqual(t, duration, 2*time.Second) // Ensure retry delay
		mockDB.AssertExpectations(t)
	})
}

func TestInitializeMetricsDatabase(t *testing.T) {
	ctx := context.Background()
	logger := log.NewLogger()

	cfg := &common.Config{
		MetricsDBType:            "postgres",
		MetricsDBHost:            "localhost",
		MetricsDBUser:            "testuser",
		MetricsDBPassword:        "testpass",
		MetricsDBName:            "testdb",
		MetricsDBSSLMode:         "disable",
		MetricsDBMaxOpenConns:    10,
		MetricsDBMaxIdleConns:    5,
		MetricsDBConnMaxLifetime: 30 * time.Minute,
		DBAdminUser:              "admin",
		DBAdminPassword:          "adminpass",
	}

	t.Run("Successful initialization", func(t *testing.T) {
		originalDoConnectMetrics := conn.DoConnectMetrics
		conn.DoConnectMetrics = func(logger log.Logger, db metricsdb.Storage) error {
			return nil
		}
		defer func() {
			conn.DoConnectMetrics = originalDoConnectMetrics
		}()

		dbCon, err := conn.InitializeMetricsDatabase(ctx, cfg, logger)
		assert.NoError(t, err)
		assert.NotNil(t, dbCon)
	})

	t.Run("DoConnectMetrics fails", func(t *testing.T) {
		originalDoConnectMetrics := conn.DoConnectMetrics
		conn.DoConnectMetrics = func(logger log.Logger, db metricsdb.Storage) error {
			return errors.New("failed to connect to the database")
		}
		defer func() {
			conn.DoConnectMetrics = originalDoConnectMetrics
		}()

		dbCon, err := conn.InitializeMetricsDatabase(ctx, cfg, logger)
		assert.Error(t, err)
		assert.Nil(t, dbCon)
		assert.Contains(t, err.Error(), "failed to connect to the database")
	})
}
