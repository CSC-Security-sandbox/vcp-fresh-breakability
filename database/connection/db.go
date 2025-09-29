package connection

import (
	"context"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/common"
	metricsdb "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/metrics"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	vcpdb "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

var (
	InitializeVcpDatabase     = initializeVcpDatabase
	DoConnect                 = doConnect
	DoConnectMetrics          = doConnectMetrics
	InitializeMetricsDatabase = initializeMetricsDatabase
)

// GetDBConfig retrieves the database configuration from the common config.
func GetDBConfig(cfg *common.Config) dbutils.DbConfig {
	dbConfig := dbutils.DbConfig{
		Type:            cfg.DBType,
		Host:            cfg.DBHost,
		Port:            cfg.DBPort,
		User:            cfg.DBUser,
		Password:        cfg.DBPassword,
		Name:            cfg.DBName,
		SSLMode:         cfg.DBSSLMode,
		TimeZone:        cfg.DBTimeZone.String(),
		MaxOpenConns:    cfg.DBMaxOpenConns,
		MaxIdleConns:    cfg.DBMaxIdleConns,
		ConnMaxLifetime: cfg.DBConnMaxLifetime,
		AdminUser:       cfg.DBAdminUser,
		AdminPassword:   cfg.DBAdminPassword,
	}
	if cfg.MSIEnabled {
		// When MSI is enabled, set the database user credentials from MSIDBUser
		dbConfig.User = cfg.MSIDBUser
		dbConfig.AdminUser = cfg.MSIDBUser
	}
	return dbConfig
}

// GetMetricsDBConfig retrieves the metrics database configuration from the common config.
func GetMetricsDBConfig(cfg *common.Config) dbutils.DbConfig {
	dbConfig := dbutils.DbConfig{
		Type:            cfg.MetricsDBType,
		Host:            cfg.MetricsDBHost,
		Port:            cfg.MetricsDBPort,
		User:            cfg.MetricsDBUser,
		Password:        cfg.MetricsDBPassword,
		Name:            cfg.MetricsDBName,
		SSLMode:         cfg.MetricsDBSSLMode,
		TimeZone:        cfg.MetricsDBTimeZone.String(),
		MaxOpenConns:    cfg.MetricsDBMaxOpenConns,
		MaxIdleConns:    cfg.MetricsDBMaxIdleConns,
		ConnMaxLifetime: cfg.MetricsDBConnMaxLifetime,
		AdminUser:       cfg.DBAdminUser, // Use main DB admin for metrics DB if needed
		AdminPassword:   cfg.DBAdminPassword,
	}
	return dbConfig
}

// InitializeDatabase initializes the database connection using the provided configuration and logger.
func initializeVcpDatabase(ctx context.Context, cfg *common.Config, logger log.Logger) (vcpdb.Storage, error) {
	dbConfig := GetDBConfig(cfg)
	db, err := vcpdb.New(dbConfig, logger)
	if err != nil {
		return nil, err
	}

	err = DoConnect(logger, db)
	if err != nil {
		logger.Error("Failed to connect to the database", "error", err.Error())
		return nil, err
	}

	return db, nil
}

// InitializeDatabase initializes the database connection using the provided configuration and logger.
func initializeMetricsDatabase(ctx context.Context, cfg *common.Config, logger log.Logger) (metricsdb.Storage, error) {
	dbConfig := GetMetricsDBConfig(cfg)
	db, err := metricsdb.New(dbConfig, logger)
	if err != nil {
		return nil, err
	}

	err = DoConnectMetrics(logger, db)
	if err != nil {
		logger.Error("Failed to connect to the database", "error", err.Error())
		return nil, err
	}

	return db, nil
}

func doConnect(logger log.Logger, db vcpdb.Storage) error {
	for {
		err := db.Connect(false)
		if err == nil {
			break
		}
		logger.Error("Failed to connect to the database, retrying...", "error", err.Error())
		time.Sleep(2 * time.Second) // Add a delay between retries to avoid overwhelming the database
	}
	return nil
}

func doConnectMetrics(logger log.Logger, db metricsdb.Storage) error {
	for {
		err := db.Connect(false)
		if err == nil {
			break
		}
		logger.Error("Failed to connect to the database, retrying...", "error", err.Error())
		time.Sleep(2 * time.Second) // Add a delay between retries to avoid overwhelming the database
	}
	return nil
}

// GetVcpDbConnection retrieves the database connection using the provided context and logger.
func GetVcpDbConnection(ctx context.Context, logger log.Logger) (vcpdb.Storage, error) {
	cfg := common.LoadConfig()
	dbCon, err := InitializeVcpDatabase(ctx, cfg, logger)
	if err != nil {
		logger.Error("Failed to initialize database", "error", err.Error())
		return nil, err
	}
	return dbCon, nil
}

// GetTelemetryDbConnection retrieves the database connection using the provided context and logger.
func GetTelemetryDbConnection(ctx context.Context, logger log.Logger) (metricsdb.Storage, error) {
	cfg := common.LoadTelemetryConfig()
	dbCon, err := InitializeMetricsDatabase(ctx, cfg, logger)
	if err != nil {
		logger.Error("Failed to initialize telemetry database", "error", err.Error())
		return nil, err
	}
	return dbCon, nil
}

// CloseDatabase closes the database connection and logs any errors encountered.
func CloseDatabase(dbCon vcpdb.Storage, logger log.Logger) {
	if err := dbCon.Close(); err != nil {
		logger.Error("Failed to close database connection", "error", err.Error())
	}
}

// CloseTelemetryDatabase closes the database telemetry connection and logs any errors encountered.
func CloseTelemetryDatabase(dbCon metricsdb.Storage, logger log.Logger) {
	if err := dbCon.Close(); err != nil {
		logger.Error("Failed to close telemetry database connection", "error", err.Error())
	}
}
