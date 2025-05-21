package db

import (
	"context"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"time"
)

// GetDBConfig retrieves the database configuration from the common config.
var GetDBConfig = func(cfg *common.Config) database.DbConfig {
	dbConfig := database.DbConfig{
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
		MigrationPath:   cfg.MigrationPath,
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

// InitializeDatabase initializes the database connection using the provided configuration and logger.
var InitializeDatabase = func(ctx context.Context, cfg *common.Config, logger log.Logger) (database.Storage, error) {
	dbConfig := GetDBConfig(cfg)
	db, err := database.New(dbConfig, logger)
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

var DoConnect = func(logger log.Logger, db database.Storage) error {
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

// GetDbConnection retrieves the database connection using the provided context and logger.
func GetDbConnection(ctx context.Context, logger log.Logger) (database.Storage, error) {
	cfg := common.LoadConfig()
	dbCon, err := InitializeDatabase(ctx, cfg, logger)
	if err != nil {
		logger.Error("Failed to initialize database", "error", err.Error())
		return nil, err
	}
	return dbCon, nil
}

// CloseDatabase closes the database connection and logs any errors encountered.
func CloseDatabase(dbCon database.Storage, logger log.Logger) {
	if err := dbCon.Close(); err != nil {
		logger.Error("Failed to close database connection", "error", err.Error())
	}
}
