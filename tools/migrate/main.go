package main

import (
	"context"
	"flag"
	"log"
	"os"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/common"
	_ "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/drivers/postgres"
	metricsdb "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/metrics"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	slogger "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

var (
	metricsEnabled = env.GetBool("METRICS_ENABLED", false)
)

func main() {
	rollback := flag.Bool("rollback", false, "Rollback the last migration")
	migrate := flag.Bool("migrate", false, "Run database migrations")
	setupDB := flag.Bool("setup", false, "Setup database infrastructure (requires admin credentials)")
	adminUser := flag.String("admin-user", "postgres", "Admin username for setup")
	adminPass := flag.String("admin-pass", "", "Admin password for setup")
	flag.Parse()

	logger := slogger.NewLogger()
	cfg := common.LoadConfig()

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
		AdminUser:       *adminUser,
		AdminPassword:   *adminPass,
	}

	metricsDbConfig := dbutils.DbConfig{
		Type:            cfg.MetricsDBType,
		Host:            cfg.MetricsDBHost,
		Port:            cfg.MetricsDBPort,
		User:            cfg.MetricsDBUser,
		Password:        cfg.MetricsDBPassword,
		Name:            cfg.MetricsDBName,
		SSLMode:         cfg.DBSSLMode,
		TimeZone:        cfg.DBTimeZone.String(),
		MaxOpenConns:    cfg.DBMaxOpenConns,
		MaxIdleConns:    cfg.DBMaxIdleConns,
		ConnMaxLifetime: cfg.DBConnMaxLifetime,
		AdminUser:       *adminUser,
		AdminPassword:   *adminPass,
	}

	ctx := context.Background()

	switch {
	case *setupDB:
		if err := setupDatabase(ctx, dbConfig, metricsDbConfig, logger); err != nil {
			logger.Error("Database setup failed", "error", err.Error())
			os.Exit(1)
		}
		log.Println("Database infrastructure setup completed successfully")
	case *rollback:
		if err := performRollback(ctx, dbConfig, metricsDbConfig, logger); err != nil {
			logger.Error("Rollback failed", "error", err.Error())
			os.Exit(1)
		}
		log.Println("Rollback completed successfully")
	case *migrate:
		if err := performMigration(ctx, dbConfig, metricsDbConfig, logger); err != nil {
			logger.Error("Migrations failed", "error", err.Error())
			os.Exit(1)
		}
		log.Println("Migrations completed successfully")
	default:
		flag.Usage()
		log.Fatal("No operation specified")
	}
}

func setupDatabase(ctx context.Context, dbConfig dbutils.DbConfig, metricsDBConfig dbutils.DbConfig, logger slogger.Logger) error {
	// Setup main VCP database
	dbConfig.Name = common.DefaultDB
	storage, err := database.New(dbConfig, logger)
	if err != nil {
		return err
	}
	if err := storage.SetupDatabase(ctx); err != nil {
		return err
	}
	log.Println("VCP database setup completed successfully")

	if !metricsEnabled {
		log.Println("Metrics disabled... skipping metrics database setup")
		return nil
	}

	metricsDBConfig.Name = common.DefaultDB
	metricsStorage, err := metricsdb.New(metricsDBConfig, logger)
	if err != nil {
		return err
	}

	if err := metricsStorage.SetupDatabase(ctx); err != nil {
		return err
	}

	log.Println("Metrics database setup completed successfully")

	return nil
}

func performRollback(ctx context.Context, dbConfig dbutils.DbConfig, metricsDBConfig dbutils.DbConfig, logger slogger.Logger) error {
	// Rollback VCP database
	storage, err := database.New(dbConfig, logger)
	if err == nil {
		err = storage.Connect(true)
	}
	if err != nil {
		return err
	}
	defer func(storage database.Storage) {
		err := storage.Close()
		if err != nil {
			logger.Error("Failed to close VCP database connection", "error", err.Error())
		}
	}(storage)

	if err := storage.Rollback(ctx); err != nil {
		return err
	}
	log.Println("VCP database rollback completed successfully")

	if !metricsEnabled {
		log.Println("Metrics disabled... skipping metrics rollback")
		return nil
	}
	// Rollback metrics database
	metricsStorage, err := metricsdb.New(metricsDBConfig, logger)
	if err == nil {
		err = metricsStorage.Connect(true)
	}
	if err != nil {
		return err
	}
	defer func(metricsStorage metricsdb.Storage) {
		err := metricsStorage.Close()
		if err != nil {
			logger.Error("Failed to close metrics database connection", "error", err.Error())
		}
	}(metricsStorage)

	if err := metricsStorage.Rollback(ctx); err != nil {
		return err
	}
	log.Println("Metrics database rollback completed successfully")

	return nil
}

func performMigration(ctx context.Context, dbConfig dbutils.DbConfig, metricsDBConfig dbutils.DbConfig, logger slogger.Logger) error {
	// Migrate VCP database
	storage, err := database.New(dbConfig, logger)
	if err == nil {
		err = storage.Connect(true)
	}
	if err != nil {
		return err
	}
	defer func(storage database.Storage) {
		err := storage.Close()
		if err != nil {
			logger.Error("Failed to close VCP database connection", "error", err.Error())
		}
	}(storage)

	if err := storage.Migrate(ctx); err != nil {
		return err
	}
	log.Println("VCP database migration completed successfully")

	if !env.GetBool("METRICS_ENABLED", false) {
		return nil
	}

	if !metricsEnabled {
		log.Println("Metrics disabled... skipping metrics migration")
		return nil
	}
	// Migrate metrics database
	metricsStorage, err := metricsdb.New(metricsDBConfig, logger)
	if err == nil {
		err = metricsStorage.Connect(true)
	}
	if err != nil {
		return err
	}
	defer func(metricsStorage metricsdb.Storage) {
		err := metricsStorage.Close()
		if err != nil {
			logger.Error("Failed to close metrics database connection", "error", err.Error())
		}
	}(metricsStorage)

	if err := metricsStorage.Migrate(ctx); err != nil {
		return err
	}
	log.Println("Metrics database migration completed successfully")

	return nil
}
