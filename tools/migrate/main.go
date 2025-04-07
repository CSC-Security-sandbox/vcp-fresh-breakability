package main

import (
	"context"
	"flag"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/common"
	"golang.org/x/exp/slog"
	"log"
	"os"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	_ "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/postgres"
)

func main() {
	rollback := flag.Bool("rollback", false, "Rollback the last migration")
	migrate := flag.Bool("migrate", false, "Run database migrations")
	setupDB := flag.Bool("setup", false, "Setup database infrastructure (requires admin credentials)")
	adminUser := flag.String("admin-user", "postgres", "Admin username for setup")
	adminPass := flag.String("admin-pass", "", "Admin password for setup")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg := common.LoadConfig()

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
		AdminUser:       *adminUser,
		AdminPassword:   *adminPass,
	}

	ctx := context.Background()

	switch {
	case *setupDB:
		if err := setupDatabase(ctx, dbConfig, logger); err != nil {
			logger.Error("Database setup failed", slog.String("error", err.Error()))
			os.Exit(1)
		}
		log.Println("Database infrastructure setup completed successfully")
	case *rollback:
		if err := performRollback(ctx, dbConfig, logger); err != nil {
			logger.Error("Rollback failed", slog.String("error", err.Error()))
			os.Exit(1)
		}
		log.Println("Rollback completed successfully")
	case *migrate:
		if err := performMigration(ctx, dbConfig, logger); err != nil {
			logger.Error("Migrations failed", slog.String("error", err.Error()))
			os.Exit(1)
		}
		log.Println("Migrations completed successfully")
	default:
		flag.Usage()
		log.Fatal("No operation specified")
	}
}

func setupDatabase(ctx context.Context, dbConfig database.DbConfig, logger *slog.Logger) error {
	storage, err := database.New(dbConfig, logger)
	if err != nil {
		return err
	}
	return storage.SetupDatabase(ctx)
}

func performRollback(ctx context.Context, dbConfig database.DbConfig, logger *slog.Logger) error {
	storage, err := database.New(dbConfig, logger)
	if err == nil {
		err = storage.Connect()
	}
	if err != nil {
		return err
	}
	return storage.Rollback(ctx)
}

func performMigration(ctx context.Context, dbConfig database.DbConfig, logger *slog.Logger) error {
	storage, err := database.New(dbConfig, logger)
	if err == nil {
		err = storage.Connect()
	}
	if err != nil {
		return err
	}
	return storage.Migrate(ctx)
}
