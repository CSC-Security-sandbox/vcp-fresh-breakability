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
	// Add rollback flag
	rollback := flag.Bool("rollback", false, "Rollback the last migration")
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
		AdminUser:       cfg.DBAdminUser,
		AdminPassword:   cfg.DBAdminPassword,
		Logger:          logger,
	}

	storage, err := database.New(dbConfig)
	if err != nil {
		logger.Error("Failed to initialize database", slog.String("error", err.Error()))
		os.Exit(1)
	}

	ctx := context.Background()

	if *rollback {
		if err := storage.Rollback(ctx); err != nil {
			log.Fatalf("Rollback failed: %v", err)
		}
		log.Println("Rollback completed successfully")
	} else {
		if err := storage.Migrate(ctx); err != nil {
			log.Fatalf("Migrations failed: %v", err)
		}
		log.Println("Migrations completed successfully")
	}
}
