package database

import (
	"context"
	"github.com/google/uuid"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/worker/db"
	"os/signal"
	"syscall"
)

func InitializeDatabase() orchestrator.OrchestratorFactory {
	ctx := context.WithValue(context.Background(), middleware.CorrelationContextKey, uuid.NewString())
	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	cfg := common.LoadConfig()
	logger := log.NewLogger()
	logger.Info("Initializing telemetry database connection", "dbType", cfg.DBType, "dbHost", cfg.DBHost, "dbName", cfg.DBName)
	db, err := database.InitializeDatabase(ctx, cfg, logger)
	if err != nil {
		logger.Error("Failed to initialize database for telemetry", "error", err.Error())
		return nil
	}
	orch := orchestrator.GetNewOrchestrator(db, nil)
	return orch
}
