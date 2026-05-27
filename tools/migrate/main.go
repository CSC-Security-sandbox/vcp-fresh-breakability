package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"time"

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
	// Database connection retry configuration (configurable via environment variables)
	connectRetryAttempts  = env.GetInt("DB_CONNECT_RETRY_ATTEMPTS", 10)
	connectRetryBaseDelay = time.Duration(env.GetInt("DB_CONNECT_RETRY_BASE_DELAY_MS", 500)) * time.Millisecond
	connectRetryMaxDelay  = time.Duration(env.GetInt("DB_CONNECT_RETRY_MAX_DELAY_MS", 30000)) * time.Millisecond
)

// connectWithRetry attempts to connect to the database with retry logic.
// When using Cloud SQL Proxy sidecar, the proxy may take 1-2 seconds to start.
// This function retries connection attempts with exponential backoff.
func connectWithRetry(storage interface{ Connect(bool) error }, isAdmin bool, logger slogger.Logger) error {
	var lastErr error
	for attempt := 0; attempt < connectRetryAttempts; attempt++ {
		err := storage.Connect(isAdmin)
		if err == nil {
			if attempt > 0 {
				logger.Info("Database connection succeeded after retries", "attempts", attempt)
			}
			return nil
		}

		lastErr = err

		// Calculate exponential backoff delay
		delay := connectRetryBaseDelay * time.Duration(1<<uint(attempt))
		if delay > connectRetryMaxDelay {
			delay = connectRetryMaxDelay
		}

		logger.Warn("Database connection failed, retrying",
			"attempt", attempt+1,
			"max_retries", connectRetryAttempts,
			"error", err.Error(),
			"retry_delay", delay.String())
		time.Sleep(delay)
	}

	return fmt.Errorf("failed to connect to database after %d attempts: %w", connectRetryAttempts, lastErr)
}

// shutdownCloudSQLProxy gracefully shuts down the Cloud SQL Proxy sidecar
// by sending a POST request to the admin endpoint (--quitquitquit).
// This uses raw TCP to avoid requiring curl or shell utilities (distroless-friendly).
func shutdownCloudSQLProxy(logger slogger.Logger) {
	// Only attempt shutdown if IAM auth is enabled (indicates sidecar is present)
	iamAuthEnabled := env.GetBool("CLOUD_SQL_IAM_AUTH_ENABLED", false)
	if !iamAuthEnabled {
		return
	}

	// Cloud SQL Proxy admin endpoint (default port 9091)
	adminPort := env.GetString("CLOUD_SQL_PROXY_ADMIN_PORT", "9091")
	address := net.JoinHostPort("127.0.0.1", adminPort)

	// Connect with timeout
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		logger.Warn("Failed to connect to Cloud SQL Proxy admin endpoint", "address", address, "error", err.Error())
		return
	}
	defer func() {
		if err := conn.Close(); err != nil {
			logger.Warn("Failed to close connection to Cloud SQL Proxy admin endpoint", "error", err.Error())
		}
	}()

	// Send minimal HTTP POST request to /quitquitquit
	// This triggers graceful shutdown of the proxy sidecar
	request := "POST /quitquitquit HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
	if _, err := conn.Write([]byte(request)); err != nil {
		logger.Warn("Failed to send shutdown request to Cloud SQL Proxy", "error", err.Error())
		return
	}

	// Read response (optional, but good practice)
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		logger.Warn("Failed to set read deadline", "error", err.Error())
		return
	}
	buffer := make([]byte, 1024)
	_, _ = conn.Read(buffer) // Ignore response, just ensure connection is closed

	logger.Info("Cloud SQL Proxy shutdown request sent successfully")
}

func main() {
	os.Exit(run())
}

func run() int {
	rollback := flag.Bool("rollback", false, "Rollback the last migration")
	migrate := flag.Bool("migrate", false, "Run database migrations")
	setupDB := flag.Bool("setup", false, "Setup database infrastructure (requires admin credentials)")
	adminUser := flag.String("admin-user", "postgres", "Admin username for setup")
	adminPass := flag.String("admin-pass", "", "Admin password for setup")
	flag.Parse()

	logger := slogger.NewLogger()
	cfg := common.LoadConfig()

	// Ensure Cloud SQL Proxy sidecar is shut down on all exit paths (best-effort).
	// NOTE: this must live in a function that returns normally; os.Exit would skip defers.
	defer shutdownCloudSQLProxy(logger)

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
			return 1
		}
		logger.Info("Database infrastructure setup completed successfully")
		return 0
	case *rollback:
		if err := performRollback(ctx, dbConfig, metricsDbConfig, logger); err != nil {
			logger.Error("Rollback failed", "error", err.Error())
			return 1
		}
		logger.Info("Rollback completed successfully")
		return 0
	case *migrate:
		if err := performMigration(ctx, dbConfig, metricsDbConfig, logger); err != nil {
			logger.Error("Migrations failed", "error", err.Error())
			return 1
		}
		logger.Info("Migrations completed successfully")
		return 0
	default:
		flag.Usage()
		logger.Error("No operation specified")
		return 2
	}
}

func setupDatabase(ctx context.Context, dbConfig dbutils.DbConfig, metricsDBConfig dbutils.DbConfig, logger slogger.Logger) error {
	// Setup main VCP database
	dbConfig.Name = common.DefaultDB
	storage, err := database.New(dbConfig, logger)
	if err != nil {
		return err
	}
	// Use retry logic to handle Cloud SQL Proxy startup delay
	if err := connectWithRetry(storage, true, logger); err != nil {
		return err
	}
	defer func(storage database.Storage) {
		err := storage.Close()
		if err != nil {
			logger.Error("Failed to close VCP database connection", "error", err.Error())
		}
	}(storage)

	if err := storage.SetupDatabase(ctx); err != nil {
		return err
	}
	logger.Info("VCP database setup completed successfully")

	if !metricsEnabled {
		logger.Info("Metrics disabled, skipping metrics database setup")
		return nil
	}

	metricsDBConfig.Name = common.DefaultDB
	metricsStorage, err := metricsdb.New(metricsDBConfig, logger)
	if err != nil {
		return err
	}
	// Use retry logic to handle Cloud SQL Proxy startup delay
	if err := connectWithRetry(metricsStorage, true, logger); err != nil {
		return err
	}
	defer func(metricsStorage metricsdb.Storage) {
		err := metricsStorage.Close()
		if err != nil {
			logger.Error("Failed to close metrics database connection", "error", err.Error())
		}
	}(metricsStorage)

	if err := metricsStorage.SetupDatabase(ctx); err != nil {
		return err
	}

	logger.Info("Metrics database setup completed successfully")

	return nil
}

func performRollback(ctx context.Context, dbConfig dbutils.DbConfig, metricsDBConfig dbutils.DbConfig, logger slogger.Logger) error {
	// Rollback VCP database
	storage, err := database.New(dbConfig, logger)
	if err != nil {
		return err
	}
	// Use retry logic to handle Cloud SQL Proxy startup delay
	if err := connectWithRetry(storage, true, logger); err != nil {
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
	logger.Info("VCP database rollback completed successfully")

	if !metricsEnabled {
		logger.Info("Metrics disabled, skipping metrics rollback")
		return nil
	}
	// Rollback metrics database
	metricsStorage, err := metricsdb.New(metricsDBConfig, logger)
	if err != nil {
		return err
	}
	// Use retry logic to handle Cloud SQL Proxy startup delay
	if err := connectWithRetry(metricsStorage, true, logger); err != nil {
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
	logger.Info("Metrics database rollback completed successfully")

	return nil
}

func performMigration(ctx context.Context, dbConfig dbutils.DbConfig, metricsDBConfig dbutils.DbConfig, logger slogger.Logger) error {
	// Migrate VCP database
	storage, err := database.New(dbConfig, logger)
	if err != nil {
		return err
	}
	// Use retry logic to handle Cloud SQL Proxy startup delay
	if err := connectWithRetry(storage, true, logger); err != nil {
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
	logger.Info("VCP database migration completed successfully")

	if !env.GetBool("METRICS_ENABLED", false) {
		return nil
	}

	if !metricsEnabled {
		logger.Info("Metrics disabled, skipping metrics migration")
		return nil
	}
	// Migrate metrics database
	metricsStorage, err := metricsdb.New(metricsDBConfig, logger)
	if err != nil {
		return err
	}
	// Use retry logic to handle Cloud SQL Proxy startup delay
	if err := connectWithRetry(metricsStorage, true, logger); err != nil {
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
	logger.Info("Metrics database migration completed successfully")

	return nil
}
