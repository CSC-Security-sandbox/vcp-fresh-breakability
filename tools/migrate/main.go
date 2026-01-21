package main

import (
	"context"
	"flag"
	"log"
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
)

// shutdownCloudSQLProxy gracefully shuts down the Cloud SQL Proxy sidecar
// by sending a POST request to the admin endpoint (--quitquitquit).
// This uses raw TCP to avoid requiring curl or shell utilities (distroless-friendly).
func shutdownCloudSQLProxy() {
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
		log.Printf("Failed to connect to Cloud SQL Proxy admin endpoint (%s): %v", address, err)
		return
	}
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Failed to close connection to Cloud SQL Proxy admin endpoint: %v", err)
		}
	}()

	// Send minimal HTTP POST request to /quitquitquit
	// This triggers graceful shutdown of the proxy sidecar
	request := "POST /quitquitquit HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
	if _, err := conn.Write([]byte(request)); err != nil {
		log.Printf("Failed to send shutdown request to Cloud SQL Proxy: %v", err)
		return
	}

	// Read response (optional, but good practice)
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		log.Printf("Failed to set read deadline: %v", err)
		return
	}
	buffer := make([]byte, 1024)
	_, _ = conn.Read(buffer) // Ignore response, just ensure connection is closed

	log.Println("Cloud SQL Proxy shutdown request sent successfully")
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
	defer shutdownCloudSQLProxy()

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
		log.Println("Database infrastructure setup completed successfully")
		return 0
	case *rollback:
		if err := performRollback(ctx, dbConfig, metricsDbConfig, logger); err != nil {
			logger.Error("Rollback failed", "error", err.Error())
			return 1
		}
		log.Println("Rollback completed successfully")
		return 0
	case *migrate:
		if err := performMigration(ctx, dbConfig, metricsDbConfig, logger); err != nil {
			logger.Error("Migrations failed", "error", err.Error())
			return 1
		}
		log.Println("Migrations completed successfully")
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
