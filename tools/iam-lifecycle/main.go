package main

import (
	"context"
	"log/slog"
	"os"
	"time"
)

type config struct {
	iamAuthEnabled bool

	dbHost    string
	dbPort    string
	dbSSLMode string

	adminUser string
	adminPass string

	vcpDBName         string
	metricsDBName     string
	temporalDBName    string
	temporalVisDBName string

	iamVcpCore         string
	iamVcpWorker       string
	iamClhSA           string
	iamTemporal        string
	iamMetricsProducer string

	metricsEnabled  bool
	temporalEnabled bool

	// Port for the second Cloud SQL Proxy that impersonates temporal-ksa.
	// Falls back to dbPort when not set (single-proxy mode).
	temporalDBPort string

	// Cloud SQL instance connection name: "project:region:instance"
	instanceConnName string
}

func loadConfig() config {
	return config{
		iamAuthEnabled: envBool("CLOUD_SQL_IAM_AUTH_ENABLED"),

		dbHost:    envOr("DB_HOST", "127.0.0.1"),
		dbPort:    envOr("DB_PORT", "5432"),
		dbSSLMode: envOr("DB_SSL_MODE", "disable"),

		adminUser: envOr("DB_ADMIN_USER", "postgres"),
		adminPass: os.Getenv("DB_ADMIN_PASSWORD"),

		vcpDBName:         envOr("DB_NAME", "vcp"),
		metricsDBName:     envOr("METRICS_DB_NAME", "metrics"),
		temporalDBName:    envOr("TEMPORAL_DB_NAME", "temporal"),
		temporalVisDBName: envOr("TEMPORAL_VISIBILITY_DB_NAME", "temporal_visibility"),

		iamVcpCore:         os.Getenv("IAM_VCP_CORE"),
		iamVcpWorker:       os.Getenv("IAM_VCP_WORKER"),
		iamClhSA:           os.Getenv("IAM_CLH_SA"),
		iamTemporal:        os.Getenv("IAM_TEMPORAL"),
		iamMetricsProducer: os.Getenv("IAM_METRICS_PRODUCER"),

		metricsEnabled:  envBool("METRICS_ENABLED"),
		temporalEnabled: envBool("TEMPORAL_ENABLED"),

		temporalDBPort: envOr("TEMPORAL_DB_PORT", ""),

		instanceConnName: os.Getenv("INSTANCE_CONNECTION_NAME"),
	}
}

func main() {
	logLevel := slog.LevelInfo
	if os.Getenv("LOG_LEVEL") == "debug" {
		logLevel = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel})))

	os.Exit(run())
}

func run() int {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	go func() {
		<-ctx.Done()
		if ctx.Err() == context.DeadlineExceeded {
			slog.Error("job timeout exceeded - may need to increase deadline")
		}
	}()

	cfg := loadConfig()
	defer shutdownProxy()
	defer cleanupAdminSecret()

	slog.Info("starting IAM lifecycle job", "iam_auth_enabled", cfg.iamAuthEnabled)

	if !cfg.iamAuthEnabled {
		slog.Info("IAM auth disabled — checking if rollback needed")
		if err := rollbackAll(cfg); err != nil {
			slog.Error("rollback failed", "error", err)
			return 1
		}
		return 0
	}

	missing := missingIAMUsers(cfg, false)
	if len(missing) > 0 {
		slog.Error("IAM auth enabled but IAM user env vars not set", "missing", missing)
		return 1
	}

	if err := validateIAMPermissions(ctx, cfg); err != nil {
		slog.Error("IAM permission validation failed", "error", err)
		return 1
	}

	if err := ensureIAMDBUsers(cfg); err != nil {
		slog.Error("failed to ensure IAM DB users", "error", err)
		return 1
	}

	slog.Info("IAM auth enabled — checking if enable needed")
	if err := enableAll(cfg); err != nil {
		slog.Error("enable failed", "error", err)
		return 1
	}
	slog.Info("IAM lifecycle job completed successfully")
	return 0
}

func missingIAMUsers(cfg config, isRollback bool) []string {
	var out []string
	required := []struct{ k, v string }{
		{"IAM_VCP_CORE", cfg.iamVcpCore},
		{"IAM_VCP_WORKER", cfg.iamVcpWorker},
		{"IAM_CLH_SA", cfg.iamClhSA},
		{"IAM_TEMPORAL", cfg.iamTemporal},
	}
	if !isRollback {
		required = append(required, struct{ k, v string }{"IAM_METRICS_PRODUCER", cfg.iamMetricsProducer})
	}
	for _, kv := range required {
		if kv.v == "" {
			out = append(out, kv.k)
		}
	}
	return out
}
