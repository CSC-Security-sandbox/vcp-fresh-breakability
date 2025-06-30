package common

import (
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"golang.org/x/exp/slog"
)

const (
	DefaultDB = "postgres"
)

type Config struct {
	// Server configuration
	GCPPort             string
	GCPHost             string
	CorePort            string
	ReadTimeout         time.Duration
	WriteTimeout        time.Duration
	IdleTimeout         time.Duration
	ReadHeaderTimeout   time.Duration
	RunMigrationOnStart bool
	MSIEnabled          bool

	// Database connection details
	DBType     string // Type of database (e.g., "postgres", "mysql", "sqlite", "mssql").
	DBHost     string // Database hostname or IP (e.g., "localhost", "127.0.0.1", "db.example.com").
	DBPort     string // Database port (e.g., "5432" for PostgreSQL, "3306" for MySQL, "1433" for MSSQL).
	DBUser     string // Database username.
	DBPassword string // Database password.
	DBName     string // Database name.
	DBSSLMode  string
	// Possible values:
	// "disable" - No SSL.
	// "allow"   - Try SSL, fallback to non-SSL if unavailable.
	// "prefer"  - Prefer SSL, but fallback to non-SSL if necessary.
	// "require" - Always use SSL; no verification.
	// "verify-ca" - Use SSL & verify CA certificate.
	// "verify-full" - Use SSL, verify CA & hostname (most secure, recommended for production).

	DBTimeZone *time.Location // Time zone for the database connection (e.g., "UTC", "Asia/Kolkata").
	// Connection pool settings
	DBMaxOpenConns    int           // Maximum number of open connections (0 = unlimited).
	DBMaxIdleConns    int           // Maximum number of idle connections (default: 2).
	DBConnMaxLifetime time.Duration // Maximum time a connection can be reused before closing (e.g., 30m, 1h).

	// Credential files (used for security)
	CredentialPath string // Path to a credentials file (e.g., service account JSON for GCP, AWS).
	UsernameFile   string // Path to a file containing the database username.
	PasswordFile   string // Path to a file containing the database password.

	// Migration settings
	MigrationPath string // Path to database migration files (e.g., "./migrations").

	// Admin database credentials (Use only when necessary)
	DBAdminUser     string // Admin user for privileged operations.
	DBAdminPassword string // Admin password (Avoid hardcoding, use secret management tools).
	MSIDBUser       string // MSI user

	// Admin job specifications configuration
	RefreshAdminJobSpecs bool
}

func LoadConfig() *Config {
	gcpPort := env.GetString("GCP_PROXY_PORT", "8080")
	gcpHost := env.GetString("GCP_PROXY_HOST", "")
	corePort := env.GetString("CORE_API_PORT", "8081")
	readTimeout := parseDuration(env.GetString("READ_TIMEOUT", "5s"))
	writeTimeout := parseDuration(env.GetString("WRITE_TIMEOUT", "10s"))
	idleTimeout := parseDuration(env.GetString("IDLE_TIMEOUT", "120s"))
	readHeaderTimeout := parseDuration(env.GetString("READ_HEADER_TIMEOUT", "2s"))

	runMigrationOnStart := env.GetBool("RUN_MIGRATION_ON_START", false)
	msiEnabled := env.GetBool("MSI_ENABLED", false)
	dbType := env.GetString("DB_TYPE", "postgres")
	dbHost := env.GetString("DB_HOST", "")
	dbPort := env.GetString("DB_PORT", "5432")
	dbUser := env.GetString("DB_USER", "")
	dbPassword := env.GetString("DB_PASSWORD", "")
	dbName := env.GetString("DB_NAME", "")
	dbSSLMode := env.GetString("DB_SSL_MODE", "disable")
	dbTimeZone := env.GetString("DB_TIMEZONE", "UTC")
	dbMaxOpenConns := env.GetInt("DB_MAX_OPEN_CONNS", 25)
	dbMaxIdleConns := env.GetInt("DB_MAX_IDLE_CONNS", 25)
	dbConnMaxLifetime := parseDuration(env.GetString("DB_CONN_MAX_LIFETIME", "1h"))
	MigrationPath := env.GetString("MIGRATION_PATH", "migrations/core")
	dbAdminUser := env.GetString("DB_ADMIN_USER", "")
	dbAdminPassword := env.GetString("DB_ADMIN_PASSWORD", "")
	dbMSIUser := env.GetString("DB_MSI_USER", "")
	refreshAdminJobSpecs := env.GetBool("REFRESH_ADMIN_JOB_SPECS", false)

	location, err := time.LoadLocation(dbTimeZone)
	if err != nil {
		slog.Error("Invalid timezone: %v", err)
		return nil
	}

	return &Config{
		GCPPort:              gcpPort,
		GCPHost:              gcpHost,
		CorePort:             corePort,
		ReadTimeout:          readTimeout,
		WriteTimeout:         writeTimeout,
		IdleTimeout:          idleTimeout,
		ReadHeaderTimeout:    readHeaderTimeout,
		RunMigrationOnStart:  runMigrationOnStart,
		DBType:               dbType,
		DBHost:               dbHost,
		DBPort:               dbPort,
		DBUser:               dbUser,
		DBPassword:           dbPassword,
		DBName:               dbName,
		DBSSLMode:            dbSSLMode,
		DBTimeZone:           location,
		DBMaxOpenConns:       dbMaxOpenConns,
		DBMaxIdleConns:       dbMaxIdleConns,
		DBConnMaxLifetime:    dbConnMaxLifetime,
		MigrationPath:        MigrationPath,
		DBAdminUser:          dbAdminUser,
		DBAdminPassword:      dbAdminPassword,
		MSIEnabled:           msiEnabled,
		MSIDBUser:            dbMSIUser,
		RefreshAdminJobSpecs: refreshAdminJobSpecs,
	}
}

func parseDuration(value string) time.Duration {
	duration, err := time.ParseDuration(value)
	if err != nil {
		slog.Error("Invalid timeout value: %v", err)
		return 0
	}
	return duration
}
