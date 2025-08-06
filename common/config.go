package common

import (
	"encoding/json"
	"fmt"
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

	DBTimeZone        *time.Location // Time zone for the database connection (e.g., "UTC", "Asia/Kolkata").
	DBMaxOpenConns    int            // Maximum number of open connections (0 = unlimited).
	DBMaxIdleConns    int            // Maximum number of idle connections (default: 2).
	DBConnMaxLifetime time.Duration  // Maximum time a connection can be reused before closing (e.g., 30m, 1h).

	// Metrics DB connection details (second DB)
	MetricsDBType            string
	MetricsDBHost            string
	MetricsDBPort            string
	MetricsDBUser            string
	MetricsDBPassword        string
	MetricsDBName            string
	MetricsDBSSLMode         string
	MetricsDBTimeZone        *time.Location
	MetricsDBMaxOpenConns    int
	MetricsDBMaxIdleConns    int
	MetricsDBConnMaxLifetime time.Duration
	MetricsServerPort        string

	// Credential files (used for security)
	CredentialPath string // Path to a credentials file (e.g., service account JSON for GCP, AWS).
	UsernameFile   string // Path to a file containing the database username.
	PasswordFile   string // Path to a file containing the database password.

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
	dbAdminUser := env.GetString("DB_ADMIN_USER", "")
	dbAdminPassword := env.GetString("DB_ADMIN_PASSWORD", "")
	dbMSIUser := env.GetString("DB_MSI_USER", "")
	refreshAdminJobSpecs := env.GetBool("REFRESH_ADMIN_JOB_SPECS", true)

	metricsDBType := env.GetString("METRICS_DB_TYPE", "postgres")
	metricsDBHost := env.GetString("METRICS_DB_HOST", "")
	metricsDBPort := env.GetString("METRICS_DB_PORT", "5432")
	metricsDBUser := env.GetString("METRICS_DB_USER", "")
	metricsDBPassword := env.GetString("METRICS_DB_PASSWORD", "")
	metricsDBName := env.GetString("METRICS_DB_NAME", "")
	metricsDBSSLMode := env.GetString("METRICS_DB_SSL_MODE", "disable")
	metricsDBTimeZone := env.GetString("METRICS_DB_TIMEZONE", "UTC")
	metricsDBMaxOpenConns := env.GetInt("METRICS_DB_MAX_OPEN_CONNS", 25)
	metricsDBMaxIdleConns := env.GetInt("METRICS_DB_MAX_IDLE_CONNS", 25)
	metricsDBConnMaxLifetime := parseDuration(env.GetString("METRICS_DB_CONN_MAX_LIFETIME", "1h"))
	metricsServerPort := env.GetString("METRICS_SERVER_PORT", "8080")

	location, err := time.LoadLocation(dbTimeZone)
	if err != nil {
		slog.Error("Invalid timezone: %v", err)
		return nil
	}
	metricsLocation, err := time.LoadLocation(metricsDBTimeZone)
	if err != nil {
		slog.Error("Invalid metrics DB timezone: %v", err)
		return nil
	}

	region := env.GetString("LOCAL_REGION", "")
	regionMapJsonForNodeSerialNumber := env.GetString("REGION_NUMBER_MAP", `{"africa-south1": "01","asia-east1": "02","asia-east2": "03","asia-northeast1": "04","asia-northeast2": "05","asia-northeast3": "06","asia-south1": "07","asia-south2": "08","asia-southeast1": "09","asia-southeast2": "10","australia-southeast1": "11","australia-southeast2": "12","europe-central2": "13","europe-north1": "14","europe-north2": "15","europe-southwest1": "16","europe-west1": "17","europe-west10": "18","europe-west12": "19","europe-west2": "20","europe-west3": "21","europe-west4": "22","europe-west6": "23","europe-west8": "24","europe-west9": "25","me-central1": "26","me-central2": "27","me-west1": "28","northamerica-northeast1": "29","northamerica-northeast2": "30","northamerica-south1": "31","southamerica-east1": "32","southamerica-west1": "33","us-central1": "34","us-east1": "35","us-east4": "36","us-east5": "37","us-south1": "38","us-west1": "39","us-west2": "40","us-west3": "41","us-west4": "42"}`)

	if err := validateRegionMap(region, regionMapJsonForNodeSerialNumber); err != nil {
		slog.Error("Invalid Region: %v", err)
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
		DBAdminUser:          dbAdminUser,
		DBAdminPassword:      dbAdminPassword,
		MSIEnabled:           msiEnabled,
		MSIDBUser:            dbMSIUser,
		RefreshAdminJobSpecs: refreshAdminJobSpecs,

		MetricsDBType:            metricsDBType,
		MetricsDBHost:            metricsDBHost,
		MetricsDBPort:            metricsDBPort,
		MetricsDBUser:            metricsDBUser,
		MetricsDBPassword:        metricsDBPassword,
		MetricsDBName:            metricsDBName,
		MetricsDBSSLMode:         metricsDBSSLMode,
		MetricsDBTimeZone:        metricsLocation,
		MetricsDBMaxOpenConns:    metricsDBMaxOpenConns,
		MetricsDBMaxIdleConns:    metricsDBMaxIdleConns,
		MetricsDBConnMaxLifetime: metricsDBConnMaxLifetime,
		MetricsServerPort:        metricsServerPort,
	}
}

// LoadTelemetryConfig TODO: Add telemetry config to this function
func LoadTelemetryConfig() *Config {
	return LoadConfig()
}

func parseDuration(value string) time.Duration {
	duration, err := time.ParseDuration(value)
	if err != nil {
		slog.Error("Invalid timeout value: %v", err)
		return 0
	}
	return duration
}

func validateRegionMap(region, regionMapJsonForNodeSerialNumber string) error {
	regionMap := make(map[string]string)
	if err := json.Unmarshal([]byte(regionMapJsonForNodeSerialNumber), &regionMap); err != nil {
		return fmt.Errorf("failed to parse region code map: %w", err)
	}
	_, ok := regionMap[region]
	if !ok {
		return fmt.Errorf("failed to find region code for cluster serial number generation: region %s is not mapped in region code map", region)
	}
	// Check for duplicate values
	valueSet := make(map[string]struct{})
	for _, v := range regionMap {
		if _, exists := valueSet[v]; exists {
			return fmt.Errorf("duplicate region code value found: %s", v)
		}
		valueSet[v] = struct{}{}
	}
	return nil
}
