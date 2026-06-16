package main

import "os"

type config struct {
	namespace  string
	dryRun     bool
	dbHost     string
	dbPort     string
	dbName     string
	dbUser     string
	dbPassword string
	dbSSLMode  string
}

func configFromEnv() config {
	return config{
		namespace:  os.Getenv("NAMESPACE"),
		dryRun:     os.Getenv("DRY_RUN") == "true",
		dbHost:     os.Getenv("DB_HOST"),
		dbPort:     envDefault("DB_PORT", "5432"),
		dbName:     envDefault("DB_NAME", "vcp"),
		dbUser:     os.Getenv("DB_USER"),
		dbPassword: os.Getenv("DB_PASSWORD"),
		dbSSLMode:  envDefault("DB_SSL_MODE", "disable"),
	}
}

func envDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
