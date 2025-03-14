package common

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/util/env"
	"golang.org/x/exp/slog"

	"time"
)

type Config struct {
	GCPPort           string
	CorePort          string
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	ReadHeaderTimeout time.Duration
}

func LoadConfig() *Config {
	gcpPort := env.GetString("GCP_PROXY_PORT", "8080")
	corePort := env.GetString("CORE_API_PORT", "8081")
	readTimeout := parseDuration(env.GetString("READ_TIMEOUT", "5s"))
	writeTimeout := parseDuration(env.GetString("WRITE_TIMEOUT", "10s"))
	idleTimeout := parseDuration(env.GetString("IDLE_TIMEOUT", "120s"))
	readHeaderTimeout := parseDuration(env.GetString("READ_HEADER_TIMEOUT", "2s"))

	return &Config{
		GCPPort:           gcpPort,
		CorePort:          corePort,
		ReadTimeout:       readTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       idleTimeout,
		ReadHeaderTimeout: readHeaderTimeout,
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
