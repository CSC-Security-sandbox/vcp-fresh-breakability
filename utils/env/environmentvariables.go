package env

import (
	"os"
	"strings"
)

var (
	volumeEnvPath       = "/etc/config/config.yaml"
	useVolumeEnv        = strings.ToLower(os.Getenv("ENABLE_VOLUME_MOUNTED_ENV")) == "true"
	localConfig         = map[string]string{}
	SlogHandlerType     string
	ExporterType        string
	LogLevel            string
	LoggerType          string
	AddSource           bool
	ServiceName         string
	OtelGoogleProjectID string
)
