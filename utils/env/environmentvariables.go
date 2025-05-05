package env

var (
	LogLevel            = GetString("LOGGER_LEVEL", "info")
	LoggerType          = GetString("LOGGER_TYPE", "slog")
	AddSource           = GetBool("ADD_LOG_SOURCE_FILE", false)
	ServiceName         = GetString("OTEL_SERVICE_NAME", "VCP-VSA")
	OtelGoogleProjectID = GetString("OTEL_GOOGLE_PROJECT_ID", "")
)
