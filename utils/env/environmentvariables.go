package env

var (
	LogLevel        = GetString("LOGGER_LEVEL", "info")
	LoggerType      = GetString("LOGGER_TYPE", "slog")
	SlogHandlerType = GetString("SLOG_HANDLER_TYPE", "json")
	ExporterType    = GetString("EXPORTER_TYPE", "stdout")
	AddSource       = GetBool("ADD_LOG_SOURCE_FILE", false)
)
