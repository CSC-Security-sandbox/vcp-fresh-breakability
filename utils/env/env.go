package env

import (
    "fmt"
    "os"
    "regexp"
    "strconv"
    "strings"

    "github.com/goccy/go-yaml"
)

func init() {
    if GetBool("LOCAL_ENV", false) {
        volumeEnvPath = "./config/config.yaml"
    }
    LogLevel = GetString("LOGGER_LEVEL", "info")
    LoggerType = GetString("LOGGER_TYPE", "slog")
    SlogHandlerType = GetString("SLOG_HANDLER_TYPE", "json")
    ExporterType = GetString("EXPORTER_TYPE", "stdout")
    AddSource = GetBool("ADD_LOG_SOURCE_FILE", false)
    ServiceName = GetString("OTEL_SERVICE_NAME", "VCP-VSA")
    OtelGoogleProjectID = GetString("OTEL_GOOGLE_PROJECT_ID", "")
}

func getFromVolumeOrEnv(key string) (string, error) {
    // 1. Try from localConfig map if useVolumeEnv is true
    if useVolumeEnv {
        if val, ok := localConfig[key]; ok && val != "" {
            return val, nil
        }
        // 2. If not already loaded, try to load from volume file (YAML)
        data, err := os.ReadFile(volumeEnvPath)
        if err == nil {
            tempConfig := map[string]string{}
            if yamlErr := yaml.Unmarshal(data, &tempConfig); yamlErr == nil {
                // Cache loaded config
                localConfig = tempConfig
                if val, ok := localConfig[key]; ok && val != "" {
                    return val, nil
                }
            }
        }
    }
    // 3. Fallback to os.Getenv
    if val, found := os.LookupEnv(key); found {
        return val, nil
    }
    // 4. Not found anywhere
    return "", fmt.Errorf("environment variable or config key '%s' not found", key)
}

// IsEnvSet returns true if the specified environment variable is set, false otherwise.
func IsEnvSet(key string) bool {
    _, found := os.LookupEnv(key)
    return found
}

// GetStringFromEnvIfNil returns the specified string if non-nil.
// If nil, returns the specified environment variable if set.
// If the environment variable is not set, returns the specified default.
func GetStringFromEnvIfNil(val *string, key, def string) *string {
    if val == nil {
        v := GetString(key, def)
        val = &v
    }
    return val
}

// GetStrings returns all the environment variables that match the pattern given by regex.
func GetStrings(regex string) (variables map[string]string, err error) {
    variables = map[string]string{}
    re, err := regexp.Compile(regex)
    if err != nil {
        return variables, err
    }
    for _, element := range os.Environ() {
        variable := strings.SplitN(element, "=", 2) // Split only on the first "="
        varName := variable[0]
        if re.MatchString(varName) {
            variables[varName] = variable[1]
        }
    }
    return variables, nil
}

// GetString returns the specified environment variable.
// If the environment variable is not set, returns the specified default.
func GetString(key, def string) string {
    if env, err := getFromVolumeOrEnv(key); err == nil {
        return env
    }
    return def
}

// GetIntFromEnvIfNil returns the specified integer if non-nil.
// If nil, returns the integer representation of the specified environment variable.
// If the environment variable is not set or is not valid, returns the specified default.
func GetIntFromEnvIfNil(val *int, key string, def int) *int {
    if val == nil {
        v := GetInt(key, def)
        val = &v
    }
    return val
}

// GetInt returns an integer representation of the specified environment variable.
// If the environment variable is not set or is not valid, returns the specified default.
func GetInt(key string, def int) int {
    if val, err := getFromVolumeOrEnv(key); err == nil {
        if intVal, err1 := strconv.Atoi(val); err1 == nil {
            return intVal
        }
    }
    return def
}

// GetIntNotNegative returns an integer representation of the specified environment variable.
// If the environment variable is not set, is not valid, or is negative, returns the specified default.
func GetIntNotNegative(key string, def int) int {
    if val, err := getFromVolumeOrEnv(key); err == nil {
        if intVal, err1 := strconv.Atoi(val); err1 == nil && intVal >= 0 {
            return intVal
        }
    }
    return def
}

// GetUintFromEnvIfNil returns the specified unsigned integer if non-nil.
// If nil, returns the unsigned integer representation of the specified environment variable.
// If the environment variable is not set or is not valid, returns the specified default.
func GetUintFromEnvIfNil(val *uint, key string, def uint) *uint {
    if val == nil {
        v := GetUint(key, def)
        val = &v
    }
    return val
}

// GetUint returns an unsigned integer representation of the specified environment variable.
// If the environment variable is not set or is not valid, returns the specified default.
func GetUint(key string, def uint) uint {
    if val, err := getFromVolumeOrEnv(key); err == nil {
        if intVal, err1 := strconv.ParseUint(val, 10, 32); err1 == nil {
            return uint(intVal)
        }
    }
    return def
}

// GetUint16 returns a 16-bit unsigned integer representation of the specified environment variable.
// If the environment variable is not set or is not valid, returns the specified default.
func GetUint16(key string, def uint16) uint16 {
    if val, err := getFromVolumeOrEnv(key); err == nil {
        if intVal, err1 := strconv.ParseUint(val, 10, 16); err1 == nil {
            return uint16(intVal)
        }
    }
    return def
}

// GetInt64FromEnvIfNil returns the specified 64-bit integer if non-nil.
// If nil, returns the 64-bit integer representation of the specified environment variable.
// If the environment variable is not set or is not valid, returns the specified default.
func GetInt64FromEnvIfNil(val *int64, key string, def int64) *int64 {
    if val == nil {
        v := GetInt64(key, def)
        val = &v
    }
    return val
}

// GetInt64 returns a 64-bit integer representation of the specified environment variable.
// If the environment variable is not set or is not valid, returns the specified default.
func GetInt64(key string, def int64) int64 {
    if val, err := getFromVolumeOrEnv(key); err == nil {
        if intVal, err1 := strconv.ParseInt(val, 10, 64); err1 == nil {
            return intVal
        }
    }
    return def
}

// GetUint64FromEnvIfNil returns the specified 64-bit unsigned integer if non-nil.
// If nil, returns the 64-bit unsigned integer representation of the specified environment variable.
// If the environment variable is not set or is not valid, returns the specified default.
func GetUint64FromEnvIfNil(val *uint64, key string, def uint64) *uint64 {
    if val == nil {
        v := GetUint64(key, def)
        val = &v
    }
    return val
}

// GetUint64 returns a 64-bit unsigned integer representation of the specified environment variable.
// If the environment variable is not set or is not valid, returns the specified default.
func GetUint64(key string, def uint64) uint64 {
    if val, err := getFromVolumeOrEnv(key); err == nil {
        if intVal, err1 := strconv.ParseUint(val, 10, 64); err1 == nil {
            return intVal
        }
    }
    return def
}

// GetFloat64FromEnvIfNil returns the specified 64-bit floating-point number if non-nil.
// If nil, returns the 64-bit floating-point number representation of the specified environment variable.
// If the environment variable is not set or is not valid, returns the specified default.
func GetFloat64FromEnvIfNil(val *float64, key string, def float64) *float64 {
    if val == nil {
        v := GetFloat64(key, def)
        val = &v
    }
    return val
}

// GetFloat64 returns a 64-bit floating-point number representation of the specified environment variable.
// If the environment variable is not set or is not valid, returns the specified default.
func GetFloat64(key string, def float64) float64 {
    if val, err := getFromVolumeOrEnv(key); err == nil {
        if intVal, err1 := strconv.ParseFloat(val, 64); err1 == nil {
            return intVal
        }
    }
    return def
}

// GetBoolFromEnvIfNil returns the specified boolean if non-nil.
// If nil, returns the boolean representation of the specified environment variable.
// If the environment variable is not set or is not valid, returns the specified default.
func GetBoolFromEnvIfNil(val *bool, key string, def bool) *bool {
    if val == nil {
        v := GetBool(key, def)
        val = &v
    }
    return val
}

// GetBool returns a boolean representation of the specified environment variable.
// If the environment variable is not set or is not valid, returns the specified default.
func GetBool(key string, def bool) bool {
    val, err := getFromVolumeOrEnv(key)
    if err != nil {
        return def
    }
    env := strings.ToLower(val)
    if env != "" {
        if val, err := strconv.Atoi(env); err == nil {
            return val != 0
        }
        if strings.HasPrefix(env, "true") || strings.HasPrefix(env, "yes") {
            return true
        }
        if strings.HasPrefix(env, "false") || strings.HasPrefix(env, "no") {
            return false
        }
    }
    return def
}
